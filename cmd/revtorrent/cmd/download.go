package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/revtheundead/revtorrent/internal/config"
	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

var (
	outputDir    string
	maxPeers     int
	downloadRate int64
	uploadRate   int64
	seedAfter    bool
	seedRatio    float64
	noDHT        bool
)

// downloadCmd represents the download command
var downloadCmd = &cobra.Command{
	Use:   "download [torrent-file or magnet-link]",
	Short: "Download a torrent or magnet link",
	Long: `Download a torrent from a .torrent file or magnet link.

Examples:
  revtorrent download ubuntu.torrent
  revtorrent download -o ~/Downloads ubuntu.torrent
  revtorrent download "magnet:?xt=urn:btih:..."
  revtorrent download --max-peers 100 --download-rate 1048576 ubuntu.torrent`,
	Args: cobra.ExactArgs(1),
	RunE: runDownload,
}

func init() {
	rootCmd.AddCommand(downloadCmd)

	// Download-specific flags
	downloadCmd.Flags().StringVarP(&outputDir, "output", "o", "./downloads", "output directory for downloaded files")
	downloadCmd.Flags().IntVar(&maxPeers, "max-peers", 50, "maximum number of peers to connect to")
	downloadCmd.Flags().Int64Var(&downloadRate, "download-rate", 0, "download rate limit in bytes/sec (0=unlimited)")
	downloadCmd.Flags().Int64Var(&uploadRate, "upload-rate", 0, "upload rate limit in bytes/sec (0=unlimited)")
	downloadCmd.Flags().BoolVar(&seedAfter, "seed", true, "continue seeding after download completes")
	downloadCmd.Flags().Float64Var(&seedRatio, "seed-ratio", 1.0, "seed until this upload/download ratio (0=seed forever)")
	downloadCmd.Flags().BoolVar(&noDHT, "no-dht", false, "disable DHT")

	// Bind flags to viper
	viper.BindPFlag("download_path", downloadCmd.Flags().Lookup("output"))
	viper.BindPFlag("max_peers", downloadCmd.Flags().Lookup("max-peers"))
	viper.BindPFlag("download_rate", downloadCmd.Flags().Lookup("download-rate"))
	viper.BindPFlag("upload_rate", downloadCmd.Flags().Lookup("upload-rate"))
	viper.BindPFlag("seed", downloadCmd.Flags().Lookup("seed"))
	viper.BindPFlag("seed_ratio", downloadCmd.Flags().Lookup("seed-ratio"))
}

func runDownload(cmd *cobra.Command, args []string) error {
	source := args[0]

	// Create logger
	logger := createLogger()

	logger.Info("starting download",
		"source", source,
		"output", outputDir,
		"max_peers", maxPeers)

	// Create client configuration
	cfg := config.Default()
	cfg.DownloadPath = outputDir
	cfg.MaxPeers = maxPeers
	cfg.DHTEnabled = !noDHT
	cfg.Seed = seedAfter
	cfg.SeedRatio = seedRatio
	cfg.LogLevel = logLevel
	cfg.ListenPort = port

	// Create client
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath(outputDir),
		bittorrent.WithPort(port),
		bittorrent.WithDHT(!noDHT),
		bittorrent.WithRateLimit(downloadRate, uploadRate),
	)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Stop()

	// Add torrent
	var torrent *bittorrent.Torrent
	if isMagnetLink(source) {
		torrent, err = client.AddMagnet(source)
		if err != nil {
			return fmt.Errorf("failed to add magnet link: %w", err)
		}
		logger.Info("added magnet link", "info_hash", torrent.InfoHash())
	} else {
		torrent, err = client.AddTorrent(source)
		if err != nil {
			return fmt.Errorf("failed to add torrent: %w", err)
		}
		logger.Info("added torrent",
			"name", torrent.Name(),
			"size", formatBytes(torrent.Size()))
	}

	// Start download
	if err := torrent.Start(); err != nil {
		return fmt.Errorf("failed to start download: %w", err)
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("received interrupt signal, shutting down...")
		cancel()
		torrent.Stop()
	}()

	// Monitor progress
	for {
		select {
		case <-ctx.Done():
			return nil

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventStarted:
				logger.Info("download started")

			case bittorrent.EventProgress:
				stats := torrent.Stats()
				logger.Info("progress",
					"percent", fmt.Sprintf("%.1f%%", event.Progress*100),
					"downloaded", formatBytes(stats.Downloaded),
					"uploaded", formatBytes(stats.Uploaded),
					"download_rate", formatBytes(int64(stats.DownloadRate))+"/s",
					"upload_rate", formatBytes(int64(stats.UploadRate))+"/s",
					"peers", stats.Peers)

			case bittorrent.EventComplete:
				logger.Info("download complete!")
				if !seedAfter {
					return nil
				}
				logger.Info("seeding started", "ratio_target", seedRatio)

			case bittorrent.EventSeeding:
				stats := torrent.Stats()
				ratio := float64(stats.Uploaded) / float64(stats.Downloaded)
				logger.Info("seeding",
					"uploaded", formatBytes(stats.Uploaded),
					"ratio", fmt.Sprintf("%.2f", ratio),
					"peers", stats.Peers)

				if seedRatio > 0 && ratio >= seedRatio {
					logger.Info("seed ratio reached, stopping",
						"ratio", fmt.Sprintf("%.2f", ratio))
					return nil
				}

			case bittorrent.EventError:
				return fmt.Errorf("download error: %w", event.Error)
			}
		}
	}
}

func createLogger() *slog.Logger {
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	return slog.New(handler)
}

func isMagnetLink(s string) bool {
	return len(s) >= 8 && s[:8] == "magnet:?"
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}
