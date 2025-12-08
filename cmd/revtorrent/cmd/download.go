package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/charmbracelet/lipgloss"
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
	noProgress   bool
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
	downloadCmd.Flags().BoolVar(&noProgress, "no-progress", false, "disable progress bar (show logs instead)")

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

	// Show progress bar if not in debug mode AND not disabled with --no-progress
	showProgressBar := logLevel != "debug" && !noProgress

	// Create logger (suppress INFO logs when showing progress bar)
	var logger *slog.Logger
	if showProgressBar {
		// In progress bar mode, only show WARN and above
		opts := &slog.HandlerOptions{Level: slog.LevelWarn}
		handler := slog.NewTextHandler(os.Stdout, opts)
		logger = slog.New(handler)
	} else {
		// In debug/no-progress mode, use the configured log level
		logger = createLogger()
		logger.Info("starting download",
			"source", source,
			"output", outputDir,
			"max_peers", maxPeers)
	}

	// Create client configuration
	cfg := config.Default()
	cfg.DownloadPath = outputDir
	cfg.MaxPeers = maxPeers
	cfg.DHTEnabled = !noDHT
	cfg.Seed = seedAfter
	cfg.SeedRatio = seedRatio
	// Set log level to warn when showing progress bar, otherwise use configured level
	if showProgressBar {
		cfg.LogLevel = "warn"
	} else {
		cfg.LogLevel = logLevel
	}
	cfg.ListenPort = port

	// Create client
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath(outputDir),
		bittorrent.WithPort(port),
		bittorrent.WithDHT(!noDHT),
		bittorrent.WithRateLimit(downloadRate, uploadRate),
		bittorrent.WithLogLevel(cfg.LogLevel),
	)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Stop()

	// Show initial progress bar
	if showProgressBar {
		if isMagnetLink(source) {
			fmt.Printf("⚡ Fetching metadata...\n")
		} else {
			fmt.Printf("⚡ Loading torrent...\n")
		}
	}

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

	// Track last progress bar line for clearing
	var lastLine string

	// Monitor progress
	for {
		select {
		case <-ctx.Done():
			if showProgressBar && lastLine != "" {
				fmt.Print("\n") // Final newline
			}
			return nil

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventStarted:
				if showProgressBar {
					fmt.Printf("⚡ Starting download: %s\n", torrent.Name())
				} else {
					logger.Info("download started")
				}

			case bittorrent.EventProgress:
				stats := torrent.Stats()
				if showProgressBar {
					// Determine status message
					statusMsg := ""
					if stats.Peers == 0 {
						statusMsg = "Connecting to peers..."
					} else if stats.DownloadRate < 1024 && event.Progress < 0.01 {
						statusMsg = "Starting download..."
					}
					lastLine = renderProgressBar(torrent.Name(), event.Progress, stats, false, torrent.State().String(), statusMsg)
					fmt.Print("\r" + lastLine)
				} else {
					logger.Info("progress",
						"percent", fmt.Sprintf("%.1f%%", event.Progress*100),
						"downloaded", formatBytes(stats.Downloaded),
						"uploaded", formatBytes(stats.Uploaded),
						"download_rate", formatBytes(int64(stats.DownloadRate))+"/s",
						"upload_rate", formatBytes(int64(stats.UploadRate))+"/s",
						"peers", stats.Peers)
				}

			case bittorrent.EventComplete:
				if showProgressBar {
					fmt.Print("\r" + strings.Repeat(" ", 120) + "\r") // Clear line
					fmt.Printf("✓ Download complete!\n")
				} else {
					logger.Info("download complete!")
				}
				if !seedAfter {
					return nil
				}
				if showProgressBar {
					fmt.Printf("🌱 Seeding (target ratio: %.2f)\n", seedRatio)
				} else {
					logger.Info("seeding started", "ratio_target", seedRatio)
				}

			case bittorrent.EventSeeding:
				stats := torrent.Stats()
				ratio := float64(stats.Uploaded) / float64(stats.Downloaded)
				if showProgressBar {
					statusMsg := ""
					if stats.Peers == 0 {
						statusMsg = "Waiting for peers..."
					}
					lastLine = renderProgressBar(torrent.Name(), 1.0, stats, true, torrent.State().String(), statusMsg)
					fmt.Print("\r" + lastLine)
				} else {
					logger.Info("seeding",
						"uploaded", formatBytes(stats.Uploaded),
						"ratio", fmt.Sprintf("%.2f", ratio),
						"peers", stats.Peers)
				}

				if seedRatio > 0 && ratio >= seedRatio {
					if showProgressBar {
						fmt.Print("\r" + strings.Repeat(" ", 120) + "\r") // Clear line
						fmt.Printf("✓ Seed ratio reached (%.2f)\n", ratio)
					} else {
						logger.Info("seed ratio reached, stopping",
							"ratio", fmt.Sprintf("%.2f", ratio))
					}
					return nil
				}

			case bittorrent.EventError:
				if showProgressBar {
					fmt.Print("\r" + strings.Repeat(" ", 120) + "\r") // Clear line
				}
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

func formatRate(bytesPerSec float64) string {
	const unit = 1024
	if bytesPerSec < unit {
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}

	div, exp := float64(unit), 0
	for n := bytesPerSec / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB/s", "MB/s", "GB/s"}
	return fmt.Sprintf("%.1f %s", bytesPerSec/div, units[exp])
}

func renderProgressBar(name string, progress float64, stats bittorrent.Stats, seeding bool, state string, statusMsg string) string {
	// Style definitions
	downloadColor := lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")).Bold(true)
	seedingColor := lipgloss.NewStyle().Foreground(lipgloss.Color("#00BFFF")).Bold(true)
	pausedColor := lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	infoColor := lipgloss.NewStyle().Foreground(lipgloss.Color("#AAAAAA"))
	statusColor := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFAA00"))

	// Progress bar
	barWidth := 30
	filled := int(progress * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	emptyWidth := barWidth - filled

	var bar string
	var statusStyle lipgloss.Style
	var statusText string

	if seeding {
		statusStyle = seedingColor
		statusText = "SEEDING"
		bar = strings.Repeat("█", filled) + strings.Repeat("░", emptyWidth)
	} else if state == "Paused" {
		statusStyle = pausedColor
		statusText = "PAUSED"
		bar = strings.Repeat("▓", filled) + strings.Repeat("░", emptyWidth)
	} else {
		statusStyle = downloadColor
		statusText = "DOWNLOADING"
		bar = strings.Repeat("█", filled) + strings.Repeat("░", emptyWidth)
	}

	// Format stats
	percent := fmt.Sprintf("%.1f%%", progress*100)
	downloadSpeed := formatRate(stats.DownloadRate)
	uploadSpeed := formatRate(stats.UploadRate)

	// Fix: use singular "peer" when count is 1
	var peerText string
	if stats.Peers == 1 {
		peerText = "1 peer"
	} else {
		peerText = fmt.Sprintf("%d peers", stats.Peers)
	}

	// Ratio (only if seeding)
	var ratioStr string
	if seeding && stats.Downloaded > 0 {
		ratio := float64(stats.Uploaded) / float64(stats.Downloaded)
		ratioStr = fmt.Sprintf(" | Ratio: %.2f", ratio)
	}

	// Compose the progress line
	status := statusStyle.Render(statusText)
	barStr := statusStyle.Render(bar)
	info := infoColor.Render(fmt.Sprintf("%s | ↓ %s ↑ %s | %s%s",
		percent, downloadSpeed, uploadSpeed, peerText, ratioStr))

	// Add status message if provided
	statusLine := ""
	if statusMsg != "" {
		statusLine = " " + statusColor.Render(statusMsg)
	}

	line := fmt.Sprintf("%s [%s] %s%s", status, barStr, info, statusLine)

	// Pad with spaces to clear any previous content (up to 120 chars)
	if len(line) < 120 {
		line += strings.Repeat(" ", 120-len(line))
	}

	return line
}
