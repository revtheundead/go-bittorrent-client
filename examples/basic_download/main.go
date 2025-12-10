package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Basic example demonstrating simple torrent download with progress monitoring
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: basic_download <torrent-file>")
		fmt.Println("Example: basic_download ubuntu.torrent")
		os.Exit(1)
	}

	torrentFile := os.Args[1]

	// Create a new BitTorrent client with default settings
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
		bittorrent.WithLogLevel("info"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Stop()

	// Add the torrent from file
	torrent, err := client.AddTorrent(torrentFile)
	if err != nil {
		log.Fatalf("Failed to add torrent: %v", err)
	}

	// Display torrent information
	fmt.Println("Torrent Information:")
	fmt.Printf("  Name:      %s\n", torrent.Name())
	fmt.Printf("  Size:      %s\n", formatBytes(torrent.Size()))
	fmt.Printf("  Info Hash: %s\n", torrent.InfoHash())
	fmt.Println()

	// Start the download
	if err := torrent.Start(); err != nil {
		log.Fatalf("Failed to start download: %v", err)
	}

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Monitor download progress
	fmt.Println("Downloading... (Press Ctrl+C to stop)")
	for {
		select {
		case sig := <-sigCh:
			fmt.Printf("\nReceived signal %v, stopping download...\n", sig)
			if err := torrent.Stop(); err != nil {
				log.Printf("Error stopping torrent: %v", err)
			}
			return

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventStarted:
				fmt.Println("Download started")

			case bittorrent.EventProgress:
				stats := torrent.Stats()
				fmt.Printf("\r[%s] %.1f%% | Down: %s/s | Up: %s/s | Peers: %d | State: %s",
					progressBar(stats.Progress, 30),
					stats.Progress*100,
					formatBytes(int64(stats.DownloadRate)),
					formatBytes(int64(stats.UploadRate)),
					stats.Peers,
					stats.State)

			case bittorrent.EventComplete:
				fmt.Println("\n\nDownload complete!")
				fmt.Printf("Downloaded: %s\n", formatBytes(torrent.Stats().Downloaded))
				fmt.Printf("Uploaded:   %s\n", formatBytes(torrent.Stats().Uploaded))
				return

			case bittorrent.EventError:
				fmt.Printf("\nError: %v\n", event.Error)
				return
			}
		}
	}
}

// formatBytes converts bytes to human-readable format
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

// progressBar creates a visual progress bar
func progressBar(progress float64, width int) string {
	filled := int(progress * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled

	bar := ""
	for i := 0; i < filled; i++ {
		bar += "="
	}
	if filled < width {
		bar += ">"
		empty--
	}
	for i := 0; i < empty; i++ {
		bar += " "
	}

	return bar
}
