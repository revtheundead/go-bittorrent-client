package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Magnet link example demonstrating metadata fetching and download
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: magnet_download <magnet-link>")
		fmt.Println("Example: magnet_download \"magnet:?xt=urn:btih:ABC123...\"")
		os.Exit(1)
	}

	magnetLink := os.Args[1]

	// Create client with DHT enabled (important for magnet links)
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
		bittorrent.WithPort(6881),
		bittorrent.WithMaxPeers(50),
		bittorrent.WithLogLevel("info"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Stop()

	fmt.Println("Adding magnet link...")
	fmt.Println("Note: This will fetch metadata from peers via DHT/trackers")
	fmt.Println()

	// Add the magnet link
	torrent, err := client.AddMagnet(magnetLink)
	if err != nil {
		log.Fatalf("Failed to add magnet link: %v", err)
	}

	fmt.Printf("Info Hash: %s\n", torrent.InfoHash())
	fmt.Println("Fetching metadata...")
	fmt.Println()

	// Start the download (this triggers metadata fetching)
	if err := torrent.Start(); err != nil {
		log.Fatalf("Failed to start download: %v", err)
	}

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	metadataFetched := false

	// Monitor progress
	for {
		select {
		case sig := <-sigCh:
			fmt.Printf("\nReceived signal %v, stopping...\n", sig)
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

				// Show metadata information once fetched
				if !metadataFetched && torrent.Name() != "" {
					metadataFetched = true
					fmt.Println("Metadata fetched successfully!")
					fmt.Printf("  Name:  %s\n", torrent.Name())
					fmt.Printf("  Size:  %s\n", formatBytes(torrent.Size()))
					fmt.Printf("  Files: %d\n", len(mustGetFiles(torrent)))
					fmt.Println()
					fmt.Println("Downloading...")
				}

				// Show progress
				fmt.Printf("\r[%s] %.1f%% | Down: %s/s | Up: %s/s | Peers: %d",
					progressBar(stats.Progress, 30),
					stats.Progress*100,
					formatBytes(int64(stats.DownloadRate)),
					formatBytes(int64(stats.UploadRate)),
					stats.Peers)

			case bittorrent.EventComplete:
				fmt.Println("\n\nDownload complete!")
				fmt.Println("Starting to seed...")

			case bittorrent.EventSeeding:
				stats := torrent.Stats()
				ratio := 0.0
				if stats.Downloaded > 0 {
					ratio = float64(stats.Uploaded) / float64(stats.Downloaded)
				}
				fmt.Printf("\rSeeding | Uploaded: %s | Ratio: %.2f | Peers: %d",
					formatBytes(stats.Uploaded),
					ratio,
					stats.Peers)

			case bittorrent.EventError:
				fmt.Printf("\nError: %v\n", event.Error)
				return
			}
		}
	}
}

// mustGetFiles gets files or returns empty slice on error
func mustGetFiles(torrent *bittorrent.Torrent) []bittorrent.FileInfo {
	files, err := torrent.Files()
	if err != nil {
		return []bittorrent.FileInfo{}
	}
	return files
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
