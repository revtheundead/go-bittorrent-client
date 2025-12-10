package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Example demonstrating bandwidth rate limiting
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: rate_limit <torrent-file>")
		fmt.Println("Example: rate_limit ubuntu.torrent")
		fmt.Println()
		fmt.Println("This example demonstrates download and upload rate limiting")
		os.Exit(1)
	}

	torrentFile := os.Args[1]

	// Define rate limits
	const (
		downloadLimit = 512 * 1024  // 512 KB/s
		uploadLimit   = 128 * 1024  // 128 KB/s
	)

	fmt.Println("Rate Limiting Configuration:")
	fmt.Printf("  Download limit: %s/s\n", formatBytes(downloadLimit))
	fmt.Printf("  Upload limit:   %s/s\n", formatBytes(uploadLimit))
	fmt.Println()

	// Create client with rate limits
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
		bittorrent.WithRateLimit(downloadLimit, uploadLimit),
		bittorrent.WithLogLevel("info"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Stop()

	// Add torrent
	torrent, err := client.AddTorrent(torrentFile)
	if err != nil {
		log.Fatalf("Failed to add torrent: %v", err)
	}

	fmt.Printf("Torrent: %s\n", torrent.Name())
	fmt.Printf("Size:    %s\n", formatBytes(torrent.Size()))
	fmt.Println()

	// Start download
	if err := torrent.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Downloading with rate limits...")
	fmt.Println("Note: Download/upload rates should not exceed the configured limits")
	fmt.Println("(Press Ctrl+C to stop)")
	fmt.Println()

	// Track stats for rate limit verification
	var maxDownRate, maxUpRate float64

	// Monitor progress
	for {
		select {
		case <-sigCh:
			fmt.Printf("\n\nStopping...\n")
			torrent.Stop()

			fmt.Println("\nRate Limit Verification:")
			fmt.Printf("  Maximum download rate observed: %s/s (limit: %s/s)\n",
				formatBytes(int64(maxDownRate)),
				formatBytes(downloadLimit))
			fmt.Printf("  Maximum upload rate observed:   %s/s (limit: %s/s)\n",
				formatBytes(int64(maxUpRate)),
				formatLimit(uploadLimit))

			if maxDownRate <= float64(downloadLimit)*1.1 && maxUpRate <= float64(uploadLimit)*1.1 {
				fmt.Println("\nRate limits were successfully enforced")
			} else {
				fmt.Println("\nWarning: Rate limits may have been exceeded")
			}
			return

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventProgress:
				stats := torrent.Stats()

				// Track maximum rates
				if stats.DownloadRate > maxDownRate {
					maxDownRate = stats.DownloadRate
				}
				if stats.UploadRate > maxUpRate {
					maxUpRate = stats.UploadRate
				}

				// Calculate percentage of limit used
				downPct := (stats.DownloadRate / float64(downloadLimit)) * 100
				upPct := (stats.UploadRate / float64(uploadLimit)) * 100

				fmt.Printf("\r[%s] %.1f%% | Down: %s/s (%.0f%%) | Up: %s/s (%.0f%%) | Peers: %d",
					progressBar(stats.Progress, 20),
					stats.Progress*100,
					formatBytes(int64(stats.DownloadRate)),
					downPct,
					formatBytes(int64(stats.UploadRate)),
					upPct,
					stats.Peers)

			case bittorrent.EventComplete:
				fmt.Println("\n\nDownload complete!")
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

// formatLimit formats a rate limit value
func formatLimit(limit int64) string {
	if limit == 0 {
		return "unlimited"
	}
	return formatBytes(limit)
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
