package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Example demonstrating pause/resume functionality
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: pause_resume <torrent-file>")
		fmt.Println("Example: pause_resume ubuntu.torrent")
		fmt.Println()
		fmt.Println("This example will:")
		fmt.Println("1. Start downloading")
		fmt.Println("2. Pause after 10 seconds")
		fmt.Println("3. Resume after 5 seconds")
		fmt.Println("4. Continue until complete")
		os.Exit(1)
	}

	torrentFile := os.Args[1]

	// Create client
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
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

	// Setup timers for demonstration
	pauseTimer := time.NewTimer(10 * time.Second)
	var resumeTimer *time.Timer
	paused := false
	demonstrated := false

	fmt.Println("Download started - will demonstrate pause/resume")
	fmt.Println("(Press Ctrl+C to stop early)")
	fmt.Println()

	// Monitor progress
	for {
		select {
		case <-sigCh:
			fmt.Println("\nStopping...")
			torrent.Stop()
			return

		case <-pauseTimer.C:
			if !paused {
				fmt.Println("\n[Demo] Pausing download...")
				if err := torrent.Pause(); err != nil {
					fmt.Printf("Error pausing: %v\n", err)
				} else {
					paused = true
					resumeTimer = time.NewTimer(5 * time.Second)
				}
			}

		case <-func() <-chan time.Time {
			if resumeTimer != nil {
				return resumeTimer.C
			}
			return make(chan time.Time)
		}():
			if paused {
				fmt.Println("[Demo] Resuming download...")
				if err := torrent.Resume(); err != nil {
					fmt.Printf("Error resuming: %v\n", err)
				} else {
					paused = false
					demonstrated = true
					fmt.Println("[Demo] Pause/resume demonstration complete")
					fmt.Println()
				}
			}

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventProgress:
				stats := torrent.Stats()
				statusText := ""
				if paused || stats.State.String() == "Paused" {
					statusText = " [PAUSED]"
				} else if demonstrated {
					statusText = " [RESUMED]"
				}

				fmt.Printf("\rProgress: %.1f%% | Down: %s/s | Up: %s/s | Peers: %d | State: %s%s",
					stats.Progress*100,
					formatBytes(int64(stats.DownloadRate)),
					formatBytes(int64(stats.UploadRate)),
					stats.Peers,
					stats.State,
					statusText)

			case bittorrent.EventPaused:
				paused = true
				fmt.Println("\nTorrent paused")

			case bittorrent.EventResumed:
				paused = false
				fmt.Println("\nTorrent resumed")

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
