package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Advanced example demonstrating interactive control, rate limiting, and state persistence
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: advanced_usage <torrent-file>")
		fmt.Println("Example: advanced_usage ubuntu.torrent")
		os.Exit(1)
	}

	torrentFile := os.Args[1]

	// Create client with advanced settings
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
		bittorrent.WithPEX(true),
		bittorrent.WithRateLimit(
			2*1024*1024, // 2 MB/s download limit
			512*1024,    // 512 KB/s upload limit
		),
		bittorrent.WithMaxPeers(75),
		bittorrent.WithStateDir("./.revtorrent-state"),
		bittorrent.WithSeed(true),
		bittorrent.WithSeedRatio(1.5), // Seed until 1.5 ratio
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

	fmt.Println("Torrent loaded successfully")
	fmt.Printf("Name: %s\n", torrent.Name())
	fmt.Printf("Size: %s\n", formatBytes(torrent.Size()))
	fmt.Printf("Info Hash: %s\n\n", torrent.InfoHash())

	// Show file list
	files, err := torrent.Files()
	if err == nil && len(files) > 0 {
		fmt.Printf("Files (%d):\n", len(files))
		for i, file := range files {
			if i < 10 { // Show first 10 files
				fmt.Printf("  %d. %s (%s)\n", i+1, file.Path, formatBytes(file.Size))
			}
		}
		if len(files) > 10 {
			fmt.Printf("  ... and %d more files\n", len(files)-10)
		}
		fmt.Println()
	}

	// Show available commands
	printHelp()

	// Start download automatically
	if err := torrent.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}
	fmt.Println("Download started")
	fmt.Println()

	// Start background event monitor
	stopMonitor := make(chan struct{})
	defer close(stopMonitor)
	go monitorEvents(torrent, stopMonitor)

	// Interactive command loop
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(command)

		if len(parts) == 0 {
			fmt.Print("> ")
			continue
		}

		switch parts[0] {
		case "start", "resume":
			if err := torrent.Resume(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Download resumed")
			}

		case "pause":
			if err := torrent.Pause(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Download paused")
			}

		case "stats", "status":
			showStats(torrent)

		case "files":
			showFiles(torrent)

		case "verify":
			fmt.Println("Verifying downloaded data...")
			if err := torrent.VerifyData(); err != nil {
				fmt.Printf("Verification failed: %v\n", err)
			} else {
				fmt.Println("Verification successful - all pieces are valid")
			}

		case "quit", "exit", "q":
			fmt.Println("Shutting down and saving state...")
			torrent.Stop()
			return

		case "help", "?":
			printHelp()

		default:
			fmt.Printf("Unknown command: %s\n", parts[0])
			fmt.Println("Type 'help' for available commands")
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error: %v", err)
	}
}

// monitorEvents monitors torrent events in the background
func monitorEvents(torrent *bittorrent.Torrent, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventComplete:
				fmt.Println("\nDownload complete!")
				fmt.Print("> ")
			case bittorrent.EventSeeding:
				// Silently handle seeding events
			case bittorrent.EventError:
				fmt.Printf("\nError: %v\n", event.Error)
				fmt.Print("> ")
			}
		}
	}
}

// printHelp displays available commands
func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  start/resume - Resume download")
	fmt.Println("  pause        - Pause download")
	fmt.Println("  stats        - Show statistics")
	fmt.Println("  files        - List torrent files")
	fmt.Println("  verify       - Verify downloaded data")
	fmt.Println("  quit/exit/q  - Quit (saves state)")
	fmt.Println("  help/?       - Show this help")
	fmt.Println()
}

// showStats displays torrent statistics
func showStats(torrent *bittorrent.Torrent) {
	stats := torrent.Stats()

	fmt.Println("\n========== Statistics ==========")
	fmt.Printf("State:         %s\n", stats.State)
	fmt.Printf("Progress:      %.1f%%\n", stats.Progress*100)
	fmt.Printf("Downloaded:    %s\n", formatBytes(stats.Downloaded))
	fmt.Printf("Uploaded:      %s\n", formatBytes(stats.Uploaded))
	fmt.Printf("Download Rate: %s/s\n", formatBytes(int64(stats.DownloadRate)))
	fmt.Printf("Upload Rate:   %s/s\n", formatBytes(int64(stats.UploadRate)))
	fmt.Printf("Peers:         %d\n", stats.Peers)

	if stats.Downloaded > 0 {
		ratio := float64(stats.Uploaded) / float64(stats.Downloaded)
		fmt.Printf("Ratio:         %.2f\n", ratio)
	}

	fmt.Printf("Pieces:        %d / %d\n", stats.PiecesComplete, stats.TotalPieces)

	if stats.ETA > 0 && stats.Progress < 1.0 {
		fmt.Printf("ETA:           %v\n", stats.ETA.Round(time.Second))
	}

	fmt.Println("================================\n")
}

// showFiles displays the list of files in the torrent
func showFiles(torrent *bittorrent.Torrent) {
	files, err := torrent.Files()
	if err != nil {
		fmt.Printf("Error getting files: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("No files found")
		return
	}

	fmt.Printf("\n========== Files (%d) ==========\n", len(files))
	for i, file := range files {
		fmt.Printf("%3d. %-50s %10s\n", i+1, truncate(file.Path, 50), formatBytes(file.Size))
	}
	fmt.Println("================================\n")
}

// truncate truncates a string to a maximum length
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
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
