package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

// Example demonstrating simultaneous download of multiple torrents
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: multi_torrent <torrent-file1> [torrent-file2] [...]")
		fmt.Println("Example: multi_torrent ubuntu.torrent debian.torrent")
		os.Exit(1)
	}

	torrentFiles := os.Args[1:]

	// Create single client for all torrents
	client, err := bittorrent.NewClient(
		bittorrent.WithDownloadPath("./downloads"),
		bittorrent.WithDHT(true),
		bittorrent.WithMaxPeers(50), // Per torrent
		bittorrent.WithLogLevel("info"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Stop()

	fmt.Printf("Adding %d torrent(s)...\n\n", len(torrentFiles))

	// Add all torrents
	torrents := make([]*bittorrent.Torrent, 0, len(torrentFiles))
	for _, file := range torrentFiles {
		torrent, err := client.AddTorrent(file)
		if err != nil {
			log.Printf("Failed to add %s: %v", file, err)
			continue
		}

		fmt.Printf("Added: %s (%s)\n", torrent.Name(), formatBytes(torrent.Size()))
		torrents = append(torrents, torrent)

		// Start download
		if err := torrent.Start(); err != nil {
			log.Printf("Failed to start %s: %v", torrent.Name(), err)
		}
	}

	if len(torrents) == 0 {
		log.Fatal("No torrents were successfully added")
	}

	fmt.Printf("\nStarted %d torrent(s)\n", len(torrents))
	fmt.Println("Press Ctrl+C to stop all downloads")
	fmt.Println()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Monitor all torrents concurrently
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i, torrent := range torrents {
		wg.Add(1)
		go monitorTorrent(torrent, i+1, stopCh, &wg)
	}

	// Wait for signal
	<-sigCh
	fmt.Println("\n\nStopping all torrents...")
	close(stopCh)

	// Stop all torrents
	for _, torrent := range torrents {
		if err := torrent.Stop(); err != nil {
			log.Printf("Error stopping %s: %v", torrent.Name(), err)
		}
	}

	// Wait for all monitors to finish
	wg.Wait()

	// Show final statistics
	fmt.Println("\nFinal Statistics:")
	fmt.Println("================")
	for i, torrent := range torrents {
		stats := torrent.Stats()
		fmt.Printf("%d. %s\n", i+1, torrent.Name())
		fmt.Printf("   Progress: %.1f%% | Downloaded: %s | Uploaded: %s\n",
			stats.Progress*100,
			formatBytes(stats.Downloaded),
			formatBytes(stats.Uploaded))
	}
}

// monitorTorrent monitors a single torrent's events
func monitorTorrent(torrent *bittorrent.Torrent, index int, stop chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-stop:
			return

		case event := <-torrent.Events():
			switch event.Type {
			case bittorrent.EventProgress:
				stats := torrent.Stats()
				fmt.Printf("[%d] %s: %.1f%% | %s/s down | %s/s up | %d peers\n",
					index,
					truncateName(torrent.Name(), 30),
					stats.Progress*100,
					formatBytes(int64(stats.DownloadRate)),
					formatBytes(int64(stats.UploadRate)),
					stats.Peers)

			case bittorrent.EventComplete:
				fmt.Printf("\n[%d] COMPLETE: %s\n\n", index, torrent.Name())

			case bittorrent.EventError:
				fmt.Printf("\n[%d] ERROR: %s - %v\n\n", index, torrent.Name(), event.Error)
				return
			}
		}
	}
}

// truncateName truncates a torrent name to fit display
func truncateName(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}
	return name[:maxLen-3] + "..."
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
