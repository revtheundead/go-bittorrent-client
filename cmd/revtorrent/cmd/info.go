package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/revtheundead/revtorrent/internal/core/torrent"
	"github.com/revtheundead/revtorrent/internal/protocol/magnet"
)

// infoCmd represents the info command
var infoCmd = &cobra.Command{
	Use:   "info [torrent-file or magnet-link]",
	Short: "Display information about a torrent or magnet link",
	Long: `Display detailed information about a torrent file or magnet link including:
  - Torrent name
  - Total size
  - Number of pieces
  - Piece length
  - Info hash
  - Tracker URLs (for torrents)
  - File list (for multi-file torrents)

Examples:
  revtorrent info ubuntu.torrent
  revtorrent info "magnet:?xt=urn:btih:..."`,
	Args: cobra.ExactArgs(1),
	RunE: runInfo,
}

func init() {
	rootCmd.AddCommand(infoCmd)
}

func runInfo(cmd *cobra.Command, args []string) error {
	source := args[0]

	if isMagnetLink(source) {
		return showMagnetInfo(source)
	}

	return showTorrentInfo(source)
}

func showTorrentInfo(filePath string) error {
	// Read torrent file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read torrent file: %w", err)
	}

	// Parse torrent
	meta, err := torrent.Parse(data)
	if err != nil {
		return fmt.Errorf("failed to parse torrent: %w", err)
	}

	// Display information
	fmt.Println("Torrent Information")
	fmt.Println("==================")
	fmt.Printf("Name:        %s\n", meta.Info.Name)
	fmt.Printf("Info Hash:   %x\n", meta.InfoHash)
	fmt.Printf("Announce:    %s\n", meta.Announce)

	if len(meta.AnnounceList) > 0 {
		fmt.Println("\nTracker Tiers:")
		for i, tier := range meta.AnnounceList {
			fmt.Printf("  Tier %d:\n", i+1)
			for _, tracker := range tier {
				fmt.Printf("    - %s\n", tracker)
			}
		}
	}

	fmt.Printf("\nPiece Length: %s\n", formatBytes(meta.Info.PieceLength))
	fmt.Printf("Num Pieces:   %d\n", meta.Info.NumPieces())
	fmt.Printf("Total Size:   %s\n", formatBytes(meta.Info.TotalLength()))

	// Display file information
	if meta.Info.IsMultiFile() {
		fmt.Printf("\nFiles: %d\n", len(meta.Info.Files))
		for i, file := range meta.Info.Files {
			fmt.Printf("  %d. %s (%s)\n", i+1, file.FullPath(), formatBytes(file.Length))
		}
	} else {
		fmt.Printf("\nSingle File: %s (%s)\n", meta.Info.Name, formatBytes(meta.Info.Length))
	}

	// Display creation date if available
	if meta.CreationDate > 0 {
		fmt.Printf("\nCreated:     %s\n", meta.GetCreationTime())
	}

	if meta.Comment != "" {
		fmt.Printf("Comment:     %s\n", meta.Comment)
	}

	if meta.CreatedBy != "" {
		fmt.Printf("Created By:  %s\n", meta.CreatedBy)
	}

	return nil
}

func showMagnetInfo(link string) error {
	// Parse magnet link
	mag, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	// Display information
	fmt.Println("Magnet Link Information")
	fmt.Println("=======================")
	fmt.Printf("Info Hash:   %x\n", mag.InfoHash)

	if mag.DisplayName != "" {
		fmt.Printf("Name:        %s\n", mag.DisplayName)
	}

	if len(mag.Trackers) > 0 {
		fmt.Println("\nTrackers:")
		for i, tracker := range mag.Trackers {
			fmt.Printf("  %d. %s\n", i+1, tracker)
		}
	}

	if mag.Length > 0 {
		fmt.Printf("\nSize:        %s\n", formatBytes(mag.Length))
	}

	fmt.Println("\nNote: Full torrent metadata will be fetched from peers during download.")

	return nil
}
