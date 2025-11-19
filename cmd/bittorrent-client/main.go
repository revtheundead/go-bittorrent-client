package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/revtheundead/go-bittorrent-client/internal/peer"
	"github.com/revtheundead/go-bittorrent-client/internal/torrent"
	"github.com/revtheundead/go-bittorrent-client/internal/tracker"
	"github.com/revtheundead/go-bittorrent-client/pkg/bencode"
)

func main() {
	// Basic argument validation
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	// A bencoded value is provided, decode it
	case "decode":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "decode: missing bencoded value")
			printUsage()
			os.Exit(1)
		}

		bencodedValue := os.Args[2]

		decoded, err := bencode.Decode(bencodedValue)
		if err != nil {
			fmt.Fprintln(os.Stderr, "decode error:", err)
			os.Exit(1)
		}

		jsonOutput, err := json.Marshal(decoded)
		if err != nil {
			fmt.Fprintln(os.Stderr, "json marshal error:", err)
			os.Exit(1)
		}

		fmt.Println(string(jsonOutput))
	// Parse the provided torrent file and display the metadata it holds
	case "info":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "info: missing .torrent file path")
			printUsage()
			os.Exit(1)
		}

		path := os.Args[2]

		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to read torrent file:", err)
			os.Exit(1)
		}

		mi, err := torrent.ParseSingleFile(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to parse torrent:", err)
		}

		// Minimal output
		fmt.Printf("Tracker URL: %s\n", mi.Announce)
		fmt.Printf("Length: %d\n", mi.Info.Length)
		fmt.Printf("Info Hash: %s\n", mi.InfoHashHex)
		fmt.Printf("Piece Length: %d\n", mi.Info.PieceLength)
		fmt.Printf("Piece Hashes:\n")
		for _, hash := range mi.Info.PiecesHex {
			fmt.Printf("%s\n", hash)
		}
	// Parse the provided torrent file and get peers from the tracker
	case "peers":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "peers: missing .torrent file path")
			printUsage()
			os.Exit(1)
		}

		path := os.Args[2]

		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to read torrent file:", err)
			os.Exit(1)
		}

		mi, err := torrent.ParseSingleFile(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to parse torrent:", err)
			os.Exit(1)
		}

		peerID := tracker.GeneratePeerID()

		resp, err := tracker.Announce(mi, peerID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to get peers from tracker:", err)
			os.Exit(1)
		}

		for _, p := range resp.Peers {
			fmt.Printf("%s:%d\n", p.IP.String(), p.Port)
		}
	// Make a handshake with a peer for the given torrent
	case "handshake":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "handshake: missing arguments")
			os.Exit(1)
		}

		peerAddr := os.Args[3]

		// Basic validation of ip:port format
		if _, _, err := net.SplitHostPort(peerAddr); err != nil {
			fmt.Fprintf(os.Stderr, "handshake: invalid peer address %q: %v\n", peerAddr, err)
			os.Exit(1)
		}

		path := os.Args[2]

		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to read torrent file:", err)
			os.Exit(1)
		}

		mi, err := torrent.ParseSingleFile(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to parse torrent:", err)
			os.Exit(1)
		}

		// Announce to tracker to get a peer list
		peerID := tracker.GeneratePeerID()

		tr, err := tracker.Announce(mi, peerID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to contact tracker:", err)
			os.Exit(1)
		}

		if len(tr.Peers) == 0 {
			fmt.Fprintln(os.Stderr, "tracker returned no peers")
			os.Exit(1)
		}

		remoteHS, conn, err := peer.PerformHandshake(peerAddr, mi.InfoHash, peerID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "handshake failed:", err)
			os.Exit(1)
		}
		defer conn.Close()

		fmt.Printf("Peer ID: %s\n", hex.EncodeToString(remoteHS.PeerID[:]))
	// Download a piece from a peer
	case "download_piece":
		if len(os.Args) < 6 || os.Args[2] != "-o" {
			fmt.Fprintln(os.Stderr, "usage: client download_piece -o <output-path> <torrent-file> <piece-index>")
			os.Exit(1)
		}

		outputPath := os.Args[3]
		torrentPath := os.Args[4]
		pieceIndexStr := os.Args[5]

		pieceIndex, err := strconv.Atoi(pieceIndexStr)
		if err != nil || pieceIndex < 0 {
			fmt.Fprintf(os.Stderr, "invalid piece index %q\n", pieceIndexStr)
			os.Exit(1)
		}

		data, err := os.ReadFile(torrentPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to read torrent file:", err)
			os.Exit(1)
		}

		mi, err := torrent.ParseSingleFile(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to parse torrent:", err)
			os.Exit(1)
		}

		peerID := tracker.GeneratePeerID()

		tr, err := tracker.Announce(mi, peerID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to contact tracker:", err)
			os.Exit(1)
		}
		if len(tr.Peers) == 0 {
			fmt.Fprintln(os.Stderr, "tracker returned no peers")
			os.Exit(1)
		}

		// Pick the first peer for now.
		p := tr.Peers[0]
		addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))

		remoteHS, conn, err := peer.PerformHandshake(addr, mi.InfoHash, peerID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "handshake failed:", err)
			os.Exit(1)
		}
		defer conn.Close()

		_ = remoteHS // we don't actually need it further here

		pieceData, err := peer.DownloadPiece(conn, &mi.Info, pieceIndex)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to download piece:", err)
			os.Exit(1)
		}

		// Create file with -rw-r--r-- permissions
		if err := os.WriteFile(outputPath, pieceData, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "failed to write piece to disk:", err)
			os.Exit(1)
		}

		fmt.Printf("File created successfully at the path %s\n", outputPath)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  client decode <bencoded_value>")
	fmt.Fprintln(os.Stderr, "  client info <path-to-torrent-file>")
	fmt.Fprintln(os.Stderr, "  client peers <path-to-torrent-file>")
	fmt.Fprintln(os.Stderr, "  client handshake <path-to-torrent-file> <ip:port>")
	fmt.Fprintln(os.Stderr, "  client download_piece -o <output-path> <torrent-file> <piece-index>")
}
