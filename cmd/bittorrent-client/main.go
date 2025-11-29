package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return fmt.Errorf("missing command")
	}

	switch args[0] {
	case "decode":
		return runDecode(args[1:])
	case "info":
		return runInfo(args[1:])
	case "peers":
		return runPeers(args[1:])
	case "handshake":
		return runHandshake(args[1:])
	case "download_piece":
		return runDownloadPiece(args[1:])
	case "download":
		return runDownload(args[1:])
	case "magnet_parse":
		return runMagnetParse(args[1:])
	case "magnet_handshake":
		return runMagnetHandshake(args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown command: %q", args[0])
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  client decode <bencoded_value>")
	fmt.Fprintln(os.Stderr, "  client info <path-to-torrent-file>")
	fmt.Fprintln(os.Stderr, "  client peers <path-to-torrent-file>")
	fmt.Fprintln(os.Stderr, "  client handshake <path-to-torrent-file> <ip:port>")
	fmt.Fprintln(os.Stderr, "  client download_piece -o <output-path> <torrent-file> <piece-index>")
	fmt.Fprintln(os.Stderr, "  client download -o <output-path> <torrent-file>")
	fmt.Fprintln(os.Stderr, "  client magnet_parse <magnet-link>")
	fmt.Fprintln(os.Stderr, "  client magnet_handshake <magnet-link>")
}
