package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/revtheundead/go-bittorrent-client/internal/downloader"
	"github.com/revtheundead/go-bittorrent-client/internal/magnet"
	"github.com/revtheundead/go-bittorrent-client/internal/peer"
	"github.com/revtheundead/go-bittorrent-client/internal/torrent"
	"github.com/revtheundead/go-bittorrent-client/internal/tracker"
	"github.com/revtheundead/go-bittorrent-client/pkg/bencode"
)

func runDecode(args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("decode: missing bencoded value")
	}

	bencodedValue := args[0]

	decoded, err := bencode.Decode(bencodedValue)
	if err != nil {
		return fmt.Errorf("decode error: %w", err)
	}

	jsonOutput, err := json.Marshal(decoded)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	fmt.Println(string(jsonOutput))
	return nil
}

func runInfo(args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("info: missing .torrent file path")
	}

	mi, err := loadMetainfo(args[0])
	if err != nil {
		return err
	}

	fmt.Printf("Tracker URL: %s\n", mi.Announce)
	fmt.Printf("Length: %d\n", mi.Info.Length)
	fmt.Printf("Info Hash: %s\n", mi.InfoHashHex)
	fmt.Printf("Piece Length: %d\n", mi.Info.PieceLength)
	fmt.Printf("Piece Hashes:\n")
	for _, hash := range mi.Info.PiecesHex {
		fmt.Println(hash)
	}

	return nil
}

func runPeers(args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("peers: missing .torrent file path")
	}

	mi, err := loadMetainfo(args[0])
	if err != nil {
		return err
	}

	peerID := tracker.GeneratePeerID()

	resp, err := tracker.AnnounceTorrent(mi, peerID)
	if err != nil {
		return fmt.Errorf("failed to get peers from tracker: %w", err)
	}

	if len(resp.Peers) == 0 {
		return fmt.Errorf("tracker returned no peers")
	}

	for _, p := range resp.Peers {
		fmt.Printf("%s:%d\n", p.IP.String(), p.Port)
	}

	return nil
}

func runHandshake(args []string) error {
	// Usage: client handshake <path-to-torrent-file> <ip:port>
	if len(args) < 2 {
		printUsage()
		return fmt.Errorf("handshake: missing arguments")
	}

	torrentPath := args[0]
	peerAddr := args[1]

	if _, _, err := net.SplitHostPort(peerAddr); err != nil {
		return fmt.Errorf("handshake: invalid peer address %q: %w", peerAddr, err)
	}

	mi, err := loadMetainfo(torrentPath)
	if err != nil {
		return err
	}

	peerID := tracker.GeneratePeerID()

	remoteHS, conn, err := peer.PerformHandshake(peerAddr, mi.InfoHash, peerID)
	if err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}
	defer conn.Close()

	fmt.Printf("Peer ID: %s\n", hex.EncodeToString(remoteHS.PeerID[:]))
	return nil
}

func runDownloadPiece(args []string) error {
	// Usage: client download_piece -o <output-path> <torrent-file> <piece-index>
	if len(args) < 4 || args[0] != "-o" {
		printUsage()
		return fmt.Errorf("usage: client download_piece -o <output-path> <torrent-file> <piece-index>")
	}

	outputPath := args[1]
	torrentPath := args[2]
	pieceIndexStr := args[3]

	pieceIndex, err := strconv.Atoi(pieceIndexStr)
	if err != nil || pieceIndex < 0 {
		return fmt.Errorf("invalid piece index %q", pieceIndexStr)
	}

	mi, err := loadMetainfo(torrentPath)
	if err != nil {
		return err
	}

	peerID := tracker.GeneratePeerID()
	tr, err := tracker.AnnounceTorrent(mi, peerID)
	if err != nil {
		return fmt.Errorf("failed to contact tracker: %w", err)
	}
	if len(tr.Peers) == 0 {
		return fmt.Errorf("tracker returned no peers")
	}

	p := tr.Peers[0]

	c, err := peer.NewClient(mi.InfoHash, peerID, p)
	if err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}
	defer c.Conn.Close()

	pieceData, err := c.DownloadPiece(&mi.Info, pieceIndex)
	if err != nil {
		return fmt.Errorf("failed to download piece: %w", err)
	}

	if err := os.WriteFile(outputPath, pieceData, 0o644); err != nil {
		return fmt.Errorf("failed to write piece to disk: %w", err)
	}

	fmt.Printf("Piece downloaded successfully at the path %s\n", outputPath)
	return nil
}

func runDownload(args []string) error {
	// Usage: client download -o <output-path> <torrent-file>
	if len(args) < 3 || args[0] != "-o" {
		printUsage()
		return fmt.Errorf("usage: client download -o <output-path> <torrent-file>")
	}

	outputPath := args[1]
	torrentPath := args[2]

	mi, err := loadMetainfo(torrentPath)
	if err != nil {
		return err
	}

	peerID := tracker.GeneratePeerID()

	fileData, err := downloader.DownloadAll(mi, peerID)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	if err := os.WriteFile(outputPath, fileData, 0o644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	fmt.Printf("File downloaded successfully at the path %s\n", outputPath)
	return nil
}

func runMagnetParse(args []string) error {
	// Usage: client magnet_parse <magnet-link>
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: client magnet_parse <magnet-link>")
	}

	link := args[0]

	m, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	fmt.Printf("Tracker URL: %s\n", m.TrackerUrl)
	fmt.Printf("Info Hash: %s\n", m.InfoHashHex)

	return nil
}

func runMagnetHandshake(args []string) error {
	// Usage: client magnet_handshake <magnet-link>
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: client magnet_handshake <magnet-link>")
	}

	link := args[0]

	m, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	infoHashBytes, err := hex.DecodeString(m.InfoHashHex)
	if err != nil {
		return fmt.Errorf("invalid info hash %q: %w", m.InfoHashHex, err)
	}
	if len(infoHashBytes) != 20 {
		return fmt.Errorf("info hash must be 20 bytes, got %d", len(infoHashBytes))
	}
	var infoHash [20]byte
	copy(infoHash[:], infoHashBytes)

	peerID := tracker.GeneratePeerID()

	// Announce to tracker using magnet data
	tr, err := tracker.AnnounceMagnet(m.TrackerUrl, infoHash, peerID)
	if err != nil {
		return fmt.Errorf("failed to contact tracker: %w", err)
	}
	if len(tr.Peers) == 0 {
		return fmt.Errorf("tracker returned no peers")
	}

	// Pick first peer for now
	p := tr.Peers[0]
	addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))

	remoteHS, conn, err := peer.PerformHandshake(addr, infoHash, peerID)
	if err != nil {
		return fmt.Errorf("peer handshake failed: %w", err)
	}
	defer conn.Close()

	// Only send extension handshake if remote peer supports it
	if !remoteHS.SupportsExtensions() {
		return fmt.Errorf("peer does not support extensions")
	}

	// Send extension handshake
	if err := peer.SendExtensionHandshake(conn); err != nil {
		return fmt.Errorf("failed to send extension handshake: %w", err)
	}

	// Receive peer's extension handshake and extract ut_metadata id
	utMetaID, err := peer.ReceiveExtensionHandshake(conn)
	if err != nil {
		return fmt.Errorf("failed to receive extension handshake: %w", err)
	}

	fmt.Printf("Peer ID: %s\n", hex.EncodeToString(remoteHS.PeerID[:]))
	fmt.Printf("Peer Metadata Extension ID: %d\n", utMetaID)
	return nil
}

func runMagnetInfo(args []string) error {
	// Usage: client magnet_info <magnet-link>
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: client magnet_info <magnet-link>")
	}

	link := args[0]

	m, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	infoHashBytes, err := hex.DecodeString(m.InfoHashHex)
	if err != nil {
		return fmt.Errorf("invalid info hash %q: %w", m.InfoHashHex, err)
	}
	if len(infoHashBytes) != 20 {
		return fmt.Errorf("info hash must be 20 bytes, got %d", len(infoHashBytes))
	}
	var infoHash [20]byte
	copy(infoHash[:], infoHashBytes)

	peerID := tracker.GeneratePeerID()

	mi, err := fetchMetainfoViaMagnet(m, infoHash, infoHashBytes, peerID)
	if err != nil {
		return err
	}

	fmt.Printf("Tracker URL: %s\n", m.TrackerUrl)
	fmt.Printf("Length: %d\n", mi.Info.Length)
	fmt.Printf("Info Hash: %s\n", m.InfoHashHex)
	fmt.Printf("Piece Length: %d\n", mi.Info.PieceLength)
	fmt.Printf("Piece Hashes:\n")
	for _, h := range mi.Info.PiecesHex {
		fmt.Println(h)
	}
	return nil
}

func runMagnetDownloadPiece(args []string) error {
	// Usage: client magnet_download_piece -o <output-path> <magnet_link> <piece-index>
	if len(args) < 4 || args[0] != "-o" {
		printUsage()
		return fmt.Errorf("usage: client magnet_download_piece -o <output-path> <magnet_link> <piece-index>")
	}

	outputPath := args[1]
	link := args[2]
	pieceIndexStr := args[3]

	pieceIndex, err := strconv.Atoi(pieceIndexStr)
	if err != nil || pieceIndex < 0 {
		return fmt.Errorf("invalid piece index %q", pieceIndexStr)
	}

	m, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	infoHashBytes, err := hex.DecodeString(m.InfoHashHex)
	if err != nil {
		return fmt.Errorf("invalid info hash %q: %w", m.InfoHashHex, err)
	}
	if len(infoHashBytes) != 20 {
		return fmt.Errorf("info hash must be 20 bytes, got %d", len(infoHashBytes))
	}
	var infoHash [20]byte
	copy(infoHash[:], infoHashBytes)

	peerID := tracker.GeneratePeerID()

	// 1) Get full metadata and build a Metainfo
	miTorrent, err := fetchMetainfoViaMagnet(m, infoHash, infoHashBytes, peerID)
	if err != nil {
		return err
	}

	// 2) Now behave exactly like the .torrent-based download_piece path

	tr, err := tracker.AnnounceTorrent(miTorrent, peerID)
	if err != nil {
		return fmt.Errorf("failed to contact tracker: %w", err)
	}
	if len(tr.Peers) == 0 {
		return fmt.Errorf("tracker returned no peers")
	}

	// Pick first peer for now
	p := tr.Peers[0]

	// Use the same NewClient logic as the torrent path
	c, err := peer.NewClient(miTorrent.InfoHash, peerID, p)
	if err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}
	defer c.Conn.Close()

	pieceData, err := c.DownloadPiece(&miTorrent.Info, pieceIndex)
	if err != nil {
		return fmt.Errorf("failed to download piece: %w", err)
	}

	if err := os.WriteFile(outputPath, pieceData, 0o644); err != nil {
		return fmt.Errorf("failed to write piece to disk: %w", err)
	}

	fmt.Printf("Piece downloaded successfully at the path %s\n", outputPath)
	return nil
}

func runMagnetDownload(args []string) error {
	// Usage: client magnet_download -o <output-path> <magnet_link>
	if len(args) < 3 || args[0] != "-o" {
		printUsage()
		return fmt.Errorf("usage: client magnet_download -o <output-path> <magnet_link>")
	}

	outputPath := args[1]
	link := args[2]

	m, err := magnet.Parse(link)
	if err != nil {
		return fmt.Errorf("failed to parse magnet link: %w", err)
	}

	infoHashBytes, err := hex.DecodeString(m.InfoHashHex)
	if err != nil {
		return fmt.Errorf("invalid info hash %q: %w", m.InfoHashHex, err)
	}
	if len(infoHashBytes) != 20 {
		return fmt.Errorf("info hash must be 20 bytes, got %d", len(infoHashBytes))
	}
	var infoHash [20]byte
	copy(infoHash[:], infoHashBytes)

	peerID := tracker.GeneratePeerID()

	// 1) Get full metadata and build a Metainfo
	miTorrent, err := fetchMetainfoViaMagnet(m, infoHash, infoHashBytes, peerID)
	if err != nil {
		return err
	}

	// 2) Now just use the existing downloader path
	fileData, err := downloader.DownloadAll(miTorrent, peerID)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	if err := os.WriteFile(outputPath, fileData, 0o644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	fmt.Printf("File downloaded successfully at the path %s\n", outputPath)
	return nil
}

// loadMetainfo is a small helper that loads the torrent file
func loadMetainfo(path string) (*torrent.Metainfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read torrent file %q: %w", path, err)
	}
	mi, err := torrent.ParseSingleFile(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse torrent %q: %w", path, err)
	}
	return mi, nil
}

// fetchMetainfoViaMagnet connects to one peer from the magnet's tracker,
// performs the extension handshake, fetches the metadata via ut_metadata,
// verifies the info-hash, and returns a fully-populated Metainfo.
//
// It also forces mi.InfoHash / InfoHashHex to match the magnet link's hash,
// so all later tracker announces use the **correct** info_hash.
func fetchMetainfoViaMagnet(
	m *magnet.Magnet,
	infoHash [20]byte,
	infoHashBytes []byte,
	peerID [20]byte,
) (*torrent.Metainfo, error) {
	// 1) Get a peer from the tracker using the magnet's tracker URL + info hash.
	tr, err := tracker.AnnounceMagnet(m.TrackerUrl, infoHash, peerID)
	if err != nil {
		return nil, fmt.Errorf("failed to contact tracker for metadata: %w", err)
	}
	if len(tr.Peers) == 0 {
		return nil, fmt.Errorf("tracker returned no peers")
	}

	// Pick first peer for metadata fetching
	p := tr.Peers[0]
	addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))

	// 2) Base handshake
	remoteHS, conn, err := peer.PerformHandshake(addr, infoHash, peerID)
	if err != nil {
		return nil, fmt.Errorf("peer handshake failed: %w", err)
	}
	defer conn.Close()

	// 3) Check extension support
	if !remoteHS.SupportsExtensions() {
		return nil, fmt.Errorf("peer does not support extensions")
	}

	// 4) Extension handshake (we advertise ut_metadata)
	if err := peer.SendExtensionHandshake(conn); err != nil {
		return nil, fmt.Errorf("failed to send extension handshake: %w", err)
	}

	// 5) Receive peer's extension handshake and get their ut_metadata ID
	utMetaID, err := peer.ReceiveExtensionHandshake(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to receive extension handshake: %w", err)
	}

	// 6) Fetch the full metadata via ut_metadata
	infoBytes, err := peer.FetchMetadata(conn, utMetaID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	// 7) Verify SHA-1(infoBytes) matches magnet's info hash
	sum := sha1.Sum(infoBytes)
	if !bytes.Equal(sum[:], infoHashBytes) {
		return nil, fmt.Errorf(
			"info hash mismatch: expected %s, got %s",
			hex.EncodeToString(infoHashBytes),
			hex.EncodeToString(sum[:]),
		)
	}

	// 8) Decode info dict
	infoVal, err := bencode.Decode(string(infoBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to decode info dictionary: %w", err)
	}
	infoDict, ok := infoVal.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("info dictionary has unexpected type %T", infoVal)
	}

	// 9) Build a synthetic .torrent root and parse it with existing parser
	root := map[string]interface{}{
		"announce": m.TrackerUrl,
		"info":     infoDict,
	}
	torrentBytes, err := bencode.Encode(root)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode synthetic torrent: %w", err)
	}

	mi, err := torrent.ParseSingleFile([]byte(torrentBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to parse synthetic torrent: %w", err)
	}

	// 10) Force InfoHash to match the magnet link's info-hash (canonical one)
	mi.InfoHash = infoHash
	mi.InfoHashHex = hex.EncodeToString(infoHashBytes)

	return mi, nil
}
