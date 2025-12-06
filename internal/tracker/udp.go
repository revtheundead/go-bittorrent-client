package tracker

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"time"
)

const (
	// UDP tracker protocol constants (BEP 15)
	protocolID        = 0x41727101980 // Magic constant for connect
	actionConnect     = 0
	actionAnnounce    = 1
	actionScrape      = 2
	actionError       = 3
	connectionTimeout = 60 * time.Second // Connection IDs are valid for 60 seconds
)

// UDPTracker implements the UDP tracker protocol (BEP 15)
type UDPTracker struct {
	url          *url.URL
	conn         *net.UDPConn
	addr         *net.UDPAddr
	connectionID int64
	connExpiry   time.Time
}

// NewUDPTracker creates a new UDP tracker client
func NewUDPTracker(trackerURL string) (*UDPTracker, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid tracker URL: %w", err)
	}

	if u.Scheme != "udp" {
		return nil, fmt.Errorf("not a UDP tracker URL: %s", u.Scheme)
	}

	// Resolve UDP address
	addr, err := net.ResolveUDPAddr("udp4", u.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve tracker address: %w", err)
	}

	// Create UDP connection
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP connection: %w", err)
	}

	return &UDPTracker{
		url:  u,
		conn: conn,
		addr: addr,
	}, nil
}

// Close closes the UDP connection
func (t *UDPTracker) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// Announce performs a tracker announce and returns peer list
func (t *UDPTracker) Announce(req *AnnounceRequest) (*TrackerResponse, error) {
	// Ensure we have a valid connection ID
	if time.Now().After(t.connExpiry) {
		if err := t.connect(); err != nil {
			return nil, fmt.Errorf("connect failed: %w", err)
		}
	}

	// Build announce request
	buf := make([]byte, 98)
	binary.BigEndian.PutUint64(buf[0:8], uint64(t.connectionID))
	binary.BigEndian.PutUint32(buf[8:12], actionAnnounce)

	transactionID := generateTransactionID()
	binary.BigEndian.PutUint32(buf[12:16], transactionID)

	copy(buf[16:36], req.InfoHash[:])
	copy(buf[36:56], req.PeerID[:])

	binary.BigEndian.PutUint64(buf[56:64], uint64(req.Downloaded))
	binary.BigEndian.PutUint64(buf[64:72], uint64(req.Left))
	binary.BigEndian.PutUint64(buf[72:80], uint64(req.Uploaded))

	// Event (0=none, 1=completed, 2=started, 3=stopped)
	event := uint32(0)
	switch req.Event {
	case "started":
		event = 2
	case "completed":
		event = 1
	case "stopped":
		event = 3
	}
	binary.BigEndian.PutUint32(buf[80:84], event)

	// IP address (0 = default)
	binary.BigEndian.PutUint32(buf[84:88], 0)

	// Key (random)
	key := generateTransactionID()
	binary.BigEndian.PutUint32(buf[88:92], key)

	// Num want (-1 = default, 0xFFFFFFFF in unsigned representation)
	binary.BigEndian.PutUint32(buf[92:96], 0xFFFFFFFF)

	// Port
	binary.BigEndian.PutUint16(buf[96:98], uint16(req.Port))

	// Send and receive
	if _, err := t.conn.Write(buf); err != nil {
		return nil, fmt.Errorf("failed to send announce: %w", err)
	}

	respBuf := make([]byte, 1024)
	t.conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	n, err := t.conn.Read(respBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read announce response: %w", err)
	}

	return t.parseAnnounceResponse(respBuf[:n], transactionID)
}

// Scrape gets statistics about torrents
func (t *UDPTracker) Scrape(infoHashes [][20]byte) (*ScrapeResponse, error) {
	// Ensure we have a valid connection ID
	if time.Now().After(t.connExpiry) {
		if err := t.connect(); err != nil {
			return nil, fmt.Errorf("connect failed: %w", err)
		}
	}

	// Build scrape request
	buf := make([]byte, 16+20*len(infoHashes))
	binary.BigEndian.PutUint64(buf[0:8], uint64(t.connectionID))
	binary.BigEndian.PutUint32(buf[8:12], actionScrape)

	transactionID := generateTransactionID()
	binary.BigEndian.PutUint32(buf[12:16], transactionID)

	// Add info hashes
	for i, hash := range infoHashes {
		copy(buf[16+i*20:16+(i+1)*20], hash[:])
	}

	// Send and receive
	if _, err := t.conn.Write(buf); err != nil {
		return nil, fmt.Errorf("failed to send scrape: %w", err)
	}

	respBuf := make([]byte, 8+12*len(infoHashes))
	t.conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	n, err := t.conn.Read(respBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read scrape response: %w", err)
	}

	return t.parseScrapeResponse(respBuf[:n], transactionID)
}

// connect establishes a connection to the tracker and gets a connection ID
func (t *UDPTracker) connect() error {
	// Build connect request
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], protocolID)
	binary.BigEndian.PutUint32(buf[8:12], actionConnect)

	transactionID := generateTransactionID()
	binary.BigEndian.PutUint32(buf[12:16], transactionID)

	// Send request
	if _, err := t.conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send connect request: %w", err)
	}

	// Read response
	respBuf := make([]byte, 16)
	t.conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	n, err := t.conn.Read(respBuf)
	if err != nil {
		return fmt.Errorf("failed to read connect response: %w", err)
	}

	if n < 16 {
		return fmt.Errorf("connect response too short: %d bytes", n)
	}

	// Parse response
	action := binary.BigEndian.Uint32(respBuf[0:4])
	respTransactionID := binary.BigEndian.Uint32(respBuf[4:8])

	if respTransactionID != transactionID {
		return fmt.Errorf("transaction ID mismatch: got %d, expected %d", respTransactionID, transactionID)
	}

	if action == actionError {
		// Error response
		errorMsg := string(respBuf[8:n])
		return fmt.Errorf("tracker error: %s", errorMsg)
	}

	if action != actionConnect {
		return fmt.Errorf("unexpected action in connect response: %d", action)
	}

	// Extract connection ID
	t.connectionID = int64(binary.BigEndian.Uint64(respBuf[8:16]))
	t.connExpiry = time.Now().Add(connectionTimeout)

	return nil
}

// parseAnnounceResponse parses the announce response
func (t *UDPTracker) parseAnnounceResponse(data []byte, transactionID uint32) (*TrackerResponse, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("announce response too short: %d bytes", len(data))
	}

	action := binary.BigEndian.Uint32(data[0:4])
	respTransactionID := binary.BigEndian.Uint32(data[4:8])

	if respTransactionID != transactionID {
		return nil, fmt.Errorf("transaction ID mismatch: got %d, expected %d", respTransactionID, transactionID)
	}

	if action == actionError {
		// Error response
		errorMsg := string(data[8:])
		return nil, fmt.Errorf("tracker error: %s", errorMsg)
	}

	if action != actionAnnounce {
		return nil, fmt.Errorf("unexpected action in announce response: %d", action)
	}

	interval := int64(binary.BigEndian.Uint32(data[8:12]))
	leechers := binary.BigEndian.Uint32(data[12:16])
	seeders := binary.BigEndian.Uint32(data[16:20])

	// Parse peers (6 bytes each: 4 bytes IP + 2 bytes port)
	peers := make([]Peer, 0)
	peerData := data[20:]

	for i := 0; i+6 <= len(peerData); i += 6 {
		ip := net.IPv4(peerData[i], peerData[i+1], peerData[i+2], peerData[i+3])
		port := binary.BigEndian.Uint16(peerData[i+4 : i+6])

		peers = append(peers, Peer{
			IP:   ip,
			Port: port,
		})
	}

	return &TrackerResponse{
		Interval: interval,
		Peers:    peers,
		Seeders:  int(seeders),
		Leechers: int(leechers),
	}, nil
}

// parseScrapeResponse parses the scrape response
func (t *UDPTracker) parseScrapeResponse(data []byte, transactionID uint32) (*ScrapeResponse, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("scrape response too short: %d bytes", len(data))
	}

	action := binary.BigEndian.Uint32(data[0:4])
	respTransactionID := binary.BigEndian.Uint32(data[4:8])

	if respTransactionID != transactionID {
		return nil, fmt.Errorf("transaction ID mismatch: got %d, expected %d", respTransactionID, transactionID)
	}

	if action == actionError {
		// Error response
		errorMsg := string(data[8:])
		return nil, fmt.Errorf("tracker error: %s", errorMsg)
	}

	if action != actionScrape {
		return nil, fmt.Errorf("unexpected action in scrape response: %d", action)
	}

	// Parse scrape data (12 bytes per torrent)
	files := make([]ScrapeFile, 0)
	scrapeData := data[8:]

	for i := 0; i+12 <= len(scrapeData); i += 12 {
		seeders := binary.BigEndian.Uint32(scrapeData[i : i+4])
		completed := binary.BigEndian.Uint32(scrapeData[i+4 : i+8])
		leechers := binary.BigEndian.Uint32(scrapeData[i+8 : i+12])

		files = append(files, ScrapeFile{
			Seeders:   int(seeders),
			Completed: int(completed),
			Leechers:  int(leechers),
		})
	}

	return &ScrapeResponse{
		Files: files,
	}, nil
}

// generateTransactionID generates a random transaction ID
func generateTransactionID() uint32 {
	var buf [4]byte
	rand.Read(buf[:])
	return binary.BigEndian.Uint32(buf[:])
}

// AnnounceRequest represents a tracker announce request
type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Downloaded uint64
	Left       uint64
	Uploaded   uint64
	Event      string // "started", "completed", "stopped", or ""
	Port       uint16
	Compact    bool
	NumWant    uint32
}

// ScrapeResponse represents a scrape response
type ScrapeResponse struct {
	Files []ScrapeFile
}

// ScrapeFile represents scrape data for a single torrent
type ScrapeFile struct {
	Seeders   int
	Completed int
	Leechers  int
}
