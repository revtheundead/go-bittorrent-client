package uploader

import (
	"crypto/sha1"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/core/storage"
)

// Listener handles incoming peer connections for uploading
type Listener struct {
	port        int
	infoHash    [sha1.Size]byte
	peerID      [20]byte
	storage     storage.Storage
	listener    net.Listener
	choker      *Choker
	activePeers map[string]*UploadPeer
	mu          sync.RWMutex
	stopCh      chan struct{}
	wg          sync.WaitGroup
	logger      *slog.Logger
}

// NewListener creates a new peer listener
func NewListener(port int, infoHash [sha1.Size]byte, peerID [20]byte, storage storage.Storage, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.Default()
	}

	return &Listener{
		port:        port,
		infoHash:    infoHash,
		peerID:      peerID,
		storage:     storage,
		activePeers: make(map[string]*UploadPeer),
		stopCh:      make(chan struct{}),
		logger:      logger,
		choker:      NewChoker(logger),
	}
}

// Start begins listening for incoming peer connections
func (l *Listener) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", l.port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	l.listener = ln
	l.logger.Info("peer listener started", "port", l.port)

	// Start the accept loop
	l.wg.Add(1)
	go l.acceptLoop()

	// Start the choking algorithm
	l.wg.Add(1)
	go l.choker.Run(l.stopCh, &l.wg)

	return nil
}

// Stop stops the listener and closes all connections
func (l *Listener) Stop() error {
	if l.listener == nil {
		return nil
	}

	// Signal shutdown
	close(l.stopCh)

	// Close the listener (this will unblock Accept())
	if err := l.listener.Close(); err != nil {
		l.logger.Warn("error closing listener", "error", err)
	}

	// Close all active peer connections
	l.mu.Lock()
	for addr, peer := range l.activePeers {
		peer.Close()
		delete(l.activePeers, addr)
	}
	l.mu.Unlock()

	// Wait for goroutines to finish
	l.wg.Wait()

	l.logger.Info("peer listener stopped")
	return nil
}

// acceptLoop accepts incoming connections
func (l *Listener) acceptLoop() {
	defer l.wg.Done()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			select {
			case <-l.stopCh:
				// Shutdown in progress
				return
			default:
				l.logger.Warn("failed to accept connection", "error", err)
				continue
			}
		}

		// Handle connection in a new goroutine
		l.wg.Add(1)
		go l.handleConnection(conn)
	}
}

// handleConnection handles a single incoming peer connection
func (l *Listener) handleConnection(conn net.Conn) {
	defer l.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	l.logger.Debug("incoming connection", "addr", remoteAddr)

	// Set initial timeout for handshake
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Receive handshake
	hs, err := receiveHandshake(conn)
	if err != nil {
		l.logger.Debug("handshake failed", "addr", remoteAddr, "error", err)
		return
	}

	// Verify info hash
	if hs.InfoHash != l.infoHash {
		l.logger.Debug("info hash mismatch", "addr", remoteAddr)
		return
	}

	// Send our handshake
	ourHS := Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: l.infoHash,
		PeerID:   l.peerID,
	}
	if err := sendHandshake(conn, &ourHS); err != nil {
		l.logger.Debug("failed to send handshake", "addr", remoteAddr, "error", err)
		return
	}

	// Clear deadline for normal operation
	conn.SetDeadline(time.Time{})

	// Create upload peer
	peer := NewUploadPeer(conn, remoteAddr, hs.PeerID, l.storage, l.logger)

	// Register peer
	l.mu.Lock()
	l.activePeers[remoteAddr] = peer
	l.choker.AddPeer(peer)
	l.mu.Unlock()

	l.logger.Info("peer connected", "addr", remoteAddr)

	// Send bitfield
	if err := peer.SendBitfield(); err != nil {
		l.logger.Warn("failed to send bitfield", "addr", remoteAddr, "error", err)
		l.removePeer(remoteAddr)
		return
	}

	// Handle peer messages
	if err := peer.MessageLoop(l.stopCh); err != nil {
		l.logger.Debug("peer disconnected", "addr", remoteAddr, "error", err)
	}

	// Unregister peer
	l.removePeer(remoteAddr)
}

// removePeer removes a peer from tracking
func (l *Listener) removePeer(addr string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if peer, exists := l.activePeers[addr]; exists {
		l.choker.RemovePeer(peer)
		delete(l.activePeers, addr)
		l.logger.Info("peer removed", "addr", addr)
	}
}

// GetActivePeers returns the number of active upload peers
func (l *Listener) GetActivePeers() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.activePeers)
}

// GetTotalUploaded returns the total bytes uploaded to all peers
func (l *Listener) GetTotalUploaded() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var total int64
	for _, peer := range l.activePeers {
		total += peer.Uploaded()
	}
	return total
}

// BroadcastHave broadcasts a Have message to all connected peers
func (l *Listener) BroadcastHave(pieceIndex int) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, peer := range l.activePeers {
		if err := peer.SendHave(pieceIndex); err != nil {
			l.logger.Debug("failed to send have", "addr", peer.RemoteAddr(), "error", err)
		}
	}
}

// Handshake represents a BitTorrent handshake
type Handshake struct {
	Pstr     string
	InfoHash [sha1.Size]byte
	PeerID   [20]byte
}

// receiveHandshake receives and parses a handshake from a connection
func receiveHandshake(conn net.Conn) (*Handshake, error) {
	// Read length of protocol string (1 byte)
	lengthBuf := make([]byte, 1)
	if _, err := conn.Read(lengthBuf); err != nil {
		return nil, fmt.Errorf("failed to read pstrlen: %w", err)
	}
	pstrlen := int(lengthBuf[0])

	if pstrlen == 0 {
		return nil, fmt.Errorf("invalid pstrlen: 0")
	}

	// Read the rest: pstr (19 bytes) + reserved (8 bytes) + info_hash (20 bytes) + peer_id (20 bytes)
	handshakeBuf := make([]byte, pstrlen+48)
	if _, err := conn.Read(handshakeBuf); err != nil {
		return nil, fmt.Errorf("failed to read handshake: %w", err)
	}

	hs := &Handshake{
		Pstr: string(handshakeBuf[:pstrlen]),
	}

	// Extract info hash (skip reserved bytes)
	copy(hs.InfoHash[:], handshakeBuf[pstrlen+8:pstrlen+28])

	// Extract peer ID
	copy(hs.PeerID[:], handshakeBuf[pstrlen+28:pstrlen+48])

	return hs, nil
}

// sendHandshake sends a handshake to a connection
func sendHandshake(conn net.Conn, hs *Handshake) error {
	pstrlen := byte(len(hs.Pstr))
	reserved := make([]byte, 8)

	// Build handshake buffer
	buf := make([]byte, 1+len(hs.Pstr)+8+20+20)
	buf[0] = pstrlen
	copy(buf[1:], hs.Pstr)
	copy(buf[1+len(hs.Pstr):], reserved)
	copy(buf[1+len(hs.Pstr)+8:], hs.InfoHash[:])
	copy(buf[1+len(hs.Pstr)+8+20:], hs.PeerID[:])

	_, err := conn.Write(buf)
	return err
}
