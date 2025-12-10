package uploader

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/core/storage"
)

// Message IDs
const (
	MsgChoke         uint8 = 0
	MsgUnchoke       uint8 = 1
	MsgInterested    uint8 = 2
	MsgNotInterested uint8 = 3
	MsgHave          uint8 = 4
	MsgBitfield      uint8 = 5
	MsgRequest       uint8 = 6
	MsgPiece         uint8 = 7
	MsgCancel        uint8 = 8
)

// UploadPeer represents a peer connection for uploading
type UploadPeer struct {
	conn       net.Conn
	remoteAddr string
	peerID     [20]byte
	storage    storage.Storage
	logger     *slog.Logger

	// State
	choked     bool
	interested bool

	// Statistics
	uploaded   int64
	uploadRate float64 // bytes per second
	lastActive time.Time

	mu sync.RWMutex
}

// NewUploadPeer creates a new upload peer
func NewUploadPeer(conn net.Conn, remoteAddr string, peerID [20]byte, storage storage.Storage, logger *slog.Logger) *UploadPeer {
	return &UploadPeer{
		conn:       conn,
		remoteAddr: remoteAddr,
		peerID:     peerID,
		storage:    storage,
		logger:     logger,
		choked:     true, // Start choked
		interested: false,
		lastActive: time.Now(),
	}
}

// MessageLoop handles incoming messages from the peer
func (p *UploadPeer) MessageLoop(stopCh <-chan struct{}) error {
	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		// Set read deadline
		p.conn.SetReadDeadline(time.Now().Add(2 * time.Minute))

		msg, err := p.readMessage()
		if err != nil {
			return err
		}

		if msg == nil {
			// Keep-alive message
			continue
		}

		p.mu.Lock()
		p.lastActive = time.Now()
		p.mu.Unlock()

		if err := p.handleMessage(msg); err != nil {
			p.logger.Warn("failed to handle message", "addr", p.remoteAddr, "error", err)
			return err
		}
	}
}

// handleMessage processes a single message
func (p *UploadPeer) handleMessage(msg *Message) error {
	switch msg.ID {
	case MsgInterested:
		p.mu.Lock()
		p.interested = true
		p.mu.Unlock()
		p.logger.Debug("peer interested", "addr", p.remoteAddr)

	case MsgNotInterested:
		p.mu.Lock()
		p.interested = false
		p.mu.Unlock()
		p.logger.Debug("peer not interested", "addr", p.remoteAddr)

	case MsgRequest:
		if p.IsChoked() {
			// Ignore requests while choked
			return nil
		}

		if len(msg.Payload) < 12 {
			return fmt.Errorf("invalid request message length: %d", len(msg.Payload))
		}

		pieceIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
		begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
		length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))

		if err := p.handleRequest(pieceIndex, begin, length); err != nil {
			p.logger.Warn("failed to handle request", "addr", p.remoteAddr, "error", err)
		}

	case MsgCancel:
		// Cancel message handling: Would require maintaining a request queue to cancel
		// pending requests. This is an optimization for endgame mode when peers send
		// duplicate requests and cancel them after receiving the piece from another peer.
		// Current implementation doesn't track pending requests, so we ignore cancel messages.
		p.logger.Debug("received cancel (ignored)", "addr", p.remoteAddr)

	default:
		p.logger.Debug("received message", "addr", p.remoteAddr, "id", msg.ID)
	}

	return nil
}

// handleRequest handles a piece request from the peer
func (p *UploadPeer) handleRequest(pieceIndex int, begin int, length int) error {
	// Validate request
	if length > 16*1024 {
		return fmt.Errorf("request length too large: %d", length)
	}

	// Check if we have the piece
	if !p.storage.HasPiece(pieceIndex) {
		p.logger.Debug("requested piece not available", "piece", pieceIndex, "addr", p.remoteAddr)
		return nil
	}

	// Read the piece data
	pieceData := make([]byte, length)
	fullPiece := make([]byte, 256*1024) // Max piece size
	n, err := p.storage.ReadPiece(pieceIndex, fullPiece)
	if err != nil {
		return fmt.Errorf("failed to read piece: %w", err)
	}

	// Extract the requested block
	if begin+length > n {
		return fmt.Errorf("request out of bounds: piece %d, begin %d, length %d, piece size %d",
			pieceIndex, begin, length, n)
	}

	copy(pieceData, fullPiece[begin:begin+length])

	// Send the piece
	if err := p.SendPiece(pieceIndex, begin, pieceData); err != nil {
		return fmt.Errorf("failed to send piece: %w", err)
	}

	// Update statistics
	p.mu.Lock()
	p.uploaded += int64(length)
	p.mu.Unlock()

	return nil
}

// SendBitfield sends our bitfield to the peer
func (p *UploadPeer) SendBitfield() error {
	bitfield := p.storage.Bitfield()
	payload := bitfield.Bytes()

	msg := &Message{
		ID:      MsgBitfield,
		Payload: payload,
	}

	return p.sendMessage(msg)
}

// SendHave sends a Have message for a piece
func (p *UploadPeer) SendHave(pieceIndex int) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(pieceIndex))

	msg := &Message{
		ID:      MsgHave,
		Payload: payload,
	}

	return p.sendMessage(msg)
}

// SendPiece sends piece data to the peer
func (p *UploadPeer) SendPiece(pieceIndex int, begin int, data []byte) error {
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[0:4], uint32(pieceIndex))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	copy(payload[8:], data)

	msg := &Message{
		ID:      MsgPiece,
		Payload: payload,
	}

	return p.sendMessage(msg)
}

// Choke chokes the peer
func (p *UploadPeer) Choke() error {
	p.mu.Lock()
	if p.choked {
		p.mu.Unlock()
		return nil
	}
	p.choked = true
	p.mu.Unlock()

	msg := &Message{ID: MsgChoke}
	return p.sendMessage(msg)
}

// Unchoke unchokes the peer
func (p *UploadPeer) Unchoke() error {
	p.mu.Lock()
	if !p.choked {
		p.mu.Unlock()
		return nil
	}
	p.choked = false
	p.mu.Unlock()

	msg := &Message{ID: MsgUnchoke}
	return p.sendMessage(msg)
}

// IsChoked returns whether the peer is choked
func (p *UploadPeer) IsChoked() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.choked
}

// IsInterested returns whether the peer is interested
func (p *UploadPeer) IsInterested() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.interested
}

// UploadRate returns the current upload rate
func (p *UploadPeer) UploadRate() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.uploadRate
}

// Uploaded returns total bytes uploaded to this peer
func (p *UploadPeer) Uploaded() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.uploaded
}

// RemoteAddr returns the peer's remote address
func (p *UploadPeer) RemoteAddr() string {
	return p.remoteAddr
}

// PeerID returns the peer's ID
func (p *UploadPeer) PeerID() [20]byte {
	return p.peerID
}

// UpdateUploadRate updates the upload rate calculation
func (p *UploadPeer) UpdateUploadRate(rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uploadRate = rate
}

// Close closes the peer connection
func (p *UploadPeer) Close() error {
	return p.conn.Close()
}

// Message represents a BitTorrent peer message
type Message struct {
	ID      uint8
	Payload []byte
}

// readMessage reads a single message from the connection
func (p *UploadPeer) readMessage() (*Message, error) {
	// Read length prefix (4 bytes)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(p.conn, lengthBuf); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf)

	// Keep-alive message
	if length == 0 {
		return nil, nil
	}

	// Read message ID (1 byte)
	idBuf := make([]byte, 1)
	if _, err := io.ReadFull(p.conn, idBuf); err != nil {
		return nil, err
	}

	// Read payload
	payloadLen := length - 1
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(p.conn, payload); err != nil {
			return nil, err
		}
	}

	return &Message{
		ID:      idBuf[0],
		Payload: payload,
	}, nil
}

// sendMessage sends a message to the peer
func (p *UploadPeer) sendMessage(msg *Message) error {
	length := uint32(1 + len(msg.Payload))

	// Build message buffer
	buf := make([]byte, 4+1+len(msg.Payload))
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = msg.ID
	copy(buf[5:], msg.Payload)

	p.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err := p.conn.Write(buf)
	return err
}
