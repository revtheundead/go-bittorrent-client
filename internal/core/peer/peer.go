package peer

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Protocol constants
const (
	protocolName      = "BitTorrent protocol"
	protocolNameLen   = 19
	reservedBytesLen  = 8
	infoHashLen       = 20 // SHA-1 hash size
	peerIDLen         = 20
	handshakeTotalLen = 1 + protocolNameLen + reservedBytesLen + infoHashLen + peerIDLen

	extensionBitByteIndex = 5
	extensionBitMask      = 0x10
)

// Message type constants
const (
	MsgChoke         = 0
	MsgUnchoke       = 1
	MsgInterested    = 2
	MsgNotInterested = 3
	MsgHave          = 4
	MsgBitfield      = 5
	MsgRequest       = 6
	MsgPiece         = 7
	MsgCancel        = 8
	MsgExtended      = 20 // BEP 10 extension protocol
)

// Handshake represents a BitTorrent handshake message
type Handshake struct {
	Reserved [reservedBytesLen]byte
	InfoHash [infoHashLen]byte
	PeerID   [peerIDLen]byte
}

// Message is a decoded peer wire protocol message
type Message struct {
	ID      byte
	Payload []byte
}

// NewHandshake constructs a handshake with the standard protocol name and extension support
func NewHandshake(infoHash [infoHashLen]byte, peerID [peerIDLen]byte) *Handshake {
	return &Handshake{
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Serialize converts the handshake to its wire format:
// <pstrlen><pstr><reserved><info_hash><peer_id>
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 0, handshakeTotalLen)

	// pstrlen
	buf = append(buf, byte(protocolNameLen))

	// pstr
	buf = append(buf, protocolName...)

	// reserved bytes with extension bit set
	var reserved [reservedBytesLen]byte
	reserved[extensionBitByteIndex] = extensionBitMask
	buf = append(buf, reserved[:]...)

	// info_hash
	buf = append(buf, h.InfoHash[:]...)

	// peer_id
	buf = append(buf, h.PeerID[:]...)

	return buf
}

// SupportsExtensions checks if the peer supports the extension protocol (BEP 10)
func (h *Handshake) SupportsExtensions() bool {
	return (h.Reserved[extensionBitByteIndex] & extensionBitMask) != 0
}

// ReadRemoteHandshake reads and parses a handshake from the connection.
// It validates the protocol string and length, but not the info_hash.
func ReadRemoteHandshake(r io.Reader) (*Handshake, error) {
	// First, read the pstrlen byte
	var pstrlenBuf [1]byte
	if _, err := io.ReadFull(r, pstrlenBuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read handshake length: %w", err)
	}

	pstrlen := int(pstrlenBuf[0])
	if pstrlen <= 0 {
		return nil, fmt.Errorf("invalid protocol string length: %d", pstrlen)
	}

	// Read the rest: pstr + reserved + info_hash + peer_id
	restLen := pstrlen + reservedBytesLen + infoHashLen + peerIDLen
	rest := make([]byte, restLen)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, fmt.Errorf("failed to read handshake body: %w", err)
	}

	pstr := string(rest[:pstrlen])
	if pstr != protocolName {
		return nil, fmt.Errorf("unexpected protocol name: %q", pstr)
	}

	var reserved [reservedBytesLen]byte
	copy(reserved[:], rest[pstrlen:pstrlen+reservedBytesLen])

	// Extract info_hash and peer_id
	var infoHash [infoHashLen]byte
	copy(infoHash[:], rest[pstrlen+reservedBytesLen:pstrlen+reservedBytesLen+infoHashLen])

	var peerID [peerIDLen]byte
	copy(peerID[:], rest[pstrlen+reservedBytesLen+infoHashLen:])

	return &Handshake{
		Reserved: reserved,
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}

// ReadMessage reads a single peer wire protocol message from the reader.
// Returns the message with ID and payload, or an error.
func ReadMessage(r io.Reader) (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 {
		// Keep-alive message: no ID, no payload
		return &Message{ID: 0, Payload: nil}, nil
	}

	msgBuf := make([]byte, length)
	if _, err := io.ReadFull(r, msgBuf); err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}

	return &Message{
		ID:      msgBuf[0],
		Payload: msgBuf[1:],
	}, nil
}

// WriteMessage writes a single peer wire protocol message to the writer.
// Format: <length prefix><message ID><payload>
func WriteMessage(w io.Writer, msg Message) error {
	length := uint32(1 + len(msg.Payload))

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], length)

	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed to write length prefix: %w", err)
	}

	if _, err := w.Write([]byte{msg.ID}); err != nil {
		return fmt.Errorf("failed to write message ID: %w", err)
	}

	if len(msg.Payload) > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			return fmt.Errorf("failed to write payload: %w", err)
		}
	}

	return nil
}
