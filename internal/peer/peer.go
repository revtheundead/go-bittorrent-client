package peer

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
)

const (
	protocolName      = "BitTorrent protocol"
	protocolNameLen   = 19
	reservedBytesLen  = 8
	infoHashLen       = 20 // SHA-1 hash size
	peerIDLen         = 20
	handshakeTotalLen = 1 + protocolNameLen + reservedBytesLen + infoHashLen + peerIDLen
)

type Handshake struct {
	InfoHash [infoHashLen]byte
	PeerID   [peerIDLen]byte
}

// NewHandshake constructs a handshake with the standard protocol name and zeros
// in the reserved field. It doesn't care about extensions for now
func NewHandshake(infoHash [infoHashLen]byte, peerID [peerIDLen]byte) Handshake {
	return Handshake{
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Serialize converts the handshake to its wire format:
//
// <pstrlen><pstr><reserved><info_hash><peer_id>
func (h Handshake) Serialize() []byte {
	buf := make([]byte, handshakeTotalLen)

	// pstrlen
	buf[0] = byte(protocolNameLen)

	// pstr
	copy(buf[1:], []byte(protocolName))

	// 8 zero bytes reserved by default
	// ...

	// info_hash
	copy(buf[1+protocolNameLen+reservedBytesLen:], h.InfoHash[:])

	// peer_id
	copy(buf[1+protocolNameLen+reservedBytesLen+infoHashLen:], h.PeerID[:])

	return buf
}

// PerformHandshake dials the given address, sends a handshake, and reads
// the remote handshake. It validates that the remote info_hash matches
// the expected hash.
func PerformHandshake(addr string, expectedInfoHash [infoHashLen]byte, ownPeerId [peerIDLen]byte) (*Handshake, net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to peer %q: %w", addr, err)
	}

	// If anything fails after this, close the connection.
	// On success, we return the conn to the caller
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()

	hs := NewHandshake(expectedInfoHash, ownPeerId)
	if _, err := conn.Write(hs.Serialize()); err != nil {
		return nil, nil, fmt.Errorf("failed to send handshake: %w", err)
	}

	// Read remote handshake
	remoteHs, err := readRemoteHandshake(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read remote handshake: %w", err)
	}

	// Ensure they're talking about the same torrent
	if remoteHs.InfoHash != expectedInfoHash {
		return nil, nil, fmt.Errorf("remote info_hash mismatch (got %s)", hex.EncodeToString(remoteHs.InfoHash[:]))
	}

	ok = true
	return remoteHs, conn, nil
}

// readRemoteHandshake reads and parses a handshake from the connection.
// It validates the protocol string and length, but not the info_hash.
func readRemoteHandshake(r io.Reader) (*Handshake, error) {
	// First, read the pstrlen byte
	var pstrlenBuf [1]byte
	if _, err := io.ReadFull(r, pstrlenBuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read handshake length: %w", err)
	}

	pstrlen := int(pstrlenBuf[0])
	if pstrlen <= 0 {
		return nil, fmt.Errorf("invalid protocol string length: %d", pstrlen)
	}

	// Skip reserved bytes rest[pstrlen : pstrlen+8]

	// Read the rest. pstr + reserved + info_hash + peer_id
	restLen := pstrlen + reservedBytesLen + infoHashLen + peerIDLen
	rest := make([]byte, restLen)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, fmt.Errorf("failed to read handshake body: %w", err)
	}

	pstr := string(rest[:pstrlen])
	if pstr != protocolName {
		return nil, fmt.Errorf("unexpected protocol name: %q", pstr)
	}

	// Extract info_hash and peer_id
	var infoHash [infoHashLen]byte
	copy(infoHash[:], rest[pstrlen+reservedBytesLen:pstrlen+reservedBytesLen+infoHashLen])

	var peerID [peerIDLen]byte
	copy(peerID[:], rest[pstrlen+reservedBytesLen+infoHashLen:])

	return &Handshake{
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}
