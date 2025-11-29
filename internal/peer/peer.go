package peer

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/revtheundead/go-bittorrent-client/internal/torrent"
	"github.com/revtheundead/go-bittorrent-client/internal/tracker"
)

const (
	protocolName      = "BitTorrent protocol"
	protocolNameLen   = 19
	reservedBytesLen  = 8
	infoHashLen       = 20 // SHA-1 hash size
	peerIDLen         = 20
	handshakeTotalLen = 1 + protocolNameLen + reservedBytesLen + infoHashLen + peerIDLen
)

type Client struct {
	Conn     net.Conn
	Bitfield []byte
	Addr     string
	Choked   bool
}

type Handshake struct {
	InfoHash [infoHashLen]byte
	PeerID   [peerIDLen]byte
}

// NewHandshake constructs a handshake with the standard protocol name and zeros
// in the reserved field. It doesn't care about extensions for now
func NewHandshake(infoHash [infoHashLen]byte, peerID [peerIDLen]byte) *Handshake {
	return &Handshake{
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Serialize converts the handshake to its wire format:
//
// <pstrlen><pstr><reserved><info_hash><peer_id>
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 0, handshakeTotalLen)

	// pstrlen
	buf = append(buf, byte(protocolNameLen))

	// pstr
	buf = append(buf, protocolName...)

	// reserved bytes with extension bit set
	// 00 00 00 00 00 10 00 00
	var reserved [reservedBytesLen]byte
	reserved[5] = 0x10
	buf = append(buf, reserved[:]...)

	// info_hash
	buf = append(buf, h.InfoHash[:]...)

	// peer_id
	buf = append(buf, h.PeerID[:]...)

	return buf
}

// NewClient initializes the connection with the peer once to avoid waiting
// unnecessarily
func NewClient(meta *torrent.Metainfo, peerID [20]byte, p tracker.Peer) (*Client, error) {
	addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))

	_, conn, err := PerformHandshake(addr, meta.InfoHash, peerID)
	if err != nil {
		return nil, fmt.Errorf("handshake with %s failed: %w", addr, err)
	}

	// Read initial bitfield once
	msg, err := readMessage(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read initial bitfield: %w", err)
	}
	if msg.ID != msgBitfield {
		conn.Close()
		return nil, fmt.Errorf("expected bitfield (5), got %d", msg.ID)
	}

	// Send interested (ID 2)
	if err := sendInterested(conn); err != nil {
		return nil, fmt.Errorf("failed to send interested message: %w", err)
	}

	// Wait for unchoke (ID 1)
	for {
		m, err := readMessage(conn)
		if err != nil {
			return nil, fmt.Errorf("failed while waiting for unchoke: %w", err)
		}

		if m.ID == msgUnchoke {
			break
		}
		// Ignore other messages for now
	}

	return &Client{
		Conn:     conn,
		Bitfield: msg.Payload,
		Addr:     addr,
		Choked:   false,
	}, nil
}

// PerformHandshake dials the given address, sends a handshake, and reads
// the remote handshake. It validates that the remote info_hash matches
// the expected hash.
func PerformHandshake(addr string, expectedInfoHash [infoHashLen]byte, ownPeerId [peerIDLen]byte) (*Handshake, net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
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

// DownloadPiece downloads a single piece from a peer over an already
// handshaken connection.
//
// It assumes:
//   - conn is a live TCP conn, after a valid handshake.
//   - info.Pieces contains concatenated 20-byte SHA1 hashes.
//   - pieceIndex is zero-based.
func (c *Client) DownloadPiece(info *torrent.Info, pieceIndex int) ([]byte, error) {
	// Check if the peer has the piece with pieceIndex
	if !c.HasPiece(pieceIndex) {
		return nil, fmt.Errorf("peer does not have requested piece with index: %d", pieceIndex)
	}

	// Calculate the exact length of this piece
	pieceLen, err := pieceLength(info, pieceIndex)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, pieceLen)

	// Determine the expected SHA-1 hash for this piece
	expHash, err := expectedPieceHash(info, pieceIndex)
	if err != nil {
		return nil, err
	}

	// Prepare the list of blocks for this piece
	blocks := buildBlocks(pieceLen)

	// Pipelined request/response loop
	var (
		nextToRequest    = 0 // index into blocks
		blocksInProgress = 0 // number of outstanding requests
		completedBlocks  = 0
		totalBlocks      = len(blocks)
	)

	// Set a per-piece read deadline to be safe
	_ = c.Conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer c.Conn.SetReadDeadline(time.Time{})

	for completedBlocks < totalBlocks {
		// Send requests while we have capacity and blocks left, only send requests if unchoked
		for !c.Choked && blocksInProgress < pipelineDepth && nextToRequest < totalBlocks {
			blk := &blocks[nextToRequest]
			if err := sendRequest(c.Conn, pieceIndex, blk.Begin, blk.Len); err != nil {
				return nil, fmt.Errorf(
					"failed to send request for block begin=%d len=%d: %w",
					blk.Begin, blk.Len, err,
				)
			}
			blocksInProgress++
			nextToRequest++
		}

		// Read messages until we handle a 'piece' block we care about
		msg, err := readMessage(c.Conn)
		if err != nil {
			return nil, fmt.Errorf("failed to read message while downloading piece: %w", err)
		}

		switch msg.ID {
		case msgPiece:
			// Parse piece message: index (4 bytes), begin (4 bytes), block (rest)
			if len(msg.Payload) < 8 {
				return nil, fmt.Errorf("piece message payload too short: %d", len(msg.Payload))
			}
			index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
			blockData := msg.Payload[8:]

			// Only process blocks for the piece we requested
			if index != pieceIndex {
				// Ignore mismatched pieces
				continue
			}

			blk := findBlock(blocks, begin, len(blockData))
			if blk == nil {
				// Could be a duplicate or something we didn't request so ignore it for now
				continue
			}
			if blk.Done {
				// Already have it so ignore duplicates
				continue
			}

			// Copy data into the correct offset of the piece buffer
			copy(buf[begin:begin+len(blockData)], blockData)
			blk.Done = true
			completedBlocks++
			blocksInProgress--

		case msgChoke:
			// Peer choked us: stop sending new requests,
			// keep reading until unchoke or timeout
			c.Choked = true

		case msgUnchoke:
			// Peer unchoked so we can resume sending requests
			c.Choked = false

		default:
			// Ignore other messages for now
			continue
		}
	}

	// Verify piece hash
	sum := sha1.Sum(buf)
	if !bytes.Equal(sum[:], expHash[:]) {
		return nil, fmt.Errorf(
			"piece hash mismatch at index %d: expected %s, got %s",
			pieceIndex,
			hex.EncodeToString(expHash[:]),
			hex.EncodeToString(sum[:]),
		)
	}

	return buf, nil
}

// peerHasPiece checks whether if the peer holds a certain piece
func (c *Client) HasPiece(pieceIndex int) bool {
	if pieceIndex < 0 {
		return false
	}

	byteIndex := pieceIndex / 8
	if byteIndex >= len(c.Bitfield) {
		return false
	}

	bitOffset := uint(7 - (pieceIndex % 8))
	return (c.Bitfield[byteIndex] & (1 << bitOffset)) != 0
}

// pieceLength computes the length of a piece, handling the last (possibly shorter)
// piece correctly
func pieceLength(info *torrent.Info, pieceIndex int) (int, error) {
	if info.PieceLength <= 0 {
		return 0, fmt.Errorf("invalid piece length %d", info.PieceLength)
	}

	totalLen := info.Length
	pl := info.PieceLength

	numPieces := int((totalLen + pl - 1) / pl)
	if pieceIndex < 0 || pieceIndex >= numPieces {
		return 0, fmt.Errorf("piece index %d out of range (0...%d)", pieceIndex, numPieces)
	}

	// For all but the last piece, the length is PieceLength
	// For the last piece, it may be shorter
	if pieceIndex == numPieces-1 {
		lastLen := int(totalLen - int64(pieceIndex)*pl)
		return lastLen, nil
	}

	return int(pl), nil
}

// expectedPieceHash extracts the 20-byte SHA-1 hash for the given piece from
// info.Pieces, which must be a concatenation of all piece hashes
func expectedPieceHash(info *torrent.Info, pieceIndex int) ([20]byte, error) {
	var out [sha1.Size]byte

	pieces := info.Pieces
	offset := pieceIndex * sha1.Size
	if offset+sha1.Size > len(pieces) {
		return out, fmt.Errorf("pieces field too short for piece index %d", pieceIndex)
	}

	copy(out[:], pieces[offset:offset+sha1.Size])
	return out, nil
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

// sendInterested sends a 'interested' message to the peer
func sendInterested(conn net.Conn) error {
	return writeMessage(conn, Message{ID: msgInterested})
}

// sendRequest sends a 'request' message for a block of a piece
func sendRequest(conn net.Conn, pieceIndex, begin, length int) error {
	payload := make([]byte, 12)

	binary.BigEndian.PutUint32(payload[0:4], uint32(pieceIndex))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))

	return writeMessage(conn, Message{msgRequest, payload})
}
