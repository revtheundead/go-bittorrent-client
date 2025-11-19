package peer

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	"github.com/revtheundead/go-bittorrent-client/internal/torrent"
)

const (
	msgChoke         = 0
	msgUnchoke       = 1
	msgInterested    = 2
	msgNotInterested = 3
	msgHave          = 4
	msgBitfield      = 5
	msgRequest       = 6
	msgPiece         = 7
	msgCancel        = 8

	blockSize     = 16 * 1024 // 16 kiB
	pipelineDepth = 5         // number of requests in-flight
)

// Message is a decoded peer message (length prefix stripped)
type Message struct {
	ID      byte
	Payload []byte
}

type block struct {
	Begin int // offset within the piece
	Len   int
	Done  bool
}

// readMessage reads a single peer message from conn.
func readMessage(r io.Reader) (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	length := binary.BigEndian.Uint32((lenBuf[:]))
	if length == 0 {
		// Keep-alive: no ID, no payload
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

// writeMessage writes a single peer message with the given ID and payload
func writeMessage(w io.Writer, msg Message) error {
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

// DownloadPiece downloads a single piece from a peer over an already
// handshaken connection.
//
// It assumes:
//   - conn is a live TCP conn, after a valid handshake.
//   - info.Pieces contains concatenated 20-byte SHA1 hashes.
//   - pieceIndex is zero-based.
func DownloadPiece(conn net.Conn, info *torrent.Info, pieceIndex int) ([]byte, error) {
	// Expect a bitfield message (ID 5)
	msg, err := readMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read bitfield message: %w", err)
	}
	if msg.ID != msgBitfield {
		return nil, fmt.Errorf("expected bitfield (5), got message ID %d", msg.ID)
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

	for completedBlocks < totalBlocks {
		// Send requests while we have capacity and blocks left
		for blocksInProgress < pipelineDepth && nextToRequest < totalBlocks {
			blk := &blocks[nextToRequest]
			if err := sendRequest(conn, pieceIndex, blk.Begin, blk.Len); err != nil {
				return nil, fmt.Errorf(
					"failed to send request for block begin=%d len=%d: %w",
					blk.Begin, blk.Len, err,
				)
			}
			blocksInProgress++
			nextToRequest++
		}

		// Read messages until we handle a 'piece' block we care about
		msg, err := readMessage(conn)
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
			// TODO: Handle re-requests. Throw an error for now
			return nil, fmt.Errorf("peer choked while downloading piece %d", pieceIndex)

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

// buildBlocks splits a piece of given length into block descriptors, each of up
// to blockSize bytes
func buildBlocks(pieceLen int) []block {
	numBlocks := (pieceLen + blockSize - 1) / blockSize
	blocks := make([]block, 0, numBlocks)

	for begin := 0; begin < pieceLen; begin += blockSize {
		remaining := pieceLen - begin
		l := blockSize
		if remaining < l {
			l = remaining
		}
		blocks = append(blocks, block{
			Begin: begin,
			Len:   l,
			Done:  false,
		})
	}

	return blocks
}

// findBlock finds the block with the given begin/len in the slice
func findBlock(blocks []block, begin, length int) *block {
	for i := range blocks {
		if blocks[i].Begin == begin && blocks[i].Len == length {
			return &blocks[i]
		}
	}
	return nil
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
