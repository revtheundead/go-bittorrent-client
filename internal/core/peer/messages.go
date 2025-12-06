package peer

import (
	"encoding/binary"
	"fmt"
	"io"
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
	pipelineDepth = 12        // number of requests in-flight
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
