package peer

import (
	"fmt"
	"io"

	"github.com/revtheundead/revtorrent/internal/protocol/bencode"
)

const (
	extMsgHandshake     byte = 0         // extension handshake
	utMetadataExtension byte = 1         // 1...255, not 0
	metadataPieceSize   int  = 16 * 1024 // 16 KiB
)

// SendExtensionHandshake sends a handshake to the peer to indicate extension support
func SendExtensionHandshake(w io.Writer) error {
	// {"m": {"ut_metadata": 1}}
	payloadDict := map[string]interface{}{
		"m": map[string]interface{}{
			"ut_metadata": int64(utMetadataExtension),
		},
	}

	benc, err := bencode.Encode(payloadDict)
	if err != nil {
		return fmt.Errorf("failed to bencode extension handshake: %w", err)
	}

	// payload = <ext_msg_id=0> + <bencoded_dict>
	payload := append([]byte{extMsgHandshake}, []byte(benc)...)

	msg := Message{
		ID:      MsgExtended,
		Payload: payload,
	}

	return WriteMessage(w, msg)
}

// ReceiveExtensionHandshake reads messages until it finds an extension handshake,
// then extracts and returns the peer's ut_metadata extension ID.
func ReceiveExtensionHandshake(r io.Reader) (byte, error) {
	for {
		msg, err := ReadMessage(r)
		if err != nil {
			return 0, fmt.Errorf("failed to read message while waiting for extension handshake: %w", err)
		}
		if msg == nil {
			continue
		}

		// We only care about extended messages (ID = 20)
		if msg.ID != MsgExtended {
			// Ignore keep-alives, bitfield, choke/unchoke, etc.
			continue
		}

		if len(msg.Payload) < 1 {
			// Malformed extended message
			continue
		}

		extMsgID := msg.Payload[0]
		if extMsgID != extMsgHandshake {
			// Some other extended message, not the handshake
			continue
		}

		// msg.Payload[1:] should be the bencoded dict {"m": {"ut_metadata": <id>}}
		dictBytes := msg.Payload[1:]

		rootVal, err := bencode.Decode(string(dictBytes))
		if err != nil {
			return 0, fmt.Errorf("failed to decode extension handshake: %w", err)
		}

		root, ok := rootVal.(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("extension handshake is not a dictionary, got %T", rootVal)
		}

		mVal, ok := root["m"]
		if !ok {
			return 0, fmt.Errorf("extension handshake missing 'm' key")
		}

		mDict, ok := mVal.(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("extension 'm' value is not a dictionary, got %T", mVal)
		}

		id64, err := getInt(mDict, "ut_metadata")
		if err != nil {
			return 0, err
		}

		if id64 < 1 || id64 > 255 {
			return 0, fmt.Errorf("ut_metadata id %d out of [1,255] range", id64)
		}

		return byte(id64), nil
	}
}

// SendMetadataRequest sends a metadata request (msg_type = 0) for the specified piece
// using the peer's ut_metadata extension ID.
func SendMetadataRequest(w io.Writer, peerUtMetadataID byte, piece int) error {
	if peerUtMetadataID == 0 {
		return fmt.Errorf("invalid ut_metadata extension id: 0")
	}
	if piece < 0 {
		return fmt.Errorf("invalid metadata piece index: %d", piece)
	}

	// {"msg_type": 0, "piece": <piece>}
	payloadDict := map[string]interface{}{
		"msg_type": int64(0), // request
		"piece":    int64(piece),
	}

	benc, err := bencode.Encode(payloadDict)
	if err != nil {
		return fmt.Errorf("failed to bencode metadata request: %w", err)
	}

	payload := append([]byte{peerUtMetadataID}, []byte(benc)...)

	msg := Message{
		ID:      MsgExtended,
		Payload: payload,
	}

	return WriteMessage(w, msg)
}

// FetchMetadata fetches the complete metadata from a peer using BEP 9
func FetchMetadata(rw io.ReadWriter, peerUtMetadataID byte) ([]byte, error) {
	if peerUtMetadataID == 0 {
		return nil, fmt.Errorf("invalid ut_metadata extension id: 0")
	}

	// First, request piece 0 since we don't know total_size yet
	if err := SendMetadataRequest(rw, peerUtMetadataID, 0); err != nil {
		return nil, fmt.Errorf("failed to send metadata request for piece 0: %w", err)
	}

	// Map to collect received pieces
	pieces := make(map[int][]byte)

	totalSize := -1 // from total_size field
	numPieces := -1 // derived from totalSize
	gotPieces := 0  // number of distinct pieces we have

	for {
		msg, err := ReadMessage(rw)
		if err != nil {
			return nil, fmt.Errorf("failed to read message while fetching metadata: %w", err)
		}

		if msg == nil {
			continue
		}

		// Ignore non-extended messages
		if msg.ID != MsgExtended {
			continue
		}

		if len(msg.Payload) < 2 {
			// Need at least [ext_id][bencoded dict...]
			continue
		}

		extID := msg.Payload[0]
		if extID != utMetadataExtension {
			// Some other extension message
			continue
		}

		buf := msg.Payload[1:]
		rootVal, consumed, err := bencode.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode metadata header dict: %w", err)
		}
		if consumed <= 0 || consumed > len(buf) {
			return nil, fmt.Errorf("invalid consumed length from metadata dict: %d", consumed)
		}

		root, ok := rootVal.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("metadata header is not a dictionary, got %T", rootVal)
		}

		msgType, err := getInt(root, "msg_type")
		if err != nil {
			return nil, err
		}
		pieceIdx64, err := getInt(root, "piece")
		if err != nil {
			return nil, err
		}
		pieceIdx := int(pieceIdx64)

		totalSize64, err := getInt(root, "total_size")
		if err != nil {
			return nil, err
		}
		if totalSize64 <= 0 {
			return nil, fmt.Errorf("invalid total_size in metadata header: %d", totalSize64)
		}

		switch msgType {
		case 0:
			// "request" (we shouldn't see this from a peer in this scenario)
			continue
		case 1:
			// "data", this is what we want
		case 2:
			// "reject"
			return nil, fmt.Errorf("metadata request for piece %d was rejected by peer", pieceIdx)
		default:
			// Unknown msg_type
			continue
		}

		// Initialize global size and piece count from first valid data message
		if totalSize == -1 {
			totalSize = int(totalSize64)
			numPieces = (totalSize + metadataPieceSize - 1) / metadataPieceSize

			// Request remaining pieces (1...numPieces-1)
			for i := 1; i < numPieces; i++ {
				if err := SendMetadataRequest(rw, peerUtMetadataID, i); err != nil {
					return nil, fmt.Errorf("failed to request metadata piece %d: %w", i, err)
				}
			}
		}

		if pieceIdx < 0 || pieceIdx >= numPieces {
			continue // out of range
		}

		// Extract this piece's bytes
		data := buf[consumed:]
		if len(data) == 0 {
			continue
		}

		if _, exists := pieces[pieceIdx]; !exists {
			// Make a copy to avoid aliasing
			cp := make([]byte, len(data))
			copy(cp, data)
			pieces[pieceIdx] = cp
			gotPieces++
		}

		// Check if we have every piece
		if numPieces > 0 && gotPieces == numPieces {
			break
		}
	}

	if totalSize <= 0 || numPieces <= 0 {
		return nil, fmt.Errorf("did not obtain valid metadata size")
	}

	// Reassemble the full metadata buffer in the correct order
	full := make([]byte, totalSize)
	for i := 0; i < numPieces; i++ {
		chunk, ok := pieces[i]
		if !ok {
			return nil, fmt.Errorf("missing metadata piece %d", i)
		}
		offset := i * metadataPieceSize
		if offset >= len(full) {
			return nil, fmt.Errorf("piece %d offset %d out of bounds", i, offset)
		}

		// Last piece may be shorter
		maxCopy := len(full) - offset
		if len(chunk) > maxCopy {
			chunk = chunk[:maxCopy]
		}
		copy(full[offset:offset+len(chunk)], chunk)
	}

	return full, nil
}

// getInt helper carefully extracts an integer type from a map with string keys
func getInt(m map[string]interface{}, key string) (int64, error) {
	v, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("metadata header missing key %q", key)
	}
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("metadata header %q has unexpected type %T", key, v)
	}
}
