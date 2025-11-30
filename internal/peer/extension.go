package peer

import (
	"fmt"
	"io"

	"github.com/revtheundead/go-bittorrent-client/pkg/bencode"
)

const (
	msgExtended         byte = 20 // extension message
	extMsgHandshake     byte = 0  // extension handshake
	utMetadataExtension byte = 1  // 1...255, not 0
)

// SendExtensionHandshake sends a handshake to the peer in order to make it known that
// our client supports extensions
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
		ID:      msgExtended,
		Payload: payload,
	}

	return writeMessage(w, msg)
}

// ReceiveExtensionHandshake reads messages from r until it finds
// an extension handshake message, then extracts and returns the
// peer's ut_metadata extension ID.
//
// It returns an error if the message is malformed or ut_metadata
// is missing / invalid.
func ReceiveExtensionHandshake(r io.Reader) (byte, error) {
	for {
		msg, err := readMessage(r)
		if err != nil {
			return 0, fmt.Errorf("failed to read message while waiting for extension handshake: %w", err)
		}
		if msg == nil {
			continue
		}

		// We only care about extended messages (ID = 20)
		if msg.ID != msgExtended {
			// Ignore keep-alives, bitfield, choke/unchoke, etc.
			continue
		}

		if len(msg.Payload) < 1 {
			// Malformed extended message, not the handshake
			continue
		}

		extMsgID := msg.Payload[0]
		if extMsgID != extMsgHandshake {
			// Some other extended message, not the handshake
			continue
		}

		// Now msg.Payload[1:] should be the bencoded dict {"m": {"ut_metadata": <id>}}
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

		utVal, ok := mDict["ut_metadata"]
		if !ok {
			return 0, fmt.Errorf("extension handshake missing 'ut_metadata' entry")
		}

		var id64 int64
		switch v := utVal.(type) {
		case int64:
			id64 = v
		case int:
			id64 = int64(v)
		case float64:
			id64 = int64(v)
		default:
			return 0, fmt.Errorf("ut_metadata id has unexpected type %T", utVal)
		}

		if id64 < 1 || id64 > 255 {
			return 0, fmt.Errorf("ut_metadata id %d out of [1,255] range", id64)
		}

		return byte(id64), nil
	}
}
