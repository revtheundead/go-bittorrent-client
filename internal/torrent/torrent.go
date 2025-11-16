package torrent

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/revtheundead/go-bittorrent-client/pkg/bencode"
)

const PieceBytes = 20

// Metainfo represents the top-level structure of a .torrent file
type Metainfo struct {
	Announce    string          // Tracker URL
	Info        Info            // 'Info' dictionary
	InfoHash    [sha1.Size]byte // raw SHA-1 bytes
	InfoHashHex string          // hex-encoded 'info' hash
}

type Info struct {
	PieceLength int64    // "piece length"
	Pieces      []byte   // raw 20-byte SHA1 hashes concatenated
	PiecesHex   []string // hex-encoded hashes of each piece
	Name        string   // file name
	Length      int64    // file length in bytes
}

// ParseSingleFile parses the given .torrent data as a single-file torrent.
// It assumes the .torrent has no "files" list inside "info"
func ParseSingleFile(data []byte) (*Metainfo, error) {
	// Parse the dictionary with the bencode decoder which returns interface{}
	value, err := bencode.Decode(string(data))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	root, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("top-level bencode value must be a dictionary, got %T, value")
	}

	// Get "announce" (tracker URL)
	rawAnnounce, ok := root["announce"]
	if !ok {
		return nil, fmt.Errorf("missing required key: 'announce'")
	}
	announce, ok := rawAnnounce.(string)
	if !ok {
		return nil, fmt.Errorf("announce must be a string, got %T", rawAnnounce)
	}

	// Get the "info" dictionary
	rawInfo, ok := root["info"]
	if !ok {
		return nil, fmt.Errorf("missing required key: 'info'")
	}
	infoDict, ok := rawInfo.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("info must be a dictionary, got %T", rawInfo)
	}

	// Re-encode the 'info' dictionary to get its hash
	encodedInfo, err := bencode.Encode(infoDict)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode the info dictionary: %w", err)
	}

	// Compute SHA-1 of the encoded info bytes
	sum := sha1.Sum([]byte(encodedInfo))
	infoHashHex := hex.EncodeToString(sum[:])

	var info Info

	// Get "piece length" from "info" (required, integer)
	rawPieceLength, ok := infoDict["piece length"]
	if !ok {
		return nil, fmt.Errorf("missing required key in info: 'piece length'")
	}
	pieceLength, ok := rawPieceLength.(int64)
	if !ok {
		return nil, fmt.Errorf("piece length must be an integer, got %T", rawPieceLength)
	}
	info.PieceLength = pieceLength

	// Get "pieces" from "info" (required, string of raw bytes)
	rawPieces, ok := infoDict["pieces"]
	if !ok {
		return nil, fmt.Errorf("missing required key in info: pieces")
	}
	piecesStr, ok := rawPieces.(string)
	if !ok {
		return nil, fmt.Errorf("pieces must be a string, got %T", rawPieces)
	}
	// Torrent spec says this is concatenated 20-byte SHA1 hashes; treat as raw bytes.
	info.Pieces = []byte(piecesStr)

	// Compute hash of each piece and store it in info
	if len(info.Pieces)%sha1.Size != 0 {
		return nil, fmt.Errorf("invalid pieces length %d (not multiple of %d)", len(info.Pieces), sha1.Size)
	}

	numPieces := len(info.Pieces) / sha1.Size
	info.PiecesHex = make([]string, 0, numPieces)

	for i := 0; i < numPieces; i++ {
		start := i * sha1.Size
		end := start + sha1.Size

		hashBytes := info.Pieces[start:end]
		info.PiecesHex = append(info.PiecesHex, hex.EncodeToString(hashBytes))
	}

	// Get "name" from "info" (required, string)
	rawName, ok := infoDict["name"]
	if !ok {
		return nil, fmt.Errorf("missing required key in info: name")
	}
	name, ok := rawName.(string)
	if !ok {
		return nil, fmt.Errorf("name must be a string, got %T", rawName)
	}
	info.Name = name

	// Get "length" from "info" (required for single-file torrents, integer)
	rawLength, ok := infoDict["length"]
	if !ok {
		return nil, fmt.Errorf("missing required key in info: length (single-file torrent)")
	}
	length, ok := rawLength.(int64)
	if !ok {
		return nil, fmt.Errorf("length must be an integer, got %T", rawLength)
	}
	info.Length = length

	return &Metainfo{
		Announce:    announce,
		Info:        info,
		InfoHash:    sum,
		InfoHashHex: infoHashHex,
	}, nil
}
