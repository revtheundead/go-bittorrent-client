package torrent

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/revtheundead/revtorrent/internal/protocol/bencode"
)

const PieceBytes = 20

// Metainfo represents the top-level structure of a .torrent file
type Metainfo struct {
	Announce     string          // Tracker URL
	AnnounceList [][]string      // Multi-tracker support (optional)
	Info         Info            // 'Info' dictionary
	InfoHash     [sha1.Size]byte // raw SHA-1 bytes
	InfoHashHex  string          // hex-encoded 'info' hash
	CreationDate int64           // Unix timestamp (optional)
	Comment      string          // Torrent comment (optional)
	CreatedBy    string          // Client that created the torrent (optional)
}

// Info represents the 'info' dictionary in a .torrent file.
// It supports both single-file and multi-file torrents.
type Info struct {
	PieceLength int64    // "piece length" - size of each piece in bytes
	Pieces      []byte   // raw 20-byte SHA1 hashes concatenated
	PiecesHex   []string // hex-encoded hashes of each piece
	Name        string   // file or directory name

	// Single-file mode: only Length is set
	Length int64 // file length in bytes

	// Multi-file mode: Files list is populated
	Files []FileInfo // list of files in the torrent
}

// FileInfo represents a single file in a multi-file torrent
type FileInfo struct {
	Path   []string // path components (subdirectories + filename)
	Length int64    // file length in bytes
}

// IsMultiFile returns true if this is a multi-file torrent
func (i *Info) IsMultiFile() bool {
	return len(i.Files) > 0
}

// TotalLength returns the total size of all files in bytes
func (i *Info) TotalLength() int64 {
	if !i.IsMultiFile() {
		return i.Length
	}

	total := int64(0)
	for _, f := range i.Files {
		total += f.Length
	}
	return total
}

// NumPieces returns the number of pieces in the torrent
func (i *Info) NumPieces() int {
	return len(i.Pieces) / sha1.Size
}

// GetPieceHash returns the SHA-1 hash for the specified piece index
func (i *Info) GetPieceHash(index int) ([sha1.Size]byte, error) {
	var hash [sha1.Size]byte

	if index < 0 || index >= i.NumPieces() {
		return hash, fmt.Errorf("piece index %d out of range [0, %d)", index, i.NumPieces())
	}

	start := index * sha1.Size
	copy(hash[:], i.Pieces[start:start+sha1.Size])
	return hash, nil
}

// FullPath returns the full file path for a FileInfo
func (f *FileInfo) FullPath() string {
	return filepath.Join(f.Path...)
}

// GetCreationTime returns the creation time as a formatted string
func (m *Metainfo) GetCreationTime() string {
	if m.CreationDate == 0 {
		return ""
	}
	return fmt.Sprintf("%d", m.CreationDate)
}

// Parse parses a .torrent file from raw bytes.
// It supports both single-file and multi-file torrents.
func Parse(data []byte) (*Metainfo, error) {
	// Parse the dictionary with the bencode decoder which returns interface{}
	value, err := bencode.Decode(string(data))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	root, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("top-level bencode value must be a dictionary, got %T", value)
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

	// Parse the info dictionary
	info, err := parseInfo(infoDict)
	if err != nil {
		return nil, err
	}

	// Parse optional fields
	metainfo := &Metainfo{
		Announce:    announce,
		Info:        *info,
		InfoHash:    sum,
		InfoHashHex: infoHashHex,
	}

	// Parse announce-list (optional)
	if rawAnnounceList, ok := root["announce-list"]; ok {
		if announceListRaw, ok := rawAnnounceList.([]interface{}); ok {
			announceList := make([][]string, 0, len(announceListRaw))
			for _, tierRaw := range announceListRaw {
				if tier, ok := tierRaw.([]interface{}); ok {
					tierStrings := make([]string, 0, len(tier))
					for _, trackerRaw := range tier {
						if tracker, ok := trackerRaw.(string); ok {
							tierStrings = append(tierStrings, tracker)
						}
					}
					if len(tierStrings) > 0 {
						announceList = append(announceList, tierStrings)
					}
				}
			}
			metainfo.AnnounceList = announceList
		}
	}

	// Parse creation date (optional)
	if rawCreationDate, ok := root["creation date"]; ok {
		if creationDate, ok := rawCreationDate.(int64); ok {
			metainfo.CreationDate = creationDate
		}
	}

	// Parse comment (optional)
	if rawComment, ok := root["comment"]; ok {
		if comment, ok := rawComment.(string); ok {
			metainfo.Comment = comment
		}
	}

	// Parse created by (optional)
	if rawCreatedBy, ok := root["created by"]; ok {
		if createdBy, ok := rawCreatedBy.(string); ok {
			metainfo.CreatedBy = createdBy
		}
	}

	return metainfo, nil
}

// ParseInfo parses raw metadata bytes (info dictionary) from a magnet link (BEP 9)
// and creates a Metainfo struct. It verifies the info hash matches the expected value.
func ParseInfo(metadataBytes []byte, expectedInfoHash [20]byte) (*Metainfo, error) {
	// Decode the bencoded info dictionary
	value, err := bencode.Decode(string(metadataBytes))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	infoDict, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("metadata must be a dictionary, got %T", value)
	}

	// Re-encode to verify the info hash
	encodedInfo, err := bencode.Encode(infoDict)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode info dictionary: %w", err)
	}

	// Compute SHA-1 and verify
	sum := sha1.Sum([]byte(encodedInfo))
	if sum != expectedInfoHash {
		return nil, fmt.Errorf("info hash mismatch: expected %x, got %x", expectedInfoHash, sum)
	}

	// Parse the info dictionary
	info, err := parseInfo(infoDict)
	if err != nil {
		return nil, err
	}

	// Create Metainfo with the info and hash
	metainfo := &Metainfo{
		Announce:     "", // Will be set from magnet link trackers
		Info:         *info,
		InfoHash:     sum,
		InfoHashHex:  hex.EncodeToString(sum[:]),
		AnnounceList: nil, // Will be set from magnet link trackers
	}

	return metainfo, nil
}

// parseInfo parses the 'info' dictionary
func parseInfo(infoDict map[string]interface{}) (*Info, error) {
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

	// Validate pieces length
	if len(info.Pieces)%sha1.Size != 0 {
		return nil, fmt.Errorf("invalid pieces length %d (not multiple of %d)", len(info.Pieces), sha1.Size)
	}

	// Compute hex-encoded hash of each piece
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

	// Check if this is a single-file or multi-file torrent
	// Single-file: has "length" key
	// Multi-file: has "files" key
	if rawLength, hasLength := infoDict["length"]; hasLength {
		// Single-file torrent
		length, ok := rawLength.(int64)
		if !ok {
			return nil, fmt.Errorf("length must be an integer, got %T", rawLength)
		}
		info.Length = length
	} else if rawFiles, hasFiles := infoDict["files"]; hasFiles {
		// Multi-file torrent
		filesList, ok := rawFiles.([]interface{})
		if !ok {
			return nil, fmt.Errorf("files must be a list, got %T", rawFiles)
		}

		info.Files = make([]FileInfo, 0, len(filesList))
		for i, rawFile := range filesList {
			fileDict, ok := rawFile.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("files[%d] must be a dictionary, got %T", i, rawFile)
			}

			// Parse file length
			rawFileLength, ok := fileDict["length"]
			if !ok {
				return nil, fmt.Errorf("files[%d] missing required key: length", i)
			}
			fileLength, ok := rawFileLength.(int64)
			if !ok {
				return nil, fmt.Errorf("files[%d] length must be an integer, got %T", i, rawFileLength)
			}

			// Parse file path
			rawPath, ok := fileDict["path"]
			if !ok {
				return nil, fmt.Errorf("files[%d] missing required key: path", i)
			}
			pathList, ok := rawPath.([]interface{})
			if !ok {
				return nil, fmt.Errorf("files[%d] path must be a list, got %T", i, rawPath)
			}

			path := make([]string, 0, len(pathList))
			for j, rawPathComponent := range pathList {
				pathComponent, ok := rawPathComponent.(string)
				if !ok {
					return nil, fmt.Errorf("files[%d] path[%d] must be a string, got %T", i, j, rawPathComponent)
				}
				path = append(path, pathComponent)
			}

			if len(path) == 0 {
				return nil, fmt.Errorf("files[%d] path cannot be empty", i)
			}

			info.Files = append(info.Files, FileInfo{
				Path:   path,
				Length: fileLength,
			})
		}

		if len(info.Files) == 0 {
			return nil, fmt.Errorf("multi-file torrent must have at least one file")
		}
	} else {
		return nil, fmt.Errorf("info dictionary must have either 'length' (single-file) or 'files' (multi-file)")
	}

	return &info, nil
}
