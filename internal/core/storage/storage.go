package storage

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/revtheundead/revtorrent/internal/core/torrent"
)

// Storage defines the interface for torrent file storage
type Storage interface {
	// Init initializes the storage (creates directories, preallocates files)
	Init() error

	// ReadPiece reads piece data into the provided buffer
	// Returns the number of bytes read
	ReadPiece(pieceIndex int, buf []byte) (int, error)

	// WritePiece writes piece data to storage
	WritePiece(pieceIndex int, data []byte) error

	// HasPiece returns true if the piece is complete and verified
	HasPiece(pieceIndex int) bool

	// ClearPiece marks a piece as incomplete (used when verification fails)
	ClearPiece(pieceIndex int)

	// VerifyPiece verifies the SHA-1 hash of a piece
	VerifyPiece(pieceIndex int) (bool, error)

	// Completion returns the download completion percentage (0.0 to 1.0)
	Completion() float64

	// CompletedPieces returns the number of completed pieces
	CompletedPieces() int

	// TotalPieces returns the total number of pieces
	TotalPieces() int

	// Bitfield returns a copy of the completion bitfield
	Bitfield() *Bitfield

	// Close closes all file handles
	Close() error
}

// FileStorage implements Storage for disk-based file storage
type FileStorage struct {
	basePath    string
	info        *torrent.Info
	files       []*os.File
	fileOffsets []int64 // Starting byte offset of each file in the torrent
	bitfield    *Bitfield
	mu          sync.RWMutex
}

// fileMapping represents how a byte range maps to a file
type fileMapping struct {
	FileIndex  int   // Index into files array
	FileOffset int64 // Offset within the file
	Length     int   // Number of bytes
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(basePath string, info *torrent.Info) *FileStorage {
	numPieces := info.NumPieces()

	return &FileStorage{
		basePath: basePath,
		info:     info,
		bitfield: NewBitfield(numPieces),
	}
}

// Init initializes the storage by creating directories and preallocating files
func (fs *FileStorage) Init() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.info.IsMultiFile() {
		return fs.initMultiFile()
	}
	return fs.initSingleFile()
}

// initSingleFile initializes storage for a single-file torrent
func (fs *FileStorage) initSingleFile() error {
	// Ensure base directory exists
	if err := os.MkdirAll(fs.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	filePath := filepath.Join(fs.basePath, fs.info.Name)

	// Create or open the file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	// Pre-allocate the file to the full size
	if err := file.Truncate(fs.info.Length); err != nil {
		file.Close()
		return fmt.Errorf("failed to preallocate file: %w", err)
	}

	fs.files = []*os.File{file}
	fs.fileOffsets = []int64{0}

	return nil
}

// initMultiFile initializes storage for a multi-file torrent
func (fs *FileStorage) initMultiFile() error {
	// Create base directory
	baseDir := filepath.Join(fs.basePath, fs.info.Name)
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	fs.files = make([]*os.File, len(fs.info.Files))
	fs.fileOffsets = make([]int64, len(fs.info.Files))

	currentOffset := int64(0)

	for i, fileInfo := range fs.info.Files {
		// Build full file path
		fullPath := filepath.Join(baseDir, fileInfo.FullPath())

		// Create parent directories
		parentDir := filepath.Dir(fullPath)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			fs.closeAllFiles()
			return fmt.Errorf("failed to create directory %s: %w", parentDir, err)
		}

		// Create or open the file
		file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fs.closeAllFiles()
			return fmt.Errorf("failed to create file %s: %w", fullPath, err)
		}

		// Pre-allocate the file
		if err := file.Truncate(fileInfo.Length); err != nil {
			file.Close()
			fs.closeAllFiles()
			return fmt.Errorf("failed to preallocate file %s: %w", fullPath, err)
		}

		fs.files[i] = file
		fs.fileOffsets[i] = currentOffset
		currentOffset += fileInfo.Length
	}

	return nil
}

// ReadPiece reads piece data into the provided buffer
func (fs *FileStorage) ReadPiece(pieceIndex int, buf []byte) (int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if pieceIndex < 0 || pieceIndex >= fs.info.NumPieces() {
		return 0, fmt.Errorf("piece index out of range: %d", pieceIndex)
	}

	pieceLength := fs.getPieceLength(pieceIndex)
	if len(buf) < pieceLength {
		return 0, fmt.Errorf("buffer too small: need %d, got %d", pieceLength, len(buf))
	}

	offset := int64(pieceIndex) * fs.info.PieceLength
	mappings := fs.calculateFileMapping(offset, int64(pieceLength))

	totalRead := 0
	for _, mapping := range mappings {
		file := fs.files[mapping.FileIndex]
		n, err := file.ReadAt(buf[totalRead:totalRead+mapping.Length], mapping.FileOffset)
		if err != nil && n == 0 {
			return totalRead, fmt.Errorf("failed to read from file: %w", err)
		}
		totalRead += n
	}

	return totalRead, nil
}

// WritePiece writes piece data to storage
func (fs *FileStorage) WritePiece(pieceIndex int, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if pieceIndex < 0 || pieceIndex >= fs.info.NumPieces() {
		return fmt.Errorf("piece index out of range: %d", pieceIndex)
	}

	offset := int64(pieceIndex) * fs.info.PieceLength
	mappings := fs.calculateFileMapping(offset, int64(len(data)))

	totalWritten := 0
	for _, mapping := range mappings {
		file := fs.files[mapping.FileIndex]
		n, err := file.WriteAt(data[totalWritten:totalWritten+mapping.Length], mapping.FileOffset)
		if err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
		totalWritten += n
	}

	// Mark piece as complete (will be verified separately)
	fs.bitfield.Set(pieceIndex)

	return nil
}

// HasPiece returns true if the piece is marked as complete
func (fs *FileStorage) HasPiece(pieceIndex int) bool {
	return fs.bitfield.Has(pieceIndex)
}

// ClearPiece marks a piece as incomplete (used when hash verification fails)
func (fs *FileStorage) ClearPiece(pieceIndex int) {
	fs.bitfield.Clear(pieceIndex)
}

// VerifyPiece verifies the SHA-1 hash of a piece
func (fs *FileStorage) VerifyPiece(pieceIndex int) (bool, error) {
	pieceLength := fs.getPieceLength(pieceIndex)
	buf := make([]byte, pieceLength)

	n, err := fs.ReadPiece(pieceIndex, buf)
	if err != nil {
		return false, err
	}

	if n != pieceLength {
		return false, fmt.Errorf("incomplete read: got %d bytes, expected %d", n, pieceLength)
	}

	// Calculate SHA-1 hash
	hash := sha1.Sum(buf[:n])

	// Get expected hash
	expectedHash, err := fs.info.GetPieceHash(pieceIndex)
	if err != nil {
		return false, err
	}

	return hash == expectedHash, nil
}

// Completion returns the download completion percentage
func (fs *FileStorage) Completion() float64 {
	completed := fs.bitfield.Count()
	total := fs.info.NumPieces()

	if total == 0 {
		return 0.0
	}

	return float64(completed) / float64(total)
}

// CompletedPieces returns the number of completed pieces
func (fs *FileStorage) CompletedPieces() int {
	return fs.bitfield.Count()
}

// TotalPieces returns the total number of pieces
func (fs *FileStorage) TotalPieces() int {
	return fs.info.NumPieces()
}

// Bitfield returns a copy of the completion bitfield
func (fs *FileStorage) Bitfield() *Bitfield {
	return fs.bitfield
}

// IsComplete returns true if all pieces are downloaded
func (fs *FileStorage) IsComplete() bool {
	return fs.bitfield.IsComplete()
}

// GetProgress returns the download progress (0.0 to 1.0) - alias for Completion
func (fs *FileStorage) GetProgress() float64 {
	return fs.Completion()
}

// Close closes all file handles
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.closeAllFiles()
}

// closeAllFiles closes all open file handles (internal, not thread-safe)
func (fs *FileStorage) closeAllFiles() error {
	var firstErr error

	for i, file := range fs.files {
		if file != nil {
			if err := file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			fs.files[i] = nil
		}
	}

	return firstErr
}

// getPieceLength returns the length of a specific piece
// The last piece may be shorter than PieceLength
func (fs *FileStorage) getPieceLength(pieceIndex int) int {
	totalLength := fs.info.TotalLength()
	pieceLength := fs.info.PieceLength

	begin := int64(pieceIndex) * pieceLength
	end := begin + pieceLength

	if end > totalLength {
		end = totalLength
	}

	return int(end - begin)
}

// calculateFileMapping determines which files a byte range spans
func (fs *FileStorage) calculateFileMapping(offset int64, length int64) []fileMapping {
	var mappings []fileMapping

	if !fs.info.IsMultiFile() {
		// Single file - simple case
		return []fileMapping{{
			FileIndex:  0,
			FileOffset: offset,
			Length:     int(length),
		}}
	}

	// Multi-file - need to map across files
	remaining := length
	currentOffset := offset

	for i, fileInfo := range fs.info.Files {
		fileStart := fs.fileOffsets[i]
		fileEnd := fileStart + fileInfo.Length

		// Skip files that don't overlap with our range
		if currentOffset >= fileEnd {
			continue
		}

		// Calculate overlap
		fileOffset := currentOffset - fileStart
		if fileOffset < 0 {
			fileOffset = 0
		}

		bytesInFile := fileEnd - (fileStart + fileOffset)
		if bytesInFile > remaining {
			bytesInFile = remaining
		}

		mappings = append(mappings, fileMapping{
			FileIndex:  i,
			FileOffset: fileOffset,
			Length:     int(bytesInFile),
		})

		remaining -= bytesInFile
		currentOffset += bytesInFile

		if remaining <= 0 {
			break
		}
	}

	return mappings
}
