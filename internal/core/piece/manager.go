package piece

import (
	"crypto/sha1"
	"fmt"
	"sync"

	"github.com/revtheundead/revtorrent/internal/core/storage"
	"github.com/revtheundead/revtorrent/internal/core/torrent"
)

const (
	// BlockSize is the standard block size for piece downloads (16 KiB)
	BlockSize = 16 * 1024
)

// Manager coordinates piece downloads and assembly
type Manager struct {
	info    *torrent.Info
	storage storage.Storage
	pieces  map[int]*PieceDownload
	mu      sync.RWMutex
}

// PieceDownload represents an in-progress piece download
type PieceDownload struct {
	Index      int
	Length     int
	Blocks     []Block
	Buffer     []byte
	Downloaded int
	mu         sync.Mutex
}

// Block represents a single block within a piece
type Block struct {
	Offset    int
	Length    int
	Done      bool
	Requested bool // Tracks if block is currently requested (in-flight)
}

// NewManager creates a new piece manager
func NewManager(info *torrent.Info, storage storage.Storage) *Manager {
	return &Manager{
		info:    info,
		storage: storage,
		pieces:  make(map[int]*PieceDownload),
	}
}

// StartPiece initializes tracking for a piece download
func (m *Manager) StartPiece(pieceIndex int) (*PieceDownload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if piece is already complete
	if m.storage.HasPiece(pieceIndex) {
		return nil, fmt.Errorf("piece %d already complete", pieceIndex)
	}

	// Check if already downloading
	if pd, exists := m.pieces[pieceIndex]; exists {
		return pd, nil
	}

	// Calculate piece length
	pieceLength := m.calculatePieceLength(pieceIndex)

	// Create piece download
	pd := &PieceDownload{
		Index:      pieceIndex,
		Length:     pieceLength,
		Blocks:     buildBlocks(pieceLength),
		Buffer:     make([]byte, pieceLength),
		Downloaded: 0,
	}

	m.pieces[pieceIndex] = pd
	return pd, nil
}

// GetPiece retrieves an in-progress piece download
func (m *Manager) GetPiece(pieceIndex int) (*PieceDownload, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pd, exists := m.pieces[pieceIndex]
	return pd, exists
}

// CompletePiece marks a piece as complete after verification
func (m *Manager) CompletePiece(pieceIndex int) error {
	m.mu.Lock()
	pd, exists := m.pieces[pieceIndex]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("piece %d not in progress", pieceIndex)
	}

	// Write to storage
	if err := m.storage.WritePiece(pieceIndex, pd.Buffer); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("failed to write piece: %w", err)
	}

	// Verify the piece
	valid, err := m.storage.VerifyPiece(pieceIndex)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("failed to verify piece: %w", err)
	}

	if !valid {
		// Hash mismatch - mark blocks as incomplete for retry
		pd.mu.Lock()
		for i := range pd.Blocks {
			pd.Blocks[i].Done = false
			pd.Blocks[i].Requested = false
		}
		pd.Downloaded = 0
		pd.mu.Unlock()
		m.mu.Unlock()
		return fmt.Errorf("piece %d hash verification failed", pieceIndex)
	}

	// Remove from in-progress map
	delete(m.pieces, pieceIndex)
	m.mu.Unlock()

	return nil
}

// FailPiece marks a piece download as failed and resets it for retry
func (m *Manager) FailPiece(pieceIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.pieces, pieceIndex)
}

// WriteBlock writes a received block to the piece buffer
func (pd *PieceDownload) WriteBlock(offset int, data []byte) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Find the block
	blockIndex := -1
	for i, block := range pd.Blocks {
		if block.Offset == offset {
			blockIndex = i
			break
		}
	}

	if blockIndex == -1 {
		return fmt.Errorf("block at offset %d not found", offset)
	}

	block := &pd.Blocks[blockIndex]

	// Check if already done
	if block.Done {
		return nil // Duplicate block, ignore
	}

	// Validate block length
	if len(data) != block.Length {
		return fmt.Errorf("block length mismatch: expected %d, got %d", block.Length, len(data))
	}

	// Copy data to buffer
	copy(pd.Buffer[offset:offset+block.Length], data)

	// Mark block as done and clear requested flag
	block.Done = true
	block.Requested = false
	pd.Downloaded += block.Length

	return nil
}

// IsComplete returns true if all blocks have been downloaded
func (pd *PieceDownload) IsComplete() bool {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	for _, block := range pd.Blocks {
		if !block.Done {
			return false
		}
	}
	return true
}

// NextBlock returns the next block that needs to be downloaded
func (pd *PieceDownload) NextBlock() (offset int, length int, found bool) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	for i := range pd.Blocks {
		block := &pd.Blocks[i]
		if !block.Done && !block.Requested {
			// Mark as requested to prevent duplicate requests
			block.Requested = true
			return block.Offset, block.Length, true
		}
	}

	return 0, 0, false
}

// MissingBlocks returns all blocks that still need to be downloaded
func (pd *PieceDownload) MissingBlocks() []Block {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	missing := make([]Block, 0)
	for _, block := range pd.Blocks {
		if !block.Done {
			missing = append(missing, block)
		}
	}
	return missing
}

// Progress returns the download progress for this piece (0.0 to 1.0)
func (pd *PieceDownload) Progress() float64 {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	if pd.Length == 0 {
		return 0.0
	}

	return float64(pd.Downloaded) / float64(pd.Length)
}

// Helper functions

// calculatePieceLength returns the length of a specific piece
func (m *Manager) calculatePieceLength(pieceIndex int) int {
	totalLength := m.info.TotalLength()
	pieceLength := m.info.PieceLength

	begin := int64(pieceIndex) * pieceLength
	end := begin + pieceLength

	if end > totalLength {
		end = totalLength
	}

	return int(end - begin)
}

// buildBlocks divides a piece into blocks
func buildBlocks(pieceLength int) []Block {
	numBlocks := (pieceLength + BlockSize - 1) / BlockSize
	blocks := make([]Block, numBlocks)

	for i := 0; i < numBlocks; i++ {
		offset := i * BlockSize
		length := BlockSize

		// Last block might be smaller
		if offset+length > pieceLength {
			length = pieceLength - offset
		}

		blocks[i] = Block{
			Offset: offset,
			Length: length,
			Done:   false,
		}
	}

	return blocks
}

// GetInProgressBytes returns total bytes downloaded for in-progress pieces
func (m *Manager) GetInProgressBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int64
	for _, pd := range m.pieces {
		pd.mu.Lock()
		total += int64(pd.Downloaded)
		pd.mu.Unlock()
	}
	return total
}

// VerifyPieceData verifies a piece's SHA-1 hash without writing to storage
func VerifyPieceData(data []byte, expectedHash [sha1.Size]byte) bool {
	actualHash := sha1.Sum(data)
	return actualHash == expectedHash
}
