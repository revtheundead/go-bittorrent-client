package downloader

import (
	"math/rand"
	"sort"
	"sync"
)

// PieceStrategy defines an interface for piece selection strategies
type PieceStrategy interface {
	// SelectPiece returns the next piece to download from a peer
	SelectPiece(peerBitfield []byte, havePieces []bool) (int, bool)
	// UpdateAvailability updates piece availability when receiving bitfield/have
	UpdateAvailability(pieceIndex int, increment bool)
	// Reset resets the strategy state
	Reset()
}

// RarestFirstStrategy implements rarest-first piece selection
type RarestFirstStrategy struct {
	numPieces    int
	availability []int // How many peers have each piece
	endgameMode  bool
	mu           sync.RWMutex
}

// NewRarestFirstStrategy creates a new rarest-first strategy
func NewRarestFirstStrategy(numPieces int) *RarestFirstStrategy {
	return &RarestFirstStrategy{
		numPieces:    numPieces,
		availability: make([]int, numPieces),
		endgameMode:  false,
	}
}

// SelectPiece selects the rarest piece that the peer has and we don't
func (s *RarestFirstStrategy) SelectPiece(peerBitfield []byte, havePieces []bool) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build list of available pieces (peer has, we don't)
	type pieceWithRarity struct {
		index  int
		rarity int
	}

	available := make([]pieceWithRarity, 0)

	for i := 0; i < s.numPieces; i++ {
		// Check if we already have this piece
		if havePieces[i] {
			continue
		}

		// Check if peer has this piece
		if !hasPiece(peerBitfield, i) {
			continue
		}

		available = append(available, pieceWithRarity{
			index:  i,
			rarity: s.availability[i],
		})
	}

	if len(available) == 0 {
		return -1, false
	}

	// Sort by rarity (ascending - rarest first)
	sort.Slice(available, func(i, j int) bool {
		// If rarity is the same, randomize to avoid all peers
		// requesting the same piece
		if available[i].rarity == available[j].rarity {
			return rand.Intn(2) == 0
		}
		return available[i].rarity < available[j].rarity
	})

	// Return the rarest piece
	return available[0].index, true
}

// UpdateAvailability updates the availability count for a piece
func (s *RarestFirstStrategy) UpdateAvailability(pieceIndex int, increment bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pieceIndex < 0 || pieceIndex >= s.numPieces {
		return
	}

	if increment {
		s.availability[pieceIndex]++
	} else {
		if s.availability[pieceIndex] > 0 {
			s.availability[pieceIndex]--
		}
	}
}

// UpdateBitfield updates availability for all pieces in a bitfield
func (s *RarestFirstStrategy) UpdateBitfield(bitfield []byte, increment bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < s.numPieces; i++ {
		if hasPiece(bitfield, i) {
			if increment {
				s.availability[i]++
			} else {
				if s.availability[i] > 0 {
					s.availability[i]--
				}
			}
		}
	}
}

// EnableEndgameMode enables endgame mode for final pieces
func (s *RarestFirstStrategy) EnableEndgameMode() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endgameMode = true
}

// IsEndgameMode returns whether endgame mode is active
func (s *RarestFirstStrategy) IsEndgameMode() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.endgameMode
}

// Reset resets the strategy state
func (s *RarestFirstStrategy) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.availability {
		s.availability[i] = 0
	}
	s.endgameMode = false
}

// GetRarity returns the rarity (availability) of a specific piece
func (s *RarestFirstStrategy) GetRarity(pieceIndex int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if pieceIndex < 0 || pieceIndex >= s.numPieces {
		return 0
	}

	return s.availability[pieceIndex]
}

// SequentialStrategy implements sequential piece selection (for streaming)
type SequentialStrategy struct {
	numPieces int
	mu        sync.RWMutex
}

// NewSequentialStrategy creates a new sequential strategy
func NewSequentialStrategy(numPieces int) *SequentialStrategy {
	return &SequentialStrategy{
		numPieces: numPieces,
	}
}

// SelectPiece selects the next piece sequentially
func (s *SequentialStrategy) SelectPiece(peerBitfield []byte, havePieces []bool) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := 0; i < s.numPieces; i++ {
		if !havePieces[i] && hasPiece(peerBitfield, i) {
			return i, true
		}
	}

	return -1, false
}

// UpdateAvailability is a no-op for sequential strategy
func (s *SequentialStrategy) UpdateAvailability(pieceIndex int, increment bool) {
	// Not needed for sequential strategy
}

// Reset resets the strategy state
func (s *SequentialStrategy) Reset() {
	// Nothing to reset for sequential
}

// RandomStrategy implements random piece selection
type RandomStrategy struct {
	numPieces int
	mu        sync.RWMutex
}

// NewRandomStrategy creates a new random strategy
func NewRandomStrategy(numPieces int) *RandomStrategy {
	return &RandomStrategy{
		numPieces: numPieces,
	}
}

// SelectPiece selects a random piece
func (s *RandomStrategy) SelectPiece(peerBitfield []byte, havePieces []bool) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build list of available pieces
	available := make([]int, 0)
	for i := 0; i < s.numPieces; i++ {
		if !havePieces[i] && hasPiece(peerBitfield, i) {
			available = append(available, i)
		}
	}

	if len(available) == 0 {
		return -1, false
	}

	// Pick random piece
	return available[rand.Intn(len(available))], true
}

// UpdateAvailability is a no-op for random strategy
func (s *RandomStrategy) UpdateAvailability(pieceIndex int, increment bool) {
	// Not needed for random strategy
}

// Reset resets the strategy state
func (s *RandomStrategy) Reset() {
	// Nothing to reset for random
}

// hasPiece checks if a bitfield has a specific piece
func hasPiece(bitfield []byte, index int) bool {
	byteIndex := index / 8
	offset := index % 8

	if byteIndex >= len(bitfield) {
		return false
	}

	return (bitfield[byteIndex] & (0x80 >> offset)) != 0
}
