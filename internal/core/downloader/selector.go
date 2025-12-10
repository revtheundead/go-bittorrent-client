package downloader

import (
	"math/rand"
	"time"

	"github.com/revtheundead/revtorrent/internal/core/storage"
)

// PieceSelector defines the strategy for selecting which piece to download next
type PieceSelector interface {
	// SelectPiece chooses the next piece for a peer to download
	// Returns -1 if no suitable piece is found
	SelectPiece(peerBitfield *storage.Bitfield, ownBitfield *storage.Bitfield, inProgress map[int]bool) int
}

// RandomFirstSelector implements random piece selection
// Good for avoiding strict seeder problems and simple to implement
type RandomFirstSelector struct {
	rand *rand.Rand
}

// NewRandomFirstSelector creates a new random-first selector
func NewRandomFirstSelector() *RandomFirstSelector {
	return &RandomFirstSelector{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SelectPiece implements PieceSelector
func (s *RandomFirstSelector) SelectPiece(
	peerBitfield *storage.Bitfield,
	ownBitfield *storage.Bitfield,
	inProgress map[int]bool,
) int {
	// Find pieces the peer has that we need
	candidates := []int{}

	numPieces := ownBitfield.Len()
	for i := 0; i < numPieces; i++ {
		if !ownBitfield.Has(i) && // We don't have it
			peerBitfield.Has(i) && // Peer has it
			!inProgress[i] { // Not currently downloading
			candidates = append(candidates, i)
		}
	}

	// Return random piece from candidates
	if len(candidates) == 0 {
		return -1 // No suitable piece found
	}

	return candidates[s.rand.Intn(len(candidates))]
}

// SequentialSelector implements sequential piece selection
// Downloads pieces in order from 0 to N
type SequentialSelector struct {
	nextPiece int
}

// NewSequentialSelector creates a new sequential selector
func NewSequentialSelector() *SequentialSelector {
	return &SequentialSelector{
		nextPiece: 0,
	}
}

// SelectPiece implements PieceSelector
func (s *SequentialSelector) SelectPiece(
	peerBitfield *storage.Bitfield,
	ownBitfield *storage.Bitfield,
	inProgress map[int]bool,
) int {
	numPieces := ownBitfield.Len()

	// Start from where we left off
	for i := s.nextPiece; i < numPieces; i++ {
		if !ownBitfield.Has(i) && // We don't have it
			peerBitfield.Has(i) && // Peer has it
			!inProgress[i] { // Not currently downloading
			s.nextPiece = i + 1
			return i
		}
	}

	// Wrap around if nothing found from nextPiece onwards
	for i := 0; i < s.nextPiece && i < numPieces; i++ {
		if !ownBitfield.Has(i) && // We don't have it
			peerBitfield.Has(i) && // Peer has it
			!inProgress[i] { // Not currently downloading
			s.nextPiece = i + 1
			return i
		}
	}

	return -1 // No suitable piece found
}

// RarestFirstSelector implements rarest-first piece selection
// Prioritizes downloading pieces that are rarest among connected peers
type RarestFirstSelector struct {
	rand         *rand.Rand
	availability map[int]int // piece availability counts (injected by coordinator)
}

// NewRarestFirstSelector creates a new rarest-first selector
func NewRarestFirstSelector() *RarestFirstSelector {
	return &RarestFirstSelector{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		availability: make(map[int]int),
	}
}

// SetAvailability updates the piece availability counts
func (s *RarestFirstSelector) SetAvailability(availability map[int]int) {
	s.availability = availability
}

// SelectPiece implements PieceSelector with rarest-first strategy
func (s *RarestFirstSelector) SelectPiece(
	peerBitfield *storage.Bitfield,
	ownBitfield *storage.Bitfield,
	inProgress map[int]bool,
) int {
	// Find pieces the peer has that we need
	candidates := []int{}

	numPieces := ownBitfield.Len()
	for i := 0; i < numPieces; i++ {
		if !ownBitfield.Has(i) && // We don't have it
			peerBitfield.Has(i) && // Peer has it
			!inProgress[i] { // Not currently downloading
			candidates = append(candidates, i)
		}
	}

	if len(candidates) == 0 {
		return -1 // No suitable piece found
	}

	// If we have availability data, select rarest piece
	if len(s.availability) > 0 {
		// Find minimum availability among candidates
		minAvail := -1
		rarest := []int{}

		for _, piece := range candidates {
			avail := s.availability[piece]
			if avail == 0 {
				avail = 1 // Assume at least 1 (current peer)
			}

			if minAvail == -1 || avail < minAvail {
				minAvail = avail
				rarest = []int{piece}
			} else if avail == minAvail {
				rarest = append(rarest, piece)
			}
		}

		// Return random piece from rarest
		if len(rarest) > 0 {
			return rarest[s.rand.Intn(len(rarest))]
		}
	}

	// Fallback to random selection
	return candidates[s.rand.Intn(len(candidates))]
}
