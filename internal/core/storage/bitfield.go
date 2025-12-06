package storage

import "sync"

// Bitfield represents a bitfield for tracking piece completion
type Bitfield struct {
	bits []byte
	len  int
	mu   sync.RWMutex
}

// NewBitfield creates a new bitfield with the specified number of pieces
func NewBitfield(numPieces int) *Bitfield {
	numBytes := (numPieces + 7) / 8 // Round up to nearest byte
	return &Bitfield{
		bits: make([]byte, numBytes),
		len:  numPieces,
	}
}

// NewBitfieldFromBytes creates a bitfield from raw bytes
func NewBitfieldFromBytes(data []byte, numPieces int) *Bitfield {
	return &Bitfield{
		bits: data,
		len:  numPieces,
	}
}

// Has returns true if the piece at the given index is set
func (b *Bitfield) Has(index int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if index < 0 || index >= b.len {
		return false
	}

	byteIndex := index / 8
	bitIndex := uint(index % 8)

	return (b.bits[byteIndex] & (1 << (7 - bitIndex))) != 0
}

// Set marks the piece at the given index as complete
func (b *Bitfield) Set(index int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= b.len {
		return
	}

	byteIndex := index / 8
	bitIndex := uint(index % 8)

	b.bits[byteIndex] |= (1 << (7 - bitIndex))
}

// Clear marks the piece at the given index as incomplete
func (b *Bitfield) Clear(index int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= b.len {
		return
	}

	byteIndex := index / 8
	bitIndex := uint(index % 8)

	b.bits[byteIndex] &^= (1 << (7 - bitIndex))
}

// Count returns the number of pieces that are set
func (b *Bitfield) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for i := 0; i < b.len; i++ {
		if b.has(i) {
			count++
		}
	}
	return count
}

// has is the internal non-locking version of Has
func (b *Bitfield) has(index int) bool {
	if index < 0 || index >= b.len {
		return false
	}

	byteIndex := index / 8
	bitIndex := uint(index % 8)

	return (b.bits[byteIndex] & (1 << (7 - bitIndex))) != 0
}

// Bytes returns a copy of the raw bitfield bytes
func (b *Bitfield) Bytes() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]byte, len(b.bits))
	copy(result, b.bits)
	return result
}

// Len returns the number of pieces in the bitfield
func (b *Bitfield) Len() int {
	return b.len
}

// IsComplete returns true if all pieces are set
func (b *Bitfield) IsComplete() bool {
	return b.Count() == b.len
}

// CompletedPieces returns a slice of indices for all completed pieces
func (b *Bitfield) CompletedPieces() []int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]int, 0)
	for i := 0; i < b.len; i++ {
		if b.has(i) {
			result = append(result, i)
		}
	}
	return result
}

// MissingPieces returns a slice of indices for all missing pieces
func (b *Bitfield) MissingPieces() []int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]int, 0)
	for i := 0; i < b.len; i++ {
		if !b.has(i) {
			result = append(result, i)
		}
	}
	return result
}

// GetCompletedIndices returns a slice of indices for all completed pieces (alias for CompletedPieces)
func (b *Bitfield) GetCompletedIndices() []int {
	return b.CompletedPieces()
}

// GetBytes returns a copy of the raw bitfield bytes (alias for Bytes)
func (b *Bitfield) GetBytes() []byte {
	return b.Bytes()
}

// SetBytes sets the bitfield from raw bytes
func (b *Bitfield) SetBytes(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Copy data to bitfield
	copy(b.bits, data)

	// Ensure we don't have more bytes than needed
	if len(data) < len(b.bits) {
		// Pad with zeros
		for i := len(data); i < len(b.bits); i++ {
			b.bits[i] = 0
		}
	}
}
