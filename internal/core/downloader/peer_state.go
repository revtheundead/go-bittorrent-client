package downloader

import (
	"sync"

	"github.com/revtheundead/revtorrent/internal/core/storage"
)

// PeerState tracks the state of a peer connection for downloading
type PeerState struct {
	Addr string

	// Peer's pieces
	Bitfield *storage.Bitfield

	// Connection state
	AmChoking      bool // We choke them (doesn't affect download)
	AmInterested   bool // We're interested in their pieces
	PeerChoking    bool // They choke us (blocks download if true)
	PeerInterested bool // They're interested in our pieces

	// Download state
	CurrentPiece *int          // Currently downloading piece (nil if idle)
	RequestQueue *RequestQueue // Active request queue

	// Statistics
	Downloaded   int64   // Total bytes downloaded from this peer
	DownloadRate float64 // Current download rate (bytes/sec)

	mu sync.RWMutex
}

// NewPeerState creates a new peer state
func NewPeerState(addr string, numPieces int) *PeerState {
	return &PeerState{
		Addr:           addr,
		Bitfield:       storage.NewBitfield(numPieces),
		AmChoking:      true, // Start choked
		AmInterested:   false,
		PeerChoking:    true, // Assume peer chokes us initially
		PeerInterested: false,
		CurrentPiece:   nil,
		RequestQueue:   nil,
		Downloaded:     0,
		DownloadRate:   0,
	}
}

// SetAmChoking sets whether we are choking the peer
func (ps *PeerState) SetAmChoking(choking bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.AmChoking = choking
}

// SetAmInterested sets whether we are interested in the peer
func (ps *PeerState) SetAmInterested(interested bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.AmInterested = interested
}

// SetPeerChoking sets whether the peer is choking us
func (ps *PeerState) SetPeerChoking(choking bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.PeerChoking = choking
}

// SetPeerInterested sets whether the peer is interested in us
func (ps *PeerState) SetPeerInterested(interested bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.PeerInterested = interested
}

// IsPeerChoking returns whether the peer is choking us
func (ps *PeerState) IsPeerChoking() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.PeerChoking
}

// IsIdle returns true if the peer is not currently downloading a piece
func (ps *PeerState) IsIdle() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.CurrentPiece == nil
}

// AddDownloaded adds to the downloaded byte count
func (ps *PeerState) AddDownloaded(bytes int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.Downloaded += bytes
}

// GetDownloaded returns the total bytes downloaded
func (ps *PeerState) GetDownloaded() int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.Downloaded
}

// UpdateDownloadRate updates the download rate calculation
func (ps *PeerState) UpdateDownloadRate(rate float64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.DownloadRate = rate
}

// GetBitfield returns a copy of the peer's bitfield
func (ps *PeerState) GetBitfield() *storage.Bitfield {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.Bitfield
}
