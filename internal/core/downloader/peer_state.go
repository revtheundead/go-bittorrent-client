package downloader

import (
	"sync"
	"time"

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
	Downloaded       int64     // Total bytes downloaded from this peer
	DownloadRate     float64   // Current download rate (bytes/sec)
	LastRateUpdate   time.Time // Last time download rate was calculated
	LastDownloaded   int64     // Downloaded bytes at last rate update
	SlowPeerDuration time.Duration // How long peer has been slow

	// Reliability tracking
	HashFailures int // Number of hash verification failures

	mu sync.RWMutex
}

// NewPeerState creates a new peer state
func NewPeerState(addr string, numPieces int) *PeerState {
	now := time.Now()
	return &PeerState{
		Addr:             addr,
		Bitfield:         storage.NewBitfield(numPieces),
		AmChoking:        true, // Start choked
		AmInterested:     false,
		PeerChoking:      true, // Assume peer chokes us initially
		PeerInterested:   false,
		CurrentPiece:     nil,
		RequestQueue:     nil,
		Downloaded:       0,
		DownloadRate:     0,
		LastRateUpdate:   now,
		LastDownloaded:   0,
		SlowPeerDuration: 0,
		HashFailures:     0,
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

// IncrementHashFailures increments the hash failure counter
func (ps *PeerState) IncrementHashFailures() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.HashFailures++
}

// GetHashFailures returns the number of hash failures
func (ps *PeerState) GetHashFailures() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.HashFailures
}

// ShouldBanPeer returns true if the peer has too many hash failures
func (ps *PeerState) ShouldBanPeer() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	// Ban after 3 hash verification failures
	return ps.HashFailures >= 3
}

// UpdateRate calculates and updates the download rate
func (ps *PeerState) UpdateRate() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(ps.LastRateUpdate).Seconds()

	// Update every 5 seconds minimum
	if elapsed < 5.0 {
		return
	}

	bytesDownloaded := ps.Downloaded - ps.LastDownloaded
	ps.DownloadRate = float64(bytesDownloaded) / elapsed

	// Track slow peer duration
	const slowThreshold = 5 * 1024 // 5 KB/s
	if ps.DownloadRate < slowThreshold && ps.Downloaded > 0 {
		ps.SlowPeerDuration += time.Duration(elapsed) * time.Second
	} else {
		ps.SlowPeerDuration = 0 // Reset if speed improves
	}

	ps.LastRateUpdate = now
	ps.LastDownloaded = ps.Downloaded
}

// IsTooSlow returns true if peer has been consistently slow
func (ps *PeerState) IsTooSlow() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	// Disconnect if slow for more than 30 seconds
	return ps.SlowPeerDuration > 30*time.Second
}

// GetDownloadRate returns the current download rate
func (ps *PeerState) GetDownloadRate() float64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.DownloadRate
}
