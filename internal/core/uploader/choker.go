package uploader

import (
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	// Number of peers to unchoke for regular uploads
	NumUnchoked = 4

	// Interval for regular unchoke algorithm (10 seconds)
	UnchokeInterval = 10 * time.Second

	// Interval for optimistic unchoke (30 seconds)
	OptimisticUnchokeInterval = 30 * time.Second
)

// Choker implements the BitTorrent choking algorithm
type Choker struct {
	peers              []*UploadPeer
	mu                 sync.RWMutex
	logger             *slog.Logger
	lastOptimisticPeer *UploadPeer
}

// NewChoker creates a new choking algorithm manager
func NewChoker(logger *slog.Logger) *Choker {
	return &Choker{
		peers:  make([]*UploadPeer, 0),
		logger: logger,
	}
}

// AddPeer adds a peer to the choker
func (c *Choker) AddPeer(peer *UploadPeer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peers = append(c.peers, peer)
}

// RemovePeer removes a peer from the choker
func (c *Choker) RemovePeer(peer *UploadPeer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, p := range c.peers {
		if p == peer {
			c.peers = append(c.peers[:i], c.peers[i+1:]...)
			break
		}
	}

	// Clear optimistic peer if it was removed
	if c.lastOptimisticPeer == peer {
		c.lastOptimisticPeer = nil
	}
}

// Run runs the choking algorithm loop
func (c *Choker) Run(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	unchokeTicker := time.NewTicker(UnchokeInterval)
	defer unchokeTicker.Stop()

	optimisticTicker := time.NewTicker(OptimisticUnchokeInterval)
	defer optimisticTicker.Stop()

	// Initial unchoke
	c.regularUnchoke()

	for {
		select {
		case <-stopCh:
			return

		case <-unchokeTicker.C:
			c.regularUnchoke()

		case <-optimisticTicker.C:
			c.optimisticUnchoke()
		}
	}
}

// regularUnchoke performs the regular unchoke algorithm
// Unchokes the top N peers by upload rate
func (c *Choker) regularUnchoke() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.peers) == 0 {
		return
	}

	// Sort peers by upload rate (descending)
	sortedPeers := make([]*UploadPeer, len(c.peers))
	copy(sortedPeers, c.peers)

	sort.Slice(sortedPeers, func(i, j int) bool {
		return sortedPeers[i].UploadRate() > sortedPeers[j].UploadRate()
	})

	// Keep track of which peers should be unchoked
	unchokeSet := make(map[*UploadPeer]bool)

	// Unchoke top N interested peers
	unchoked := 0
	for _, peer := range sortedPeers {
		if !peer.IsInterested() {
			continue
		}

		if unchoked < NumUnchoked {
			unchokeSet[peer] = true
			unchoked++
		}
	}

	// Always keep optimistic peer unchoked if it exists
	if c.lastOptimisticPeer != nil && c.lastOptimisticPeer.IsInterested() {
		unchokeSet[c.lastOptimisticPeer] = true
	}

	// Apply choking/unchoking
	for _, peer := range c.peers {
		if unchokeSet[peer] {
			if peer.IsChoked() {
				if err := peer.Unchoke(); err != nil {
					c.logger.Warn("failed to unchoke peer", "addr", peer.RemoteAddr(), "error", err)
				} else {
					c.logger.Debug("unchoked peer", "addr", peer.RemoteAddr())
				}
			}
		} else {
			if !peer.IsChoked() {
				if err := peer.Choke(); err != nil {
					c.logger.Warn("failed to choke peer", "addr", peer.RemoteAddr(), "error", err)
				} else {
					c.logger.Debug("choked peer", "addr", peer.RemoteAddr())
				}
			}
		}
	}
}

// optimisticUnchoke performs optimistic unchoking
// Randomly unchokes one choked, interested peer
func (c *Choker) optimisticUnchoke() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.peers) == 0 {
		return
	}

	// Find all choked, interested peers
	chokedInterested := make([]*UploadPeer, 0)
	for _, peer := range c.peers {
		if peer.IsChoked() && peer.IsInterested() {
			chokedInterested = append(chokedInterested, peer)
		}
	}

	if len(chokedInterested) == 0 {
		return
	}

	// Randomly select one
	selected := chokedInterested[rand.Intn(len(chokedInterested))]

	// Choke the previous optimistic peer if it's not in the regular unchoke set
	if c.lastOptimisticPeer != nil && c.lastOptimisticPeer != selected {
		// Only choke if it has low upload rate (not in top performers)
		if c.lastOptimisticPeer.UploadRate() < c.getMinTopRate() {
			if err := c.lastOptimisticPeer.Choke(); err != nil {
				c.logger.Warn("failed to choke previous optimistic peer",
					"addr", c.lastOptimisticPeer.RemoteAddr(), "error", err)
			}
		}
	}

	// Unchoke the new optimistic peer
	if err := selected.Unchoke(); err != nil {
		c.logger.Warn("failed to optimistically unchoke peer",
			"addr", selected.RemoteAddr(), "error", err)
		return
	}

	c.lastOptimisticPeer = selected
	c.logger.Info("optimistically unchoked peer", "addr", selected.RemoteAddr())
}

// getMinTopRate returns the minimum upload rate among the top N peers
func (c *Choker) getMinTopRate() float64 {
	// Sort peers by upload rate
	sortedPeers := make([]*UploadPeer, len(c.peers))
	copy(sortedPeers, c.peers)

	sort.Slice(sortedPeers, func(i, j int) bool {
		return sortedPeers[i].UploadRate() > sortedPeers[j].UploadRate()
	})

	// Get the Nth peer's rate
	if len(sortedPeers) >= NumUnchoked {
		return sortedPeers[NumUnchoked-1].UploadRate()
	}

	// If we have fewer peers, return 0
	return 0
}

// UpdatePeerRates updates upload rates for all peers
// This should be called periodically to calculate rates based on uploaded bytes
func (c *Choker) UpdatePeerRates(interval time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, peer := range c.peers {
		// Calculate rate: uploaded bytes / time interval
		uploaded := peer.Uploaded()
		rate := float64(uploaded) / interval.Seconds()
		peer.UpdateUploadRate(rate)

		// Reset uploaded counter for next interval
		// Note: This is simplified - a real implementation would track
		// uploaded bytes over a sliding window
	}
}
