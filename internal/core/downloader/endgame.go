package downloader

import (
	"fmt"
	"sync"
)

// BlockRequest tracks a block being requested in endgame mode
type EndgameBlockRequest struct {
	PieceIndex int
	Offset     int
	Length     int
}

// EndgameTracker tracks duplicate block requests in endgame mode
type EndgameTracker struct {
	// Maps block key (pieceIndex:offset) to set of peer addrs requesting it
	blockRequests map[string]map[string]bool
	mu            sync.RWMutex
}

// NewEndgameTracker creates a new endgame tracker
func NewEndgameTracker() *EndgameTracker {
	return &EndgameTracker{
		blockRequests: make(map[string]map[string]bool),
	}
}

// AddRequest registers that a peer is requesting a block
func (et *EndgameTracker) AddRequest(peerAddr string, pieceIndex, offset int) {
	et.mu.Lock()
	defer et.mu.Unlock()

	key := blockKey(pieceIndex, offset)
	if et.blockRequests[key] == nil {
		et.blockRequests[key] = make(map[string]bool)
	}
	et.blockRequests[key][peerAddr] = true
}

// RemoveRequest removes a peer's request for a block
func (et *EndgameTracker) RemoveRequest(peerAddr string, pieceIndex, offset int) {
	et.mu.Lock()
	defer et.mu.Unlock()

	key := blockKey(pieceIndex, offset)
	if peers, exists := et.blockRequests[key]; exists {
		delete(peers, peerAddr)
		if len(peers) == 0 {
			delete(et.blockRequests, key)
		}
	}
}

// GetPeersForBlock returns all peers requesting a specific block (excluding the given peer)
func (et *EndgameTracker) GetPeersForBlock(excludePeer string, pieceIndex, offset int) []string {
	et.mu.RLock()
	defer et.mu.RUnlock()

	key := blockKey(pieceIndex, offset)
	peers, exists := et.blockRequests[key]
	if !exists {
		return nil
	}

	result := make([]string, 0, len(peers))
	for addr := range peers {
		if addr != excludePeer {
			result = append(result, addr)
		}
	}
	return result
}

// RemovePeer removes all requests from a peer (called when peer disconnects)
func (et *EndgameTracker) RemovePeer(peerAddr string) {
	et.mu.Lock()
	defer et.mu.Unlock()

	for key, peers := range et.blockRequests {
		delete(peers, peerAddr)
		if len(peers) == 0 {
			delete(et.blockRequests, key)
		}
	}
}

// blockKey creates a unique key for a block
func blockKey(pieceIndex, offset int) string {
	return fmt.Sprintf("%d:%d", pieceIndex, offset)
}
