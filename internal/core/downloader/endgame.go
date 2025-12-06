package downloader

import (
	"log/slog"
	"sync"
	"time"
)

const (
	// EndgameThreshold is the percentage of completion at which to enter endgame mode
	EndgameThreshold = 0.98 // 98%

	// EndgamePieceThreshold is the minimum number of pieces remaining to enter endgame
	EndgamePieceThreshold = 10

	// EndgameRequestDuplication is how many peers to request the same piece from
	EndgameRequestDuplication = 3
)

// EndgameManager manages endgame mode for fast completion
type EndgameManager struct {
	enabled         bool
	active          bool
	remainingPieces map[int]bool          // Set of pieces we still need
	pieceRequests   map[int][]string      // Map of piece index to peer IDs we've requested from
	cancelNotify    map[string]chan<- int // Map of peer ID to cancel notification channel
	logger          *slog.Logger
	mu              sync.RWMutex
}

// NewEndgameManager creates a new endgame manager
func NewEndgameManager(logger *slog.Logger) *EndgameManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &EndgameManager{
		enabled:         true,
		active:          false,
		remainingPieces: make(map[int]bool),
		pieceRequests:   make(map[int][]string),
		cancelNotify:    make(map[string]chan<- int),
		logger:          logger,
	}
}

// ShouldEnterEndgame checks if we should enter endgame mode
func (em *EndgameManager) ShouldEnterEndgame(totalPieces, completedPieces int) bool {
	if !em.enabled {
		return false
	}

	remaining := totalPieces - completedPieces
	progress := float64(completedPieces) / float64(totalPieces)

	// Enter endgame if:
	// 1. We're above the progress threshold (98%)
	// 2. OR we have fewer than threshold pieces remaining
	return progress >= EndgameThreshold || remaining <= EndgamePieceThreshold
}

// EnterEndgame activates endgame mode
func (em *EndgameManager) EnterEndgame(remainingPieceIndices []int) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.active {
		return
	}

	em.active = true
	em.remainingPieces = make(map[int]bool)
	em.pieceRequests = make(map[int][]string)

	for _, index := range remainingPieceIndices {
		em.remainingPieces[index] = true
	}

	em.logger.Info("entering endgame mode", "remaining_pieces", len(remainingPieceIndices))
}

// ExitEndgame deactivates endgame mode
func (em *EndgameManager) ExitEndgame() {
	em.mu.Lock()
	defer em.mu.Unlock()

	if !em.active {
		return
	}

	em.active = false
	em.remainingPieces = make(map[int]bool)
	em.pieceRequests = make(map[int][]string)

	em.logger.Info("exiting endgame mode")
}

// IsActive returns whether endgame mode is currently active
func (em *EndgameManager) IsActive() bool {
	em.mu.RLock()
	defer em.mu.RUnlock()

	return em.active
}

// RegisterPeerChannel registers a channel for sending cancel messages to a peer
func (em *EndgameManager) RegisterPeerChannel(peerID string, cancelCh chan<- int) {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.cancelNotify[peerID] = cancelCh
}

// UnregisterPeerChannel removes a peer's cancel channel
func (em *EndgameManager) UnregisterPeerChannel(peerID string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	delete(em.cancelNotify, peerID)
}

// RequestPiece records that we've requested a piece from a peer
func (em *EndgameManager) RequestPiece(pieceIndex int, peerID string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if !em.active {
		return
	}

	if !em.remainingPieces[pieceIndex] {
		return
	}

	// Add this peer to the list of peers we've requested this piece from
	if em.pieceRequests[pieceIndex] == nil {
		em.pieceRequests[pieceIndex] = make([]string, 0)
	}

	// Check if we've already requested from this peer
	for _, pid := range em.pieceRequests[pieceIndex] {
		if pid == peerID {
			return
		}
	}

	em.pieceRequests[pieceIndex] = append(em.pieceRequests[pieceIndex], peerID)

	em.logger.Debug("endgame piece requested",
		"piece", pieceIndex,
		"peer", peerID,
		"duplicate_count", len(em.pieceRequests[pieceIndex]))
}

// CompletePiece marks a piece as complete and sends cancel messages
func (em *EndgameManager) CompletePiece(pieceIndex int) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if !em.active {
		return
	}

	// Remove from remaining pieces
	delete(em.remainingPieces, pieceIndex)

	// Send cancel messages to all peers we requested this piece from
	if peerIDs, ok := em.pieceRequests[pieceIndex]; ok {
		for _, peerID := range peerIDs {
			if cancelCh, exists := em.cancelNotify[peerID]; exists {
				// Non-blocking send
				select {
				case cancelCh <- pieceIndex:
					em.logger.Debug("sent cancel message",
						"piece", pieceIndex,
						"peer", peerID)
				default:
					// Channel full or closed, skip
				}
			}
		}

		// Clear the request list
		delete(em.pieceRequests, pieceIndex)
	}

	em.logger.Debug("endgame piece completed",
		"piece", pieceIndex,
		"remaining", len(em.remainingPieces))

	// Exit endgame if no pieces remain
	if len(em.remainingPieces) == 0 {
		em.active = false
		em.logger.Info("all pieces complete, exiting endgame mode")
	}
}

// ShouldRequestPiece returns whether we should request a piece from a peer in endgame
func (em *EndgameManager) ShouldRequestPiece(pieceIndex int, peerID string) bool {
	em.mu.RLock()
	defer em.mu.RUnlock()

	if !em.active {
		return false
	}

	// Check if we still need this piece
	if !em.remainingPieces[pieceIndex] {
		return false
	}

	// Check how many peers we've already requested from
	requestCount := len(em.pieceRequests[pieceIndex])

	// Request from multiple peers but not too many
	if requestCount >= EndgameRequestDuplication {
		// Check if this peer already has a request
		for _, pid := range em.pieceRequests[pieceIndex] {
			if pid == peerID {
				return false // Already requested from this peer
			}
		}
		return false // Already requested from enough peers
	}

	return true
}

// GetRemainingPieces returns a list of pieces we still need
func (em *EndgameManager) GetRemainingPieces() []int {
	em.mu.RLock()
	defer em.mu.RUnlock()

	remaining := make([]int, 0, len(em.remainingPieces))
	for pieceIndex := range em.remainingPieces {
		remaining = append(remaining, pieceIndex)
	}

	return remaining
}

// GetRequestCount returns how many peers we've requested a piece from
func (em *EndgameManager) GetRequestCount(pieceIndex int) int {
	em.mu.RLock()
	defer em.mu.RUnlock()

	return len(em.pieceRequests[pieceIndex])
}

// Enable enables or disables endgame mode
func (em *EndgameManager) Enable(enabled bool) {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.enabled = enabled

	if !enabled && em.active {
		em.active = false
		em.logger.Info("endgame mode disabled")
	}
}

// GetStats returns current endgame statistics
func (em *EndgameManager) GetStats() EndgameStats {
	em.mu.RLock()
	defer em.mu.RUnlock()

	stats := EndgameStats{
		Active:          em.active,
		RemainingPieces: len(em.remainingPieces),
		TotalRequests:   0,
	}

	for _, peers := range em.pieceRequests {
		stats.TotalRequests += len(peers)
	}

	return stats
}

// EndgameStats holds endgame mode statistics
type EndgameStats struct {
	Active          bool
	RemainingPieces int
	TotalRequests   int
}

// MonitorProgress periodically checks if we should enter/exit endgame mode
func (em *EndgameManager) MonitorProgress(
	totalPieces int,
	getCompletedPieces func() int,
	getRemainingIndices func() []int,
	stopCh <-chan struct{},
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			completed := getCompletedPieces()

			if !em.IsActive() {
				// Check if we should enter
				if em.ShouldEnterEndgame(totalPieces, completed) {
					remaining := getRemainingIndices()
					em.EnterEndgame(remaining)
				}
			} else {
				// Check if we should exit (all complete)
				if completed == totalPieces {
					em.ExitEndgame()
				}
			}
		}
	}
}

// Reset resets the endgame manager state
func (em *EndgameManager) Reset() {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.active = false
	em.remainingPieces = make(map[int]bool)
	em.pieceRequests = make(map[int][]string)
	em.cancelNotify = make(map[string]chan<- int)
}
