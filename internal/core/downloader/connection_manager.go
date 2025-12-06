package downloader

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ConnectionManager manages peer connections and their lifecycle
type ConnectionManager struct {
	selector           *PeerSelector
	activePeers        map[string]*PeerConnection
	candidatePeers     []PeerInfo
	logger             *slog.Logger
	mu                 sync.RWMutex
	explorationRate    time.Duration
	replacementEnabled bool
}

// PeerInfo holds information about a potential peer
type PeerInfo struct {
	IP   string
	Port uint16
	ID   string
}

// PeerConnection represents an active peer connection
type PeerConnection struct {
	Info      PeerInfo
	Connected bool
	LastSeen  time.Time
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(maxPeers, minPeers int, logger *slog.Logger) *ConnectionManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &ConnectionManager{
		selector:           NewPeerSelector(maxPeers, minPeers),
		activePeers:        make(map[string]*PeerConnection),
		candidatePeers:     make([]PeerInfo, 0),
		logger:             logger,
		explorationRate:    60 * time.Second, // Try new peers every 60 seconds
		replacementEnabled: true,
	}
}

// AddCandidatePeer adds a potential peer to connect to
func (cm *ConnectionManager) AddCandidatePeer(peer PeerInfo) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Don't add if already active
	if _, exists := cm.activePeers[peer.ID]; exists {
		return
	}

	// Don't add duplicates
	for _, candidate := range cm.candidatePeers {
		if candidate.ID == peer.ID {
			return
		}
	}

	cm.candidatePeers = append(cm.candidatePeers, peer)
}

// AddActivePeer marks a peer as actively connected
func (cm *ConnectionManager) AddActivePeer(peer PeerInfo) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.activePeers[peer.ID] = &PeerConnection{
		Info:      peer,
		Connected: true,
		LastSeen:  time.Now(),
	}

	cm.selector.AddPeer(peer.ID)

	// Remove from candidates if present
	cm.removeCandidateLocked(peer.ID)
}

// RemoveActivePeer removes a peer from active connections
func (cm *ConnectionManager) RemoveActivePeer(peerID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.activePeers, peerID)
	cm.selector.RemovePeer(peerID)
}

// removeCandidateLocked removes a candidate peer (must be called with lock held)
func (cm *ConnectionManager) removeCandidateLocked(peerID string) {
	for i, candidate := range cm.candidatePeers {
		if candidate.ID == peerID {
			cm.candidatePeers = append(cm.candidatePeers[:i], cm.candidatePeers[i+1:]...)
			return
		}
	}
}

// GetNextPeerToConnect returns the next peer we should try to connect to
func (cm *ConnectionManager) GetNextPeerToConnect() (PeerInfo, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if we can accept more peers
	if !cm.selector.CanAcceptNewPeer() {
		return PeerInfo{}, false
	}

	// Try candidates
	if len(cm.candidatePeers) > 0 {
		peer := cm.candidatePeers[0]
		cm.candidatePeers = cm.candidatePeers[1:]
		return peer, true
	}

	return PeerInfo{}, false
}

// GetPeerToReplace returns a poorly performing peer that should be replaced
func (cm *ConnectionManager) GetPeerToReplace() (string, bool) {
	if !cm.replacementEnabled {
		return "", false
	}

	return cm.selector.ShouldReplacePeer()
}

// UpdatePeerMetrics updates metrics for a peer
func (cm *ConnectionManager) UpdatePeerMetrics(peerID string, downloaded, uploaded int64) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if peer, exists := cm.activePeers[peerID]; exists {
		peer.LastSeen = time.Now()
	}

	if downloaded > 0 {
		cm.selector.UpdateDownload(peerID, downloaded)
	}

	if uploaded > 0 {
		cm.selector.UpdateUpload(peerID, uploaded)
	}
}

// RecordPeerFailure records a failure for a peer
func (cm *ConnectionManager) RecordPeerFailure(peerID string) {
	cm.selector.RecordFailure(peerID)
}

// GetActivePeerCount returns the number of active peers
func (cm *ConnectionManager) GetActivePeerCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return len(cm.activePeers)
}

// GetCandidatePeerCount returns the number of candidate peers
func (cm *ConnectionManager) GetCandidatePeerCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return len(cm.candidatePeers)
}

// GetTopPeers returns the top performing peers
func (cm *ConnectionManager) GetTopPeers(n int) []string {
	return cm.selector.GetTopPeers(n)
}

// StartPeriodicMaintenance starts periodic maintenance tasks
func (cm *ConnectionManager) StartPeriodicMaintenance(stopCh <-chan struct{}) {
	ticker := time.NewTicker(cm.explorationRate)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			cm.performMaintenance()
		}
	}
}

// performMaintenance performs periodic maintenance tasks
func (cm *ConnectionManager) performMaintenance() {
	// Check for peers to replace
	if peerID, shouldReplace := cm.GetPeerToReplace(); shouldReplace {
		cm.logger.Debug("peer marked for replacement", "peer_id", peerID)
		cm.RemoveActivePeer(peerID)
	}

	// Try to connect to new peers if we have capacity
	if cm.selector.CanAcceptNewPeer() {
		if peer, ok := cm.GetNextPeerToConnect(); ok {
			cm.logger.Debug("exploring new peer", "peer_id", peer.ID)
			// In a real implementation, this would trigger a connection attempt
		}
	}

	// Log current stats
	activePeers := cm.GetActivePeerCount()
	candidatePeers := cm.GetCandidatePeerCount()
	cm.logger.Debug("connection manager stats",
		"active_peers", activePeers,
		"candidate_peers", candidatePeers,
	)
}

// GetStats returns current statistics
func (cm *ConnectionManager) GetStats() ConnectionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := ConnectionStats{
		ActivePeers:    len(cm.activePeers),
		CandidatePeers: len(cm.candidatePeers),
		TotalDownload:  0,
		TotalUpload:    0,
	}

	// Aggregate metrics
	for peerID := range cm.activePeers {
		if metrics, ok := cm.selector.GetMetrics(peerID); ok {
			stats.TotalDownload += metrics.Downloaded
			stats.TotalUpload += metrics.Uploaded
		}
	}

	return stats
}

// ConnectionStats holds connection statistics
type ConnectionStats struct {
	ActivePeers    int
	CandidatePeers int
	TotalDownload  int64
	TotalUpload    int64
}

// String returns a string representation of the stats
func (cs ConnectionStats) String() string {
	return fmt.Sprintf("Active: %d, Candidates: %d, Downloaded: %d bytes, Uploaded: %d bytes",
		cs.ActivePeers, cs.CandidatePeers, cs.TotalDownload, cs.TotalUpload)
}

// EnableReplacement enables or disables peer replacement
func (cm *ConnectionManager) EnableReplacement(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.replacementEnabled = enabled
}

// SetExplorationRate sets how often to try new peers
func (cm *ConnectionManager) SetExplorationRate(rate time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.explorationRate = rate
}

// Reset clears all connections and metrics
func (cm *ConnectionManager) Reset() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.activePeers = make(map[string]*PeerConnection)
	cm.candidatePeers = make([]PeerInfo, 0)
	cm.selector.Reset()
}
