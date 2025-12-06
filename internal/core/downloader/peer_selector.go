package downloader

import (
	"sort"
	"sync"
	"time"
)

// PeerMetrics tracks performance metrics for a single peer
type PeerMetrics struct {
	PeerID          string
	Downloaded      int64     // Total bytes downloaded from this peer
	Uploaded        int64     // Total bytes uploaded to this peer
	DownloadRate    float64   // Current download rate (bytes/sec)
	UploadRate      float64   // Current upload rate (bytes/sec)
	FailedRequests  int       // Number of failed requests
	SuccessRequests int       // Number of successful requests
	LastActive      time.Time // Last time we received data
	ConnectedAt     time.Time // When the connection was established
	Choked          bool      // Whether peer is choking us
	Interested      bool      // Whether peer is interested in us
	PiecesAvailable int       // Number of pieces this peer has
	LatencyMs       int64     // Average latency in milliseconds
}

// PeerSelector manages peer connections and selects the best peers
type PeerSelector struct {
	maxPeers        int
	minPeers        int
	metrics         map[string]*PeerMetrics
	mu              sync.RWMutex
	replacementRate time.Duration // How often to try new peers
}

// NewPeerSelector creates a new peer selector
func NewPeerSelector(maxPeers, minPeers int) *PeerSelector {
	return &PeerSelector{
		maxPeers:        maxPeers,
		minPeers:        minPeers,
		metrics:         make(map[string]*PeerMetrics),
		replacementRate: 30 * time.Second,
	}
}

// AddPeer adds a peer to be tracked
func (ps *PeerSelector) AddPeer(peerID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if _, exists := ps.metrics[peerID]; !exists {
		ps.metrics[peerID] = &PeerMetrics{
			PeerID:      peerID,
			ConnectedAt: time.Now(),
			LastActive:  time.Now(),
		}
	}
}

// RemovePeer removes a peer from tracking
func (ps *PeerSelector) RemovePeer(peerID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.metrics, peerID)
}

// UpdateDownload updates download metrics for a peer
func (ps *PeerSelector) UpdateDownload(peerID string, bytes int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.Downloaded += bytes
		metrics.LastActive = time.Now()
		metrics.SuccessRequests++

		// Calculate download rate (simple exponential moving average)
		elapsed := time.Since(metrics.ConnectedAt).Seconds()
		if elapsed > 0 {
			metrics.DownloadRate = float64(metrics.Downloaded) / elapsed
		}
	}
}

// UpdateUpload updates upload metrics for a peer
func (ps *PeerSelector) UpdateUpload(peerID string, bytes int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.Uploaded += bytes
		metrics.LastActive = time.Now()

		// Calculate upload rate
		elapsed := time.Since(metrics.ConnectedAt).Seconds()
		if elapsed > 0 {
			metrics.UploadRate = float64(metrics.Uploaded) / elapsed
		}
	}
}

// RecordFailure records a failed request for a peer
func (ps *PeerSelector) RecordFailure(peerID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.FailedRequests++
	}
}

// UpdateChokeStatus updates the choke status for a peer
func (ps *PeerSelector) UpdateChokeStatus(peerID string, choked bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.Choked = choked
	}
}

// UpdateInterestStatus updates the interest status for a peer
func (ps *PeerSelector) UpdateInterestStatus(peerID string, interested bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.Interested = interested
	}
}

// UpdatePiecesAvailable updates the number of pieces a peer has
func (ps *PeerSelector) UpdatePiecesAvailable(peerID string, count int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		metrics.PiecesAvailable = count
	}
}

// CalculateScore calculates a score for a peer based on various metrics
func (ps *PeerSelector) CalculateScore(metrics *PeerMetrics) float64 {
	score := 0.0

	// Download rate (most important - 50% weight)
	score += metrics.DownloadRate * 0.5

	// Success rate (30% weight)
	totalRequests := metrics.SuccessRequests + metrics.FailedRequests
	if totalRequests > 0 {
		successRate := float64(metrics.SuccessRequests) / float64(totalRequests)
		score += successRate * 1000000 * 0.3 // Scale to comparable range
	}

	// Pieces available (10% weight)
	score += float64(metrics.PiecesAvailable) * 100 * 0.1

	// Penalize choking peers
	if metrics.Choked {
		score *= 0.5
	}

	// Penalize inactive peers
	if time.Since(metrics.LastActive) > 60*time.Second {
		score *= 0.3
	}

	return score
}

// GetTopPeers returns the top N performing peers
func (ps *PeerSelector) GetTopPeers(n int) []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	type peerScore struct {
		peerID string
		score  float64
	}

	scores := make([]peerScore, 0, len(ps.metrics))
	for peerID, metrics := range ps.metrics {
		scores = append(scores, peerScore{
			peerID: peerID,
			score:  ps.CalculateScore(metrics),
		})
	}

	// Sort by score descending
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// Return top N peer IDs
	result := make([]string, 0, n)
	for i := 0; i < n && i < len(scores); i++ {
		result = append(result, scores[i].peerID)
	}

	return result
}

// GetWorstPeers returns the worst N performing peers
func (ps *PeerSelector) GetWorstPeers(n int) []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	type peerScore struct {
		peerID string
		score  float64
	}

	scores := make([]peerScore, 0, len(ps.metrics))
	for peerID, metrics := range ps.metrics {
		scores = append(scores, peerScore{
			peerID: peerID,
			score:  ps.CalculateScore(metrics),
		})
	}

	// Sort by score ascending (worst first)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score < scores[j].score
	})

	// Return worst N peer IDs
	result := make([]string, 0, n)
	for i := 0; i < n && i < len(scores); i++ {
		result = append(result, scores[i].peerID)
	}

	return result
}

// ShouldReplacePeer determines if we should replace a poorly performing peer
func (ps *PeerSelector) ShouldReplacePeer() (string, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	// Don't replace if we're below minimum peers
	if len(ps.metrics) <= ps.minPeers {
		return "", false
	}

	// Find the worst performing peer
	var worstPeer string
	var worstScore float64 = -1

	for peerID, metrics := range ps.metrics {
		score := ps.CalculateScore(metrics)

		// Consider replacing if:
		// 1. Very low score
		// 2. High failure rate
		// 3. Inactive for too long
		failureRate := 0.0
		totalRequests := metrics.SuccessRequests + metrics.FailedRequests
		if totalRequests > 0 {
			failureRate = float64(metrics.FailedRequests) / float64(totalRequests)
		}

		shouldConsider := score < 100 || // Very low score
			failureRate > 0.5 || // High failure rate
			time.Since(metrics.LastActive) > 120*time.Second // Inactive

		if shouldConsider && (worstScore < 0 || score < worstScore) {
			worstScore = score
			worstPeer = peerID
		}
	}

	if worstPeer != "" {
		return worstPeer, true
	}

	return "", false
}

// CanAcceptNewPeer returns whether we can accept a new peer connection
func (ps *PeerSelector) CanAcceptNewPeer() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return len(ps.metrics) < ps.maxPeers
}

// GetPeerCount returns the current number of tracked peers
func (ps *PeerSelector) GetPeerCount() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return len(ps.metrics)
}

// GetMetrics returns a copy of metrics for a specific peer
func (ps *PeerSelector) GetMetrics(peerID string) (*PeerMetrics, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if metrics, exists := ps.metrics[peerID]; exists {
		// Return a copy to avoid race conditions
		copy := *metrics
		return &copy, true
	}

	return nil, false
}

// GetAllMetrics returns a copy of all peer metrics
func (ps *PeerSelector) GetAllMetrics() map[string]*PeerMetrics {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	result := make(map[string]*PeerMetrics, len(ps.metrics))
	for peerID, metrics := range ps.metrics {
		copy := *metrics
		result[peerID] = &copy
	}

	return result
}

// Reset clears all peer metrics
func (ps *PeerSelector) Reset() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.metrics = make(map[string]*PeerMetrics)
}
