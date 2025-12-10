package tracker

import (
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"
)

// Tracker interface for both HTTP and UDP trackers
type Tracker interface {
	Announce(req *AnnounceRequest) (*TrackerResponse, error)
	Scrape(infoHashes [][20]byte) (*ScrapeResponse, error)
}

// Manager coordinates multiple trackers (HTTP and UDP)
type Manager struct {
	trackers []trackerEntry
	logger   *slog.Logger
	mu       sync.RWMutex
}

type trackerEntry struct {
	url     string
	tracker Tracker
	tier    int
	active  bool
}

// NewManager creates a new tracker manager
func NewManager(trackerURLs []string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	m := &Manager{
		trackers: make([]trackerEntry, 0),
		logger:   logger,
	}

	// Initialize trackers
	for i, trackerURL := range trackerURLs {
		// Skip empty tracker URLs
		if trackerURL == "" {
			logger.Debug("skipping empty tracker URL")
			continue
		}

		tracker, err := createTracker(trackerURL)
		if err != nil {
			logger.Warn("failed to create tracker", "url", trackerURL, "error", err)
			continue
		}

		m.trackers = append(m.trackers, trackerEntry{
			url:     trackerURL,
			tracker: tracker,
			tier:    i, // Simple tier assignment
			active:  true,
		})
	}

	return m
}

// Announce announces to all trackers in parallel and returns the first successful response
func (m *Manager) Announce(req *AnnounceRequest) (*TrackerResponse, error) {
	m.mu.RLock()
	activeTrackers := m.getActiveTrackers()
	m.mu.RUnlock()

	if len(activeTrackers) == 0 {
		return nil, fmt.Errorf("no active trackers")
	}

	type result struct {
		resp *TrackerResponse
		url  string
		err  error
	}

	results := make(chan result, len(activeTrackers))

	// Query all trackers in parallel
	for _, entry := range activeTrackers {
		entry := entry // Capture loop variable
		go func() {
			resp, err := entry.tracker.Announce(req)
			results <- result{
				resp: resp,
				url:  entry.url,
				err:  err,
			}
		}()
	}

	// Collect results
	var bestResponse *TrackerResponse
	var lastError error

	for i := 0; i < len(activeTrackers); i++ {
		res := <-results

		if res.err != nil {
			m.logger.Debug("tracker announce failed", "url", res.url, "error", res.err)
			lastError = res.err
			// Mark tracker as inactive on error
			m.markTrackerStatus(res.url, false)
			continue
		}

		m.logger.Debug("tracker announce succeeded", "url", res.url, "peers", len(res.resp.Peers))

		// Mark tracker as active on success
		m.markTrackerStatus(res.url, true)

		// Return first successful response with peers
		if len(res.resp.Peers) > 0 {
			return res.resp, nil
		}

		// Keep track of best response (even if no peers)
		if bestResponse == nil || len(res.resp.Peers) > len(bestResponse.Peers) {
			bestResponse = res.resp
		}
	}

	// Return best response if we got any, otherwise return error
	if bestResponse != nil {
		return bestResponse, nil
	}

	if lastError != nil {
		return nil, fmt.Errorf("all trackers failed, last error: %w", lastError)
	}

	return nil, fmt.Errorf("no trackers responded")
}

// AnnounceSingle announces to a single tracker (sequential, tries each until success)
func (m *Manager) AnnounceSingle(req *AnnounceRequest) (*TrackerResponse, error) {
	m.mu.RLock()
	activeTrackers := m.getActiveTrackers()
	m.mu.RUnlock()

	if len(activeTrackers) == 0 {
		return nil, fmt.Errorf("no active trackers")
	}

	var lastError error

	// Try each tracker sequentially
	for _, entry := range activeTrackers {
		resp, err := entry.tracker.Announce(req)
		if err != nil {
			m.logger.Debug("tracker announce failed", "url", entry.url, "error", err)
			lastError = err
			m.markTrackerStatus(entry.url, false)
			continue
		}

		m.logger.Debug("tracker announce succeeded", "url", entry.url, "peers", len(resp.Peers))
		m.markTrackerStatus(entry.url, true)
		return resp, nil
	}

	if lastError != nil {
		return nil, fmt.Errorf("all trackers failed, last error: %w", lastError)
	}

	return nil, fmt.Errorf("no active trackers")
}

// Scrape scrapes all trackers for torrent statistics
func (m *Manager) Scrape(infoHashes [][20]byte) (*ScrapeResponse, error) {
	m.mu.RLock()
	activeTrackers := m.getActiveTrackers()
	m.mu.RUnlock()

	if len(activeTrackers) == 0 {
		return nil, fmt.Errorf("no active trackers")
	}

	// Try first active tracker
	for _, entry := range activeTrackers {
		resp, err := entry.tracker.Scrape(infoHashes)
		if err != nil {
			m.logger.Debug("tracker scrape failed", "url", entry.url, "error", err)
			continue
		}

		m.logger.Debug("tracker scrape succeeded", "url", entry.url)
		return resp, nil
	}

	return nil, fmt.Errorf("all scrape requests failed")
}

// AddTracker adds a new tracker to the manager
func (m *Manager) AddTracker(trackerURL string) error {
	tracker, err := createTracker(trackerURL)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.trackers = append(m.trackers, trackerEntry{
		url:     trackerURL,
		tracker: tracker,
		tier:    len(m.trackers),
		active:  true,
	})

	return nil
}

// GetTrackerCount returns the number of active trackers
func (m *Manager) GetTrackerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, entry := range m.trackers {
		if entry.active {
			count++
		}
	}

	return count
}

// PeriodicAnnounce performs periodic announces to trackers
func (m *Manager) PeriodicAnnounce(req *AnnounceRequest, interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			resp, err := m.AnnounceSingle(req)
			if err != nil {
				m.logger.Warn("periodic announce failed", "error", err)
			} else {
				m.logger.Info("periodic announce succeeded", "peers", len(resp.Peers))
			}
		}
	}
}

// Helper methods

func (m *Manager) getActiveTrackers() []trackerEntry {
	active := make([]trackerEntry, 0)
	for _, entry := range m.trackers {
		if entry.active {
			active = append(active, entry)
		}
	}
	return active
}

func (m *Manager) markTrackerStatus(url string, active bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, entry := range m.trackers {
		if entry.url == url {
			m.trackers[i].active = active
			break
		}
	}
}

// createTracker creates the appropriate tracker type based on URL scheme
func createTracker(trackerURL string) (Tracker, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid tracker URL: %w", err)
	}

	switch u.Scheme {
	case "http", "https":
		return NewHTTPTracker(trackerURL)
	case "udp":
		return NewUDPTracker(trackerURL)
	default:
		return nil, fmt.Errorf("unsupported tracker scheme: %s", u.Scheme)
	}
}
