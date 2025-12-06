package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/config"
	"github.com/revtheundead/revtorrent/internal/core/storage"
	"github.com/revtheundead/revtorrent/internal/core/torrent"
	"github.com/revtheundead/revtorrent/internal/tracker"
)

// SessionState represents the current state of a torrent session
type SessionState int

const (
	StateStopped SessionState = iota
	StateChecking
	StateDownloading
	StateSeeding
	StatePaused
	StateError
)

// Session manages a single torrent download/upload session
type Session struct {
	// Metadata
	meta   *torrent.Metainfo
	config *config.Config
	logger *slog.Logger

	// Storage
	storage       *storage.FileStorage
	resumeManager *storage.ResumeManager

	// Tracker management
	trackerManager *tracker.Manager

	// Statistics
	stats      SessionStats
	statsMu    sync.RWMutex
	startedAt  time.Time
	completedAt *time.Time

	// State
	state   SessionState
	stateMu sync.RWMutex

	// Event channel for API layer
	eventCh chan SessionEvent

	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Running flag
	running bool
	mu      sync.Mutex
}

// SessionStats holds statistics for a session
type SessionStats struct {
	Downloaded     int64
	Uploaded       int64
	DownloadRate   float64
	UploadRate     float64
	Progress       float64
	PiecesComplete int
	TotalPieces    int
	Peers          int
}

// SessionEvent represents an event from a session
type SessionEvent struct {
	Type      SessionEventType
	InfoHash  string
	Progress  float64
	Stats     SessionStats
	Error     error
	Timestamp time.Time
}

// SessionEventType represents the type of session event
type SessionEventType int

const (
	EventStarted SessionEventType = iota
	EventProgress
	EventPieceComplete
	EventComplete
	EventSeeding
	EventPaused
	EventResumed
	EventError
)

// NewSession creates a new torrent session
func NewSession(meta *torrent.Metainfo, cfg *config.Config, logger *slog.Logger) (*Session, error) {
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create storage
	stor := storage.NewFileStorage(cfg.DownloadPath, &meta.Info)

	// Create tracker manager
	trackerURLs := []string{meta.Announce}
	for _, tier := range meta.AnnounceList {
		trackerURLs = append(trackerURLs, tier...)
	}
	trackerMgr := tracker.NewManager(trackerURLs, logger)

	// Create resume manager
	resumeMgr, err := storage.NewResumeManager(cfg.StateDir)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create resume manager: %w", err)
	}

	session := &Session{
		meta:           meta,
		config:         cfg,
		logger:         logger,
		storage:        stor,
		resumeManager:  resumeMgr,
		trackerManager: trackerMgr,
		state:          StateStopped,
		eventCh:        make(chan SessionEvent, 100),
		ctx:            ctx,
		cancel:         cancel,
	}

	return session, nil
}

// Start starts the session
func (s *Session) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("session already running")
	}

	s.logger.Info("starting session", "name", s.meta.Info.Name, "info_hash", s.meta.InfoHashHex)

	// Initialize storage
	if err := s.storage.Init(); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Try to load resume data
	if resumeData, err := s.resumeManager.Load(s.meta.InfoHashHex); err == nil {
		s.logger.Info("loading resume data", "pieces_complete", len(resumeData.CompletedPieces))
		if err := storage.ApplyResumeData(resumeData, s.storage); err != nil {
			s.logger.Warn("failed to apply resume data", "error", err)
		}
	} else {
		s.logger.Debug("no resume data found")
	}

	// Update state
	s.setState(StateChecking)
	s.running = true
	s.startedAt = time.Now()

	// Send started event
	s.sendEvent(SessionEvent{
		Type:      EventStarted,
		InfoHash:  s.meta.InfoHashHex,
		Timestamp: time.Now(),
	})

	// Start progress monitor
	s.wg.Add(1)
	go s.progressMonitor()

	// Start tracker announcements
	s.wg.Add(1)
	go s.trackerAnnounceLoop()

	// TODO: Start download workers (Phase 12.1)
	// TODO: Start upload workers (Phase 12.1)

	// For now, just check if already complete
	if s.storage.IsComplete() {
		s.setState(StateSeeding)
		now := time.Now()
		s.completedAt = &now
		s.sendEvent(SessionEvent{
			Type:      EventComplete,
			InfoHash:  s.meta.InfoHashHex,
			Timestamp: time.Now(),
		})
	} else {
		s.setState(StateDownloading)
	}

	return nil
}

// Stop stops the session
func (s *Session) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping session", "info_hash", s.meta.InfoHashHex)

	// Signal shutdown
	s.cancel()

	// Save resume data
	resumeData := storage.CreateResumeData(s.meta.InfoHashHex, s.meta.Info.Name, s.storage)
	if err := s.resumeManager.Save(resumeData); err != nil {
		s.logger.Warn("failed to save resume data", "error", err)
	}

	// Announce to tracker with event=stopped
	if s.trackerManager != nil {
		s.announceToTracker("stopped")
	}

	// Close storage
	if s.storage != nil {
		s.storage.Close()
	}

	// Wait for goroutines
	s.wg.Wait()

	// Close event channel
	close(s.eventCh)

	s.setState(StateStopped)
	s.logger.Info("session stopped", "info_hash", s.meta.InfoHashHex)

	return nil
}

// Pause pauses the session
func (s *Session) Pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("session not running")
	}

	s.logger.Info("pausing session", "info_hash", s.meta.InfoHashHex)

	// Save current state
	resumeData := storage.CreateResumeData(s.meta.InfoHashHex, s.meta.Info.Name, s.storage)
	if err := s.resumeManager.Save(resumeData); err != nil {
		s.logger.Warn("failed to save resume data", "error", err)
	}

	// Update state
	s.setState(StatePaused)
	s.sendEvent(SessionEvent{
		Type:      EventPaused,
		InfoHash:  s.meta.InfoHashHex,
		Timestamp: time.Now(),
	})

	// TODO: Pause download/upload workers without stopping them

	return nil
}

// Resume resumes a paused session
func (s *Session) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("session not running")
	}

	currentState := s.getState()
	if currentState != StatePaused {
		return fmt.Errorf("session not paused")
	}

	s.logger.Info("resuming session", "info_hash", s.meta.InfoHashHex)

	// Determine new state
	if s.storage.IsComplete() {
		s.setState(StateSeeding)
	} else {
		s.setState(StateDownloading)
	}

	s.sendEvent(SessionEvent{
		Type:      EventResumed,
		InfoHash:  s.meta.InfoHashHex,
		Timestamp: time.Now(),
	})

	// TODO: Resume download/upload workers

	return nil
}

// HandleIncomingPeer handles an incoming peer connection
func (s *Session) HandleIncomingPeer(conn interface{}) error {
	// TODO: Implement incoming peer handling with proper handshake and uploader
	s.logger.Debug("incoming peer (not yet implemented)")
	return fmt.Errorf("incoming peer handling not yet implemented")
}

// Name returns the torrent name
func (s *Session) Name() string {
	return s.meta.Info.Name
}

// InfoHashHex returns the hex-encoded info hash
func (s *Session) InfoHashHex() string {
	return s.meta.InfoHashHex
}

// Size returns the total size
func (s *Session) Size() int64 {
	return s.meta.Info.TotalLength()
}

// Progress returns download progress (0.0 to 1.0)
func (s *Session) Progress() float64 {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats.Progress
}

// Stats returns current statistics
func (s *Session) Stats() SessionStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// Events returns the event channel
func (s *Session) Events() <-chan SessionEvent {
	return s.eventCh
}

// Files returns file information
func (s *Session) Files() []FileInfo {
	if s.meta.Info.IsMultiFile() {
		files := make([]FileInfo, len(s.meta.Info.Files))
		offset := int64(0)
		for i, f := range s.meta.Info.Files {
			files[i] = FileInfo{
				Path:   f.FullPath(),
				Size:   f.Length,
				Offset: offset,
			}
			offset += f.Length
		}
		return files
	}

	// Single file
	return []FileInfo{
		{
			Path:   s.meta.Info.Name,
			Size:   s.meta.Info.Length,
			Offset: 0,
		},
	}
}

// FileInfo represents file information
type FileInfo struct {
	Path   string
	Size   int64
	Offset int64
}

// Internal methods

func (s *Session) setState(state SessionState) {
	s.stateMu.Lock()
	s.state = state
	s.stateMu.Unlock()
}

func (s *Session) getState() SessionState {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

func (s *Session) sendEvent(event SessionEvent) {
	select {
	case s.eventCh <- event:
	default:
		// Channel full, drop event
	}
}

func (s *Session) updateStats() {
	progress := s.storage.GetProgress()

	s.statsMu.Lock()
	s.stats.Progress = progress
	s.stats.PiecesComplete = s.storage.Bitfield().Count()
	s.stats.TotalPieces = s.storage.Bitfield().Len()
	s.stats.Downloaded = int64(float64(s.meta.Info.TotalLength()) * progress)
	// TODO: Track actual uploaded bytes
	s.statsMu.Unlock()
}

func (s *Session) progressMonitor() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateStats()

			// Send progress event if downloading
			state := s.getState()
			if state == StateDownloading || state == StateSeeding {
				stats := s.Stats()
				s.sendEvent(SessionEvent{
					Type:      EventProgress,
					InfoHash:  s.meta.InfoHashHex,
					Progress:  stats.Progress,
					Stats:     stats,
					Timestamp: time.Now(),
				})

				// Check if complete
				if state == StateDownloading && s.storage.IsComplete() {
					s.setState(StateSeeding)
					now := time.Now()
					s.completedAt = &now
					s.sendEvent(SessionEvent{
						Type:      EventComplete,
						InfoHash:  s.meta.InfoHashHex,
						Timestamp: time.Now(),
					})
				}
			}
		}
	}
}

func (s *Session) trackerAnnounceLoop() {
	defer s.wg.Done()

	// Initial announce
	s.announceToTracker("started")

	// Regular announces
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.announceToTracker("")
		}
	}
}

func (s *Session) announceToTracker(event string) {
	stats := s.Stats()

	req := &tracker.AnnounceRequest{
		InfoHash:   s.meta.InfoHash,
		PeerID:     tracker.GeneratePeerID(),
		Port:       uint16(s.config.ListenPort),
		Uploaded:   uint64(stats.Uploaded),
		Downloaded: uint64(stats.Downloaded),
		Left:       uint64(s.meta.Info.TotalLength() - stats.Downloaded),
		Compact:    true,
		Event:      event,
		NumWant:    50,
	}

	resp, err := s.trackerManager.Announce(req)
	if err != nil {
		s.logger.Warn("tracker announce failed", "error", err)
		return
	}

	s.logger.Debug("tracker announce successful",
		"peers", len(resp.Peers),
		"interval", resp.Interval)

	// TODO: Connect to returned peers
}
