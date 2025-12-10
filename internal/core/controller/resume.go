package controller

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/revtheundead/revtorrent/internal/core/storage"
)

// TorrentState represents the current state of a torrent
type TorrentState int

const (
	StateQueued TorrentState = iota
	StateChecking
	StateDownloading
	StateSeeding
	StatePaused
	StateStopped
	StateError
)

// String returns a string representation of the torrent state
func (s TorrentState) String() string {
	switch s {
	case StateQueued:
		return "queued"
	case StateChecking:
		return "checking"
	case StateDownloading:
		return "downloading"
	case StateSeeding:
		return "seeding"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	case StateError:
		return "error"
	default:
		return "unknown"
	}
}

// TorrentController manages the lifecycle of a single torrent
type TorrentController struct {
	infoHash      string
	name          string
	state         TorrentState
	storage       *storage.FileStorage
	resumeManager *storage.ResumeManager
	logger        *slog.Logger
	mu            sync.RWMutex
	stopCh        chan struct{}
	pauseCh       chan struct{}
	resumeCh      chan struct{}
}

// NewTorrentController creates a new torrent controller
func NewTorrentController(
	infoHash string,
	name string,
	storage *storage.FileStorage,
	resumeManager *storage.ResumeManager,
	logger *slog.Logger,
) *TorrentController {
	if logger == nil {
		logger = slog.Default()
	}

	return &TorrentController{
		infoHash:      infoHash,
		name:          name,
		state:         StateQueued,
		storage:       storage,
		resumeManager: resumeManager,
		logger:        logger,
		stopCh:        make(chan struct{}),
		pauseCh:       make(chan struct{}),
		resumeCh:      make(chan struct{}),
	}
}

// Start starts the torrent download/seeding
func (tc *TorrentController) Start() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.state == StateDownloading || tc.state == StateSeeding {
		return fmt.Errorf("torrent already running")
	}

	// Check for resume data
	if tc.resumeManager.Exists(tc.infoHash) {
		tc.logger.Info("found resume data, attempting to resume", "info_hash", tc.infoHash)
		if err := tc.loadResumeData(); err != nil {
			tc.logger.Warn("failed to load resume data, starting fresh", "error", err)
		}
	}

	// Determine initial state
	if tc.storage.IsComplete() {
		tc.state = StateSeeding
	} else {
		tc.state = StateDownloading
	}

	tc.logger.Info("torrent started", "info_hash", tc.infoHash, "state", tc.state)
	return nil
}

// Pause pauses the torrent
func (tc *TorrentController) Pause() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.state == StatePaused {
		return fmt.Errorf("torrent already paused")
	}

	if tc.state == StateStopped {
		return fmt.Errorf("torrent is stopped")
	}

	// Save resume data before pausing
	if err := tc.saveResumeData(); err != nil {
		tc.logger.Warn("failed to save resume data on pause", "error", err)
	}

	tc.state = StatePaused

	// Signal pause
	select {
	case tc.pauseCh <- struct{}{}:
	default:
	}

	tc.logger.Info("torrent paused", "info_hash", tc.infoHash)
	return nil
}

// Resume resumes a paused torrent
func (tc *TorrentController) Resume() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.state != StatePaused {
		return fmt.Errorf("torrent is not paused")
	}

	// Determine state based on completion
	if tc.storage.IsComplete() {
		tc.state = StateSeeding
	} else {
		tc.state = StateDownloading
	}

	// Signal resume
	select {
	case tc.resumeCh <- struct{}{}:
	default:
	}

	tc.logger.Info("torrent resumed", "info_hash", tc.infoHash, "state", tc.state)
	return nil
}

// Stop stops the torrent completely
func (tc *TorrentController) Stop() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.state == StateStopped {
		return nil
	}

	// Save resume data before stopping
	if err := tc.saveResumeData(); err != nil {
		tc.logger.Warn("failed to save resume data on stop", "error", err)
	}

	tc.state = StateStopped

	// Signal stop
	close(tc.stopCh)

	tc.logger.Info("torrent stopped", "info_hash", tc.infoHash)
	return nil
}

// GetState returns the current state of the torrent
func (tc *TorrentController) GetState() TorrentState {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.state
}

// SetState sets the torrent state
func (tc *TorrentController) SetState(state TorrentState) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.state = state
}

// IsPaused returns whether the torrent is paused
func (tc *TorrentController) IsPaused() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.state == StatePaused
}

// IsRunning returns whether the torrent is actively running
func (tc *TorrentController) IsRunning() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.state == StateDownloading || tc.state == StateSeeding
}

// saveResumeData saves the current state to disk
func (tc *TorrentController) saveResumeData() error {
	resumeData := storage.CreateResumeData(tc.infoHash, tc.name, tc.storage)
	return tc.resumeManager.Save(resumeData)
}

// loadResumeData loads and applies resume data from disk
func (tc *TorrentController) loadResumeData() error {
	resumeData, err := tc.resumeManager.Load(tc.infoHash)
	if err != nil {
		return fmt.Errorf("failed to load resume data: %w", err)
	}

	// Verify and apply resume data
	if err := storage.ApplyResumeData(resumeData, tc.storage); err != nil {
		return fmt.Errorf("failed to apply resume data: %w", err)
	}

	tc.logger.Info("resume data applied",
		"pieces_complete", len(resumeData.CompletedPieces),
		"progress", fmt.Sprintf("%.1f%%", resumeData.CalculateProgress()*100))

	return nil
}

// SaveResume manually saves resume data
func (tc *TorrentController) SaveResume() error {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.saveResumeData()
}

// DeleteResume deletes resume data from disk
func (tc *TorrentController) DeleteResume() error {
	return tc.resumeManager.Delete(tc.infoHash)
}

// GetProgress returns the download progress (0.0 to 1.0)
func (tc *TorrentController) GetProgress() float64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.storage.GetProgress()
}

// GetInfoHash returns the info hash
func (tc *TorrentController) GetInfoHash() string {
	return tc.infoHash
}

// GetName returns the torrent name
func (tc *TorrentController) GetName() string {
	return tc.name
}

// StopChan returns the stop channel
func (tc *TorrentController) StopChan() <-chan struct{} {
	return tc.stopCh
}

// PauseChan returns the pause channel
func (tc *TorrentController) PauseChan() <-chan struct{} {
	return tc.pauseCh
}

// ResumeChan returns the resume channel
func (tc *TorrentController) ResumeChan() <-chan struct{} {
	return tc.resumeCh
}

// PeriodicSave periodically saves resume data
func (tc *TorrentController) PeriodicSave(interval int) {
	// Implement periodic save logic
	// This would be called in a goroutine
}
