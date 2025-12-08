package bittorrent

import (
	"fmt"
	"sync"

	"github.com/revtheundead/revtorrent/internal/engine"
)

// ============================================================================
// PUBLIC API LAYER - Torrent Handle
// ============================================================================
// This file defines the Torrent handle for the public API.
// The actual torrent session management is coordinated by the engine layer.
// ============================================================================

// Torrent represents a single torrent download/upload session
type Torrent struct {
	// Session from engine layer
	session *engine.Session

	// Event channel for API users
	events chan Event

	// Event forwarding
	stopEventForwarder chan struct{}
	wg                 sync.WaitGroup
}

// newTorrent creates a new Torrent handle from a session
func newTorrent(session *engine.Session) *Torrent {
	t := &Torrent{
		session:            session,
		events:             make(chan Event, 100),
		stopEventForwarder: make(chan struct{}),
	}

	// Start event forwarder
	t.wg.Add(1)
	go t.forwardEvents()

	return t
}

// Name returns the name of the torrent
func (t *Torrent) Name() string {
	return t.session.Name()
}

// InfoHash returns the info hash as a hex string
func (t *Torrent) InfoHash() string {
	return t.session.InfoHashHex()
}

// Size returns the total size of the torrent in bytes
func (t *Torrent) Size() int64 {
	return t.session.Size()
}

// Progress returns the current download progress (0.0 to 1.0)
func (t *Torrent) Progress() float64 {
	return t.session.Progress()
}

// Stats returns a snapshot of the current statistics
func (t *Torrent) Stats() Stats {
	sessionStats := t.session.Stats()

	return Stats{
		Progress:       sessionStats.Progress,
		Downloaded:     sessionStats.Downloaded,
		Uploaded:       sessionStats.Uploaded,
		DownloadRate:   sessionStats.DownloadRate,
		UploadRate:     sessionStats.UploadRate,
		Peers:          sessionStats.Peers,
		PiecesComplete: sessionStats.PiecesComplete,
		TotalPieces:    sessionStats.TotalPieces,
		State:          convertSessionState(t.session),
	}
}

// State returns the current state of the torrent
func (t *Torrent) State() TorrentState {
	return convertSessionState(t.session)
}

func convertSessionState(session *engine.Session) TorrentState {
	// Get the actual state from session
	sessionState := session.State()

	switch sessionState {
	case engine.StateStopped:
		return StateStopped
	case engine.StateDownloading:
		return StateDownloading
	case engine.StateSeeding:
		return StateSeeding
	case engine.StatePaused:
		return StatePaused
	case engine.StateChecking:
		return StateChecking
	case engine.StateError:
		return StateError
	default:
		return StateDownloading
	}
}

// Start begins downloading the torrent
func (t *Torrent) Start() error {
	return t.session.Start()
}

// Stop stops the torrent download/upload
func (t *Torrent) Stop() error {
	// Stop event forwarder
	close(t.stopEventForwarder)
	t.wg.Wait()

	// Stop session
	return t.session.Stop()
}

// Pause pauses the torrent (saves state but stops activity)
func (t *Torrent) Pause() error {
	return t.session.Pause()
}

// Resume resumes a paused torrent
func (t *Torrent) Resume() error {
	return t.session.Resume()
}

// Events returns a read-only channel for receiving torrent events
func (t *Torrent) Events() <-chan Event {
	return t.events
}

// WaitForCompletion blocks until the torrent download is complete or an error occurs
func (t *Torrent) WaitForCompletion() error {
	if t.events == nil {
		return fmt.Errorf("no event channel available")
	}

	for event := range t.events {
		switch event.Type {
		case EventComplete:
			return nil
		case EventError:
			return event.Error
		}
	}

	return fmt.Errorf("event channel closed unexpectedly")
}

// VerifyData verifies all downloaded pieces
func (t *Torrent) VerifyData() error {
	verified, failed, err := t.session.VerifyData()
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	if failed > 0 {
		return fmt.Errorf("verification found %d corrupted pieces (verified: %d)", failed, verified)
	}

	return nil
}

// Files returns information about the files in this torrent
func (t *Torrent) Files() ([]FileInfo, error) {
	sessionFiles := t.session.Files()
	files := make([]FileInfo, len(sessionFiles))
	for i, f := range sessionFiles {
		files[i] = FileInfo{
			Path:   f.Path,
			Size:   f.Size,
			Offset: f.Offset,
		}
	}
	return files, nil
}

// FileInfo represents information about a file in a torrent
type FileInfo struct {
	Path   string // Full path relative to download directory
	Size   int64  // Size in bytes
	Offset int64  // Byte offset in the torrent
}

// forwardEvents forwards events from session to API event channel
func (t *Torrent) forwardEvents() {
	defer t.wg.Done()

	sessionEvents := t.session.Events()

	for {
		select {
		case <-t.stopEventForwarder:
			return
		case sessionEvent, ok := <-sessionEvents:
			if !ok {
				// Session event channel closed
				return
			}

			// Convert session event to API event
			apiEvent := Event{
				Type:      convertEventType(sessionEvent.Type),
				InfoHash:  sessionEvent.InfoHash,
				Progress:  sessionEvent.Progress,
				Error:     sessionEvent.Error,
				Timestamp: sessionEvent.Timestamp,
			}

			// Convert stats
			apiEvent.Stats = Stats{
				Progress:       sessionEvent.Stats.Progress,
				Downloaded:     sessionEvent.Stats.Downloaded,
				Uploaded:       sessionEvent.Stats.Uploaded,
				DownloadRate:   sessionEvent.Stats.DownloadRate,
				UploadRate:     sessionEvent.Stats.UploadRate,
				Peers:          sessionEvent.Stats.Peers,
				PiecesComplete: sessionEvent.Stats.PiecesComplete,
				TotalPieces:    sessionEvent.Stats.TotalPieces,
				State:          convertSessionState(t.session),
			}

			// Send to API event channel (non-blocking)
			select {
			case t.events <- apiEvent:
			default:
				// Event channel full, drop event
			}
		}
	}
}

func convertEventType(sessionEventType engine.SessionEventType) EventType {
	switch sessionEventType {
	case engine.EventStarted:
		return EventStarted
	case engine.EventProgress:
		return EventProgress
	case engine.EventPieceComplete:
		return EventPieceComplete
	case engine.EventComplete:
		return EventComplete
	case engine.EventSeeding:
		return EventSeeding
	case engine.EventPaused:
		return EventPaused
	case engine.EventResumed:
		return EventResumed
	case engine.EventError:
		return EventError
	default:
		return EventProgress
	}
}
