package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/config"
	"github.com/revtheundead/revtorrent/internal/core/downloader"
	"github.com/revtheundead/revtorrent/internal/core/peer"
	"github.com/revtheundead/revtorrent/internal/core/piece"
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
	engine *Engine // Reference to engine for DHT access

	// Storage
	storage       *storage.FileStorage
	resumeManager *storage.ResumeManager

	// Tracker management
	trackerManager *tracker.Manager

	// Download coordination
	pieceManager    *piece.Manager
	coordinator     *downloader.DownloadCoordinator
	downloadPeers   map[string]net.Conn // addr -> connection
	downloadPeersMu sync.RWMutex
	endgameTracker  *downloader.EndgameTracker

	// Upload coordination
	uploadPeers   map[string]net.Conn // addr -> connection for upload peers
	uploadPeersMu sync.RWMutex

	// Statistics
	stats              SessionStats
	statsMu            sync.RWMutex
	startedAt          time.Time
	completedAt        *time.Time
	uploadedBytes      int64     // Track uploaded bytes
	previousDownloaded int64     // For rate calculation
	previousUploaded   int64     // For rate calculation
	lastStatsUpdate    time.Time // For rate calculation

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
func NewSession(meta *torrent.Metainfo, cfg *config.Config, logger *slog.Logger, engine *Engine) (*Session, error) {
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
		engine:         engine,
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
		s.logger.Debug("loading resume data", "pieces_complete", len(resumeData.CompletedPieces))
		if err := storage.ApplyResumeData(resumeData, s.storage); err != nil {
			s.logger.Debug("failed to apply resume data", "error", err)
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

	// Start DHT peer discovery (if DHT is enabled)
	if s.config.DHTEnabled {
		s.wg.Add(1)
		go s.dhtPeerDiscoveryLoop()
	}

	// Initialize piece manager
	s.pieceManager = piece.NewManager(&s.meta.Info, s.storage)

	// Initialize download peer tracker
	s.downloadPeers = make(map[string]net.Conn)

	// Initialize upload peer tracker
	s.uploadPeers = make(map[string]net.Conn)

	// Initialize endgame tracker
	s.endgameTracker = downloader.NewEndgameTracker()

	// Initialize and start download coordinator
	if !s.storage.IsComplete() {
		s.coordinator = downloader.NewCoordinator(
			s.storage,
			s.pieceManager,
			&s.meta.Info,
			s.logger,
		)

		s.wg.Add(1)
		go s.coordinator.Start(s.ctx, &s.wg)
	}

	// Check if already complete
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

	// Close all upload peer connections
	s.uploadPeersMu.Lock()
	for addr, conn := range s.uploadPeers {
		conn.Close()
		delete(s.uploadPeers, addr)
	}
	s.uploadPeersMu.Unlock()

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

	// Note: State change to StatePaused signals workers to suspend operations
	// Download workers continue running but won't request new pieces
	// Upload workers continue serving existing connections
	s.logger.Debug("session paused - workers will suspend new operations")

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

	// Note: State change signals workers to resume operations
	// Download workers will resume requesting new pieces
	// Upload workers continue normal operation
	s.logger.Debug("session resumed - workers will resume normal operations")

	return nil
}

// HandleIncomingPeer handles an incoming peer connection
func (s *Session) HandleIncomingPeer(conn net.Conn, handshake *peer.Handshake) error {
	s.logger.Info("UPLOAD: handling incoming peer", "addr", conn.RemoteAddr())

	// Send our handshake
	ourHandshake := peer.NewHandshake(s.meta.InfoHash, tracker.GeneratePeerID())
	if _, err := conn.Write(ourHandshake.Serialize()); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	// Send bitfield message
	bitfield := s.storage.Bitfield()
	bitfieldMsg := peer.Message{
		ID:      peer.MsgBitfield,
		Payload: bitfield.GetBytes(),
	}
	if err := peer.WriteMessage(conn, bitfieldMsg); err != nil {
		return fmt.Errorf("failed to send bitfield: %w", err)
	}

	// Register upload peer
	addr := conn.RemoteAddr().String()
	s.uploadPeersMu.Lock()
	s.uploadPeers[addr] = conn
	s.uploadPeersMu.Unlock()

	// Start upload peer in background
	s.wg.Add(1)
	go s.serveUploadPeer(conn, handshake)

	return nil
}

// serveUploadPeer serves an upload peer connection
func (s *Session) serveUploadPeer(conn net.Conn, handshake *peer.Handshake) {
	defer s.wg.Done()
	defer conn.Close()

	addr := conn.RemoteAddr().String()
	defer func() {
		// Unregister upload peer
		s.uploadPeersMu.Lock()
		delete(s.uploadPeers, addr)
		s.uploadPeersMu.Unlock()
	}()

	s.logger.Debug("serving upload peer", "addr", addr)

	// Peer state
	choked := true       // We start with peer choked
	peerInterested := false

	// Message loop to handle upload requests
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(1 * time.Minute))

		// Read message
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			s.logger.Debug("upload peer disconnected", "addr", conn.RemoteAddr(), "error", err)
			return
		}

		if msg == nil {
			// Keep-alive
			continue
		}

		// Handle different message types
		switch msg.ID {
		case peer.MsgInterested:
			peerInterested = true
			s.logger.Info("upload: peer interested", "addr", addr)

			// Unchoke the peer (optimistic unchoking - we unchoke all interested peers)
			if choked {
				choked = false
				unchokeMsg := peer.Message{ID: peer.MsgUnchoke}
				if err := peer.WriteMessage(conn, unchokeMsg); err != nil {
					s.logger.Debug("failed to send unchoke", "error", err, "to", addr)
					return
				}
				s.logger.Info("upload: unchoked peer", "addr", addr)
			}

		case peer.MsgNotInterested:
			peerInterested = false
			s.logger.Debug("peer not interested", "addr", addr)

		case peer.MsgRequest:
			if len(msg.Payload) != 12 {
				continue
			}

			// Only serve requests if peer is unchoked
			if choked {
				s.logger.Debug("ignoring request from choked peer", "addr", addr)
				continue
			}

			// Only serve requests if peer is interested
			if !peerInterested {
				s.logger.Debug("ignoring request from uninterested peer", "addr", addr)
				continue
			}
			pieceIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
			length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))

			// Validate request
			if length > 16*1024 {
				s.logger.Warn("request length too large", "length", length, "from", addr)
				continue
			}

			// Check if we have the piece
			if !s.storage.HasPiece(pieceIndex) {
				s.logger.Debug("don't have requested piece", "piece", pieceIndex, "from", addr)
				continue
			}

			// Calculate piece length (last piece may be smaller)
			totalLength := s.meta.Info.TotalLength()
			pieceLength := s.meta.Info.PieceLength
			pieceOffset := int64(pieceIndex) * pieceLength
			pieceEnd := pieceOffset + pieceLength
			if pieceEnd > totalLength {
				pieceEnd = totalLength
			}
			actualPieceLength := int(pieceEnd - pieceOffset)

			// Validate request is within piece bounds
			if begin+length > actualPieceLength {
				s.logger.Warn("request out of bounds",
					"piece", pieceIndex,
					"begin", begin,
					"length", length,
					"piece_length", actualPieceLength,
					"from", addr)
				continue
			}

			// Read the entire piece from storage
			pieceBuf := make([]byte, actualPieceLength)
			n, err := s.storage.ReadPiece(pieceIndex, pieceBuf)
			if err != nil {
				s.logger.Warn("failed to read piece for upload",
					"piece", pieceIndex, "error", err, "from", addr)
				continue
			}

			if n != actualPieceLength {
				s.logger.Warn("incomplete piece read",
					"piece", pieceIndex,
					"expected", actualPieceLength,
					"got", n,
					"from", addr)
				continue
			}

			// Extract the requested block from the piece
			blockData := pieceBuf[begin : begin+length]

			// Send piece message
			payload := make([]byte, 8+length)
			binary.BigEndian.PutUint32(payload[0:4], uint32(pieceIndex))
			binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
			copy(payload[8:], blockData)

			pieceMsg := peer.Message{
				ID:      peer.MsgPiece,
				Payload: payload,
			}

			if err := peer.WriteMessage(conn, pieceMsg); err != nil {
				s.logger.Debug("failed to send piece", "error", err, "to", addr)
				return
			}

			// Track uploaded bytes
			s.AddUploaded(int64(length))

			s.logger.Info("upload: sent block",
				"piece", pieceIndex,
				"begin", begin,
				"length", length,
				"to", addr)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// VerifyData verifies all downloaded pieces
func (s *Session) VerifyData() (int, int, error) {
	s.logger.Info("verifying pieces", "info_hash", s.meta.InfoHashHex)

	numPieces := s.meta.Info.NumPieces()
	verified := 0
	failed := 0

	for i := 0; i < numPieces; i++ {
		valid, err := s.storage.VerifyPiece(i)
		if err != nil {
			s.logger.Warn("failed to verify piece", "index", i, "error", err)
			failed++
			continue
		}

		if valid {
			verified++
		} else {
			failed++
			// Clear the piece from bitfield since it's invalid
			s.storage.Bitfield().Clear(i)
			s.logger.Debug("piece verification failed", "index", i)
		}
	}

	s.logger.Info("verification complete",
		"verified", verified,
		"failed", failed,
		"total", numPieces)

	// Update stats after verification
	s.updateStats()

	return verified, failed, nil
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

// State returns the current state of the session (public API)
func (s *Session) State() SessionState {
	return s.getState()
}

func (s *Session) sendEvent(event SessionEvent) {
	select {
	case s.eventCh <- event:
	default:
		// Channel full, drop event
	}
}

func (s *Session) updateStats() {
	// Calculate total downloaded bytes (completed + in-progress)
	totalLength := s.meta.Info.TotalLength()
	completedPieces := s.storage.Bitfield().Count()
	totalPieces := s.storage.Bitfield().Len()

	// Bytes from completed pieces - properly handle last piece size
	completedBytes := int64(0)
	for i := 0; i < totalPieces; i++ {
		if s.storage.HasPiece(i) {
			// Calculate actual piece length (last piece may be smaller)
			pieceOffset := int64(i) * s.meta.Info.PieceLength
			pieceEnd := pieceOffset + s.meta.Info.PieceLength
			if pieceEnd > totalLength {
				pieceEnd = totalLength
			}
			actualPieceLength := pieceEnd - pieceOffset
			completedBytes += actualPieceLength
		}
	}

	// Add bytes from in-progress pieces
	inProgressBytes := int64(0)
	if s.pieceManager != nil {
		inProgressBytes = s.pieceManager.GetInProgressBytes()
	}

	totalDownloaded := completedBytes + inProgressBytes

	// Clamp to total length to avoid > 100%
	if totalDownloaded > totalLength {
		totalDownloaded = totalLength
	}

	// Calculate actual progress
	progress := float64(totalDownloaded) / float64(totalLength)
	if progress > 1.0 {
		progress = 1.0
	}

	// Calculate rates
	now := time.Now()
	s.statsMu.Lock()

	// Get uploaded bytes from session tracker
	totalUploaded := s.uploadedBytes

	downloadRate := float64(0)
	uploadRate := float64(0)

	if !s.lastStatsUpdate.IsZero() {
		elapsed := now.Sub(s.lastStatsUpdate).Seconds()
		if elapsed > 0 {
			downloadDelta := totalDownloaded - s.previousDownloaded
			uploadDelta := totalUploaded - s.previousUploaded

			// Clamp negative deltas to 0 (can happen during piece verification failures)
			if downloadDelta < 0 {
				downloadDelta = 0
			}
			if uploadDelta < 0 {
				uploadDelta = 0
			}

			downloadRate = float64(downloadDelta) / elapsed
			uploadRate = float64(uploadDelta) / elapsed
		}
	}

	// Count active peers (download + upload)
	s.downloadPeersMu.RLock()
	downloadPeerCount := len(s.downloadPeers)
	s.downloadPeersMu.RUnlock()

	s.uploadPeersMu.RLock()
	uploadPeerCount := len(s.uploadPeers)
	s.uploadPeersMu.RUnlock()

	peerCount := downloadPeerCount + uploadPeerCount

	// Update stats
	s.stats.Progress = progress
	s.stats.PiecesComplete = completedPieces
	s.stats.TotalPieces = totalPieces
	s.stats.Downloaded = totalDownloaded
	s.stats.Uploaded = totalUploaded
	s.stats.DownloadRate = downloadRate
	s.stats.UploadRate = uploadRate
	s.stats.Peers = peerCount

	// Save for next calculation
	s.previousDownloaded = totalDownloaded
	s.previousUploaded = totalUploaded
	s.lastStatsUpdate = now

	s.statsMu.Unlock()
}

// AddUploaded increments the uploaded bytes counter
func (s *Session) AddUploaded(bytes int64) {
	s.statsMu.Lock()
	s.uploadedBytes += bytes
	s.statsMu.Unlock()
}

func (s *Session) progressMonitor() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Diagnostic counter for periodic logging
	diagnosticCounter := 0

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateStats()

			// Periodic diagnostic logging (every 10 seconds)
			diagnosticCounter++
			if diagnosticCounter >= 10 {
				diagnosticCounter = 0
				stats := s.Stats()

				s.downloadPeersMu.RLock()
				downloadPeers := len(s.downloadPeers)
				s.downloadPeersMu.RUnlock()

				s.uploadPeersMu.RLock()
				uploadPeers := len(s.uploadPeers)
				s.uploadPeersMu.RUnlock()

				s.logger.Info("status",
					"progress", fmt.Sprintf("%.1f%%", stats.Progress*100),
					"download_peers", downloadPeers,
					"upload_peers", uploadPeers,
					"down_rate", fmt.Sprintf("%.1f KB/s", stats.DownloadRate/1024),
					"up_rate", fmt.Sprintf("%.1f KB/s", stats.UploadRate/1024))
			}

			// Send progress event if downloading or seeding
			state := s.getState()
			if state == StateDownloading || state == StateSeeding {
				stats := s.Stats()

				// Use appropriate event type based on state
				eventType := EventProgress
				if state == StateSeeding {
					eventType = EventSeeding
				}

				s.sendEvent(SessionEvent{
					Type:      eventType,
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

	// Retry more frequently if we have too few peers
	targetPeers := s.config.MaxPeers / 2 // Target at least half of max peers
	if targetPeers < 10 {
		targetPeers = 10 // Minimum target of 10 peers
	}
	shortInterval := 30 * time.Second
	longInterval := 3 * time.Minute // Optimal balance for peer discovery

	ticker := time.NewTicker(shortInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Count active download peers
			s.downloadPeersMu.RLock()
			activePeers := len(s.downloadPeers)
			s.downloadPeersMu.RUnlock()

			state := s.getState()

			// During download, be more aggressive about finding peers
			if state == StateDownloading {
				// If we have critically few peers, announce immediately
				if activePeers < 3 {
					s.logger.Debug("critically low peer count, announcing to tracker", "active_peers", activePeers)
					s.announceToTracker("")
					ticker.Reset(shortInterval)
				} else if activePeers < targetPeers {
					s.logger.Debug("below target peers, announcing to tracker", "active_peers", activePeers, "target", targetPeers)
					s.announceToTracker("")
					ticker.Reset(shortInterval)
				} else {
					// We have enough peers, but still announce periodically
					ticker.Reset(longInterval)
					s.logger.Debug("enough peers connected, using long announce interval", "active_peers", activePeers, "target", targetPeers)
				}
			} else if state == StateSeeding {
				// When seeding, announce less frequently
				ticker.Reset(longInterval)
			}
		}
	}
}

func (s *Session) dhtPeerDiscoveryLoop() {
	defer s.wg.Done()

	// Check if DHT is available
	if s.engine == nil || s.engine.dht == nil {
		s.logger.Debug("DHT not available, skipping DHT peer discovery")
		return
	}

	// Wait a bit before first DHT query to let initial tracker announce complete
	time.Sleep(10 * time.Second)

	ticker := time.NewTicker(30 * time.Second) // Query DHT every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			state := s.getState()
			if state != StateDownloading {
				continue
			}

			// Check current peer count
			s.downloadPeersMu.RLock()
			activePeers := len(s.downloadPeers)
			s.downloadPeersMu.RUnlock()

			targetPeers := s.config.MaxPeers / 2
			if targetPeers < 10 {
				targetPeers = 10
			}

			// Only query DHT if we need more peers
			if activePeers >= targetPeers {
				continue
			}

			s.logger.Debug("querying DHT for additional peers", "active_peers", activePeers, "target", targetPeers)

			// Get peers from DHT
			dhtPeers, err := s.engine.dht.GetPeers(s.meta.InfoHash)
			if err != nil {
				s.logger.Debug("DHT peer lookup failed", "error", err)
				continue
			}

			if len(dhtPeers) == 0 {
				s.logger.Debug("DHT returned no peers")
				continue
			}

			s.logger.Debug("DHT returned peers", "count", len(dhtPeers))

			// Try to connect to DHT peers
			maxAttempts := 20 // Try up to 20 DHT peers
			if len(dhtPeers) < maxAttempts {
				maxAttempts = len(dhtPeers)
			}

			for i := 0; i < maxAttempts; i++ {
				dhtPeer := dhtPeers[i]
				peerInfo := tracker.Peer{
					IP:   dhtPeer.IP,
					Port: uint16(dhtPeer.Port),
				}

				s.wg.Add(1)
				go s.connectToPeer(peerInfo)
			}
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
		NumWant:    100, // Request more peers to compensate for high failure rate
	}

	s.logger.Info("TRACKER ANNOUNCE",
		"event", event,
		"port", req.Port,
		"uploaded", stats.Uploaded,
		"downloaded", stats.Downloaded)

	resp, err := s.trackerManager.Announce(req)
	if err != nil {
		s.logger.Warn("tracker announce failed", "error", err)
		return
	}

	s.logger.Debug("tracker announce successful",
		"peers", len(resp.Peers),
		"interval", resp.Interval)

	// Connect to peers with concurrency limiting (like metadata fetch)
	// Try more peers to compensate for ~98% failure rate
	maxAttempts := 100
	if len(resp.Peers) < maxAttempts {
		maxAttempts = len(resp.Peers)
	}

	maxConcurrent := 20 // Limit concurrent connections to avoid resource exhaustion
	activeChan := make(chan struct{}, maxConcurrent)

	s.logger.Debug("attempting to connect to peers", "count", maxAttempts, "max_concurrent", maxConcurrent)

	// Launch connection attempts with rate limiting
	for i := 0; i < maxAttempts; i++ {
		select {
		case <-s.ctx.Done():
			return
		case activeChan <- struct{}{}: // Acquire slot
			peer := resp.Peers[i]
			s.wg.Add(1)
			go func(p tracker.Peer) {
				defer func() { <-activeChan }() // Release slot
				s.connectToPeer(p)
			}(peer)
		}
	}
}

// connectToPeer attempts to connect to a peer and start downloading
func (s *Session) connectToPeer(peerInfo tracker.Peer) {
	defer s.wg.Done()

	addr := fmt.Sprintf("%s:%d", peerInfo.IP, peerInfo.Port)

	// Check if we're already connected to this peer
	s.downloadPeersMu.RLock()
	_, exists := s.downloadPeers[addr]
	currentPeerCount := len(s.downloadPeers)
	s.downloadPeersMu.RUnlock()

	if exists {
		s.logger.Debug("already connected to peer, skipping", "addr", addr)
		return
	}

	// Check if we have too many connections (respect max_peers setting)
	if currentPeerCount >= s.config.MaxPeers {
		s.logger.Debug("max peers reached, skipping connection", "addr", addr, "current", currentPeerCount, "max", s.config.MaxPeers)
		return
	}

	s.logger.Debug("connecting to peer for download", "addr", addr, "current_peers", currentPeerCount)

	// Attempt connection with short timeout (2s like metadata fetch)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		s.logger.Debug("failed to connect to peer", "addr", addr, "error", err)
		return
	}

	// Set overall deadline for handshake (10s total like metadata fetch)
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	s.logger.Debug("connected to peer, sending handshake", "addr", addr)

	// Send our handshake
	ourHandshake := peer.NewHandshake(s.meta.InfoHash, tracker.GeneratePeerID())
	if _, err := conn.Write(ourHandshake.Serialize()); err != nil {
		s.logger.Debug("failed to send handshake", "addr", addr, "error", err)
		conn.Close()
		return
	}

	s.logger.Debug("handshake sent, waiting for remote handshake", "addr", addr)

	// Read remote handshake (covered by SetDeadline above)
	remoteHandshake, err := peer.ReadRemoteHandshake(conn)
	if err != nil {
		s.logger.Debug("failed to read handshake", "addr", addr, "error", err)
		conn.Close()
		return
	}

	// Verify info hash matches
	if remoteHandshake.InfoHash != s.meta.InfoHash {
		s.logger.Debug("info hash mismatch", "addr", addr)
		conn.Close()
		return
	}

	// Remove deadline for normal operation
	conn.SetDeadline(time.Time{})

	s.logger.Debug("handshake successful, starting download", "addr", addr, "peer_id", fmt.Sprintf("%x", remoteHandshake.PeerID[:8]))

	// Start download peer session
	s.wg.Add(1)
	go s.serveDownloadPeer(conn, remoteHandshake, addr)
}

// sendBlockRequest sends a request message for a block
func (s *Session) sendBlockRequest(conn net.Conn, pieceIndex, offset, length int) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(pieceIndex))
	binary.BigEndian.PutUint32(payload[4:8], uint32(offset))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))

	msg := peer.Message{
		ID:      peer.MsgRequest,
		Payload: payload,
	}

	return peer.WriteMessage(conn, msg)
}

// sendCancelMessage sends a cancel message for a block
func (s *Session) sendCancelMessage(conn net.Conn, pieceIndex, offset, length int) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(pieceIndex))
	binary.BigEndian.PutUint32(payload[4:8], uint32(offset))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))

	msg := peer.Message{
		ID:      peer.MsgCancel,
		Payload: payload,
	}

	return peer.WriteMessage(conn, msg)
}

// broadcastHave sends a Have message to all connected download peers
func (s *Session) broadcastHave(pieceIndex int) {
	// Create Have message
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(pieceIndex))

	msg := peer.Message{
		ID:      peer.MsgHave,
		Payload: payload,
	}

	// Get snapshot of connected peers
	s.downloadPeersMu.RLock()
	peers := make(map[string]net.Conn, len(s.downloadPeers))
	for addr, conn := range s.downloadPeers {
		peers[addr] = conn
	}
	s.downloadPeersMu.RUnlock()

	// Broadcast to all download peers
	for addr, conn := range peers {
		if err := peer.WriteMessage(conn, msg); err != nil {
			s.logger.Debug("failed to send Have message", "addr", addr, "piece", pieceIndex, "error", err)
			// Don't disconnect on broadcast failure - peer might be slow or disconnecting
		}
	}

	s.logger.Debug("broadcast Have to download peers", "piece", pieceIndex, "peers", len(peers))
}

// handleReceivedBlock processes a received piece block
func (s *Session) handleReceivedBlock(peerState *downloader.PeerState, pieceIndex, offset int, data []byte, conn net.Conn, addr string) error {
	// Get the piece download
	pd, exists := s.pieceManager.GetPiece(pieceIndex)
	if !exists {
		return fmt.Errorf("no active download for piece %d", pieceIndex)
	}

	// Write block to piece
	if err := pd.WriteBlock(offset, data); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	// Update statistics
	peerState.AddDownloaded(int64(len(data)))

	// Update session stats
	s.statsMu.Lock()
	s.stats.Downloaded += int64(len(data))
	s.statsMu.Unlock()

	// Complete this block in the request queue
	if peerState.RequestQueue != nil {
		peerState.RequestQueue.CompleteBlock(offset)
	}

	// In endgame mode, send cancel to other peers requesting this block
	if s.coordinator != nil && s.endgameTracker != nil && s.coordinator.IsEndgameActive() {
		otherPeers := s.endgameTracker.GetPeersForBlock(addr, pieceIndex, offset)
		if len(otherPeers) > 0 {
			s.logger.Debug("canceling duplicate block requests",
				"piece", pieceIndex,
				"offset", offset,
				"peers", len(otherPeers))

			// Get peer connections snapshot
			s.downloadPeersMu.RLock()
			for _, peerAddr := range otherPeers {
				if peerConn, exists := s.downloadPeers[peerAddr]; exists {
					if err := s.sendCancelMessage(peerConn, pieceIndex, offset, len(data)); err != nil {
						s.logger.Debug("failed to send cancel", "addr", peerAddr, "error", err)
					}
				}
			}
			s.downloadPeersMu.RUnlock()
		}

		// Remove this block from endgame tracking
		s.endgameTracker.RemoveRequest(addr, pieceIndex, offset)
	}

	s.logger.Debug("received block",
		"addr", addr,
		"piece", pieceIndex,
		"offset", offset,
		"length", len(data),
		"progress", pd.Progress())

	// Check if piece is complete
	if pd.IsComplete() {
		return s.completePiece(peerState, pieceIndex, addr)
	}

	// Fill pipeline with next block request
	if peerState.RequestQueue != nil {
		peerState.RequestQueue.FillPipeline(func(offset, length int) {
			if err := s.sendBlockRequest(conn, pieceIndex, offset, length); err != nil {
				s.logger.Debug("failed to send block request", "addr", addr, "error", err)
				return
			}

			// Track request in endgame mode
			if s.coordinator != nil && s.endgameTracker != nil && s.coordinator.IsEndgameActive() {
				s.endgameTracker.AddRequest(addr, pieceIndex, offset)
			}
		})
	}

	return nil
}

// completePiece verifies and completes a downloaded piece
func (s *Session) completePiece(peerState *downloader.PeerState, pieceIndex int, addr string) error {
	// Complete the piece in piece manager (writes to storage and verifies)
	// Returns error if write fails or verification fails
	err := s.pieceManager.CompletePiece(pieceIndex)
	if err != nil {
		s.logger.Warn("piece completion failed", "piece", pieceIndex, "addr", addr, "error", err)

		// Track hash verification failure
		peerState.IncrementHashFailures()
		hashFailures := peerState.GetHashFailures()
		s.logger.Warn("peer hash failure", "addr", addr, "failures", hashFailures)

		// Ban peer if too many failures
		if peerState.ShouldBanPeer() {
			s.logger.Warn("banning peer due to repeated hash failures", "addr", addr, "failures", hashFailures)
			// Mark for disconnection by returning a special error
			s.coordinator.FailPiece(pieceIndex)
			peerState.CurrentPiece = nil
			peerState.RequestQueue = nil
			return fmt.Errorf("peer banned: too many hash failures (%d)", hashFailures)
		}

		s.coordinator.FailPiece(pieceIndex)
		peerState.CurrentPiece = nil
		peerState.RequestQueue = nil
		return fmt.Errorf("failed to complete piece: %w", err)
	}

	// Piece is valid and written to storage
	s.logger.Debug("piece completed", "piece", pieceIndex, "addr", addr)

	// Notify coordinator
	s.coordinator.NotifyPieceComplete(pieceIndex)

	// Clear peer state
	peerState.CurrentPiece = nil
	peerState.RequestQueue = nil

	// Broadcast Have to all peers
	s.broadcastHave(pieceIndex)

	// Send completion event
	s.sendEvent(SessionEvent{
		Type:      EventPieceComplete,
		InfoHash:  fmt.Sprintf("%x", s.meta.InfoHash),
		Progress:  s.storage.GetProgress(),
		Stats:     s.Stats(),
		Timestamp: time.Now(),
	})

	// Check if torrent is complete
	if s.storage.IsComplete() {
		s.logger.Debug("download complete!")

		// Transition to seeding state
		s.setState(StateSeeding)
		now := time.Now()
		s.completedAt = &now

		// Send complete event
		s.sendEvent(SessionEvent{
			Type:      EventComplete,
			InfoHash:  fmt.Sprintf("%x", s.meta.InfoHash),
			Progress:  1.0,
			Stats:     s.Stats(),
			Timestamp: time.Now(),
		})
	}

	return nil
}

// startDownloading initiates downloading a piece from a peer
func (s *Session) startDownloading(peerState *downloader.PeerState, peerBitfield *storage.Bitfield, conn net.Conn, addr string) error {
	// Don't start if already downloading
	if !peerState.IsIdle() {
		return nil
	}

	// Don't start new downloads if paused
	if s.getState() == StatePaused {
		return nil
	}

	// Check if coordinator exists
	if s.coordinator == nil {
		return fmt.Errorf("coordinator is nil")
	}

	// Request piece from coordinator
	assignment := s.coordinator.RequestPiece(addr, peerBitfield)
	if !assignment.Found {
		// No piece available
		return nil
	}

	pieceIndex := assignment.PieceIndex

	// Start the piece download
	pd, err := s.pieceManager.StartPiece(pieceIndex)
	if err != nil {
		s.logger.Error("failed to start piece", "piece", pieceIndex, "error", err)
		s.coordinator.FailPiece(pieceIndex)
		return fmt.Errorf("failed to start piece: %w", err)
	}

	// Set peer state
	peerState.CurrentPiece = &pieceIndex

	// Create request queue with max pipeline of 20 (was 5, way too low)
	// With 16KB blocks, 20 requests = 320KB in flight per peer
	// This keeps the pipeline full and maximizes throughput
	rq := downloader.NewRequestQueue(pieceIndex, pd, 20)
	peerState.RequestQueue = rq

	s.logger.Info("download: starting piece",
		"addr", addr,
		"piece", pieceIndex,
		"size", pd.Length)

	// Fill the pipeline with initial requests
	rq.FillPipeline(func(offset, length int) {
		if err := s.sendBlockRequest(conn, pieceIndex, offset, length); err != nil {
			s.logger.Debug("failed to send block request", "addr", addr, "error", err)
			return
		}

		// Track request in endgame mode
		if s.coordinator != nil && s.endgameTracker != nil && s.coordinator.IsEndgameActive() {
			s.endgameTracker.AddRequest(addr, pieceIndex, offset)
		}
	})

	return nil
}

// serveDownloadPeer handles a download peer session
func (s *Session) serveDownloadPeer(conn net.Conn, handshake *peer.Handshake, addr string) {
	defer s.wg.Done()
	defer conn.Close()

	s.logger.Debug("serving download peer", "addr", addr)

	// Create peer state
	peerState := downloader.NewPeerState(addr, s.meta.Info.NumPieces())

	// Register connection for Have broadcasts
	s.downloadPeersMu.Lock()
	s.downloadPeers[addr] = conn
	s.downloadPeersMu.Unlock()

	// Register with coordinator (skip if nil - happens when download is complete)
	if s.coordinator == nil {
		s.logger.Debug("coordinator is nil, skipping download peer", "addr", addr)
		return
	}

	s.coordinator.RegisterPeer(peerState)
	defer func() {
		s.coordinator.UnregisterPeer(addr)
		// Fail any in-progress piece
		if peerState.CurrentPiece != nil {
			s.coordinator.FailPiece(*peerState.CurrentPiece)
		}
		// Unregister connection
		s.downloadPeersMu.Lock()
		delete(s.downloadPeers, addr)
		s.downloadPeersMu.Unlock()
		// Clean up endgame tracker
		s.endgameTracker.RemovePeer(addr)
	}()

	// Send bitfield message (required by BitTorrent spec after handshake)
	bitfield := s.storage.Bitfield()
	bitfieldMsg := peer.Message{
		ID:      peer.MsgBitfield,
		Payload: bitfield.GetBytes(),
	}
	if err := peer.WriteMessage(conn, bitfieldMsg); err != nil {
		s.logger.Debug("failed to send bitfield", "addr", addr, "error", err)
		return
	}

	// Send interested message
	interestedMsg := peer.Message{ID: peer.MsgInterested}
	if err := peer.WriteMessage(conn, interestedMsg); err != nil {
		s.logger.Debug("failed to send interested", "addr", addr, "error", err)
		return
	}

	// Track if peer has unchoked us
	unchoked := false
	chokedSince := time.Now() // Track how long we've been choked

	// Timeout checker - runs frequently to ensure idle peers quickly grab new pieces
	timeoutTicker := time.NewTicker(1 * time.Second)
	defer timeoutTicker.Stop()

	// Message loop
	for {
		select {
		case <-s.ctx.Done():
			return

		case <-timeoutTicker.C:
			// Update peer download rate
			peerState.UpdateRate()

			// Check if peer is too slow
			if peerState.IsTooSlow() {
				s.logger.Info("disconnecting: peer too slow",
					"addr", addr,
					"rate", peerState.GetDownloadRate())
				return
			}

			// Check if peer has kept us choked for too long
			if !unchoked && time.Since(chokedSince) > 15*time.Second {
				s.logger.Debug("disconnecting: peer kept us choked too long",
					"addr", addr,
					"duration", time.Since(chokedSince))
				return
			}
			// Check for timed out block requests
			if peerState.RequestQueue != nil {
				timedOut := peerState.RequestQueue.CheckTimeouts()
				if len(timedOut) > 0 {
					s.logger.Debug("block requests timed out",
						"addr", addr,
						"piece", *peerState.CurrentPiece,
						"count", len(timedOut))

					// Fail the piece and let coordinator reassign
					if peerState.CurrentPiece != nil {
						s.coordinator.FailPiece(*peerState.CurrentPiece)
						s.pieceManager.FailPiece(*peerState.CurrentPiece)
						peerState.CurrentPiece = nil
						peerState.RequestQueue = nil
					}

					// Try to start a new piece if unchoked
					if unchoked {
						if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
							s.logger.Debug("failed to start downloading after timeout", "addr", addr, "error", err)
						}
					}
				}
			}

			// Also retry piece assignment for idle peers (important!)
			// This ensures peers don't sit idle when pieces become available
			if unchoked && peerState.IsIdle() && s.getState() == StateDownloading {
				s.logger.Debug("idle peer retry", "addr", addr, "unchoked", unchoked)
				if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
					s.logger.Debug("retry failed", "addr", addr, "error", err)
				}
			}

			// Log peer status for diagnostics
			if !unchoked && peerState.IsIdle() {
				s.logger.Debug("peer idle and choked", "addr", addr)
			}
			continue

		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(1 * time.Minute))

		// Read message
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			s.logger.Debug("download peer disconnected", "addr", addr, "error", err)
			return
		}

		if msg == nil {
			// Keep-alive
			continue
		}

		// Handle messages
		switch msg.ID {
		case peer.MsgChoke:
			s.logger.Info("download: peer choked us", "addr", addr)
			peerState.SetPeerChoking(true)
			unchoked = false
			chokedSince = time.Now() // Reset choke timer

		case peer.MsgUnchoke:
			s.logger.Info("download: peer unchoked us", "addr", addr)
			peerState.SetPeerChoking(false)
			unchoked = true

			// Start downloading if we don't have a piece yet
			if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
				s.logger.Debug("failed to start downloading", "addr", addr, "error", err)
			}

		case peer.MsgHave:
			if len(msg.Payload) != 4 {
				s.logger.Debug("invalid Have message", "addr", addr)
				continue
			}

			pieceIndex := int(binary.BigEndian.Uint32(msg.Payload))
			s.logger.Debug("peer has piece", "addr", addr, "piece", pieceIndex)

			// Update peer's bitfield
			peerState.Bitfield.Set(pieceIndex)

			// If we're idle and unchoked, try to start downloading
			if unchoked && peerState.IsIdle() {
				if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
					s.logger.Debug("failed to start downloading", "addr", addr, "error", err)
				}
			}

		case peer.MsgBitfield:
			s.logger.Debug("received bitfield", "addr", addr, "size", len(msg.Payload))

			// Parse and store peer's bitfield
			bf := storage.NewBitfieldFromBytes(msg.Payload, s.meta.Info.NumPieces())
			peerState.Bitfield = bf

			// If we're idle and unchoked, try to start downloading
			if unchoked && peerState.IsIdle() {
				if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
					s.logger.Debug("failed to start downloading", "addr", addr, "error", err)
				}
			}

		case peer.MsgPiece:
			// Parse piece message: [index:4][offset:4][data:...]
			if len(msg.Payload) < 8 {
				s.logger.Debug("invalid Piece message", "addr", addr)
				continue
			}

			pieceIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
			offset := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
			data := msg.Payload[8:]

			// Handle the received block
			if err := s.handleReceivedBlock(peerState, pieceIndex, offset, data, conn, addr); err != nil {
				s.logger.Debug("failed to handle block", "addr", addr, "error", err)
				// On error, fail the piece
				if peerState.CurrentPiece != nil {
					s.coordinator.FailPiece(*peerState.CurrentPiece)
					peerState.CurrentPiece = nil
					peerState.RequestQueue = nil
				}
				// If peer is banned, disconnect
				if strings.Contains(err.Error(), "peer banned") {
					s.logger.Warn("disconnecting banned peer", "addr", addr)
					return // Exit peer loop to close connection
				}
				// Immediately try to get a new piece instead of waiting for timeout
				if peerState.IsIdle() && unchoked {
					if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
						s.logger.Debug("failed to start downloading after error", "addr", addr, "error", err)
					}
				}
				continue
			}

			// If piece completed successfully, start next piece
			if peerState.IsIdle() && unchoked {
				if err := s.startDownloading(peerState, peerState.GetBitfield(), conn, addr); err != nil {
					s.logger.Debug("failed to start downloading", "addr", addr, "error", err)
				}
			}

		case peer.MsgRequest:
			// Ignore download requests in download peer (handled by upload workers)
			continue

		case peer.MsgCancel:
			// Handle cancel message (endgame mode)
			if len(msg.Payload) != 12 {
				s.logger.Debug("invalid Cancel message", "addr", addr)
				continue
			}

			pieceIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
			offset := int(binary.BigEndian.Uint32(msg.Payload[4:8]))

			s.logger.Debug("received cancel", "addr", addr, "piece", pieceIndex, "offset", offset)

			// Remove from request queue if active
			if peerState.RequestQueue != nil && peerState.CurrentPiece != nil {
				if *peerState.CurrentPiece == pieceIndex {
					peerState.RequestQueue.CompleteBlock(offset)
					s.logger.Debug("removed canceled block from queue", "piece", pieceIndex, "offset", offset)
				}
			}
		}
	}
}
