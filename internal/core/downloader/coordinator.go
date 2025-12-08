package downloader

import (
	"context"
	"log/slog"
	"sync"

	"github.com/revtheundead/revtorrent/internal/core/piece"
	"github.com/revtheundead/revtorrent/internal/core/storage"
	"github.com/revtheundead/revtorrent/internal/core/torrent"
)

// PieceStatus represents the status of a piece
type PieceStatus int

const (
	PieceNeeded PieceStatus = iota
	PieceDownloading
	PieceComplete
)

// PieceRequest represents a request for a piece assignment
type PieceRequest struct {
	PeerAddr     string
	PeerBitfield *storage.Bitfield
	ResponseChan chan PieceAssignment
}

// PieceAssignment represents the result of a piece request
type PieceAssignment struct {
	PieceIndex int
	Found      bool // false if no pieces available
}

// DownloadCoordinator coordinates piece downloads across multiple peers
type DownloadCoordinator struct {
	// Dependencies
	storage      *storage.FileStorage
	pieceManager *piece.Manager
	torrentInfo  *torrent.Info
	logger       *slog.Logger

	// Piece selection strategy
	selector PieceSelector

	// State tracking
	peers             map[string]*PeerState // addr -> peer state
	pieceStates       map[int]PieceStatus   // piece index -> status
	pieceAvailability map[int]int           // piece index -> count of peers having it

	// Coordination channels
	pieceRequests    chan PieceRequest
	pieceCompleted   chan int
	pieceFailed      chan int
	peerRegistered   chan *PeerState
	peerDisconnected chan string

	// Endgame mode (future)
	endgameActive    bool
	endgameThreshold float64 // 0.95

	// Synchronization
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCoordinator creates a new download coordinator
func NewCoordinator(
	storage *storage.FileStorage,
	pieceManager *piece.Manager,
	torrentInfo *torrent.Info,
	logger *slog.Logger,
) *DownloadCoordinator {
	if logger == nil {
		logger = slog.Default()
	}

	return &DownloadCoordinator{
		storage:           storage,
		pieceManager:      pieceManager,
		torrentInfo:       torrentInfo,
		logger:            logger,
		selector:          NewRandomFirstSelector(),
		peers:             make(map[string]*PeerState),
		pieceStates:       make(map[int]PieceStatus),
		pieceAvailability: make(map[int]int),
		pieceRequests:     make(chan PieceRequest, 10),
		pieceCompleted:    make(chan int, 10),
		pieceFailed:       make(chan int, 10),
		peerRegistered:    make(chan *PeerState, 10),
		peerDisconnected:  make(chan string, 10),
		endgameActive:     false,
		endgameThreshold:  0.95,
	}
}

// Start starts the coordinator
func (c *DownloadCoordinator) Start(ctx context.Context, wg *sync.WaitGroup) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	defer wg.Done()
	defer c.cancel()

	c.logger.Info("download coordinator started")

	// Initialize piece states
	c.initializePieceStates()

	// Run coordination loop
	c.coordinationLoop()

	c.logger.Info("download coordinator stopped")
}

// Stop stops the coordinator
func (c *DownloadCoordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

// RegisterPeer registers a new peer
func (c *DownloadCoordinator) RegisterPeer(peer *PeerState) {
	select {
	case c.peerRegistered <- peer:
	case <-c.ctx.Done():
	}
}

// UnregisterPeer unregisters a peer
func (c *DownloadCoordinator) UnregisterPeer(addr string) {
	select {
	case c.peerDisconnected <- addr:
	case <-c.ctx.Done():
	}
}

// RequestPiece requests a piece assignment for a peer
func (c *DownloadCoordinator) RequestPiece(peerAddr string, peerBitfield *storage.Bitfield) PieceAssignment {
	respChan := make(chan PieceAssignment, 1)

	req := PieceRequest{
		PeerAddr:     peerAddr,
		PeerBitfield: peerBitfield,
		ResponseChan: respChan,
	}

	select {
	case c.pieceRequests <- req:
		// Wait for response
		select {
		case assignment := <-respChan:
			return assignment
		case <-c.ctx.Done():
			return PieceAssignment{Found: false}
		}
	case <-c.ctx.Done():
		return PieceAssignment{Found: false}
	}
}

// NotifyPieceComplete notifies that a piece has been completed
func (c *DownloadCoordinator) NotifyPieceComplete(pieceIndex int) {
	select {
	case c.pieceCompleted <- pieceIndex:
	case <-c.ctx.Done():
	}
}

// FailPiece marks a piece as failed and available for retry
func (c *DownloadCoordinator) FailPiece(pieceIndex int) {
	select {
	case c.pieceFailed <- pieceIndex:
	case <-c.ctx.Done():
	}
}

// coordinationLoop is the main event loop for coordination
func (c *DownloadCoordinator) coordinationLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case req := <-c.pieceRequests:
			c.handlePieceRequest(req)

		case pieceIndex := <-c.pieceCompleted:
			c.handlePieceComplete(pieceIndex)

		case pieceIndex := <-c.pieceFailed:
			c.handlePieceFailed(pieceIndex)

		case peer := <-c.peerRegistered:
			c.handlePeerJoin(peer)

		case addr := <-c.peerDisconnected:
			c.handlePeerLeave(addr)
		}
	}
}

// initializePieceStates initializes the piece status map
func (c *DownloadCoordinator) initializePieceStates() {
	c.mu.Lock()
	defer c.mu.Unlock()

	numPieces := c.torrentInfo.NumPieces()
	bitfield := c.storage.Bitfield()

	for i := 0; i < numPieces; i++ {
		if bitfield.Has(i) {
			c.pieceStates[i] = PieceComplete
		} else {
			c.pieceStates[i] = PieceNeeded
		}
	}
}

// handlePieceRequest handles a piece assignment request
func (c *DownloadCoordinator) handlePieceRequest(req PieceRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build in-progress map (exclude in-progress in normal mode, allow in endgame)
	inProgress := make(map[int]bool)
	if !c.endgameActive {
		for pieceIndex, status := range c.pieceStates {
			if status == PieceDownloading {
				inProgress[pieceIndex] = true
			}
		}
	}

	// Inject availability into RarestFirstSelector if applicable
	if rarestSelector, ok := c.selector.(*RarestFirstSelector); ok {
		rarestSelector.SetAvailability(c.pieceAvailability)
	}

	// Select piece using strategy
	pieceIndex := c.selector.SelectPiece(
		req.PeerBitfield,
		c.storage.Bitfield(),
		inProgress,
	)

	if pieceIndex == -1 {
		// No piece available
		req.ResponseChan <- PieceAssignment{Found: false}
		return
	}

	// Mark piece as downloading (may already be downloading in endgame mode)
	c.pieceStates[pieceIndex] = PieceDownloading

	if c.endgameActive {
		c.logger.Debug("assigned piece to peer (endgame)",
			"piece", pieceIndex,
			"peer", req.PeerAddr)
	} else {
		c.logger.Debug("assigned piece to peer",
			"piece", pieceIndex,
			"peer", req.PeerAddr)
	}

	req.ResponseChan <- PieceAssignment{
		PieceIndex: pieceIndex,
		Found:      true,
	}
}

// handlePieceComplete handles piece completion
func (c *DownloadCoordinator) handlePieceComplete(pieceIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pieceStates[pieceIndex] = PieceComplete

	c.logger.Debug("piece marked complete", "piece", pieceIndex)

	// Check if we should activate endgame mode
	c.checkEndgameMode()
}

// checkEndgameMode checks if we should activate endgame mode
// Must be called with c.mu held
func (c *DownloadCoordinator) checkEndgameMode() {
	if c.endgameActive {
		return // Already in endgame
	}

	// Count pieces
	complete := 0
	total := len(c.pieceStates)

	for _, status := range c.pieceStates {
		if status == PieceComplete {
			complete++
		}
	}

	// Calculate progress
	progress := float64(complete) / float64(total)

	// Activate endgame if we're at threshold
	if progress >= c.endgameThreshold {
		c.endgameActive = true
		c.logger.Debug("endgame mode activated",
			"progress", progress,
			"complete", complete,
			"total", total)
	}
}

// handlePieceFailed handles piece failure
func (c *DownloadCoordinator) handlePieceFailed(pieceIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset to needed so it can be reassigned
	c.pieceStates[pieceIndex] = PieceNeeded

	c.logger.Debug("piece marked as failed, will retry", "piece", pieceIndex)
}

// handlePeerJoin handles a new peer joining
func (c *DownloadCoordinator) handlePeerJoin(peer *PeerState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peers[peer.Addr] = peer

	// Update piece availability counts
	numPieces := c.torrentInfo.NumPieces()
	for i := 0; i < numPieces; i++ {
		if peer.Bitfield.Has(i) {
			c.pieceAvailability[i]++
		}
	}

	c.logger.Debug("peer registered", "addr", peer.Addr)
}

// handlePeerLeave handles a peer leaving
func (c *DownloadCoordinator) handlePeerLeave(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	peer, exists := c.peers[addr]
	if !exists {
		return
	}

	// Update piece availability counts
	numPieces := c.torrentInfo.NumPieces()
	for i := 0; i < numPieces; i++ {
		if peer.Bitfield.Has(i) {
			c.pieceAvailability[i]--
			if c.pieceAvailability[i] <= 0 {
				delete(c.pieceAvailability, i)
			}
		}
	}

	delete(c.peers, addr)

	c.logger.Debug("peer unregistered", "addr", addr)
}

// IsEndgameActive returns whether endgame mode is active
func (c *DownloadCoordinator) IsEndgameActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.endgameActive
}

// GetPieceAvailability returns a copy of piece availability counts
func (c *DownloadCoordinator) GetPieceAvailability() map[int]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	availability := make(map[int]int, len(c.pieceAvailability))
	for piece, count := range c.pieceAvailability {
		availability[piece] = count
	}
	return availability
}
