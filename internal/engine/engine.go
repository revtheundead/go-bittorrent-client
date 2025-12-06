package engine

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/revtheundead/revtorrent/internal/config"
	"github.com/revtheundead/revtorrent/internal/dht"
)

// Engine coordinates all BitTorrent operations for the client
type Engine struct {
	config *config.Config
	logger *slog.Logger

	// DHT for peer discovery
	dht *dht.DHT

	// Listener for incoming peer connections
	listener net.Listener

	// Sessions for active torrents (keyed by info hash hex)
	sessions   map[string]*Session
	sessionsMu sync.RWMutex

	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Running state
	running bool
	mu      sync.Mutex
}

// NewEngine creates a new BitTorrent engine
func NewEngine(cfg *config.Config, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	engine := &Engine{
		config:   cfg,
		logger:   logger,
		sessions: make(map[string]*Session),
		ctx:      ctx,
		cancel:   cancel,
	}

	return engine, nil
}

// Start starts the engine (DHT, listener, etc.)
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return fmt.Errorf("engine already running")
	}

	// Start DHT if enabled
	if e.config.DHTEnabled {
		e.logger.Info("starting DHT node", "port", e.config.ListenPort)
		dhtNode, err := dht.NewDHT(e.config.ListenPort, e.logger)
		if err != nil {
			return fmt.Errorf("failed to create DHT node: %w", err)
		}

		if err := dhtNode.Start(); err != nil {
			return fmt.Errorf("failed to start DHT node: %w", err)
		}

		if err := dhtNode.Bootstrap(dht.DefaultBootstrapNodes()); err != nil {
			e.logger.Warn("DHT bootstrap failed", "error", err)
			// Don't fail - DHT can still work from incoming queries
		}

		e.dht = dhtNode
	}

	// Start peer listener
	addr := fmt.Sprintf(":%d", e.config.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener on %s: %w", addr, err)
	}

	e.listener = listener
	e.logger.Info("listening for incoming connections", "address", listener.Addr())

	// Start accepting connections in background
	e.wg.Add(1)
	go e.acceptLoop()

	e.running = true
	return nil
}

// Stop stops the engine and all sessions
func (e *Engine) Stop() error {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = false
	e.mu.Unlock()

	e.logger.Info("stopping engine")

	// Signal shutdown
	e.cancel()

	// Close listener
	if e.listener != nil {
		e.listener.Close()
	}

	// Stop all sessions
	e.sessionsMu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.sessionsMu.Unlock()

	for _, session := range sessions {
		if err := session.Stop(); err != nil {
			e.logger.Warn("failed to stop session", "info_hash", session.InfoHashHex(), "error", err)
		}
	}

	// Stop DHT
	if e.dht != nil {
		e.dht.Stop()
	}

	// Wait for goroutines
	e.wg.Wait()

	e.logger.Info("engine stopped")
	return nil
}

// AddSession adds a new torrent session
func (e *Engine) AddSession(session *Session) error {
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()

	infoHashHex := session.InfoHashHex()
	if _, exists := e.sessions[infoHashHex]; exists {
		return fmt.Errorf("session already exists for info hash %s", infoHashHex)
	}

	e.sessions[infoHashHex] = session
	e.logger.Info("session added", "info_hash", infoHashHex, "name", session.Name())

	return nil
}

// RemoveSession removes a torrent session
func (e *Engine) RemoveSession(infoHashHex string) error {
	e.sessionsMu.Lock()
	session, exists := e.sessions[infoHashHex]
	if !exists {
		e.sessionsMu.Unlock()
		return fmt.Errorf("session not found for info hash %s", infoHashHex)
	}
	delete(e.sessions, infoHashHex)
	e.sessionsMu.Unlock()

	// Stop the session
	if err := session.Stop(); err != nil {
		return fmt.Errorf("failed to stop session: %w", err)
	}

	e.logger.Info("session removed", "info_hash", infoHashHex)
	return nil
}

// GetSession retrieves a session by info hash
func (e *Engine) GetSession(infoHashHex string) (*Session, bool) {
	e.sessionsMu.RLock()
	defer e.sessionsMu.RUnlock()

	session, ok := e.sessions[infoHashHex]
	return session, ok
}

// Context returns the engine's context
func (e *Engine) Context() context.Context {
	return e.ctx
}

// DHT returns the DHT node (may be nil if disabled)
func (e *Engine) DHT() *dht.DHT {
	return e.dht
}

// acceptLoop accepts incoming peer connections
func (e *Engine) acceptLoop() {
	defer e.wg.Done()

	for {
		conn, err := e.listener.Accept()
		if err != nil {
			select {
			case <-e.ctx.Done():
				// Shutting down
				return
			default:
				e.logger.Warn("failed to accept connection", "error", err)
				continue
			}
		}

		// Handle connection in background
		e.wg.Add(1)
		go e.handleIncomingConnection(conn)
	}
}

// handleIncomingConnection handles an incoming peer connection
func (e *Engine) handleIncomingConnection(conn net.Conn) {
	defer e.wg.Done()
	defer conn.Close()

	// TODO: Implement incoming peer handshake and routing to appropriate session
	// For now, just close the connection
	e.logger.Debug("incoming connection (handshake not yet implemented)",
		"addr", conn.RemoteAddr())
}
