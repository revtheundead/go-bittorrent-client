package shutdown

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Handler is a function that performs cleanup
type Handler func(ctx context.Context) error

// Manager manages graceful shutdown
type Manager struct {
	handlers     []handlerEntry
	logger       *slog.Logger
	timeout      time.Duration
	signalCh     chan os.Signal
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	mu           sync.RWMutex
}

type handlerEntry struct {
	name    string
	handler Handler
}

// NewManager creates a new shutdown manager
func NewManager(logger *slog.Logger, timeout time.Duration) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &Manager{
		handlers:   make([]handlerEntry, 0),
		logger:     logger,
		timeout:    timeout,
		signalCh:   make(chan os.Signal, 1),
		shutdownCh: make(chan struct{}),
	}
}

// RegisterHandler registers a shutdown handler
// Handlers are executed in reverse order of registration (LIFO)
func (m *Manager) RegisterHandler(name string, handler Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.handlers = append(m.handlers, handlerEntry{
		name:    name,
		handler: handler,
	})

	m.logger.Debug("registered shutdown handler", "name", name)
}

// Start starts listening for shutdown signals
func (m *Manager) Start() {
	signal.Notify(m.signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-m.signalCh
		m.logger.Info("received shutdown signal", "signal", sig)
		m.Shutdown()
	}()
}

// Shutdown initiates graceful shutdown
func (m *Manager) Shutdown() {
	m.shutdownOnce.Do(func() {
		close(m.shutdownCh)
		m.executeHandlers()
	})
}

// Wait blocks until shutdown is initiated
func (m *Manager) Wait() {
	<-m.shutdownCh
}

// Done returns a channel that's closed when shutdown is initiated
func (m *Manager) Done() <-chan struct{} {
	return m.shutdownCh
}

// executeHandlers executes all registered handlers in reverse order
func (m *Manager) executeHandlers() {
	m.mu.RLock()
	handlers := make([]handlerEntry, len(m.handlers))
	copy(handlers, m.handlers)
	m.mu.RUnlock()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	m.logger.Info("starting graceful shutdown", "handlers", len(handlers), "timeout", m.timeout)

	// Execute handlers in reverse order (LIFO)
	for i := len(handlers) - 1; i >= 0; i-- {
		entry := handlers[i]

		m.logger.Debug("executing shutdown handler", "name", entry.name)

		if err := m.executeHandler(ctx, entry); err != nil {
			m.logger.Error("shutdown handler failed",
				"name", entry.name,
				"error", err)
		} else {
			m.logger.Debug("shutdown handler completed", "name", entry.name)
		}
	}

	m.logger.Info("graceful shutdown complete")
}

// executeHandler executes a single handler with timeout
func (m *Manager) executeHandler(ctx context.Context, entry handlerEntry) error {
	done := make(chan error, 1)

	go func() {
		done <- entry.handler(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("shutdown handler timed out: %s", entry.name)
	}
}

// Stop stops listening for signals (for testing)
func (m *Manager) Stop() {
	signal.Stop(m.signalCh)
	close(m.signalCh)
}

// Context returns a context that's cancelled when shutdown is initiated
func (m *Manager) Context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-m.shutdownCh
		cancel()
	}()

	return ctx
}

// WithShutdown wraps a context to be cancelled on shutdown
func (m *Manager) WithShutdown(ctx context.Context) context.Context {
	newCtx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-m.shutdownCh:
			cancel()
		case <-ctx.Done():
			cancel()
		}
	}()

	return newCtx
}
