package bittorrent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/config"
	"github.com/revtheundead/revtorrent/internal/core/torrent"
	"github.com/revtheundead/revtorrent/internal/engine"
	"github.com/revtheundead/revtorrent/internal/protocol/magnet"
)

// ============================================================================
// PUBLIC API LAYER - Integration Layer
// ============================================================================
// This file defines the public API for the revTorrent library.
// Individual components (DHT, tracker manager, storage, peer protocol, etc.)
// are fully implemented in internal/. This layer wires them together
// through the engine coordinator.
// ============================================================================

// Client is the main entry point for the BitTorrent client library
type Client struct {
	config   *config.Config
	logger   *slog.Logger
	engine   *engine.Engine
	torrents map[string]*Torrent // keyed by info hash (hex)
	mu       sync.RWMutex
}

// NewClient creates a new BitTorrent client with the given options
func NewClient(opts ...Option) (*Client, error) {
	cfg := config.Default()

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Create logger with configured log level
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))

	// Create engine
	eng, err := engine.NewEngine(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	// Start engine
	if err := eng.Start(); err != nil {
		return nil, fmt.Errorf("failed to start engine: %w", err)
	}

	client := &Client{
		config:   cfg,
		logger:   logger,
		engine:   eng,
		torrents: make(map[string]*Torrent),
	}

	return client, nil
}

// AddTorrent adds a torrent from a .torrent file
func (c *Client) AddTorrent(path string) (*Torrent, error) {
	// Read torrent file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read torrent file: %w", err)
	}

	return c.AddTorrentFromBytes(data)
}

// AddMagnet adds a torrent from a magnet link
func (c *Client) AddMagnet(uri string) (*Torrent, error) {
	// Parse magnet link
	mag, err := magnet.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse magnet link: %w", err)
	}

	// Check if already exists
	infoHashHex := mag.InfoHashHex
	c.mu.Lock()
	if existing, ok := c.torrents[infoHashHex]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()

	// Fetch metadata from DHT/peers using BEP 9
	c.logger.Info("fetching metadata for magnet link", "info_hash", infoHashHex)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	metadataBytes, err := c.engine.FetchMetadata(ctx, mag.InfoHash, mag.Trackers)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	c.logger.Info("metadata fetched successfully", "size", len(metadataBytes))

	// Parse the metadata as a torrent info dictionary
	meta, err := torrent.ParseInfo(metadataBytes, mag.InfoHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Add trackers from magnet link (skip empty URLs)
	meta.AnnounceList = make([][]string, 0)
	for _, tracker := range mag.Trackers {
		if tracker != "" {
			meta.AnnounceList = append(meta.AnnounceList, []string{tracker})
		}
	}

	// If no trackers available, add default public trackers
	if len(meta.AnnounceList) == 0 {
		defaultTrackers := []string{
			"udp://tracker.opentrackr.org:1337/announce",
			"udp://open.stealth.si:80/announce",
			"udp://tracker.torrent.eu.org:451/announce",
			"udp://tracker.bittor.pw:1337/announce",
			"udp://public.popcorn-tracker.org:6969/announce",
		}
		c.logger.Info("no trackers in metadata, using defaults", "count", len(defaultTrackers))
		for _, tracker := range defaultTrackers {
			meta.AnnounceList = append(meta.AnnounceList, []string{tracker})
		}
	}

	// Create session
	session, err := engine.NewSession(meta, c.config, c.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// Add session to engine
	if err := c.engine.AddSession(session); err != nil {
		return nil, fmt.Errorf("failed to add session to engine: %w", err)
	}

	// Wrap in Torrent and store
	t := newTorrent(session)

	c.mu.Lock()
	c.torrents[infoHashHex] = t
	c.mu.Unlock()

	c.logger.Info("magnet link added successfully", "info_hash", infoHashHex, "name", meta.Info.Name)

	return t, nil
}

// AddTorrentFromBytes adds a torrent from raw .torrent file data
func (c *Client) AddTorrentFromBytes(data []byte) (*Torrent, error) {
	// Parse torrent file
	meta, err := torrent.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse torrent: %w", err)
	}

	infoHashHex := meta.InfoHashHex

	// Check if already exists
	c.mu.Lock()
	if existing, ok := c.torrents[infoHashHex]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()

	// Create session
	session, err := engine.NewSession(meta, c.config, c.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// Add session to engine
	if err := c.engine.AddSession(session); err != nil {
		return nil, fmt.Errorf("failed to add session to engine: %w", err)
	}

	// Create Torrent handle
	t := newTorrent(session)

	// Store in client
	c.mu.Lock()
	c.torrents[infoHashHex] = t
	c.mu.Unlock()

	c.logger.Info("torrent added",
		"name", meta.Info.Name,
		"info_hash", infoHashHex,
		"size", meta.Info.TotalLength())

	return t, nil
}

// GetTorrent retrieves a torrent by its info hash (hex encoded)
func (c *Client) GetTorrent(infoHash string) (*Torrent, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, ok := c.torrents[infoHash]
	return t, ok
}

// Torrents returns a list of all torrents
func (c *Client) Torrents() []*Torrent {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*Torrent, 0, len(c.torrents))
	for _, t := range c.torrents {
		result = append(result, t)
	}

	return result
}

// RemoveTorrent removes a torrent and stops all activity related to it
func (c *Client) RemoveTorrent(infoHash string, deleteFiles bool) error {
	c.mu.Lock()
	t, ok := c.torrents[infoHash]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("torrent not found: %s", infoHash)
	}
	delete(c.torrents, infoHash)
	c.mu.Unlock()

	// Stop the torrent
	if err := t.Stop(); err != nil {
		return fmt.Errorf("failed to stop torrent: %w", err)
	}

	// Remove from engine
	if err := c.engine.RemoveSession(infoHash); err != nil {
		return fmt.Errorf("failed to remove session from engine: %w", err)
	}

	// Delete files if requested
	if deleteFiles {
		files, err := t.Files()
		if err == nil && len(files) > 0 {
			// Delete each file
			for _, file := range files {
				fullPath := fmt.Sprintf("%s/%s", c.config.DownloadPath, file.Path)
				if err := os.Remove(fullPath); err != nil {
					c.logger.Warn("failed to delete file", "path", fullPath, "error", err)
				} else {
					c.logger.Debug("deleted file", "path", fullPath)
				}
			}
		}
	}

	c.logger.Info("torrent removed", "info_hash", infoHash)
	return nil
}

// Start starts the client (begins listening for incoming connections, etc.)
func (c *Client) Start() error {
	// Engine is already started in NewClient
	return nil
}

// Stop stops the client and all torrents gracefully
func (c *Client) Stop() error {
	c.logger.Info("stopping client")

	// Stop all torrents
	torrents := c.Torrents()
	for _, t := range torrents {
		if err := t.Stop(); err != nil {
			c.logger.Warn("failed to stop torrent", "error", err)
		}
	}

	// Stop engine (this stops DHT, listener, etc.)
	if err := c.engine.Stop(); err != nil {
		return fmt.Errorf("failed to stop engine: %w", err)
	}

	c.logger.Info("client stopped")
	return nil
}

// Close is an alias for Stop
func (c *Client) Close() error {
	return c.Stop()
}

// Config returns a copy of the client's configuration
func (c *Client) Config() config.Config {
	return *c.config
}
