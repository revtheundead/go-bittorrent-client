package config

import (
	"os"
	"path/filepath"
)

// Config holds all configuration options for the BitTorrent client
type Config struct {
	// Download settings
	DownloadPath string // Where to save downloaded files

	// Network settings
	ListenPort     int  // Port to listen on for incoming connections
	MaxPeers       int  // Maximum number of peers to connect to
	MaxConnections int  // Maximum total connections

	// Rate limiting (bytes per second, 0 = unlimited)
	UploadRate   int64
	DownloadRate int64

	// Protocol settings
	DHTEnabled bool // Enable DHT for trackerless peer discovery
	PEXEnabled bool // Enable Peer Exchange

	// Seeding settings
	Seed      bool    // Continue seeding after download completes
	SeedRatio float64 // Seed until this upload/download ratio

	// State management
	StateDir string // Directory for resume data and state files

	// Logging
	LogLevel string // debug, info, warn, error
}

// Default returns a Config with sensible default values
func Default() *Config {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}

	return &Config{
		DownloadPath:   filepath.Join(homeDir, "Downloads"),
		ListenPort:     6881,
		MaxPeers:       50,
		MaxConnections: 200,
		UploadRate:     0, // unlimited
		DownloadRate:   0, // unlimited
		DHTEnabled:     true,
		PEXEnabled:     true,
		Seed:           true,
		SeedRatio:      1.0,
		StateDir:       filepath.Join(homeDir, ".bittorrent"),
		LogLevel:       "info",
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.ListenPort < 1 || c.ListenPort > 65535 {
		return &ValidationError{Field: "ListenPort", Message: "must be between 1 and 65535"}
	}

	if c.MaxPeers < 1 {
		return &ValidationError{Field: "MaxPeers", Message: "must be at least 1"}
	}

	if c.MaxConnections < c.MaxPeers {
		return &ValidationError{Field: "MaxConnections", Message: "must be >= MaxPeers"}
	}

	if c.SeedRatio < 0 {
		return &ValidationError{Field: "SeedRatio", Message: "cannot be negative"}
	}

	return nil
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return "config validation error: " + e.Field + ": " + e.Message
}
