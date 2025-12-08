package bittorrent

import "github.com/revtheundead/revtorrent/internal/config"

// Option is a functional option for configuring the Client
type Option func(*config.Config)

// WithDownloadPath sets the directory where downloaded files will be saved
func WithDownloadPath(path string) Option {
	return func(c *config.Config) {
		c.DownloadPath = path
	}
}

// WithPort sets the port to listen on for incoming peer connections
func WithPort(port int) Option {
	return func(c *config.Config) {
		c.ListenPort = port
	}
}

// WithDHT enables or disables DHT (Distributed Hash Table) for peer discovery
func WithDHT(enabled bool) Option {
	return func(c *config.Config) {
		c.DHTEnabled = enabled
	}
}

// WithPEX enables or disables PEX (Peer Exchange)
func WithPEX(enabled bool) Option {
	return func(c *config.Config) {
		c.PEXEnabled = enabled
	}
}

// WithRateLimit sets upload and download rate limits in bytes per second
// Set to 0 for unlimited
func WithRateLimit(downloadBps, uploadBps int64) Option {
	return func(c *config.Config) {
		c.DownloadRate = downloadBps
		c.UploadRate = uploadBps
	}
}

// WithStateDir sets the directory for storing resume data and state
func WithStateDir(dir string) Option {
	return func(c *config.Config) {
		c.StateDir = dir
	}
}

// WithSeed sets whether to continue seeding after download completes
func WithSeed(enabled bool) Option {
	return func(c *config.Config) {
		c.Seed = enabled
	}
}

// WithSeedRatio sets the seed ratio (upload/download) to reach before stopping
func WithSeedRatio(ratio float64) Option {
	return func(c *config.Config) {
		c.SeedRatio = ratio
	}
}

// WithMaxPeers sets the maximum number of peers to connect to per torrent
func WithMaxPeers(max int) Option {
	return func(c *config.Config) {
		c.MaxPeers = max
	}
}

// WithLogLevel sets the logging level (debug, info, warn, error)
func WithLogLevel(level string) Option {
	return func(c *config.Config) {
		c.LogLevel = level
	}
}
