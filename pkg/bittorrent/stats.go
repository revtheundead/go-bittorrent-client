package bittorrent

import "time"

// Stats holds statistics about a torrent's download and upload progress
type Stats struct {
	// Progress from 0.0 to 1.0
	Progress float64

	// Total bytes downloaded
	Downloaded int64

	// Total bytes uploaded
	Uploaded int64

	// Current download rate in bytes per second
	DownloadRate float64

	// Current upload rate in bytes per second
	UploadRate float64

	// Number of active peer connections
	Peers int

	// Number of seeds in the swarm (if known)
	Seeds int

	// Number of leechers in the swarm (if known)
	Leechers int

	// Total size of the torrent in bytes
	TotalSize int64

	// Number of pieces completed
	PiecesComplete int

	// Total number of pieces
	TotalPieces int

	// Estimated time remaining (0 if unknown or complete)
	ETA time.Duration

	// Upload/download ratio
	Ratio float64

	// Time when download started
	StartedAt time.Time

	// Time when download completed (zero if not complete)
	CompletedAt time.Time

	// Current state
	State TorrentState
}

// TorrentState represents the current state of a torrent
type TorrentState int

const (
	// StateStopped indicates the torrent is stopped
	StateStopped TorrentState = iota

	// StateDownloading indicates the torrent is actively downloading
	StateDownloading

	// StateSeeding indicates the torrent is seeding
	StateSeeding

	// StatePaused indicates the torrent is paused
	StatePaused

	// StateChecking indicates piece verification is in progress
	StateChecking

	// StateError indicates the torrent is in an error state
	StateError
)

// String returns a string representation of the torrent state
func (s TorrentState) String() string {
	switch s {
	case StateStopped:
		return "Stopped"
	case StateDownloading:
		return "Downloading"
	case StateSeeding:
		return "Seeding"
	case StatePaused:
		return "Paused"
	case StateChecking:
		return "Checking"
	case StateError:
		return "Error"
	default:
		return "Unknown"
	}
}
