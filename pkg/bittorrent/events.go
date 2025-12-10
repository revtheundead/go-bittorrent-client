package bittorrent

import "time"

// EventType represents the type of torrent event
type EventType int

const (
	// EventStarted indicates the torrent download has started
	EventStarted EventType = iota

	// EventProgress indicates download progress update
	EventProgress

	// EventPieceComplete indicates a piece has been downloaded and verified
	EventPieceComplete

	// EventComplete indicates the entire torrent download is complete
	EventComplete

	// EventSeeding indicates the torrent is now seeding
	EventSeeding

	// EventSeedingComplete indicates seeding has finished (ratio reached)
	EventSeedingComplete

	// EventPaused indicates the torrent has been paused
	EventPaused

	// EventResumed indicates the torrent has been resumed
	EventResumed

	// EventError indicates an error occurred
	EventError

	// EventPeerConnected indicates a new peer connection
	EventPeerConnected

	// EventPeerDisconnected indicates a peer disconnected
	EventPeerDisconnected

	// EventTrackerAnnounce indicates a tracker announce occurred
	EventTrackerAnnounce
)

// Event represents a torrent event
type Event struct {
	// Type of event
	Type EventType

	// InfoHash of the torrent (hex encoded)
	InfoHash string

	// Progress (0.0 to 1.0) for EventProgress
	Progress float64

	// PieceIndex for EventPieceComplete
	PieceIndex int

	// Error for EventError
	Error error

	// PeerAddr for EventPeerConnected/EventPeerDisconnected
	PeerAddr string

	// Stats snapshot at time of event
	Stats Stats

	// Timestamp when event occurred
	Timestamp time.Time
}

// String returns a string representation of the event type
func (e EventType) String() string {
	switch e {
	case EventStarted:
		return "Started"
	case EventProgress:
		return "Progress"
	case EventPieceComplete:
		return "PieceComplete"
	case EventComplete:
		return "Complete"
	case EventSeeding:
		return "Seeding"
	case EventSeedingComplete:
		return "SeedingComplete"
	case EventPaused:
		return "Paused"
	case EventResumed:
		return "Resumed"
	case EventError:
		return "Error"
	case EventPeerConnected:
		return "PeerConnected"
	case EventPeerDisconnected:
		return "PeerDisconnected"
	case EventTrackerAnnounce:
		return "TrackerAnnounce"
	default:
		return "Unknown"
	}
}
