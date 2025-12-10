# revTorrent Examples

This directory contains example programs demonstrating how to use the revTorrent client library.

> **Note:** These examples demonstrate the public API interface. The underlying implementation is a work in progress - individual components (DHT, tracker manager, storage, peer protocol, etc.) are implemented, but the final integration layer connecting all components through the Client API is pending. These examples serve as a blueprint for how the API will be used once integration is complete.

## Examples Overview

| Example | Complexity | Features |
|---------|-----------|----------|
| basic_download | Beginner | Simple download with progress tracking |
| magnet_download | Beginner | Magnet link support, metadata fetching |
| pause_resume | Intermediate | Pause/resume control demonstration |
| multi_torrent | Intermediate | Multiple simultaneous downloads |
| rate_limit | Intermediate | Bandwidth rate limiting |
| advanced_usage | Advanced | Interactive CLI, verification, persistence |

## Basic Download

The simplest example showing how to download a torrent file.

```bash
cd basic_download
go run main.go /path/to/torrent/file.torrent
```

**Features demonstrated:**
- Creating a client
- Adding a torrent from file
- Monitoring download progress
- Handling events
- Graceful shutdown

**What you'll learn:**
- Basic client setup
- Event loop structure
- Progress bar implementation
- Signal handling

## Magnet Download

Example showing how to download from a magnet link.

```bash
cd magnet_download
go run main.go "magnet:?xt=urn:btih:..."
```

**Features demonstrated:**
- Working with magnet links
- DHT peer discovery
- Metadata fetching from peers
- Seeding after download
- File count display

**What you'll learn:**
- Magnet link handling
- Metadata fetch detection
- DHT importance for magnet links
- Automatic seeding

## Pause/Resume

Demonstrates pause and resume functionality with automatic demonstration.

```bash
cd pause_resume
go run main.go /path/to/torrent/file.torrent
```

**Features demonstrated:**
- Pausing active downloads
- Resuming paused downloads
- EventPaused/EventResumed handling
- State tracking

**What you'll learn:**
- Download control methods
- State transition events
- Timer-based automation
- Visual status indicators

**Behavior:**
- Starts download automatically
- Pauses after 10 seconds
- Resumes after 5 seconds
- Shows state changes in real-time

## Multi-Torrent

Shows how to download multiple torrents simultaneously with a single client.

```bash
cd multi_torrent
go run main.go file1.torrent file2.torrent file3.torrent
```

**Features demonstrated:**
- Single client managing multiple torrents
- Concurrent monitoring with goroutines
- Coordinated shutdown
- Per-torrent statistics

**What you'll learn:**
- Client resource sharing
- Concurrent event monitoring
- WaitGroup coordination
- Error handling for multiple torrents

## Rate Limit

Demonstrates bandwidth rate limiting with real-time monitoring and verification.

```bash
cd rate_limit
go run main.go /path/to/torrent/file.torrent
```

**Features demonstrated:**
- Download rate limiting (512 KB/s)
- Upload rate limiting (128 KB/s)
- Rate usage percentage display
- Limit verification on exit

**What you'll learn:**
- Rate limit configuration
- Bandwidth monitoring
- Performance tracking
- Limit enforcement verification

## Advanced Usage

Interactive example with full control and state management.

```bash
cd advanced_usage
go run main.go /path/to/torrent/file.torrent
```

**Features demonstrated:**
- Interactive command interface
- Rate limiting (2 MB/s down, 512 KB/s up)
- Pause/resume control
- Data verification
- State persistence
- Seed ratio configuration (1.5)
- Comprehensive statistics
- File listing

**Available commands:**
- `start` / `resume` - Start or resume download
- `pause` - Pause download
- `stats` / `status` - Show detailed statistics
- `files` - List all files in torrent
- `verify` - Verify downloaded data integrity
- `quit` / `exit` / `q` - Save state and exit
- `help` / `?` - Show command help

**What you'll learn:**
- Full client configuration
- Interactive control implementation
- Data integrity verification
- State persistence
- Seeding configuration

## Library Usage

### Basic Usage

```go
package main

import (
    "github.com/revtheundead/revtorrent/pkg/bittorrent"
)

func main() {
    // Create client
    client, _ := bittorrent.NewClient(
        bittorrent.WithDownloadPath("./downloads"),
        bittorrent.WithDHT(true),
    )
    defer client.Stop()

    // Add torrent
    torrent, _ := client.AddTorrent("file.torrent")

    // Start download
    torrent.Start()

    // Monitor events
    for event := range torrent.Events() {
        switch event.Type {
        case bittorrent.EventComplete:
            return
        case bittorrent.EventError:
            panic(event.Error)
        }
    }
}
```

### Configuration Options

```go
client, err := bittorrent.NewClient(
    // Download directory
    bittorrent.WithDownloadPath("./downloads"),

    // Listening port (default: random)
    bittorrent.WithPort(6881),

    // Enable/disable DHT (required for magnet links)
    bittorrent.WithDHT(true),

    // Enable/disable PEX (Peer Exchange)
    bittorrent.WithPEX(true),

    // Rate limiting (bytes/sec, 0 = unlimited)
    bittorrent.WithRateLimit(
        1024*1024,    // 1 MB/s download
        256*1024,     // 256 KB/s upload
    ),

    // Maximum peers per torrent
    bittorrent.WithMaxPeers(50),

    // State directory for resume data
    bittorrent.WithStateDir("./.revtorrent-state"),

    // Enable seeding after download
    bittorrent.WithSeed(true),

    // Seed until ratio (uploaded/downloaded)
    bittorrent.WithSeedRatio(1.5),

    // Log level: debug, info, warn, error
    bittorrent.WithLogLevel("info"),
)
```

**Configuration Table:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| WithDownloadPath | string | "./downloads" | Download directory path |
| WithPort | int | random | Listening port for incoming connections |
| WithDHT | bool | false | Enable DHT for trackerless operation |
| WithPEX | bool | false | Enable Peer Exchange protocol |
| WithRateLimit | int, int | 0, 0 | Download and upload rate limits (bytes/sec) |
| WithMaxPeers | int | 50 | Maximum peers per torrent |
| WithStateDir | string | "" | Directory for state persistence |
| WithSeed | bool | false | Continue seeding after download |
| WithSeedRatio | float64 | 0.0 | Stop seeding after reaching ratio |
| WithLogLevel | string | "info" | Logging verbosity level |

### Torrent Operations

```go
// Get torrent information
name := torrent.Name()
size := torrent.Size()
hash := torrent.InfoHash()

// Control download
torrent.Start()
torrent.Pause()
torrent.Resume()
torrent.Stop()

// Get statistics
stats := torrent.Stats()
progress := torrent.Progress() // 0.0 to 1.0

// Wait for completion
torrent.WaitForCompletion()
```

### Event Handling

```go
for event := range torrent.Events() {
    switch event.Type {
    case bittorrent.EventStarted:
        // Download started
    case bittorrent.EventProgress:
        // Progress update
        fmt.Printf("%.1f%%\n", event.Progress * 100)
    case bittorrent.EventPaused:
        // Torrent paused
    case bittorrent.EventResumed:
        // Torrent resumed
    case bittorrent.EventComplete:
        // Download finished
    case bittorrent.EventSeeding:
        // Now seeding
    case bittorrent.EventError:
        // Error occurred
        log.Fatal(event.Error)
    }
}
```

**Event Types:**

| Event | Description | When Triggered |
|-------|-------------|----------------|
| EventStarted | Download started | After calling Start() |
| EventProgress | Progress update | Periodically during download |
| EventPaused | Download paused | After calling Pause() |
| EventResumed | Download resumed | After calling Resume() |
| EventComplete | Download complete | When all pieces downloaded |
| EventSeeding | Seeding active | When uploading to peers |
| EventError | Error occurred | On any error condition |

### Statistics

```go
stats := torrent.Stats()

// Access statistics fields
fmt.Printf("State: %s\n", stats.State)                   // Current state
fmt.Printf("Progress: %.1f%%\n", stats.Progress * 100)   // 0.0 to 1.0
fmt.Printf("Downloaded: %d bytes\n", stats.Downloaded)   // Total downloaded
fmt.Printf("Uploaded: %d bytes\n", stats.Uploaded)       // Total uploaded
fmt.Printf("Download rate: %.1f KB/s\n", stats.DownloadRate / 1024)
fmt.Printf("Upload rate: %.1f KB/s\n", stats.UploadRate / 1024)
fmt.Printf("Peers: %d\n", stats.Peers)                   // Connected peers
fmt.Printf("Pieces: %d/%d\n", stats.PiecesComplete, stats.TotalPieces)
fmt.Printf("ETA: %s\n", stats.ETA)                       // Estimated time remaining
```

**Stats Fields:**

| Field | Type | Description |
|-------|------|-------------|
| State | TorrentState | Current torrent state |
| Progress | float64 | Download progress (0.0 to 1.0) |
| Downloaded | int64 | Total bytes downloaded |
| Uploaded | int64 | Total bytes uploaded |
| DownloadRate | float64 | Current download rate (bytes/sec) |
| UploadRate | float64 | Current upload rate (bytes/sec) |
| Peers | int | Number of connected peers |
| PiecesComplete | int | Number of pieces downloaded |
| TotalPieces | int | Total number of pieces |
| ETA | time.Duration | Estimated time to completion |

## Running the Examples

Make sure you have the dependencies installed:

```bash
go mod download
```

Then run any example:

```bash
cd examples/basic_download
go run main.go ~/Downloads/ubuntu.torrent
```

## Notes

- All examples save downloads to `./downloads` by default
- The advanced example saves state to `./.revtorrent-state`
- Press Ctrl+C to gracefully shutdown (state is saved)
- DHT is enabled by default for trackerless operation
- Rate limiting can be adjusted or disabled (0 = unlimited)
