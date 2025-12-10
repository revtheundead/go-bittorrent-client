# revTorrent Public API Documentation

This document describes the public API of the revTorrent library.

## Installation

```bash
go get github.com/revtheundead/revtorrent/pkg/bittorrent
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/revtheundead/revtorrent/pkg/bittorrent"
)

func main() {
    // Create client
    client, err := bittorrent.NewClient(
        bittorrent.WithDownloadPath("./downloads"),
        bittorrent.WithDHT(true),
    )
    if err != nil {
        panic(err)
    }
    defer client.Stop()

    // Add torrent
    torrent, err := client.AddTorrent("ubuntu.torrent")
    if err != nil {
        panic(err)
    }

    // Start download
    torrent.Start()

    // Monitor progress
    for event := range torrent.Events() {
        if event.Type == bittorrent.EventComplete {
            fmt.Println("Download complete!")
            break
        }
    }
}
```

## Client API

### Creating a Client

```go
func NewClient(opts ...Option) (*Client, error)
```

Creates a new BitTorrent client with the specified options.

**Example:**
```go
client, err := bittorrent.NewClient(
    bittorrent.WithDownloadPath("./downloads"),
    bittorrent.WithPort(6881),
    bittorrent.WithDHT(true),
    bittorrent.WithRateLimit(1024*1024, 256*1024), // 1MB/s down, 256KB/s up
    bittorrent.WithStateDir("./.revtorrent"),
    bittorrent.WithLogLevel("info"),
)
```

### Client Methods

#### AddTorrent

```go
func (c *Client) AddTorrent(path string) (*Torrent, error)
```

Adds a torrent from a `.torrent` file.

**Parameters:**
- `path` - Path to the .torrent file

**Returns:**
- `*Torrent` - Handle to the torrent
- `error` - Error if file cannot be read/parsed

**Example:**
```go
torrent, err := client.AddTorrent("/path/to/file.torrent")
```

#### AddMagnet

```go
func (c *Client) AddMagnet(uri string) (*Torrent, error)
```

Adds a torrent from a magnet link.

**Parameters:**
- `uri` - Magnet link URI

**Returns:**
- `*Torrent` - Handle to the torrent
- `error` - Error if magnet link is invalid

**Example:**
```go
torrent, err := client.AddMagnet("magnet:?xt=urn:btih:...")
```

#### AddTorrentFromBytes

```go
func (c *Client) AddTorrentFromBytes(data []byte) (*Torrent, error)
```

Adds a torrent from raw `.torrent` file data.

**Parameters:**
- `data` - Raw torrent file bytes

**Returns:**
- `*Torrent` - Handle to the torrent
- `error` - Error if data cannot be parsed

#### GetTorrent

```go
func (c *Client) GetTorrent(infoHash string) (*Torrent, bool)
```

Retrieves a torrent by its info hash.

**Parameters:**
- `infoHash` - Info hash (hex-encoded)

**Returns:**
- `*Torrent` - Torrent handle
- `bool` - Whether torrent exists

#### Torrents

```go
func (c *Client) Torrents() []*Torrent
```

Returns all torrents managed by this client.

**Returns:**
- `[]*Torrent` - Slice of all torrents

#### RemoveTorrent

```go
func (c *Client) RemoveTorrent(infoHash string, deleteFiles bool) error
```

Removes a torrent and optionally deletes downloaded files.

**Parameters:**
- `infoHash` - Info hash (hex-encoded)
- `deleteFiles` - Whether to delete downloaded files

**Returns:**
- `error` - Error if removal fails

#### Stop

```go
func (c *Client) Stop() error
```

Stops the client and all torrents gracefully.

**Returns:**
- `error` - Error if shutdown fails

**Alias:** `Close()`

## Torrent API

### Torrent Methods

#### Start

```go
func (t *Torrent) Start() error
```

Begins downloading/seeding the torrent.

#### Stop

```go
func (t *Torrent) Stop() error
```

Stops the torrent and saves state.

#### Pause

```go
func (t *Torrent) Pause() error
```

Pauses the torrent (saves state but stops activity).

#### Resume

```go
func (t *Torrent) Resume() error
```

Resumes a paused torrent.

#### Name

```go
func (t *Torrent) Name() string
```

Returns the torrent name.

#### InfoHash

```go
func (t *Torrent) InfoHash() string
```

Returns the info hash as a hex string.

#### Size

```go
func (t *Torrent) Size() int64
```

Returns the total size in bytes.

#### Progress

```go
func (t *Torrent) Progress() float64
```

Returns download progress from 0.0 to 1.0.

#### Stats

```go
func (t *Torrent) Stats() Stats
```

Returns a snapshot of current statistics.

#### State

```go
func (t *Torrent) State() TorrentState
```

Returns the current state.

#### Events

```go
func (t *Torrent) Events() <-chan Event
```

Returns a read-only event channel for monitoring.

#### WaitForCompletion

```go
func (t *Torrent) WaitForCompletion() error
```

Blocks until download completes or error occurs.

**Example:**
```go
if err := torrent.WaitForCompletion(); err != nil {
    log.Fatal(err)
}
```

## Configuration Options

### WithDownloadPath

```go
func WithDownloadPath(path string) Option
```

Sets the directory where files will be saved.

**Default:** `./downloads`

### WithPort

```go
func WithPort(port int) Option
```

Sets the listening port for incoming connections.

**Default:** `6881`

### WithDHT

```go
func WithDHT(enabled bool) Option
```

Enables or disables DHT for peer discovery.

**Default:** `true`

### WithPEX

```go
func WithPEX(enabled bool) Option
```

Enables or disables Peer Exchange.

**Default:** `true`

### WithRateLimit

```go
func WithRateLimit(downloadBps, uploadBps int64) Option
```

Sets bandwidth limits in bytes per second. Use 0 for unlimited.

**Default:** `0, 0` (unlimited)

**Example:**
```go
bittorrent.WithRateLimit(
    10*1024*1024,  // 10 MB/s download
    2*1024*1024,   // 2 MB/s upload
)
```

### WithStateDir

```go
func WithStateDir(dir string) Option
```

Sets the directory for resume data and state.

**Default:** `./.revtorrent`

### WithSeed

```go
func WithSeed(enabled bool) Option
```

Whether to continue seeding after download.

**Default:** `true`

### WithSeedRatio

```go
func WithSeedRatio(ratio float64) Option
```

Seed until this upload/download ratio. Use 0 to seed forever.

**Default:** `1.0`

### WithMaxPeers

```go
func WithMaxPeers(max int) Option
```

Maximum number of peers per torrent.

**Default:** `50`

### WithLogLevel

```go
func WithLogLevel(level string) Option
```

Sets logging level: "debug", "info", "warn", "error".

**Default:** `"info"`

## Events

### Event Types

```go
const (
    EventStarted            // Download started
    EventProgress           // Progress update
    EventPieceComplete      // Single piece completed
    EventComplete           // All pieces downloaded
    EventSeeding            // Now seeding
    EventSeedingComplete    // Seeding finished (ratio reached)
    EventPaused             // Torrent paused
    EventResumed            // Torrent resumed
    EventError              // Error occurred
    EventPeerConnected      // New peer connected
    EventPeerDisconnected   // Peer disconnected
    EventTrackerAnnounce    // Tracker announce occurred
)
```

### Event Structure

```go
type Event struct {
    Type       EventType   // Type of event
    InfoHash   string      // Torrent info hash
    Progress   float64     // Progress (0.0-1.0) for EventProgress
    PieceIndex int         // Piece index for EventPieceComplete
    Error      error       // Error for EventError
    PeerAddr   string      // Peer address for peer events
    Stats      Stats       // Statistics snapshot
    Timestamp  time.Time   // When event occurred
}
```

### Event Handling

```go
for event := range torrent.Events() {
    switch event.Type {
    case bittorrent.EventStarted:
        fmt.Println("Download started")

    case bittorrent.EventProgress:
        fmt.Printf("Progress: %.1f%%\n", event.Progress * 100)

    case bittorrent.EventComplete:
        fmt.Println("Download complete!")
        return

    case bittorrent.EventError:
        log.Fatal(event.Error)
    }
}
```

## Statistics

### Stats Structure

```go
type Stats struct {
    Progress       float64       // 0.0 to 1.0
    Downloaded     int64         // Total bytes downloaded
    Uploaded       int64         // Total bytes uploaded
    DownloadRate   float64       // Bytes per second
    UploadRate     float64       // Bytes per second
    Peers          int           // Active peer count
    Seeds          int           // Seeders (if known)
    Leechers       int           // Leechers (if known)
    TotalSize      int64         // Total torrent size
    PiecesComplete int           // Completed pieces
    TotalPieces    int           // Total pieces
    ETA            time.Duration // Estimated time remaining
    Ratio          float64       // Upload/download ratio
    StartedAt      time.Time     // When started
    CompletedAt    time.Time     // When completed (zero if not)
    State          TorrentState  // Current state
}
```

### Torrent States

```go
const (
    StateStopped     // Torrent is stopped
    StateDownloading // Actively downloading
    StateSeeding     // Actively seeding
    StatePaused      // Paused
    StateChecking    // Verifying pieces
    StateError       // Error state
)
```

## Advanced Usage

### Custom Event Handling

```go
// Launch event handler in goroutine
go func() {
    for event := range torrent.Events() {
        switch event.Type {
        case bittorrent.EventProgress:
            // Update UI
            updateProgressBar(event.Progress)
        case bittorrent.EventPeerConnected:
            log.Printf("Peer connected: %s", event.PeerAddr)
        }
    }
}()
```

### Pause and Resume

```go
// Pause torrent
if err := torrent.Pause(); err != nil {
    log.Fatal(err)
}

// Do something else...

// Resume later
if err := torrent.Resume(); err != nil {
    log.Fatal(err)
}
```

### Multiple Torrents

```go
client, _ := bittorrent.NewClient()

// Add multiple torrents
t1, _ := client.AddTorrent("file1.torrent")
t2, _ := client.AddTorrent("file2.torrent")
t3, _ := client.AddMagnet("magnet:?xt=...")

// Start all
t1.Start()
t2.Start()
t3.Start()

// Monitor all
for _, torrent := range client.Torrents() {
    go monitorTorrent(torrent)
}
```

### Graceful Shutdown

```go
import (
    "os"
    "os/signal"
)

// Setup signal handling
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

go func() {
    <-sigCh
    log.Println("Shutting down...")
    client.Stop() // Saves all state
}()
```

## Error Handling

All errors are wrapped with context using `fmt.Errorf` with `%w`:

```go
torrent, err := client.AddTorrent("file.torrent")
if err != nil {
    // Check for specific error types
    if errors.Is(err, os.ErrNotExist) {
        log.Fatal("Torrent file not found")
    }
    log.Fatalf("Failed to add torrent: %v", err)
}
```

## Thread Safety

All public API methods are thread-safe and can be called from multiple goroutines concurrently.

## Best Practices

1. **Always defer client.Stop()** to ensure graceful shutdown
2. **Handle events in goroutines** to avoid blocking
3. **Check errors** from all API calls
4. **Use context** for cancelation where needed
5. **Monitor Stats()** for performance insights
6. **Configure rate limits** to be a good network citizen

## See Also

- [Architecture Documentation](./ARCHITECTURE.md)
- [Protocol Implementation](./PROTOCOL.md)
- [Examples](../examples/README.md)
