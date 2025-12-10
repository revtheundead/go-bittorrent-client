# revTorrent

A high-performance BitTorrent client implementation in Go with DHT support, magnet links, multi-file torrents, and a command-line interface.

## Features

### Core Protocol
- Torrent file parsing (single and multi-file)
- Magnet link support via BEP 9 metadata exchange
- Complete peer wire protocol implementation
- SHA-1 piece verification with automatic retry
- BEP 10 extension protocol support

### Network & Discovery
- DHT (Distributed Hash Table) via Kademlia (BEP 5)
- HTTP tracker protocol (BEP 3)
- UDP tracker protocol (BEP 15)
- Multi-tracker coordination with parallel announces
- Peer Exchange (PEX) protocol

### Download Optimization
- Piece selection strategies: random-first, rarest-first, sequential
- Endgame mode activation at 70% completion
- Request pipelining: 20 concurrent block requests per peer (320KB in-flight)
- Performance-based peer selection and replacement
- Automatic piece retry on failure

### Upload & Seeding
- Standard BitTorrent choking algorithm with optimistic unchoke
- Tit-for-tat fairness mechanism
- Configurable seed ratios
- Have message broadcasting

### Storage
- Direct-to-disk streaming (no memory assembly)
- Multi-file torrent support
- Resume data persistence
- Bitfield tracking for piece completion
- File preallocation

### Performance
- Token bucket rate limiting for upload/download
- Concurrent peer downloads
- Configurable timeouts: block requests (20s), choke timeout (45s), peer discovery (30s/3min)
- Memory-efficient streaming architecture

### User Interface
- Command-line interface via Cobra
- Real-time progress display
- Interactive controls: pause/resume, quit
- Graceful shutdown with state persistence
- Configurable log levels: debug, info, warn, error

## Installation

### From Source

```bash
git clone https://github.com/revtheundead/revtorrent
cd revtorrent
go build -o revtorrent ./cmd/revtorrent

# Optional: Install globally
go install ./cmd/revtorrent
```

### Dependencies

```bash
go mod download
```

## Usage

### CLI Commands

#### Download

```bash
# Basic download
revtorrent download ubuntu.torrent

# Specify output directory
revtorrent download -o ~/Downloads ubuntu.torrent

# Download from magnet link
revtorrent download "magnet:?xt=urn:btih:..."

# Custom settings
revtorrent download --max-peers 100 --port 51413 ubuntu.torrent

# Disable seeding after completion
revtorrent download --seed=false ubuntu.torrent

# Enable debug logging
revtorrent download --log-level debug ubuntu.torrent
```

#### Interactive Controls

When progress bar is enabled:
- Press `p` to pause/resume
- Press `q` or `Ctrl+C` to quit gracefully

#### Show Torrent Information

```bash
revtorrent info ubuntu.torrent
revtorrent info "magnet:?xt=urn:btih:..."
```

#### Available Flags

- `-o, --output`: Output directory (default: "./downloads")
- `--port`: Listen port for incoming connections (default: 6881)
- `--max-peers`: Maximum peers per torrent (default: 50)
- `--download-rate`: Download rate limit in bytes/sec (0=unlimited)
- `--upload-rate`: Upload rate limit in bytes/sec (0=unlimited)
- `--seed`: Continue seeding after download (default: true)
- `--seed-ratio`: Target upload/download ratio (default: 1.0, 0=unlimited)
- `--no-dht`: Disable DHT
- `--no-progress`: Disable progress bar
- `--log-level`: Set log level: debug, info, warn, error (default: info)

### Library Usage

```go
package main

import (
    "fmt"
    "github.com/revtheundead/revtorrent/pkg/bittorrent"
)

func main() {
    client, err := bittorrent.NewClient(
        bittorrent.WithDownloadPath("./downloads"),
        bittorrent.WithPort(6881),
        bittorrent.WithDHT(true),
        bittorrent.WithRateLimit(1024*1024, 256*1024),
        bittorrent.WithLogLevel("info"),
    )
    if err != nil {
        panic(err)
    }
    defer client.Stop()

    torrent, err := client.AddTorrent("ubuntu.torrent")
    if err != nil {
        panic(err)
    }

    if err := torrent.Start(); err != nil {
        panic(err)
    }

    for event := range torrent.Events() {
        switch event.Type {
        case bittorrent.EventProgress:
            stats := torrent.Stats()
            fmt.Printf("Progress: %.1f%% | Down: %.1f KB/s | Up: %.1f KB/s | Peers: %d\n",
                event.Progress*100,
                stats.DownloadRate/1024,
                stats.UploadRate/1024,
                stats.Peers)

        case bittorrent.EventComplete:
            fmt.Println("Download complete")
            return

        case bittorrent.EventError:
            fmt.Printf("Error: %v\n", event.Error)
            return
        }
    }
}

// Pause and Resume
func pauseResumeExample() {
    // ... client and torrent setup ...

    if err := torrent.Pause(); err != nil {
        fmt.Printf("Failed to pause: %v\n", err)
    }

    if err := torrent.Resume(); err != nil {
        fmt.Printf("Failed to resume: %v\n", err)
    }

    state := torrent.State()
    fmt.Printf("Current state: %s\n", state.String())
}
```

## Configuration

### Configuration File

Default location: `~/.revtorrent/config.yaml`

```yaml
download_path: "./downloads"
listen_port: 6881
max_peers: 50
max_connections: 200
upload_rate: 0
download_rate: 0
dht_enabled: true
pex_enabled: true
seed: true
seed_ratio: 1.0
state_dir: "~/.revtorrent"
log_level: "info"
```

### Environment Variables

```bash
REVTORRENT_PORT=6881
REVTORRENT_DOWNLOAD_PATH=~/Downloads
REVTORRENT_LOG_LEVEL=debug
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Public API (pkg/bittorrent)              │
│  Client, Torrent, Options, Events, Stats                    │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Engine Layer (internal/engine)           │
│  • Session management (per-torrent coordination)            │
│  • Download/upload peer management                          │
│  • Incoming connection routing                              │
│  • Metadata fetching (magnet links)                         │
└─────────────────────────────────────────────────────────────┘
                              │
    ┌─────────────────────────┼─────────────────────────┐
    │                         │                         │
┌─────────┐          ┌──────────────┐          ┌──────────┐
│ Tracker │          │     DHT      │          │  Peers   │
│ Manager │          │  (Kademlia)  │          │ Protocol │
└─────────┘          └──────────────┘          └──────────┘
                              │
    ┌─────────────────────────┼─────────────────────────┐
    │                         │                         │
┌─────────────┐    ┌──────────────────┐    ┌──────────────┐
│  Download   │    │     Storage      │    │   Upload     │
│ Coordinator │    │  (File I/O)      │    │  Coordinator │
└─────────────┘    └──────────────────┘    └──────────────┘
     │                      │                      │
┌─────────────┐    ┌──────────────┐    ┌──────────────────┐
│   Piece     │    │   Bitfield   │    │  Choking Algo    │
│ Selection   │    │   Tracking   │    │                  │
└─────────────┘    └──────────────┘    └──────────────────┘
```

## Performance Tuning

### Maximum Download Speed

1. Increase max peers: `--max-peers 100`
2. Configure port forwarding for the listen port in your router
3. Ensure DHT is enabled (default)
4. Select torrents with multiple seeders

### Maximum Upload (Seeding)

1. Configure port forwarding (required for incoming connections)
2. Set unlimited upload rate: `--upload-rate 0`
3. Keep client running for extended periods

### Low-Bandwidth Connections

1. Set rate limits: `--download-rate 524288 --upload-rate 131072` (512KB/s down, 128KB/s up)
2. Reduce max peers: `--max-peers 20`

## Troubleshooting

### No Uploads (0 B uploaded)

**Cause**: No incoming connections due to NAT/firewall blocking.

**Solution**:
1. Forward the listen port (default 6881) in router settings
2. Check Windows Firewall or antivirus settings
3. Try alternate port: `--port 51413`
4. Verify port is open using online port checker tools

### Download Stalls at High Percentage

**Cause**: Endgame mode not activating or rare pieces unavailable.

**Solution**: Endgame mode activates automatically at 70% completion. With few peers, completion may be slow. Select torrents with more seeders for faster completion.

### Slow Download Speeds

**Cause**: Peers are choking due to lack of uploads (tit-for-tat mechanism).

**Solution**:
1. Configure port forwarding to enable uploads
2. Wait for optimistic unchoke cycles
3. Client will automatically request additional peers

### Port Already in Use

**Cause**: Another application or instance using the port.

**Solution**:
1. Use alternate port: `--port 51413`
2. Terminate existing revtorrent processes
3. On Linux: `lsof -i :6881` to identify the process

### High CPU Usage

**Cause**: Too many peers, aggressive verification, or debug logging.

**Solution**:
1. Reduce max peers: `--max-peers 30`
2. Use info log level: `--log-level info`
3. Close resource-intensive applications

## Project Structure

```
revtorrent/
├── cmd/revtorrent/           # CLI application
│   ├── main.go
│   └── cmd/                  # Cobra commands
├── pkg/bittorrent/           # Public API
│   ├── client.go
│   ├── torrent.go
│   ├── options.go
│   ├── stats.go
│   └── events.go
├── internal/
│   ├── engine/               # Engine layer
│   │   ├── engine.go
│   │   ├── session.go
│   │   └── metadata.go
│   ├── core/
│   │   ├── torrent/          # Torrent file parsing
│   │   ├── storage/          # File I/O and bitfield
│   │   ├── peer/             # Peer wire protocol
│   │   ├── piece/            # Piece management
│   │   ├── downloader/       # Download coordinator
│   │   ├── uploader/         # Upload coordinator
│   │   ├── ratelimit/        # Bandwidth control
│   │   ├── controller/       # Torrent lifecycle
│   │   └── shutdown/         # Graceful shutdown
│   ├── tracker/              # Tracker communication
│   ├── dht/                  # DHT implementation
│   ├── protocol/
│   │   ├── bencode/          # Bencode encoding/decoding
│   │   └── magnet/           # Magnet link parsing
│   ├── config/               # Configuration management
│   └── ui/                   # Terminal UI
├── examples/                 # Usage examples
└── configs/                  # Configuration files
```

## BitTorrent Enhancement Proposals (BEPs) Implemented

- BEP 3: The BitTorrent Protocol
- BEP 5: DHT Protocol (Kademlia)
- BEP 9: Extension for Peers to Send Metadata Files
- BEP 10: Extension Protocol
- BEP 15: UDP Tracker Protocol
- BEP 23: Tracker Returns Compact Peer Lists

## Development Status

Complete and functional. All core components are implemented and tested:
- Engine and session management
- Download/upload coordination
- DHT and tracker support
- Magnet link support
- Multi-file torrents
- Resume/pause functionality
- Rate limiting
- CLI and library interfaces

## Contributing

Areas for improvement:
- Web UI (HTTP API + web interface)
- Additional piece selection strategies
- IPv6 support
- UPnP/NAT-PMP for automatic port forwarding
- Encryption (BEP 52)
- Streaming mode optimizations

## License

MIT License

## References

- BitTorrent Protocol Specification: https://www.bittorrent.org/beps/bep_0003.html
- Kademlia DHT Paper: https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
- BitTorrent Enhancement Proposals: https://www.bittorrent.org/beps/bep_0000.html

## Support

For issues and questions:
- GitHub Issues: https://github.com/revtheundead/revtorrent/issues
- Documentation: See this README and examples directory
