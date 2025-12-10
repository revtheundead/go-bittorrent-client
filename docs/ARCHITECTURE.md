# revTorrent Architecture

This document describes the architecture and design of revTorrent.

## Overview

revTorrent follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                     Public API Layer                        │
│                   (pkg/bittorrent)                          │
│  Client, Torrent, Options, Events, Stats                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Engine Layer (Pending)                   │
│              Coordinates all components                     │
│        Session Management, Event Distribution               │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   Tracker    │   │     DHT      │   │   Storage    │
│   Manager    │   │   Discovery  │   │  Subsystem   │
└──────────────┘   └──────────────┘   └──────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
                   ┌──────────────────┐
                   │  Peer Management │
                   │   & Download     │
                   └──────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   Download   │   │    Upload    │   │ Rate Limiter │
│  Strategies  │   │   (Seeding)  │   │              │
└──────────────┘   └──────────────┘   └──────────────┘
```

## Component Overview

### 1. Public API Layer (`pkg/bittorrent`)

**Purpose:** Provides a clean, simple API for library users.

**Components:**
- `Client` - Main entry point, manages torrents
- `Torrent` - Handle for individual torrent operations
- `Options` - Functional options for configuration
- `Events` - Event system for monitoring progress
- `Stats` - Statistics and metrics

**Design Principles:**
- Simple and intuitive API
- Thread-safe operations
- Non-blocking event channels
- Graceful error handling

### 2. Configuration Layer (`internal/config`)

**Purpose:** Centralized configuration management.

**Features:**
- YAML configuration files
- Environment variable support (via Viper)
- Validation of configuration values
- Default values for all settings

### 3. Core Components (`internal/core`)

#### 3.1 Torrent Parser (`internal/core/torrent`)

**Responsibilities:**
- Parse .torrent files (bencode)
- Support single and multi-file torrents
- Calculate info hashes
- Extract metadata

**Key Types:**
- `Metainfo` - Complete torrent metadata
- `Info` - Torrent info dictionary
- `FileInfo` - Individual file information

#### 3.2 Storage Subsystem (`internal/core/storage`)

**Responsibilities:**
- File I/O operations
- Piece verification (SHA-1 hashing)
- Multi-file mapping
- Resume data management

**Key Components:**
- `Storage` interface - Abstract storage operations
- `FileStorage` - Concrete implementation
- `Bitfield` - Track piece completion
- `ResumeManager` - Save/load state

**Design Features:**
- Streaming to disk (no in-memory assembly)
- Pre-allocation to prevent fragmentation
- Handles pieces spanning multiple files
- Thread-safe operations

#### 3.3 Peer Protocol (`internal/core/peer`)

**Responsibilities:**
- Peer wire protocol (BEP 3)
- Extension protocol (BEP 10)
- Message encoding/decoding
- Metadata exchange (BEP 9)

**Message Types:**
- Handshake, KeepAlive
- Choke, Unchoke, Interested, NotInterested
- Have, Bitfield
- Request, Piece, Cancel
- Extended (for metadata and extensions)

#### 3.4 Download Optimization (`internal/core/downloader`)

**Components:**

**a) Piece Selection Strategies:**
- `RarestFirstStrategy` - Download rarest pieces first
- `SequentialStrategy` - Download in order (streaming)
- `RandomStrategy` - Random selection

**b) Peer Selection (`peer_selector.go`):**
- Performance-based scoring
- Track download rates, failure rates
- Replace poor performers

**c) Connection Management:**
- Maintain optimal peer count
- Explore new peers periodically
- Replace slow/unresponsive peers

**d) Endgame Mode:**
- Activated when <2% remaining
- Request final pieces from multiple peers
- Send cancel messages when piece completes

#### 3.5 Upload/Seeding (`internal/core/uploader`)

**Components:**
- `Listener` - Accept incoming connections
- `UploadPeer` - Handle upload requests
- `Choker` - Choking algorithm

**Choking Algorithm:**
- Unchoke top 4 uploaders
- Optimistic unchoke 1 random peer (every 30s)
- Re-evaluate every 10 seconds

#### 3.6 Rate Limiting (`internal/core/ratelimit`)

**Implementation:**
- Token bucket algorithm (via `golang.org/x/time/rate`)
- Separate upload/download limiters
- Configurable presets and parsing

### 4. Tracker Support (`internal/tracker`)

**Components:**
- `HTTPTracker` - HTTP/HTTPS trackers (BEP 3)
- `UDPTracker` - UDP trackers (BEP 15)
- `Manager` - Coordinates multiple trackers

**Tracker Manager:**
- Queries all trackers in parallel
- Returns first successful response
- Marks trackers as active/inactive
- Handles both announce and scrape

**UDP Tracker Flow:**
1. Connect (get connection ID, valid 60s)
2. Announce (send stats, receive peers)
3. Scrape (get swarm statistics)

### 5. DHT (`internal/dht`)

**Implementation:** Kademlia DHT (BEP 5)

**Components:**
- `DHT` - Main DHT coordinator
- `RoutingTable` - 160 buckets, K=8 nodes per bucket
- `RPC` - DHT protocol messages

**RPC Messages:**
- `ping/pong` - Keep-alive
- `find_node` - Find nodes close to target
- `get_peers` - Find peers for info_hash
- `announce_peer` - Announce we have info_hash

**Distance Metric:** XOR-based (Kademlia)

**Bootstrap:** Connect to well-known DHT nodes

### 6. Protocol Layer (`internal/protocol`)

**Components:**
- `bencode` - Bencode encoding/decoding
- `magnet` - Magnet link parsing

### 7. User Interface

#### CLI (`cmd/revtorrent`)
- Cobra-based command structure
- Download, info, and other commands
- Configuration file support
- Environment variable support

#### TUI (`internal/ui`)
- Bubbletea-based terminal UI
- Real-time progress bar
- Statistics display
- Keyboard controls

### 8. Infrastructure

#### Graceful Shutdown (`internal/core/shutdown`)

**Features:**
- Signal handling (SIGINT, SIGTERM)
- Ordered shutdown handlers (LIFO)
- Timeout management
- Context cancellation

**Shutdown Flow:**
1. Receive signal
2. Stop accepting new connections
3. Close peer connections
4. Save resume data
5. Announce to trackers (event=stopped)
6. Close storage
7. Wait for goroutines

#### Resume/Pause (`internal/core/controller`)

**State Machine:**
```
Queued → Checking → Downloading → Seeding
   ↓         ↓            ↓           ↓
   └────────→ Paused ←────┘           │
                ↓                     ↓
              Stopped ←───────────────┘
```

**Resume Data:**
- Info hash
- Completed pieces (bitfield)
- Upload/download statistics
- Timestamps

## Data Flow

### Download Flow

```
1. User adds torrent
   ↓
2. Parse .torrent file / fetch metadata (magnet)
   ↓
3. Create storage (preallocate files)
   ↓
4. Announce to trackers / DHT lookup
   ↓
5. Connect to peers
   ↓
6. Exchange bitfields
   ↓
7. Download pieces (rarest-first)
   │  ↓
   │  a. Select piece (strategy)
   │  b. Request blocks from peer
   │  c. Verify piece (SHA-1)
   │  d. Write to storage
   │  e. Broadcast HAVE message
   │  f. Update progress
   ↓
8. Download complete
   ↓
9. Switch to seeding mode
```

### Upload Flow

```
1. Peer connects to listener
   ↓
2. Validate handshake
   ↓
3. Send bitfield (our pieces)
   ↓
4. Receive INTERESTED message
   ↓
5. Choking algorithm decides to unchoke
   ↓
6. Receive REQUEST message
   ↓
7. Read piece from storage
   ↓
8. Apply rate limiting
   ↓
9. Send PIECE message
   ↓
10. Update statistics
```

## Concurrency Model

### Goroutine Usage

**Per Torrent:**
- 1 main control goroutine
- N peer worker goroutines (one per peer)
- 1 tracker announce goroutine
- 1 progress monitoring goroutine

**Global:**
- 1 DHT service goroutine
- 1 peer listener goroutine
- 1 shutdown monitor goroutine

### Synchronization

**Mutexes:**
- Storage operations (`sync.RWMutex`)
- Bitfield updates (`sync.RWMutex`)
- Peer selection (`sync.RWMutex`)
- Statistics (`sync.RWMutex`)

**Channels:**
- Event distribution (buffered)
- Shutdown signals (unbuffered)
- Peer communication (buffered)

## Design Patterns

### 1. Interface-Based Design
All major components implement interfaces for testability and flexibility.

### 2. Functional Options Pattern
Configuration uses functional options for clean API.

```go
client, _ := bittorrent.NewClient(
    bittorrent.WithDownloadPath("./downloads"),
    bittorrent.WithDHT(true),
)
```

### 3. Event-Driven Architecture
Non-blocking event channels for progress monitoring.

### 4. Strategy Pattern
Pluggable piece selection strategies.

### 5. Manager Pattern
Centralized coordination (TrackerManager, ConnectionManager).

### 6. Builder Pattern
Shutdown handlers can be composed.

## Performance Considerations

### Memory Management
- Stream pieces to disk (no full-file buffering)
- Bounded peer connections
- Event channel buffering

### I/O Optimization
- File pre-allocation
- Batch writes where possible
- Async I/O operations

### Network Optimization
- Parallel tracker queries
- Connection pooling
- Smart peer selection
- Rate limiting

## Security Considerations

### Input Validation
- Torrent file parsing (bencode)
- Peer handshake validation
- Info hash verification

### Resource Limits
- Maximum peers per torrent
- Maximum total connections
- Bandwidth limits
- Storage limits (implicit via filesystem)

### No Unsafe Operations
- No use of `unsafe` package
- All array accesses bounds-checked
- Thread-safe data structures

## Error Handling

### Strategy
- Errors propagate up with context (`fmt.Errorf` with `%w`)
- Non-fatal errors logged, execution continues
- Fatal errors stop torrent but not client
- Graceful degradation where possible

### Recovery
- Tracker failures → try next tracker or DHT
- Peer disconnections → connect to new peers
- Piece verification failures → re-download
- Storage errors → pause torrent, alert user

## Future Extensions

### Planned Features
1. **Engine Layer** - Integration of all components
2. **WebUI** - Web-based interface
3. **Streaming** - HTTP server for media streaming
4. **Encryption** - MSE/PE protocol encryption
5. **IPv6** - Dual-stack support
6. **UPnP** - Port forwarding automation
7. **Scripting** - Lua/JavaScript hooks

### Extension Points
- Storage backend (current: filesystem, future: S3, etc.)
- Piece selection strategies (current: 3, easily extensible)
- Trackers (current: HTTP/UDP, future: WebSocket)
- UI (current: CLI/TUI, future: Web/GUI)

## References

- [BEP 3: The BitTorrent Protocol](http://www.bittorrent.org/beps/bep_0003.html)
- [BEP 5: DHT Protocol](http://www.bittorrent.org/beps/bep_0005.html)
- [BEP 9: Extension for Peers to Send Metadata Files](http://www.bittorrent.org/beps/bep_0009.html)
- [BEP 10: Extension Protocol](http://www.bittorrent.org/beps/bep_0010.html)
- [BEP 15: UDP Tracker Protocol](http://www.bittorrent.org/beps/bep_0015.html)
