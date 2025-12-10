# BitTorrent Protocol Implementation

This document describes how revTorrent implements the BitTorrent protocol and related BEPs (BitTorrent Enhancement Proposals).

## Implemented Specifications

- ✅ BEP 3: The BitTorrent Protocol Specification
- ✅ BEP 5: DHT Protocol
- ✅ BEP 9: Extension for Peers to Send Metadata Files
- ✅ BEP 10: Extension Protocol
- ✅ BEP 15: UDP Tracker Protocol

## BEP 3: The BitTorrent Protocol

### Peer ID

revTorrent uses the Azureus-style peer ID:

```
-RT0001-XXXXXXXXXXXX
```

- `-RT0001-` - Client identifier (revTorrent version 0.0.1)
- `XXXXXXXXXXXX` - 12 random bytes

**Implementation:** `internal/tracker/tracker.go`

### Handshake

```
<pstrlen><pstr><reserved><info_hash><peer_id>
```

- `pstrlen` (1 byte) = 19
- `pstr` (19 bytes) = "BitTorrent protocol"
- `reserved` (8 bytes) = Extension bits
  - Bit 20: Extension protocol (BEP 10)
  - Bit 43: DHT support
- `info_hash` (20 bytes) = SHA-1 hash of info dictionary
- `peer_id` (20 bytes) = Peer ID

**Implementation:** `internal/core/peer/peer.go`

### Messages

#### Keep-Alive
```
<len=0>
```

Sent every 2 minutes to maintain connection.

#### Choke (0)
```
<len=1><id=0>
```

Tells peer we're choking them (won't send pieces).

#### Unchoke (1)
```
<len=1><id=1>
```

Tells peer we're unchoking them (will send pieces).

#### Interested (2)
```
<len=1><id=2>
```

We're interested in downloading from peer.

#### Not Interested (3)
```
<len=1><id=3>
```

We're not interested in downloading from peer.

#### Have (4)
```
<len=5><id=4><piece index>
```

Advertises that we have a piece.

#### Bitfield (5)
```
<len=1+X><id=5><bitfield>
```

Sent after handshake, indicates which pieces we have.

#### Request (6)
```
<len=13><id=6><index><begin><length>
```

Request a block of a piece. Typically 16 KiB blocks.

#### Piece (7)
```
<len=9+X><id=7><index><begin><block>
```

Delivers a requested block.

#### Cancel (8)
```
<len=13><id=8><index><begin><length>
```

Cancels a previously sent request (used in endgame).

**Implementation:** `internal/core/peer/messages.go`

### Piece Selection

revTorrent implements multiple strategies:

#### Rarest First (Default)
1. Track piece availability across all peers
2. Select rarest piece we don't have
3. Randomize among equally rare pieces
4. Updates on every HAVE/BITFIELD message

**Benefits:**
- Improves swarm health
- Ensures rare pieces don't disappear
- Better for long-term seeding

#### Sequential
Download pieces in order (0, 1, 2, ...).

**Use case:** Streaming media

#### Random
Random piece selection.

**Use case:** Initial pieces (avoid requesting same as other new peers)

**Implementation:** `internal/core/downloader/strategies.go`

### Endgame Mode

Activated when <2% of pieces remain:

1. Request remaining pieces from ALL peers
2. Send CANCEL when piece completes
3. Reduces final piece wait time significantly

**Implementation:** `internal/core/downloader/endgame.go`

### Choking Algorithm

revTorrent implements the standard choking algorithm:

**Every 10 seconds:**
- Unchoke top 4 peers by upload rate
- Choke all others

**Every 30 seconds (optimistic unchoke):**
- Unchoke 1 random peer
- Gives new peers a chance
- Explores for better peers

**Implementation:** `internal/core/uploader/choker.go`

## BEP 5: DHT Protocol

### Overview

Kademlia-based Distributed Hash Table for trackerless peer discovery.

### Node ID

```
20-byte random node ID (SHA-1 hash recommended)
```

### Routing Table

- 160 buckets (one per bit in 160-bit space)
- Each bucket holds up to K=8 nodes
- Nodes sorted by last-seen time
- Replacement only when node is unreachable

**Implementation:** `internal/dht/routing.go`

### Distance Metric

XOR metric: `distance(A, B) = A XOR B`

Closest nodes have smallest XOR distance.

### RPC Messages

All messages use bencode over UDP.

#### ping Query
```python
{
    "t": "aa",  # transaction ID
    "y": "q",   # query
    "q": "ping",
    "a": {
        "id": "<node_id>"
    }
}
```

Response:
```python
{
    "t": "aa",
    "y": "r",
    "r": {
        "id": "<node_id>"
    }
}
```

#### find_node Query
```python
{
    "t": "aa",
    "y": "q",
    "q": "find_node",
    "a": {
        "id": "<querying_node_id>",
        "target": "<target_node_id>"
    }
}
```

Response includes compact node info:
```
26 bytes per node: 20-byte ID + 4-byte IP + 2-byte port
```

#### get_peers Query
```python
{
    "t": "aa",
    "y": "q",
    "q": "get_peers",
    "a": {
        "id": "<querying_node_id>",
        "info_hash": "<torrent_info_hash>"
    }
}
```

Response either peers or nodes:
```python
{
    "t": "aa",
    "y": "r",
    "r": {
        "id": "<responding_node_id>",
        "token": "<opaque_token>",
        "peers": "<compact_peers>"  # OR
        "nodes": "<compact_nodes>"
    }
}
```

#### announce_peer Query
```python
{
    "t": "aa",
    "y": "q",
    "q": "announce_peer",
    "a": {
        "id": "<querying_node_id>",
        "info_hash": "<torrent_info_hash>",
        "port": 6881,
        "token": "<token_from_get_peers>"
    }
}
```

**Implementation:** `internal/dht/rpc.go`

### Bootstrap Process

1. Connect to well-known bootstrap nodes:
   - `router.bittorrent.com:6881`
   - `dht.transmissionbt.com:6881`
   - `router.utorrent.com:6881`

2. Find nodes close to our node ID
3. Populate routing table
4. Start accepting queries

**Implementation:** `internal/dht/bootstrap.go`

## BEP 9: Metadata Extension

Allows downloading .torrent metadata from peers (for magnet links).

### Extension Handshake

```python
{
    "m": {
        "ut_metadata": 1  # Extension ID
    },
    "metadata_size": 12345  # Size in bytes
}
```

### Metadata Messages

#### Request
```
<len><ext_id><msg_type=0><piece>
```

#### Data
```
<len><ext_id><msg_type=1><piece><total_size><data>
```

#### Reject
```
<len><ext_id><msg_type=2><piece>
```

Metadata is split into 16 KiB pieces.

**Implementation:** `internal/core/peer/extension.go`

## BEP 10: Extension Protocol

Allows peers to negotiate extensions.

### Extended Handshake (Message ID 20, 0)

```python
{
    "m": {
        "ut_metadata": 1,
        "ut_pex": 2
    },
    "v": "revTorrent 0.0.1",
    "reqq": 250,  # Request queue depth
}
```

**Implementation:** `internal/core/peer/extension.go`

## BEP 15: UDP Tracker Protocol

### Connection Flow

#### 1. Connect Request

```
Offset  Size            Name            Value
0       64-bit int      protocol_id     0x41727101980
8       32-bit int      action          0 (connect)
12      32-bit int      transaction_id  random
```

#### 2. Connect Response

```
Offset  Size            Name            Value
0       32-bit int      action          0
4       32-bit int      transaction_id  from request
8       64-bit int      connection_id   use for 60 seconds
```

#### 3. Announce Request

```
Offset  Size            Name            Value
0       64-bit int      connection_id   from connect
8       32-bit int      action          1 (announce)
12      32-bit int      transaction_id  random
16      20 bytes        info_hash
36      20 bytes        peer_id
56      64-bit int      downloaded
64      64-bit int      left
72      64-bit int      uploaded
80      32-bit int      event          0=none, 1=completed, 2=started, 3=stopped
84      32-bit int      IP              0 = default
88      32-bit int      key             random
92      32-bit int      num_want        -1 = default
96      16-bit int      port
```

#### 4. Announce Response

```
Offset  Size            Name            Value
0       32-bit int      action          1
4       32-bit int      transaction_id
8       32-bit int      interval        seconds
12      32-bit int      leechers
16      32-bit int      seeders
20+     N * 6 bytes     peers           compact format
```

Peers: 4-byte IP + 2-byte port (big-endian)

#### 5. Scrape (Optional)

Similar structure, action=2.

**Implementation:** `internal/tracker/udp.go`

## HTTP Tracker Protocol

### Announce Request

```
GET /announce?info_hash=...&peer_id=...&port=6881&uploaded=0&downloaded=0&left=1234&compact=1
```

### Announce Response (Bencode)

```python
{
    "interval": 1800,  # Re-announce interval (seconds)
    "complete": 50,     # Seeders
    "incomplete": 20,   # Leechers
    "peers": "..."      # Compact or dictionary format
}
```

**Compact format:** 6 bytes per peer (4-byte IP + 2-byte port)

**Dictionary format:**
```python
{
    "peers": [
        {"ip": "1.2.3.4", "port": 6881},
        {"ip": "5.6.7.8", "port": 6882}
    ]
}
```

**Implementation:** `internal/tracker/http.go`

## Bencode

### Encoding Rules

- **Integers:** `i<number>e`
  - Example: `i42e` = 42
  - Example: `i-42e` = -42

- **Strings:** `<length>:<string>`
  - Example: `4:spam` = "spam"
  - Example: `0:` = ""

- **Lists:** `l<elements>e`
  - Example: `l4:spam4:eggse` = ["spam", "eggs"]

- **Dictionaries:** `d<key1><value1>...e`
  - Keys must be strings, sorted lexicographically
  - Example: `d3:cow3:moo4:spam4:eggse` = {"cow": "moo", "spam": "eggs"}

### Torrent File Structure

```python
{
    "announce": "http://tracker.example.com:8080/announce",
    "announce-list": [  # Multi-tracker support
        ["http://tracker1.com/announce"],
        ["http://tracker2.com/announce"]
    ],
    "info": {
        "name": "ubuntu-20.04.iso",
        "piece length": 262144,  # 256 KiB
        "pieces": "<binary_sha1_hashes>",  # 20 bytes per piece

        # Single file:
        "length": 1234567890,

        # OR multi-file:
        "files": [
            {"length": 12345, "path": ["dir1", "file1.txt"]},
            {"length": 67890, "path": ["dir1", "file2.txt"]}
        ]
    },
    "creation date": 1609459200,  # Unix timestamp (optional)
    "comment": "Ubuntu 20.04 LTS",  # Optional
    "created by": "revTorrent 0.0.1"  # Optional
}
```

**Info hash:** SHA-1 hash of bencoded info dictionary

**Implementation:** `internal/protocol/bencode/bencode.go`

## Magnet Links

Format:
```
magnet:?xt=urn:btih:<info-hash>&dn=<name>&tr=<tracker-url>
```

Parameters:
- `xt` (exact topic) - `urn:btih:<40-hex-chars>` or `urn:btih:<32-base32-chars>`
- `dn` (display name) - URL-encoded torrent name
- `tr` (tracker) - Tracker URL (can appear multiple times)
- `xl` (exact length) - Size in bytes
- `ws` (web seed) - HTTP/FTP URL

Example:
```
magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12
&dn=Ubuntu+20.04&tr=http://tracker.example.com/announce
```

**Implementation:** `internal/protocol/magnet/magnet.go`

## Rate Limiting

Token bucket algorithm via `golang.org/x/time/rate`:

- Separate buckets for upload and download
- Tokens represent bytes
- Refill at configured rate (bytes/second)
- Request waits if insufficient tokens

**Implementation:** `internal/core/ratelimit/limiter.go`

## Security Considerations

### Piece Verification

Every piece is verified with SHA-1 hash from .torrent file:
- Hash mismatch → discard piece and re-download
- Prevents malicious/corrupted data

### Handshake Validation

- Verify info hash matches
- Validate peer ID format
- Reject invalid/malformed messages

### Resource Limits

- Max peers per torrent (default: 50)
- Max total connections (default: 200)
- Bandwidth limits (configurable)
- Request queue limits

### No Code Execution

- No `eval()` or similar
- All input validated before parsing
- Bencode parser bounds-checked

## Performance Optimizations

### Network

- Parallel tracker queries
- Connection pooling
- Pipeline requests (request queue)
- Smart peer selection

### I/O

- Pre-allocate files
- Batch writes
- Asynchronous I/O
- Memory-mapped files (future)

### CPU

- Parallel piece hashing
- Efficient bitfield operations
- Minimize allocations

## Testing

Run protocol tests:
```bash
go test ./internal/protocol/...
go test ./internal/tracker/...
go test ./internal/dht/...
```

## References

- [BitTorrent Specification (BEP 3)](http://www.bittorrent.org/beps/bep_0003.html)
- [DHT Protocol (BEP 5)](http://www.bittorrent.org/beps/bep_0005.html)
- [Extension Protocol (BEP 10)](http://www.bittorrent.org/beps/bep_0010.html)
- [UDP Tracker (BEP 15)](http://www.bittorrent.org/beps/bep_0015.html)
- [Index of all BEPs](http://www.bittorrent.org/beps/bep_0000.html)
