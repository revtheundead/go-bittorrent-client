package tracker

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/revtheundead/revtorrent/internal/core/torrent"
	"github.com/revtheundead/revtorrent/internal/protocol/bencode"
)

const (
	peerIDSize           = 20                // 20 bytes per BitTorrent spec
	peerIDPrefix         = "-RT0001-"        // 8-byte revTorrent client ID prefix
	peerIDPrefixSize     = len(peerIDPrefix) // 8 bytes
	clientPort           = 6881              // operating port number
	compactPeerEntrySize = 6                 // 4 bytes IP + 2 bytes port
	ipv4Octets           = 4                 // number of bytes in an IPv4 address
	portBytes            = 2                 // number of bytes in a port (big-endian)
)

type Peer struct {
	IP   net.IP
	Port uint16
}

type TrackerResponse struct {
	Interval int64
	Peers    []Peer
	Seeders  int // Optional: number of seeders
	Leechers int // Optional: number of leechers
}

// GeneratePeerID returns a random 20-byte peer ID for this client
func GeneratePeerID() [peerIDSize]byte {
	var id [peerIDSize]byte

	// Copy client prefix
	copy(id[:peerIDPrefixSize], []byte(peerIDPrefix))

	// Fill remaining random bytes
	if _, err := rand.Read(id[peerIDPrefixSize:]); err != nil {
		copy(id[peerIDPrefixSize:], []byte("fallback-entropy"))
	}

	return id
}

// Announce contacts the tracker for the given torrent metainfo and peer ID,
// and returns the tracker response (interval + list of peers).
func AnnounceTorrent(meta *torrent.Metainfo, peerID [peerIDSize]byte) (*TrackerResponse, error) {
	return announceWithParams(meta.Announce, meta.InfoHash, meta.Info.Length, peerID)
}

// AnnounceMagnet contacts the tracker given a raw tracker URL and info hash,
// using left=0 because we don't know the file length from the magnet link.
func AnnounceMagnet(trackerURL string, infoHash [20]byte, peerID [20]byte) (*TrackerResponse, error) {
	const unknownLength int64 = 0
	return announceWithParams(trackerURL, infoHash, unknownLength, peerID)
}

// announceWithParams makes a GET request to the tracker in the given URL and
// returns a list of peers
func announceWithParams(announceURL string, infoHash [20]byte, length int64, peerID [peerIDSize]byte) (*TrackerResponse, error) {
	announceURL, err := buildAnnounceURL(announceURL, infoHash, length, peerID, clientPort)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(announceURL)
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("tracker returned %s: %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading tracker response failed: %w", err)
	}

	decoded, err := bencode.Decode(string(body))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("tracker response is not a dictionary (got %T)", decoded)
	}

	// Handle protocol-level failure, even if HTTP status is 200.
	if fr, ok := dict["failure reason"]; ok {
		if msg, ok := fr.(string); ok {
			return nil, fmt.Errorf("tracker failure: %s", msg)
		}
		return nil, fmt.Errorf("tracker failure with non-string reason: %#v", fr)
	}

	// interval: how often to re-announce (seconds).
	var interval int64
	if rawInterval, ok := dict["interval"]; ok {
		if iv, ok := rawInterval.(int64); ok {
			interval = iv
		} else {
			return nil, fmt.Errorf("tracker 'interval' has unexpected type %T", rawInterval)
		}
	}

	peers, err := getPeers(dict)
	if err != nil {
		return nil, err
	}

	return &TrackerResponse{
		Interval: interval,
		Peers:    peers,
	}, nil
}

// buildAnnounceURL constructs the tracker announce URL with the required
// query parameters
func buildAnnounceURL(announceURL string, infoHash [20]byte, length int64, peerID [peerIDSize]byte, port uint16) (string, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return "", fmt.Errorf("invalid announce URL %q: %w", announceURL, err)
	}

	q := u.Query()

	// info_hash is the *raw* 20 bytes, not hex.
	// url.Values.Encode will URL-encode it correctly
	q.Set("info_hash", string(infoHash[:]))

	// peer_id is the 20-byte ID identifying this client
	q.Set("peer_id", string(peerID[:]))

	// Listening port for this client. We use 6881.
	q.Set("port", strconv.Itoa(int(port)))

	// No data transferred yet.
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")

	// Bytes left to download – for a fresh client, this is the full length.
	left := length
	if left <= 0 {
		// For magnets (or unknown size), trackers usually expect a positive "left"
		left = 1
	}
	q.Set("left", strconv.FormatInt(left, 10))

	// Use compact peer representation
	q.Set("compact", "1")

	u.RawQuery = q.Encode()
	return u.String(), nil
}

// getPeersFromDict extracts and parses the compact peers list from the
// tracker response dictionary. Each peer is 6 bytes: 4 bytes IPv4 + 2
// bytes port (big-endian).
func getPeers(dict map[string]interface{}) ([]Peer, error) {
	rawPeers, ok := dict["peers"]
	if !ok {
		return nil, fmt.Errorf("tracker response missing 'peers'")
	}

	peersStr, ok := rawPeers.(string)
	if !ok {
		return nil, fmt.Errorf("'peers' field is not a string (got %T)", rawPeers)
	}

	b := []byte(peersStr)

	if len(b)%compactPeerEntrySize != 0 {
		return nil, fmt.Errorf("invalid compact peer list length %d", len(b))
	}

	numPeers := len(b) / compactPeerEntrySize
	peers := make([]Peer, 0, numPeers)

	for i := 0; i < numPeers; i++ {
		offset := i * compactPeerEntrySize

		ip := net.IPv4(
			b[offset],
			b[offset+1],
			b[offset+2],
			b[offset+3],
		)

		port := binary.BigEndian.Uint16(
			b[offset+ipv4Octets : offset+ipv4Octets+portBytes],
		)

		peers = append(peers, Peer{
			IP:   ip,
			Port: port,
		})
	}

	return peers, nil
}
