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

	"github.com/revtheundead/go-bittorrent-client/internal/torrent"
	"github.com/revtheundead/go-bittorrent-client/pkg/bencode"
)

const (
	PeerIDSize       = 20                // 20 bytes per BitTorrent spec
	PeerIDPrefix     = "-GT0001-"        // 8-byte client ID prefix
	PeerIDPrefixSize = len(PeerIDPrefix) // 8 bytes
	PeerIDRandomSize = PeerIDSize - PeerIDPrefixSize

	ClientPort = 6881 // required port for the exercise

	CompactPeerEntrySize = 6 // 4 bytes IP + 2 bytes port

	IPv4Octets = 4 // number of bytes in an IPv4 address
	PortBytes  = 2 // number of bytes in a port (big-endian)
)

type Peer struct {
	IP   net.IP
	Port uint16
}

type TrackerResponse struct {
	Interval int64
	Peers    []Peer
}

// GeneratePeerID returns a random 20-byte peer ID for this client
func GeneratePeerID() [PeerIDSize]byte {
	var id [PeerIDSize]byte

	// Copy client prefix
	copy(id[:PeerIDPrefixSize], []byte(PeerIDPrefix))

	// Fill remaining random bytes
	if _, err := rand.Read(id[PeerIDPrefixSize:]); err != nil {
		copy(id[PeerIDPrefixSize:], []byte("fallback-entropy"))
	}

	return id
}

// Announce contacts the tracker for the given torrent metainfo and peer ID,
// and returns the tracker response (interval + list of peers).
func Announce(meta *torrent.Metainfo, peerID [PeerIDSize]byte) (*TrackerResponse, error) {
	announceURL, err := buildAnnounceURL(meta, peerID, ClientPort)
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
func buildAnnounceURL(meta *torrent.Metainfo, peerID [PeerIDSize]byte, port uint16) (string, error) {
	u, err := url.Parse(meta.Announce)
	if err != nil {
		return "", fmt.Errorf("invalid announce URL %q: %w", meta.Announce, err)
	}

	q := u.Query()

	// info_hash is the *raw* 20 bytes, not hex.
	// url.Values.Encode will URL-encode it correctly
	q.Set("info_hash", string(meta.InfoHash[:]))

	// peer_id is the 20-byte ID identifying this client
	q.Set("peer_id", string(peerID[:]))

	// Listening port for this client. We use 6881.
	q.Set("port", strconv.Itoa(int(port)))

	// No data transferred yet.
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")

	// Bytes left to download – for a fresh client, this is the full length.
	q.Set("left", strconv.FormatInt(meta.Info.Length, 10))

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

	if len(b)%CompactPeerEntrySize != 0 {
		return nil, fmt.Errorf("invalid compact peer list length %d", len(b))
	}

	numPeers := len(b) / CompactPeerEntrySize
	peers := make([]Peer, 0, numPeers)

	for i := 0; i < numPeers; i++ {
		offset := i * CompactPeerEntrySize

		ip := net.IPv4(
			b[offset],
			b[offset+1],
			b[offset+2],
			b[offset+3],
		)

		port := binary.BigEndian.Uint16(
			b[offset+IPv4Octets : offset+IPv4Octets+PortBytes],
		)

		peers = append(peers, Peer{
			IP:   ip,
			Port: port,
		})
	}

	return peers, nil
}
