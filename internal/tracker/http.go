package tracker

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/revtheundead/revtorrent/internal/protocol/bencode"
)

// HTTPTracker implements the HTTP tracker protocol (BEP 3)
type HTTPTracker struct {
	url    *url.URL
	client *http.Client
}

// NewHTTPTracker creates a new HTTP tracker client
func NewHTTPTracker(trackerURL string) (*HTTPTracker, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid tracker URL: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("not an HTTP tracker URL: %s", u.Scheme)
	}

	return &HTTPTracker{
		url:    u,
		client: &http.Client{Timeout: 15},
	}, nil
}

// Announce performs a tracker announce and returns peer list
func (t *HTTPTracker) Announce(req *AnnounceRequest) (*TrackerResponse, error) {
	announceURL := t.buildAnnounceURL(req)

	resp, err := t.client.Get(announceURL)
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

	return t.parseResponse(body)
}

// Scrape gets statistics about torrents
func (t *HTTPTracker) Scrape(infoHashes [][20]byte) (*ScrapeResponse, error) {
	// Build scrape URL by replacing "announce" with "scrape"
	scrapeURL := t.url.String()
	// Simple replacement (may not work for all trackers)
	if len(scrapeURL) > 8 && scrapeURL[len(scrapeURL)-8:] == "announce" {
		scrapeURL = scrapeURL[:len(scrapeURL)-8] + "scrape"
	}

	u, err := url.Parse(scrapeURL)
	if err != nil {
		return nil, err
	}

	// Add info_hash parameters
	q := u.Query()
	for _, hash := range infoHashes {
		q.Add("info_hash", string(hash[:]))
	}
	u.RawQuery = q.Encode()

	resp, err := t.client.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("scrape request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading scrape response failed: %w", err)
	}

	return t.parseScrapeResponse(body)
}

// buildAnnounceURL constructs the announce URL with all parameters
func (t *HTTPTracker) buildAnnounceURL(req *AnnounceRequest) string {
	params := url.Values{}
	params.Add("info_hash", string(req.InfoHash[:]))
	params.Add("peer_id", string(req.PeerID[:]))
	params.Add("port", strconv.Itoa(int(req.Port)))
	params.Add("uploaded", strconv.FormatInt(int64(req.Uploaded), 10))
	params.Add("downloaded", strconv.FormatInt(int64(req.Downloaded), 10))
	params.Add("left", strconv.FormatInt(int64(req.Left), 10))
	params.Add("compact", "1")

	if req.Event != "" {
		params.Add("event", req.Event)
	}

	u := *t.url
	if u.RawQuery == "" {
		u.RawQuery = params.Encode()
	} else {
		u.RawQuery = u.RawQuery + "&" + params.Encode()
	}

	return u.String()
}

// parseResponse parses the bencoded tracker response
func (t *HTTPTracker) parseResponse(data []byte) (*TrackerResponse, error) {
	decoded, err := bencode.Decode(string(data))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("tracker response is not a dictionary")
	}

	// Check for failure
	if failureReason, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("tracker failure: %s", failureReason)
	}

	// Parse interval
	interval := int64(1800) // Default 30 minutes
	if rawInterval, ok := dict["interval"].(int64); ok {
		interval = rawInterval
	}

	// Parse peers
	var peers []Peer

	if peersStr, ok := dict["peers"].(string); ok {
		// Compact format (6 bytes per peer)
		peers = parseCompactPeers([]byte(peersStr))
	} else if peersList, ok := dict["peers"].([]interface{}); ok {
		// Dictionary format
		peers = parseDictionaryPeers(peersList)
	}

	// Optional: parse seeders and leechers
	seeders := 0
	leechers := 0
	if complete, ok := dict["complete"].(int64); ok {
		seeders = int(complete)
	}
	if incomplete, ok := dict["incomplete"].(int64); ok {
		leechers = int(incomplete)
	}

	return &TrackerResponse{
		Interval: interval,
		Peers:    peers,
		Seeders:  seeders,
		Leechers: leechers,
	}, nil
}

// parseScrapeResponse parses the scrape response
func (t *HTTPTracker) parseScrapeResponse(data []byte) (*ScrapeResponse, error) {
	decoded, err := bencode.Decode(string(data))
	if err != nil {
		return nil, fmt.Errorf("bencode decode failed: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("scrape response is not a dictionary")
	}

	filesDict, ok := dict["files"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("scrape response missing files")
	}

	files := make([]ScrapeFile, 0)
	for _, fileData := range filesDict {
		fileDict, ok := fileData.(map[string]interface{})
		if !ok {
			continue
		}

		file := ScrapeFile{}
		if complete, ok := fileDict["complete"].(int64); ok {
			file.Seeders = int(complete)
		}
		if downloaded, ok := fileDict["downloaded"].(int64); ok {
			file.Completed = int(downloaded)
		}
		if incomplete, ok := fileDict["incomplete"].(int64); ok {
			file.Leechers = int(incomplete)
		}

		files = append(files, file)
	}

	return &ScrapeResponse{
		Files: files,
	}, nil
}

// parseCompactPeers parses compact peer format (6 bytes per peer)
func parseCompactPeers(data []byte) []Peer {
	peers := make([]Peer, 0)

	for i := 0; i+6 <= len(data); i += 6 {
		ip := net.IPv4(data[i], data[i+1], data[i+2], data[i+3])
		port := uint16(data[i+4])<<8 | uint16(data[i+5])

		peers = append(peers, Peer{
			IP:   ip,
			Port: port,
		})
	}

	return peers
}

// parseDictionaryPeers parses dictionary-format peer list
func parseDictionaryPeers(peersList []interface{}) []Peer {
	peers := make([]Peer, 0)

	for _, rawPeer := range peersList {
		peerDict, ok := rawPeer.(map[string]interface{})
		if !ok {
			continue
		}

		ipStr, ok := peerDict["ip"].(string)
		if !ok {
			continue
		}

		portInt, ok := peerDict["port"].(int64)
		if !ok {
			continue
		}

		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}

		peers = append(peers, Peer{
			IP:   ip,
			Port: uint16(portInt),
		})
	}

	return peers
}
