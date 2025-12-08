package engine

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/core/peer"
	"github.com/revtheundead/revtorrent/internal/tracker"
)

// Default public trackers to use as fallback for magnet links
var defaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.bittor.pw:1337/announce",
	"udp://public.popcorn-tracker.org:6969/announce",
}

// FetchMetadata fetches metadata from peers for a magnet link using BEP 9
func (e *Engine) FetchMetadata(ctx context.Context, infoHash [20]byte, trackerURLs []string) ([]byte, error) {
	e.logger.Debug("fetching metadata", "info_hash", fmt.Sprintf("%x", infoHash))

	// Add default trackers if none provided
	if len(trackerURLs) == 0 {
		e.logger.Debug("no trackers in magnet link, using defaults", "count", len(defaultTrackers))
		trackerURLs = defaultTrackers
	}

	// Get peers from DHT first
	var peers []tracker.Peer

	if e.dht != nil {
		e.logger.Debug("querying DHT for peers")
		dhtPeers, err := e.dht.GetPeers(infoHash)
		if err != nil {
			e.logger.Debug("DHT peer lookup failed", "error", err)
		} else if len(dhtPeers) > 0 {
			// Convert DHT peers to tracker peers
			for _, dp := range dhtPeers {
				peers = append(peers, tracker.Peer{
					IP:   dp.IP,
					Port: uint16(dp.Port),
				})
			}
			e.logger.Debug("found peers from DHT", "count", len(dhtPeers))
		} else {
			e.logger.Debug("DHT returned no peers")
		}
	}

	// Also try trackers if provided (always try if we have few peers)
	if len(trackerURLs) > 0 {
		e.logger.Debug("querying trackers for peers", "count", len(trackerURLs))

		for _, trackerURL := range trackerURLs {
			// Skip empty tracker URLs
			if trackerURL == "" {
				e.logger.Debug("skipping empty tracker URL")
				continue
			}

			e.logger.Debug("trying tracker", "url", trackerURL)

			// Create a tracker request
			req := tracker.AnnounceRequest{
				InfoHash:   infoHash,
				PeerID:     tracker.GeneratePeerID(),
				Port:       uint16(e.config.ListenPort),
				Uploaded:   0,
				Downloaded: 0,
				Left:       999999999, // Unknown size for magnet links
				Compact:    true,
				Event:      "started",
			}

			// Try appropriate tracker type based on URL scheme
			var resp *tracker.TrackerResponse
			var err error

			if len(trackerURL) > 6 && trackerURL[:6] == "udp://" {
				udpTracker, newErr := tracker.NewUDPTracker(trackerURL)
				if newErr != nil {
					e.logger.Warn("failed to create UDP tracker", "url", trackerURL, "error", newErr)
					continue
				}
				resp, err = udpTracker.Announce(&req)
			} else {
				httpTracker, newErr := tracker.NewHTTPTracker(trackerURL)
				if newErr != nil {
					e.logger.Warn("failed to create HTTP tracker", "url", trackerURL, "error", newErr)
					continue
				}
				resp, err = httpTracker.Announce(&req)
			}

			if err != nil {
				e.logger.Debug("tracker announce failed", "url", trackerURL, "error", err)
				continue
			}

			if len(resp.Peers) > 0 {
				peers = append(peers, resp.Peers...)
				e.logger.Debug("found peers from tracker", "url", trackerURL, "count", len(resp.Peers))
			} else {
				e.logger.Debug("tracker returned no peers", "url", trackerURL)
			}

			// Stop if we have enough peers
			if len(peers) >= 50 {
				break
			}
		}
	}

	if len(peers) == 0 {
		dhtUsed := "no"
		if e.dht != nil {
			dhtUsed = "yes"
		}
		return nil, fmt.Errorf("no peers found for info hash (DHT: %s, trackers tried: %d)",
			dhtUsed, len(trackerURLs))
	}

	e.logger.Debug("total peers found", "count", len(peers))

	// Try to fetch metadata from peers in parallel
	// Aggressive settings for restrictive networks
	maxConcurrent := 20                 // Try 20 peers at once (was 10)
	maxAttempts := min(len(peers), 100) // Try up to 100 peers total (was 50)

	type result struct {
		metadata []byte
		addr     string
		err      error
	}

	resultChan := make(chan result, maxConcurrent)
	activeChan := make(chan struct{}, maxConcurrent)

	// Context for peer attempts with shorter timeout
	peerCtx, peerCancel := context.WithCancel(ctx)
	defer peerCancel()

	var wg sync.WaitGroup
	launched := 0
	received := 0
	failureReasons := make(map[string]int)

	// Launch goroutines to try peers in parallel
	go func() {
		defer func() {
			// Wait for all workers to finish, then close the channel
			wg.Wait()
			close(resultChan)
		}()

		for i := 0; i < maxAttempts; i++ {
			select {
			case <-peerCtx.Done():
				// Context cancelled, stop launching new workers
				return
			case activeChan <- struct{}{}:
				p := peers[i]
				addr := fmt.Sprintf("%s:%d", p.IP, p.Port)
				launched++

				wg.Add(1)
				go func(addr string, attempt int) {
					defer wg.Done()
					defer func() { <-activeChan }()

					e.logger.Debug("attempting to fetch metadata from peer", "addr", addr, "attempt", attempt, "of", maxAttempts)

					metadata, err := e.fetchMetadataFromPeer(peerCtx, addr, infoHash)

					// Only send result if channel is still open
					select {
					case resultChan <- result{metadata: metadata, addr: addr, err: err}:
					case <-peerCtx.Done():
						// Context cancelled, don't send result
					}
				}(addr, i+1)
			}
		}
	}()

	// Collect results
	for res := range resultChan {
		received++
		if res.err != nil {
			e.logger.Debug("failed to fetch from peer", "addr", res.addr, "error", res.err)
			failureReasons[res.err.Error()]++
		} else {
			e.logger.Info("successfully fetched metadata", "addr", res.addr, "size", len(res.metadata))
			peerCancel() // Cancel other attempts
			return res.metadata, nil
		}

		// Check context
		select {
		case <-ctx.Done():
			e.logger.Warn("metadata fetch cancelled", "reason", ctx.Err())
			return nil, ctx.Err()
		default:
		}
	}

	// Log summary of failure reasons
	e.logger.Warn("metadata fetch failed from all peers", "attempts", received, "failure_reasons", failureReasons)

	return nil, fmt.Errorf("failed to fetch metadata from any peer (tried %d peers)", received)
}

// fetchMetadataFromPeer attempts to fetch metadata from a single peer
func (e *Engine) fetchMetadataFromPeer(ctx context.Context, addr string, infoHash [20]byte) ([]byte, error) {
	// Connect to peer with very short timeout for restrictive networks
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second) // Reduced from 3s to 2s
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set overall deadline (shorter for each peer attempt)
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(10 * time.Second)) // Reduced from 15s to 10s
	}

	// Generate our peer ID
	ourPeerID := tracker.GeneratePeerID()

	// Send handshake
	ourHandshake := peer.NewHandshake(infoHash, ourPeerID)
	if _, err := conn.Write(ourHandshake.Serialize()); err != nil {
		return nil, fmt.Errorf("failed to send handshake: %w", err)
	}

	// Read remote handshake
	remoteHandshake, err := peer.ReadRemoteHandshake(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read handshake: %w", err)
	}

	// Verify info hash
	if remoteHandshake.InfoHash != infoHash {
		return nil, fmt.Errorf("info hash mismatch")
	}

	// Check if peer supports extensions
	if !remoteHandshake.SupportsExtensions() {
		return nil, fmt.Errorf("peer does not support extensions")
	}

	// Send extension handshake
	if err := peer.SendExtensionHandshake(conn); err != nil {
		return nil, fmt.Errorf("failed to send extension handshake: %w", err)
	}

	// Receive extension handshake
	peerUtMetadataID, err := peer.ReceiveExtensionHandshake(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to receive extension handshake: %w", err)
	}

	// Fetch metadata
	metadata, err := peer.FetchMetadata(conn, peerUtMetadataID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	return metadata, nil
}
