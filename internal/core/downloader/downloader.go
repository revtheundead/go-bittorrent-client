/*
* THIS IS THE LEGACY DOWNLOAD LOGIC, IT NEEDS TO BE REMOVED/REWORKED
 */

package downloader

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/peer"
	"github.com/revtheundead/revtorrent/internal/torrent"
	"github.com/revtheundead/revtorrent/internal/tracker"
)

const (
	maxWorkers          = 8 // max concurrent peers to use
	maxAttemptsPerPiece = 3
)

// pieceJob represents work for a single piece, with retry count
type pieceJob struct {
	Index   int
	Attempt int
}

// pieceResult is what workers send back to the coordinator
type pieceResult struct {
	Index   int
	Attempt int
	Data    []byte
	Err     error
}

// DownloadAll downloads the *entire* file described by meta and returns
// its content as a byte slice. It uses multiple peers and retries pieces
// on failure up to maxAttemptsPerPiece.
func DownloadAll(meta *torrent.Metainfo, peerID [20]byte) ([]byte, error) {
	info := &meta.Info

	if info.PieceLength <= 0 {
		return nil, fmt.Errorf("invalid piece length %d", info.PieceLength)
	}

	// Compute number of pieces (ceiling division).
	numPieces := int((info.Length + info.PieceLength - 1) / info.PieceLength)
	if numPieces <= 0 {
		return nil, fmt.Errorf("no pieces to download")
	}

	// Allocate full file buffer in memory
	full := make([]byte, info.Length)

	// Get a list of peers from the tracker.
	tr, err := tracker.AnnounceTorrent(meta, peerID)
	if err != nil {
		return nil, fmt.Errorf("tracker announce failed: %w", err)
	}
	if len(tr.Peers) == 0 {
		return nil, fmt.Errorf("tracker returned no peers")
	}

	workerCount := maxWorkers
	if len(tr.Peers) < workerCount {
		workerCount = len(tr.Peers)
	}
	if workerCount == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	// Select peers for workers (random subset)
	peers := pickRandomPeers(tr.Peers, workerCount)

	jobs := make(chan *pieceJob, numPieces)
	results := make(chan *pieceResult)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	// Start one worker per peer (up to workerCount)
	for _, p := range peers {
		peer := p
		go func() {
			defer wg.Done()
			workerLoop(meta, peerID, peer, jobs, results)
		}()
	}

	// Close results when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Seed initial jobs for all pieces
	go func() {
		// see available piece picker strategies in strategies.go
		order := buildPieceOrder(numPieces, StrategyRandom)
		for _, idx := range order {
			jobs <- &pieceJob{Index: idx, Attempt: 0}
		}
		// Don't close jobs here, we way enqueue retries
	}()

	// Start recording stats
	stats := NewDownloadStats()
	statsDone := make(chan struct{})
	go stats.LogPeriodically(time.Second, statsDone)
	defer close(statsDone)

	completed := 0
	completedPieces := make([]bool, numPieces)

	for res := range results {
		idx := res.Index

		if res.Err == nil {
			// Successful piece download
			if !completedPieces[idx] {
				// Copy piece into final buffer at correct offset
				offset := int64(idx) * info.PieceLength
				copy(full[offset:offset+int64(len(res.Data))], res.Data)

				// Update download stats
				stats.Add(len(res.Data))

				// Mark as complete
				completedPieces[idx] = true
				completed++
			}

			if completed == numPieces {
				// All pieces done, no more jobs needed
				close(jobs)
				break
			}
			continue
		}

		// An error occured, retry
		if completedPieces[idx] {
			// Already have this piece from another attempt, ignore error
			continue
		}

		if res.Attempt+1 >= maxAttemptsPerPiece {
			// Give up on this piece
			close(jobs)
			return nil, fmt.Errorf("piece %d failed after %d attempts: %w", idx, res.Attempt+1, res.Err)
		}

		// Retry the piece with increased attempt count
		jobs <- &pieceJob{
			Index:   idx,
			Attempt: res.Attempt + 1,
		}
	}

	if completed != numPieces {
		return nil, fmt.Errorf("download incomplete: got %d/%d pieces", completed, numPieces)
	}

	return full, nil
}

// workerLoop repeatedly takes piece jobs from jobs, downloads them from
// the given peer, and sends results back over results. Each worker manages
// a small set of reusable connections.
func workerLoop(
	meta *torrent.Metainfo,
	peerID [20]byte,
	p tracker.Peer,
	jobs <-chan *pieceJob,
	results chan<- *pieceResult,
) {
	// Establish a persistent connection
	c, err := peer.NewClient(meta.InfoHash, peerID, p)
	if err != nil {
		log.Printf("worker %s failed handshake: %v", c.Addr, err)
		return
	}
	defer c.Conn.Close()

	for job := range jobs {
		index := job.Index

		data, err := c.DownloadPiece(&meta.Info, index)
		if err != nil {
			results <- &pieceResult{
				Index:   index,
				Attempt: job.Attempt,
				Err:     err,
			}

			// Drop connection and reconnect on next iteration
			c.Conn.Close()
			c = nil
			c, _ = peer.NewClient(meta.InfoHash, peerID, p)
			continue
		}

		results <- &pieceResult{
			Index:   index,
			Attempt: job.Attempt,
			Data:    data,
			Err:     nil,
		}
	}
}

func pickRandomPeers(all []tracker.Peer, count int) []tracker.Peer {
	if len(all) <= count {
		return all
	}
	out := make([]tracker.Peer, len(all))
	copy(out, all)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out[:count]
}
