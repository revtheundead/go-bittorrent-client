package downloader

import (
	"sync"
	"time"

	"github.com/revtheundead/revtorrent/internal/core/piece"
)

// BlockRequest represents a single pending block request
type BlockRequest struct {
	Offset  int
	Length  int
	SentAt  time.Time
	Timeout time.Duration
}

// RequestQueue manages pipelined block requests for a single piece
type RequestQueue struct {
	pieceIndex    int
	requests      []*BlockRequest
	maxPipeline   int
	pieceDownload *piece.PieceDownload
	mu            sync.Mutex
}

// NewRequestQueue creates a new request queue for a piece
func NewRequestQueue(pieceIndex int, pd *piece.PieceDownload, maxPipeline int) *RequestQueue {
	return &RequestQueue{
		pieceIndex:    pieceIndex,
		requests:      make([]*BlockRequest, 0, maxPipeline),
		maxPipeline:   maxPipeline,
		pieceDownload: pd,
	}
}

// FillPipeline fills the request pipeline with block requests
// Calls sendRequest for each new block request
func (rq *RequestQueue) FillPipeline(sendRequest func(offset, length int)) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	// Fill up to maxPipeline concurrent requests
	for len(rq.requests) < rq.maxPipeline {
		offset, length, found := rq.pieceDownload.NextBlock()
		if !found {
			break // All blocks have been requested
		}

		req := &BlockRequest{
			Offset:  offset,
			Length:  length,
			SentAt:  time.Now(),
			Timeout: 30 * time.Second,
		}

		rq.requests = append(rq.requests, req)
		sendRequest(offset, length)
	}
}

// CompleteBlock marks a block as complete and removes it from the queue
func (rq *RequestQueue) CompleteBlock(offset int) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	// Find and remove the request
	for i, req := range rq.requests {
		if req.Offset == offset {
			// Remove from slice
			rq.requests = append(rq.requests[:i], rq.requests[i+1:]...)
			return
		}
	}
}

// CheckTimeouts returns a list of requests that have timed out
func (rq *RequestQueue) CheckTimeouts() []BlockRequest {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	now := time.Now()
	timedOut := []BlockRequest{}

	for _, req := range rq.requests {
		if now.Sub(req.SentAt) > req.Timeout {
			timedOut = append(timedOut, *req)
		}
	}

	return timedOut
}

// Pending returns the number of pending requests
func (rq *RequestQueue) Pending() int {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	return len(rq.requests)
}

// Clear removes all pending requests
func (rq *RequestQueue) Clear() {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	rq.requests = rq.requests[:0]
}
