package ratelimit

import (
	"context"
	"io"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Direction represents the direction of data transfer
type Direction int

const (
	Download Direction = iota
	Upload
)

// Limiter manages rate limiting for both upload and download
type Limiter struct {
	downloadLimiter *rate.Limiter
	uploadLimiter   *rate.Limiter
	downloadLimit   int64 // bytes per second (0 = unlimited)
	uploadLimit     int64 // bytes per second (0 = unlimited)
	mu              sync.RWMutex
}

// NewLimiter creates a new rate limiter
// downloadBps and uploadBps are bytes per second (0 means unlimited)
func NewLimiter(downloadBps, uploadBps int64) *Limiter {
	l := &Limiter{
		downloadLimit: downloadBps,
		uploadLimit:   uploadBps,
	}

	// Create download limiter
	if downloadBps > 0 {
		l.downloadLimiter = rate.NewLimiter(rate.Limit(downloadBps), int(downloadBps))
	} else {
		l.downloadLimiter = rate.NewLimiter(rate.Inf, 0) // Unlimited
	}

	// Create upload limiter
	if uploadBps > 0 {
		l.uploadLimiter = rate.NewLimiter(rate.Limit(uploadBps), int(uploadBps))
	} else {
		l.uploadLimiter = rate.NewLimiter(rate.Inf, 0) // Unlimited
	}

	return l
}

// Wait blocks until n bytes can be transferred in the given direction
func (l *Limiter) Wait(ctx context.Context, dir Direction, n int) error {
	l.mu.RLock()
	limiter := l.getLimiter(dir)
	l.mu.RUnlock()

	if limiter == nil {
		return nil
	}

	return limiter.WaitN(ctx, n)
}

// WaitN is an alias for Wait for consistency with rate.Limiter API
func (l *Limiter) WaitN(ctx context.Context, dir Direction, n int) error {
	return l.Wait(ctx, dir, n)
}

// Reserve reserves n bytes for transfer
func (l *Limiter) Reserve(dir Direction, n int) *rate.Reservation {
	l.mu.RLock()
	limiter := l.getLimiter(dir)
	l.mu.RUnlock()

	if limiter == nil {
		return &rate.Reservation{} // Empty reservation for unlimited
	}

	return limiter.ReserveN(time.Now(), n)
}

// Allow checks if n bytes can be transferred immediately
func (l *Limiter) Allow(dir Direction, n int) bool {
	l.mu.RLock()
	limiter := l.getLimiter(dir)
	l.mu.RUnlock()

	if limiter == nil {
		return true // Unlimited
	}

	return limiter.AllowN(time.Now(), n)
}

// getLimiter returns the appropriate limiter for the direction
func (l *Limiter) getLimiter(dir Direction) *rate.Limiter {
	if dir == Download {
		return l.downloadLimiter
	}
	return l.uploadLimiter
}

// SetDownloadLimit updates the download rate limit
func (l *Limiter) SetDownloadLimit(bytesPerSec int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.downloadLimit = bytesPerSec

	if bytesPerSec > 0 {
		l.downloadLimiter = rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
	} else {
		l.downloadLimiter = rate.NewLimiter(rate.Inf, 0)
	}
}

// SetUploadLimit updates the upload rate limit
func (l *Limiter) SetUploadLimit(bytesPerSec int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.uploadLimit = bytesPerSec

	if bytesPerSec > 0 {
		l.uploadLimiter = rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
	} else {
		l.uploadLimiter = rate.NewLimiter(rate.Inf, 0)
	}
}

// GetDownloadLimit returns the current download limit in bytes per second
func (l *Limiter) GetDownloadLimit() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.downloadLimit
}

// GetUploadLimit returns the current upload limit in bytes per second
func (l *Limiter) GetUploadLimit() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.uploadLimit
}

// LimitedReader wraps an io.Reader with rate limiting
type LimitedReader struct {
	r       io.Reader
	limiter *Limiter
	ctx     context.Context
}

// NewLimitedReader creates a new rate-limited reader
func NewLimitedReader(r io.Reader, limiter *Limiter, ctx context.Context) *LimitedReader {
	if ctx == nil {
		ctx = context.Background()
	}

	return &LimitedReader{
		r:       r,
		limiter: limiter,
		ctx:     ctx,
	}
}

// Read reads data with rate limiting
func (lr *LimitedReader) Read(p []byte) (n int, err error) {
	// Read the data first
	n, err = lr.r.Read(p)

	if n > 0 && lr.limiter != nil {
		// Wait for rate limiter
		if waitErr := lr.limiter.Wait(lr.ctx, Download, n); waitErr != nil {
			return n, waitErr
		}
	}

	return n, err
}

// LimitedWriter wraps an io.Writer with rate limiting
type LimitedWriter struct {
	w       io.Writer
	limiter *Limiter
	ctx     context.Context
}

// NewLimitedWriter creates a new rate-limited writer
func NewLimitedWriter(w io.Writer, limiter *Limiter, ctx context.Context) *LimitedWriter {
	if ctx == nil {
		ctx = context.Background()
	}

	return &LimitedWriter{
		w:       w,
		limiter: limiter,
		ctx:     ctx,
	}
}

// Write writes data with rate limiting
func (lw *LimitedWriter) Write(p []byte) (n int, err error) {
	if lw.limiter != nil {
		// Wait for rate limiter before writing
		if waitErr := lw.limiter.Wait(lw.ctx, Upload, len(p)); waitErr != nil {
			return 0, waitErr
		}
	}

	return lw.w.Write(p)
}

// Stats represents rate limiting statistics
type Stats struct {
	DownloadLimit     int64
	UploadLimit       int64
	DownloadBurst     int
	UploadBurst       int
	DownloadAvailable int
	UploadAvailable   int
}

// GetStats returns current rate limiting statistics
func (l *Limiter) GetStats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats := Stats{
		DownloadLimit: l.downloadLimit,
		UploadLimit:   l.uploadLimit,
	}

	if l.downloadLimiter != nil {
		stats.DownloadBurst = l.downloadLimiter.Burst()
		stats.DownloadAvailable = int(l.downloadLimiter.Tokens())
	}

	if l.uploadLimiter != nil {
		stats.UploadBurst = l.uploadLimiter.Burst()
		stats.UploadAvailable = int(l.uploadLimiter.Tokens())
	}

	return stats
}

// IsUnlimited returns whether the limiter is unlimited for a direction
func (l *Limiter) IsUnlimited(dir Direction) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if dir == Download {
		return l.downloadLimit == 0
	}
	return l.uploadLimit == 0
}

// Reset resets the limiter state
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Recreate limiters with current limits
	if l.downloadLimit > 0 {
		l.downloadLimiter = rate.NewLimiter(rate.Limit(l.downloadLimit), int(l.downloadLimit))
	}

	if l.uploadLimit > 0 {
		l.uploadLimiter = rate.NewLimiter(rate.Limit(l.uploadLimit), int(l.uploadLimit))
	}
}
