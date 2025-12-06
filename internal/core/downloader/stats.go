package downloader

import (
	"log"
	"sync/atomic"
	"time"
)

type DownloadStats struct {
	startedAt time.Time
	bytes     int64
}

func NewDownloadStats() *DownloadStats {
	return &DownloadStats{
		startedAt: time.Now(),
	}
}

func (s *DownloadStats) Add(n int) {
	atomic.AddInt64(&s.bytes, int64(n))
}

// LogPeriodically prints current and average rate every interval.
// Call this in a goroutine and cancel by closing 'done' channel.
func (s *DownloadStats) LogPeriodically(interval time.Duration, done <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			total := atomic.LoadInt64(&s.bytes)
			elapsed := time.Since(s.startedAt).Seconds()
			if elapsed <= 0 {
				continue
			}
			instKBs := float64(total) / 1024.0 / elapsed
			log.Printf("downloaded %d bytes (%.2f KiB/s avg)", total, instKBs)
		case <-done:
			return
		}
	}
}
