package ratelimit

import (
	"fmt"
)

// Common bandwidth presets (bytes per second)
const (
	Unlimited = 0

	// Download speeds
	Speed256Kbps  = 32 * 1024      // 256 Kbps = 32 KB/s
	Speed512Kbps  = 64 * 1024      // 512 Kbps = 64 KB/s
	Speed1Mbps    = 128 * 1024     // 1 Mbps = 128 KB/s
	Speed2Mbps    = 256 * 1024     // 2 Mbps = 256 KB/s
	Speed5Mbps    = 640 * 1024     // 5 Mbps = 640 KB/s
	Speed10Mbps   = 1280 * 1024    // 10 Mbps = 1.25 MB/s
	Speed25Mbps   = 3200 * 1024    // 25 Mbps = 3.2 MB/s
	Speed50Mbps   = 6400 * 1024    // 50 Mbps = 6.4 MB/s
	Speed100Mbps  = 12800 * 1024   // 100 Mbps = 12.8 MB/s
	Speed1Gbps    = 128000 * 1024  // 1 Gbps = 128 MB/s

	// Upload speeds (typically lower)
	UploadSlow   = 32 * 1024   // 32 KB/s
	UploadMedium = 128 * 1024  // 128 KB/s
	UploadFast   = 512 * 1024  // 512 KB/s
)

// ParseBandwidth parses a bandwidth string and returns bytes per second
// Supports units: B, KB, MB, GB (per second)
// Examples: "1MB", "500KB", "5MB"
func ParseBandwidth(s string) (int64, error) {
	if s == "" || s == "0" || s == "unlimited" {
		return 0, nil
	}

	var value float64
	var unit string

	_, err := fmt.Sscanf(s, "%f%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid bandwidth format: %s", s)
	}

	switch unit {
	case "B", "b":
		return int64(value), nil
	case "KB", "kb", "K", "k":
		return int64(value * 1024), nil
	case "MB", "mb", "M", "m":
		return int64(value * 1024 * 1024), nil
	case "GB", "gb", "G", "g":
		return int64(value * 1024 * 1024 * 1024), nil
	case "Kbps", "kbps":
		return int64(value * 1024 / 8), nil
	case "Mbps", "mbps":
		return int64(value * 1024 * 1024 / 8), nil
	case "Gbps", "gbps":
		return int64(value * 1024 * 1024 * 1024 / 8), nil
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}
}

// FormatBandwidth formats bytes per second into a human-readable string
func FormatBandwidth(bytesPerSec int64) string {
	if bytesPerSec == 0 {
		return "unlimited"
	}

	const unit = 1024
	if bytesPerSec < unit {
		return fmt.Sprintf("%d B/s", bytesPerSec)
	}

	div, exp := int64(unit), 0
	for n := bytesPerSec / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB/s", "MB/s", "GB/s", "TB/s"}
	return fmt.Sprintf("%.1f %s", float64(bytesPerSec)/float64(div), units[exp])
}

// FormatBandwidthBits formats bytes per second into bits per second
func FormatBandwidthBits(bytesPerSec int64) string {
	if bytesPerSec == 0 {
		return "unlimited"
	}

	bitsPerSec := bytesPerSec * 8
	const unit = 1000 // Use 1000 for bits

	if bitsPerSec < unit {
		return fmt.Sprintf("%d bps", bitsPerSec)
	}

	div, exp := int64(unit), 0
	for n := bitsPerSec / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"Kbps", "Mbps", "Gbps", "Tbps"}
	return fmt.Sprintf("%.1f %s", float64(bitsPerSec)/float64(div), units[exp])
}

// NewUnlimitedLimiter creates a limiter with no bandwidth restrictions
func NewUnlimitedLimiter() *Limiter {
	return NewLimiter(Unlimited, Unlimited)
}

// NewDefaultLimiter creates a limiter with sensible defaults
// 10 Mbps download, 1 Mbps upload
func NewDefaultLimiter() *Limiter {
	return NewLimiter(Speed10Mbps, UploadMedium)
}

// NewConservativeLimiter creates a conservative limiter for limited bandwidth
// 2 Mbps download, 256 Kbps upload
func NewConservativeLimiter() *Limiter {
	return NewLimiter(Speed2Mbps, Speed256Kbps)
}

// NewAggressiveLimiter creates an aggressive limiter for high bandwidth
// 100 Mbps download, 10 Mbps upload
func NewAggressiveLimiter() *Limiter {
	return NewLimiter(Speed100Mbps, Speed10Mbps)
}

// CalculateETA calculates estimated time to completion given current rate
func CalculateETA(remainingBytes int64, bytesPerSec float64) float64 {
	if bytesPerSec <= 0 {
		return 0
	}

	return float64(remainingBytes) / bytesPerSec
}

// FormatETA formats seconds into a human-readable duration
func FormatETA(seconds float64) string {
	if seconds <= 0 {
		return "unknown"
	}

	if seconds < 60 {
		return fmt.Sprintf("%.0fs", seconds)
	}

	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%.0fm %.0fs", minutes, int(seconds)%60)
	}

	hours := minutes / 60
	if hours < 24 {
		return fmt.Sprintf("%.0fh %.0fm", hours, int(minutes)%60)
	}

	days := hours / 24
	return fmt.Sprintf("%.0fd %.0fh", days, int(hours)%24)
}

// BandwidthPreset represents a named bandwidth configuration
type BandwidthPreset struct {
	Name     string
	Download int64
	Upload   int64
}

// Common presets
var Presets = []BandwidthPreset{
	{"Unlimited", Unlimited, Unlimited},
	{"Conservative", Speed2Mbps, Speed256Kbps},
	{"Default", Speed10Mbps, UploadMedium},
	{"Fast", Speed25Mbps, UploadFast},
	{"Aggressive", Speed100Mbps, Speed10Mbps},
	{"Gigabit", Speed1Gbps, Speed100Mbps},
}

// GetPreset returns a limiter for a named preset
func GetPreset(name string) *Limiter {
	for _, preset := range Presets {
		if preset.Name == name {
			return NewLimiter(preset.Download, preset.Upload)
		}
	}

	// Default to unlimited if preset not found
	return NewUnlimitedLimiter()
}
