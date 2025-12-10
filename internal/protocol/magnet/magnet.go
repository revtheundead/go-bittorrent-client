package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

type Magnet struct {
	InfoHashHex string   // Hex-encoded info hash
	InfoHash    [20]byte // Binary info hash
	DisplayName string   // Display name (dn parameter)
	Trackers    []string // List of tracker URLs
	Length      int64    // Exact length in bytes (xl parameter)
}

// Parse parses a magnet URI and extracts information.
//
// It expects:
//   - scheme: magnet
//   - xt parameter: urn:btih:<hash> (hash can be hex or base32)
//   - tr parameter: tracker URL (can appear multiple times)
//   - dn parameter: display name (optional)
//   - xl parameter: exact length in bytes (optional)
func Parse(raw string) (*Magnet, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet URI: %w", err)
	}

	if u.Scheme != "magnet" {
		return nil, fmt.Errorf("invalid scheme %q (expected 'magnet')", u.Scheme)
	}

	q := u.Query()

	// Info hash (xt: urn:btih:<hash>)
	xts := q["xt"]
	if len(xts) == 0 {
		return nil, fmt.Errorf("magnet URI missing xt parameter")
	}

	var infoHashHex string
	found := false

	for _, xt := range xts {
		const prefix = "urn:btih:"
		if !strings.HasPrefix(xt, prefix) {
			continue
		}
		hashPart := strings.TrimPrefix(xt, prefix)

		switch {
		// Hex-encoded info hash (40 hex characters)
		case len(hashPart) == 40 && isHexString(hashPart):
			infoHashHex = strings.ToLower(hashPart)
			found = true
		// Base32-encoded info hash (usually 32 chars, uppercase, no padding)
		default:
			// Try base32 decode, no padding
			enc := base32.StdEncoding.WithPadding(base32.NoPadding)
			decoded, err := enc.DecodeString(strings.ToUpper(hashPart))
			if err != nil {
				return nil, fmt.Errorf("failed to decode base32 info hash %q: %w", hashPart, err)
			}
			if len(decoded) != 20 {
				return nil, fmt.Errorf("decoded info hash invalid length %d (expected 20)", len(decoded))
			}
			infoHashHex = hex.EncodeToString(decoded)
			found = true
		}

		if found {
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("magnet URI missing valid xt=urn:btih:<hash> parameter")
	}

	// Convert hex string to binary hash
	var infoHash [20]byte
	hashBytes, err := hex.DecodeString(infoHashHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode info hash: %w", err)
	}
	copy(infoHash[:], hashBytes)

	// Parse trackers (tr parameter, can appear multiple times)
	trackers := q["tr"]

	// Parse display name (dn parameter)
	displayName := q.Get("dn")

	// Parse exact length (xl parameter)
	var length int64
	if xlStr := q.Get("xl"); xlStr != "" {
		if parsedLen, err := fmt.Sscanf(xlStr, "%d", &length); err == nil && parsedLen == 1 {
			// Successfully parsed length
		}
	}

	return &Magnet{
		InfoHashHex: infoHashHex,
		InfoHash:    infoHash,
		DisplayName: displayName,
		Trackers:    trackers,
		Length:      length,
	}, nil
}

func isHexString(s string) bool {
	for _, r := range s {
		if (r >= '0' && r <= '9') ||
			(r >= 'a' && r <= 'f') ||
			(r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
}
