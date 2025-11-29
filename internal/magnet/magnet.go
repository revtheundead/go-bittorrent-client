package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

type Magnet struct {
	InfoHashHex string
	TrackerUrl  string
}

// Parse parses a magnet URI and extracts the info hash and tracker URL.
//
// It expects:
//   - scheme: magnet
//   - xt parameter: urn:btih:<hash> (hash can be hex or base32)
//   - tr parameter: tracker URL (first one is used)
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

	tr := q.Get("tr")
	if tr == "" {
		return nil, fmt.Errorf("magnet URI missing tr (tracker) parameter")
	}

	return &Magnet{
		InfoHashHex: infoHashHex,
		TrackerUrl:  tr,
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
