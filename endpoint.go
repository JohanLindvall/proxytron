package proxytron

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

// Endpoint is a validated feed entry. Metadata is retained for diagnostics;
// scores supplied by a feed never influence the pool's learned scores.
type Endpoint struct {
	URL      string
	Metadata json.RawMessage
}

// ParseEndpoint validates a proxy URL or host:port. It requires an explicit
// port, supports HTTP(S), SOCKS4(a), SOCKS5(h), and recognizes Direct.
// SOCKS5 entries are normalized to SOCKS5h for remote destination DNS.
// Credentials and authority spelling are retained as stable inventory keys.
func ParseEndpoint(raw, defaultScheme string) (string, error) {
	if IsDirect(raw) {
		return Direct, nil
	}
	u, ok := endpointURL(raw, defaultScheme)
	if !ok {
		return "", errors.New("proxytron: invalid proxy endpoint")
	}
	return u, nil
}

// CanonicalEndpoint also lowercases host names and normalizes URL escaping.
// Applications migrating existing records should retain their original key
// convention; ParseEndpoint preserves authority spelling instead.
func CanonicalEndpoint(raw, defaultScheme string) (string, error) {
	s, err := ParseEndpoint(raw, defaultScheme)
	if err != nil || s == Direct {
		return s, err
	}
	u, _ := url.Parse(s)
	u.Host = strings.ToLower(u.Host)
	return u.String(), nil
}

// ParseFeed accepts text lines, JSON URL arrays and Proxifly-style records.
// country optionally filters record metadata. Malformed nonempty feeds fail;
// an explicit empty feed succeeds. A feed can never enable direct egress.
func ParseFeed(body []byte, defaultScheme, country string) ([]Endpoint, error) {
	rows, err := parseFeed(body, defaultScheme, country)
	if err != nil {
		return nil, err
	}
	out := make([]Endpoint, 0, len(rows))
	for _, row := range rows {
		out = append(out, Endpoint{row.url, row.metadata})
	}
	return out, nil
}
