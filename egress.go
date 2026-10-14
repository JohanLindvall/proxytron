package proxytron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

// HTTPDoer is the part of http.Client needed by an egress detector.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Egress records the actual address observed by an IP-check service, which
// may differ from a proxy's listening address or DNS name. It is a point-in-time
// measurement: rotating gateways can change addresses between requests.
type Egress struct {
	IP        netip.Addr `json:"ip"`
	CheckedAt time.Time  `json:"checked_at"`
}

// Detector checks egress via a caller-selected URL that returns a plain IP or
// a JSON object with an "ip" or "origin" field. No external service is contacted
// until Detect is called. Client optionally adapts an application's own stack.
// It must honor endpoint (including Direct), and must not fall back to direct.
type Detector struct {
	URL     string
	Timeout time.Duration
	Client  func(endpoint string) (HTTPDoer, error)
}

// ProbeResponseError means an IP-check service answered but did not supply a
// usable address. It can be retried against another caller-configured service;
// by itself it is not evidence of a broken route. Diagnostics omit reply bodies
// and URLs, which may contain credentials. Status is zero for an invalid body.
type ProbeResponseError struct {
	Status int
	Reason string
}

// Error describes the rejected response without including its contents.
func (e *ProbeResponseError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("proxytron: IP-check status %d", e.Status)
	}
	return "proxytron: " + e.Reason
}

// Detect measures the route's externally observed IPv4 or IPv6 address. Replies
// are bounded to 4 KiB; malformed, ambiguous and unspecified addresses fail.
func (d Detector) Detect(ctx context.Context, endpoint string) (Egress, error) {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.URL, nil)
	if err != nil || req.URL.Hostname() == "" || (req.URL.Scheme != "https" && req.URL.Scheme != "http") {
		return Egress{}, errors.New("proxytron: invalid IP-check URL")
	}
	var client HTTPDoer
	if d.Client != nil {
		client, err = d.Client(endpoint)
	} else {
		var tr *http.Transport
		tr, err = NewTransport(endpoint, TransportOptions{Timeout: timeout})
		if err == nil {
			defer tr.CloseIdleConnections()
			client = &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		}
	}
	if err != nil {
		return Egress{}, fmt.Errorf("proxytron: create egress client: %w", err)
	}
	if client == nil {
		return Egress{}, errors.New("proxytron: nil egress client")
	}
	resp, err := client.Do(req)
	if err != nil {
		return Egress{}, fmt.Errorf("proxytron: check egress: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Egress{}, &ProbeResponseError{Status: resp.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil {
		return Egress{}, err
	}
	if len(body) > 4096 {
		return Egress{}, &ProbeResponseError{Reason: "IP-check reply too large"}
	}
	value := strings.TrimSpace(string(body))
	if strings.HasPrefix(value, "{") {
		var record struct {
			IP     string `json:"ip"`
			Origin string `json:"origin"`
		}
		if err := json.Unmarshal(body, &record); err != nil {
			return Egress{}, &ProbeResponseError{Reason: "invalid IP-check JSON"}
		}
		value = record.IP
		if value == "" {
			value = record.Origin
		}
	}
	ip, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
		return Egress{}, &ProbeResponseError{Reason: "IP-check reply is not a single unicast address"}
	}
	return Egress{IP: ip.Unmap(), CheckedAt: time.Now()}, nil
}

// EgressComparison compares a proxy's observed address with the local route.
// A different IP alone does not prove anonymity or absence of identifying headers.
type EgressComparison struct{ Direct, Proxy Egress }

// SameIP reports that the proxy and direct connection used the same exit IP.
func (c EgressComparison) SameIP() bool { return c.Direct.IP.IsValid() && c.Direct.IP == c.Proxy.IP }

// Compare explicitly probes both Direct and endpoint, failing if either fails.
func (d Detector) Compare(ctx context.Context, endpoint string) (EgressComparison, error) {
	var result EgressComparison
	var err error
	result.Direct, err = d.Detect(ctx, Direct)
	if err != nil {
		return result, err
	}
	result.Proxy, err = d.Detect(ctx, endpoint)
	return result, err
}

// DetectEgress probes a configured endpoint without holding the pool lock and
// persists the resulting identity with its scores. Failures leave prior data intact.
func (p *Pool) DetectEgress(ctx context.Context, d Detector, endpoint string) (Egress, error) {
	p.mu.Lock()
	e := p.find(endpoint)
	p.mu.Unlock()
	if e == nil {
		return Egress{}, errors.New("proxytron: endpoint is not in the pool")
	}
	result, err := d.Detect(ctx, endpoint)
	if err != nil {
		return Egress{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.find(endpoint) != e {
		return Egress{}, errors.New("proxytron: endpoint changed during detection")
	}
	e.egress = result
	return result, p.persistLocked(ctx, e)
}

// Egress returns the latest stored exit address, false if none was measured.
func (p *Pool) Egress(endpoint string) (Egress, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.find(endpoint)
	if e == nil {
		return Egress{}, false
	}
	return e.egress, e.egress.IP.IsValid()
}

// Save checkpoints all state and reports errors synchronously. Outcome methods
// already persist individual records; Save also retries any previously failed save.
func (p *Pool) Save(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.persistLocked(ctx, nil)
}
