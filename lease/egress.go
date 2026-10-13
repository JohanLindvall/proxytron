package lease

import (
	"context"
	"errors"
	"net/netip"

	"github.com/JohanLindvall/proxytron"
)

// DetectEgress records the real exit IP and groups leases and service cooldowns
// by that address. Different gateways can share an exit. Detection runs outside
// the pool lock; a failed check preserves the previous identity. For rotating
// gateways the caller should check again when its sticky session changes.
func (p *Pool) DetectEgress(ctx context.Context, d proxytron.Detector, endpoint string) (proxytron.Egress, error) {
	p.mu.Lock()
	e := p.entries[endpoint]
	closed := p.closed
	p.mu.Unlock()
	if closed || e == nil {
		return proxytron.Egress{}, errors.New("proxytron: endpoint is not in an open pool")
	}
	result, err := d.Detect(ctx, endpoint)
	if err != nil {
		return proxytron.Egress{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.entries[endpoint] != e {
		return proxytron.Egress{}, errors.New("proxytron: endpoint changed during detection")
	}
	e.Exit, e.ExitAt = result.IP.String(), result.CheckedAt
	// Detection can reveal that in-flight leases share an exit. Account for
	// all of them immediately, even if the resulting count exceeds the cap.
	for l := range p.activeLeases {
		if l.entry != e {
			continue
		}
		p.unclaimExit(l)
		if p.exitLeases == nil {
			p.exitLeases = make(map[exitSite]int)
		}
		p.exitLeases[exitSite{e.Exit, l.site}]++
		l.exit, l.autoExit = e.Exit, true
	}
	p.seedExitCooling(result.CheckedAt)
	return result, p.saveAll()
}

// Egress returns the last measured identity, false when it has never been checked.
func (p *Pool) Egress(endpoint string) (proxytron.Egress, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries[endpoint]
	if e == nil {
		return proxytron.Egress{}, false
	}
	ip, err := netip.ParseAddr(e.Exit)
	return proxytron.Egress{IP: ip, CheckedAt: e.ExitAt}, err == nil
}

// Observe records an application's classified outcome for this lease. It does
// not release the lease. The caller should exclude user cancellation.
func (l *Lease) Observe(o Observation) { l.observe(o) }

// Endpoint returns the selected route, including credentials. Use it only to
// establish the connection; inventory pages already redact credentials.
func (l *Lease) Endpoint() string { return l.entry.URL }

// WithChallengeClassifier recognizes an application's challenge errors,
// including failures reported through Failed or RefusedDirect.
func WithChallengeClassifier(classify func(error) bool) Option {
	return func(p *Pool) { p.isChallenge = classify }
}
