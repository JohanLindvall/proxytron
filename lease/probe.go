package lease

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/proxytron"
)

var (
	// ErrExitInUse means the observed exit is reserved or cooling for this scope.
	ErrExitInUse = errors.New("proxytron: exit address is in use or cooling")
	// ErrExitUnknown means no configured probe returned a usable address.
	ErrExitUnknown = errors.New("proxytron: could not determine the route's exit")
	// ErrExitProbeFailed means probes work independently but not on this route.
	ErrExitProbeFailed = errors.New("proxytron: route did not pass exit probes")
	// ErrInactiveLease means the lease was released or its pool was closed.
	ErrInactiveLease = errors.New("proxytron: lease is no longer active")
)

// ExitProber confirms a lease's public address before destination work starts.
// URLs are tried in order, through the lease's existing client and session.
// It owns no clients or goroutines and is safe to share if its configuration is
// not mutated and its callbacks are concurrency-safe. Serialize Claim calls on
// any one lease, just as the destination setup using that lease is serialized.
type ExitProber struct {
	URLs    []string
	Timeout time.Duration // Per probe; nonpositive uses Detector's default.
	// IsResponseError optionally recognizes an adapter's HTTP response errors.
	// Such errors try the next URL; other I/O errors stop immediately. Standard
	// Detector response errors are recognized without this callback.
	IsResponseError func(error) bool
	// Reachable optionally checks the probes independently after all replies
	// were unusable. True confirms a route failure, which is scored once;
	// false permits proceeding with an unknown exit during a probe outage.
	// An error stops without a penalty. Nil fails with ErrExitUnknown without
	// penalizing the route. The caller chooses the diagnostic route, bounds its
	// I/O with ctx, and may cache/coalesce checks. Direct checks are never implicit.
	Reachable func(context.Context) (bool, error)
}

// Claim probes and atomically claims the observed exit using Lease.ClaimExit.
// A fresh claim is reused; a cached identity still requires confirmation. An
// empty URL list disables probing. A nil error with an invalid IP means probing
// was disabled or Reachable explicitly allowed an outage. No destination request
// is sent, no lease is released, and no route is silently replaced with Direct.
// The caller must release the lease after its work, including on a Claim error.
//
// ProbeContext reports transport outcomes through the Client adapter; responses
// from the probe service do not penalize the destination scope. A confirmed
// unusable route returns ErrExitProbeFailed; a shared or cooling exit returns
// ErrExitInUse without a health penalty. Errors may also wrap the last probe error.
func (p ExitProber) Claim(ctx context.Context, l *Lease) (proxytron.Egress, error) {
	if err := ctx.Err(); err != nil {
		return proxytron.Egress{}, err
	}
	if l == nil {
		return proxytron.Egress{}, ErrInactiveLease
	}
	l.pool.mu.Lock()
	inactive := l.released || l.pool.closed
	claimed := l.exit != "" && !l.autoExit
	ip, _ := netip.ParseAddr(l.exit)
	previous := proxytron.Egress{IP: ip, CheckedAt: l.entry.ExitAt}
	l.pool.mu.Unlock()
	if inactive {
		return proxytron.Egress{}, ErrInactiveLease
	}
	if claimed {
		return previous, nil
	}
	if len(p.URLs) == 0 {
		return proxytron.Egress{}, nil
	}
	start := time.Now()
	var last error
	for _, url := range p.URLs {
		var observed atomic.Bool
		started := time.Now()
		d := proxytron.Detector{URL: url, Timeout: p.Timeout, Client: func(string) (proxytron.HTTPDoer, error) {
			return probeClient{lease: l, observe: func(o Observation) {
				// An adapter can classify a challenge even on an HTTP 200.
				// That is the probe service's response, not this scope's health.
				if ctx.Err() == nil && !o.Challenged && !errors.Is(o.Err, context.Canceled) &&
					(o.Status == 0 || (o.Status >= 200 && o.Status < 300)) {
					observed.Store(true)
					l.observe(o)
				}
			}}, nil
		}}
		identity, err := d.Detect(ctx, l.Endpoint())
		if cerr := ctx.Err(); cerr != nil {
			return proxytron.Egress{}, cerr
		}
		if errors.Is(err, context.Canceled) {
			return proxytron.Egress{}, err
		}
		// Detector bounds each probe with its own deadline. Adapters correctly
		// ignore canceled request contexts, so record an internal probe timeout
		// ourselves when no observation was delivered. Parent cancellation above
		// remains neutral, and adapters that already reported are not counted twice.
		if errors.Is(err, context.DeadlineExceeded) && !observed.Load() {
			l.Failed(context.DeadlineExceeded, time.Since(started), true)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return proxytron.Egress{}, err
		}
		if err == nil {
			if !l.ClaimExit(identity.IP.String()) {
				return proxytron.Egress{}, ErrExitInUse
			}
			return identity, nil
		}
		var response *proxytron.ProbeResponseError
		if !errors.As(err, &response) && (p.IsResponseError == nil || !p.IsResponseError(err)) {
			return proxytron.Egress{}, err
		}
		last = err
	}
	if p.Reachable == nil {
		return proxytron.Egress{}, errors.Join(ErrExitUnknown, last)
	}
	up, err := p.Reachable(ctx)
	if cerr := ctx.Err(); cerr != nil {
		return proxytron.Egress{}, cerr
	}
	if err != nil {
		return proxytron.Egress{}, err
	}
	if !up {
		return proxytron.Egress{}, nil
	}
	l.Failed(ErrExitProbeFailed, time.Since(start), true)
	return proxytron.Egress{}, errors.Join(ErrExitProbeFailed, last)
}

type probeClient struct {
	lease   *Lease
	observe func(Observation)
}

func (c probeClient) Do(req *http.Request) (*http.Response, error) {
	// Use the adapter's request factory as well as its routed client, retaining
	// custom headers, session identity and request-context routing.
	l := c.lease
	ctx := l.client.WithRoute(req.Context(), l.ID(), l.site, c.observe)
	req, err := l.client.NewRequest(ctx, req.Method, req.URL.String(), nil)
	if err != nil {
		return nil, err
	}
	return l.client.DoOnce(req)
}
