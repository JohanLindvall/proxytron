package lease

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExitProberFallbackUsesReservedClientAndClaimsOnce(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Adapter") != "session" {
			t.Error("adapter's request factory was bypassed")
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/blocked":
			w.WriteHeader(http.StatusForbidden)
		case "/invalid":
			_, _ = io.WriteString(w, "not an address")
		default:
			_, _ = io.WriteString(w, `{"origin":"::ffff:198.51.100.8"}`)
		}
	}))
	defer proxy.Close()
	p, err := Open(filepath.Join(t.TempDir(), "leases.db"), []string{proxy.URL}, nil,
		probeAdapter{NewHTTPClient(nil)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	l := p.Acquire("service", "", 1)
	defer l.Release()
	prober := ExitProber{URLs: []string{"http://echo.test/blocked", "http://echo.test/invalid", "http://echo.test/ip"},
		IsResponseError: func(err error) bool { var status *probeStatusError; return errors.As(err, &status) }}
	for range 2 {
		got, err := prober.Claim(t.Context(), l)
		if err != nil || got.IP.String() != "198.51.100.8" || got.CheckedAt.IsZero() || !l.Claimed() {
			t.Fatalf("claim: %+v, %v", got, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(paths, []string{"/blocked", "/invalid", "/ip"}) {
		t.Fatalf("fallback or claim reuse: %v", paths)
	}
	if l.entry.Fails != 0 || l.entry.stat("service").Fails != 0 || !l.entry.stat("service").Until.IsZero() {
		t.Fatal("probe refusal penalized the destination scope")
	}
}

type probeStatusError struct{}

func (*probeStatusError) Error() string { return "probe refused" }

// This adapter verifies both request construction and error classification hooks
// while its embedded standard client supplies routing and body observations.
type probeAdapter struct{ Client }

func (c probeAdapter) ThroughProxy(endpoint string) (Client, error) {
	client, err := c.Client.ThroughProxy(endpoint)
	return probeAdapter{client}, err
}

func (c probeAdapter) NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := c.Client.NewRequest(ctx, method, url, body)
	if err == nil {
		req.Header.Set("X-Adapter", "session")
	}
	return req, err
}

func (c probeAdapter) DoOnce(req *http.Request) (*http.Response, error) {
	resp, err := c.Client.DoOnce(req)
	if err == nil && resp.StatusCode == http.StatusForbidden {
		resp.Body.Close()
		return nil, &probeStatusError{}
	}
	return resp, err
}

func TestExitProberConcurrentClaimsPreserveExitLimit(t *testing.T) {
	answer := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "192.0.2.8") })
	a, b := httptest.NewServer(answer), httptest.NewServer(answer)
	defer a.Close()
	defer b.Close()
	p := testPool(t, []string{a.URL, b.URL}, nil)
	leases := []*Lease{p.Acquire("service", routeID(a.URL), 1), p.Acquire("service", routeID(b.URL), 1)}
	for _, l := range leases {
		if l == nil {
			t.Fatal("untried proxy ports could not be reserved independently")
		}
		defer l.Release()
	}
	prober := ExitProber{URLs: []string{"http://echo.test/ip"}}
	var wg sync.WaitGroup
	errs := make([]error, len(leases))
	for i, l := range leases {
		wg.Go(func() { _, errs[i] = prober.Claim(t.Context(), l) })
	}
	wg.Wait()
	winner, loser := 0, 1
	if errs[winner] != nil {
		winner, loser = loser, winner
	}
	if errs[winner] != nil || !errors.Is(errs[loser], ErrExitInUse) || !leases[winner].Claimed() || leases[loser].Claimed() {
		t.Fatalf("concurrent exit claims: %v", errs)
	}
	for _, l := range leases {
		if l.entry.Fails != 0 || l.entry.stat("service").Fails != 0 {
			t.Fatal("shared exit became a route failure")
		}
	}
	leases[winner].Release()
	if _, err := prober.Claim(t.Context(), leases[loser]); err != nil {
		t.Fatalf("released exit still held: %v", err)
	}
	leases[loser].Cooldown(time.Now().Add(time.Hour))
	leases[loser].Release()
	if l := p.Acquire("service", "", 1); l != nil {
		l.Release()
		t.Fatal("probe identities did not share the service cooldown")
	}
	if l := p.Acquire("other", "", 1); l == nil {
		t.Fatal("exit cooldown leaked into another scope")
	} else {
		l.Release()
	}
}

func TestExitProberOutagePolicyAndConfirmedFailure(t *testing.T) {
	for _, policy := range []string{"unset", "outage", "working", "error"} {
		t.Run(policy, func(t *testing.T) {
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
			defer proxy.Close()
			p := testPool(t, []string{proxy.URL}, nil)
			l := p.Acquire("service", "", 1)
			defer l.Release()
			prober := ExitProber{URLs: []string{"http://echo.test/ip"}}
			cause := errors.New("health check failed")
			if policy != "unset" {
				prober.Reachable = func(ctx context.Context) (bool, error) {
					if ctx != t.Context() || !l.Configured() {
						t.Fatal("health callback lost caller context or lease")
					}
					if policy == "error" {
						return false, cause
					}
					return policy == "working", nil
				}
			}
			got, err := prober.Claim(t.Context(), l)
			var want error
			switch policy {
			case "unset":
				want = ErrExitUnknown
			case "working":
				want = ErrExitProbeFailed
			case "error":
				want = cause
			}
			if !errors.Is(err, want) || got.IP.IsValid() || l.Claimed() {
				t.Fatalf("result=%+v err=%v, want %v", got, err, want)
			}
			fails := 0
			if policy == "working" {
				fails = 1
			}
			if l.entry.Fails != fails || l.entry.stat("service").Fails != 0 {
				t.Fatalf("endpoint fails=%d service fails=%d", l.entry.Fails, l.entry.stat("service").Fails)
			}
		})
	}
}

func TestExitProberTransportFailureNeverFallsBackToDirect(t *testing.T) {
	var directCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		directCalls.Add(1)
		_, _ = io.WriteString(w, "192.0.2.1")
	}))
	defer target.Close()
	proxy := httptest.NewServer(http.NotFoundHandler())
	p := testPool(t, []string{proxy.URL}, nil)
	l := p.Acquire("service", "", 1)
	defer l.Release()
	proxy.Close()
	prober := ExitProber{URLs: []string{target.URL, target.URL + "/second"}, Timeout: time.Second,
		Reachable: func(context.Context) (bool, error) { t.Fatal("transport failure ran outage check"); return false, nil }}
	if _, err := prober.Claim(t.Context(), l); err == nil {
		t.Fatal("broken proxy passed the probe")
	}
	if directCalls.Load() != 0 || l.entry.Fails != 1 || l.Claimed() {
		t.Fatalf("direct calls=%d failures=%d claimed=%v", directCalls.Load(), l.entry.Fails, l.Claimed())
	}
}

func TestExitProberCancellationDoesNotPenalizeRoute(t *testing.T) {
	entered := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	defer proxy.Close()
	p := testPool(t, []string{proxy.URL}, nil)
	l := p.Acquire("service", "", 1)
	defer l.Release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := (ExitProber{URLs: []string{"http://echo.test/ip"}}).Claim(ctx, l); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if l.entry.Fails != 0 || l.entry.stat("service").Tries != 0 || l.Claimed() {
		t.Fatal("cancellation changed route health or exit reservation")
	}
}

func TestExitProberCachedIdentityNeedsFreshConfirmation(t *testing.T) {
	var calls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, "198.51.100.9")
	}))
	defer proxy.Close()
	p := testPool(t, []string{proxy.URL}, nil)
	l := p.Acquire("service", "", 1)
	if !l.ClaimExit("192.0.2.9") {
		t.Fatal("initial claim failed")
	}
	l.Release()
	l = p.Acquire("service", "", 1)
	defer l.Release()
	if l.Claimed() || l.Exit() != "192.0.2.9" {
		t.Fatal("cached exit not reserved for fresh confirmation")
	}
	prober := ExitProber{URLs: []string{"http://echo.test/ip"}}
	if _, err := prober.Claim(t.Context(), l); err != nil || l.Exit() != "198.51.100.9" || calls.Load() != 1 {
		t.Fatalf("fresh confirmation: exit=%s err=%v calls=%d", l.Exit(), err, calls.Load())
	}
	l.Release()
	if _, err := prober.Claim(t.Context(), l); !errors.Is(err, ErrInactiveLease) || calls.Load() != 1 {
		t.Fatalf("released lease was probed: err=%v calls=%d", err, calls.Load())
	}
	if _, ok := p.Egress(proxy.URL); !ok {
		t.Fatal("probe did not update cached identity")
	}
}

func TestExitProberOwnTimeoutIsOneTransportFailure(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer proxy.Close()
	p := testPool(t, []string{proxy.URL}, nil)
	l := p.Acquire("service", "", 1)
	defer l.Release()
	prober := ExitProber{URLs: []string{"http://echo.test/ip"}, Timeout: 20 * time.Millisecond}
	if _, err := prober.Claim(t.Context(), l); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe timeout: %v", err)
	}
	if l.entry.Fails != 1 || l.entry.stat("service").Tries != 1 || l.Claimed() {
		t.Fatalf("timeout counted incorrectly: %+v", l.entry.stat("service"))
	}
}

type challengeProbeAdapter struct{ Client }

func (c challengeProbeAdapter) ThroughProxy(endpoint string) (Client, error) {
	client, err := c.Client.ThroughProxy(endpoint)
	return challengeProbeAdapter{client}, err
}

func (c challengeProbeAdapter) WithRoute(ctx context.Context, id, scope string, observe func(Observation)) context.Context {
	return c.Client.WithRoute(ctx, id, scope, func(o Observation) {
		o.Challenged, o.Err = true, errors.New("probe service challenge")
		observe(o)
	})
}

func TestExitProberHTTP200ChallengeDoesNotPenalizeDestination(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "synthetic challenge page")
	}))
	defer proxy.Close()
	p, err := Open(filepath.Join(t.TempDir(), "leases.db"), []string{proxy.URL}, nil,
		challengeProbeAdapter{NewHTTPClient(nil)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	l := p.Acquire("service", "", 1)
	defer l.Release()
	_, err = (ExitProber{URLs: []string{"http://echo.test/ip"}}).Claim(t.Context(), l)
	if !errors.Is(err, ErrExitUnknown) || l.entry.WAF.Tries != 0 || l.entry.Fails != 0 || l.entry.stat("service").Tries != 0 {
		t.Fatalf("probe challenge scored as destination health: error=%v WAF=%+v service=%+v", err, l.entry.WAF, l.entry.stat("service"))
	}
}
