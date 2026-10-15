package proxytron

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestProxyTypeAllows(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		socks    bool
		http     bool
	}{
		{"socks4://proxy.test:1080", true, false},
		{"socks4a://proxy.test:1080", true, false},
		{"socks5://proxy.test:1080", true, false},
		{"socks5h://user:secret@proxy.test:1080", true, false},
		{" SOCKS5H://proxy.test:1080 ", true, false},
		{"http://user:secret@proxy.test:8080", false, true},
		{"HTTPS://proxy.test:8443", false, true},
		{"proxy.test:8080", false, true},
		{"[::1]:8080", false, true},
		{"http://socks5h:secret@proxy.test:8080", false, true},
		{"tcp://proxy.test:1080", false, false},
		{"socks://proxy.test:1080", false, false},
		{" Direct ", false, false},
		{"", false, false},
	} {
		if got := ProxyTypeSOCKS.Allows(tc.endpoint); got != tc.socks {
			t.Errorf("SOCKS allows %q = %v", tc.endpoint, got)
		}
		if got := ProxyTypeHTTP.Allows(tc.endpoint); got != tc.http {
			t.Errorf("HTTP allows %q = %v", tc.endpoint, got)
		}
		if !ProxyTypeAny.Allows(tc.endpoint) || ProxyType("unsupported").Allows(tc.endpoint) {
			t.Errorf("unrestricted or unsupported type mishandled %q", tc.endpoint)
		}
	}
}

func TestProxyTypeSelectionAndCooldowns(t *testing.T) {
	endpoints := []string{Direct, "http://http.test:80", "https://https.test:443",
		"socks4://four.test:1080", "socks4a://four-a.test:1080",
		"socks5://five.test:1080", "socks5h://five-h.test:1080"}
	for _, selection := range []string{SelectionThroughput, SelectionRoundRobin} {
		for _, kind := range []ProxyType{ProxyTypeSOCKS, ProxyTypeHTTP} {
			t.Run(selection+"/"+string(kind), func(t *testing.T) {
				clock := newClock()
				settings := DefaultConfig().Settings
				settings.Selection = selection
				p := testFromURLs(endpoints, WithClock(clock), WithSettings(settings), WithProxyType(kind))
				want := endpoints[1:3]
				if kind == ProxyTypeSOCKS {
					want = endpoints[3:]
				}
				if p.Len() != len(want) || p.PathCount("service", true) != len(want) {
					t.Fatal("route counts include filtered types")
				}
				if len(p.Health()) != len(endpoints) {
					t.Fatal("filter discarded inventory")
				}
				excluded := map[string]bool{}
				for range len(want) * 2 {
					route := p.Acquire("service", true, excluded, nil)
					if !slices.Contains(want, route) {
						t.Fatalf("selected forbidden route %q, including after exclusions reset", route)
					}
					excluded[route] = true
				}
				if len(excluded) != len(want) {
					t.Fatal("some matching protocols were never selected")
				}
				for _, route := range want {
					p.DemoteTransport(t.Context(), "service", route, "failed", time.Second)
				}
				if route := p.Acquire("service", true, nil, nil); route != "" {
					t.Fatalf("cooling routes fell back to %q", route)
				}
				if wait, ok := p.WaitFor("service", true, nil); !ok || wait != settings.BrokenCooldown {
					t.Fatalf("cooldown wait = %v, %v", wait, ok)
				}
				if _, ok := p.WaitFor("service", true, excluded); ok {
					t.Fatal("excluded matching routes left an eligible route")
				}
				clock.Advance(settings.BrokenCooldown)
				if route := p.Acquire("service", true, nil, nil); !slices.Contains(want, route) {
					t.Fatalf("cooldown recovery selected %q", route)
				}
			})
		}
	}
}

func TestProxyTypePreservesScopeRestrictions(t *testing.T) {
	const socks, http = "socks5h://socks.test:1080", "http://http.test:80"
	p := testFromURLs([]string{socks, http}, WithProxyType(ProxyTypeSOCKS))
	p.Refresh(t.Context(), []string{socks, http}, map[string][]string{socks: {"other"}}, nil)
	p.Demote(t.Context(), "service", http, "refused", time.Second)
	if p.RefusedCooling("service", true) || p.PathCount("service", true) != 0 {
		t.Fatal("a filtered route affected scope health or count")
	}
	if route := p.Acquire("service", true, nil, nil); route != "" {
		t.Fatalf("foreign SOCKS route selected: %q", route)
	}
	if _, ok := p.WaitFor("service", true, nil); ok {
		t.Fatal("waited for a route the scope cannot use")
	}
	p.Demote(t.Context(), "other", socks, "refused", time.Second)
	if !p.RefusedCooling("other", true) {
		t.Fatal("matching route's refusal was hidden")
	}
}

func TestDoProxyTypeRetriesAndWaits(t *testing.T) {
	const socks = "socks5h://socks.test:1080"
	clock := newClock()
	p := testFromURLs([]string{Direct, "http://http.test:80", socks},
		WithClock(clock), WithProxyType(ProxyTypeSOCKS))
	calls := 0
	err := p.Do(t.Context(), "service", RequestOptions{
		AllowDirect: true, RouteAttempts: 3, MaxRouteWait: time.Minute, Sleep: clock.Sleep,
	}, func(_ context.Context, endpoint string) AttemptResult {
		if endpoint != socks {
			t.Fatalf("retry selected %q", endpoint)
		}
		calls++
		if calls == 1 {
			return AttemptResult{Outcome: Refused, Retry: true, RouteFailure: true}
		}
		return AttemptResult{Outcome: Succeeded}
	})
	if err != nil || calls != 2 || !slices.Equal(clock.Slept(), []time.Duration{time.Minute}) {
		t.Fatalf("calls = %d, waits = %v, error = %v", calls, clock.Slept(), err)
	}
	p.Refresh(t.Context(), []string{Direct, "https://https.test:443"}, nil, nil)
	err = p.Do(t.Context(), "service", RequestOptions{AllowDirect: true}, func(context.Context, string) AttemptResult {
		t.Fatal("an empty selection invoked the request callback")
		return AttemptResult{}
	})
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("no matching route: %v", err)
	}
}

func TestProxyTypeLiveFeedsOverridesAndRestart(t *testing.T) {
	const http, socks = "http://http.test:80", "socks5h://socks.test:1080"
	clock := newClock()
	feed := stubFeeds(feedText(http + "\n" + socks))
	cfg := DefaultConfig()
	cfg.Endpoints = []string{Direct}
	cfg.FeedURL = "https://feed.test/proxies"
	cfg.MinReady = 1
	path := filepath.Join(t.TempDir(), "proxies.db")
	live := newLive(t, WithClock(clock), feed.option(), WithProxyType(ProxyTypeSOCKS))
	p, err := live.Pool(t.Context(), &cfg, nil, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Acquire("service", true, nil, nil); got != socks {
		t.Fatalf("feed selection = %q", got)
	}
	p.ReportOK(t.Context(), "service", http, time.Second)
	p.DemoteTransport(t.Context(), "service", socks, "failed", time.Second)
	// A healthy HTTP route must not prevent replenishing the SOCKS pool.
	clock.Advance(refreshRetry)
	if _, err := live.Pool(t.Context(), &cfg, nil, path); err != nil {
		t.Fatal(err)
	}
	if len(feed.calls) != 2 {
		t.Fatalf("short SOCKS inventory fetched %d times, want 2", len(feed.calls))
	}
	if _, err := live.Override(t.Context(), []string{Direct, http}, &cfg, nil); err != nil {
		t.Fatal(err)
	}
	p, err = live.Pool(t.Context(), &cfg, nil, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Acquire("service", true, nil, nil); got != "" {
		t.Fatalf("override bypassed type filter: %q", got)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = LoadPool(t.Context(), &cfg, nil, path, WithClock(clock), feed.option(), WithProxyType(ProxyTypeHTTP))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if got := p.Acquire("service", true, nil, nil); got != http {
		t.Fatalf("restored selection = %q", got)
	}
	if p.find(http).served != 1 || p.find(socks).fails != 1 {
		t.Fatal("changing type lost health of retained inventory")
	}
}

func TestInvalidProxyTypeFailsClosed(t *testing.T) {
	invalid := ProxyType("unsupported")
	if invalid.Validate() == nil {
		t.Fatal("unsupported type was accepted")
	}
	for _, kind := range []ProxyType{ProxyTypeAny, ProxyTypeSOCKS, ProxyTypeHTTP} {
		if err := kind.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	p := testFromURLs([]string{Direct, "http://proxy.test:80"}, WithProxyType(invalid))
	if p.Acquire("service", true, nil, nil) != "" || p.Len() != 0 {
		t.Fatal("unsupported type allowed a route")
	}
	feed := stubFeeds(nil)
	cfg := DefaultConfig()
	cfg.FeedURL = "https://feed.test/proxies"
	if p, err := LoadPool(t.Context(), &cfg, nil, "", feed.option(), WithProxyType(invalid)); err == nil {
		p.Close()
		t.Fatal("LoadPool accepted an unsupported type")
	}
	if len(feed.calls) != 0 {
		t.Fatal("invalid options triggered discovery")
	}
}
