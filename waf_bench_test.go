package proxytron

import (
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// TestWafChallengedRoutes: a route a site's AWS WAF challenged is benched
// there for an hour, then competes again on its record. (The fetcher's side
// of the challenge is internal/fetch's TestWafChallengedRoutes.)
func TestWafChallengedRoutes_ABenchedRouteSitsOutTheHourThenRejoinsRotation(t *testing.T) {
	const flagged, clean = "http://flagged:80", "http://clean:80"
	clock := newClock()
	pool := testFromURLs([]string{flagged, clean}, WithClock(clock), withRand(seeded(7)))
	pool.Refresh(t.Context(), []string{flagged, clean}, map[string][]string{flagged: {"se"}, clean: {"se"}}, nil)
	for _, url := range []string{flagged, clean} {
		pool.ReportOK(t.Context(), "se", url, time.Second)
	}
	pool.Punish(t.Context(), "se", flagged, "", 0)
	clock.Advance(59 * time.Minute)
	early := acquireN(pool, "se", 20)
	if contains(early, flagged) {
		t.Fatalf("flagged picked within the hour: %v", early)
	}
	clock.Advance(2 * time.Minute)
	picks := acquireN(pool, "se", 60)
	// Back in contention, but with the challenge on its record: it earns
	// turns again, fewer than the route that was never judged.
	if count(picks, flagged) == 0 {
		t.Fatalf("flagged earned no turns: %v", picks)
	}
	if count(picks, flagged) >= count(picks, clean) {
		t.Fatalf("flagged %d turns, clean %d", count(picks, flagged), count(picks, clean))
	}
}

// RefusedCooling is whether a route the site may use is cooling right now
// for the site's refusal: what tells the fetcher that a site with no route
// left was refused into stopping, rather than out of routes for their own
// failures. A failed connection or an AWS WAF challenge is not a refusal,
// another site's refusal is not this one's, a bench that has run out is
// none, and a refusal still running counts whatever else benched the route
// since.
func TestRefusedCooling_OnlyTheSitesOwnRefusalCounts(t *testing.T) {
	const a, b = "http://a:80", "http://b:80"
	for _, tc := range []struct {
		name  string
		bench func(p *Pool, clock *testutil.Clock)
		want  bool
	}{
		{"nothing benched", func(*Pool, *testutil.Clock) {}, false},
		{"a refusal", func(p *Pool, _ *testutil.Clock) { p.Demote(t.Context(), "se", a, "HTTP 503", 0) }, true},
		{"a refusal on another site", func(p *Pool, _ *testutil.Clock) { p.Demote(t.Context(), "de", a, "HTTP 503", 0) }, false},
		{"a refusal that has run out", func(p *Pool, clock *testutil.Clock) {
			p.Demote(t.Context(), "se", a, "HTTP 503", 0)
			clock.Advance(61 * time.Second)
		}, false},
		{"a failed connection", func(p *Pool, _ *testutil.Clock) { p.DemoteTransport(t.Context(), "se", a, "EOF", 0) }, false},
		{"an AWS WAF challenge", func(p *Pool, _ *testutil.Clock) { p.Punish(t.Context(), "se", a, "AWS WAF challenge", 0) }, false},
		{"every route out, none refused", func(p *Pool, _ *testutil.Clock) {
			p.DemoteTransport(t.Context(), "se", a, "EOF", 0)
			p.Punish(t.Context(), "se", b, "AWS WAF challenge", 0)
		}, false},
		{"a refusal, then a failed connection", func(p *Pool, _ *testutil.Clock) {
			p.Demote(t.Context(), "se", a, "HTTP 503", 0)
			p.DemoteTransport(t.Context(), "se", a, "EOF", 0)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newClock()
			pool := testFromURLs([]string{a, b}, WithClock(clock))
			tc.bench(pool, clock)
			if got := pool.RefusedCooling("se", false); got != tc.want {
				t.Errorf("RefusedCooling = %v, want %v", got, tc.want)
			}
		})
	}
}
