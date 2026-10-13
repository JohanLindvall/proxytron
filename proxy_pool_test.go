package proxytron

// TestProxyPool: optional egress-proxy rotation: round-robin,
// demote-on-failure, refresh. All offline — no real proxy is ever
// contacted.
//
// The tests that drive a Fetcher through the pool are internal/fetch's
// TestProxyPool_* (proxy_pool_test.go there).

import (
	"reflect"
	"testing"
	"time"
)

// The running refusal rate is reported even after a successful page.
func TestProxyPool_RefusalMeasurementsSurviveSuccessWithoutBiasingRotation(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://clean:1080", "socks5h://walled:1080"},
		baseCooldown(0), roundRobin())
	for i := range 20 {
		pool.ReportOK(t.Context(), "se", "socks5h://clean:1080", time.Second)
		if i%3 == 0 { // every third page refused, never twice in a row
			pool.Demote(t.Context(), "se", "socks5h://walled:1080", "bot wall", 0)
		}
		pool.ReportOK(t.Context(), "se", "socks5h://walled:1080", time.Second)
	}
	rows := siteRows(pool.SiteHealth("se"))
	if got := rows["socks5h://walled:1080"].Fails; got != 0 { // the old blind spot
		t.Errorf("fails = %d, want 0", got)
	}
	if r := rows["socks5h://walled:1080"].Refusal; r == nil || !(*r > 0.1) {
		t.Errorf("refusal = %s, want > 0.1", show(r))
	}
	picks := acquireN(pool, "se", 40)
	if n := count(picks, "socks5h://clean:1080"); n != 20 {
		t.Errorf("clean picked %d times, want 20", n)
	}
	if n := count(picks, "socks5h://walled:1080"); n != 20 {
		t.Errorf("walled picked %d times, want 20", n)
	}
}

// Straight off the first real proxied pass: se ran at 32.8% refused while
// be and fr sat at exactly 0.0%, and because one shared counter carried all
// of it, both endpoints reported a ~0.5 refusal rate — a number that
// described amazon.se rather than either endpoint. Per site, the same
// traffic has to read honestly.
func TestProxyPool_AWalledSiteDoesNotSmearItsReputationOverTheOthers(t *testing.T) {
	pool := testFromURLs([]string{Direct, "socks5h://t:1080"}, baseCooldown(0), roundRobin())
	for i := range 30 {
		for _, url := range []string{Direct, "socks5h://t:1080"} {
			pool.ReportOK(t.Context(), "fr", url, 1500*time.Millisecond) // fr: never refused
			if i%3 == 0 {
				pool.Demote(t.Context(), "se", url, "bot wall", 0) // se: walls both paths
			}
			pool.ReportOK(t.Context(), "se", url, 1500*time.Millisecond)
		}
	}
	fr := siteRows(pool.SiteHealth("fr"))
	se := siteRows(pool.SiteHealth("se"))
	for _, url := range []string{Direct, "socks5h://t:1080"} {
		if r := fr[url].Refusal; r == nil || *r != 0.0 {
			t.Errorf("fr was clean for %s: refusal = %s", url, show(r))
		}
		if r := se[url].Refusal; r == nil || !(*r > 0.1) {
			t.Errorf("se did wall %s: refusal = %s", url, show(r))
		}
	}
	// ...and fr still splits its traffic evenly, undisturbed by se.
	picks := acquireN(pool, "fr", 20)
	if n := count(picks, Direct); n != 10 {
		t.Errorf("direct picked %d times, want 10", n)
	}
	if n := count(picks, "socks5h://t:1080"); n != 10 {
		t.Errorf("tunnel picked %d times, want 10", n)
	}
}

// Refusal is bimodal and an isolated 503 is noise. Seeding the rate at the
// first observation would put one wall at 1.0 in the report.
func TestProxyPool_OneStrayRefusalIsNotTreatedAsAWalledEndpoint(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1080"})
	pool.Demote(t.Context(), "", "socks5h://a:1080", "one bad page", 0)
	if r := pool.Health()[0].Refusal; r == nil || !(*r <= 0.2) {
		t.Errorf("refusal after one wall = %s, want <= 0.2", show(r))
	}
	for range 20 { // a sustained wall does converge on 1.0
		pool.Demote(t.Context(), "", "socks5h://a:1080", "walled", 0)
		if pool.Len() == 0 {
			break
		}
	}
	if pool.Len() != 0 {
		if r := pool.Health()[0].Refusal; r == nil || !(*r > 0.8) {
			t.Errorf("refusal after a sustained wall = %s, want > 0.8", show(r))
		}
	}
}

func TestProxyPool_LatencyDoesNotChangeRoundRobinOrder(t *testing.T) {
	urls := []string{"socks5h://fast:1080", "socks5h://slow:1080", "socks5h://unknown:1080"}
	pool := testFromURLs(urls, roundRobin())
	pool.ReportOK(t.Context(), "", urls[0], 500*time.Millisecond)
	pool.ReportOK(t.Context(), "", urls[1], 20*time.Second)
	if got, want := acquireN(pool, "", 30), repeat(urls, 10); !reflect.DeepEqual(got, want) {
		t.Errorf("picks = %v, want %v", got, want)
	}
}

func TestProxyPool_TransportFailureMeasurementsRecoverGraduallyAfterSuccess(t *testing.T) {
	pool := testFromURLs([]string{"http://reliable:80", "http://flaky:80"}, baseCooldown(0),
		roundRobin())
	for range 20 {
		pool.ReportOK(t.Context(), "de", "http://reliable:80", time.Second)
		pool.DemoteTransport(t.Context(), "se", "http://flaky:80", "", 0)
		pool.ReportOK(t.Context(), "de", "http://flaky:80", 500*time.Millisecond)
	}
	row := healthRows(pool.Health())["http://flaky:80"]
	if row.TransportFails != 0 {
		t.Errorf("transport_fails = %d, want 0", row.TransportFails)
	}
	if !(row.TransportFailure > 0.3) {
		t.Errorf("transport_failure = %v, want > 0.3", row.TransportFailure)
	}
	picks := acquireN(pool, "de", 80)
	if n := count(picks, "http://reliable:80"); n != 40 {
		t.Errorf("reliable picked %d times, want 40", n)
	}
	if n := count(picks, "http://flaky:80"); n != 40 {
		t.Errorf("flaky picked %d times, want 40", n)
	}
	for range 30 {
		pool.ReportOK(t.Context(), "de", "http://flaky:80", 500*time.Millisecond)
	}
	recovered := healthRows(pool.Health())["http://flaky:80"]
	if !(recovered.TransportFailure < row.TransportFailure) {
		t.Errorf("transport_failure %v did not fall below %v", recovered.TransportFailure, row.TransportFailure)
	}
}

// Health updates leave the next endpoint's turn intact.
func TestProxyPool_SuccessDoesNotResetRoundRobin(t *testing.T) {
	urls := []string{"socks5h://a:1080", "socks5h://b:1080"}
	pool := testFromURLs(urls, roundRobin())
	for _, expected := range repeat(urls, 3) {
		chosen := pool.Acquire("", true, nil, nil)
		if chosen != expected {
			t.Fatalf("chose %s, want %s", chosen, expected)
		}
		pool.ReportOK(t.Context(), "", chosen, time.Second)
	}
}

func TestProxyPool_CoolingEndpointRejoinsRoundRobinAfterRecovery(t *testing.T) {
	urls := []string{"http://good:80", "http://flaky:80"}
	clock := newClock()
	pool := testFromURLs(urls, roundRobin(), WithClock(clock))
	pool.Demote(t.Context(), "se", urls[1], "", 0)
	if got, want := acquireN(pool, "se", 3), repeat(urls[:1], 3); !reflect.DeepEqual(got, want) {
		t.Errorf("while cooling: %v, want %v", got, want)
	}
	clock.Advance(61 * time.Second)
	if got, want := acquireN(pool, "se", 4), repeat([]string{urls[1], urls[0]}, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("after the cooldown: %v, want %v", got, want)
	}
}

// A newly added endpoint participates without a previous measurement.
func TestProxyPool_AnUnmeasuredEndpointStillGetsATurn(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://known:1080", "socks5h://new:1080"}, roundRobin())
	for range 5 {
		pool.ReportOK(t.Context(), "", "socks5h://known:1080", time.Second)
	}
	if picks := acquireN(pool, "", 6); !contains(picks, "socks5h://new:1080") {
		t.Errorf("the new endpoint never got a turn: %v", picks)
	}
}

func TestProxyPool_RoundRobinSkipsCoolingAndExcludedRoutes(t *testing.T) {
	urls := []string{"http://known:80", "http://new-a:80", "http://new-b:80"}
	for _, cooling := range []bool{false, true} {
		name := "ready"
		if cooling {
			name = "cooling"
		}
		t.Run(name, func(t *testing.T) {
			pool := testFromURLs(urls, WithClock(newClock()))
			pool.ReportOK(t.Context(), "se", urls[0], time.Second)
			if cooling {
				pool.Demote(t.Context(), "se", urls[0], "", 0)
			}
			var exclude map[string]bool
			if !cooling {
				exclude = set(urls[0])
			}
			counter := map[string]int{}
			for range 20 {
				counter[pool.Acquire("se", true, exclude, nil)]++
			}
			if want := map[string]int{urls[1]: 10, urls[2]: 10}; !reflect.DeepEqual(counter, want) {
				t.Errorf("Counter(picks) = %v, want %v", counter, want)
			}
		})
	}
}

func TestProxyPool_HealthReportsWhatCarriedTheRun(t *testing.T) {
	pool := testFromURLs([]string{Direct, "socks5h://u:p@host:1080"})
	pool.ReportOK(t.Context(), "", Direct, 1250*time.Millisecond)
	rows := healthRows(pool.Health())
	if l := rows[Direct].Latency; l == nil || *l != 1.25 {
		t.Errorf("direct latency = %s, want 1.25", show(l))
	}
	redacted, ok := rows["socks5h://***@host:1080"] // credentials redacted
	if !ok {
		t.Fatalf("no redacted row in %v", healthURLs(pool.Health()))
	}
	if redacted.Latency != nil {
		t.Errorf("unmeasured latency = %s, want nil", show(redacted.Latency))
	}
}

// Alternating between a tunnel and the machine's own address is a rotation,
// so "direct" has to be a member rather than a special case — otherwise the
// two egress paths cannot take turns.
func TestProxyPool_DirectIsAPoolMemberNotTheAbsenceOfOne(t *testing.T) {
	if got := normalize("direct", defaultScheme); got != Direct {
		t.Errorf("Normalize(direct) = %q", got)
	}
	if got := normalize("DIRECT", defaultScheme); got != Direct {
		t.Errorf("Normalize(DIRECT) = %q", got)
	}
	// ...and a bare host:port still gets the configured scheme.
	if got := normalize("192.168.1.4:1080", defaultScheme); got != "socks5h://192.168.1.4:1080" {
		t.Errorf("Normalize(host:port) = %q", got)
	}
	pool := testFromURLs([]string{Direct, "socks5h://192.168.1.4:1080"})
	if got, want := acquireN(pool, "", 4), repeat([]string{Direct, "socks5h://192.168.1.4:1080"}, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("picks = %v, want %v", got, want)
	}
}

// The point of having two: whichever address is being refused earns a
// cooldown and the other one carries the run. Applies to `direct` exactly
// as it does to the tunnel.
func TestProxyPool_AWalledEgressPathDropsOutOfTheRotation(t *testing.T) {
	pool := testFromURLs([]string{Direct, "socks5h://192.168.1.4:1080"}, baseCooldown(300.0), withRand(seeded(1)))
	pool.Demote(t.Context(), "", Direct, "bot wall", 0)
	if got, want := acquireN(pool, "", 3), repeat([]string{"socks5h://192.168.1.4:1080"}, 3); !reflect.DeepEqual(got, want) {
		t.Errorf("while walled: %v, want %v", got, want)
	}
	pool.ReportOK(t.Context(), "", Direct, 0)
	// Back in contention once it serves again. Selection samples, so give
	// it enough turns that missing it would take a ~1e-10 run of luck.
	if picks := acquireN(pool, "", 20); !contains(picks, Direct) {
		t.Errorf("direct never came back: %v", picks)
	}
}

func TestProxyPool_RoundRobinRotation(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1", "socks5h://b:1", "socks5h://c:1"})
	got := acquireN(pool, "", 4)
	if want := []string{"socks5h://a:1", "socks5h://b:1", "socks5h://c:1", "socks5h://a:1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("picks = %v, want %v", got, want)
	}
}

func TestProxyPool_RoundRobinPositionIsPerSiteAndSurvivesRefresh(t *testing.T) {
	a, b, c, d := "http://a:80", "http://b:80", "http://c:80", "http://d:80"
	pool := testFromURLs([]string{a, b, c})
	check := func(site, want, msg string) {
		t.Helper()
		if got := pool.Acquire(site, true, nil, nil); got != want {
			t.Errorf("acquire(%s) = %s, want %s %s", site, got, want, msg)
		}
	}
	check("se", a, "")
	check("se", b, "")
	check("de", a, "")
	pool.Refresh(t.Context(), []string{d, a, b, c}, nil, nil)
	check("se", c, "")
	check("de", b, "")
	pool.NewPass()
	check("se", d, "")
	pool.Refresh(t.Context(), []string{a, b, c}, nil, nil)
	check("se", a, "(a removed last route restarts at the head)")
}

func TestProxyPool_RoundRobinRetryPrefersUntriedRoutesButCanReuseTheOnlyRoute(t *testing.T) {
	a, b, c := "http://a:80", "http://b:80", "http://c:80"
	pool := testFromURLs([]string{a, b, c})
	if got := pool.Acquire("se", true, nil, nil); got != a {
		t.Errorf("first = %s, want %s", got, a)
	}
	if got := pool.Acquire("se", true, set(b), nil); got != c {
		t.Errorf("excluding b = %s, want %s", got, c)
	}
	if got := pool.Acquire("se", true, set(a, b, c), nil); got != a {
		t.Errorf("excluding all = %s, want %s", got, a)
	}
	pool.Refresh(t.Context(), []string{a}, nil, nil)
	if got := pool.Acquire("se", true, set(a), nil); got != a {
		t.Errorf("the only route = %s, want %s", got, a)
	}
}

func TestProxyPool_FailureDemotesToTheBackWithCooldown(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1", "socks5h://b:1"}, baseCooldown(9999))
	first := pool.Acquire("", true, nil, nil)
	pool.Demote(t.Context(), "", first, "bot wall", 0)
	// the demoted one is skipped while it cools down
	if nxt := acquireN(pool, "", 3); contains(nxt, first) {
		t.Errorf("%s handed out while cooling: %v", first, nxt)
	}
}

func TestProxyPool_AllCoolingDownYieldsNoneNotAStall(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1"}, baseCooldown(9999))
	pool.Demote(t.Context(), "", pool.Acquire("", true, nil, nil), "x", 0)
	if got := pool.Acquire("", true, nil, nil); got != "" { // caller then goes direct
		t.Errorf("acquire = %q, want none", got)
	}
}

func TestProxyPool_ReportOKClearsTheCooldown(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1"}, baseCooldown(9999))
	u := pool.Acquire("", true, nil, nil)
	pool.Demote(t.Context(), "", u, "x", 0)
	if got := pool.Acquire("", true, nil, nil); got != "" {
		t.Fatalf("acquire while cooling = %q, want none", got)
	}
	pool.ReportOK(t.Context(), "", u, 0)
	if got := pool.Acquire("", true, nil, nil); got != u {
		t.Errorf("acquire after report_ok = %q, want %q", got, u)
	}
}

func TestProxyPool_RepeatedTransportFailuresKeepTheRecordUntilAgeBasedCleanup(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1", "socks5h://b:1"}, baseCooldown(0))
	for range 3 {
		pool.DemoteTransport(t.Context(), "se", "socks5h://a:1", "dead", 0)
	}
	if pool.Len() != 2 {
		t.Errorf("size = %d, want 2", pool.Len())
	}
	if c := pool.Health()[0].Cooling; !(c > 0) {
		t.Errorf("cooling = %v, want > 0", c)
	}
}

// The counterpart, and the whole point of splitting the two: refusal is a
// per-domain verdict, so amazon.se walling an endpoint must not take it
// away from the four marketplaces it is serving perfectly well.
func TestProxyPool_OneSiteRefusingNeverEvictsAnEndpoint(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1", "socks5h://b:1"}, baseCooldown(0))
	for range 10 {
		pool.Demote(t.Context(), "se", "socks5h://a:1", "bot wall", 0)
	}
	if pool.Len() != 2 { // still in the pool
		t.Errorf("size = %d, want 2", pool.Len())
	}
	if picks := acquireN(pool, "it", 4); !contains(picks, "socks5h://a:1") {
		t.Errorf("a never served it: %v", picks)
	}
	if r := siteRows(pool.SiteHealth("it"))["socks5h://a:1"].Refusal; r != nil { // it never saw a refusal
		t.Errorf("it refusal = %s, want nil", show(r))
	}
	if r := siteRows(pool.SiteHealth("se"))["socks5h://a:1"].Refusal; r == nil || !(*r > 0.8) {
		t.Errorf("se refusal = %s, want > 0.8", show(r))
	}
}

func TestProxyPool_RefreshPreservesHealthOfCarryovers(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://a:1", "socks5h://b:1"}, baseCooldown(9999))
	pool.Demote(t.Context(), "", "socks5h://a:1", "x", 0) // a:1 now cooling
	added, removed := pool.Refresh(t.Context(), []string{"socks5h://a:1", "socks5h://c:1"}, nil, nil)
	if added != 1 || removed != 1 { // +c, -b
		t.Errorf("(added, removed) = (%d, %d), want (1, 1)", added, removed)
	}
	// a:1 carried its cooldown over, so rotation still skips it
	if got := pool.Acquire("", true, nil, nil); got != "socks5h://c:1" {
		t.Errorf("acquire = %s, want socks5h://c:1", got)
	}
}

func TestProxyPool_DisabledConfigBuildsNoPool(t *testing.T) {
	empty := proxyCfg(t)
	for _, cfg := range []*Config{&empty, nil} {
		if pool, err := testLoadPool(t.Context(), cfg, nil, ""); err != nil || pool != nil {
			t.Errorf("testLoadPool(%v) = %v, %v; want nil", cfg, pool, err)
		}
	}
	if proxyCfg(t).Enabled() {
		t.Error("a [proxies] section without endpoints is enabled")
	}
	if !proxyCfg(t, withEndpoints("socks5h://a:1")).Enabled() {
		t.Error("a [proxies] section with an endpoint is not enabled")
	}
}
