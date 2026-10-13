package proxytron

// TestLiveProxyPool: the pool outlives scans and preserves health when the
// list changes. Each test has a Live of its own (newLive).

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// liveProxyPoolCfg is a [proxies] section of these static endpoints and
// refresh URL.
func liveProxyPoolCfg(t *testing.T, refreshURL string, endpoints ...string) *Config {
	t.Helper()
	cfg := proxyCfg(t, withEndpoints(endpoints...), withFeedURL(refreshURL))
	return &cfg
}

// livePool is live.Pool(ctx, cfg, marketplaces, database), failing the test
// on an error.
func livePool(t *testing.T, live *Live, cfg *Config, marketplaces []string, database string) *Pool {
	t.Helper()
	pool, err := live.Pool(t.Context(), cfg, marketplaces, database)
	if err != nil {
		t.Fatalf("Live.Pool: %v", err)
	}
	return pool
}

// liveOverride is live.Override(ctx, urls, cfg, marketplaces), failing the
// test on an error.
func liveOverride(t *testing.T, live *Live, urls []string, cfg *Config, marketplaces []string) *Pool {
	t.Helper()
	pool, err := live.Override(t.Context(), urls, cfg, marketplaces)
	if err != nil {
		t.Fatalf("Live.Override: %v", err)
	}
	return pool
}

// liveProxyPoolSameFloat reports whether two optional floats are equal.
func liveProxyPoolSameFloat(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// liveProxyPoolSet is xs without repeats, sorted for comparison.
func liveProxyPoolSet(xs []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func TestLiveProxyPool_ThePoolSurvivesARunSoItsHistoryAccumulates(t *testing.T) {
	live := newLive(t)
	cfg := liveProxyPoolCfg(t, "", "direct", "socks5h://a:1080")
	first := livePool(t, live, cfg, nil, "")
	first.ReportOK(t.Context(), "", "socks5h://a:1080", 2500*time.Millisecond)
	second := livePool(t, live, cfg, nil, "") // the next pass
	if second != first {
		t.Fatalf("the next pass got a different pool")
	}
	rows := healthRows(second.Health())
	if l := rows["socks5h://a:1080"].Latency; l == nil || *l != 2.5 {
		t.Errorf("latency = %s, want 2.5", show(l))
	}
}

func TestLiveProxyPool_EditingTheConfigChangesTheListWithoutARestart(t *testing.T) {
	live := newLive(t)
	pool := livePool(t, live, liveProxyPoolCfg(t, "", "direct", "socks5h://a:1080"), nil, "")
	pool.ReportOK(t.Context(), "", "direct", time.Second)
	same := livePool(t, live, liveProxyPoolCfg(t, "", "direct", "socks5h://b:1080"), nil, "")
	urls := healthURLs(same.Health())
	if want := []string{"direct", "socks5h://b:1080"}; !reflect.DeepEqual(urls, want) {
		t.Errorf("urls = %v, want %v", urls, want)
	}
	// health carries over for the endpoint that survived the edit
	if l := healthRows(same.Health())["direct"].Latency; l == nil || *l != 1.0 {
		t.Errorf("direct latency = %s, want 1.0", show(l))
	}
}

// The trap: Live.Pool reconciles against the watchlist at the start of
// every pass, so without recording the config at override time the next
// scan sees a difference that is really the override, and undoes it.
func TestLiveProxyPool_AnAPIOverrideIsNotRevertedByTheNextScan(t *testing.T) {
	live := newLive(t)
	cfg := liveProxyPoolCfg(t, "", "direct", "socks5h://a:1080")
	// The case that actually bites: the override lands *before* any scan
	// has built the pool, so nothing has recorded what the config looked
	// like and the first pass would otherwise treat the override as drift.
	liveOverride(t, live, []string{"socks5h://b:1080"}, cfg, nil)
	after := livePool(t, live, cfg, nil, "") // a scan starts
	if got, want := healthURLs(after.Health()), []string{"socks5h://b:1080"}; !reflect.DeepEqual(got, want) {
		t.Errorf("urls = %v, want %v", got, want)
	}
	// ...and again once a pass has been through it.
	liveOverride(t, live, []string{"socks5h://d:1080"}, cfg, nil)
	if got, want := healthURLs(livePool(t, live, cfg, nil, "").Health()),
		[]string{"socks5h://d:1080"}; !reflect.DeepEqual(got, want) {
		t.Errorf("urls = %v, want %v", got, want)
	}
}

func TestLiveProxyPool_ButAnActualConfigEditStillWins(t *testing.T) {
	live := newLive(t)
	cfg := liveProxyPoolCfg(t, "", "direct", "socks5h://a:1080")
	livePool(t, live, cfg, nil, "")
	liveOverride(t, live, []string{"socks5h://b:1080"}, cfg, nil)
	after := livePool(t, live, liveProxyPoolCfg(t, "", "direct", "socks5h://c:1080"), nil, "")
	if got, want := healthURLs(after.Health()), []string{"direct", "socks5h://c:1080"}; !reflect.DeepEqual(got, want) {
		t.Errorf("urls = %v, want %v", got, want)
	}
}

func TestLiveProxyPool_ConfiguredAnswersBeforeTheFirstScanBuildsThePool(t *testing.T) {
	live := newLive(t)
	cfg := liveProxyPoolCfg(t, "", "direct", "socks5h://u:p@host:1080")
	if p := live.Current(); p != nil {
		t.Errorf("Current() = %p, want nil", p)
	}
	if got, want := Configured(cfg), []string{"direct", "socks5h://***@host:1080"}; !reflect.DeepEqual(got, want) {
		t.Errorf("configured = %v, want %v", got, want)
	}
	empty := proxyCfg(t)
	for _, c := range []*Config{&empty, nil} {
		if got := Configured(c); !reflect.DeepEqual(got, []string{}) {
			t.Errorf("Configured(%v) = %#v, want []", c, got)
		}
	}
}

func TestLiveProxyPool_DisablingProxiesInTheConfigDropsThePool(t *testing.T) {
	live := newLive(t)
	livePool(t, live, liveProxyPoolCfg(t, "", "socks5h://a:1080"), nil, "")
	if live.Current() == nil {
		t.Errorf("Current() is nil after a scan built the pool")
	}
	empty := proxyCfg(t)
	if p := livePool(t, live, &empty, nil, ""); p != nil {
		t.Errorf("Pool(an empty [proxies]) = %p, want nil", p)
	}
	if p := live.Current(); p != nil {
		t.Errorf("Current() = %p, want nil", p)
	}
	livePool(t, live, liveProxyPoolCfg(t, "", "socks5h://a:1080"), nil, "")
	if p := livePool(t, live, nil, nil, ""); p != nil || live.Current() != nil {
		t.Errorf("Pool(nil) = %p, Current() = %p; want nil, nil", p, live.Current())
	}
}

func TestLiveProxyPool_CountryFeedsRefreshHourlyAndPreserveHealth(t *testing.T) {
	clock := newClock()
	countries := []string{"se", "de", "fr", "be", "it"}
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/{country}/data.json"),
		withRefreshInterval(60), withMinReady(0))

	response := func(url string) (string, error) {
		parts := strings.Split(url, "/")
		country := parts[len(parts)-2]
		// A record of this country and one of another.
		return fmt.Sprintf(`[{"proxy": "socks5://%s.example:1080", "geolocation": {"country": "%s"}}, `+
			`{"proxy": "http://wrong.example:8080", "geolocation": {"country": "XX"}}]`,
			strings.ToLower(country), country), nil
	}

	get := stubFeeds(response)
	live := newLive(t, WithClock(clock), get.option())
	pool := livePool(t, live, &cfg, countries, "")
	if n := get.count(); n != 5 {
		t.Errorf("downloads = %d, want 5", n)
	}
	var want []string
	for _, c := range countries {
		want = append(want, fmt.Sprintf("https://feeds.test/%s/data.json", strings.ToUpper(c)))
	}
	if got := liveProxyPoolSet(get.urls()); !reflect.DeepEqual(got, liveProxyPoolSet(want)) {
		t.Errorf("feeds asked for = %v, want %v", got, liveProxyPoolSet(want))
	}
	for _, country := range countries {
		endpoint := fmt.Sprintf("socks5h://%s.example:1080", country)
		if got := pool.Acquire(country, true, nil, nil); got != endpoint {
			t.Errorf("acquire(%s) = %q, want %q", country, got, endpoint)
		}
		if n := pool.PathCount(country, true); n != 1 {
			t.Errorf("path_count(%s) = %d, want 1", country, n)
		}
		if got := siteURLs(pool.SiteHealth(country)); !reflect.DeepEqual(got, []string{endpoint}) {
			t.Errorf("health(%s) urls = %v, want [%s]", country, got, endpoint)
		}
	}
	if got := pool.Acquire("es", true, nil, nil); got != "" {
		t.Errorf("Acquire(es) = %q, want none", got)
	}
	if secs, ok := pool.WaitFor("es", true, nil); ok {
		t.Errorf("WaitFor(es) = %v, want no route at all", secs)
	}
	pool.ReportOK(t.Context(), "se", "socks5h://se.example:1080", 400*time.Millisecond)
	pool.settings.BrokenCooldown = 9999 * time.Second // a failed connection's bench, for a proxy that has served
	pool.DemoteTransport(t.Context(), "se", "socks5h://se.example:1080", "", 0)
	before := pool.SiteHealth("se")[0]
	clock.Advance(3599 * time.Second)
	if p := livePool(t, live, &cfg, countries, ""); p != pool {
		t.Errorf("Live.Pool returned a different pool")
	}
	if n := get.count(); n != 5 {
		t.Errorf("downloads = %d, want 5", n)
	}
	clock.Advance(time.Second)
	if p := livePool(t, live, &cfg, countries, ""); p != pool {
		t.Errorf("Live.Pool returned a different pool")
	}
	if n := get.count(); n != 10 {
		t.Errorf("downloads = %d, want 10", n)
	}
	after := pool.SiteHealth("se")[0]
	if !liveProxyPoolSameFloat(after.Latency, before.Latency) {
		t.Errorf("latency = %s, want %s", show(after.Latency), show(before.Latency))
	}
	if after.TransportFailure != before.TransportFailure {
		t.Errorf("transport_failure = %v, want %v", after.TransportFailure, before.TransportFailure)
	}
	if !(after.Cooling > 0) {
		t.Errorf("cooling = %v, want > 0", after.Cooling)
	}
}

func TestLiveProxyPool_AFailedCountryFeedKeepsItsPreviousListWhileOthersUpdate(t *testing.T) {
	clock := newClock()
	cfg := proxyCfg(t, withEndpoints("http://static.example:80"),
		withFeedURL("https://feeds.test/{country}/data.json"), withRefreshInterval(60))
	var mu sync.Mutex
	feeds := map[string]string{"SE": `[{"proxy":"http://se.example:80"}]`,
		"DE": `[{"proxy":"http://old-de.example:80"}]`}

	response := func(url string) (string, error) {
		parts := strings.Split(url, "/")
		mu.Lock()
		defer mu.Unlock()
		return feeds[parts[len(parts)-2]], nil
	}
	update := func(key, body string) {
		mu.Lock()
		defer mu.Unlock()
		feeds[key] = body
	}

	live := newLive(t, WithClock(clock), stubFeeds(response).option())
	sites := []string{"se", "de"}
	pool := livePool(t, live, &cfg, sites, "")
	update("SE", "{broken json")
	update("DE", `["http://new-de.example:80"]`)
	clock.Advance(time.Hour)
	livePool(t, live, &cfg, sites, "")
	if got, want := liveProxyPoolSet(siteURLs(pool.SiteHealth("se"))),
		liveProxyPoolSet([]string{"http://se.example:80", "http://static.example:80"}); !reflect.DeepEqual(got, want) {
		t.Errorf("se urls = %v, want %v", got, want)
	}
	if got, want := liveProxyPoolSet(siteURLs(pool.SiteHealth("de"))),
		liveProxyPoolSet([]string{"http://old-de.example:80", "http://new-de.example:80",
			"http://static.example:80"}); !reflect.DeepEqual(got, want) {
		t.Errorf("de urls = %v, want %v", got, want)
	}
	update("SE", "[]") // disappearance no longer forgets a young endpoint
	clock.Advance(time.Hour)
	livePool(t, live, &cfg, sites, "")
	if got, want := liveProxyPoolSet(siteURLs(pool.SiteHealth("se"))),
		liveProxyPoolSet([]string{"http://static.example:80", "http://se.example:80"}); !reflect.DeepEqual(got, want) {
		t.Errorf("se urls = %v, want %v", got, want)
	}
}

func TestLiveProxyPool_InitialFeedFailureIsRetriedOnSchedule(t *testing.T) {
	clock := newClock()
	cfg := liveProxyPoolCfg(t, "https://feeds.test/data.json")
	feedDown := errors.New("feed down")
	get := stubFeeds(func(string) (string, error) { return "", feedDown })
	live := newLive(t, WithClock(clock), get.option())
	pool := livePool(t, live, cfg, nil, "")
	if n := pool.Len(); n != 0 {
		t.Errorf("size = %d, want 0", n)
	}
	clock.Advance(30 * time.Second)
	if p := livePool(t, live, cfg, nil, ""); p != pool {
		t.Errorf("Live.Pool returned a different pool")
	}
	if n := get.count(); n != 1 {
		t.Errorf("downloads = %d, want 1", n)
	}
	clock.Advance(3570 * time.Second)
	livePool(t, live, cfg, nil, "")
	if n := get.count(); n != 2 {
		t.Errorf("downloads = %d, want 2", n)
	}
}

// The server's timer keeps the feeds fresh without any scan: over an hour
// of one-minute ticks, the feed is downloaded on the first and again an
// hour later, and the loop stops when its ctx ends.
func TestLiveProxyPool_ServerTimerRefreshesWithoutAnyScans(t *testing.T) {
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/data.json"), withRefreshInterval(60), withMinReady(1))
	app := *fixtureApp()
	app.Proxies = cfg
	synctest.Test(t, func(t *testing.T) {
		get := stubFeeds(feedText(`["http://a.example:80"]`))
		live := testNewLive(get.option()) // the bubble's clock
		var ticks int
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			live.RefreshLoop(ctx, func() (*Config, []string, error) { ticks++; return &app.Proxies, app.Codes(), nil }, "")
		}()
		time.Sleep(time.Hour + 30*time.Second)
		synctest.Wait()
		cancel()
		<-done
		if n := get.count(); n != 2 {
			t.Errorf("%d downloads, want 2: one initial load and another an hour later, without scans", n)
		}
		if ticks != 61 { // at 0 s and every minute through 60 min
			t.Errorf("%d ticks in 60.5 minutes, want 61", ticks)
		}
		if err := live.Close(); err != nil {
			t.Error(err)
		}
	})
}

// RefreshLoop ticks only while its ctx lives: an ended ctx runs no tick at
// all, and one that ends between ticks stops the loop.
func TestRefreshLoop_EndsWithItsContext(t *testing.T) {
	app := fixtureApp()
	t.Run("already ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		calls := 0
		newLive(t).RefreshLoop(ctx, func() (*Config, []string, error) { calls++; return &app.Proxies, app.Codes(), nil }, "")
		if calls != 0 {
			t.Errorf("an ended ctx still ran %d ticks", calls)
		}
	})
	t.Run("ends between ticks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			calls := 0
			done := make(chan struct{})
			go func() {
				defer close(done)
				testNewLive().RefreshLoop(ctx, func() (*Config, []string, error) {
					calls++
					return &Config{}, nil, nil // no [proxies]: nothing to do
				}, "")
			}()
			time.Sleep(150 * time.Second)
			cancel()
			<-done
			if calls != 3 { // 0, 60 and 120 s
				t.Errorf("%d ticks in 150 s, want 3", calls)
			}
		})
	})
}

func TestLiveProxyPool_CountryAPIOverrideSurvivesTimerTicks(t *testing.T) {
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/{country}/data.json"))
	get := stubFeeds(nil)
	live := newLive(t, get.option())
	liveOverride(t, live, []string{"http://override.example:80"}, &cfg, []string{"se", "de"})
	pool := livePool(t, live, &cfg, []string{"se", "de"}, "")
	if n := get.count(); n != 0 {
		t.Errorf("%d downloads, want none: %v", n, get.urls())
	}
	if got := pool.Acquire("se", true, nil, nil); got != "http://override.example:80" {
		t.Errorf("acquire(se) = %q, want http://override.example:80", got)
	}
}
