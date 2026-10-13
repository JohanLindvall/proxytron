package proxytron

// TestBrokenProxiesBenched: round-robin gives every proxy off its bench a
// full turn, so a dead feed entry used to cost an attempt a minute after it
// failed: on c016fcf's first pass (2026-10-09T11:36Z) amazon.de spent 429 of
// its 500 requests and amazon.fr 396 on proxies that never connected. Now a
// failed connection benches a proxy that never served a page for a day, and
// one that has served for ten minutes doubling, and the benches outlive a
// restart.
//
// The rest of the story is in the packages above internal/proxy:
//   - internal/fetch's TestBrokenProxiesBenched_* drive a Fetcher through
//     the pool (broken_proxies_test.go);
//   - internal/scan's TestBrokenProxiesBenched_RunUsesTheDatabaseAndImports-
//     LegacyMemoryOnce and _ProxyOutcomesAreSavedEvenIfASiteFailsMidPass run
//     a pass, whose pool-side assertions the *_PoolPart tests here check
//     without the pass around them;
//   - internal/report's TestProxyState names the bench on the page.

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/logtest"
	"github.com/JohanLindvall/proxytron/internal/testutil"
)

const (
	brokenProxiesBenchedDead = "http://dead:80"
	brokenProxiesBenchedGood = "http://good:80"
)

// brokenProxiesBenchedFeed is _feed: a pool whose entries came from a
// refresh feed, as the shipped country feeds' do; FromURLs alone builds
// static endpoints.
func brokenProxiesBenchedFeed(t testing.TB, urls []string, opts ...Option) *Pool {
	t.Helper()
	pool := testFromURLs(urls, opts...)
	pool.Refresh(t.Context(), urls, nil, urls)
	return pool
}

// brokenProxiesBenchedRow is url's SiteHealth row on site, failing the test
// when the URL has no row.
func brokenProxiesBenchedRow(t *testing.T, pool *Pool, url, site string) SiteHealth {
	t.Helper()
	row, ok := siteRows(pool.SiteHealth(site))[url]
	if !ok {
		t.Fatalf("no health row for %s on %s", url, site)
	}
	return row
}

// brokenProxiesBenchedBench renders a bench for a message ("none" for "").
func brokenProxiesBenchedBench(b Bench) string {
	if b == "" {
		return "none"
	}
	return "'" + string(b) + "'"
}

func TestBrokenProxiesBenched_AProxyThatNeverServedSitsOutADayAfterOneFailedConnection(t *testing.T) {
	dead, good := brokenProxiesBenchedDead, brokenProxiesBenchedGood
	clock := newClock()
	pool := brokenProxiesBenchedFeed(t, []string{dead, good}, WithClock(clock), WithLegacyImport(true))
	pool.DemoteTransport(t.Context(), "se", dead, "network error", 0)
	row := brokenProxiesBenchedRow(t, pool, dead, "se")
	if row.Bench != BenchDead {
		t.Errorf("bench = %s, want 'dead'", brokenProxiesBenchedBench(row.Bench))
	}
	if !(row.Cooling > 86000) {
		t.Errorf("cooling = %v, want > 86000", row.Cooling)
	}
	clock.Advance(23 * time.Hour)
	picked := map[string]bool{}
	for range 10 {
		picked[pool.Acquire("se", true, nil, nil)] = true
	}
	if !reflect.DeepEqual(picked, set(good)) {
		t.Errorf("picked %v, want only %s", picked, good)
	}
	clock.Advance(3601 * time.Second)
	if got := pool.Acquire("se", true, set(good), nil); got != dead {
		t.Errorf("acquire excluding the good route = %q, want %q: a day, not forever", got, dead)
	}
}

// A personal tunnel that blips right after a restart, before its first
// page, is still the operator's chosen route: a day on the bench would take
// its site down with it.
func TestBrokenProxiesBenched_AConfiguredEndpointOrDirectIsNeverJudgedDeadOnOneFailure(t *testing.T) {
	pool := testFromURLs([]string{"socks5h://tunnel:1080", "direct"}, WithClock(newClock()))
	for _, url := range []string{"socks5h://tunnel:1080", "direct"} {
		pool.DemoteTransport(t.Context(), "se", url, "network error", 0)
		if got := brokenProxiesBenchedRow(t, pool, url, "se").Bench; got != "broken" {
			t.Errorf("%s: bench = %s, want 'broken'", url, brokenProxiesBenchedBench(got))
		}
		if got := brokenProxiesBenchedRow(t, pool, url, "se").Cooling; !(got <= 600) {
			t.Errorf("%s: cooling = %v, want <= 600", url, got)
		}
	}
}

func TestBrokenProxiesBenched_ACountryFeedMarksItsEntriesAndTheStaticListDoesNot(t *testing.T) {
	cfg := proxyCfg(t, withEndpoints("socks5h://tunnel:1080"),
		withFeedURL("https://feeds.test/{country}/data.json"))
	// One Proxifly record for SE.
	feeds := stubFeeds(feedText(`[{"proxy": "socks5://se.example:1080", "geolocation": {"country": "SE"}}]`))
	pool, err := testLoadPool(t.Context(), &cfg, []string{"se"}, "", feeds.option())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range pool.entries {
		got[e.url] = e.fromFeed
	}
	want := map[string]bool{"socks5h://tunnel:1080": false, "socks5h://se.example:1080": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("{e.url: e.from_feed} = %v, want %v", got, want)
	}
}

func TestBrokenProxiesBenched_AProxyThatHasServedGetsMinutesDoublingUpToTheDay(t *testing.T) {
	good := brokenProxiesBenchedGood
	pool := testFromURLs([]string{good}, WithClock(newClock()))
	pool.ReportOK(t.Context(), "se", good, time.Second)
	var benches []int
	for i := range 10 {
		pool.DemoteTransport(t.Context(), "se", good, "network error", 0)
		benches = append(benches, int(math.RoundToEven(pool.SiteHealth("se")[0].Cooling)))
		if got := pool.SiteHealth("se")[0].Bench; got != "broken" {
			t.Errorf("demotion %d: bench = %s, want 'broken'", i+1, brokenProxiesBenchedBench(got))
		}
	}
	pool.ReportOK(t.Context(), "se", good, time.Second) // it came back
	pool.DemoteTransport(t.Context(), "se", good, "network error", 0)
	again := int(math.RoundToEven(pool.SiteHealth("se")[0].Cooling))
	if want := []int{600, 1200, 2400, 4800, 9600}; !reflect.DeepEqual(benches[:5], want) {
		t.Errorf("benches[:5] = %v, want %v", benches[:5], want)
	}
	if got := benches[len(benches)-1]; got != 86400 {
		t.Errorf("benches[-1] = %d, want 86400: capped at the dead entry's day", got)
	}
	if again != 600 {
		t.Errorf("again = %d, want 600: a served page clears the doubling", again)
	}
}

// What each route has served and the benches it earned outlive a restart:
// every outcome is persisted as it happens, and the next process's pool
// restores them from the database before its first request.
func TestBrokenProxiesBenched_BenchesAndProvenProxiesSurviveARestart(t *testing.T) {
	const waf = "http://flagged:80"
	dead, good := brokenProxiesBenchedDead, brokenProxiesBenchedGood
	urls := []string{dead, good, waf, "http://untouched:80"}
	path := filepath.Join(t.TempDir(), "run.sqlite3")
	cfg := proxyCfg(t)
	clock := newClock()
	before := brokenProxiesBenchedFeed(t, urls, WithClock(clock), WithLegacyImport(true))
	testutil.NoErr(t, before.attach(t.Context(), path, &cfg, nil))
	before.DemoteTransport(t.Context(), "se", dead, "", 0)
	before.ReportOK(t.Context(), "se", good, time.Second)
	before.Punish(t.Context(), "se", waf, "AWS WAF challenge", 0)
	testutil.NoErr(t, before.Close())
	after := brokenProxiesBenchedFeed(t, append(slices.Clone(urls), "http://new:80"), WithClock(clock), WithLegacyImport(true)) // a restart
	testutil.NoErr(t, after.attach(t.Context(), path, &cfg, nil))
	t.Cleanup(func() { _ = after.Close() })
	testutil.NoErr(t, after.attach(t.Context(), path, &cfg, nil)) // once per pool: a no-op
	if got := brokenProxiesBenchedRow(t, after, dead, "se").Bench; got != BenchDead {
		t.Errorf("dead: bench = %s, want 'dead'", brokenProxiesBenchedBench(got))
	}
	if got := brokenProxiesBenchedRow(t, after, dead, "se").Cooling; !(got > 86000) {
		t.Errorf("dead: cooling = %v, want > 86000", got)
	}
	if got := brokenProxiesBenchedRow(t, after, waf, "se").Bench; got != BenchWAF {
		t.Errorf("flagged: bench = %s, want 'waf'", brokenProxiesBenchedBench(got))
	}
	if got := brokenProxiesBenchedRow(t, after, waf, "se").Cooling; !(got > 3500) {
		t.Errorf("flagged: cooling = %v, want > 3500", got)
	}
	if got := brokenProxiesBenchedRow(t, after, good, "se").Bench; got != "" {
		t.Errorf("good: bench = %s, want none", brokenProxiesBenchedBench(got))
	}
	after.DemoteTransport(t.Context(), "se", good, "", 0)
	if got := brokenProxiesBenchedRow(t, after, good, "se").Bench; got != BenchBroken {
		t.Errorf("good: bench = %s, want 'broken': remembered as having served, so a blip is minutes, not a day",
			brokenProxiesBenchedBench(got))
	}
	for _, url := range []string{"http://untouched:80", "http://new:80"} {
		if got := brokenProxiesBenchedRow(t, after, url, "se").Bench; got != "" {
			t.Errorf("%s: bench = %s, want none", url, brokenProxiesBenchedBench(got))
		}
	}
}

// The one-time import of the legacy proxy-memory.json beside a fresh
// database: a bench that has run out is not restored, and a file that
// cannot be read, or is not there, is a fresh start rather than an error.
func TestBrokenProxiesBenched_AnExpiredBenchOrAnUnreadableFileIsAFreshStart(t *testing.T) {
	dead := brokenProxiesBenchedDead
	clock := newClock()
	cfg := proxyCfg(t, withEndpoints(dead))
	load := func(memory string) *Pool {
		t.Helper()
		dir := t.TempDir()
		if memory != "" {
			testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(memory), 0o644))
		}
		pool := testutil.Must(testLoadPool(t.Context(), &cfg, nil, filepath.Join(dir, "run.sqlite3"), WithClock(clock), WithLegacyImport(true)))(t)
		t.Cleanup(func() { _ = pool.Close() })
		return pool
	}
	// The dead route's day ran out five seconds ago.
	pool := load(fmt.Sprintf(`[{"url": %q, "served": 0, "fails": 1, "until": %f, "sites": {}}]`,
		dead, wallOf(clock)-5))
	if got := brokenProxiesBenchedRow(t, pool, dead, "se"); got.Bench != "" || got.Cooling != 0 {
		t.Errorf("bench, cooling = %s, %v; want none, 0: its day had run out",
			brokenProxiesBenchedBench(got.Bench), got.Cooling)
	}
	rec := logtest.Capture(t, slog.LevelWarn)
	pool = load("{not json")
	if got := brokenProxiesBenchedRow(t, pool, dead, "se").Bench; got != "" {
		t.Errorf("unreadable file: bench = %s, want none", brokenProxiesBenchedBench(got))
	}
	if got := rec.Messages("amzscrape.proxy"); len(got) != 1 || !strings.HasPrefix(got[0], "legacy proxy memory unreadable: ") {
		t.Errorf("logged %q, want one 'legacy proxy memory unreadable' warning", got)
	}
	if pool = load(""); pool.Len() != 1 {
		t.Errorf("a missing file: %d routes, want 1", pool.Len())
	}
}

func TestBrokenProxiesBenched_TheBenchesComeFromConfig(t *testing.T) {

	good := brokenProxiesBenchedGood
	cfg := proxyCfg(t, withEndpoints(good),
		func(c *Config) { c.BrokenCooldown = 5 * time.Minute },
		func(c *Config) { c.DeadCooldown = 2 * time.Hour })
	pool, err := testLoadPool(t.Context(), &cfg, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := pool.settings; got.BrokenCooldown != 5*time.Minute || got.DeadCooldown != 2*time.Hour {
		t.Errorf("(broken_cooldown, dead_cooldown) = (%v, %v), want (5m, 2h)", got.BrokenCooldown, got.DeadCooldown)
	}
	plain := proxyCfg(t, withEndpoints(good))
	k1, k2 := testutil.Must(keyOf(&cfg, nil))(t), testutil.Must(keyOf(&plain, nil))(t)
	if k1 == k2 {
		t.Errorf("keyOf does not see broken_proxy_minutes / dead_proxy_hours")
	}
}

// The pool half of internal/scan's
// TestBrokenProxiesBenched_RunUsesTheDatabaseAndImportsLegacyMemoryOnce:
// LoadPool with a database imports the legacy proxy-memory.json beside it
// once, and the database, not the old file, is what the next start reads.
func TestBrokenProxiesBenched_RunUsesTheDatabaseAndImportsLegacyMemoryOnce_PoolPart(t *testing.T) {
	dead, good := brokenProxiesBenchedDead, brokenProxiesBenchedGood
	tmp := t.TempDir()
	text := fmt.Sprintf(`[{"url": %q, "served": 0, "fails": 1, "until": %f, "sites": {}}]`,
		dead, float64(time.Now().UnixNano())/1e9+3600)
	if err := os.WriteFile(filepath.Join(tmp, legacyMemory), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(tmp, "run.sqlite3")
	cfg := proxyCfg(t, withEndpoints(dead, good))
	pool, err := testLoadPool(t.Context(), &cfg, nil, path, WithLegacyImport(true))
	if err != nil {
		t.Fatal(err)
	}
	pool.NewPass() // what scan.Run does first
	if got := brokenProxiesBenchedRow(t, pool, dead, "se").Bench; got != "broken" {
		t.Errorf("bench = %s, want 'broken': restored before the pass", brokenProxiesBenchedBench(got))
	}
	pool.ReportOK(t.Context(), "se", dead, time.Second)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := testLoadPool(t.Context(), &cfg, nil, path, WithLegacyImport(true))
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if got := brokenProxiesBenchedRow(t, after, dead, "se").Bench; got != "" {
		t.Errorf("bench = %s, want none: old JSON cannot resurrect the bench", brokenProxiesBenchedBench(got))
	}
	if n := after.Len(); n != 2 {
		t.Errorf("size = %d, want 2: the untested endpoint is stored too", n)
	}
}

// The pool half of internal/scan's
// TestBrokenProxiesBenched_ProxyOutcomesAreSavedEvenIfASiteFailsMidPass: an
// outcome is committed when it happens, so the next start sees it whatever
// became of the pass.
func TestBrokenProxiesBenched_ProxyOutcomesAreSavedEvenIfASiteFailsMidPass_PoolPart(t *testing.T) {
	dead, good := brokenProxiesBenchedDead, brokenProxiesBenchedGood
	path := filepath.Join(t.TempDir(), "run.sqlite3")
	cfg := proxyCfg(t, withEndpoints(dead, good))
	pool, err := testLoadPool(t.Context(), &cfg, nil, path, WithLegacyImport(true))
	if err != nil {
		t.Fatal(err)
	}
	pool.DemoteTransport(t.Context(), "se", dead, "", 0) // se's scan, before de's fails
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := testLoadPool(t.Context(), &cfg, nil, path, WithLegacyImport(true))
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if got := brokenProxiesBenchedRow(t, after, dead, "se").Bench; got != "broken" {
		t.Errorf("bench = %s, want 'broken'", brokenProxiesBenchedBench(got))
	}
	if n := after.Len(); n != 2 {
		t.Errorf("size = %d, want 2", n)
	}
}
