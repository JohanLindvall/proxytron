package proxytron

// TestThroughputSelection: a marketplace's fetcher sends one request at a
// time, so the route to use is the one with the highest P(valid) /
// E[seconds]: valid responses per second. Thompson sampling over each
// route's record decides, untried routes share one draw from a learned
// prior, and a failed proxy is never charged to the page.
//
// The draws are seeded, and the assertions are on shares far from their
// thresholds, so they hold for any seed; TestThroughputSelection_Shares
// checks the shares themselves over many seeds. What the page shows of the
// selection is internal/report's test.

import (
	"fmt"
	"maps"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// throughputSelectionPool is a pool of urls, all from a feed, whose draws
// are seeded with seed.
func throughputSelectionPool(t testing.TB, urls []string, seed uint64, opts ...Option) *Pool {
	t.Helper()
	pool := testFromURLs(urls, append([]Option{withRand(seeded(seed))}, opts...)...)
	pool.Refresh(t.Context(), append([]string(nil), urls...), nil, append([]string(nil), urls...))
	return pool
}

// throughputSelectionRecord records valid served pages and invalid
// refusals through url on site, each taking seconds. The refusal cooldown
// is lifted afterwards: the record, not the refusal cooldown, is under
// test.
func throughputSelectionRecord(t *testing.T, pool *Pool, url string, valid, invalid int,
	seconds float64, site string) {
	t.Helper()
	for range valid {
		pool.ReportOK(t.Context(), site, url, durationOf(seconds))
	}
	for range invalid {
		pool.Demote(t.Context(), site, url, "HTTP 503", durationOf(seconds))
	}
	entry := pool.find(url)
	if entry == nil {
		t.Fatalf("%s is not in the pool", url)
	}
	entry.stat(site).cooldownUntil = time.Time{} // the record, not the refusal cooldown, is under test
}

// throughputSelectionShare is the fraction of n acquisitions on site that
// hand out url.
func throughputSelectionShare(pool *Pool, url string, n int, site string,
	wait func(string) time.Duration) float64 {
	hits := 0
	for range n {
		if pool.Acquire(site, true, nil, wait) == url {
			hits++
		}
	}
	return float64(hits) / float64(n)
}

func TestThroughputSelection_TheFasterOfTwoReliableRoutesCarriesMostPages(t *testing.T) {
	pool := throughputSelectionPool(t, []string{"http://quick:80", "http://slow:80"}, 1)
	throughputSelectionRecord(t, pool, "http://quick:80", 10, 0, 1.0, "de")
	throughputSelectionRecord(t, pool, "http://slow:80", 10, 0, 5.0, "de")
	if share := throughputSelectionShare(pool, "http://quick:80", 300, "de", nil); !(share > 0.9) {
		t.Errorf("quick share = %v, want > 0.9", share)
	}
}

// The WAF-flagged routes of 2026-10-09 came back in a fraction of a second
// because a challenge page is tiny; speed is worth nothing when the page is
// not valid.
func TestThroughputSelection_AFastRouteThatIsMostlyRefusedLosesToASlowerReliableOne(t *testing.T) {
	pool := throughputSelectionPool(t, []string{"http://flagged:80", "http://clean:80"}, 1)
	throughputSelectionRecord(t, pool, "http://flagged:80", 1, 9, 0.4, "de")
	throughputSelectionRecord(t, pool, "http://clean:80", 10, 0, 3.0, "de")
	if share := throughputSelectionShare(pool, "http://clean:80", 300, "de", nil); !(share > 0.8) {
		t.Errorf("clean share = %v, want > 0.8", share)
	}
}

func TestThroughputSelection_FourHundredUntriedRoutesDoNotOutbidOneThatWorks(t *testing.T) {
	urls := []string{"http://good:80"}
	for i := range 400 {
		urls = append(urls, fmt.Sprintf("http://u%d:80", i))
	}
	pool := throughputSelectionPool(t, urls, 1)
	throughputSelectionRecord(t, pool, "http://good:80", 10, 0, 2.0, "de")
	if share := throughputSelectionShare(pool, "http://good:80", 300, "de", nil); !(share > 0.95) {
		t.Errorf("good share = %v, want > 0.95: one draw for all untried routes, not four hundred lottery tickets",
			share)
	}
}

func TestThroughputSelection_UntriedRoutesAreExploredWhenTheKnownOnesArePoor(t *testing.T) {
	urls := []string{"http://poor:80"}
	for i := range 20 {
		urls = append(urls, fmt.Sprintf("http://u%d:80", i))
	}
	pool := throughputSelectionPool(t, urls, 1)
	throughputSelectionRecord(t, pool, "http://poor:80", 1, 9, 10.0, "de")
	if share := throughputSelectionShare(pool, "http://poor:80", 300, "de", nil); !(share < 0.5) {
		t.Errorf("poor share = %v, want < 0.5", share)
	}
}

func TestThroughputSelection_UntriedRoutesAreTriedInListOrder(t *testing.T) {
	var urls []string
	for i := range 4 {
		urls = append(urls, fmt.Sprintf("http://u%d:80", i))
	}
	pool := throughputSelectionPool(t, urls, 1)
	if got := acquireN(pool, "de", 4); !reflect.DeepEqual(got, urls) {
		t.Errorf("picks = %v, want %v", got, urls)
	}
}

func TestThroughputSelection_ABusyHostIsPricedIn(t *testing.T) {
	pool := throughputSelectionPool(t, []string{"http://a:80", "http://b:80"}, 1)
	throughputSelectionRecord(t, pool, "http://a:80", 10, 0, 1.0, "de")
	throughputSelectionRecord(t, pool, "http://b:80", 10, 0, 1.5, "de")
	if share := throughputSelectionShare(pool, "http://a:80", 300, "de", nil); !(share > 0.5) {
		t.Errorf("a share = %v, want > 0.5", share)
	}
	busy := func(url string) time.Duration {
		if url == "http://a:80" {
			return 30 * time.Second
		}
		return 0
	}
	if share := throughputSelectionShare(pool, "http://b:80", 300, "de", busy); !(share > 0.9) {
		t.Errorf("b share = %v, want > 0.9: thirty seconds of pacing wait makes the quicker route the slower",
			share)
	}
}

func TestThroughputSelection_ThePriorLearnsFromFirstAttempts(t *testing.T) {
	var urls []string
	for i := range 30 {
		urls = append(urls, fmt.Sprintf("http://d%d:80", i))
	}
	pool := throughputSelectionPool(t, urls, 1)
	p0 := pool.prior("de")
	if got := p0.A / (p0.A + p0.B); math.Abs(got-0.25) > 1e-9 {
		t.Errorf("a0 / (a0 + b0) = %v, want 0.25", got)
	}
	for _, e := range pool.entries {
		pool.DemoteTransport(t.Context(), "de", e.url, "network error", 10*time.Second)
	}
	p1 := pool.prior("de")
	if got := p1.A / (p1.A + p1.B); !(got < 0.05) {
		t.Errorf("a1 / (a1 + b1) = %v, want < 0.05: thirty dead first attempts", got)
	}
	if !(p1.D > 9.0) {
		t.Errorf("d1 = %v, want > 9.0", p1.D)
	}
}

func TestThroughputSelection_RoundRobinIsStillAvailable(t *testing.T) {
	urls := []string{"http://a:80", "http://b:80", "http://c:80"}
	pool := throughputSelectionPool(t, urls, 1, roundRobin())
	throughputSelectionRecord(t, pool, "http://c:80", 10, 0, 0.5, "de") // would win throughput selection
	if got, want := acquireN(pool, "de", 6), repeat(urls, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("picks = %v, want %v", got, want)
	}
	fastest := DefaultConfig()
	fastest.Selection = "fastest"
	if err := fastest.Validate(); err == nil || !strings.Contains(err.Error(), `selection must be "throughput" or "round_robin"`) {
		t.Errorf(`selection = "fastest": %v, want the selection refused`, err)
	}
	rr := proxyCfg(t, withEndpoints("http://a:80"), func(c *Config) { c.Selection = SelectionRoundRobin })
	loaded, err := testLoadPool(t.Context(), &rr, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.settings.Selection != SelectionRoundRobin {
		t.Errorf("LoadPool's selection = %q, want %q", loaded.settings.Selection, SelectionRoundRobin)
	}
}

// Every outcome is persisted as it happens, and the next process's pool
// restores the record and the learned prior from the database.
func TestThroughputSelection_TheRecordSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.sqlite3")
	cfg, clock := proxyCfg(t), newClock()
	urls := []string{"http://good:80", "http://new:80"}
	before := throughputSelectionPool(t, urls, 1, WithClock(clock))
	testutil.NoErr(t, before.attach(t.Context(), path, &cfg, nil))
	throughputSelectionRecord(t, before, "http://good:80", 5, 1, 2.5, "de")
	testutil.NoErr(t, before.Close())
	after := throughputSelectionPool(t, urls, 1, WithClock(clock))
	testutil.NoErr(t, after.attach(t.Context(), path, &cfg, nil))
	t.Cleanup(func() { _ = after.Close() })
	row, ok := siteRows(after.SiteHealth("de"))["http://good:80"]
	if !ok {
		t.Fatal("no health row for http://good:80")
	}
	if row.Tries != 6 || row.AttemptSeconds == nil || *row.AttemptSeconds != 2.5 {
		t.Errorf("tries, attempt seconds = %d, %s; want 6, 2.5", row.Tries, show(row.AttemptSeconds))
	}
	if got, want := throughputSelectionFirst(after), throughputSelectionFirst(before); !reflect.DeepEqual(got, want) {
		t.Errorf("restored prior = %v, want %v: the learned prior survives too", got, want)
	}
	if row.ValidPerMinute == nil || !(*row.ValidPerMinute > 0) {
		t.Errorf("valid_per_minute = %v, want > 0", show(row.ValidPerMinute))
	}
}

// throughputSelectionFirst is a copy of the pool's learned prior.
func throughputSelectionFirst(pool *Pool) map[string]firstStat {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return maps.Clone(pool.first)
}

// Thompson sampling must draw each route's share in proportion to how
// likely it is the best. Each scenario's share of 300 acquisitions,
// averaged over 40 seeds, against the reference recorded over 200 seeds
// when throughput selection was introduced.
func TestThroughputSelection_Shares(t *testing.T) {
	share := func(p *Pool, url string) float64 { return throughputSelectionShare(p, url, 300, "de", nil) }
	rec := func(p *Pool, url string, valid, invalid int, secs float64) {
		throughputSelectionRecord(t, p, url, valid, invalid, secs, "de")
	}
	for _, sc := range []struct {
		name      string
		reference float64
		run       func(seed uint64) float64
	}{
		{"the quicker of two reliable routes", 0.9527, func(s uint64) float64 {
			p := throughputSelectionPool(t, []string{"http://quick:80", "http://slow:80"}, s)
			rec(p, "http://quick:80", 10, 0, 1.0)
			rec(p, "http://slow:80", 10, 0, 5.0)
			return share(p, "http://quick:80")
		}},
		{"a poor route against twenty untried", 0.2106, func(s uint64) float64 {
			urls := []string{"http://poor:80"}
			for i := range 20 {
				urls = append(urls, fmt.Sprintf("http://u%d:80", i))
			}
			p := throughputSelectionPool(t, urls, s)
			rec(p, "http://poor:80", 1, 9, 10.0)
			return share(p, "http://poor:80")
		}},
		{"1.0 s against 1.5 s", 0.6393, func(s uint64) float64 {
			p := throughputSelectionPool(t, []string{"http://a:80", "http://b:80"}, s)
			rec(p, "http://a:80", 10, 0, 1.0)
			rec(p, "http://b:80", 10, 0, 1.5)
			return share(p, "http://a:80")
		}},
		{"a near tie", 0.4580, func(s uint64) float64 {
			p := throughputSelectionPool(t, []string{"http://a:80", "http://b:80"}, s)
			rec(p, "http://a:80", 3, 2, 2.0)
			rec(p, "http://b:80", 2, 1, 2.5)
			return share(p, "http://a:80")
		}},
	} {
		const seeds = 40
		sum := 0.0
		for s := uint64(1); s <= seeds; s++ {
			sum += sc.run(s)
		}
		if mean := sum / seeds; math.Abs(mean-sc.reference) > 0.025 {
			t.Errorf("%s: mean share %.4f, reference %.4f", sc.name, mean, sc.reference)
		}
	}
}
