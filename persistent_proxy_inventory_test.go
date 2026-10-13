package proxytron

// TestPersistentProxyInventory: database-first startup, source freshness
// and inventory retirement.

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// persistentProxyInventory is one test's setting: a database path, a fake
// clock, a {country} feed config and the feeds it downloads, with the
// routes those feeds list.
type persistentProxyInventory struct {
	t *testing.T

	URL    string
	GOOD   string
	UNUSED string
	DEAD   string

	path  string
	clock *testutil.Clock
	cfg   Config
	get   *feedStub

	mu sync.Mutex
	// What each feed answers, by uppercase country: a []any to encode as
	// JSON, or an error for the download to fail with. Read from the
	// refresh goroutines, so locked.
	feeds map[string]any
}

// persistentProxyInventorySetUp is a temporary database path, the fake
// clock, a {country} feed config with min_ready=1, and feed downloads
// answered from s.feeds, which list GOOD, UNUSED and DEAD for SE.
func persistentProxyInventorySetUp(t *testing.T) *persistentProxyInventory {
	t.Helper()
	s := &persistentProxyInventory{
		t:      t,
		URL:    "https://raw.githubusercontent.com/example/proxies/main/{country}.json",
		GOOD:   "http://good.example:80",
		UNUSED: "http://unused.example:80",
		DEAD:   "http://dead.example:80",
	}
	s.path = filepath.Join(t.TempDir(), "test.sqlite3")
	s.clock = newClock()
	s.cfg = proxyCfg(t, withFeedURL(s.URL), withMinReady(1))
	s.feeds = map[string]any{"SE": []any{s.GOOD, s.UNUSED, s.DEAD}}
	s.get = stubFeeds(s.response)
	return s
}

// errPersistentProxyInventoryKey is the download failure for a country the
// test did not list.
var errPersistentProxyInventoryKey = errors.New("no such feed")

func (s *persistentProxyInventory) response(url string) (string, error) {
	name := url[strings.LastIndex(url, "/")+1:]
	s.mu.Lock()
	value, ok := s.feeds[strings.Split(name, ".")[0]]
	s.mu.Unlock()
	if !ok {
		return "", errPersistentProxyInventoryKey
	}
	if err, isErr := value.(error); isErr {
		return "", err
	}
	body, err := json.Marshal(value)
	return string(body), err
}

// setFeed makes the feed for country key answer value.
func (s *persistentProxyInventory) setFeed(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeds[key] = value
}

// editFeed changes the feed for country key in place.
func (s *persistentProxyInventory) editFeed(key string, edit func([]any) []any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeds[key] = edit(s.feeds[key].([]any))
}

// load is LoadPool from the database, closed when the test ends: with s.cfg
// when cfg is nil, and for se when no countries are given.
func (s *persistentProxyInventory) load(cfg *Config, countries ...string) *Pool {
	s.t.Helper()
	c := s.cfg
	if cfg != nil {
		c = *cfg
	}
	if len(countries) == 0 {
		countries = []string{"se"}
	}
	pool, err := testLoadPool(s.t.Context(), &c, countries, s.path, WithClock(s.clock), s.get.option())
	if err != nil {
		s.t.Fatalf("LoadPool: %v", err)
	}
	s.t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// close is pool.close(), which raises if the commit or close fails.
func (s *persistentProxyInventory) close(pool *Pool) {
	s.t.Helper()
	if err := pool.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
}

// rows is every stored proxy row, read over a separate connection.
func (s *persistentProxyInventory) rows() map[string]map[string]any {
	s.t.Helper()
	return storedProxyRows(s.t, s.path)
}

// refreshSources is refreshSources(ctx, pool, cfg, marketplaces), failing
// the test on an error.
func (s *persistentProxyInventory) refreshSources(pool *Pool, cfg Config, marketplaces ...string) {
	s.t.Helper()
	if err := refreshSources(s.t.Context(), pool, &cfg, marketplaces); err != nil {
		s.t.Fatalf("refreshSources: %v", err)
	}
}

// sourceKey is the SE feed's source.
func (s *persistentProxyInventory) sourceKey() source {
	return source{url: strings.ReplaceAll(s.URL, "{country}", "SE"), site: "se"}
}

// persistentProxyInventoryReplace is a copy of cfg with the changes,
// validated again.
func persistentProxyInventoryReplace(t *testing.T, cfg Config, opts ...cfgOpt) Config {
	t.Helper()
	for _, o := range opts {
		o(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("ProxyCfg: %v", err)
	}
	return cfg
}

// persistentProxyInventoryKeys is the keys of a {url: row} mapping, sorted.
func persistentProxyInventoryKeys[V any](m map[string]V) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// persistentProxyInventorySorted is xs, sorted.
func persistentProxyInventorySorted(xs ...string) []string {
	out := append([]string{}, xs...)
	sort.Strings(out)
	return out
}

// persistentProxyInventoryFirst is a copy of the pool's learned prior.
func persistentProxyInventoryFirst(pool *Pool) map[string]firstStat {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return maps.Clone(pool.first)
}

// persistentProxyInventoryCooling is each row's cooling seconds by URL.
func persistentProxyInventoryCooling(rows []SiteHealth) map[string]float64 {
	out := map[string]float64{}
	for _, h := range rows {
		out[h.URL] = h.Cooling
	}
	return out
}

func TestPersistentProxyInventory_AllInventoryAndFeedMetadataAreStoredBeforeAnyRequest(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	metadata := map[string]any{
		"proxy":       s.UNUSED,
		"protocol":    "http",
		"https":       false,
		"score":       7.0,
		"geolocation": map[string]any{"country": "SE", "city": "Stockholm"},
		"custom":      map[string]any{"keep": true},
	}
	s.editFeed("SE", func(feed []any) []any { feed[1] = metadata; return feed })
	pool := s.load(nil)
	rows := s.rows()
	if got, want := persistentProxyInventoryKeys(rows),
		persistentProxyInventorySorted(s.GOOD, s.UNUSED, s.DEAD); !reflect.DeepEqual(got, want) {
		t.Fatalf("stored urls = %v, want %v", got, want)
	}
	unused := rows[s.UNUSED]
	if v, has := unused["last_tested"]; !has || v != nil {
		t.Errorf("last_tested = %v (present %v), want null", v, has)
	}
	if v, has := unused["last_success"]; !has || v != nil {
		t.Errorf("last_success = %v (present %v), want null", v, has)
	}
	stored := object(unused, "metadata")[strings.ReplaceAll(s.URL, "{country}", "SE")]
	if !reflect.DeepEqual(stored, metadata) {
		t.Errorf("metadata = %v, want %v", stored, metadata)
	}
	s.close(pool)
	s.get.reset()
	after := s.load(nil)
	if n := s.get.count(); n != 0 {
		t.Errorf("%d downloads, want none: %v", n, s.get.urls())
	}
	if n := after.Len(); n != 3 {
		t.Errorf("size = %d, want 3", n)
	}
	if n := after.PathCount("de", true); n != 0 {
		t.Errorf("path_count(de) = %d, want 0: restoration preserves country scope", n)
	}
	for _, h := range after.SiteHealth("se") {
		if h.ValidPerMinute != nil {
			t.Errorf("%s valid_per_minute = %s, want nil", h.URL, show(h.ValidPerMinute))
		}
	}
}

func TestPersistentProxyInventory_FullScoresBenchesAndPriorsSurviveWithoutAScanCheckpoint(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	pool.ReportOK(t.Context(), "se", s.GOOD, 2500*time.Millisecond)
	pool.Demote(t.Context(), "se", s.GOOD, "refused", time.Second)
	pool.DemoteTransport(t.Context(), "se", s.DEAD, "timeout", 10*time.Second)
	pool.Punish(t.Context(), "se", s.UNUSED, "challenge", 500*time.Millisecond)
	// A separate connection sees outcomes immediately, before close/run completion.
	rows := s.rows()
	se := object(object(rows[s.GOOD], "sites"), "se")
	if score, ok := se["score"].(float64); !ok || !(score > 0) {
		t.Errorf("score = %v, want > 0", se["score"])
	}
	if tries := se["tries"]; tries != 2.0 {
		t.Errorf("tries = %v, want 2", tries)
	}
	before := pool.SiteHealth("se")
	prior := persistentProxyInventoryFirst(pool)
	s.close(pool)
	after := s.load(nil)
	if got := after.SiteHealth("se"); !reflect.DeepEqual(got, before) {
		t.Errorf("health(se) after restart:\n got %s\nwant %s", asJSON(t, got), asJSON(t, before))
	}
	if got := persistentProxyInventoryFirst(after); !reflect.DeepEqual(got, prior) {
		t.Errorf("prior = %v, want %v", got, prior)
	}
	if tf := after.catalog.get(s.DEAD).transportFailure; tf != 0.2 {
		t.Errorf("transport_failure = %v, want 0.2", tf)
	}
}

// A restarted process starts a new monotonic clock (a new pool's origin):
// the bench it restores is the stored wall-clock end, not a deadline.
func TestPersistentProxyInventory_CooldownsUseWallTimeAfterAMonotonicClockReset(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	pool.Punish(t.Context(), "se", s.DEAD, "", 0)
	s.close(pool)
	s.clock.Advance(1800 * time.Second) // restarted process, thirty minutes later
	after := s.load(nil)
	if c := persistentProxyInventoryCooling(after.SiteHealth("se"))[s.DEAD]; c != 1800 {
		t.Errorf("cooling = %v, want 1800", c)
	}
	s.close(after)
	s.clock.Advance(1801 * time.Second)
	expired := s.load(nil)
	if c := persistentProxyInventoryCooling(expired.SiteHealth("se"))[s.DEAD]; c != 0 {
		t.Errorf("cooling = %v, want 0", c)
	}
	if n := expired.catalog.get(s.DEAD).sites["se"].challenges; n != 1 {
		t.Errorf("challenges = %d, want 1", n)
	}
}

func TestPersistentProxyInventory_DailyRefreshIsCappedAt24HoursAndSurvivesRestarts(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	cfg := persistentProxyInventoryReplace(t, s.cfg, withRefreshInterval(7*1440))
	pool := s.load(&cfg)
	s.close(pool)
	s.clock.Advance(12 * time.Hour)
	s.close(s.load(&cfg))
	if n := s.get.count(); n != 1 {
		t.Errorf("downloads = %d, want 1: a restart does not download a fresh list", n)
	}
	s.clock.Advance(12*time.Hour - time.Second)
	s.close(s.load(&cfg))
	if n := s.get.count(); n != 1 {
		t.Errorf("downloads = %d, want 1", n)
	}
	s.clock.Advance(time.Second)
	s.load(&cfg)
	if n := s.get.count(); n != 2 {
		t.Errorf("downloads = %d, want 2: enough routes still require a daily refresh", n)
	}
}

func TestPersistentProxyInventory_ShortageRefreshesOnlyTheAffectedCountryAndIsRateLimited(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	cfg := persistentProxyInventoryReplace(t, s.cfg, withEndpoints("direct"), withMinReady(2))
	s.setFeed("DE", []any{"http://de1.example:80", "http://de2.example:80"})
	pool := s.load(&cfg, "se", "de")
	pool.DemoteTransport(t.Context(), "se", s.GOOD, "", 0)
	pool.Punish(t.Context(), "se", s.DEAD, "", 0)
	s.editFeed("SE", func(feed []any) []any { return append(feed, "http://new.example:80") })
	s.get.reset()
	s.clock.Advance(59 * time.Second)
	s.refreshSources(pool, cfg, "se", "de")
	if n := s.get.count(); n != 0 {
		t.Errorf("%d downloads, want none: %v", n, s.get.urls())
	}
	s.clock.Advance(time.Second)
	s.refreshSources(pool, cfg, "se", "de")
	if got, want := s.get.urls(), []string{strings.ReplaceAll(s.URL, "{country}", "SE")}; !reflect.DeepEqual(got, want) {
		t.Errorf("feeds asked for = %v, want %v", got, want)
	}
	if _, ok := s.rows()["http://new.example:80"]; !ok {
		t.Errorf("http://new.example:80 not stored")
	}
	s.clock.Advance(60 * time.Second)
	s.refreshSources(pool, cfg, "se", "de")
	if n := s.get.count(); n != 1 {
		t.Errorf("downloads = %d, want 1: stop replenishing once enough routes are available", n)
	}
}

func TestPersistentProxyInventory_FailedFeedKeepsInventoryMembershipAndRetriesAfterRestart(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	first := *pool.sources[s.sourceKey()].FetchedAt
	s.close(pool)
	s.clock.Advance(31 * day)
	s.setFeed("SE", errors.New("GitHub unavailable")) // a download that timed out
	after := s.load(nil)
	if n := after.Len(); n != 3 {
		t.Errorf("size = %d, want 3: the last successful listing still protects all entries", n)
	}
	if got := after.sources[s.sourceKey()].FetchedAt; got == nil || *got != first {
		t.Errorf("fetched_at = %s, want %v", show(got), first)
	}
	s.close(after)
	s.close(s.load(nil))
	if n := s.get.count(); n != 2 {
		t.Errorf("downloads = %d, want 2: restart does not bypass the retry limit", n)
	}
	s.clock.Advance(60 * time.Second)
	s.setFeed("SE", []any{})
	empty := s.load(nil)
	if n := s.get.count(); n != 3 {
		t.Errorf("downloads = %d, want 3", n)
	}
	if n := empty.Len(); n != 0 {
		t.Errorf("size = %d, want 0: a confirmed empty list allows retirement", n)
	}
	if rows := s.rows(); len(rows) != 0 {
		t.Errorf("stored rows = %v, want none", persistentProxyInventoryKeys(rows))
	}
}

func TestPersistentProxyInventory_ListedProxiesAreKeptEvenWhenOldUnusedOrRepeatedlyFailing(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	for range 10 {
		pool.DemoteTransport(t.Context(), "se", s.DEAD, "", 0)
	}
	s.clock.Advance(90 * day)
	s.refreshSources(pool, s.cfg, "se")
	if got, want := persistentProxyInventoryKeys(s.rows()),
		persistentProxyInventorySorted(s.GOOD, s.UNUSED, s.DEAD); !reflect.DeepEqual(got, want) {
		t.Errorf("stored urls = %v, want %v", got, want)
	}
	if n := pool.catalog.get(s.DEAD).fails; n != 10 {
		t.Errorf("fails = %d, want 10", n)
	}
	if lt := pool.catalog.get(s.UNUSED).lastTested; lt != nil {
		t.Errorf("last_tested = %v, want nil", *lt)
	}
}

func TestPersistentProxyInventory_AbsentUnusedAndFailedProxiesExpireAt30DaysNotLastSeen(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	pool.ReportOK(t.Context(), "se", s.GOOD, time.Second)
	pool.DemoteTransport(t.Context(), "se", s.DEAD, "", 0)
	s.clock.Advance(29 * day)
	s.refreshSources(pool, s.cfg, "se") // last seen yesterday
	s.setFeed("SE", []any{})
	s.clock.Advance(day - time.Second)
	s.refreshSources(pool, persistentProxyInventoryReplace(t, s.cfg, withMinReady(10)), "se")
	if n := pool.Len(); n != 3 {
		t.Errorf("size = %d, want 3: just short of thirty days", n)
	}
	s.clock.Advance(time.Second)
	s.refreshSources(pool, s.cfg, "se")
	if rows := s.rows(); len(rows) != 0 {
		t.Errorf("stored rows = %v, want none: neither feed appearances nor failed tests reset the age",
			persistentProxyInventoryKeys(rows))
	}
	if n := pool.Len(); n != 0 {
		t.Errorf("size = %d, want 0", n)
	}
	s.close(pool)
	if n := s.load(nil).Len(); n != 0 {
		t.Errorf("size = %d, want 0: retired rows do not return on restart", n)
	}
}

func TestPersistentProxyInventory_ASuccessOnAnySiteExtendsRetentionWhenNoLongerListed(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	cfg := persistentProxyInventoryReplace(t, s.cfg, withEndpoints(s.GOOD)) // explicitly shared
	pool := s.load(&cfg)
	s.clock.Advance(29 * day)
	pool.ReportOK(t.Context(), "de", s.GOOD, time.Second)
	s.setFeed("SE", []any{})
	s.clock.Advance(day)
	s.refreshSources(pool, cfg, "se")
	if got, want := persistentProxyInventoryKeys(s.rows()), []string{s.GOOD}; !reflect.DeepEqual(got, want) {
		t.Errorf("stored urls = %v, want %v", got, want)
	}
	s.clock.Advance(29 * day)
	s.refreshSources(pool, cfg, "se")
	if rows := s.rows(); len(rows) != 0 {
		t.Errorf("stored rows = %v, want none", persistentProxyInventoryKeys(rows))
	}
	s.close(pool)
	if n := s.load(&cfg).Len(); n != 0 {
		t.Errorf("size = %d, want 0: unchanged static config cannot resurrect an expired proxy", n)
	}
}

func TestPersistentProxyInventory_ANewGithubListingCanReintroduceARetiredProxy(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)
	s.setFeed("SE", []any{})
	s.clock.Advance(31 * day)
	s.refreshSources(pool, s.cfg, "se")
	if n := pool.Len(); n != 0 {
		t.Errorf("size = %d, want 0", n)
	}
	s.setFeed("SE", []any{s.UNUSED})
	s.clock.Advance(60 * time.Second)
	s.refreshSources(pool, s.cfg, "se")
	if got, want := persistentProxyInventoryKeys(s.rows()), []string{s.UNUSED}; !reflect.DeepEqual(got, want) {
		t.Errorf("stored urls = %v, want %v", got, want)
	}
	e := pool.catalog.get(s.UNUSED)
	if e == nil {
		t.Fatalf("%s is not in the catalog", s.UNUSED)
	}
	if now := wallOf(s.clock); e.firstSeen != now {
		t.Errorf("first_seen = %v, want the clock's %v", e.firstSeen, now)
	}
}

func TestPersistentProxyInventory_ConcurrentOutcomesAndRefreshCommitWithoutLosingScores(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	pool := s.load(nil)

	record := func(url string) {
		for range 20 {
			pool.ReportOK(t.Context(), "se", url, 1500*time.Millisecond)
		}
	}

	// Three routes' outcomes and a feed refresh, all at once.
	var wg sync.WaitGroup
	var refreshErr error
	for _, u := range []string{s.GOOD, s.UNUSED, s.DEAD} {
		wg.Go(func() { record(u) })
	}
	wg.Go(func() { refreshErr = refreshSources(t.Context(), pool, &s.cfg, []string{"se"}) })
	wg.Wait()
	if refreshErr != nil {
		t.Fatalf("refreshSources: %v", refreshErr)
	}
	s.close(pool)
	after := s.load(nil)
	for _, e := range after.entries {
		if e.served != 20 || e.stat("se").tries != 20 {
			t.Errorf("%s: served %d, tries %d, want 20 and 20", e.url, e.served, e.stat("se").tries)
		}
	}
	if got := persistentProxyInventoryFirst(after)["se"].Attempts; got != 3 {
		t.Errorf("prior[se].Attempts = %d, want 3", got)
	}
}

// One tick of the server's timer (RefreshLoop runs one a minute) downloads
// the feed and stores the inventory; the scan after it reuses the pool and
// downloads nothing.
func TestPersistentProxyInventory_TimerPersistsInventoryBetweenScansAndScanReusesIt(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	app := *fixtureApp()
	app.Proxies = s.cfg
	app.Scopes = []testScope{testScope{Code: "se"}}
	live := newLive(t, WithClock(s.clock), s.get.option())
	live.refreshTick(t.Context(), func() (*Config, []string, error) { return &app.Proxies, app.Codes(), nil }, s.path)
	pool, err := live.Pool(t.Context(), &s.cfg, []string{"se"}, s.path)
	if err != nil {
		t.Fatalf("Live.Pool: %v", err)
	}
	if pool != live.Current() {
		t.Error("the scan did not get the pool the timer built")
	}
	if n := s.get.count(); n != 1 {
		t.Errorf("%d downloads, want 1", n)
	}
	if got, want := persistentProxyInventoryKeys(s.rows()),
		persistentProxyInventorySorted(s.GOOD, s.UNUSED, s.DEAD); !reflect.DeepEqual(got, want) {
		t.Errorf("stored urls = %v, want %v", got, want)
	}
}

func TestPersistentProxyInventory_APIOverrideBeforeDatabaseAttachmentRestoresOnlyItsRoutes(t *testing.T) {
	s := persistentProxyInventorySetUp(t)
	before := s.load(nil)
	before.ReportOK(t.Context(), "se", s.GOOD, 1500*time.Millisecond)
	s.close(before)
	live := newLive(t, WithClock(s.clock), s.get.option())
	if _, err := live.Override(t.Context(), []string{s.GOOD}, &s.cfg, []string{"se"}); err != nil {
		t.Fatalf("Live.Override: %v", err)
	}
	pool, err := live.Pool(t.Context(), &s.cfg, []string{"se"}, s.path)
	if err != nil {
		t.Fatalf("Live.Pool: %v", err)
	}
	entries := pool.entries
	var urls []string
	for _, e := range entries {
		urls = append(urls, e.url)
	}
	if !reflect.DeepEqual(urls, []string{s.GOOD}) {
		t.Fatalf("entries = %v, want [%s]", urls, s.GOOD)
	}
	if n := entries[0].served; n != 1 {
		t.Errorf("served = %d, want 1", n)
	}
	if got := pool.Acquire("de", true, nil, nil); got != s.GOOD {
		t.Errorf("acquire(de) = %q, want %q: an explicit override remains shared", got, s.GOOD)
	}
	if n := s.get.count(); n != 1 {
		t.Errorf("%d downloads, want 1", n)
	}
}
