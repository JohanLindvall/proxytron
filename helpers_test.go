package proxytron

// Helpers the package's tests share beyond internal/testutil (the shipped
// watchlist and the fake clock): clock readings, a seeded draw, a feed
// download stub, ProxyCfg literals and a Live closed with the test. Other
// test files in this package reuse them.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/store"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// -- clocks -------------------------------------------------------------------

// newClock is a fake clock reading the real time now, so the wall-clock
// stamps a pool stores compare with the test's own. Hand it to a pool, or
// to a Live, with WithClock, and advance it by hand: a test can walk
// through hours of cooldowns at once.
func newClock() *testutil.Clock { return testutil.NewClock(time.Now()) }

// wallOf is c's reading in the Unix seconds a pool stores.
func wallOf(c *testutil.Clock) float64 { return float64(c.Now().UnixNano()) / 1e9 }

// withRand seeds the pool's Thompson draws.
func withRand(r *rand.Rand) Option { return func(p *Pool) { p.rng = r } }

// -- feeds --------------------------------------------------------------------

// feedStub answers feed downloads from memory, recording every URL asked
// for: a pool given its option (WithHTTPClient) downloads through it.
// Feeds are fetched on several goroutines, so it is locked.
type feedStub struct {
	mu      sync.Mutex
	calls   []string
	respond func(url string) (string, error)
}

// errNoFeed is what a stub with nothing to answer fails every download
// with.
var errNoFeed = errors.New("no feed")

// stubFeeds answers every download with respond(url): its text as a 200's
// body, or its error as a failed download. A nil respond fails them all.
func stubFeeds(respond func(url string) (string, error)) *feedStub {
	return &feedStub{respond: respond}
}

// feedText answers every URL with the same body.
func feedText(text string) func(string) (string, error) {
	return func(string) (string, error) { return text, nil }
}

// RoundTrip is the stub as an http.RoundTripper.
func (s *feedStub) RoundTrip(r *http.Request) (*http.Response, error) {
	url := r.URL.String()
	s.mu.Lock()
	s.calls = append(s.calls, url)
	respond := s.respond
	s.mu.Unlock()
	if respond == nil {
		return nil, errNoFeed
	}
	body, err := respond(url)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1,
		ProtoMinor: 1, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// client is an *http.Client whose every request the stub answers.
func (s *feedStub) client() *http.Client { return &http.Client{Transport: s} }

// option makes a pool, or every pool a Live builds, download through the
// stub.
func (s *feedStub) option() Option { return WithHTTPClient(s.client()) }

// count is how many downloads were asked for.
func (s *feedStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// urls is every URL asked for, in order.
func (s *feedStub) urls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// reset forgets the downloads asked for so far.
func (s *feedStub) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
}

// -- config ---------------------------------------------------------------------

// cfgOpt sets one field of a [proxies] config.
type cfgOpt func(*Config)

func withEndpoints(urls ...string) cfgOpt {
	return func(c *Config) { c.Endpoints = urls }
}

func withFeedURL(u string) cfgOpt { return func(c *Config) { c.FeedURL = u } }

func withRefreshInterval(n int) cfgOpt {
	return func(c *Config) { c.RefreshInterval = time.Duration(n) * time.Minute }
}

func withMinReady(n int) cfgOpt { return func(c *Config) { c.MinReady = n } }

// proxyCfg is a [proxies] config: the defaults, the options, then its
// validation.
func proxyCfg(t testing.TB, opts ...cfgOpt) Config {
	t.Helper()
	c := DefaultConfig()
	for _, o := range opts {
		o(&c)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("ProxyCfg: %v", err)
	}
	return c
}

// -- the live pool --------------------------------------------------------------

// newLive is a Live whose pools are built with opts, closed when the test
// ends.
func newLive(t testing.TB, opts ...Option) *Live {
	t.Helper()
	l := testNewLive(opts...)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// -- small conveniences ------------------------------------------------------------

// seeded is a generator seeded with seed, for reproducible draws.
func seeded(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) }

// siteRows is SiteHealth rows by URL.
func siteRows(rows []SiteHealth) map[string]SiteHealth {
	out := map[string]SiteHealth{}
	for _, r := range rows {
		out[r.URL] = r
	}
	return out
}

// healthRows is Health rows by URL.
func healthRows(rows []HealthRow) map[string]HealthRow {
	out := map[string]HealthRow{}
	for _, r := range rows {
		out[r.URL] = r
	}
	return out
}

func siteURLs(rows []SiteHealth) []string {
	out := []string{}
	for _, r := range rows {
		out = append(out, r.URL)
	}
	return out
}

func healthURLs(rows []HealthRow) []string {
	out := []string{}
	for _, r := range rows {
		out = append(out, r.URL)
	}
	return out
}

// count is how many times x occurs in xs.
func count(xs []string, x string) int {
	n := 0
	for _, s := range xs {
		if s == x {
			n++
		}
	}
	return n
}

func contains(xs []string, x string) bool { return count(xs, x) > 0 }

// acquireN is the routes n acquisitions on site hand out, in order.
func acquireN(pool *Pool, site string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = pool.Acquire(site, true, nil, nil)
	}
	return out
}

// set is a set of strings, as Acquire's exclude takes it.
func set(xs ...string) map[string]bool {
	out := map[string]bool{}
	for _, x := range xs {
		out[x] = true
	}
	return out
}

// repeat is xs n times over.
func repeat(xs []string, n int) []string {
	var out []string
	for range n {
		out = append(out, xs...)
	}
	return out
}

// show is an optional figure as a message prints it.
func show(p *float64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprint(*p)
}

// jsonValue is JSON text decoded for a by-value comparison: objects as
// maps, numbers as float64.
func jsonValue(t testing.TB, text []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(text, &v); err != nil {
		t.Fatalf("decoding %.200s: %v", text, err)
	}
	return v
}

// asJSON is v encoded, for a message or a golden file.
func asJSON(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

// storedRow is one row of the proxies table: the URL it is keyed by, and
// its record decoded for a by-value comparison.
type storedRow struct {
	URL    string         `json:"url"`
	Record map[string]any `json:"record"`
}

// storedProxies is every row of the proxies table in inventory (rowid)
// order, read over a separate raw connection (driver "sqlite", registered
// by internal/db).
func storedProxies(t testing.TB, path string) []storedRow {
	t.Helper()
	var inv store.Inventory
	var err error
	if cached, ok := testStores.Load(path); ok {
		inv, err = cached.(store.Store).LoadProxies(context.Background())
	} else {
		err = errors.New("no open store")
	}
	if err != nil {
		db := testutil.Must(store.Open(context.Background(), path))(t)
		defer db.Close()
		inv, err = db.LoadProxies(context.Background())
	}
	testutil.NoErr(t, err)
	var out []storedRow
	for _, raw := range inv.Rows {
		record := jsonValue(t, raw).(map[string]any)
		out = append(out, storedRow{record["url"].(string), record})
	}
	return out
}

// storedProxyRows is storedProxies by URL.
func storedProxyRows(t testing.TB, path string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, r := range storedProxies(t, path) {
		out[r.URL] = r.Record
	}
	return out
}

// object is v[key] as a JSON object (nil when it is none).
func object(v map[string]any, key string) map[string]any {
	o, _ := v[key].(map[string]any)
	return o
}

// -- settings ---------------------------------------------------------------------

// tuned changes the pool's default settings.
func tuned(change func(*Settings)) Option { return func(p *Pool) { change(&p.settings) } }

// baseCooldown is the refusal bench, secs seconds doubling.
func baseCooldown(secs float64) Option {
	return tuned(func(s *Settings) { s.BaseCooldown = durationOf(secs) })
}

// roundRobin selects in list order (selection = "round_robin").
func roundRobin() Option {
	return tuned(func(s *Settings) { s.Selection = SelectionRoundRobin })
}

var testStores sync.Map

func testStore(ctx context.Context, path string) (store.Store, error) {
	s, err := store.Open(ctx, path)
	if err == nil {
		testStores.Store(path, s)
	}
	return s, err
}
func testOptions(opts []Option) []Option {
	return append([]Option{WithStoreOpener(testStore), WithLegacyImport(true)}, opts...)
}
func testNew(opts ...Option) *Pool                     { return New(testOptions(opts)...) }
func testFromURLs(urls []string, opts ...Option) *Pool { return FromURLs(urls, testOptions(opts)...) }
func testLoadPool(ctx context.Context, cfg *Config, scopes []string, path string, opts ...Option) (*Pool, error) {
	return LoadPool(ctx, cfg, scopes, path, testOptions(opts)...)
}
func testNewLive(opts ...Option) *Live { return NewLive(testOptions(opts)...) }

type testScope struct{ Code string }
type testApp struct {
	Proxies Config
	Scopes  []testScope
}

func (a *testApp) Codes() []string {
	var out []string
	for _, s := range a.Scopes {
		out = append(out, s.Code)
	}
	return out
}
func fixtureApp() *testApp {
	c := DefaultConfig()
	c.FeedURL = countryFeed
	c.Scopes = append([]string(nil), shippedCodes...)
	app := &testApp{Proxies: c}
	for _, code := range shippedCodes {
		app.Scopes = append(app.Scopes, testScope{code})
	}
	return app
}

type fixtureStore struct{ inv store.Inventory }

func (s fixtureStore) LoadProxies(context.Context) (store.Inventory, error)           { return s.inv, nil }
func (s fixtureStore) SaveProxy(context.Context, store.Record, json.RawMessage) error { return nil }
func (s fixtureStore) SaveProxies(context.Context, []store.Record, []store.Source, json.RawMessage, map[string]float64) error {
	return nil
}
func (s fixtureStore) Path() string { return "" }
func (s fixtureStore) Close() error { return nil }
