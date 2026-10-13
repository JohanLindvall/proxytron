package proxytron

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/logtest"
	"github.com/JohanLindvall/proxytron/internal/testutil"
	"github.com/JohanLindvall/proxytron/store"
)

// snapshotNow is when testdata/inventory.json was taken (Unix seconds): the
// dead entry's day and the WAF bench in it are still running then.
const snapshotNow = 1_791_631_900

// countryFeed is the shipped config's refresh_url.
const countryFeed = "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/countries/{country}/data.json"

// shippedCodes is the shipped config's marketplaces.
var shippedCodes = []string{"se", "de", "be", "fr", "it"}

// snapshotPool is an empty pool whose clock reads snapshotNow.
func snapshotPool() *Pool {
	return testNew(WithClock(testutil.NewClock(time.Unix(snapshotNow, 0))))
}

// inventoryFixture is testdata/inventory.json: stored documents as the
// service wrote them.
type inventoryFixture struct {
	Rows []struct {
		URL  string `json:"url"`
		Data string `json:"data"`
	} `json:"rows"`
	Sources []struct {
		URL   string `json:"url"`
		Scope string `json:"marketplace"`
		Data  string `json:"data"`
	} `json:"sources"`
	First string `json:"first"`
}

// writeInventory stores the fixture in a fresh database at path, as the
// service's last checkpoint left it.
func writeInventory(t *testing.T, path string, fx inventoryFixture) {
	t.Helper()
	database := testutil.Must(store.Open(t.Context(), path))(t)
	var rows []store.Record
	for _, r := range fx.Rows {
		rows = append(rows, store.Record{URL: r.URL, Data: json.RawMessage(r.Data)})
	}
	var sources []store.Source
	for _, s := range fx.Sources {
		sources = append(sources, store.Source{URL: s.URL, Scope: s.Scope, Data: json.RawMessage(s.Data)})
	}
	testutil.NoErr(t, database.SaveProxies(t.Context(), rows, sources, json.RawMessage(fx.First), nil))
	testutil.NoErr(t, database.Close())
}

// withoutScore is a stored record with its score dropped: selection
// recomputes the score from the learned prior, which moves on after a row
// is written.
func withoutScore(record map[string]any) map[string]any {
	for _, s := range object(record, "sites") {
		delete(s.(map[string]any), "score")
	}
	return record
}

// Production rows restore to the state they were written from: a restart
// reads back every count, bench, record, origin and feed record, writes
// the same values when it next saves them, and keeps the inventory's
// order, which is the order untried routes are explored in.
func TestInventory_ProductionRowsSurviveARestart(t *testing.T) {
	var fx inventoryFixture
	testutil.NoErr(t, json.Unmarshal(testutil.Must(os.ReadFile(filepath.Join("testdata", "inventory.json")))(t), &fx))
	path := filepath.Join(t.TempDir(), "amzscrape.sqlite3")
	writeInventory(t, path, fx)
	cfg := proxyCfg(t, withFeedURL(countryFeed))
	pool := snapshotPool()
	testutil.NoErr(t, pool.attach(t.Context(), path, &cfg, shippedCodes))
	t.Cleanup(func() { _ = pool.Close() })

	stored := storedProxies(t, path)
	if len(stored) != len(fx.Rows) {
		t.Fatalf("%d rows stored, want %d", len(stored), len(fx.Rows))
	}
	for i, r := range fx.Rows {
		want := withoutScore(jsonValue(t, []byte(r.Data)).(map[string]any))
		if got := stored[i]; got.URL != r.URL || !reflect.DeepEqual(withoutScore(got.Record), want) {
			t.Errorf("row %d: stored %s %v\nwant %s %v", i+1, got.URL, got.Record, r.URL, want)
		}
	}
	first, err := json.Marshal(pool.first)
	testutil.NoErr(t, err)
	if got, want := jsonValue(t, first), jsonValue(t, []byte(fx.First)); !reflect.DeepEqual(got, want) {
		t.Errorf("prior = %s, want %s", first, fx.First)
	}
	for _, s := range fx.Sources {
		var want listing
		testutil.NoErr(t, json.Unmarshal([]byte(s.Data), &want))
		if got := pool.sources[source{url: s.URL, site: s.Scope}]; got == nil || !reflect.DeepEqual(*got, want) {
			t.Errorf("listing %s: %+v, want %+v", s.Scope, got, want)
		}
	}

	// What the restored routes report, once handed out.
	pool.mu.Lock()
	pool.entries = pool.catalog.values()
	pool.mu.Unlock()
	got := asJSON(t, map[string]any{"be": pool.SiteHealth("be"), "de": pool.SiteHealth("de"), "all": pool.Health()})
	golden := filepath.Join("testdata", "inventory.golden.json")
	testutil.WriteGolden(t, golden, got)
	if want := testutil.ReadGolden(t, golden); !reflect.DeepEqual(jsonValue(t, got), jsonValue(t, want)) {
		t.Errorf("health:\n got %s\nwant %s", got, want)
	}
}

// A stored document that does not decode fails the attach, which leaves
// the pool as it was and no database attached; what a record lacks, or
// holds besides, is no error.
func TestInventory_ACorruptDocumentFailsTheAttach(t *testing.T) {
	const good = `{"url": "http://a:80", "served": 1}`
	for _, c := range []struct {
		name   string
		row    string
		listed string // a proxy_sources document
		first  string
		ok     bool
	}{
		{name: "a minimal record", row: good, ok: true},
		{name: "a listing without members", row: good, listed: `{"fetched_at": 1791631346.4}`, ok: true},
		{name: "a null prior", row: good, first: `null`, ok: true},
		{name: "keys no writer used", row: `{"url": "http://a:80", "cooldown_until": 5, "extra": [1]}`, ok: true},
		{name: "null bench and sites", row: `{"url": "http://a:80", "until": null, "sites": null}`, ok: true},
		{name: "not an object", row: `[1]`},
		{name: "no url", row: `{"served": 1}`},
		{name: "a counter that is a string", row: `{"url": "http://a:80", "served": "1"}`},
		{name: "a count with a fraction", row: `{"url": "http://a:80", "fails": 1.5}`},
		{name: "a bench that is a string", row: `{"url": "http://a:80", "until": "soon"}`},
		{name: "sites that are a list", row: `{"url": "http://a:80", "sites": ["se"]}`},
		{name: "a site record that is a number", row: `{"url": "http://a:80", "sites": {"se": 5}}`},
		{name: "marketplaces that are a number", row: `{"url": "http://a:80", "marketplaces": 5}`},
		{name: "a NaN token", row: `{"url": "http://a:80", "transport_failure": NaN}`},
		{name: "not JSON", row: `{"url": `},
		{name: "a listing that is a list", row: good, listed: `[]`},
		{name: "a prior that is a list", row: good, first: `[]`},
		{name: "a prior of three values", row: good, first: `{"se": [1, 0.5, 2]}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var sources []store.Source
			if c.listed != "" {
				sources = append(sources, store.Source{URL: "https://f/SE.json", Scope: "se", Data: json.RawMessage(c.listed)})
			}
			first := c.first
			if first == "" {
				first = "{}"
			}
			stored := store.Inventory{Rows: []json.RawMessage{json.RawMessage(c.row)}, Sources: sources, First: json.RawMessage(first), Imported: true}

			cfg := proxyCfg(t, withEndpoints("http://b:80"))
			pool := testFromURLs([]string{"http://b:80"}, WithClock(newClock()))
			err := pool.loadInventory(t.Context(), fixtureStore{inv: stored}, &cfg, nil)
			t.Cleanup(func() { _ = pool.Close() })
			if c.ok {
				if err != nil {
					t.Fatalf("attach: %v", err)
				}
				if e := pool.catalog.get("http://a:80"); e == nil {
					t.Error("the stored route was not restored")
				}
				return
			}
			if err == nil {
				t.Fatal("attach succeeded")
			}
			if p := pool.storePath(); p != "" {
				t.Errorf("a database stays attached: %s", p)
			}
			if got := pool.catalog.keys; !slices.Equal(got, []string{"http://b:80"}) {
				t.Errorf("catalog = %v, want the pool's own route alone", got)
			}
		})
	}
}

// The legacy proxy-memory.json, in each shape it was written in, imports
// into a fresh database once: the counts, the benches still running and
// the records, with the feeds the config downloads as the origins. A
// bench that has run out leaves no trace, not even an empty site record.
//
//   - benches: the first shape (21f1b20, and 8d01a69, which only wrote it
//     more often): which routes have served, and the benches;
//   - records: the last (f3d7b3c), which adds each route's record per site
//     and a row of the sites' first-attempt priors;
//   - repeats: a route listed twice merges (the larger count, the later
//     bench), a record is not overwritten, the first prior row wins.
func TestLegacyImport_EveryShape(t *testing.T) {
	for _, name := range []string{"benches", "records", "repeats"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			memory := testutil.Must(os.ReadFile(filepath.Join("testdata", "legacy", name+".json")))(t)
			testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), memory, 0o644))
			path := filepath.Join(dir, "amzscrape.sqlite3")
			cfg := proxyCfg(t, withFeedURL(countryFeed))
			pool := snapshotPool()
			testutil.NoErr(t, pool.attach(t.Context(), path, &cfg, shippedCodes))
			testutil.NoErr(t, pool.Close())

			first, err := json.Marshal(pool.first)
			testutil.NoErr(t, err)
			got := asJSON(t, map[string]any{"rows": storedProxies(t, path), "first": json.RawMessage(first)})
			golden := filepath.Join("testdata", "legacy", name+".golden.json")
			testutil.WriteGolden(t, golden, got)
			if want := testutil.ReadGolden(t, golden); !reflect.DeepEqual(importedState(t, got), importedState(t, want)) {
				t.Errorf("imported:\n got %s\nwant %s", got, want)
			}

			// Once: the next start reads the database, not the file.
			testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(`[{"url": "http://new:80"}]`), 0o644))
			again := snapshotPool()
			testutil.NoErr(t, again.attach(t.Context(), path, &cfg, shippedCodes))
			t.Cleanup(func() { _ = again.Close() })
			if again.catalog.get("http://new:80") != nil {
				t.Error("the file was imported a second time")
			}
		})
	}
}

// importedState is a legacy-import golden decoded for comparison, every
// score dropped: the score is stored for forensic queries and recomputed,
// never read, and its last digit follows whether the build fuses
// multiply-adds (GOAMD64=v3) or not.
func importedState(t *testing.T, golden []byte) any {
	t.Helper()
	v := jsonValue(t, golden).(map[string]any)
	for _, row := range v["rows"].([]any) {
		withoutScore(object(row.(map[string]any), "record"))
	}
	return v
}

// A first-attempt prior the file holds in another shape, null included, is
// skipped, so a later prior row can still supply that site; the rest of the
// file still imports, and of two good priors for a site the first wins.
func TestLegacyImport_APriorOfAnotherShapeIsSkipped(t *testing.T) {
	dir := t.TempDir()
	memory := `[{"url": "http://a:80", "served": 1},
		{"first": {"se": [1, 1.0, "x", 1], "it": [1, 1.0], "de": [2, 1.0, 3.5, 2], "fr": null}},
		{"first": {"fr": [4, 2.0, 8.0, 4], "de": [9, 9.0, 9.0, 9]}}]`
	testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(memory), 0o644))
	cfg := proxyCfg(t, withFeedURL(countryFeed))
	pool := snapshotPool()
	testutil.NoErr(t, pool.attach(t.Context(), filepath.Join(dir, "amzscrape.sqlite3"), &cfg, shippedCodes))
	t.Cleanup(func() { _ = pool.Close() })
	if want := map[string]firstStat{"de": {2, 1, 3.5, 2}, "fr": {4, 2, 8, 4}}; !reflect.DeepEqual(pool.first, want) {
		t.Errorf("prior = %+v, want %+v", pool.first, want)
	}
	if e := pool.catalog.get("http://a:80"); e == nil || e.served != 1 {
		t.Errorf("the route was not imported: %+v", e)
	}
}

// What the legacy routes are matched against has to exist: a {country}
// feed with no marketplace to expand it with fails the attach, as it fails
// the refresh after it.
func TestLegacyImport_ACountryFeedWithoutScopesFailsTheAttach(t *testing.T) {
	dir := t.TempDir()
	testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(`[{"url": "http://a:80"}]`), 0o644))
	cfg := proxyCfg(t, withFeedURL(countryFeed))
	pool := snapshotPool()
	if err := pool.attach(t.Context(), filepath.Join(dir, "amzscrape.sqlite3"), &cfg, nil); !errors.Is(err, ErrNoScopes) {
		t.Errorf("attach = %v, want ErrNoScopes", err)
	}
	if p := pool.storePath(); p != "" {
		t.Errorf("a database stays attached: %s", p)
	}
}

// A database that cannot be opened fails the attach and attaches nothing.
func TestAttach_ADatabaseThatCannotBeOpened(t *testing.T) {
	file := filepath.Join(t.TempDir(), "a-file")
	testutil.NoErr(t, os.WriteFile(file, nil, 0o644))
	pool := snapshotPool()
	cfg := proxyCfg(t)
	if err := pool.attach(t.Context(), filepath.Join(file, "amzscrape.sqlite3"), &cfg, nil); err == nil {
		t.Error("attach under a file succeeded")
	}
	if p := pool.storePath(); p != "" {
		t.Errorf("a database is attached: %s", p)
	}
}

// A static endpoint the file names is the operator's route, not a feed's:
// its counts and benches come back, but it is credited to no feed.
func TestLegacyImport_AStaticEndpointIsNoFeeds(t *testing.T) {
	const tunnel = "socks5h://192.168.1.4:1080"
	dir := t.TempDir()
	memory := `[{"url": "` + tunnel + `", "served": 7, "fails": 2, "until": 0, "sites": {"se": {"ok": 1.0, "bad": 0.0, "tries": 1, "duration": 0.5}}}]`
	testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(memory), 0o644))
	cfg := proxyCfg(t, withEndpoints("192.168.1.4:1080"), withFeedURL(countryFeed))
	pool := snapshotPool()
	testutil.NoErr(t, pool.attach(t.Context(), filepath.Join(dir, "amzscrape.sqlite3"), &cfg, shippedCodes))
	t.Cleanup(func() { _ = pool.Close() })
	e := pool.catalog.get(tunnel)
	if e == nil {
		t.Fatal("the tunnel was not imported")
	}
	if e.fromFeed || len(e.origins) != 0 {
		t.Errorf("fromFeed %v, origins %v: want no feed's", e.fromFeed, e.origins)
	}
	if e.served != 7 || e.fails != 2 || e.lastSuccess == nil || e.sites["se"].tries != 1 {
		t.Errorf("served %d, fails %d, last success %s, tries %d; want 7, 2, now, 1",
			e.served, e.fails, show(e.lastSuccess), e.sites["se"].tries)
	}
}

// A legacy file that is not a list of routes is unreadable: a fresh start
// with a warning, never a failed attach.
func TestLegacyImport_AnUnreadableFileIsAFreshStart(t *testing.T) {
	for _, text := range []string{`{not json`, `{"url": "http://a:80"}`, `[1, 2]`, `[{"url": "http://a:80", "served": "1"}]`,
		strings.Repeat("[", 20000) + strings.Repeat("]", 20000)} {
		dir := t.TempDir()
		testutil.NoErr(t, os.WriteFile(filepath.Join(dir, legacyMemory), []byte(text), 0o644))
		rec := logtest.Capture(t, slog.LevelWarn)
		pool := snapshotPool()
		cfg := proxyCfg(t, withFeedURL(countryFeed))
		if err := pool.attach(t.Context(), filepath.Join(dir, "amzscrape.sqlite3"), &cfg, shippedCodes); err != nil {
			t.Errorf("%.20s: attach = %v", text, err)
		}
		if got := rec.Messages("amzscrape.proxy"); len(got) != 1 || !strings.HasPrefix(got[0], "legacy proxy memory unreadable: ") {
			t.Errorf("%.20s: logged %q", text, got)
		}
		if n := len(pool.catalog.keys); n != 0 {
			t.Errorf("%.20s: %d routes imported", text, n)
		}
		_ = pool.Close()
	}
}

// Retiring a route forgets it everywhere: the pool's routes, the catalog,
// any listing that still names it, and the rotation's place; only a marker
// of its fingerprint remains, so an old static config cannot bring it back.
func TestRetire_ForgetsEveryTraceOfARoute(t *testing.T) {
	const a, gone = "http://a:80", "https://old.example/feed.json"
	clock := newClock()
	pool := testFromURLs([]string{a}, WithClock(clock))
	if got := pool.Acquire("se", true, nil, nil); got != a {
		t.Fatalf("acquire = %q", got)
	}
	// A listing of a feed no longer configured, which retirement does not
	// count as current.
	pool.sources[source{url: gone}] = &listing{Members: map[string]json.RawMessage{a: json.RawMessage(`{}`)}}
	clock.Advance(retireAfter + time.Second)
	if n := pool.retire(t.Context(), nil); n != 1 {
		t.Fatalf("retired %d, want 1", n)
	}
	if pool.Len() != 0 || pool.catalog.get(a) != nil || len(pool.sources[source{url: gone}].Members) != 0 {
		t.Errorf("a trace remains: %d routes, catalog %v, listing %v",
			pool.Len(), pool.catalog.keys, pool.sources[source{url: gone}].Members)
	}
	if _, ok := pool.lastUsed["se"]; ok {
		t.Error("the rotation still continues from the retired route")
	}
	if _, ok := pool.retired[fingerprint(a)]; !ok {
		t.Error("no retirement marker")
	}
}

// failingStore is an inventory store whose every write and Close fail.
type failingStore struct{ err error }

func (f failingStore) LoadProxies(context.Context) (store.Inventory, error) {
	return store.Inventory{}, f.err
}

func (f failingStore) SaveProxy(context.Context, store.Record, json.RawMessage) error { return f.err }

func (f failingStore) SaveProxies(context.Context, []store.Record, []store.Source, json.RawMessage,
	map[string]float64) error {
	return f.err
}

func (failingStore) Path() string { return "/data/amzscrape.sqlite3" }

func (f failingStore) Close() error { return f.err }

var _ store.Store = failingStore{}

// A save that fails is logged and costs nothing else: an outcome still
// counts, and a retirement still happens, though it reports none saved; a
// database that fails to close says so.
func TestPersist_AFailedSaveIsLogged(t *testing.T) {
	const a = "http://a:80"
	errDisk := errors.New("disk I/O error")
	clock := newClock()
	pool := testFromURLs([]string{a}, WithClock(clock))
	pool.store = failingStore{errDisk}
	rec := logtest.Capture(t, slog.LevelWarn)
	pool.log = slog.Default()

	pool.ReportOK(t.Context(), "se", a, time.Second)
	if e := pool.find(a); e.served != 1 {
		t.Errorf("served = %d, want 1: the outcome counts though its save failed", e.served)
	}
	clock.Advance(retireAfter + time.Second)
	if n := pool.retire(t.Context(), nil); n != 0 || pool.Len() != 0 {
		t.Errorf("retire = %d with %d routes left, want 0 (not saved) with the route gone", n, pool.Len())
	}
	closeLogged(t.Context(), pool)
	closeLogged(t.Context(), failingStore{errDisk})
	want := []string{
		"saving proxy state failed: disk I/O error",
		"saving proxy state failed: disk I/O error",
		"closing the proxy database failed: disk I/O error",
		"closing the proxy database failed: disk I/O error",
	}
	if got := rec.Lines(); !slices.Equal(got, want) {
		t.Errorf("logged\n%q\nwant\n%q", got, want)
	}
	if pool.storePath() != "" {
		t.Error("a failed Close left the database attached")
	}
}

// A store that cannot even list its inventory fails the attach that
// restores from it.
func TestPersist_AStoreThatCannotLoadFailsTheRestore(t *testing.T) {
	errDisk := errors.New("disk I/O error")
	pool := testFromURLs([]string{"http://a:80"}, WithClock(newClock()))
	cfg := proxyCfg(t)
	pool.mu.Lock()
	err := pool.loadInventory(t.Context(), failingStore{errDisk}, &cfg, nil)
	pool.mu.Unlock()
	if !errors.Is(err, errDisk) {
		t.Errorf("loadInventory = %v, want %v", err, errDisk)
	}
}
