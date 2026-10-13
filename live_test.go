package proxytron

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/JohanLindvall/proxytron/internal/logtest"
	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// samePath decides whether the live pool's database must be reopened: the
// same file however it is spelled (relative, absolute, ./, through a
// symbolic link, not created yet) is the same, another file is not.
func TestSamePath(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(root, "data")
	testutil.NoErr(t, os.MkdirAll(data, 0o755))
	testutil.NoErr(t, os.WriteFile(filepath.Join(data, "proxies.db"), nil, 0o644))
	if err := os.Symlink(data, filepath.Join(root, "link")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlinks unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Chdir(root)
	for _, tc := range []struct {
		a, b string
		same bool
	}{
		{"data/proxies.db", "data/proxies.db", true},
		{root + "/data/proxies.db", root + "/data/proxies.db", true},
		{"data/proxies.db", "./data/proxies.db", true},
		{"data/proxies.db", root + "/data/proxies.db", true},
		{"data/proxies.db", "link/proxies.db", true},
		{"data/new.sqlite3", "link/new.sqlite3", true}, // not created yet
		{"data/proxies.db", "data/other.sqlite3", false},
		{"data/proxies.db", "missing/proxies.db", false},
		{"/data/proxies.db", "/data/proxies.db", true},
	} {
		if got := samePath(tc.a, tc.b); got != tc.same {
			t.Errorf("samePath(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.same)
		}
	}
}

// Every [proxies] setting is part of the key, so an edit to any of them is
// a config change that ends an API override; the same config, and a
// marketplace list that expands {country} the same way, is the same key.
func TestKeyOf_EverySettingIsAChange(t *testing.T) {
	base := proxyCfg(t, withEndpoints("socks5h://a:1080"), withFeedURL("https://f/{country}.json"))
	codes := []string{"se", "de"}
	key := func(c *Config, codes []string) cfgKey {
		t.Helper()
		return testutil.Must(keyOf(c, codes))(t)
	}
	k := key(&base, codes)
	if k == (cfgKey{}) {
		t.Fatal("a config's key is the zero key, which stands for none")
	}
	same := base
	if key(&same, []string{"de", "SE", "de"}) != k {
		t.Error("the same config and countries gave another key")
	}
	for name, edit := range map[string]func(*Config){
		"revision":                   func(c *Config) { c.Revision = "changed" },
		"endpoints":                  func(c *Config) { c.Endpoints = []string{"socks5h://b:1080"} },
		"refresh_url":                func(c *Config) { c.FeedURL = "https://g/{country}.json" },
		"refresh_scheme":             func(c *Config) { c.DefaultScheme = "http" },
		"refresh_minutes":            func(c *Config) { c.RefreshInterval++ },
		"base_cooldown_seconds":      func(c *Config) { c.BaseCooldown++ },
		"challenge_cooldown_minutes": func(c *Config) { c.ChallengeCooldown++ },
		"broken_proxy_minutes":       func(c *Config) { c.BrokenCooldown++ },
		"dead_proxy_hours":           func(c *Config) { c.DeadCooldown++ },
		"selection":                  func(c *Config) { c.Selection = SelectionRoundRobin },
		"min_ready":                  func(c *Config) { c.MinReady++ },
	} {
		edited := base
		edited.Endpoints = append([]string(nil), base.Endpoints...)
		edit(&edited)
		if key(&edited, codes) == k {
			t.Errorf("an edit of %s keeps the key", name)
		}
	}
	if key(&base, []string{"se", "fr"}) == k {
		t.Error("another country's feed keeps the key")
	}
	if _, err := keyOf(&base, nil); !errors.Is(err, ErrNoScopes) {
		t.Errorf("keyOf without marketplaces = %v, want ErrNoScopes", err)
	}
}

// The live pool keeps its database while the path names the same file, and
// reopens from the other database when it names another.
func TestLive_AnotherDatabaseReopensThePool(t *testing.T) {
	dir := t.TempDir()
	live := newLive(t, WithClock(newClock()))
	cfg := proxyCfg(t, withEndpoints("socks5h://a:1080"))
	first := filepath.Join(dir, "first.sqlite3")
	pool := testutil.Must(live.Pool(t.Context(), &cfg, nil, first))(t)
	pool.ReportOK(t.Context(), "se", "socks5h://a:1080", 0)
	t.Chdir(dir)
	if again := testutil.Must(live.Pool(t.Context(), &cfg, nil, "./first.sqlite3"))(t); again != pool {
		t.Fatal("the same database by another spelling rebuilt the pool")
	}
	other := testutil.Must(live.Pool(t.Context(), &cfg, nil, filepath.Join(dir, "other.sqlite3")))(t)
	if other == pool || live.Current() != other {
		t.Fatal("another database kept the pool")
	}
	if got := other.storePath(); !samePath(got, filepath.Join(dir, "other.sqlite3")) {
		t.Errorf("attached to %s, want other.sqlite3", got)
	}
	if pool.storePath() != "" {
		t.Error("the first pool's database was left open")
	}
	if siteRows(other.SiteHealth("se"))["socks5h://a:1080"].Latency != nil {
		t.Error("the other database's pool carries the first one's measurements")
	}
	// A pool built without a database attaches one when a pass names it.
	bare := newLive(t)
	p := testutil.Must(bare.Pool(t.Context(), &cfg, nil, ""))(t)
	if testutil.Must(bare.Pool(t.Context(), &cfg, nil, first))(t) != p || p.storePath() == "" {
		t.Error("the database was not attached to the pool already built")
	}
}

// Close closes the live pool's database and drops the pool; a nil Live has
// nothing to close or show.
func TestLive_Close(t *testing.T) {
	live := testNewLive()
	cfg := proxyCfg(t, withEndpoints("socks5h://a:1080"))
	pool := testutil.Must(live.Pool(t.Context(), &cfg, nil, filepath.Join(t.TempDir(), "p.sqlite3")))(t)
	testutil.NoErr(t, live.Close())
	if pool.storePath() != "" || live.Current() != nil {
		t.Errorf("after Close: database %q, Current %p; want closed and nil", pool.storePath(), live.Current())
	}
	testutil.NoErr(t, live.Close())
	var none *Live
	if none.Current() != nil || none.Close() != nil {
		t.Error("a nil Live has a pool, or fails to close")
	}
}

// An override made without the config holds against the config the pool
// was last brought into line with: before any pass there is none, and the
// first pass reads the override as a config change. A config that cannot
// be keyed fails the override after the list has changed.
func TestLive_OverrideWithoutTheConfig(t *testing.T) {
	cfg := proxyCfg(t, withEndpoints("socks5h://a:1080"))
	urls := func(p *Pool) []string { return healthURLs(p.Health()) }
	t.Run("before any pass", func(t *testing.T) {
		live := newLive(t)
		testutil.Must(live.Override(t.Context(), []string{"b:1080", " ", "# comment"}, nil, nil))(t)
		if got := urls(live.Current()); !slices.Equal(got, []string{"socks5h://b:1080"}) {
			t.Errorf("override = %v, want [socks5h://b:1080]", got)
		}
		if got := urls(testutil.Must(live.Pool(t.Context(), &cfg, nil, ""))(t)); !slices.Equal(got, []string{"socks5h://a:1080"}) {
			t.Errorf("after the first pass = %v, want the config's [socks5h://a:1080]", got)
		}
	})
	t.Run("after a pass", func(t *testing.T) {
		live := newLive(t)
		testutil.Must(live.Pool(t.Context(), &cfg, nil, ""))(t)
		testutil.Must(live.Override(t.Context(), []string{"b:1080"}, nil, nil))(t)
		if got := urls(testutil.Must(live.Pool(t.Context(), &cfg, nil, ""))(t)); !slices.Equal(got, []string{"socks5h://b:1080"}) {
			t.Errorf("after the next pass = %v, want the override's [socks5h://b:1080]", got)
		}
		country := proxyCfg(t, withFeedURL("https://f/{country}.json"))
		if _, err := live.Override(t.Context(), []string{"c:1080"}, &country, nil); !errors.Is(err, ErrNoScopes) {
			t.Errorf("Override = %v, want ErrNoScopes", err)
		}
		if got := urls(live.Current()); !slices.Equal(got, []string{"socks5h://c:1080"}) {
			t.Errorf("after the failed override = %v, want [socks5h://c:1080]", got)
		}
	})
}

// Raw configured keys must survive reconciliation and overrides: normalizing a
// legacy credential or an explicit SOCKS5 URL would discard its measured history.
func TestLive_PreservesConfiguredKeys(t *testing.T) {
	want := []string{"http://usér:pässwörd@192.0.2.4:8080", "socks5://gateway.test:1080"}
	cfg := DefaultConfig()
	cfg.Endpoints = want
	live := newLive(t)
	p := testutil.Must(live.Pool(t.Context(), &cfg, nil, ""))(t)
	for _, endpoint := range want {
		p.ReportOK(t.Context(), "catalog", endpoint, 0)
	}
	p = testutil.Must(live.Override(t.Context(), want, &cfg, nil))(t)
	rows := siteRows(p.SiteHealth("catalog"))
	for _, endpoint := range want {
		if row, ok := rows[Redact(endpoint)]; p.find(endpoint) == nil || !ok || row.Served != 1 {
			t.Errorf("history lost for %q: %+v", endpoint, row)
		}
	}
}

// A pool that cannot be built is no pool: the error is returned and the
// next pass tries again.
func TestLive_AFailedBuildLeavesNoPool(t *testing.T) {
	live := newLive(t)
	cfg := proxyCfg(t, withFeedURL("https://f/{country}.json"))
	if p, err := live.Pool(t.Context(), &cfg, nil, ""); !errors.Is(err, ErrNoScopes) || p != nil {
		t.Errorf("Pool = %p, %v; want nil, ErrNoScopes", p, err)
	}
	if live.Current() != nil {
		t.Error("a failed build left a pool")
	}
}

// Once ctx ends a refresh commits nothing: the feed whose download it cut
// short is not recorded as attempted, let alone failed.
func TestRefreshSources_NothingIsCommittedOnceCtxEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	get := stubFeeds(func(string) (string, error) { cancel(); return "", context.Canceled })
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/data.json"))
	pool := testNew(WithClock(newClock()), get.option())
	if err := refreshSources(ctx, pool, &cfg, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("refreshSources = %v, want context.Canceled", err)
	}
	if get.count() != 1 {
		t.Fatalf("%d downloads, want 1", get.count())
	}
	if l := pool.sources[source{url: "https://feeds.test/data.json"}]; l != nil {
		t.Errorf("the cut-short feed was recorded: %+v", l)
	}
}

// A tick cut short by the end of its ctx has nothing to report.
func TestRefreshTick_AnEndedTickIsNotLogged(t *testing.T) {
	rec := logtest.Capture(t, slog.LevelInfo)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/data.json"))
	app := &testApp{Proxies: cfg}
	get := stubFeeds(feedText(`["http://a.example:80"]`))
	live := newLive(t, get.option())
	live.refreshTick(ctx, func() (*Config, []string, error) { return &app.Proxies, app.Codes(), nil }, "")
	if got := rec.Lines(); len(got) != 0 {
		t.Errorf("logged %q, want nothing", got)
	}
	if p := live.Current(); p != nil {
		t.Errorf("an ended tick built a pool of %d", p.Len())
	}
}

// A {country} feed is one source per marketplace, in code order: the code
// lowercased (strings.ToLower) and put in the URL uppercased
// (strings.ToUpper), the simple case mappings.
func TestSourceURLs_OnePerMarketplace(t *testing.T) {
	cfg := proxyCfg(t, withFeedURL("https://f/{country}.json"))
	got, err := sourceURLs(&cfg, []string{"\u00df", "\u0130t", "SE", "se"})
	if err != nil {
		t.Fatal(err)
	}
	want := []source{
		{"https://f/IT.json", "it"},
		{"https://f/SE.json", "se"},
		{"https://f/\u00df.json", "\u00df"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sourceURLs = %q, want %q", got, want)
	}
}

// A {country} feed with no code to expand it with is ErrNoScopes; a
// plain URL needs none.
func TestSourceURLs_CountryFeedWithoutScopes(t *testing.T) {
	cfg := proxyCfg(t, withFeedURL("https://f/{country}.json"))
	if got, err := sourceURLs(&cfg, nil); !errors.Is(err, ErrNoScopes) || got != nil {
		t.Errorf("sourceURLs = %q, %v; want ErrNoScopes", got, err)
	}
	plain := proxyCfg(t, withFeedURL("https://f/all.json"))
	if got, err := sourceURLs(&plain, nil); err != nil || !reflect.DeepEqual(got, []source{{"https://f/all.json", ""}}) {
		t.Errorf("a plain feed: %q, %v", got, err)
	}
}
