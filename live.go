package proxytron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LoadPool builds a pool from the database first (database "" = none),
// replenishing or refreshing feeds if due. Its settings are cfg's
// (SettingsFrom); opts may give it a Clock and the HTTP client its feeds
// download through.
//
// Returns nil when nothing is configured. An enabled but empty pool is kept
// so a failed source can be retried on schedule. Static endpoints, if any,
// remain usable when a source fails.
func LoadPool(ctx context.Context, cfg *Config, marketplaces []string, database string,
	opts ...Option) (*Pool, error) {
	if cfg == nil || !cfg.Enabled() {
		return nil, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	pool := New(append([]Option{WithSettings(SettingsFrom(cfg))}, opts...)...)
	if err := pool.proxyType.Validate(); err != nil {
		_ = pool.Close()
		return nil, err
	}
	if database != "" {
		if err := pool.attach(ctx, database, cfg, marketplaces); err != nil {
			_ = pool.Close()
			return nil, err
		}
	}
	if err := refreshSources(ctx, pool, cfg, marketplaces); err != nil {
		_ = pool.Close()
		return nil, err
	}
	suffix := ""
	if cfg.FeedURL != "" {
		suffix = " (+refresh from " + Redact(cfg.FeedURL) + ")"
	}
	pool.log.InfoContext(ctx, fmt.Sprintf("proxy pool: %d endpoint(s)%s", pool.Len(), suffix))
	return pool, nil
}

// Live is the process's proxy pool, kept from one pass to the next:
// rebuilding it per pass would discard cooldowns, rotation positions and
// measured health. cli owns one, hands it to every pass and to the server,
// and closes it on the way out.
//
// Pool brings it into line with the config at the start of every pass, and
// RefreshLoop between passes; a config change goes through a refresh, which
// keeps the history of the endpoints that survive it. Override replaces the
// list from the API until the config changes. Current reads it.
//
// Pool, Override and Close take turns, and Pool keeps its turn across the
// feed downloads it makes, so a refresh in progress finishes before the
// next starts. Current never waits. The Live's lock is taken before the
// pool's, never after.
type Live struct {
	opts []Option

	mu       sync.Mutex
	key      cfgKey // the config the pool was last brought into line with
	override bool   // an API override holds until that config changes
	current  atomic.Pointer[Pool]
}

// NewLive returns a Live with no pool yet. opts are the options every pool
// it builds is given: WithClock, WithHTTPClient.
func NewLive(opts ...Option) *Live { return &Live{opts: opts} }

// Pool is the live pool brought into line with cfg (nil when [proxies] is
// not enabled, which closes and drops any pool there was), with database
// ("" = none) attached.
//
// Country URL templates use uppercase scope codes, and each source
// keeps its last good list if a later download fails. Health survives list
// updates. A different database path than the attached one reopens the
// pool from that database; feeds are downloaded only when due. Once ctx has
// ended nothing is built or refreshed, and ctx's error is returned.
func (l *Live) Pool(ctx context.Context, cfg *Config, marketplaces []string, database string) (*Pool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pool := l.current.Load()
	if cfg == nil || !cfg.Enabled() {
		if pool != nil {
			closeLogged(ctx, pool)
		}
		l.current.Store(nil)
		l.key, l.override = cfgKey{}, false
		return nil, nil
	}
	marketplaces = slices.Clone(marketplaces)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	key, err := keyOf(cfg, marketplaces)
	if err != nil {
		return nil, err
	}
	if pool != nil && database != "" {
		if stored := pool.storePath(); stored != "" && !samePath(stored, database) {
			closeLogged(ctx, pool)
			l.current.Store(nil)
			pool = nil
		}
	}
	if pool == nil {
		if pool, err = LoadPool(ctx, cfg, marketplaces, database, l.opts...); err != nil {
			return nil, err
		}
		l.current.Store(pool)
		l.key, l.override = key, false
		return pool, nil
	}
	if database != "" && pool.storePath() == "" {
		if err := pool.attach(ctx, database, cfg, marketplaces); err != nil {
			return nil, err
		}
	}
	if l.override && key == l.key {
		// The API override holds: the endpoints stay as they were set, and
		// only aged-out inventory is retired.
		sources, err := sourceURLs(cfg, marketplaces)
		if err != nil {
			return nil, err
		}
		pool.retire(ctx, sources)
		return pool, nil
	}
	// Due feeds follow their stored download times, not the process's age.
	if err := refreshSources(ctx, pool, cfg, marketplaces); err != nil {
		return nil, err
	}
	l.key, l.override = key, false
	return pool, nil
}

// Override replaces the live pool's endpoints with urls right now, from
// outside a pass, and returns the pool. This is what backs
// POST /api/proxies: the list changes mid-flight without a restart, and
// anything still on it keeps its health, because the pool is refreshed
// rather than rebuilt.
//
// cfg is the config in force, so that the override sticks: Pool reconciles
// with the config at the start of every pass, and without knowing that cfg
// is what the override was made under, the very next pass would read the
// override as a config change and undo it. With it, the override holds
// until someone actually edits [proxies], and that edit wins. cfg may be
// nil.
// Explicit URL spelling is preserved, as in Config.Endpoints.
func (l *Live) Override(ctx context.Context, urls []string, cfg *Config, marketplaces []string) (*Pool, error) {
	var cleaned []string
	for _, x := range urls {
		if u := normalize(x, defaultScheme); u != "" {
			cleaned = append(cleaned, u)
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	pool := l.current.Load()
	if pool == nil {
		pool = FromURLs(cleaned, l.opts...)
		l.current.Store(pool)
	} else {
		pool.Refresh(ctx, cleaned, nil, nil)
	}
	if cfg != nil {
		key, err := keyOf(cfg, marketplaces)
		if err != nil {
			return nil, err
		}
		l.key = key
	}
	l.override = true
	return pool, nil
}

// Current is the live pool as it stands, nil before the first Pool or
// Override (and for a nil Live). It never waits for a refresh in progress:
// it is what the status endpoints read.
func (l *Live) Current() *Pool {
	if l == nil {
		return nil
	}
	return l.current.Load()
}

// Close closes the live pool's database, if one is attached, and drops the
// pool; a later Pool builds a new one. It waits for a refresh in progress.
// Closing a nil Live does nothing.
func (l *Live) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.key, l.override = cfgKey{}, false
	if pool := l.current.Swap(nil); pool != nil {
		return pool.Close()
	}
	return nil
}

// refreshEvery is how often RefreshLoop brings the pool into line.
const refreshEvery = time.Minute

// RefreshLoop brings the pool into line once a minute until ctx ends, so
// feeds refresh even between scans: each tick re-reads the config with
// loadCfg, so an edit applies without a restart, and downloads only the
// feeds that are due. database "" is none. A tick that fails, or panics, is
// logged and the next one runs.
func (l *Live) RefreshLoop(ctx context.Context, loadCfg func() (*Config, []string, error), database string, logs ...*slog.Logger) {
	ticker := time.NewTicker(refreshEvery)
	defer ticker.Stop()
	for ctx.Err() == nil {
		l.refreshTick(ctx, loadCfg, database, logs...)
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

// refreshFailed starts the line a failed refresh tick logs; the error
// follows it.
const refreshFailed = "proxy refresh failed; will retry on the next timer tick: "

func (l *Live) refreshTick(ctx context.Context, loadCfg func() (*Config, []string, error), database string, logs ...*slog.Logger) {
	log := slog.Default()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContext(ctx, fmt.Sprintf("%s%v", refreshFailed, r), "stack", string(debug.Stack()))
		}
	}()
	cfg, scopes, err := loadCfg()
	if err == nil {
		_, err = l.Pool(ctx, cfg, scopes, database)
	}
	// A tick cut short by the end of ctx failed nothing worth a line.
	if err != nil && ctx.Err() == nil {
		log.ErrorContext(ctx, refreshFailed+err.Error())
	}
}

// samePath reports whether the database paths a and b name the same file:
// the same text, or the same absolute path once symbolic links are
// resolved.
func samePath(a, b string) bool {
	return a == b || canonicalPath(a) == canonicalPath(b)
}

// canonicalPath is p absolute with its symbolic links resolved. A file that
// does not exist yet resolves through its directory; what cannot be
// resolved at all stays as filepath.Abs leaves it.
func canonicalPath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved
	}
	if dir, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
		return filepath.Join(dir, filepath.Base(abs))
	}
	return abs
}

// cfgKey is what Live.Pool compares to tell that [proxies] has actually
// changed: the normalised static endpoints, the expanded feed sources and
// every setting. Comparable with ==; the zero cfgKey is no config.
type cfgKey struct {
	set               bool // false only for the zero cfgKey
	revision          string
	static            string
	refreshURL        string
	refreshScheme     string
	sources           string
	refreshMinutes    time.Duration
	baseCooldown      float64
	challengeCooldown float64
	brokenProxy       float64
	deadProxy         float64
	selection         string
	minReady          int
}

// keyOf is cfg's cfgKey for these scopes.
func keyOf(cfg *Config, marketplaces []string) (cfgKey, error) {
	sources, err := sourceURLs(cfg, marketplaces)
	if err != nil {
		return cfgKey{}, err
	}
	var src []string
	for _, s := range sources {
		src = append(src, s.url+"\x01"+s.site)
	}
	return cfgKey{
		set:               true,
		revision:          cfg.Revision,
		static:            strings.Join(staticEndpoints(cfg), "\x00"),
		refreshURL:        cfg.FeedURL,
		refreshScheme:     cfg.DefaultScheme,
		sources:           strings.Join(src, "\x00"),
		refreshMinutes:    cfg.RefreshInterval,
		baseCooldown:      secondsOf(cfg.BaseCooldown),
		challengeCooldown: secondsOf(cfg.ChallengeCooldown),
		brokenProxy:       secondsOf(cfg.BrokenCooldown),
		deadProxy:         secondsOf(cfg.DeadCooldown),
		selection:         cfg.Selection,
		minReady:          cfg.MinReady,
	}, nil
}

// wantedCodes is the scopes a [proxies] section serves, lowercased:
// the given ones (else cfg.Scopes) that cfg.Scopes allows.
func wantedCodes(cfg *Config, marketplaces []string) []string {
	allowed := map[string]bool{}
	for _, c := range cfg.Scopes {
		allowed[c] = true
	}
	from := marketplaces
	if len(from) == 0 {
		from = cfg.Scopes
	}
	var out []string
	for _, code := range from {
		code = strings.ToLower(code)
		if len(cfg.Scopes) == 0 || allowed[code] {
			out = append(out, code)
		}
	}
	return out
}

// ErrNoScopes is a `{country}` refresh_url with no scope codes
// to expand it with: no [proxies] scopes and none scanned.
var ErrNoScopes = errors.New("a {country} proxy refresh URL requires marketplace codes")

// sourceURLs is (URL, scope restriction) per feed; a plain URL
// remains a shared source.
func sourceURLs(cfg *Config, marketplaces []string) ([]source, error) {
	if cfg.FeedURL == "" {
		return nil, nil
	}
	if !strings.Contains(cfg.FeedURL, "{country}") {
		return []source{{url: cfg.FeedURL, site: ""}}, nil
	}
	codes := slices.Compact(slices.Sorted(slices.Values(wantedCodes(cfg, marketplaces))))
	if len(codes) == 0 {
		return nil, ErrNoScopes
	}
	out := make([]source, 0, len(codes))
	for _, code := range codes {
		out = append(out, source{url: strings.ReplaceAll(cfg.FeedURL, "{country}", strings.ToUpper(code)), site: code})
	}
	return out, nil
}

func refreshSources(ctx context.Context, pool *Pool, cfg *Config, marketplaces []string) error {
	sources, err := sourceURLs(cfg, marketplaces)
	if err != nil {
		return err
	}
	// Every locked section below unlocks by defer: a panic here is recovered
	// by the refresh timer or the scan runner, and a lock it left held would
	// hang every later Acquire, Health and Live.Pool.
	pending := duePending(pool, cfg, marketplaces, sources)
	if len(pending) > 0 {
		if err := collectFeeds(ctx, pool, cfg, sources, pending); err != nil {
			return err
		}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if _, err := pool.retireLocked(ctx, sources); err != nil {
		return err
	}
	activate(pool, cfg, sources)
	return pool.persistLocked(ctx, nil)
}

// duePending is refreshSources' first locked section: reconcile, then the
// sources whose refresh is due.
func duePending(pool *Pool, cfg *Config, marketplaces []string, sources []source) []source {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	activate(pool, cfg, sources)
	t := pool.now()
	now := unixSeconds(t)
	interval := secondsOf(min(day, max(time.Minute, cfg.RefreshInterval)))
	wanted := wantedCodes(cfg, marketplaces)
	if len(wanted) == 0 {
		wanted = []string{""}
	}

	short := func(site string) bool {
		n := 0
		for _, e := range pool.entries {
			if e.mayCarry(site, false, pool.proxyType) && e.available(t) && e.stat(site).available(t) {
				n++
			}
		}
		return n < cfg.MinReady
	}

	due := func(src source) bool {
		l := pool.sources[src]
		if l == nil {
			return true
		}
		if l.AttemptedAt != nil && now-*l.AttemptedAt < secondsOf(refreshRetry) {
			return false
		}
		if l.FetchedAt == nil || now-*l.FetchedAt >= interval {
			return true
		}
		sites := wanted
		if src.site != "" {
			sites = []string{src.site}
		}
		return slices.ContainsFunc(sites, short)
	}

	var pending []source
	for _, src := range sources {
		if due(src) {
			pending = append(pending, src)
		}
	}
	return pending
}

// collectFeeds downloads the pending feeds and commits each list as it
// arrives, in order. Network I/O never holds the pool or persistent lock.
// Countries fail independently, and each keeps its own last successful
// membership. Once ctx ends nothing more is committed: a download it cut
// short is not a failed feed.
func collectFeeds(ctx context.Context, pool *Pool, cfg *Config, sources, pending []source) error {
	client := pool.feeds
	// The downloads share the client's keep-alive connections; none outlives
	// the refresh.
	defer client.CloseIdleConnections()
	results := make([]chan []feedRecord, len(pending))
	slots := make(chan struct{}, min(5, len(pending)))
	var wg sync.WaitGroup
	// Every download finishes before this returns, a failed commit
	// included.
	defer wg.Wait()
	for i, src := range pending {
		results[i] = make(chan []feedRecord, 1)
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			results[i] <- fetchList(ctx, client, src.url, cfg.DefaultScheme, src.site, pool.log)
		})
	}
	for i, src := range pending {
		records := <-results[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := commitFeed(ctx, pool, cfg, sources, src, records); err != nil {
			return err
		}
	}
	return nil
}

// commitFeed records one feed's download under the pool lock and persists
// the inventory: records is what it listed, in feed order, or nil when the
// download failed, which only counts as an attempt.
func commitFeed(ctx context.Context, pool *Pool, cfg *Config, sources []source, src source,
	records []feedRecord) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	l := pool.sources[src]
	if l == nil {
		l = &listing{Members: map[string]json.RawMessage{}}
		pool.sources[src] = l
	}
	now := unixSeconds(pool.now())
	l.AttemptedAt = new(now)
	if records != nil {
		l.Members, l.FetchedAt = make(map[string]json.RawMessage, len(records)), new(now)
		for _, r := range records {
			l.Members[r.url] = r.metadata
			delete(pool.retired, fingerprint(r.url))
			e := pool.catalog.getOrAdd(r.url, now)
			e.origins[src] = true
			e.metadata[src.url] = r.metadata
			e.lastSeen = now
		}
	}
	// Every discovered endpoint is committed immediately, even if no scan
	// has tried or scored it yet.
	activate(pool, cfg, sources)
	return pool.persistLocked(ctx, nil)
}

// activate reconciles config with all retained inventory: the pool hands
// out the static endpoints first, in config order, then every catalog
// endpoint a current feed has listed, in catalog order. Caller holds the
// pool lock.
func activate(pool *Pool, cfg *Config, sources []source) {
	static := staticEndpoints(cfg)
	for _, u := range static {
		// The clock is read only for an endpoint new to the catalog.
		if _, gone := pool.retired[fingerprint(u)]; !gone && pool.catalog.get(u) == nil {
			pool.catalog.set(u, newEntry(u, unixSeconds(pool.now())))
		}
	}
	current := make(map[source]bool, len(sources))
	for _, s := range sources {
		current[s] = true
	}
	active := []*entry{}
	for _, u := range static {
		if e := pool.catalog.get(u); e != nil { // nil: retired since
			e.fromFeed, e.marketplaces = false, map[string]bool{}
			active = append(active, e)
		}
	}
	for _, u := range pool.catalog.keys {
		if slices.Contains(static, u) {
			continue
		}
		e := pool.catalog.get(u)
		if scope, listed := e.feedScope(current); listed {
			e.fromFeed, e.marketplaces = true, scope
			active = append(active, e)
		}
	}
	pool.entries = active
	pool.settings = SettingsFrom(cfg)
}

// feedScope is the scopes a feed endpoint may serve, given the feeds
// that are current: those of the country feeds that list it, or every
// scope (an empty scope) once a shared feed lists it too. listed is
// false when no current feed lists it at all.
func (e *entry) feedScope(current map[source]bool) (scope map[string]bool, listed bool) {
	scope = map[string]bool{}
	shared := false
	for o := range e.origins {
		if current[o] {
			listed = true
			shared = shared || o.site == ""
			scope[o.site] = true
		}
	}
	if shared {
		return map[string]bool{}, listed
	}
	return scope, listed
}

// Configured is the endpoints cfg describes, redacted — what the pool
// *would* hold.
//
// Lets a status endpoint answer before the first scan has built the pool,
// without installing anything as a side effect.
func Configured(cfg *Config) []string {
	out := []string{}
	if cfg == nil || !cfg.Enabled() {
		return out
	}
	for _, u := range staticEndpoints(cfg) {
		out = append(out, Redact(u))
	}
	return out
}
