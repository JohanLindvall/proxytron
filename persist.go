package proxytron

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/JohanLindvall/proxytron/store"
)

// errNoURL is a stored proxy record without the URL that keys it.
var errNoURL = errors.New("no url")

// persistLocked saves one entry's outcome (e != nil) or checkpoints the
// whole inventory: every record, the feed listings, the retired markers and
// the learned prior. The caller holds the pool lock, so the save is
// committed before another outcome can change the pool.
func (p *Pool) persistLocked(ctx context.Context, e *entry) error {
	if p.store == nil {
		return nil
	}
	now := p.now()
	first, err := json.Marshal(p.first)
	if err != nil {
		return err
	}
	if e != nil {
		data, err := json.Marshal(p.record(e, now))
		if err != nil {
			return err
		}
		return p.store.SaveProxy(ctx, store.Record{URL: e.url, Data: data}, first)
	}
	rows := make([]store.Record, 0, len(p.catalog.keys))
	for _, e := range p.catalog.values() {
		data, err := json.Marshal(p.record(e, now))
		if err != nil {
			return err
		}
		rows = append(rows, store.Record{URL: e.url, Data: data})
	}
	sources := make([]store.Source, 0, len(p.sources))
	for _, src := range sortedSources(p.sources) {
		data, err := json.Marshal(p.sources[src])
		if err != nil {
			return err
		}
		sources = append(sources, store.Source{URL: src.url, Scope: src.site, Data: data})
	}
	return p.store.SaveProxies(ctx, rows, sources, first, maps.Clone(p.retired))
}

// persistLogged is persistLocked for the outcome paths (ReportOK, Demote,
// DemoteTransport, Punish, Refresh), which have no error to return: a
// failed save is logged under ctx's scope, and the next outcome saves
// the entry again.
func (p *Pool) persistLogged(ctx context.Context, e *entry) {
	if err := p.persistLocked(ctx, e); err != nil {
		p.log.ErrorContext(ctx, fmt.Sprintf("saving proxy state failed: %v", err))
	}
}

// sortedSources is the keys of a feed-listing map, by URL then scope.
func sortedSources[V any](m map[source]V) []source {
	return slices.SortedFunc(maps.Keys(m), func(a, b source) int {
		return cmp.Or(cmp.Compare(a.url, b.url), cmp.Compare(a.site, b.site))
	})
}

// -- attaching the database ----------------------------------------------------

// inventory is the stored state attach restores, decoded.
type inventory struct {
	entries  []*entry // in inventory (rowid) order
	sources  map[source]*listing
	first    map[string]firstStat
	retired  map[string]float64
	imported bool // the legacy proxy-memory.json was imported once already
}

// decodeInventory decodes what LoadProxies read, restoring benches against
// now. Any document that does not decode is an error: a corrupt row fails
// the attach rather than being dropped from the inventory.
func decodeInventory(stored store.Inventory, now time.Time) (inventory, error) {
	inv := inventory{
		sources:  make(map[source]*listing, len(stored.Sources)),
		first:    map[string]firstStat{},
		retired:  make(map[string]float64, len(stored.Retired)),
		imported: stored.Imported,
	}
	maps.Copy(inv.retired, stored.Retired)
	for i, raw := range stored.Rows {
		e, err := decodeEntry(raw, now)
		if err != nil {
			return inv, fmt.Errorf("stored proxy record %d: %w", i+1, err)
		}
		inv.entries = append(inv.entries, e)
	}
	for _, src := range stored.Sources {
		l := &listing{}
		if err := json.Unmarshal(src.Data, l); err != nil {
			return inv, fmt.Errorf("stored proxy feed listing %s (%s): %w", src.URL, src.Scope, err)
		}
		if l.Members == nil {
			l.Members = map[string]json.RawMessage{}
		}
		inv.sources[source{url: src.URL, site: src.Scope}] = l
	}
	if err := json.Unmarshal(stored.First, &inv.first); err != nil {
		return inv, fmt.Errorf("stored first-attempt prior: %w", err)
	}
	if inv.first == nil { // a stored null
		inv.first = map[string]firstStat{}
	}
	return inv, nil
}

// attach opens the database at path and restores the inventory from it
// before anything decides whether to contact a feed; on a database that
// has never imported it, the legacy proxy-memory.json beside it is imported
// first, matched against cfg's feeds for these scopes. The routes the
// pool held stay the routes it hands out, now with their stored history. A
// second call is a no-op.
//
// When anything stored fails to decode, nothing of the pool changes and no
// database stays attached.
func (p *Pool) attach(ctx context.Context, path string, cfg *Config, marketplaces []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store != nil {
		return nil
	}
	store, err := p.openStore(ctx, path)
	if err != nil {
		return err
	}
	if err := p.loadInventory(ctx, store, cfg, marketplaces); err != nil {
		closeLogged(ctx, store)
		return err
	}
	p.store = store
	return p.persistLocked(ctx, nil)
}

// loadInventory restores store's inventory into the pool, imports the
// legacy file if the inventory has not, and puts back the routes the pool
// was handing out. Everything that can fail happens before the pool
// changes. Lock held.
func (p *Pool) loadInventory(ctx context.Context, store store.Store, cfg *Config,
	marketplaces []string) error {
	stored, err := store.LoadProxies(ctx)
	if err != nil {
		return err
	}
	now := p.now()
	inv, err := decodeInventory(stored, now)
	if err != nil {
		return err
	}
	var legacy *legacyImport
	if p.legacyImport && !inv.imported {
		if legacy, err = loadLegacy(ctx, store.Path(), cfg, marketplaces, p.log); err != nil {
			return err
		}
	}
	active := activeRoutes(p.entries)
	p.sources, p.first, p.retired = inv.sources, inv.first, inv.retired
	for _, e := range inv.entries {
		p.catalog.set(e.url, e)
	}
	if legacy != nil {
		p.importLegacy(legacy, now)
	}
	p.keepActive(active)
	return nil
}

// activeRoute is a route the pool hands out, as it stood before an attach.
type activeRoute struct {
	url          string
	marketplaces map[string]bool
	fromFeed     bool
}

func activeRoutes(entries []*entry) []activeRoute {
	out := make([]activeRoute, 0, len(entries))
	for _, e := range entries {
		out = append(out, activeRoute{url: e.url, marketplaces: e.marketplaces, fromFeed: e.fromFeed})
	}
	return out
}

// keepActive makes the routes active the pool's entries again, each now
// the catalog's entry for its URL with the scope scope and feed
// origin it had: attaching after an API override restores the override's
// routes' health without expanding the override to every stored endpoint.
// Lock held.
func (p *Pool) keepActive(active []activeRoute) {
	entries := make([]*entry, 0, len(active))
	for _, a := range active {
		if e := p.catalog.get(a.url); e != nil { // every route is in the catalog
			e.marketplaces, e.fromFeed = a.marketplaces, a.fromFeed
			entries = append(entries, e)
		}
	}
	p.entries = entries
}

// closeLogged closes the proxy database, a pool's or one whose attach
// failed, where no caller is left to take the error: a failure is a
// warning under ctx's scope.
func closeLogged(ctx context.Context, c io.Closer) {
	log := slog.Default()
	if p, ok := c.(*Pool); ok && p.log != nil {
		log = p.log
	}
	if err := c.Close(); err != nil {
		log.WarnContext(ctx, fmt.Sprintf("closing the proxy database failed: %v", err))
	}
}

// -- the legacy proxy-memory.json -------------------------------------------------

// legacyImport is the legacy file's rows, with what they are matched
// against: the feeds the config downloads, and the static endpoints, which
// are no feed's.
type legacyImport struct {
	rows    []legacyRow
	sources []source
	static  map[string]bool
}

// loadLegacy reads the pre-persistent proxy-memory.json beside the database at
// dbPath. A file that is missing or cannot be read is a fresh start (nil),
// the latter with a warning.
func loadLegacy(ctx context.Context, dbPath string, cfg *Config, marketplaces []string, logs ...*slog.Logger) (*legacyImport, error) {
	log := slog.Default()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	rows, err := readLegacy(filepath.Join(filepath.Dir(dbPath), legacyMemory))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		log.WarnContext(ctx, fmt.Sprintf("legacy proxy memory unreadable: %v", err))
		return nil, nil
	}
	sources, err := sourceURLs(cfg, marketplaces)
	if err != nil {
		return nil, err
	}
	static := map[string]bool{}
	for _, u := range staticEndpoints(cfg) {
		static[u] = true
	}
	return &legacyImport{rows: rows, sources: sources, static: static}, nil
}

// readLegacy reads the legacy file: a JSON array of legacyRow.
func readLegacy(path string) ([]legacyRow, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []legacyRow
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// importLegacy imports the legacy file into the catalog. It kept no
// inventory or timestamps, so each endpoint it names starts its age clock
// at now; a bench that has run out meanwhile is not restored. Lock held.
func (p *Pool) importLegacy(imp *legacyImport, now time.Time) {
	wall := unixSeconds(now)
	for _, row := range imp.rows {
		if row.URL != "" {
			p.adopt(row, imp.sources, imp.static, wall)
		}
	}
	for _, row := range imp.rows {
		if row.URL == "" {
			p.restoreFirst(row.First)
		} else {
			p.catalog.get(row.URL).restore(row, now)
		}
	}
}

// adopt adds the endpoint row names to the catalog, first seen at wall
// (Unix seconds). A feed endpoint is credited to every shared feed and to
// each country feed whose scope the row has a record for; one that
// had served counts as successful now. Lock held.
func (p *Pool) adopt(row legacyRow, sources []source, static map[string]bool, wall float64) {
	e := p.catalog.getOrAdd(row.URL, wall)
	if !static[e.url] {
		for _, src := range sources {
			if _, listed := row.Sites[src.site]; src.site == "" || listed {
				e.origins[src] = true
			}
		}
		e.fromFeed = true
	}
	if row.Served != 0 {
		e.lastSuccess = new(wall)
	}
}

// restore puts back what row kept for the endpoint: the served and failure
// counts, every bench still running at now, and the record per site where it
// has none yet. A site's record is created only for something restored
// into it, a bench or a record: a row whose only bench there has run out
// leaves no trace. Lock held.
func (e *entry) restore(row legacyRow, now time.Time) {
	e.served = max(e.served, row.Served)
	e.fails = max(e.fails, row.Fails)
	e.cooldownUntil = later(e.cooldownUntil, deadlineFromUntil(now, row.Until))
	for code, st := range row.Sites {
		if bench := deadlineFromUntil(now, st.Until); !bench.IsZero() {
			s := e.stat(code)
			s.challenges = max(s.challenges, st.Challenges)
			s.cooldownUntil = later(s.cooldownUntil, bench)
		}
		if s := e.sites[code]; st.Tries != 0 && (s == nil || s.tries == 0) {
			s = e.stat(code)
			s.ok, s.bad, s.tries, s.duration = st.OK, st.Bad, st.Tries, st.Duration
		}
	}
}

// restoreFirst puts back the first-attempt priors of the sites the pool has
// none for; one that is not [attempts, valid, seconds, timed], null
// included, is skipped and leaves the site to a later row. Lock held.
func (p *Pool) restoreFirst(first map[string]json.RawMessage) {
	for code, raw := range first {
		var f *firstStat // stays nil for null
		if _, known := p.first[code]; !known && json.Unmarshal(raw, &f) == nil && f != nil {
			p.first[code] = *f
		}
	}
}

// storePath is the path of the attached database, "" when none is.
func (p *Pool) storePath() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		return ""
	}
	return p.store.Path()
}

// Close detaches (and closes) the database, if one is attached.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		return nil
	}
	err := p.store.Close()
	p.store = nil
	return err
}

// -- retirement -------------------------------------------------------------------

// retire forgets 30-day-old, unsuccessful endpoints absent from current
// feeds. Returns how many were retired.
//
// A successful feed listing protects even dead or unused proxies. A failed
// fetch keeps the last good listing, so an outage cannot erase inventory.
// Direct is an egress policy, not a proxy to retire.
func (p *Pool) retire(ctx context.Context, sources []source) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, err := p.retireLocked(ctx, sources)
	if err != nil {
		p.log.ErrorContext(ctx, fmt.Sprintf("saving proxy state failed: %v", err))
	}
	return n
}

func (p *Pool) retireLocked(ctx context.Context, sources []source) (int, error) {
	now := unixSeconds(p.now())
	stale := p.staleLocked(sources, now)
	if len(stale) == 0 {
		return 0, nil
	}
	p.catalog.deleteAll(stale)
	for u := range stale {
		// No endpoint/credentials remain in this marker. Prevent an old
		// static config reintroducing it on every timer tick; a new feed
		// listing explicitly permits rediscovery.
		p.retired[fingerprint(u)] = now
	}
	for _, l := range p.sources {
		maps.DeleteFunc(l.Members, func(u string, _ json.RawMessage) bool { return stale[u] })
	}
	p.entries = slices.DeleteFunc(p.entries, func(e *entry) bool { return stale[e.url] })
	maps.DeleteFunc(p.lastUsed, func(_, u string) bool { return stale[u] })
	if err := p.persistLocked(ctx, nil); err != nil {
		return 0, err
	}
	p.log.InfoContext(ctx, fmt.Sprintf("retired %d proxies absent from feeds without a working response for 30 days", len(stale)))
	return len(stale), nil
}

// staleLocked is the endpoints retirement forgets at now (Unix seconds):
// none of sources' last listings names them, and neither their first
// sighting nor their last success is less than retireAfter old. Lock held.
func (p *Pool) staleLocked(sources []source, now float64) map[string]bool {
	listed := map[string]bool{}
	for _, src := range sources {
		if l := p.sources[src]; l != nil {
			for u := range l.Members {
				listed[u] = true
			}
		}
	}
	stale := map[string]bool{}
	for _, e := range p.catalog.values() {
		since := e.firstSeen
		if e.lastSuccess != nil {
			since = *e.lastSuccess
		}
		if e.url != Direct && !listed[e.url] && now-since >= secondsOf(retireAfter) {
			stale[e.url] = true
		}
	}
	return stale
}
