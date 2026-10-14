package proxytron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/JohanLindvall/proxytron/store"
)

// Direct is a pool member meaning "send this one straight out, no proxy".
// Listing it alongside real endpoints is what makes alternating between a
// tunnel and the machine's own address a rotation rather than a switch, and
// it earns the same health handling as any endpoint: if the home address is
// the one being walled, it demotes and the tunnel carries the run, and vice
// versa.
const Direct = "direct"

// IsDirect reports whether s names Direct as a config line or a request
// body may write it: in any case, surrounding space ignored.
func IsDirect(s string) bool { return strings.EqualFold(strings.TrimSpace(s), Direct) }

const (
	day = 24 * time.Hour
	// retireAfter is how long an endpoint absent from every current feed may
	// go without a working response before it is forgotten.
	retireAfter = 30 * day
	// refreshRetry bounds low-inventory retries, including across restarts.
	refreshRetry = time.Minute
	// legacyMemory is the file name of the pre-persistent proxy memory, imported
	// once from beside the database.
	legacyMemory = "proxy-memory.json"
	// defaultScheme is what a schemeless line of a list the pool is handed
	// directly (Live.Override) speaks; config lines use [proxies]
	// refresh_scheme.
	defaultScheme = "socks5h"
)

// -- clocks -------------------------------------------------------------------

// Clock is where a Pool reads the time: every cooldown, bench and stored
// timestamp follows it.
type Clock interface {
	Now() time.Time
}

// systemClock is the real time.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// secondsOf is d in the float seconds the pool computes in and stores:
// one correctly rounded division. Duration.Seconds rounds twice (the whole
// seconds, then the fraction) and lands one ulp away on 173 of ten million
// random durations, which would move stored deadlines.
func secondsOf(d time.Duration) float64 { return float64(d) / float64(time.Second) }

// durationOf is s seconds as a Duration, to the nearest nanosecond, so a
// round trip through secondsOf is exact for any value with at most nine
// decimals.
func durationOf(s float64) time.Duration {
	return time.Duration(math.Round(s * float64(time.Second)))
}

// -- arithmetic -----------------------------------------------------------------

// indicator is 1 for true and 0 for false: an outcome as what it adds to a
// count of such outcomes.
func indicator(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// backoff is base doubled for each failure in a row after the first (n
// counts them, the first included), up to ceiling. The doubling is done in
// float seconds, since base doubled thirty times would overflow a Duration.
func backoff(base, ceiling time.Duration, n int) time.Duration {
	return durationOf(min(secondsOf(base)*math.Pow(2, float64(min(n-1, 30))), secondsOf(ceiling)))
}

// -- per-site and per-endpoint state ---------------------------------------------

// siteStat is what one endpoint has done on one scope.
//
// Everything the destination-shaped is keyed this way, because refusal is a
// per-domain verdict: `se` walling an endpoint says nothing about whether
// `it` will. A single shared counter conflated "this endpoint is bad" with
// "one destination refuses everyone", which is exactly what the first measured
// pass showed — both endpoints sat near a 0.5 refusal rate while four of the
// five sites ran at 0-4.7%, because `se`'s walls were being charged to
// whichever endpoint happened to carry them.
type siteStat struct {
	// Recent-weighted means of the seconds a served page took and of the
	// share of the destination's verdicts that refused; nil until there is one.
	latency *float64
	refusal *float64
	fails   int // consecutive refusals *on this site*
	// Consecutive WAF challenges on this site. Unlike fails it outlives
	// the pass (NewPass leaves it), because it is what doubles the bench: a
	// route the WAF keeps judging stays out for longer each time.
	challenges int
	// The end of this site's bench, the zero Time when none is running; the
	// other sites keep using the route meanwhile.
	cooldownUntil time.Time
	// What throughput selection reads (Pool.Acquire): recency-weighted
	// counts of valid and invalid attempts on this site, how many attempts
	// it has had here at all, and the mean seconds an attempt took whatever
	// its outcome — a dead proxy's connect timeout costs the pass as surely
	// as a page does, so failures are timed too.
	ok       float64
	bad      float64
	tries    int
	duration *float64 // nil until an attempt is timed
}

// available reports whether the site's own cooldown has run out.
func (s *siteStat) available(now time.Time) bool { return !now.Before(s.cooldownUntil) }

// refusalBench reports whether the site's own bench is running at now for
// a refusal: one with no WAF challenge on the record, which Punish
// would have counted (benchOf).
func (s *siteStat) refusalBench(now time.Time) bool {
	return s.cooldownUntil.After(now) && s.challenges == 0
}

// observe folds the time a served page took into the latency mean,
// favouring recent evidence. A time that is not positive measures nothing.
func (s *siteStat) observe(took time.Duration) {
	const alpha = 0.3
	if took <= 0 {
		return
	}
	seconds := secondsOf(took)
	if s.latency == nil {
		s.latency = new(seconds)
	} else {
		s.latency = new(float64(alpha*seconds) + float64((1-alpha)*(*s.latency)))
	}
}

// observeOutcome folds one the destination verdict into the refusal rate.
//
// Seeded at 0 rather than at the first observation. Taking the first value
// directly would put a single transient 503 at a refusal rate of 1.0 — far
// too harsh a reading of one refusal when refusal is bimodal and an isolated
// one is noise. Blending from zero makes one wall in a healthy stream worth
// 0.2 and a sustained wall converge on 1.0.
func (s *siteStat) observeOutcome(refused bool) {
	const alpha = 0.2
	x := indicator(refused)
	prev := 0.0
	if s.refusal != nil {
		prev = *s.refusal
	}
	s.refusal = new(float64(alpha*x) + float64((1-alpha)*prev))
}

// source is one feed listing: (URL, scope restriction). An empty site
// is a shared source.
type source struct {
	url  string
	site string
}

// entry is one endpoint and everything measured about it.
type entry struct {
	egress Egress
	url    string // e.g. "socks5h://user:pass@host:1080" or "http://host:3128"
	// The scopes it may serve; empty = explicitly shared endpoint.
	// Always replaced, never changed in place: an attach hands the map an
	// entry had to the stored entry that takes its place (keepActive).
	marketplaces map[string]bool
	// Transport health: the endpoint itself is unreachable (connect refused,
	// timeout, dead tunnel). That really is endpoint-wide, so it cools the
	// endpoint everywhere. Inventory retirement is separate from cooldowns.
	// An the destination refusal is emphatically *not* this, and lives in the
	// per-site stats.
	fails int
	// The end of the endpoint-wide bench, the zero Time when none is
	// running.
	cooldownUntil    time.Time
	transportFailure float64 // EWMA; survives an intermittent success
	sites            map[string]*siteStat
	// Pages this endpoint has ever served, on any site. What separates a
	// dead feed entry from a working proxy that blipped, and so how hard a
	// failed connection is punished: see DemoteTransport.
	served int
	// Came from a refresh feed rather than the static list. Only a feed
	// entry can be judged dead on one failure: a configured tunnel, or
	// Direct, that blips before its first page is still the operator's
	// chosen route.
	fromFeed bool
	// When it was first and last listed, last tried and last served a page,
	// in Unix seconds, as they are stored; nil when it never was.
	firstSeen   float64
	lastSeen    float64
	lastTested  *float64
	lastSuccess *float64
	// Origins outlive membership in the latest feed, so a recently working
	// endpoint can still be used after disappearing from a download.
	origins map[source]bool
	// Each feed's record of it, keyed by feed URL, as the feed listed it.
	metadata map[string]json.RawMessage
}

// newEntry is an endpoint nothing is known about yet, first seen at wall
// (Unix seconds).
func newEntry(url string, wall float64) *entry {
	return &entry{
		url:          url,
		marketplaces: map[string]bool{},
		sites:        map[string]*siteStat{},
		firstSeen:    wall,
		lastSeen:     wall,
		origins:      map[source]bool{},
		metadata:     map[string]json.RawMessage{},
	}
}

// supports reports whether the endpoint may carry site's traffic.
func (e *entry) supports(site string) bool {
	return len(e.marketplaces) == 0 || e.marketplaces[site]
}

// mayCarry reports whether the endpoint is one of site's routes, cooling or
// not: one it supports, and not the home address unless allowDirect.
func (e *entry) mayCarry(site string, allowDirect bool) bool {
	return e.supports(site) && (allowDirect || e.url != Direct)
}

// dead reports whether a failed connection judges the endpoint dead rather
// than broken: a feed entry that has never served a page. The operator's
// own routes, static endpoints and Direct, are never judged on one failure.
func (e *entry) dead() bool { return e.fromFeed && e.served == 0 }

// observeTransport folds one connection outcome into transportFailure.
func (e *entry) observeTransport(failed bool) {
	e.transportFailure = float64(0.2*indicator(failed)) + float64(0.8*e.transportFailure)
}

// available reports whether the endpoint-wide cooldown has run out.
func (e *entry) available(now time.Time) bool { return !now.Before(e.cooldownUntil) }

// stat is the endpoint's record on site, created on first use.
func (e *entry) stat(site string) *siteStat {
	s, ok := e.sites[site]
	if !ok {
		s = &siteStat{}
		e.sites[site] = s
	}
	return s
}

// siteCodes is the scopes the endpoint has a record on, in code
// order: the order its averages add them up in.
func (e *entry) siteCodes() []string { return slices.Sorted(maps.Keys(e.sites)) }

// siteMean is the mean of figure over the sites that have measured it,
// added up in code order: the endpoint-wide figure the health summary
// shows. nil when no site has.
func (e *entry) siteMean(figure func(*siteStat) *float64) *float64 {
	sum, n := 0.0, 0
	for _, code := range e.siteCodes() {
		if x := figure(e.sites[code]); x != nil {
			sum += *x
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return new(sum / float64(n))
}

func latencyOf(s *siteStat) *float64 { return s.latency }

func refusalOf(s *siteStat) *float64 { return s.refusal }

// sortedCodes is the members of a set of scope codes in order, never
// nil: no codes is [] in a stored record and on /api/proxies.
func sortedCodes(set map[string]bool) []string {
	codes := slices.AppendSeq(make([]string, 0, len(set)), maps.Keys(set))
	slices.Sort(codes)
	return codes
}

// -- the catalog -------------------------------------------------------------------

// catalog is every endpoint the pool knows, listed or not, in the order it
// first learned of them: the database's row order, then each feed's. That
// order is load-bearing, since activate hands routes out in it: it is the
// order untried routes are explored in and the rotation's.
type catalog struct {
	keys []string
	m    map[string]*entry
}

func (c *catalog) get(u string) *entry { return c.m[u] }

// set assigns; an existing key keeps its place.
func (c *catalog) set(u string, e *entry) {
	if c.m == nil {
		c.m = map[string]*entry{}
	}
	if _, ok := c.m[u]; !ok {
		c.keys = append(c.keys, u)
	}
	c.m[u] = e
}

// getOrAdd is the entry for u, added (first seen at wall) when there is
// none yet.
func (c *catalog) getOrAdd(u string, wall float64) *entry {
	if e, ok := c.m[u]; ok {
		return e
	}
	e := newEntry(u, wall)
	c.set(u, e)
	return e
}

func (c *catalog) deleteAll(stale map[string]bool) {
	keys := c.keys[:0:0]
	for _, k := range c.keys {
		if stale[k] {
			delete(c.m, k)
			continue
		}
		keys = append(keys, k)
	}
	c.keys = keys
}

func (c *catalog) values() []*entry {
	out := make([]*entry, 0, len(c.keys))
	for _, k := range c.keys {
		out = append(out, c.m[k])
	}
	return out
}

// -- the pool ---------------------------------------------------------------------

// Selection's constants. Each new outcome counts once and older ones fade by
// outcomeDecay, so an endpoint's record reflects roughly its last ten
// attempts here: free proxies change fast, and a record that never forgot
// would keep sending pages to one that has started failing.
const outcomeDecay = 0.9

// The prior an untried endpoint starts from is the site's record of first
// attempts, shrunk toward priorValid / priorSeconds by priorWeight
// pseudo-attempts so the first few do not swing it. 0.25 is measured: 216
// of the 851 country-feed endpoints had served a page ten hours in. Eight
// seconds sits between a refusal's one and a dead proxy's ten-second
// connect timeout. priorStrength is how many attempts the prior is worth
// against an endpoint's own record.
const (
	priorValid    = 0.25
	priorSeconds  = 8.0
	priorWeight   = 4.0
	priorStrength = 2.0
	minSeconds    = 0.05
)

// invalidPenalty is what an invalid attempt costs beyond its seconds: it
// spends one of the pass's `max_requests`, and a refusal pushes the site
// toward the guard that ends its pass at four in a row. Priced at a dead
// proxy's connect timeout, so a route that is quick because it is refused —
// the WAF-flagged routes of 2026-10-09 answered in a fraction of a second
// with a 1.9 kB challenge — cannot win on speed: at 10% valid and 0.4 s it
// would otherwise beat a 90%-valid route at 3 s by pure valid-per-second.
const invalidPenalty = 10.0

// The ceilings of the two doubling benches a site gives a route: a
// refusal's (Settings.BaseCooldown doubling per refusal in a row) and an AWS
// WAF challenge's (Settings.ChallengeCooldown doubling per challenge in a
// row).
const (
	maxCooldown          = 30 * time.Minute
	maxChallengeCooldown = day
)

// Settings are a Pool's tunables, as a [proxies] section sets them
// (SettingsFrom).
type Settings struct {
	// BaseCooldown benches a route a site refused, on that site, doubling
	// per refusal in a row up to half an hour. See Pool.Demote.
	BaseCooldown time.Duration
	// ChallengeCooldown benches a route a site's WAF challenged, on that
	// site, doubling per challenge in a row up to a day. See Pool.Punish.
	ChallengeCooldown time.Duration
	// A failed connection benches the route everywhere. A feed entry that
	// has never served a page sits out DeadCooldown at once; anything else
	// sits out BrokenCooldown, doubling per failure in a row up to
	// DeadCooldown. See Pool.DemoteTransport.
	BrokenCooldown time.Duration
	DeadCooldown   time.Duration
	// Selection is SelectionThroughput (see Pool.Acquire) or
	// SelectionRoundRobin, the rotation it replaced: kept so the two
	// can be compared from the watchlist.
	Selection string
}

// SettingsFrom is the settings cfg configures. A pool reconciled with a
// config (LoadPool, and every refresh after) takes its settings from there,
// so an edit of the watchlist applies without a restart.
func SettingsFrom(cfg *Config) Settings {
	return Settings{
		BaseCooldown:      cfg.BaseCooldown,
		ChallengeCooldown: cfg.ChallengeCooldown,
		BrokenCooldown:    cfg.BrokenCooldown,
		DeadCooldown:      cfg.DeadCooldown,
		Selection:         cfg.Selection,
	}
}

// Pool is a pool of proxy URLs, shared across scope goroutines.
//
// Acquire picks the available endpoint expected to deliver valid responses
// fastest (or rotates in list order with selection = "round_robin");
// Demote, DemoteTransport and Punish push a failing one into a growing
// cooldown; Refresh swaps in a new list without losing the health state of
// endpoints that carry over. Country feeds are restricted to their
// scope. "" from Acquire means no eligible route is available. Direct egress
// requires an explicitly configured Direct member and the caller's permission.
//
// Build one with New, FromURLs or LoadPool. A nil *Pool is an empty one to
// Len.
type Pool struct {
	mu       sync.Mutex
	entries  []*entry
	settings Settings
	clock    Clock
	// rng is the source of the Thompson-sampling draws. Only used under the
	// pool's lock.
	rng *rand.Rand

	// Per site, the first attempt ever made through each endpoint: what an
	// untried endpoint is expected to do, so it is learned from the feed
	// rather than assumed (see prior).
	first map[string]firstStat
	// The route each scope was last handed, which round-robin
	// selection continues from.
	lastUsed map[string]string
	// Each feed's last download.
	sources map[source]*listing
	catalog catalog
	// When each retired endpoint was retired, by fingerprint (Unix seconds).
	retired      map[string]float64
	store        store.Store
	openStore    StoreOpener
	log          *slog.Logger
	scopeContext func(context.Context, string) context.Context
	legacyImport bool
	// feeds is the client the pool downloads its feeds through.
	feeds *http.Client
}

// Option configures a Pool built by New, FromURLs or LoadPool, or every
// pool a Live builds (NewLive).
type Option func(*Pool)

// WithSettings replaces the pool's settings, which default to those of an
// empty [proxies] section. LoadPool's pool takes them from its config
// regardless.
func WithSettings(s Settings) Option { return func(p *Pool) { p.settings = s } }

// WithClock makes the pool read the time from c instead of the system
// clock; nil keeps the system clock.
func WithClock(c Clock) Option {
	return func(p *Pool) {
		if c != nil {
			p.clock = c
		}
	}
}

// WithHTTPClient makes the pool download its feeds through c instead of a
// client of its own (newFeedClient); nil keeps that. Tests hand it a client
// that answers from memory.
func WithHTTPClient(c *http.Client) Option {
	return func(p *Pool) {
		if c != nil {
			p.feeds = c
		}
	}
}

func newRand() *rand.Rand { return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) }

// New is an empty pool: the default settings, then the options.
func New(opts ...Option) *Pool {
	defaults := DefaultConfig()
	p := &Pool{
		settings:     SettingsFrom(&defaults),
		clock:        systemClock{},
		openStore:    store.Open,
		log:          slog.Default(),
		scopeContext: func(ctx context.Context, _ string) context.Context { return ctx },
		rng:          newRand(),
		first:        map[string]firstStat{},
		lastUsed:     map[string]string{},
		sources:      map[source]*listing{},
		retired:      map[string]float64{},
		feeds:        newFeedClient(feedTimeout, nil),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// FromURLs is a pool of the static endpoints urls, blanks and repeats
// dropped.
func FromURLs(urls []string, opts ...Option) *Pool {
	p := New(opts...)
	wall := unixSeconds(p.now())
	for _, u := range uniqueURLs(urls) {
		e := newEntry(u, wall)
		p.entries = append(p.entries, e)
		p.catalog.set(u, e)
	}
	return p
}

// Len is the number of routes the pool hands out; 0 for a nil pool, so
// `pool.Len() > 0` is the test for a pool worth asking.
func (p *Pool) Len() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// now reads the pool's clock. A real clock's readings carry a monotonic
// component, which deadlines inherit and compare by, so a bench never
// stretches or shrinks with a step of the wall clock.
func (p *Pool) now() time.Time { return p.clock.Now() }

// find is the pooled endpoint url, nil when it is not in the pool. Lock
// held.
func (p *Pool) find(url string) *entry {
	for _, e := range p.entries {
		if e.url == url {
			return e
		}
	}
	return nil
}

// priorT is (alpha, beta, seconds) for an endpoint never tried on a site.
type priorT struct{ A, B, D float64 }

// prior is (alpha, beta, seconds) for an endpoint never tried on site.
func (p *Pool) prior(site string) priorT {
	f := p.first[site]
	w := priorWeight
	q := (f.Valid + priorValid*w) / (float64(f.Attempts) + w)
	d := (f.Seconds + priorSeconds*w) / (float64(f.Timed) + w)
	return priorT{priorStrength * q, priorStrength * (1.0 - q), d}
}

// observe folds one attempt's outcome into what selection reads: an
// attempt on site that took took (a negative duration times nothing),
// finished at now. Lock held.
func (p *Pool) observe(e *entry, site string, valid bool, took time.Duration, now time.Time) {
	e.lastTested = new(unixSeconds(now))
	if valid {
		e.lastSuccess = new(*e.lastTested)
	}
	s := e.stat(site)
	timed, seconds := took >= 0, secondsOf(took)
	if s.tries == 0 {
		first := p.first[site]
		first.Attempts++
		first.Valid += indicator(valid)
		if timed {
			first.Seconds += seconds
			first.Timed++
		}
		p.first[site] = first
	}
	s.tries++
	s.ok = float64(s.ok*outcomeDecay) + indicator(valid)
	s.bad = float64(s.bad*outcomeDecay) + indicator(!valid)
	if timed {
		if s.duration == nil {
			s.duration = new(seconds)
		} else {
			// Explicit rounding keeps persisted scores identical on CPUs with FMA.
			s.duration = new(float64(0.3*seconds) + float64(0.7*(*s.duration)))
		}
	}
}

// expectedRate is valid responses per second through an endpoint: P(valid)
// over the seconds an attempt costs, including any wait its host's pacing
// floor imposes right now and invalidPenalty for the chance it comes back
// invalid. sample draws P(valid) from its Beta posterior (Thompson
// sampling) instead of taking the mean.
func (p *Pool) expectedRate(s *siteStat, pr priorT, pause float64, sample bool) float64 {
	a, b := pr.A+s.ok, pr.B+s.bad
	var q float64
	if sample {
		q = betaVariate(p.rng, a, b)
	} else {
		q = a / (a + b)
	}
	seconds := pr.D
	if s.duration != nil {
		seconds = *s.duration
	}
	seconds += pause
	return q / max(seconds+invalidPenalty*(1.0-q), minSeconds)
}

// Acquire is the route for this scope's next attempt, or "" when
// every route it may use is benched.
//
// Cooling routes and forbidden direct/foreign routes are never handed out,
// and a retry prefers a route it has not tried for this page (exclude).
// Among the rest, the default selection = "throughput" maximises valid
// responses per second. A scope's fetcher sends one request at a
// time, so its throughput is what an attempt yields over what it costs, and
// the best route is the one with the highest P(valid) / E[seconds] (a
// renewal-reward argument: the long-run rate of a sequence of attempts is
// E[reward] / E[time] per attempt).
//
// P(valid) is a Beta posterior over this route's recent attempts on this
// site, and seconds is its mean attempt time whatever the outcome, plus
// what wait(url) says its host's pacing floor would add right now. wait may
// be nil; it runs under the pool's lock, so it must not call back into the
// pool. Each candidate's P(valid) is drawn from its posterior rather than
// taken at the mean (Thompson sampling), so a route that looks slightly
// worse still gets the occasional turn its uncertainty earns, and a route
// that starts failing loses its share within a few attempts. Untried routes
// are interchangeable until tried: one draw from the site's prior stands
// for all of them, and the next one in list order is the one tried. Drawing
// once per untried route instead would let four hundred of them, three in
// four dead, outbid a route known to work by sheer number of lottery
// tickets.
//
// selection = "round_robin" restores the rotation this replaced: each
// scope keeps its own position in the list, the last URL rather than
// an index, so a feed refresh that reorders entries does not reset it.
func (p *Pool) Acquire(site string, allowDirect bool, exclude map[string]bool,
	wait func(url string) time.Duration) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	var live []*entry
	for _, e := range p.entries {
		if e.supports(site) && e.available(now) && e.stat(site).available(now) &&
			(allowDirect || e.url != Direct) {
			live = append(live, e)
		}
	}
	if len(live) == 0 {
		return ""
	}
	var fresh []*entry
	for _, e := range live {
		if !exclude[e.url] {
			fresh = append(fresh, e)
		}
	}
	if len(fresh) == 0 {
		fresh = live
	}
	var pick *entry
	if p.settings.Selection == SelectionRoundRobin {
		pick = p.nextInOrder(site, fresh)
	} else {
		pick = p.bestRate(site, fresh, wait)
	}
	p.lastUsed[site] = pick.url
	return pick.url
}

// nextInOrder is the first of among after the last route handed out for
// site.
func (p *Pool) nextInOrder(site string, among []*entry) *entry {
	members := make(map[*entry]bool, len(among))
	for _, e := range among {
		members[e] = true
	}
	previous, had := p.lastUsed[site]
	start := 0
	if had {
		for i, e := range p.entries {
			if e.url == previous {
				start = i + 1
				break
			}
		}
	}
	for _, e := range append(slices.Clone(p.entries[start:]), p.entries[:start]...) {
		if members[e] {
			return e
		}
	}
	return among[0]
}

// bestRate is the candidate with the highest sampled rate of valid
// responses per second on site. The untried candidates compete as one, by
// the next of them in list order (see Acquire). Lock held.
func (p *Pool) bestRate(site string, candidates []*entry, wait func(string) time.Duration) *entry {
	pr := p.prior(site)
	pause := func(e *entry) float64 {
		if wait == nil {
			return 0
		}
		return secondsOf(max(0, wait(e.url)))
	}

	var best *entry
	bestRate := -1.0
	var untried []*entry
	for _, e := range candidates {
		s := e.stat(site)
		if s.tries == 0 {
			untried = append(untried, e)
			continue
		}
		rate := p.expectedRate(s, pr, pause(e), true)
		if rate > bestRate {
			best, bestRate = e, rate
		}
	}
	if len(untried) > 0 {
		nxt := p.nextInOrder(site, untried)
		if best == nil || p.expectedRate(nxt.stat(site), pr, pause(nxt), true) > bestRate {
			best = nxt
		}
	}
	return best
}

// WaitFor is how long until an endpoint site may use comes off cooldown: 0
// when one is usable now; ok is false when the pool holds none it may use
// at all.
//
// This lets callers wait for a route instead of spending attempts that send
// nothing; Pool.Do uses it for cooldown waits. Both cooldowns
// count, the endpoint-wide one (transport) and the site's own (refusal),
// since either keeps Acquire from handing the endpoint out.
func (p *Pool) WaitFor(site string, allowDirect bool, exclude map[string]bool) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	var waits []time.Duration
	for _, e := range p.entries {
		if e.mayCarry(site, allowDirect) && !exclude[e.url] {
			waits = append(waits, max(0, later(e.cooldownUntil, e.stat(site).cooldownUntil).Sub(now)))
		}
	}
	if len(waits) == 0 {
		return 0, false
	}
	return slices.Min(waits), true
}

// RefusedCooling reports whether a route site may use is cooling right now
// because site refused it (Demote), rather than benched for a failed
// connection (DemoteTransport) or an WAF challenge (Punish): the
// difference the proxy list draws between "kyls ned" and "spärrad …
// (WAF)" (benchOf, whose endpoint-wide benches do not hide a refusal
// here). A site that runs out of routes while one of them is cooling for
// its refusal has been refused into stopping, whatever benched the rest;
// see fetch's HaltRefused. A route on a single-route site that WAF
// challenged was demoted, not punished, and counts.
func (p *Pool) RefusedCooling(site string, allowDirect bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for _, e := range p.entries {
		if s, ok := e.sites[site]; ok && e.mayCarry(site, allowDirect) && s.refusalBench(now) {
			return true
		}
	}
	return false
}

// PathCount counts this scope's routes, including ones cooling down.
func (p *Pool) PathCount(site string, allowDirect bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.entries {
		if e.mayCarry(site, allowDirect) {
			n++
		}
	}
	return n
}

// NewPass starts every site's count of refusals in a row afresh.
//
// That count doubles a route's cooldown per refusal and resets only on a
// served page, and the pool outlives the pass, so a pass whose warm-up was
// refused twice handed the next one, two hours later, a count of two. Its
// first 503 then benched the tunnel for 240 s instead of 60, the warm-up's
// second chance one minute later found no route, and the pass was lost as
// "refused" without a second request: one destination on 2026-10-03, 10-04 and
// 10-05 alike. Two hours is four times the longest cooldown, so the count
// carries nothing a new pass should inherit. What does carry over is kept:
// a cooldown still running is still honoured, and the refusal rate that
// describes the endpoints is evidence, not a streak.
func (p *Pool) NewPass() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.entries {
		for _, s := range e.sites {
			s.fails = 0
		}
	}
}

// ReportOK records that a real page came back through url for site, in an
// attempt that took took (the whole request, body read included). It
// clears the failure state for this site and updates the latency
// measurement. Transport health is cleared too: the endpoint demonstrably
// carries traffic. A url not in the pool is ignored.
//
// The outcome methods log, and persist, under ctx tagged with site.
func (p *Pool) ReportOK(ctx context.Context, site, url string, took time.Duration) {
	ctx = p.scopeContext(ctx, site)
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.find(url)
	if e == nil {
		return
	}
	p.observe(e, site, true, took, p.now())
	e.served++
	e.fails = 0
	e.cooldownUntil = time.Time{}
	e.observeTransport(false)
	s := e.stat(site)
	s.fails = 0
	s.challenges = 0
	s.cooldownUntil = time.Time{}
	s.observe(took)
	s.observeOutcome(false)
	p.persistLogged(ctx, e)
}

// Demote sends a route that site refused to cooldown on that site alone,
// after an attempt that took took. A url not in the pool is ignored.
//
// Refusal is a per-domain verdict: it cools the route for that site and
// leaves the other four using it, since letting `se` evict an endpoint
// everywhere would throw away a path that four other sites are being served
// by perfectly well. The cooldown grows geometrically from
// Settings.BaseCooldown (a minute) per refusal in a row, up to half an hour.
// A failed connection is DemoteTransport's and an WAF challenge
// Punish's.
func (p *Pool) Demote(ctx context.Context, site, url, reason string, took time.Duration) {
	ctx = p.scopeContext(ctx, site)
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.find(url)
	if e == nil {
		return
	}
	now := p.now()
	p.observe(e, site, false, took, now)
	s := e.stat(site)
	s.observeOutcome(true)
	s.fails++
	bench := backoff(p.settings.BaseCooldown, maxCooldown, s.fails)
	s.cooldownUntil = now.Add(bench)
	p.log.InfoContext(ctx, fmt.Sprintf("proxy demoted %.0fs for %s (refusal %d): %s (%s)",
		bench.Seconds(), orQuestion(site), s.fails, Redact(url), reason))
	p.persistLogged(ctx, e)
}

// DemoteTransport benches a route that did not carry the request at all —
// a dead tunnel, a refused connection — after an attempt for site that
// took took. That is endpoint-wide, so it cools the route for every site,
// but the record is kept until age-based cleanup. A url not in the pool is
// ignored.
//
// A failed connection is punished far harder than a refusal, because a
// rotation gives every endpoint off its bench a full turn: on c016fcf's
// first pass one destination spent 429 of its 500 requests and another 396 on
// proxies that never connected, because each came back after a minute. A
// feed entry that has never served a page is a dead entry and sits out
// Settings.DeadCooldown (a day) at once. One that has served, or a static
// endpoint or Direct (the operator's own routes, not a list's guesses),
// sits out Settings.BrokenCooldown (ten minutes), doubling per failure in a
// row up to the same day.
func (p *Pool) DemoteTransport(ctx context.Context, site, url, reason string, took time.Duration) {
	ctx = p.scopeContext(ctx, site)
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.find(url)
	if e == nil {
		return
	}
	now := p.now()
	p.observe(e, site, false, took, now)
	e.observeTransport(true)
	e.fails++
	bench, kind := p.settings.DeadCooldown, "feed entry that never served a page"
	if !e.dead() {
		bench, kind = backoff(p.settings.BrokenCooldown, p.settings.DeadCooldown, e.fails), string(BenchBroken)
	}
	e.cooldownUntil = now.Add(bench)
	p.log.InfoContext(ctx, fmt.Sprintf("proxy benched %.0f min everywhere (%s, transport fail %d): %s (%s)",
		bench.Minutes(), kind, e.fails, Redact(url), reason))
	p.persistLogged(ctx, e)
}

func orQuestion(site string) string {
	if site == "" {
		return "?"
	}
	return site
}

// Punish benches an endpoint on one scope after that site's WAF
// challenged it, in an attempt that took took. It returns the bench, 0 if
// url is not in the pool.
//
// A challenge is the one verdict that is about the route rather than the
// request or the site: the WAF looked at the address and asked it to prove
// it is a browser, which it cannot. On 2026-10-09 seven routes in
// one destination's feed drew it on most requests, pass after pass, while the
// route carrying all 151 of a working destination's pages drew none — and because a
// challenge page is small, it came back fast and had been *raising* those
// routes' share. So the bench is hours rather than the minute a refusal
// earns (Settings.ChallengeCooldown, doubling per challenge in a row up to
// a day), the challenge counts in the site's refusal rate so its history
// remains visible once it is back, and only a page it actually serves
// clears the doubling. Per site: another scope's WAF has not judged
// it. Nothing is dropped — it can still prove it came back.
func (p *Pool) Punish(ctx context.Context, site, url, reason string, took time.Duration) time.Duration {
	ctx = p.scopeContext(ctx, site)
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.find(url)
	if e == nil {
		return 0
	}
	now := p.now()
	p.observe(e, site, false, took, now)
	s := e.stat(site)
	s.observeOutcome(true)
	s.challenges++
	bench := backoff(p.settings.ChallengeCooldown, maxChallengeCooldown, s.challenges)
	s.cooldownUntil = later(s.cooldownUntil, now.Add(bench))
	p.log.InfoContext(ctx, fmt.Sprintf("proxy benched %.0f min for %s (AWS WAF challenge %d in a row): %s (%s)",
		bench.Minutes(), orQuestion(site), s.challenges, Redact(url), reason))
	p.persistLogged(ctx, e)
	return bench
}

// Refresh replaces the endpoint list, preserving health state for
// carry-overs. scopes restricts endpoints (url -> codes; nil = every
// endpoint shared) and feed names the ones that came from a refresh feed.
//
// Returns (added, removed). An endpoint present before and after keeps its
// fail count and cooldown, so a periodic refresh from the same source does
// not reset the cooldown on something that has been failing all along.
func (p *Pool) Refresh(ctx context.Context, urls []string, marketplaces map[string][]string, feed []string) (added, removed int) {
	incoming := uniqueURLs(urls)
	p.mu.Lock()
	defer p.mu.Unlock()
	old := map[string]bool{}
	for _, e := range p.entries {
		old[e.url] = true
	}
	wall := unixSeconds(p.now())
	entries := []*entry{}
	for _, u := range incoming {
		if _, gone := p.retired[fingerprint(u)]; gone {
			continue
		}
		entries = append(entries, p.catalog.getOrAdd(u, wall))
	}
	p.entries = entries
	feedSet := map[string]bool{}
	for _, u := range feed {
		feedSet[u] = true
	}
	for _, e := range p.entries {
		scope := map[string]bool{}
		for _, code := range marketplaces[e.url] {
			scope[code] = true
		}
		e.marketplaces = scope
		e.fromFeed = feedSet[e.url]
	}
	listed := make(map[string]bool, len(incoming))
	for _, u := range incoming {
		listed[u] = true
		if !old[u] {
			added++
		}
	}
	for u := range old {
		if !listed[u] {
			removed++
		}
	}
	p.log.InfoContext(ctx, fmt.Sprintf("proxy pool refreshed: %d endpoints (+%d, -%d)", len(p.entries), added, removed))
	p.persistLogged(ctx, nil)
	return added, removed
}

// Bench is why a route is out of selection on a site; "" when it is not.
type Bench string

// The benches, spelled as /api/proxies and deals.json carry them.
const (
	// BenchDead is a feed entry that has never served a page and failed to
	// connect: out everywhere for a day.
	BenchDead Bench = "dead"
	// BenchBroken is any other route that failed to connect: out everywhere
	// for minutes, doubling.
	BenchBroken Bench = "broken"
	// BenchWAF is a route the site's WAF challenged.
	BenchWAF Bench = "waf"
	// BenchRefused is a route the site refused.
	BenchRefused Bench = "refused"
)

// SiteHealth is one row of Pool.SiteHealth: that scope's own numbers
// for one endpoint — what the report reads as a site's proxy health.
type SiteHealth struct {
	URL              string   `json:"url"` // redacted
	Site             string   `json:"site"`
	Latency          *float64 `json:"latency"` // nil until measured
	Refusal          *float64 `json:"refusal"` // nil until measured
	Fails            int      `json:"fails"`
	Challenges       int      `json:"challenges"`
	TransportFailure float64  `json:"transport_failure"`
	Served           int      `json:"served"`
	// Why it is out, if it is: BenchDead or BenchBroken (a failed
	// connection, endpoint-wide), BenchWAF (an WAF challenge) or
	// BenchRefused (this site); "" when it is not benched.
	Bench   Bench   `json:"bench"`
	Cooling float64 `json:"cooling"`
	// What throughput selection believes: the share of attempts that come
	// back valid, the seconds one takes, and so valid responses per minute.
	Tries          int      `json:"tries"`
	Valid          *float64 `json:"valid"`            // nil when never tried
	AttemptSeconds *float64 `json:"attempt_seconds"`  // nil until an attempt is timed
	ValidPerMinute *float64 `json:"valid_per_minute"` // nil when never tried
}

// SiteBrief is one site's numbers inside a HealthRow.
type SiteBrief struct {
	Latency    *float64 `json:"latency"`
	Refusal    *float64 `json:"refusal"`
	Fails      int      `json:"fails"`
	Challenges int      `json:"challenges"`
	Cooling    float64  `json:"cooling"`
}

// HealthRow is one row of Pool.Health: the endpoint-wide picture plus a
// per-site breakdown.
type HealthRow struct {
	URL              string   `json:"url"` // redacted
	Scopes           []string `json:"marketplaces"`
	Latency          *float64 `json:"latency"`
	Refusal          *float64 `json:"refusal"`
	TransportFails   int      `json:"transport_fails"`
	TransportFailure float64  `json:"transport_failure"`
	Served           int      `json:"served"`
	Cooling          float64  `json:"cooling"`
	// Keyed by scope code (never ""). encoding/json writes it in code
	// order; iterate it in that order with slices.Sorted(maps.Keys(row.Sites)).
	Sites map[string]SiteBrief `json:"sites"`
}

func round2(p *float64) *float64 {
	if p == nil {
		return nil
	}
	return new(roundDecimal(*p, 2))
}

// coolingLeft is the seconds left of the bench running until deadline, to
// a tenth; 0 when none is running at now.
func coolingLeft(deadline, now time.Time) float64 {
	return max(0, roundDecimal(secondsOf(deadline.Sub(now)), 1))
}

// benchOf is why e is out of selection on the site s is its record of, at
// now: a failed connection, endpoint-wide (BenchDead for an entry that is
// dead, BenchBroken otherwise), else an WAF challenge or a refusal on
// that site; "" when it is not benched.
func benchOf(e *entry, s *siteStat, now time.Time) Bench {
	switch {
	case e.cooldownUntil.After(now) && e.dead():
		return BenchDead
	case e.cooldownUntil.After(now):
		return BenchBroken
	case !s.cooldownUntil.After(now):
		return ""
	case s.refusalBench(now):
		return BenchRefused
	default:
		return BenchWAF
	}
}

// Health is one row per endpoint: the endpoint-wide picture plus a per-site
// breakdown.
//
// The breakdown is the point: a single blended refusal rate reads as "this
// endpoint is half-refused" when the truth is "one scope refuses
// everything and the other four are clean".
func (p *Pool) Health() []HealthRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	rows := []HealthRow{}
	for _, e := range p.entries {
		sites := map[string]SiteBrief{}
		for code, s := range e.sites {
			if code == "" {
				continue
			}
			sites[code] = SiteBrief{
				Latency:    round2(s.latency),
				Refusal:    round2(s.refusal),
				Fails:      s.fails,
				Challenges: s.challenges,
				Cooling:    coolingLeft(s.cooldownUntil, now),
			}
		}
		rows = append(rows, HealthRow{
			URL:              Redact(e.url),
			Scopes:           sortedCodes(e.marketplaces),
			Latency:          round2(e.siteMean(latencyOf)),
			Refusal:          round2(e.siteMean(refusalOf)),
			TransportFails:   e.fails,
			TransportFailure: roundDecimal(e.transportFailure, 2),
			Served:           e.served,
			Cooling:          coolingLeft(e.cooldownUntil, now),
			Sites:            sites,
		})
	}
	return rows
}

// SiteHealth is one row per endpoint site may use, with that scope's
// own numbers.
func (p *Pool) SiteHealth(site string) []SiteHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	pr := p.prior(site)
	rows := []SiteHealth{}
	for _, e := range p.entries {
		if !e.supports(site) {
			continue
		}
		s := e.stat(site)
		row := SiteHealth{
			URL:              Redact(e.url),
			Site:             site,
			Latency:          round2(s.latency),
			Refusal:          round2(s.refusal),
			Fails:            s.fails,
			Challenges:       s.challenges,
			TransportFailure: roundDecimal(e.transportFailure, 2),
			Served:           e.served,
			Bench:            benchOf(e, s, now),
			Cooling:          coolingLeft(later(s.cooldownUntil, e.cooldownUntil), now),
			Tries:            s.tries,
			AttemptSeconds:   round2(s.duration),
		}
		if s.tries != 0 {
			row.Valid = new(roundDecimal((pr.A+s.ok)/(pr.A+pr.B+s.ok+s.bad), 2))
			row.ValidPerMinute = new(roundDecimal(60*p.expectedRate(s, pr, 0.0, false), 1))
		}
		rows = append(rows, row)
	}
	return rows
}
