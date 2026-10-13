package proxytron

// The stored forms of the pool's state, and the conversions at that
// boundary.
//
// Every row of the proxies table is an entryRecord encoded with
// encoding/json, a proxy_sources row is a listing, and meta 'proxy:first'
// is the learned prior, a map of firstStat. Decoding starts from a record
// filled with the defaults, so a key a row lacks keeps its default; a key of
// the wrong type is an error. The keys are the ones every earlier writer
// used, and counters are written as integers, so rows written here are
// still read by the code they replace.
//
// In memory a bench is a time.Time deadline. It is stored as the
// wall-clock moment it ends, in Unix seconds (0 when none is running), so a
// bench survives a restart: deadlineFromUntil and untilFromDeadline convert
// at the boundary.

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// entryRecord is one row of the proxies table: an endpoint's whole state.
type entryRecord struct {
	Egress           *Egress                    `json:"egress,omitempty"`
	URL              string                     `json:"url"`
	Scopes           []string                   `json:"marketplaces"`
	Fails            int                        `json:"fails"`
	TransportFailure float64                    `json:"transport_failure"`
	Sites            map[string]siteRecord      `json:"sites"`
	Served           int                        `json:"served"`
	FromFeed         bool                       `json:"from_feed"`
	FirstSeen        float64                    `json:"first_seen"`
	LastSeen         float64                    `json:"last_seen"`
	LastTested       *float64                   `json:"last_tested"`
	LastSuccess      *float64                   `json:"last_success"`
	Origins          [][]string                 `json:"origins"` // [feed URL, marketplace] pairs
	Metadata         map[string]json.RawMessage `json:"metadata"`
	Until            float64                    `json:"until"`
}

// siteRecord is what one endpoint has done on one scope. Score is
// written for forensic queries and never read: selection recomputes it.
type siteRecord struct {
	Latency    *float64 `json:"latency"`
	Refusal    *float64 `json:"refusal"`
	Fails      int      `json:"fails"`
	Challenges int      `json:"challenges"`
	OK         float64  `json:"ok"`
	Bad        float64  `json:"bad"`
	Tries      int      `json:"tries"`
	Duration   *float64 `json:"duration"`
	Until      float64  `json:"until"`
	Score      *float64 `json:"score"` // nil until the endpoint is tried there
}

// listing is a feed's last download, as proxy_sources stores it: the
// endpoints it listed, each with its record as the feed gave it ({} for a
// bare URL), when it last succeeded (nil: never) and when it was last
// attempted.
type listing struct {
	Members     map[string]json.RawMessage `json:"members"`
	FetchedAt   *float64                   `json:"fetched_at"`
	AttemptedAt *float64                   `json:"attempted_at"`
}

// firstStat is one site's record of the first attempt ever made through
// each endpoint there: what an untried endpoint is expected to do (see
// Pool.prior). It is stored as the array [attempts, valid, seconds, timed].
type firstStat struct {
	Attempts int
	Valid    float64
	Seconds  float64
	Timed    int
}

// MarshalJSON writes the stat as [attempts, valid, seconds, timed].
func (f firstStat) MarshalJSON() ([]byte, error) {
	return json.Marshal([4]any{f.Attempts, finiteOr0(f.Valid), finiteOr0(f.Seconds), f.Timed})
}

// UnmarshalJSON reads [attempts, valid, seconds, timed]; any other shape is
// an error, and null leaves the stat as it was.
func (f *firstStat) UnmarshalJSON(b []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(b, &parts); err != nil {
		return err
	}
	if parts == nil { // null
		return nil
	}
	if len(parts) != 4 {
		return fmt.Errorf("first-attempt stat: want [attempts, valid, seconds, timed], got %d values", len(parts))
	}
	var s firstStat
	for i, dst := range []any{&s.Attempts, &s.Valid, &s.Seconds, &s.Timed} {
		if err := json.Unmarshal(parts[i], dst); err != nil {
			return fmt.Errorf("first-attempt stat: %w", err)
		}
	}
	*f = s
	return nil
}

// legacyRow is one element of the pre-persistent proxy-memory.json: an
// endpoint's benches and record, or, with no URL, the sites' first-attempt
// priors. Every shape the file was written in reads into it: the oldest kept
// only benches in Sites, the last added records and the First row.
type legacyRow struct {
	URL    string                     `json:"url"`
	Served int                        `json:"served"`
	Fails  int                        `json:"fails"`
	Until  float64                    `json:"until"`
	Sites  map[string]legacySite      `json:"sites"`
	First  map[string]json.RawMessage `json:"first"`
}

// legacySite is one site's part of a legacyRow: an WAF bench still
// running (Challenges, Until), a record (OK, Bad, Tries, Duration), or both.
type legacySite struct {
	Challenges int      `json:"challenges"`
	Until      float64  `json:"until"`
	OK         float64  `json:"ok"`
	Bad        float64  `json:"bad"`
	Tries      int      `json:"tries"`
	Duration   *float64 `json:"duration"`
}

// -- benches and wall-clock time ----------------------------------------------

// maxStoredBench bounds a bench a stored end puts back: far beyond any the
// pool hands out (a day), and well inside what a time.Duration holds.
const maxStoredBench = 100 * 365 * day

// unixSeconds is t as the Unix seconds the records store.
func unixSeconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// deadlineFromUntil is the bench a stored wall-clock end puts back at now:
// the zero Time, no bench, when that end has passed.
func deadlineFromUntil(now time.Time, until float64) time.Time {
	left := until - unixSeconds(now)
	if !(left > 0) {
		return time.Time{}
	}
	return now.Add(durationOf(min(left, secondsOf(maxStoredBench))))
}

// untilFromDeadline is the wall-clock end of the bench running until
// deadline, as it is stored: 0 when none is running at now.
func untilFromDeadline(now, deadline time.Time) float64 {
	if !deadline.After(now) {
		return 0
	}
	return unixSeconds(now) + secondsOf(deadline.Sub(now))
}

// later is whichever of a and b is later.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// -- writing ------------------------------------------------------------------

// finiteOr0 is x, or 0 for NaN and the infinities, which JSON cannot
// carry: a record that failed to encode would never be saved again.
func finiteOr0(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return x
}

// finiteOrNil is p, or nil when *p is NaN or infinite.
func finiteOrNil(p *float64) *float64 {
	if p == nil || math.IsNaN(*p) || math.IsInf(*p, 0) {
		return nil
	}
	return p
}

// record is e's stored form at now. Lock held: the pool lock keeps the
// metadata a feed refresh adds to from changing underneath it.
func (p *Pool) record(e *entry, now time.Time) entryRecord {
	sites := make(map[string]siteRecord, len(e.sites))
	for code, s := range e.sites {
		r := siteRecord{
			Latency:    finiteOrNil(s.latency),
			Refusal:    finiteOrNil(s.refusal),
			Fails:      s.fails,
			Challenges: s.challenges,
			OK:         finiteOr0(s.ok),
			Bad:        finiteOr0(s.bad),
			Tries:      s.tries,
			Duration:   finiteOrNil(s.duration),
			Until:      untilFromDeadline(now, s.cooldownUntil),
		}
		if s.tries != 0 {
			r.Score = finiteOrNil(new(p.expectedRate(s, p.prior(code), 0, false)))
		}
		sites[code] = r
	}
	origins := make([][]string, 0, len(e.origins))
	for _, o := range sortedSources(e.origins) {
		origins = append(origins, []string{o.url, o.site})
	}
	var egress *Egress
	if e.egress.IP.IsValid() {
		v := e.egress
		egress = &v
	}
	return entryRecord{
		Egress:           egress,
		URL:              e.url,
		Scopes:           sortedCodes(e.marketplaces),
		Fails:            e.fails,
		TransportFailure: finiteOr0(e.transportFailure),
		Sites:            sites,
		Served:           e.served,
		FromFeed:         e.fromFeed,
		FirstSeen:        finiteOr0(e.firstSeen),
		LastSeen:         finiteOr0(e.lastSeen),
		LastTested:       finiteOrNil(e.lastTested),
		LastSuccess:      finiteOrNil(e.lastSuccess),
		Origins:          origins,
		Metadata:         e.metadata,
		Until:            untilFromDeadline(now, e.cooldownUntil),
	}
}

// -- reading ------------------------------------------------------------------

// decodeEntry rebuilds an endpoint from its stored record at now. A key the
// record lacks keeps its default (first and last seen now); a record that
// is not an object, lacks its URL or holds a value of the wrong type is an
// error.
func decodeEntry(raw json.RawMessage, now time.Time) (*entry, error) {
	wall := unixSeconds(now)
	r := entryRecord{FirstSeen: wall, LastSeen: wall}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.URL == "" {
		return nil, errNoURL
	}
	e := newEntry(r.URL, wall)
	if r.Egress != nil {
		e.egress = *r.Egress
	}
	for _, code := range r.Scopes {
		e.marketplaces[code] = true
	}
	e.fails, e.transportFailure, e.served, e.fromFeed = r.Fails, r.TransportFailure, r.Served, r.FromFeed
	e.firstSeen, e.lastSeen, e.lastTested, e.lastSuccess = r.FirstSeen, r.LastSeen, r.LastTested, r.LastSuccess
	e.cooldownUntil = deadlineFromUntil(now, r.Until)
	// Only a (feed URL, scope) pair can name a source.
	for _, o := range r.Origins {
		if len(o) == 2 {
			e.origins[source{url: o[0], site: o[1]}] = true
		}
	}
	if r.Metadata != nil {
		e.metadata = r.Metadata
	}
	for code, s := range r.Sites {
		e.sites[code] = &siteStat{
			latency:       s.Latency,
			refusal:       s.Refusal,
			fails:         s.Fails,
			challenges:    s.Challenges,
			cooldownUntil: deadlineFromUntil(now, s.Until),
			ok:            s.OK,
			bad:           s.Bad,
			tries:         s.Tries,
			duration:      s.Duration,
		}
	}
	return e, nil
}
