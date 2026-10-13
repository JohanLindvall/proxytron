package proxytron

import (
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// outcomes is a pool after one of each outcome on two routes, under a
// clock at Unix 1760010000.5: a feed route served twice on se and refused
// once on de, a static one challenged on se and failed to connect on it.
func outcomes(t *testing.T) *Pool {
	t.Helper()
	const a, b = "http://a:80", "socks5h://u:p@b:1080"
	clock := testutil.NewClock(time.Unix(1_760_010_000, 500_000_000))
	pool := testFromURLs([]string{a, b}, WithClock(clock))
	pool.Refresh(t.Context(), []string{a, b}, map[string][]string{a: {"se", "de"}}, []string{a})
	pool.ReportOK(t.Context(), "se", a, 1250*time.Millisecond)
	pool.Demote(t.Context(), "de", a, "x", 500*time.Millisecond)
	pool.ReportOK(t.Context(), "se", a, 750*time.Millisecond)
	pool.Punish(t.Context(), "se", b, "waf", 300*time.Millisecond)
	pool.DemoteTransport(t.Context(), "it", b, "dead", 10*time.Second)
	pool.Acquire("fr", true, nil, nil) // creates no record: neither route may carry fr now
	e := pool.find(a)
	e.origins[source{"https://f/SE.json", "se"}] = true
	e.origins[source{"https://f/DE.json", "de"}] = true
	e.metadata["https://f/SE.json"] = json.RawMessage(
		`{"proxy": "http://a:80", "score": 1, "geo": {"country": "SE"}, "x": 1.5, "u": "\u00f6"}`)
	return pool
}

// The record of every field an outcome changes, the learned prior and both
// health views, by value. The golden values are the ones the service has
// always stored for this sequence; the stored rows are read by whichever
// version runs next, so they must not drift.
func TestRecord_StoresEveryOutcome(t *testing.T) {
	pool := outcomes(t)
	var records []entryRecord
	for _, e := range pool.entries {
		records = append(records, pool.record(e, pool.now()))
	}
	got := asJSON(t, map[string]any{
		"records":        records,
		"first":          pool.first,
		"health":         pool.Health(),
		"site_health_se": pool.SiteHealth("se"),
	})
	path := filepath.Join("testdata", "outcomes.golden.json")
	testutil.WriteGolden(t, path, got)
	if want := testutil.ReadGolden(t, path); !reflect.DeepEqual(jsonValue(t, got), jsonValue(t, want)) {
		t.Errorf("stored state:\n got %s\nwant %s", got, want)
	}
}

// What is written reads back as the same entry: every field, every bench
// and every site record, whatever the clock reads by then.
func TestRecord_RoundTrips(t *testing.T) {
	pool := outcomes(t)
	now := pool.now()
	for _, e := range pool.entries {
		text, err := json.Marshal(pool.record(e, now))
		if err != nil {
			t.Fatal(err)
		}
		back, err := decodeEntry(text, now)
		if err != nil {
			t.Fatalf("%s: %v", e.url, err)
		}
		got, want := asJSON(t, pool.record(back, now)), asJSON(t, pool.record(e, now))
		if !reflect.DeepEqual(jsonValue(t, got), jsonValue(t, want)) {
			t.Errorf("%s:\n got %s\nwant %s", e.url, got, want)
		}
		for code, s := range e.sites {
			if got := back.sites[code].cooldownUntil; !got.Equal(s.cooldownUntil) {
				t.Errorf("%s on %s: bench ends %v, want %v", e.url, code, got, s.cooldownUntil)
			}
		}
		if !back.cooldownUntil.Equal(e.cooldownUntil) {
			t.Errorf("%s: bench ends %v, want %v", e.url, back.cooldownUntil, e.cooldownUntil)
		}
	}
}

// Counters are written as integers, so a reader that takes them as such
// reads the rows back.
func TestRecord_CountsAreIntegers(t *testing.T) {
	pool := outcomes(t)
	text, err := json.Marshal(pool.record(pool.find("http://a:80"), pool.now()))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"fails":0,`, `"served":2,`, `"tries":2,`, `"fails":1,`, `"challenges":0,`} {
		if !strings.Contains(string(text), want) {
			t.Errorf("%s lacks %s", text, want)
		}
	}
	first, err := json.Marshal(pool.first)
	if err != nil {
		t.Fatal(err)
	}
	if want := `"se":[2,1,1.55,2]`; !strings.Contains(string(first), want) {
		t.Errorf("first = %s, want it to hold %s", first, want)
	}
}

// JSON cannot carry NaN or an infinity, and a record that fails to encode
// would never be saved again: such a figure is stored as nothing (an
// optional one) or 0.
func TestRecord_NonFiniteFiguresAreStoredAsNothing(t *testing.T) {
	pool := testFromURLs([]string{"http://a:80"}, WithClock(newClock()))
	e := pool.entries[0]
	e.transportFailure, e.lastTested = math.NaN(), new(math.Inf(1))
	s := e.stat("se")
	s.latency, s.duration, s.ok, s.bad, s.tries = new(math.NaN()), new(math.Inf(-1)), math.Inf(1), math.NaN(), 1
	pool.first["se"] = firstStat{Attempts: 1, Valid: math.NaN(), Seconds: math.Inf(1), Timed: 1}
	text, err := json.Marshal(pool.record(e, pool.now()))
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	r := jsonValue(t, text).(map[string]any)
	se := object(object(r, "sites"), "se")
	for key, want := range map[string]any{"transport_failure": 0.0, "last_tested": nil} {
		if r[key] != want {
			t.Errorf("%s = %v, want %v", key, r[key], want)
		}
	}
	for key, want := range map[string]any{"latency": nil, "duration": nil, "ok": 0.0, "bad": 0.0, "score": nil} {
		if se[key] != want {
			t.Errorf("sites.se.%s = %v, want %v", key, se[key], want)
		}
	}
	if first, err := json.Marshal(pool.first); err != nil || string(first) != `{"se":[1,0,0,1]}` {
		t.Errorf("first = %s, %v; want {\"se\":[1,0,0,1]}", first, err)
	}
}

func TestFirstStat_IsAFourTuple(t *testing.T) {
	var f firstStat
	if err := json.Unmarshal([]byte(`[26, 3.0, 110.77254338708008, 26]`), &f); err != nil {
		t.Fatal(err)
	}
	if want := (firstStat{26, 3, 110.77254338708008, 26}); f != want {
		t.Errorf("decoded %+v, want %+v", f, want)
	}
	for _, bad := range []string{`[1, 2, 3]`, `[1, 2, 3, 4, 5]`, `{"attempts": 1}`, `[1.5, 1, 1, 1]`, `["1", 1, 1, 1]`, `null`} {
		f := firstStat{Attempts: 9}
		if err := json.Unmarshal([]byte(bad), &f); err == nil && bad != `null` {
			t.Errorf("%s decoded as %+v, want an error", bad, f)
		} else if f.Attempts != 9 {
			t.Errorf("%s changed the stat to %+v", bad, f)
		}
	}
}

// A bench is a deadline in memory and the wall-clock moment it ends on
// disk; the conversions are inverses for any bench still running.
func TestDeadlineFromUntil(t *testing.T) {
	now := time.Unix(1_760_010_000, 500_000_000)
	for _, c := range []struct {
		name  string
		until float64
		want  time.Time
	}{
		{"a minute ahead", 1_760_010_060.5, now.Add(time.Minute)},
		{"a day ahead", 1_760_096_400.5, now.Add(day)},
		{"just ended", 1_760_010_000.5, time.Time{}},
		{"long ago", 1_000, time.Time{}},
		{"none", 0, time.Time{}},
		{"not a number", math.NaN(), time.Time{}},
		{"forever, bounded", math.Inf(1), now.Add(maxStoredBench)},
		{"absurdly far, bounded", 1e300, now.Add(maxStoredBench)},
	} {
		got := deadlineFromUntil(now, c.until)
		if !got.Equal(c.want) {
			t.Errorf("%s: deadlineFromUntil(%v) = %v, want %v", c.name, c.until, got, c.want)
		}
		if got.IsZero() {
			if back := untilFromDeadline(now, got); back != 0 {
				t.Errorf("%s: untilFromDeadline = %v, want 0", c.name, back)
			}
		} else if back := untilFromDeadline(now, got); c.until < 1e12 && back != c.until {
			t.Errorf("%s: untilFromDeadline = %v, want %v back", c.name, back, c.until)
		}
	}
	if got := untilFromDeadline(now, now.Add(-time.Second)); got != 0 {
		t.Errorf("a bench that has run: until = %v, want 0", got)
	}
}
