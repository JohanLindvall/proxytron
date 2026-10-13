package proxytron

import (
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/logtest"
)

func TestIsDirect(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bool
	}{
		{"direct", true},
		{"DIRECT", true},
		{" Direct\t", true},
		{"\ndirect\n", true},
		{"", false},
		{"directly", false},
		{"socks5h://direct:1080", false},
		{"D\u0130RECT", false}, // a dotted capital İ does not fold to i
	} {
		if got := IsDirect(c.in); got != c.want {
			t.Errorf("IsDirect(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestPool_LenIsNilSafe(t *testing.T) {
	var none *Pool
	if n := none.Len(); n != 0 {
		t.Errorf("nil pool: Len() = %d, want 0", n)
	}
	if n := testNew().Len(); n != 0 {
		t.Errorf("testNew(): Len() = %d, want 0", n)
	}
	if n := testFromURLs([]string{"http://a:80", " http://a:80 ", "", "http://b:80"}).Len(); n != 2 {
		t.Errorf("blanks and repeats dropped: Len() = %d, want 2", n)
	}
}

func TestSettingsFrom(t *testing.T) {
	defaults := DefaultConfig()
	want := Settings{
		BaseCooldown:      time.Minute,
		ChallengeCooldown: time.Hour,
		BrokenCooldown:    10 * time.Minute,
		DeadCooldown:      24 * time.Hour,
		Selection:         SelectionThroughput,
	}
	if got := SettingsFrom(&defaults); got != want {
		t.Errorf("SettingsFrom(defaults) = %+v, want %+v", got, want)
	}
	if got := testNew().settings; got != want {
		t.Errorf("testNew() settings = %+v, want those of an empty [proxies]: %+v", got, want)
	}
	edited := defaults
	edited.BaseCooldown = 500 * time.Millisecond
	edited.ChallengeCooldown = 90 * time.Second
	edited.BrokenCooldown = 15 * time.Second
	edited.DeadCooldown = 2 * time.Hour
	edited.Selection = SelectionRoundRobin
	want = Settings{
		BaseCooldown:      500 * time.Millisecond,
		ChallengeCooldown: 90 * time.Second,
		BrokenCooldown:    15 * time.Second,
		DeadCooldown:      2 * time.Hour,
		Selection:         SelectionRoundRobin,
	}
	if got := SettingsFrom(&edited); got != want {
		t.Errorf("SettingsFrom(edited) = %+v, want %+v", got, want)
	}
	if got := testNew(WithSettings(want)).settings; got != want {
		t.Errorf("WithSettings: %+v, want %+v", got, want)
	}
}

func TestWithClock_NilKeepsTheSystemClock(t *testing.T) {
	if _, ok := testNew(WithClock(nil)).clock.(systemClock); !ok {
		t.Error("WithClock(nil) replaced the system clock")
	}
	clock := newClock()
	if got := testNew(WithClock(clock)).clock; got != clock {
		t.Errorf("WithClock(c): the pool reads %v", got)
	}
}

// WaitFor is how long until a route the site may use is back: both
// cooldowns count, the site's own and the endpoint-wide one; exclude and a
// barred home address leave routes out, and none left at all is ok=false.
func TestPool_WaitFor(t *testing.T) {
	const a, b = "http://a:80", "http://b:80"
	clock := newClock()
	pool := testFromURLs([]string{Direct, a, b}, WithClock(clock))
	pool.Refresh(t.Context(), []string{Direct, a, b}, map[string][]string{b: {"de"}}, nil)
	type want struct {
		wait time.Duration
		ok   bool
	}
	for _, step := range []struct {
		name        string
		do          func()
		site        string
		allowDirect bool
		exclude     map[string]bool
		want        want
	}{
		{"a route is ready", nil, "se", false, nil, want{0, true}},
		{"no route the site may use", nil, "se", false, set(a), want{0, false}},
		{"a foreign route is not the site's", nil, "it", false, set(a), want{0, false}},
		{"a refusal cools the site's only route",
			func() { pool.Demote(t.Context(), "se", a, "HTTP 503", time.Second) },
			"se", false, nil, want{time.Minute, true}},
		{"the home address, where allowed, is ready", nil, "se", true, nil, want{0, true}},
		{"another site is not cooled by se's refusal", nil, "fr", false, nil, want{0, true}},
		{"time passes", func() { clock.Advance(15 * time.Second) }, "se", false, nil, want{45 * time.Second, true}},
		{"a failed connection cools it everywhere, the longer bench counts",
			func() { pool.DemoteTransport(t.Context(), "se", a, "network error", 10*time.Second) },
			"fr", false, nil, want{10 * time.Minute, true}},
		{"the soonest route counts", nil, "de", false, nil, want{0, true}},
		{"both benches over", func() { clock.Advance(10 * time.Minute) }, "se", false, nil, want{0, true}},
	} {
		if step.do != nil {
			step.do()
		}
		wait, ok := pool.WaitFor(step.site, step.allowDirect, step.exclude)
		if got := (want{wait, ok}); got != step.want {
			t.Errorf("%s: WaitFor(%s) = %+v, want %+v", step.name, step.site, got, step.want)
		}
	}
}

// An outcome for a URL the pool does not hold (a refresh dropped it
// meanwhile) changes nothing and logs nothing.
func TestPool_OutcomesForAURLNotInThePoolAreIgnored(t *testing.T) {
	pool := testFromURLs([]string{"http://a:80"}, WithClock(newClock()))
	before := pool.SiteHealth("se")
	rec := logtest.Capture(t, slog.LevelDebug)
	gone := "http://gone:80"
	pool.ReportOK(t.Context(), "se", gone, time.Second)
	pool.Demote(t.Context(), "se", gone, "HTTP 503", time.Second)
	pool.DemoteTransport(t.Context(), "se", gone, "network error", time.Second)
	if d := pool.Punish(t.Context(), "se", gone, "AWS WAF challenge", time.Second); d != 0 {
		t.Errorf("Punish = %v, want 0", d)
	}
	if got := rec.Messages("amzscrape.proxy"); len(got) != 0 {
		t.Errorf("logged %q", got)
	}
	if got := pool.SiteHealth("se"); !reflect.DeepEqual(got, before) {
		t.Errorf("health changed: %+v, was %+v", got, before)
	}
}

// The outcome methods time an attempt with a Duration; the pool keeps
// seconds, and a negative one is no measurement.
func TestPool_AnOutcomeIsTimedByItsDuration(t *testing.T) {
	const a = "http://a:80"
	pool := testFromURLs([]string{a}, WithClock(newClock()), baseCooldown(0))
	pool.ReportOK(t.Context(), "se", a, 1250*time.Millisecond)
	pool.Demote(t.Context(), "se", a, "HTTP 503", -time.Second)
	row := pool.SiteHealth("se")[0]
	if row.Tries != 2 || row.AttemptSeconds == nil || *row.AttemptSeconds != 1.25 {
		t.Errorf("(tries, attempt_seconds) = (%d, %s), want (2, 1.25)", row.Tries, show(row.AttemptSeconds))
	}
	if row.Latency == nil || *row.Latency != 1.25 {
		t.Errorf("latency = %s, want 1.25", show(row.Latency))
	}
	if got, want := pool.first["se"], (firstStat{Attempts: 1, Valid: 1, Seconds: 1.25, Timed: 1}); got != want {
		t.Errorf("first attempt = %+v, want %+v", got, want)
	}
}

// A pool reads its benches against its clock wherever that is set (here in
// 2001): a route with no bench is available, and one benched at that time
// is out until its bench has run.
func TestPool_BenchesFollowItsClock(t *testing.T) {
	const a = "http://a:80"
	clock := newClock()
	clock.Set(time.Date(2001, 9, 9, 1, 46, 40, 0, time.UTC))
	pool := testFromURLs([]string{a}, WithClock(clock))
	if got := pool.Acquire("se", true, nil, nil); got != a {
		t.Fatalf("acquire = %q, want %s", got, a)
	}
	pool.DemoteTransport(t.Context(), "se", a, "network error", 0)
	if got, want := pool.find(a).cooldownUntil, clock.Now().Add(10*time.Minute); !got.Equal(want) {
		t.Errorf("bench ends %v, want %v", got, want)
	}
	clock.Advance(10*time.Minute - time.Second)
	if got := pool.Acquire("se", true, nil, nil); got != "" {
		t.Errorf("acquire a second early = %q, want none", got)
	}
	clock.Advance(time.Second)
	if got := pool.Acquire("se", true, nil, nil); got != a {
		t.Errorf("acquire once the bench has run = %q, want %s", got, a)
	}
}

func TestDurationOfAndSecondsOfRoundTrip(t *testing.T) {
	for _, s := range []float64{0, 0.3, 0.05, 1.25, 2.5, 59.999999999, 86400, 1e-9, -1.5} {
		if got := secondsOf(durationOf(s)); got != s {
			t.Errorf("secondsOf(durationOf(%v)) = %v", s, got)
		}
	}
	if got := durationOf(0.0000000004); got != 0 {
		t.Errorf("durationOf(0.4 ns) = %v, want the nearest nanosecond, 0", got)
	}
}
