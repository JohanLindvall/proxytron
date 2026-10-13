package proxytron

import (
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/logtest"
	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// A panic inside either of refreshSources' locked sections is recovered
// above it (refreshTick on the timer path, the scan runner on the scan
// path), so it must not leave pool.mu held, nor the Live's lock: the tick
// logs it, and the pool keeps serving scans and /api/proxies.
func TestRefreshSourcesReleasesThePoolLockAfterARecoveredPanic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		panicAt int32 // which clock reading inside the tick panics
		due     bool  // whether the feed is due, so the collector runs
	}{
		{"first section, the clock read after activate", 1, false},
		{"collector, the attempted_at stamp of a due feed", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &panicClock{Clock: newClock()}
			get := stubFeeds(feedText("socks5://10.1.1.1:1080\nsocks5://10.1.1.2:1080\n"))
			live := newLive(t, WithClock(clock), get.option())
			cfg := proxyCfg(t, withFeedURL("https://feeds.test/list.txt"),
				withRefreshInterval(60), withMinReady(0))
			app := *fixtureApp()
			app.Proxies = cfg
			codes := make([]string, 0, len(app.Scopes))
			for _, m := range app.Scopes {
				codes = append(codes, m.Code)
			}
			pool := livePool(t, live, &cfg, codes, "")
			if tc.due {
				clock.Advance(time.Hour) // refresh_minutes = 60
			}
			fetched := get.count()

			clock.at.Store(tc.panicAt)
			rec := logtest.Capture(t, slog.LevelError)
			live.refreshTick(t.Context(), func() (*Config, []string, error) { return &app.Proxies, app.Codes(), nil }, "")
			clock.at.Store(0)

			if !clock.panicked.Load() {
				t.Fatalf("the injected panic never fired (%d clock reads)", clock.calls.Load())
			}
			// One line naming the panic, its stack under it.
			if got := rec.Entries(); len(got) != 1 || got[0].Message != refreshFailed+"injected" ||
				!strings.Contains(got[0].Stack, "refreshTick") {
				t.Errorf("logged %+v", got)
			}
			if got := get.count() > fetched; got != tc.due {
				t.Fatalf("feed fetched during the tick = %v, want %v: the panic was not where the case says",
					got, tc.due)
			}
			if !pool.mu.TryLock() {
				t.Fatal("pool.mu is still held after refreshTick recovered the panic: " +
					"every later Acquire, Health and Live.Pool would block forever")
			}
			pool.mu.Unlock()
			if !live.mu.TryLock() {
				t.Fatal("the Live's lock is still held after refreshTick recovered the panic")
			}
			live.mu.Unlock()

			// The pool keeps serving, and the next tick runs.
			done := make(chan string, 1)
			go func() {
				live.refreshTick(t.Context(), func() (*Config, []string, error) { return &app.Proxies, app.Codes(), nil }, "")
				done <- pool.Acquire("se", true, nil, nil)
			}()
			select {
			case got := <-done:
				if got == "" {
					t.Error("Acquire(se) = none after the recovered panic, want an endpoint")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the next refresh tick and acquire(se) blocked")
			}
		})
	}
}

// panicClock reads clock until armed with at: then its at-th reading
// panics, inside whichever locked section of the refresh asked for the time.
type panicClock struct {
	*testutil.Clock
	at, calls atomic.Int32
	panicked  atomic.Bool
}

func (c *panicClock) Now() time.Time {
	if at := c.at.Load(); at > 0 && c.calls.Add(1) == at {
		c.panicked.Store(true)
		panic("injected")
	}
	return c.Clock.Now()
}
