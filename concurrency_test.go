package proxytron

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/testutil"
)

// The marketplace goroutines share one pool while the server's timer
// refreshes its feeds and the API replaces its list: every outcome, read,
// refresh and replacement may meet any other, with the database attached.
// Run under -race, this is the check that every path takes the pool's lock
// (and the Live's before it); afterwards no outcome on a route that stayed
// listed is lost, in memory or in the database.
func TestPool_ConcurrentOutcomesRefreshesAndReads(t *testing.T) {
	const stable = "http://stable.example:80" // listed by every feed and every replacement
	clock := newClock()
	var downloads atomic.Int64
	feed := stubFeeds(func(string) (string, error) {
		n := downloads.Add(1)
		return fmt.Sprintf(`[%q, "http://f%d.example:80", "http://f%d.example:80"]`, stable, n%4, (n+1)%4), nil
	})
	cfg := proxyCfg(t, withFeedURL("https://feeds.test/all.json"), withRefreshInterval(1), withMinReady(0))
	codes := []string{"se", "de"}
	path := filepath.Join(t.TempDir(), "amzscrape.sqlite3")
	live := newLive(t, WithClock(clock), feed.option(), withRand(seeded(7)))
	pool := testutil.Must(live.Pool(t.Context(), &cfg, codes, path))(t)

	var served atomic.Int64 // pages reported served through stable
	report := func(url, site string, took time.Duration) {
		pool.ReportOK(t.Context(), site, url, took)
		if url == stable {
			served.Add(1)
		}
	}
	var wg sync.WaitGroup
	for _, site := range codes {
		wg.Go(func() {
			for i := range 150 {
				url := pool.Acquire(site, false, nil, func(string) time.Duration { return time.Second })
				took := time.Duration(i%7) * 100 * time.Millisecond
				switch i % 6 {
				case 0, 1:
					report(url, site, took)
				case 2:
					pool.Demote(t.Context(), site, url, "HTTP 503", took)
				case 3:
					pool.DemoteTransport(t.Context(), site, url, "connection refused", took)
				case 4:
					pool.Punish(t.Context(), site, url, "AWS WAF challenge", took)
				default:
					report(stable, site, took)
				}
				pool.WaitFor(site, false, nil)
				pool.PathCount(site, false)
				pool.SiteHealth(site)
			}
		})
	}
	edited := cfg // an edit of [proxies] ends an API override
	edited.RefreshInterval = 2 * time.Minute
	wg.Go(func() { // the server's timer: a feed refresh each time one is due
		for i := range 20 {
			clock.Advance(2 * time.Minute)
			if _, err := live.Pool(t.Context(), []*Config{&cfg, &edited}[i%2], codes, path); err != nil {
				t.Errorf("Live.Pool: %v", err)
			}
		}
	})
	wg.Go(func() { // POST /api/proxies, and a list replaced in place
		for i := range 20 {
			urls := []string{stable, fmt.Sprintf("http://api%d.example:80", i%3)}
			if i%2 == 0 {
				pool.Refresh(t.Context(), urls, nil, nil)
			} else if _, err := live.Override(t.Context(), urls, &cfg, codes); err != nil {
				t.Errorf("Live.Override: %v", err)
			}
		}
	})
	wg.Go(func() { // the status endpoints, and the next pass starting
		for range 50 {
			pool.Health()
			pool.Len()
			live.Current()
			pool.NewPass()
		}
	})
	wg.Wait()

	if got := healthRows(pool.Health())[stable].Served; int64(got) != served.Load() {
		t.Errorf("stable served %d pages in memory, want the %d reported", got, served.Load())
	}
	testutil.NoErr(t, live.Close())
	stored := storedProxyRows(t, path)[stable]
	if got, ok := stored["served"].(float64); !ok || int64(got) != served.Load() {
		t.Errorf("stable served %v pages in the database, want the %d reported", stored["served"], served.Load())
	}
}
