package proxytron_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
	"github.com/JohanLindvall/proxytron/internal/testutil"
)

func requestPool(t *testing.T, endpoints ...string) (*proxytron.Pool, *testutil.Clock) {
	t.Helper()
	clock := testutil.NewClock(time.Unix(100, 0))
	p := proxytron.FromURLs(endpoints, proxytron.WithClock(clock), proxytron.WithSettings(proxytron.Settings{
		Selection: proxytron.SelectionRoundRobin, BaseCooldown: time.Second,
		ChallengeCooldown: time.Hour, BrokenCooldown: time.Minute, DeadCooldown: time.Hour,
	}))
	t.Cleanup(func() { _ = p.Close() })
	return p, clock
}

func TestDoRouteFailuresDoNotSpendOperationAttempts(t *testing.T) {
	p, clock := requestPool(t, "http://a.test:80", "http://b.test:80", "http://c.test:80")
	broken := errors.New("broken")
	var routes []string
	err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{Attempts: 1, Sleep: clock.Sleep,
		Backoff: func(int) time.Duration { t.Fatal("route failure backed off"); return 0 },
	}, func(ctx context.Context, route string) proxytron.AttemptResult {
		// The callback can use pool APIs: it does not run under a pool lock.
		_ = p.Health()
		routes = append(routes, route)
		clock.Advance(time.Second)
		if len(routes) < 3 {
			return proxytron.AttemptResult{Outcome: proxytron.TransportFailed, Err: broken, Retry: true, RouteFailure: true}
		}
		return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
	})
	if err != nil || !slices.Equal(routes, []string{"http://a.test:80", "http://b.test:80", "http://c.test:80"}) {
		t.Fatalf("routes %v, error %v", routes, err)
	}
	rows := p.SiteHealth("catalog")
	if rows[0].Bench != proxytron.BenchBroken || rows[1].Bench != proxytron.BenchBroken || rows[2].Served != 1 {
		t.Fatalf("outcomes: %+v", rows)
	}
	if got := p.Acquire("other", false, nil, nil); got != "http://c.test:80" {
		t.Fatalf("broken endpoint survived in another scope: %q", got)
	}
}

func TestDoSeparateBudgetsAndBackoff(t *testing.T) {
	for _, routeFailure := range []bool{false, true} {
		p, clock := requestPool(t, "http://a.test:80", "http://b.test:80")
		cause := errors.New("try again")
		calls, backoffs := 0, 0
		err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{Attempts: 2, RouteAttempts: 3, Sleep: clock.Sleep,
			Backoff: func(n int) time.Duration { backoffs++; return time.Duration(n) * time.Second },
		}, func(context.Context, string) proxytron.AttemptResult {
			calls++
			return proxytron.AttemptResult{Err: cause, Retry: true, RouteFailure: routeFailure}
		})
		wantCalls, wantBackoffs := 2, 1
		if routeFailure {
			wantCalls, wantBackoffs = 3, 0
		}
		if !errors.Is(err, proxytron.ErrAttemptsExhausted) || !errors.Is(err, cause) || calls != wantCalls || backoffs != wantBackoffs {
			t.Fatalf("routeFailure=%v: calls=%d backoffs=%d err=%v", routeFailure, calls, backoffs, err)
		}
	}
}

func TestDoWaitsForCooldownWithoutSpendingAnAttempt(t *testing.T) {
	p, clock := requestPool(t, "http://a.test:80")
	p.Demote(t.Context(), "catalog", "http://a.test:80", "refused", 0)
	calls := 0
	err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{MaxRouteWait: time.Second, Sleep: clock.Sleep},
		func(context.Context, string) proxytron.AttemptResult {
			calls++
			return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
		})
	if err != nil || calls != 1 || !slices.Equal(clock.Slept(), []time.Duration{time.Second}) {
		t.Fatalf("calls=%d waits=%v err=%v", calls, clock.Slept(), err)
	}
}

func TestDoCooldownWaitBudgetIsCumulativeAndKeepsLastError(t *testing.T) {
	p, clock := requestPool(t, "http://a.test:80")
	cause := errors.New("refused")
	calls := 0
	err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{RouteAttempts: 4, MaxRouteWait: 2 * time.Second, Sleep: clock.Sleep},
		func(context.Context, string) proxytron.AttemptResult {
			calls++
			return proxytron.AttemptResult{Outcome: proxytron.Refused, Err: cause, Retry: true, RouteFailure: true}
		})
	if !errors.Is(err, proxytron.ErrRouteWait) || !errors.Is(err, cause) || calls != 2 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	if !slices.Equal(clock.Slept(), []time.Duration{time.Second}) {
		t.Fatalf("wait budget was exceeded: %v", clock.Slept())
	}
}

func TestDoRequiresBothDirectConfigurationAndPermission(t *testing.T) {
	for _, configured := range []bool{false, true} {
		for _, allowed := range []bool{false, true} {
			var endpoints []string
			if configured {
				endpoints = []string{proxytron.Direct}
			}
			p, _ := requestPool(t, endpoints...)
			called := false
			err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{AllowDirect: allowed}, func(_ context.Context, route string) proxytron.AttemptResult {
				called = true
				if route != proxytron.Direct {
					t.Fatalf("unexpected direct route %q", route)
				}
				return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
			})
			if configured && allowed {
				if !called || err != nil {
					t.Fatalf("configured direct request: called=%v err=%v", called, err)
				}
			} else if called || !errors.Is(err, proxytron.ErrNoRoute) {
				t.Fatalf("unauthorized direct request: configured=%v allowed=%v called=%v err=%v", configured, allowed, called, err)
			}
		}
	}
}

func TestDoCanceledAttemptsDoNotScoreOrRetry(t *testing.T) {
	for _, when := range []string{"before", "during", "callback"} {
		t.Run(when, func(t *testing.T) {
			p, _ := requestPool(t, "http://a.test:80")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if when == "before" {
				cancel()
			}
			calls := 0
			err := p.Do(ctx, "catalog", proxytron.RequestOptions{Attempts: 3}, func(context.Context, string) proxytron.AttemptResult {
				calls++
				if when == "during" {
					cancel()
				}
				return proxytron.AttemptResult{Outcome: proxytron.TransportFailed, Err: context.Canceled, Retry: true}
			})
			if !errors.Is(err, context.Canceled) || calls > 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			if h := p.SiteHealth("catalog")[0]; h.Tries != 0 || h.Bench != "" {
				t.Fatalf("cancellation scored: %+v", h)
			}
		})
	}
}

func TestDoRefusalsAndChallengesStayInScope(t *testing.T) {
	for _, outcome := range []proxytron.Outcome{proxytron.Refused, proxytron.Challenged} {
		p, clock := requestPool(t, "http://a.test:80")
		pacer := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: time.Second}, clock)
		cause := errors.New("not served")
		err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{Pacer: pacer}, func(context.Context, string) proxytron.AttemptResult {
			return proxytron.AttemptResult{Outcome: outcome, Err: cause}
		})
		if err != cause || p.Acquire("catalog", false, nil, nil) != "" || p.Acquire("other", false, nil, nil) == "" {
			t.Fatalf("scope isolation failed for %v: %v", outcome, err)
		}
		wantDelay := time.Second
		if outcome == proxytron.Refused {
			wantDelay *= 2
		}
		if got := pacer.ForRoute("http://a.test:80").Delay(); got != wantDelay {
			t.Fatalf("pacing for %v: %v", outcome, got)
		}
	}
}

func TestDoCancellationDuringCooldownDoesNotSend(t *testing.T) {
	p, clock := requestPool(t, "http://a.test:80")
	p.Demote(t.Context(), "catalog", "http://a.test:80", "refused", time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := p.Do(ctx, "catalog", proxytron.RequestOptions{MaxRouteWait: time.Hour,
		Sleep: func(ctx context.Context, d time.Duration) error { cancel(); return clock.Sleep(ctx, d) },
	}, func(context.Context, string) proxytron.AttemptResult {
		t.Fatal("canceled cooldown sent a request")
		return proxytron.AttemptResult{}
	})
	if !errors.Is(err, context.Canceled) || len(clock.Slept()) != 0 || p.SiteHealth("catalog")[0].Tries != 1 {
		t.Fatalf("canceled cooldown: %v, sleeps %v, health %+v", err, clock.Slept(), p.SiteHealth("catalog"))
	}
}

func TestDoCoolingProxyDoesNotImplicitlyEnableDirect(t *testing.T) {
	p, _ := requestPool(t, "http://a.test:80")
	p.DemoteTransport(t.Context(), "catalog", "http://a.test:80", "connection failed", 0)
	err := p.Do(t.Context(), "catalog", proxytron.RequestOptions{AllowDirect: true}, func(context.Context, string) proxytron.AttemptResult {
		t.Fatal("unconfigured direct route was used")
		return proxytron.AttemptResult{}
	})
	if !errors.Is(err, proxytron.ErrRouteWait) {
		t.Fatalf("cooling route: %v", err)
	}
}
