package proxytron

import (
	"context"
	"errors"
	"time"
)

// Errors returned by Pool.Do describe why another attempt could not be made.
// They may wrap the last attempt's error as well; use errors.Is to inspect both.
var (
	// ErrNoRoute means the pool has no route the scope is allowed to use.
	ErrNoRoute = errors.New("proxytron: no eligible route")
	// ErrRouteWait means eligible routes are cooling beyond the remaining wait budget.
	ErrRouteWait = errors.New("proxytron: route wait budget exhausted")
	// ErrAttemptsExhausted means one of the independent retry budgets was spent.
	ErrAttemptsExhausted = errors.New("proxytron: attempt budget exhausted")
)

// Outcome is an application's classification of an attempt for route scoring.
// It is independent of whether the application wants to retry the operation.
type Outcome uint8

const (
	// Unreported leaves route health unchanged, for example for a local error.
	Unreported Outcome = iota
	// Succeeded records a usable response.
	Succeeded
	// Refused records a refusal limited to the request's scope.
	Refused
	// TransportFailed records an endpoint-wide connection failure.
	TransportFailed
	// Challenged records a challenge limited to the request's scope.
	Challenged
)

// AttemptResult separates scoring, retry policy and the error returned to the
// caller. Retry requests another attempt. RouteFailure charges a retry to the
// route budget instead of the operation budget, without applying Backoff. This
// lets applications decide whether a refusal or challenge costs the operation.
// Err may be nil for a classified response; exhausted retries still return an
// error. The zero value ends the operation successfully without scoring it.
type AttemptResult struct {
	Outcome      Outcome
	Err          error
	Retry        bool
	RouteFailure bool
}

// RequestOptions controls one Pool.Do call. Attempts limits ordinary failed
// attempts and defaults to one. RouteAttempts limits failures charged to routes
// and defaults to max(Attempts, the scope's configured route count).
type RequestOptions struct {
	AllowDirect   bool // Direct must also be a configured pool member.
	Attempts      int
	RouteAttempts int
	// MaxRouteWait bounds the total time spent waiting for route cooldowns.
	// Zero disables those waits. Pacing, Backoff and I/O are bounded by ctx.
	MaxRouteWait time.Duration
	Pacer        *Pacer
	// Backoff receives the number of ordinary failed attempts, starting at one.
	// Nil retries immediately. It is never called for a route failure.
	Backoff func(failures int) time.Duration
	// Sleep defaults to a context-aware timer. Tests using WithClock should
	// supply a sleep function that advances the same clock.
	Sleep func(context.Context, time.Duration) error
}

// Do rotates routes, waits for cooldowns, paces request starts and scores the
// outcomes returned by attempt. The callback owns request construction, session
// state, response classification and body cleanup. Close or consume bodies before
// returning; a result can be retained in the callback's closure. Only retry
// operations the application can safely replay.
//
// Do invokes callbacks synchronously, outside pool locks, and owns no clients or
// goroutines. Cancellation of ctx or a context.Canceled attempt is never scored.
// An empty pool never permits direct egress. Failed routes are avoided while
// alternatives are ready, then may be reused after their cooldowns, like Acquire.
func (p *Pool) Do(ctx context.Context, scope string, options RequestOptions, attempt func(context.Context, string) AttemptResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attempt == nil {
		return errors.New("proxytron: nil request attempt")
	}
	if p == nil {
		return ErrNoRoute
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = systemClock{}.Sleep
	}
	var delay func(string) time.Duration
	if options.Pacer != nil {
		delay = func(endpoint string) time.Duration { return options.Pacer.ForRoute(endpoint).Delay() }
	}
	attempts := max(1, options.Attempts)
	routeAttempts := options.RouteAttempts
	if routeAttempts <= 0 {
		routeAttempts = max(attempts, p.PathCount(scope, options.AllowDirect))
	}
	excluded := make(map[string]bool)
	waitBudget := max(0, options.MaxRouteWait)
	var last error
	var failures, routeFailures int
	for {
		endpoint, err := p.waitRoute(ctx, scope, options.AllowDirect, excluded, delay, &waitBudget, sleep)
		if err != nil {
			return errors.Join(err, last)
		}
		var pacer *Pacer
		if options.Pacer != nil {
			pacer = options.Pacer.ForRoute(endpoint)
			if err := pacer.Wait(ctx); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		start := p.now()
		result := attempt(ctx, endpoint)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errors.Is(result.Err, context.Canceled) {
			return result.Err
		}
		took := max(0, p.now().Sub(start))
		switch result.Outcome {
		case Unreported:
		case Succeeded:
			p.ReportOK(ctx, scope, endpoint, took)
			if pacer != nil {
				pacer.OK()
			}
		case Refused:
			p.Demote(ctx, scope, endpoint, "request refused", took)
			if pacer != nil {
				pacer.Refused()
			}
		case TransportFailed:
			p.DemoteTransport(ctx, scope, endpoint, "transport failed", took)
		case Challenged:
			p.Punish(ctx, scope, endpoint, "request challenged", took)
		default:
			return errors.New("proxytron: invalid attempt outcome")
		}
		if !result.Retry {
			return result.Err
		}
		last = result.Err
		excluded[endpoint] = true
		if result.RouteFailure {
			routeFailures++
		} else {
			failures++
		}
		if failures >= attempts || routeFailures >= routeAttempts {
			return errors.Join(ErrAttemptsExhausted, last)
		}
		if !result.RouteFailure && options.Backoff != nil {
			if err := sleep(ctx, max(0, options.Backoff(failures))); err != nil {
				return err
			}
		}
	}
}

func (p *Pool) waitRoute(ctx context.Context, scope string, allowDirect bool, exclude map[string]bool,
	delay func(string) time.Duration, budget *time.Duration, sleep func(context.Context, time.Duration) error) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if endpoint := p.Acquire(scope, allowDirect, exclude, delay); endpoint != "" {
			return endpoint, nil
		}
		// Acquire treats exclusions as preferences. A failed route may return
		// after cooling, so exclusions must not hide it from this wait either.
		wait, ok := p.WaitFor(scope, allowDirect, nil)
		if !ok {
			return "", ErrNoRoute
		}
		// A concurrent inventory/outcome update can make WaitFor report zero
		// after an empty Acquire. Bound such retries without a busy loop.
		if wait <= 0 {
			wait = time.Millisecond
		}
		if wait > *budget {
			return "", ErrRouteWait
		}
		if err := sleep(ctx, wait); err != nil {
			return "", err
		}
		*budget -= wait
	}
}
