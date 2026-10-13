package testutil

import (
	"context"
	"math"
	"sync"
	"time"
)

// Clock is a fake clock that tests advance by hand, so a test can walk
// through hours of cooldowns at once. It satisfies proxy.Clock (Now) and
// fetch.Clock (Now and Sleep, which records the duration and moves the
// clock on instead of blocking).
//
// The same *Clock handed to every package under test is what keeps them in
// step: a route wait the proxy pool computes from Now is the one the
// fetcher then sleeps through. It is safe for concurrent use.
type Clock struct {
	mu    sync.Mutex
	now   time.Time
	slept []time.Duration
}

// NewClock returns a Clock reading start. Only start's wall time is kept,
// so every reading compares by wall time and Set may move the clock
// anywhere.
func NewClock(start time.Time) *Clock { return &Clock{now: start.Round(0)} }

// Now is the clock's current time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock on by d (back, for a negative d).
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set puts the clock at t.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.Round(0)
}

// Sleep records d and advances the clock by it (by nothing for a negative
// d) instead of blocking. A cancelled ctx returns its error and neither
// records nor advances, as a real sleep would end at once.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slept = append(c.slept, d)
	c.now = c.now.Add(max(0, d))
	return nil
}

// Slept is every duration Sleep was given, in order.
func (c *Clock) Slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

// Waits is the sleeps longer than over, in whole seconds rounded half to
// even: the waits for a route, without the fractions of a millisecond the
// backoff costs under Fast. Never nil.
func (c *Clock) Waits(over time.Duration) []int {
	out := []int{}
	for _, d := range c.Slept() {
		if d > over {
			out = append(out, int(math.RoundToEven(d.Seconds())))
		}
	}
	return out
}
