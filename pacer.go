package proxytron

import (
	"context"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"
)

// SleepClock supplies time and cancelable waits. A test clock should advance Now
// when Sleep succeeds. Implementations must be safe for concurrent use.
type SleepClock interface {
	Clock
	Sleep(context.Context, time.Duration) error
}

func (systemClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// PacerConfig controls the request floor for each proxy host. MinInterval <= 0
// disables pacing. Refusals multiply the floor by SlowFactor, up to MaxPenalty;
// successes multiply the penalty by Decay, never below one. Invalid or zero
// factors default to 2, 8 and 0.9 respectively. A factor of one disables that
// adjustment. Pacing is shared across scopes using the same host.
type PacerConfig struct {
	MinInterval time.Duration
	SlowFactor  float64
	MaxPenalty  float64
	Decay       float64
}

// Pacer spaces request starts and adapts to caller-reported refusals. Share a
// root Pacer across workers and use ForRoute for each attempt. It is safe for
// concurrent use, owns no goroutines or clients, and needs no cleanup.
type Pacer struct {
	config PacerConfig
	clock  SleepClock
	root   *Pacer
	gate   chan struct{}

	mu      sync.Mutex
	last    time.Time
	started bool
	penalty float64

	routesMu sync.Mutex
	routes   map[string]*Pacer
}

// NewPacer constructs a direct-route pacer and its shared registry of proxy
// hosts. A nil clock uses real time. Settings are copied at construction.
func NewPacer(config PacerConfig, clock SleepClock) *Pacer {
	if clock == nil {
		clock = systemClock{}
	}
	valid := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	if !valid(config.SlowFactor) || config.SlowFactor < 1 {
		config.SlowFactor = 2
	}
	if !valid(config.MaxPenalty) || config.MaxPenalty < 1 {
		config.MaxPenalty = 8
	}
	if !valid(config.Decay) || config.Decay <= 0 || config.Decay > 1 {
		config.Decay = 0.9
	}
	return &Pacer{config: config, clock: clock, gate: make(chan struct{}, 1), penalty: 1, routes: make(map[string]*Pacer)}
}

// ForRoute returns the pacer shared by endpoint's host, ignoring credentials,
// protocol and port. Empty and Direct select the root pacer; this is only a
// pacing key and never authorizes direct routing. Invalid URLs use their full
// string as a private key and remain subject to transport validation.
func (p *Pacer) ForRoute(endpoint string) *Pacer {
	root := p
	if p.root != nil {
		root = p.root
	}
	if endpoint == "" || IsDirect(endpoint) {
		return root
	}
	key := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Hostname() != "" {
		key = strings.ToLower(u.Hostname())
	}
	root.routesMu.Lock()
	defer root.routesMu.Unlock()
	if q := root.routes[key]; q != nil {
		return q
	}
	q := &Pacer{config: root.config, clock: root.clock, root: root, gate: make(chan struct{}, 1), penalty: 1}
	root.routes[key] = q
	return q
}

// Refused widens this host's request floor, without affecting other hosts.
func (p *Pacer) Refused() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.penalty = min(p.config.MaxPenalty, p.penalty*p.config.SlowFactor)
}

// OK eases this host's request floor toward its configured minimum.
func (p *Pacer) OK() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.penalty = max(1, p.penalty*p.config.Decay)
}

// Delay estimates the wait for this host without taking a slot. It never waits
// for a sleeping request and can be used in Pool.Acquire's wait callback.
func (p *Pacer) Delay() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay()
}

func (p *Pacer) delay() time.Duration {
	if p.config.MinInterval <= 0 || !p.started {
		return 0
	}
	interval := float64(p.config.MinInterval) * p.penalty
	d := time.Duration(math.MaxInt64)
	if interval < float64(math.MaxInt64) {
		d = time.Duration(interval)
	}
	return max(0, p.last.Add(d).Sub(p.clock.Now()))
}

// Wait takes the next slot for this host. Cancellation while sleeping or queued
// returns promptly without consuming a slot. Different hosts wait independently.
func (p *Pacer) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.config.MinInterval <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.gate <- struct{}{}:
	}
	defer func() { <-p.gate }()
	for {
		p.mu.Lock()
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return err
		}
		delay := p.delay()
		if delay == 0 {
			p.last, p.started = p.clock.Now(), true
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		if err := p.clock.Sleep(ctx, delay); err != nil {
			return err
		}
	}
}
