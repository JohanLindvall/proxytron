package proxytron_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
	"github.com/JohanLindvall/proxytron/internal/testutil"
)

func TestPacerSharesHostsAndAdaptsIndependently(t *testing.T) {
	clock := testutil.NewClock(time.Unix(100, 0))
	p := proxytron.NewPacer(proxytron.PacerConfig{
		MinInterval: time.Second, SlowFactor: 2, MaxPenalty: 4, Decay: 0.5,
	}, clock)
	a := p.ForRoute("http://alice:secret@EXAMPLE.test:80")
	for _, route := range []string{"https://example.test:443", "socks5h://bob:password@example.test:1080"} {
		if p.ForRoute(route) != a || a.ForRoute(route) != a {
			t.Fatal("host variants got different pacing floors")
		}
	}
	if p.ForRoute("") != p || a.ForRoute(" DIRECT ") != p || p.ForRoute("http://[::1]:80") != p.ForRoute("socks5h://[::1]:1080") {
		t.Fatal("direct or IPv6 pacing key changed")
	}
	for _, route := range []*proxytron.Pacer{p, a} {
		if err := route.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	a.Refused()
	a.Refused()
	a.Refused()
	if a.Delay() != 4*time.Second || p.Delay() != time.Second || p.ForRoute("http://other.test:80").Delay() != 0 {
		t.Fatal("penalty escaped the host or exceeded its cap")
	}
	a.OK()
	if err := a.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	a.OK()
	a.OK()
	if err := a.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := clock.Slept(); !slices.Equal(got, []time.Duration{2 * time.Second, time.Second}) {
		t.Fatalf("adaptive waits: %v", got)
	}
}

func TestPacerConcurrentWaitersTakeDistinctSlots(t *testing.T) {
	clock := testutil.NewClock(time.Unix(100, 0))
	p := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: time.Second}, clock)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if err := p.Wait(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := clock.Slept(); len(got) != 31 {
		t.Fatalf("32 starts took %d waits, want 31", len(got))
	} else {
		for _, wait := range got {
			if wait != time.Second {
				t.Fatalf("overlapping slots: %v", got)
			}
		}
	}
}

type heldPacerClock struct {
	*testutil.Clock
	entered chan struct{}
}

func (c *heldPacerClock) Sleep(ctx context.Context, _ time.Duration) error {
	select {
	case c.entered <- struct{}{}:
	case <-ctx.Done():
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestPacerQueuedCancellationAndOtherHostsDoNotWaitForSleeper(t *testing.T) {
	clock := &heldPacerClock{Clock: testutil.NewClock(time.Unix(100, 0)), entered: make(chan struct{}, 1)}
	p := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: time.Hour}, clock)
	if err := p.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sleeping := make(chan error, 1)
	go func() { sleeping <- p.Wait(ctx) }()
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("wait did not begin")
	}
	// A second request must be cancelable even while queued behind the first.
	queued, abort := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer abort()
	finished := make(chan error, 1)
	go func() { finished <- p.Wait(queued) }()
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request could not cancel")
	}
	other := make(chan error, 1)
	go func() { other <- p.ForRoute("http://independent.test:80").Wait(t.Context()) }()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sleeping direct request blocked an independent proxy")
	}
	if got := p.Delay(); got != time.Hour {
		t.Fatalf("Delay blocked or changed the slot: %v", got)
	}
	cancel()
	if err := <-sleeping; !errors.Is(err, context.Canceled) {
		t.Fatalf("sleep cancellation: %v", err)
	}
	clock.Advance(time.Hour)
	if err := p.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPacerCanceledContextNeverConsumesASlot(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Second} {
		p := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: interval}, nil)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := p.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait with interval %v: %v", interval, err)
		}
		if p.Delay() != 0 {
			t.Fatal("canceled wait consumed a slot")
		}
	}
}
