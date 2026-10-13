package proxytron_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func TestDetectorParsesBoundedSingleAddresses(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"plain", "192.0.2.17\n", "192.0.2.17"},
		{"json", `{"ip":"2001:db8::17"}`, "2001:db8::17"},
		{"origin", `{"origin":"192.0.2.18"}`, "192.0.2.18"},
		{"mapped", "::ffff:192.0.2.17", "192.0.2.17"},
		{"ambiguous", `{"origin":"192.0.2.1, 192.0.2.2"}`, ""},
		{"zone", "fe80::1%eth0", ""},
		{"unspecified", "0.0.0.0", ""},
		{"multicast", "ff02::1", ""},
		{"html", "<html>blocked</html>", ""},
		{"oversize", strings.Repeat(" ", 4096) + "192.0.2.1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.body) }))
			defer s.Close()
			d := proxytron.Detector{URL: s.URL}
			got, err := d.Detect(t.Context(), proxytron.Direct)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("accepted %q as %v", tc.body, got)
				}
				return
			}
			if err != nil || got.IP.String() != tc.want || got.CheckedAt.IsZero() {
				t.Fatalf("got %+v, %v", got, err)
			}
		})
	}
}

func TestDetectorUsesProxyAndNeverFallsBack(t *testing.T) {
	var directCalls, proxyCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		directCalls.Add(1)
		_, _ = io.WriteString(w, "192.0.2.1")
	}))
	defer target.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		if r.URL.String() != target.URL+"/" {
			t.Errorf("proxy received %s", r.URL)
		}
		_, _ = io.WriteString(w, `{"ip":"198.51.100.7"}`)
	}))
	d := proxytron.Detector{URL: target.URL + "/", Timeout: time.Second}
	got, err := d.Compare(t.Context(), proxy.URL)
	if err != nil || got.SameIP() || got.Direct.IP.String() != "192.0.2.1" || got.Proxy.IP.String() != "198.51.100.7" {
		t.Fatalf("comparison: %+v, %v", got, err)
	}
	if directCalls.Load() != 1 || proxyCalls.Load() != 1 {
		t.Fatal("wrong routes used")
	}
	proxy.Close()
	if _, err := d.Detect(t.Context(), proxy.URL); err == nil {
		t.Fatal("closed proxy succeeded")
	}
	if directCalls.Load() != 1 {
		t.Fatal("failed proxy leaked a direct request")
	}
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestDetectorCancellationAndFailedStatus(t *testing.T) {
	d := proxytron.Detector{URL: "https://ip.example.test", Timeout: 10 * time.Millisecond,
		Client: func(string) (proxytron.HTTPDoer, error) {
			return doerFunc(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() }), nil
		}}
	if _, err := d.Detect(t.Context(), "http://proxy.test:80"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	d.Client = func(string) (proxytron.HTTPDoer, error) {
		return doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("192.0.2.1"))}, nil
		}), nil
	}
	if _, err := d.Detect(t.Context(), proxytron.Direct); err == nil {
		t.Fatal("accepted refused IP-check")
	}
}

func TestScoredInventoryAndEgressSurviveRestart(t *testing.T) {
	const endpoint = "http://user:secret@proxy.example.test:8080"
	cfg := proxytron.DefaultConfig()
	cfg.Endpoints = []string{endpoint}
	path := filepath.Join(t.TempDir(), "inventory.db")
	p, err := proxytron.LoadPool(t.Context(), &cfg, nil, path)
	if err != nil {
		t.Fatal(err)
	}
	p.ReportOK(t.Context(), "catalog", endpoint, 100*time.Millisecond)
	p.Punish(t.Context(), "catalog", endpoint, "challenge", time.Second)
	d := proxytron.Detector{URL: "https://ip.example.test", Client: func(route string) (proxytron.HTTPDoer, error) {
		if route != endpoint {
			t.Errorf("detected wrong route: %q", route)
		}
		return doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("2001:db8::8"))}, nil
		}), nil
	}}
	before, err := p.DetectEgress(t.Context(), d, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = proxytron.LoadPool(t.Context(), &cfg, nil, path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	after, ok := p.Egress(endpoint)
	if !ok || after.IP != netip.MustParseAddr("2001:db8::8") || !after.CheckedAt.Equal(before.CheckedAt) {
		t.Fatalf("lost identity: %+v", after)
	}
	if rows := p.SiteHealth("catalog"); len(rows) != 1 || rows[0].Bench != proxytron.BenchWAF || rows[0].Tries != 2 {
		t.Fatalf("lost scores: %+v", rows)
	}
	if got := p.Acquire("catalog", false, nil, nil); got != "" {
		t.Fatalf("restored challenge was ignored: %s", proxytron.Redact(got))
	}
	if got := p.Acquire("other-service", false, nil, nil); got != endpoint {
		t.Fatal("challenge leaked into another scope")
	}
	d.Client = func(string) (proxytron.HTTPDoer, error) { return nil, fmt.Errorf("check unavailable") }
	if _, err := p.DetectEgress(t.Context(), d, endpoint); err == nil {
		t.Fatal("failed check succeeded")
	}
	if kept, _ := p.Egress(endpoint); kept != after {
		t.Fatal("failed check overwrote identity")
	}
}
