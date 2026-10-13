package lease

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func TestRealExitIPsShareLimitsAndPersist(t *testing.T) {
	const a, b = "http://gateway-a.example.test:8080", "http://gateway-b.example.test:9090"
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "198.51.100.8") }))
	defer echo.Close()
	d := proxytron.Detector{URL: echo.URL, Client: func(string) (proxytron.HTTPDoer, error) { return echo.Client(), nil }}
	path := filepath.Join(t.TempDir(), "leases.db")
	p, err := Open(path, []string{a, b}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{a, b} {
		if _, err := p.DetectEgress(t.Context(), d, endpoint); err != nil {
			t.Fatal(err)
		}
	}
	l := p.Acquire("downloads", "", 1)
	if l == nil {
		t.Fatal("no route")
	}
	if second := p.Acquire("downloads", "", 1); second != nil {
		second.Release()
		t.Fatal("same public IP leased twice")
	}
	l.Cooldown(time.Now().Add(time.Hour))
	l.Release()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(path, []string{a, b}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if got, ok := p.Egress(b); !ok || got.IP.String() != "198.51.100.8" {
		t.Fatalf("lost identity: %+v", got)
	}
	if l := p.Acquire("downloads", "", 1); l != nil {
		l.Release()
		t.Fatal("lost shared cooldown")
	}
	if l := p.Acquire("another-service", "", 1); l == nil {
		t.Fatal("service cooldown became global")
	} else {
		l.Release()
	}
}

func TestDetectionRebuildsAnIndexWithActiveLeases(t *testing.T) {
	p := testPool(t, []string{"http://a.test:80", "http://b.test:80"}, nil)
	a := p.Acquire("service", routeID("http://a.test:80"), 1)
	b := p.Acquire("service", routeID("http://b.test:80"), 1)
	defer a.Release()
	defer b.Release()
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "192.0.2.7") }))
	defer echo.Close()
	d := proxytron.Detector{URL: echo.URL, Client: func(string) (proxytron.HTTPDoer, error) { return echo.Client(), nil }}
	for _, endpoint := range []string{a.Endpoint(), b.Endpoint()} {
		if _, err := p.DetectEgress(t.Context(), d, endpoint); err != nil {
			t.Fatal(err)
		}
	}
	if l := p.Acquire("service", "", 2); l != nil {
		l.Release()
		t.Fatal("active alias leases were lost")
	}
	a.Release()
	if l := p.Acquire("service", "", 2); l == nil {
		t.Fatal("released alias still occupies capacity")
	} else {
		l.Release()
	}
}
