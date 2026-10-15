package lease

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func TestProxyTypeSelection(t *testing.T) {
	endpoints := []string{Direct, "http://http.test:80", "https://https.test:443",
		"socks4://four.test:1080", "socks4a://four-a.test:1080",
		"socks5://five.test:1080", "socks5h://five-h.test:1080"}
	for _, kind := range []proxytron.ProxyType{proxytron.ProxyTypeHTTP, proxytron.ProxyTypeSOCKS} {
		for _, measured := range []bool{false, true} {
			name := string(kind) + "/unmeasured"
			if measured {
				name = string(kind) + "/measured"
			}
			t.Run(name, func(t *testing.T) {
				p := testPool(t, endpoints, nil, WithProxyType(kind))
				if measured {
					for _, e := range p.entries {
						s := e.stat("downloads")
						s.Tries, s.OK, s.BytesPerSecond = 10, 10, 1<<20
					}
				}
				want, forbidden := 2, endpoints[3]
				if kind == proxytron.ProxyTypeSOCKS {
					want, forbidden = 4, endpoints[1]
				}
				seen := map[string]bool{}
				for range want {
					l := p.AcquireFor("downloads", routeID(forbidden), 1, 128<<20, false)
					if l == nil {
						t.Fatal("matching route was unavailable")
					}
					t.Cleanup(l.Release)
					if !kind.Allows(l.Endpoint()) || seen[l.ID()] {
						t.Fatalf("leased a forbidden or already reserved route: %s", l.Address())
					}
					seen[l.ID()] = true
				}
				for _, preferred := range []string{"", Direct, routeID(forbidden)} {
					if l := p.Acquire("downloads", preferred, 1); l != nil {
						l.Release()
						t.Fatalf("exhausted type fell back to %s", l.Address())
					}
				}
				if len(p.entries) != len(endpoints) {
					t.Fatal("filter discarded inventory")
				}
			})
		}
	}
}

func TestProxyTypeFeedRestartAndConfigure(t *testing.T) {
	const httpRoute, socksRoute = "http://http.test:80", "socks5h://socks.test:1080"
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, httpRoute+"\n"+socksRoute)
	}))
	defer feed.Close()
	p := testPool(t, []string{Direct}, []string{feed.URL}, WithProxyType(proxytron.ProxyTypeSOCKS))
	p.Refresh(t.Context())
	l := p.Acquire("downloads", routeID(httpRoute), 1)
	if l == nil || l.Endpoint() != socksRoute {
		t.Fatal("feed allowed a preferred HTTP route")
	}
	l.observe(Observation{Status: http.StatusOK, Bytes: 1 << 20, Duration: time.Second})
	l.Cooldown(time.Now().Add(time.Hour))
	l.Release()
	path := p.db.Path()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path, p.static, p.feeds, nil, p.log, WithProxyType(proxytron.ProxyTypeSOCKS))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if l := restored.Acquire("downloads", routeID(httpRoute), 1); l != nil {
		l.Release()
		t.Fatal("restored cooldown allowed a different proxy type")
	}
	cfg, err := ParseConfiguration([]string{Direct, "https://new.test:443"}, p.feeds)
	if err != nil {
		t.Fatal(err)
	}
	restored.Configure(cfg, true)
	if l := restored.Acquire("downloads", "", 1); l != nil {
		l.Release()
		t.Fatal("Configure bypassed the proxy type")
	}
	if restored.entries[socksRoute].stat("downloads").Tries != 1 || restored.entries[httpRoute] == nil {
		t.Fatal("type filter lost persisted inventory or health")
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, cfg.Endpoints, cfg.Feeds, nil, p.log, WithProxyType(proxytron.ProxyTypeHTTP))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	l = reopened.Acquire("downloads", routeID(httpRoute), 1)
	if l == nil || l.Endpoint() != httpRoute {
		t.Fatal("changing type lost the retained HTTP route")
	}
	l.Release()
}

func TestProxyTypeUpgrade(t *testing.T) {
	const slow = "socks5h://slow.test:1080"
	const fast = "socks5h://fast.test:1080"
	const forbidden = "https://fastest.test:443"
	p := testPool(t, []string{slow, fast, forbidden, Direct}, nil, WithProxyType(proxytron.ProxyTypeSOCKS))
	for _, e := range p.entries {
		s := e.stat("downloads")
		s.Tries, s.OK, s.SetupSamples, s.SetupSeconds = 100, 100, 1, 30
		s.BytesPerSecond = 64 << 10
	}
	p.entries[fast].stat("downloads").BytesPerSecond = 2 << 20
	p.entries[forbidden].stat("downloads").BytesPerSecond = 100 << 20
	p.entries[Direct].stat("downloads").BytesPerSecond = 100 << 20
	l := p.Acquire("downloads", routeID(slow), 1)
	if l == nil {
		t.Fatal("no starting lease")
	}
	defer l.Release()
	now := l.started.Add(50 * time.Second)
	l.Progress(now, 20*64<<10, 20*time.Second, false)
	if next := l.Upgrade(now, 128<<20, 64<<10); next != nil {
		next.Release()
		t.Fatal("upgraded without sustained evidence")
	}
	next := l.Upgrade(now.Add(defaultUpgradeInterval), 128<<20, 64<<10)
	if next == nil {
		t.Fatal("matching faster route was not reserved")
	}
	defer next.Release()
	if next.Endpoint() != fast {
		t.Fatalf("upgrade selected a forbidden protocol: %s", next.Address())
	}
	if extra := p.Acquire("downloads", routeID(forbidden), 1); extra != nil {
		extra.Release()
		t.Fatal("active upgrade reservation allowed fallback")
	}
}

func TestProxyTypeKeepsExitReservations(t *testing.T) {
	const first, second = "socks5h://one.test:1080", "socks5h://two.test:1080"
	p := testPool(t, []string{first, second, "http://http.test:80", Direct}, nil,
		WithProxyType(proxytron.ProxyTypeSOCKS))
	for _, raw := range []string{first, second} {
		p.entries[raw].Exit, p.entries[raw].ExitAt = "198.51.100.8", time.Now()
	}
	l := p.Acquire("downloads", routeID(first), 1)
	if l == nil {
		t.Fatal("no initial route")
	}
	defer l.Release()
	if !l.ClaimExit("198.51.100.8") {
		t.Fatal("could not confirm reserved exit")
	}
	cfg, err := ParseConfiguration([]string{second, "https://new.test:443", Direct}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Configure(cfg, false)
	if extra := p.Acquire("downloads", "", 1); extra != nil {
		extra.Release()
		t.Fatal("configuration change bypassed the active exit or proxy type")
	}
	l.Release()
	next := p.Acquire("downloads", "", 1)
	if next == nil {
		t.Fatal("released exit remained reserved")
	}
	defer next.Release()
	if next.Endpoint() != second {
		t.Fatal("selected a different proxy type after releasing the exit")
	}
}

func TestInvalidProxyTypeRejectedBeforeStoreOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.db")
	p, err := Open(path, []string{Direct}, nil, nil, nil, WithProxyType(proxytron.ProxyType("unsupported")))
	if err == nil {
		p.Close()
		t.Fatal("accepted an unsupported type")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid options touched the database: %v", err)
	}
}
