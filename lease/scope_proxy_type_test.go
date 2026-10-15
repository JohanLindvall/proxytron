package lease

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JohanLindvall/proxytron"
)

const (
	scopeHTTPRoute  = "http://http.test:80"
	scopeSOCKSRoute = "socks5h://socks.test:1080"
)

func scopeEndpoints() []string { return []string{Direct, scopeHTTPRoute, scopeSOCKSRoute} }

func TestScopeProxyTypeRestrictsItsScopeAlone(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil)
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeSOCKS); err != nil {
		t.Fatal(err)
	}
	l := p.Acquire("downloads", routeID(scopeHTTPRoute), 1)
	if l == nil || l.Endpoint() != scopeSOCKSRoute {
		t.Fatalf("restricted scope leased %v; want its SOCKS route", l)
	}
	defer l.Release()
	if extra := p.Acquire("downloads", Direct, 1); extra != nil {
		extra.Release()
		t.Fatalf("exhausted scope fell back to %s", extra.Address())
	}
	other := p.Acquire("pages", routeID(scopeHTTPRoute), 1)
	if other == nil || other.Endpoint() != scopeHTTPRoute {
		t.Fatalf("an unrestricted scope leased %v; want the HTTP route it preferred", other)
	}
	other.Release()
}

func TestScopeProxyTypeChangesAtRuntime(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil)
	acquire := func() string {
		t.Helper()
		l := p.Acquire("downloads", routeID(scopeHTTPRoute), 1)
		if l == nil {
			return ""
		}
		defer l.Release()
		return l.Endpoint()
	}
	if got := acquire(); got != scopeHTTPRoute {
		t.Fatalf("unrestricted scope leased %q", got)
	}
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeSOCKS); err != nil {
		t.Fatal(err)
	}
	if got := acquire(); got != scopeSOCKSRoute {
		t.Fatalf("after restricting to SOCKS, leased %q", got)
	}
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyType("unsupported")); err == nil {
		t.Fatal("accepted an unsupported type")
	}
	if got := acquire(); got != scopeSOCKSRoute {
		t.Fatalf("a rejected type changed the restriction: leased %q", got)
	}
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeAny); err != nil {
		t.Fatal(err)
	}
	if got := acquire(); got != scopeHTTPRoute {
		t.Fatalf("after lifting the restriction, leased %q", got)
	}
	if len(p.entries) != len(scopeEndpoints()) {
		t.Fatal("restriction discarded inventory")
	}
}

func TestScopeProxyTypeHoldsOnTopOfThePoolsOwn(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil, WithProxyType(proxytron.ProxyTypeSOCKS))
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeHTTP); err != nil {
		t.Fatal(err)
	}
	if l := p.Acquire("downloads", routeID(scopeHTTPRoute), 1); l != nil {
		l.Release()
		t.Fatalf("leased %s, which the pool's own type excludes", l.Address())
	}
	if l := p.Acquire("pages", "", 1); l == nil || l.Endpoint() != scopeSOCKSRoute {
		t.Fatalf("another scope leased %v; want the pool's SOCKS route", l)
	} else {
		l.Release()
	}
}

func TestScopeProxyTypeOption(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil, WithScopeProxyType("downloads", proxytron.ProxyTypeHTTP))
	l := p.Acquire("downloads", routeID(scopeSOCKSRoute), 1)
	if l == nil || l.Endpoint() != scopeHTTPRoute {
		t.Fatalf("option-restricted scope leased %v; want its HTTP route", l)
	}
	l.Release()

	path := filepath.Join(t.TempDir(), "leases.db")
	bad, err := Open(path, []string{Direct}, nil, nil, nil, WithScopeProxyType("downloads", proxytron.ProxyType("unsupported")))
	if err == nil {
		bad.Close()
		t.Fatal("accepted an unsupported scope type")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid options touched the database: %v", err)
	}
}

func TestScopeProxyTypeChecksReservedRoutesAgain(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil)
	l := p.Acquire("downloads", routeID(scopeHTTPRoute), 1)
	if l == nil || l.Endpoint() != scopeHTTPRoute {
		t.Fatalf("leased %v; want the HTTP route", l)
	}
	defer l.Release()
	if !l.Configured() {
		t.Fatal("an allowed route reads as excluded")
	}
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeSOCKS); err != nil {
		t.Fatal(err)
	}
	if l.Configured() {
		t.Fatal("a route the scope now excludes still reads as configured")
	}
}

func TestScopeProxyTypeShapesTheScopesPage(t *testing.T) {
	p := testPool(t, scopeEndpoints(), nil)
	if err := p.SetScopeProxyType("downloads", proxytron.ProxyTypeSOCKS); err != nil {
		t.Fatal(err)
	}
	urls := func(site string) string {
		var shown []string
		for _, row := range p.Page(site, Query{Limit: 50}).Rows {
			shown = append(shown, row.URL)
		}
		return strings.Join(shown, " ")
	}
	if got := urls("downloads"); strings.Contains(got, "http.test") || !strings.Contains(got, "socks.test") {
		t.Fatalf("restricted scope's page shows %q", got)
	}
	if got := urls("pages"); !strings.Contains(got, "http.test") {
		t.Fatalf("another scope's page shows %q", got)
	}
}
