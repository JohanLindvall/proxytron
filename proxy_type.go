package proxytron

import (
	"errors"
	"strings"
)

// ProxyType restricts pool selection to a family of proxy protocols. It does
// not certify anonymity: use verified destination TLS to protect HTTP headers
// from modification, and avoid sending identifying headers in the application.
type ProxyType string

const (
	// ProxyTypeAny leaves protocol selection unrestricted. Direct routes still
	// require explicit configuration and, for request pools, caller permission.
	ProxyTypeAny ProxyType = ""
	// ProxyTypeSOCKS selects SOCKS4, SOCKS4a, SOCKS5 and SOCKS5h TCP proxies.
	ProxyTypeSOCKS ProxyType = "socks"
	// ProxyTypeHTTP selects HTTP and HTTPS proxies. HTTPS here describes the
	// connection to the proxy, not the connection to the destination.
	ProxyTypeHTTP ProxyType = "http"
)

// Validate reports an unsupported proxy type without including its value in
// diagnostics. The zero value, ProxyTypeAny, is valid.
func (t ProxyType) Validate() error {
	switch t {
	case ProxyTypeAny, ProxyTypeSOCKS, ProxyTypeHTTP:
		return nil
	default:
		return errors.New("proxytron: proxy type must be empty, socks or http")
	}
}

// Allows reports whether endpoint's protocol matches t. A specific type excludes
// Direct and empty endpoints; an unsupported type excludes every endpoint.
// Bare host:port endpoints are treated as HTTP, as in NewTransport. Config's
// DefaultScheme is applied before pool selection. This checks the protocol only;
// use ParseEndpoint to validate endpoints at the configuration boundary.
func (t ProxyType) Allows(endpoint string) bool {
	if t == ProxyTypeAny {
		return true
	}
	if t != ProxyTypeSOCKS && t != ProxyTypeHTTP {
		return false
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || IsDirect(endpoint) {
		return false
	}
	scheme, _, explicit := strings.Cut(endpoint, "://")
	if !explicit {
		scheme = schemeHTTP
	}
	switch strings.ToLower(scheme) {
	case schemeHTTP, schemeHTTPS:
		return t == ProxyTypeHTTP
	case schemeSOCKS4, schemeSOCKS4A, schemeSOCKS5, schemeSOCKS5H:
		return t == ProxyTypeSOCKS
	default:
		return false
	}
}

// WithProxyType restricts all selections, retries and cooldown waits to t,
// including routes from feeds, restored inventory and Live overrides. A specific
// type excludes direct routing even when the caller allows it. No matching route
// means an empty acquisition, never fallback to another type or direct egress.
// Inventory and health are retained, and the option lasts for the pool's lifetime.
// LoadPool rejects unsupported types; New and FromURLs return pools that select
// no routes for an unsupported type. The default is ProxyTypeAny.
func WithProxyType(t ProxyType) Option {
	return func(p *Pool) { p.proxyType = t }
}
