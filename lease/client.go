package lease

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"sync"
	"time"

	"github.com/JohanLindvall/proxytron"
)

// Direct identifies an explicitly configured direct route.
const Direct = proxytron.Direct

// Observation describes one attempt. Applications classify challenges and
// authoritative retry deadlines; the pool owns scoring and cooldowns.
type Observation struct {
	Duration   time.Duration
	Bytes      int64
	Status     int
	Err        error
	RetryAfter time.Time
	Challenged bool
}

// Client adapts an application's HTTP stack to a lease. ThroughProxy must
// isolate cookies and connections and must never silently bypass the route.
// WithRoute pins subsequent requests to the client and reports observations.
// Its string arguments are the route ID and application scope, respectively.
type Client interface {
	ThroughProxy(string) (Client, error)
	NewRequest(context.Context, string, string, io.Reader) (*http.Request, error)
	DoOnce(*http.Request) (*http.Response, error)
	WithRoute(context.Context, string, string, func(Observation)) context.Context
	CloseIdleConnections()
}

// Policy identifies caller-defined service scopes. RecoveryScopes may borrow
// measurements from other scopes. Bandwidth from CappedScopes is excluded
// because a service's own limit does not measure the proxy's capacity.
type Policy struct {
	RecoveryScopes []string
	CappedScopes   []string
}

func (p *Policy) isRecovery(site string) bool {
	if p != nil {
		for _, s := range p.RecoveryScopes {
			if s == site {
				return true
			}
		}
	}
	return false
}
func (p *Policy) isCapped(site string) bool {
	if p != nil {
		for _, s := range p.CappedScopes {
			if s == site {
				return true
			}
		}
	}
	return false
}

// Option configures a lease pool at Open time.
type Option func(*Pool)

// WithProxyType restricts selection to t for static, discovered and restored
// routes, including preferred routes and adaptive upgrades. A specific type
// excludes Direct. No matching route means a nil lease, never fallback to another
// type or direct egress. Inventory and health are retained. The restriction lasts
// for the pool's lifetime, including Configure calls; the default is
// proxytron.ProxyTypeAny. Open rejects unsupported types.
func WithProxyType(t proxytron.ProxyType) Option {
	return func(p *Pool) { p.proxyType = t }
}

// WithScopeProxyType restricts selection for one scope to t from Open on, as
// Pool.SetScopeProxyType does later. Open rejects unsupported types.
func WithScopeProxyType(scope string, t proxytron.ProxyType) Option {
	return func(p *Pool) {
		if p.scopeTypes == nil {
			p.scopeTypes = make(map[string]proxytron.ProxyType)
		}
		p.scopeTypes[scope] = t
	}
}

// WithHealthEpoch enables an explicit application-owned health migration.
// Opening a store from another epoch forgets scores and identities, retaining
// inventory. Omit this option to preserve health regardless of epoch metadata.
func WithHealthEpoch(epoch int) Option {
	return func(p *Pool) { p.healthEpoch = &epoch }
}

// WithExitTTL sets how long a measured exit is trusted for admission decisions.
// Nonpositive values retain the default of one hour. Freshly claimed leases
// continue holding their exit until released, even after this interval.
func WithExitTTL(ttl time.Duration) Option {
	return func(p *Pool) {
		if ttl > 0 {
			p.exitTTL = ttl
		}
	}
}

// WithFeedFilter restricts discovered routes to those the application's HTTP
// adapter supports. Returning false excludes the endpoint. Static endpoints
// should be validated by the application's configuration boundary.
func WithFeedFilter(accept func(string) bool) Option {
	return func(p *Pool) { p.feedFilter = accept }
}

// WithPolicy installs service policy without hard-coding product names.
func WithPolicy(policy Policy) Option {
	return func(p *Pool) {
		p.policy = Policy{append([]string(nil), policy.RecoveryScopes...), append([]string(nil), policy.CappedScopes...)}
	}
}

// HTTPClient is the standard net/http adapter. Applications with custom TLS
// fingerprints or challenge handling can implement Client instead.
type HTTPClient struct{ client *http.Client }

// NewHTTPClient uses client for feed downloads. Nil selects a standard client.
func NewHTTPClient(client *http.Client) *HTTPClient {
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPClient{client: client}
}

// ThroughProxy creates an isolated standard transport and cookie jar.
func (c *HTTPClient) ThroughProxy(endpoint string) (Client, error) {
	tr, err := proxytron.NewTransport(endpoint, proxytron.TransportOptions{})
	if err != nil {
		return nil, err
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &HTTPClient{client: &http.Client{Transport: tr, Jar: jar, Timeout: c.client.Timeout}}, nil
}

// NewRequest constructs a context-bound request.
func (c *HTTPClient) NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, method, url, body)
}

type routeKey struct{}
type route struct {
	client  *HTTPClient
	observe func(Observation)
}

// WithRoute pins the standard client to the request context.
func (c *HTTPClient) WithRoute(ctx context.Context, _, _ string, observe func(Observation)) context.Context {
	return context.WithValue(ctx, routeKey{}, route{c, observe})
}

// DoOnce performs one request, respecting a lease context and reporting the
// complete body read. Intentional partial closes and cancellation are ignored.
func (c *HTTPClient) DoOnce(req *http.Request) (*http.Response, error) {
	r, ok := req.Context().Value(routeKey{}).(route)
	if !ok {
		return c.client.Do(req)
	}
	start := time.Now()
	resp, err := r.client.client.Do(req)
	if err != nil {
		if req.Context().Err() == nil {
			r.observe(Observation{Err: err, Duration: time.Since(start)})
		}
		return resp, err
	}
	resp.Body = &observedBody{ReadCloser: resp.Body, ctx: req.Context(), start: start, status: resp.StatusCode, observe: r.observe}
	return resp, nil
}

// CloseIdleConnections releases cached connections.
func (c *HTTPClient) CloseIdleConnections() { c.client.CloseIdleConnections() }

type observedBody struct {
	io.ReadCloser
	ctx     context.Context
	start   time.Time
	status  int
	bytes   int64
	observe func(Observation)
	once    sync.Once
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes += int64(n)
	if err != nil && b.ctx.Err() == nil {
		b.once.Do(func() {
			failure := err
			if errors.Is(err, io.EOF) {
				failure = nil
			}
			if failure == nil && b.status >= 400 {
				failure = errors.New(http.StatusText(b.status))
			}
			b.observe(Observation{Duration: time.Since(b.start), Bytes: b.bytes, Status: b.status, Err: failure})
		})
	}
	return n, err
}

func backoff(n int, base, limit time.Duration) time.Duration {
	for range min(n, 30) {
		if base >= limit/2 {
			return limit
		}
		base *= 2
	}
	return min(base, limit)
}
