package proxytron

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/net/proxy"
)

// Dialer opens explicit proxy routes, without environment proxy settings or
// fallback to a direct connection. Zero Timeout uses 20 seconds. Destination
// TLS belongs to the caller; ProxyTLSConfig configures only the outer hop.
type Dialer struct {
	ProxyURL       string
	Timeout        time.Duration
	ProxyTLSConfig *tls.Config
	Dial           func(context.Context, string, string) (net.Conn, error)
}

// DialContext opens a TCP stream through HTTP CONNECT or SOCKS. An empty
// ProxyURL or Direct selects a direct connection. SOCKS5 resolves locally;
// SOCKS5h and SOCKS4a resolve at the proxy. Context cancellation closes a
// connection still negotiating, and successful connections have no deadlines.
func (d Dialer) DialContext(ctx context.Context, network, address string) (conn net.Conn, err error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("proxytron: TCP is required")
	}
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := d.Dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: timeout}).DialContext
	}
	if d.ProxyURL == "" || IsDirect(d.ProxyURL) {
		return dial(ctx, network, address)
	}
	// ParseEndpoint validates but normalizes socks5, so retain the caller's
	// explicit local-DNS scheme when constructing the tunnel.
	endpoint, err := ParseEndpoint(d.ProxyURL, "http")
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(endpoint)
	if original, e := url.Parse(d.ProxyURL); e == nil && original.Scheme == "socks5" {
		u.Scheme = "socks5"
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("proxytron: invalid destination port")
	}
	raw, err := dial(ctx, network, u.Host)
	if err != nil {
		return nil, err
	}
	conn = raw
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer func() {
		stopped := stop()
		if err == nil && (!stopped || ctx.Err() != nil) {
			err = ctx.Err()
			if err == nil {
				err = context.Canceled
			}
		}
		if err != nil {
			_ = raw.Close()
			conn = nil
		} else {
			err = conn.SetDeadline(time.Time{})
			if err != nil {
				_ = raw.Close()
				conn = nil
			}
		}
	}()
	deadline, _ := ctx.Deadline()
	if err = raw.SetDeadline(deadline); err != nil {
		return nil, err
	}
	user, password := u.User.Username(), ""
	if u.User != nil {
		password, _ = u.User.Password()
	}
	switch u.Scheme {
	case "http", "https":
		if u.Scheme == "https" {
			cfg := &tls.Config{}
			if d.ProxyTLSConfig != nil {
				cfg = d.ProxyTLSConfig.Clone()
			}
			if cfg.ServerName == "" {
				cfg.ServerName = u.Hostname()
			}
			cfg.NextProtos = []string{"http/1.1"}
			secured := tls.Client(raw, cfg)
			if err = secured.HandshakeContext(ctx); err != nil {
				return nil, err
			}
			conn = secured
		}
		auth := ""
		if u.User != nil {
			auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
		}
		conn, err = ConnectTunnel(conn, host, port, auth)
	case "socks4", "socks4a":
		err = SOCKS4Connect(ctx, conn, u.Scheme, user, host, port)
	case "socks5", "socks5h":
		if u.Scheme == "socks5" {
			host, err = ResolveLocal(ctx, host, "ip")
			if err != nil {
				return nil, err
			}
		}
		var auth *proxy.Auth
		if u.User != nil {
			auth = &proxy.Auth{User: user, Password: password}
		}
		var socks proxy.Dialer
		socks, err = proxy.SOCKS5(network, u.Host, auth, connected{raw})
		if err == nil {
			conn, err = socks.(proxy.ContextDialer).DialContext(ctx, network, net.JoinHostPort(host, portText))
		}
	}
	return conn, err
}

type connected struct{ net.Conn }

func (c connected) Dial(string, string) (net.Conn, error)                         { return c.Conn, nil }
func (c connected) DialContext(context.Context, string, string) (net.Conn, error) { return c.Conn, nil }

// TransportOptions keeps proxy and destination TLS policy separate. Certificate
// verification is enabled by default on both hops. Insecure proxy TLS, when
// explicitly configured, never disables destination verification.
type TransportOptions struct {
	Timeout         time.Duration
	ProxyTLSConfig  *tls.Config
	TLSClientConfig *tls.Config
}

// NewTransport returns an isolated HTTP transport. Explicit routes ignore
// HTTP_PROXY, HTTPS_PROXY and NO_PROXY. HTTP(S) proxies forward HTTP requests
// and use the tolerant CONNECT implementation for HTTPS requests.
func NewTransport(endpoint string, opts TransportOptions) (*http.Transport, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	if opts.TLSClientConfig != nil {
		tr.TLSClientConfig = opts.TLSClientConfig.Clone()
	}
	d := Dialer{ProxyURL: endpoint, Timeout: opts.Timeout, ProxyTLSConfig: opts.ProxyTLSConfig}
	if endpoint == "" || IsDirect(endpoint) {
		return tr, nil
	}
	parsed, err := ParseEndpoint(endpoint, "http")
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(parsed)
	if u.Scheme == "http" || u.Scheme == "https" {
		tr.Proxy = func(req *http.Request) (*url.URL, error) {
			if req.URL.Scheme == "http" {
				return u, nil
			}
			return nil, nil
		}
		tr.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == u.Host && u.Scheme == "https" {
				cfg := &tls.Config{}
				if opts.ProxyTLSConfig != nil {
					cfg = opts.ProxyTLSConfig.Clone()
				}
				// Forwarded plaintext has no inner TLS protection.
				cfg.InsecureSkipVerify = false
				cfg.ServerName = u.Hostname()
				cfg.NextProtos = []string{"http/1.1"}
				dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 20 * time.Second}, Config: cfg}
				return dialer.DialContext(ctx, network, address)
			}
			conn, err := d.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			cfg := &tls.Config{}
			if opts.TLSClientConfig != nil {
				cfg = opts.TLSClientConfig.Clone()
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("proxytron: destination: %w", err)
			}
			if cfg.ServerName == "" {
				cfg.ServerName = host
			}
			cfg.NextProtos = []string{"http/1.1"}
			secured := tls.Client(conn, cfg)
			timeout := opts.Timeout
			if timeout <= 0 {
				timeout = 20 * time.Second
			}
			handshake, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			if err = secured.HandshakeContext(handshake); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return secured, nil
		}
	} else {
		tr.DialContext = d.DialContext
	}
	tr.ForceAttemptHTTP2 = false
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetHTTP1(true)
	return tr, nil
}
