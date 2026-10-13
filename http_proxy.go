package proxytron

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

// SetHTTPProxy configures an unused net/http transport in place, preserving
// its destination TLS verification, dialer, CONNECT hooks and other settings.
// HTTP(S) and SOCKS5(h) use net/http's native proxy support and remote DNS.
// Direct clears proxy selection. Explicit routes ignore environment bypasses.
//
// proxyTLS applies only to an HTTPS proxy's outer connection, even for forwarded
// HTTP requests. Nil verifies that certificate normally. Clone a transport
// before configuring it; changing a transport already serving requests is unsafe.
// Use NewTransport or Dialer when SOCKS4 or tolerant CONNECT replies are needed.
func SetHTTPProxy(tr *http.Transport, endpoint string, proxyTLS *tls.Config) error {
	if tr == nil {
		return errors.New("proxytron: nil transport")
	}
	if IsDirect(endpoint) {
		tr.Proxy = nil
		return nil
	}
	raw, err := ParseEndpoint(endpoint, "http")
	if err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	switch u.Scheme {
	case "http", "https", "socks5h":
	default:
		return errors.New("proxytron: protocol is not supported by net/http's native proxy transport")
	}
	tr.Proxy = http.ProxyURL(u)
	if u.Scheme != "https" {
		return nil
	}
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: 20 * time.Second}).DialContext
	}
	cfg := &tls.Config{}
	if proxyTLS != nil {
		cfg = proxyTLS.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = u.Hostname()
	}
	cfg.NextProtos = []string{"http/1.1"}
	timeout := tr.TLSHandshakeTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	tr.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != u.Host {
			return nil, errors.New("proxytron: unexpected HTTPS proxy address")
		}
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		secured := tls.Client(conn, cfg)
		handshake, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := secured.HandshakeContext(handshake); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return secured, nil
	}
	return nil
}
