package proxytron

// The HTTP client the feeds download through (newFeedClient, download,
// fetchList): what a feed host sees of it, and how its timeouts, statuses
// and redirects end a download.

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron/internal/logtest"
)

// localhostCert is a self-signed certificate for "localhost" and a pool
// that trusts it, so a feed client can verify a test host by name and send
// that name as its SNI.
func localhostCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots
}

// clientHello is what a TLS server sees of a ClientHello.
type clientHello struct {
	ServerName string
	ALPN       []string
	Versions   []uint16
	Suites     []uint16
	Curves     []tls.CurveID
	Points     []uint8
	Schemes    []tls.SignatureScheme
	Extensions []uint16
}

func helloOf(h *tls.ClientHelloInfo) clientHello {
	return clientHello{h.ServerName, h.SupportedProtos, h.SupportedVersions, h.CipherSuites,
		h.SupportedCurves, h.SupportedPoints, h.SignatureSchemes, h.Extensions}
}

// helloHost is a TLS host on 127.0.0.1 that records every ClientHello and
// the head of every request, byte for byte, and answers each with body.
type helloHost struct {
	ln    net.Listener
	mu    sync.Mutex
	hello []clientHello
	heads []string
}

func newHelloHost(t *testing.T, cert tls.Certificate, body string) *helloHost {
	t.Helper()
	h := &helloHost{}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"http/1.1"},
		GetConfigForClient: func(info *tls.ClientHelloInfo) (*tls.Config, error) {
			h.mu.Lock()
			h.hello = append(h.hello, helloOf(info))
			h.mu.Unlock()
			return nil, nil
		},
	}
	ln, err := tls.Listen("tcp4", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.ln = ln
	var wg sync.WaitGroup
	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { h.answer(conn, body) })
		}
	})
	return h
}

// answer records the request head conn carries and answers it with body.
// A connection that closes without a request (referenceHello's) records
// nothing.
func (h *helloHost) answer(conn net.Conn, body string) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	var head strings.Builder
	for {
		line, err := r.ReadString('\n')
		head.WriteString(line)
		if err != nil || line == "\r\n" {
			break
		}
	}
	if head.Len() == 0 {
		return
	}
	h.mu.Lock()
	h.heads = append(h.heads, head.String())
	h.mu.Unlock()
	_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"+
		"Content-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"+body)
}

func (h *helloHost) port() string {
	_, port, _ := net.SplitHostPort(h.ln.Addr().String())
	return port
}

// referenceHello is the ClientHello crypto/tls sends with only a server name
// and ALPN http/1.1 set: what the feed downloads have always sent.
func referenceHello(t *testing.T, h *helloHost, roots *x509.CertPool) clientHello {
	t.Helper()
	conn, err := tls.Dial("tcp4", h.ln.Addr().String(),
		&tls.Config{ServerName: "localhost", RootCAs: roots, NextProtos: []string{"http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	h.mu.Lock()
	defer h.mu.Unlock()
	ref := h.hello[len(h.hello)-1]
	h.hello = h.hello[:len(h.hello)-1]
	return ref
}

// The feed host sees the ClientHello and the request head the feed
// downloads have always sent: Go's own TLS hello with ALPN held to
// http/1.1, and three headers in net/http's order and spelling. (Both were
// recorded from the requests-shaped client this one replaced, and are
// unchanged.)
func TestFeedClient_HelloAndHeadersAreFrozen(t *testing.T) {
	cert, roots := localhostCert(t)
	host := newHelloHost(t, cert, `["http://h:80"]`)
	url := "https://localhost:" + host.port() + "/SE/data.json"
	body, err := download(t.Context(), newFeedClient(feedTimeout, roots), url)
	if err != nil || string(body) != `["http://h:80"]` {
		t.Fatalf("download = %q, %v", body, err)
	}
	ref := referenceHello(t, host, roots)
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.hello) != 1 || len(host.heads) != 1 {
		t.Fatalf("%d hellos, %d heads; want one of each", len(host.hello), len(host.heads))
	}
	got := host.hello[0]
	if got.ServerName != "localhost" || !reflect.DeepEqual(got.ALPN, []string{"http/1.1"}) {
		t.Errorf("SNI %q, ALPN %q; want localhost and [http/1.1]", got.ServerName, got.ALPN)
	}
	if !reflect.DeepEqual(got, ref) {
		t.Errorf("ClientHello\n%+v\nwant crypto/tls's own with ALPN http/1.1\n%+v", got, ref)
	}
	want := "GET /SE/data.json HTTP/1.1\r\nHost: localhost:" + host.port() + "\r\n" +
		"User-Agent: Go-http-client/1.1\r\nAccept-Encoding: gzip\r\n\r\n"
	if host.heads[0] != want {
		t.Errorf("request head\n%q\nwant\n%q", host.heads[0], want)
	}
}

// feedHost is a plain http:// feed host: /json serves a list, /slow the
// same list a byte every 60 ms, /silent nothing for a second, /r/N a chain
// of N redirects ending at the list, and any other path its own status
// (/404, /502).
func feedHost(t *testing.T) *httptest.Server {
	t.Helper()
	const list = `["http://h:80"]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, list)
		case path == "/slow":
			for _, c := range []byte(list) {
				_, _ = w.Write([]byte{c})
				w.(http.Flusher).Flush()
				time.Sleep(60 * time.Millisecond)
			}
		case path == "/silent":
			time.Sleep(time.Second)
			_, _ = io.WriteString(w, list)
		case strings.HasPrefix(path, "/r/"):
			n, _ := strconv.Atoi(strings.TrimPrefix(path, "/r/"))
			if n > 0 {
				http.Redirect(w, r, fmt.Sprintf("/r/%d", n-1), http.StatusFound)
				return
			}
			_, _ = io.WriteString(w, list)
		default:
			code, _ := strconv.Atoi(strings.TrimPrefix(path, "/"))
			w.WriteHeader(code)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A download is the body, or an error that says why there is none: an
// error status, a stalled read, too many redirects (net/http's limit: the
// tenth is not followed), a refused connection or the end of ctx. The
// timeout bounds each read, never the whole download.
func TestDownload_EndsWithTheBodyOrWhy(t *testing.T) {
	srv := feedHost(t)
	refused := func() string { // a port nothing listens on
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_ = ln.Close()
		return "http://" + ln.Addr().String() + "/json"
	}()
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		url     string
		timeout time.Duration
		want    string // the body, or "ERR " and the error's text, or "ERR*" and a prefix
		wantIs  error
	}{
		{"a list", t.Context(), srv.URL + "/json", feedTimeout, `["http://h:80"]`, nil},
		{"a feed that keeps arriving", t.Context(), srv.URL + "/slow", 250 * time.Millisecond, `["http://h:80"]`, nil},
		{"a host that falls silent", t.Context(), srv.URL + "/silent", 200 * time.Millisecond, "", os.ErrDeadlineExceeded},
		{"not found", t.Context(), srv.URL + "/404", feedTimeout, "ERR status 404 Not Found", nil},
		{"bad gateway", t.Context(), srv.URL + "/502", feedTimeout, "ERR status 502 Bad Gateway", nil},
		{"nine redirects", t.Context(), srv.URL + "/r/9", feedTimeout, `["http://h:80"]`, nil},
		{"ten redirects", t.Context(), srv.URL + "/r/10", feedTimeout, "ERR stopped after 10 redirects", nil},
		{"refused", t.Context(), refused, feedTimeout, "ERR*dial tcp ", nil},
		{"ctx ended", ended, srv.URL + "/json", feedTimeout, "", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := download(tc.ctx, newFeedClient(tc.timeout, nil), tc.url)
			got := string(body)
			if err != nil {
				got = "ERR " + err.Error()
				var ue *neturl.Error
				if errors.As(err, &ue) {
					t.Errorf("%v: the *url.Error repeats the URL", err)
				}
			}
			switch {
			case tc.wantIs != nil:
				if !errors.Is(err, tc.wantIs) {
					t.Errorf("err = %v, want %v", err, tc.wantIs)
				}
			case strings.HasPrefix(tc.want, "ERR*"):
				if !strings.HasPrefix(got, "ERR "+strings.TrimPrefix(tc.want, "ERR*")) {
					t.Errorf("got %q, want %q...", got, tc.want)
				}
			case got != tc.want:
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A download that fails, a body that is no usable feed and a panic on the
// way are logged and leave the source's previous list alone (nil); a
// download that ctx's end cut short is not worth a line.
func TestFetchList_LogsWhatFailed(t *testing.T) {
	srv := feedHost(t)
	client := newFeedClient(feedTimeout, nil)
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	bad := stubFeeds(feedText(`{"error":"unavailable"}`)).client()
	panicking := stubFeeds(func(string) (string, error) { panic("boom") }).client()
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		client *http.Client
		url    string
		keys   []string
		line   string
	}{
		{"a list", t.Context(), client, srv.URL + "/json", []string{"http://h:80"}, ""},
		{"not found", t.Context(), client, srv.URL + "/404", nil,
			"proxy refresh from " + srv.URL + "/404 failed: status 404 Not Found"},
		{"not a feed", t.Context(), bad, "https://feeds.test/data.json", nil,
			"proxy refresh from https://feeds.test/data.json failed: proxy JSON must be an array"},
		{"ctx ended", ended, client, srv.URL + "/json", nil, ""},
		{"a panic on the way", t.Context(), panicking, "https://feeds.test/data.json", nil,
			"proxy refresh from https://feeds.test/data.json failed: boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := logtest.Capture(t, slog.LevelWarn)
			records := fetchList(tc.ctx, tc.client, tc.url, "http", "")
			var keys []string
			for _, r := range records {
				keys = append(keys, r.url)
			}
			if !slices.Equal(keys, tc.keys) || (records == nil) != (tc.keys == nil) {
				t.Errorf("records = %v, want %v", keys, tc.keys)
			}
			var want []string
			if tc.line != "" {
				want = []string{tc.line}
			}
			if got := rec.Lines(); !slices.Equal(got, want) {
				t.Errorf("logged %q, want %q", got, want)
			}
		})
	}
}

// dripRelay forwards TCP connections to target: the client's bytes straight
// through, the server's at full speed until drip is set, then a byte every
// gap. Each socket read the client makes then returns within gap, so only a
// deadline that spans many reads can time out.
type dripRelay struct {
	t      *testing.T
	ln     net.Listener
	target string
	gap    time.Duration
	drip   atomic.Bool
}

func newDripRelay(t *testing.T, target string, gap time.Duration) *dripRelay {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &dripRelay{t: t, ln: ln, target: target, gap: gap}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp4", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			track(client)
			track(server)
			wg.Go(func() { _, _ = io.Copy(server, client) })
			wg.Go(func() {
				defer func() { _ = client.Close() }()
				buf := make([]byte, 32<<10)
				for {
					n, err := server.Read(buf)
					for i := 0; i < n; {
						if !r.drip.Load() {
							if _, werr := client.Write(buf[i:n]); werr != nil {
								return
							}
							break
						}
						if _, werr := client.Write(buf[i : i+1]); werr != nil {
							return
						}
						i++
						time.Sleep(r.gap)
					}
					if err != nil {
						return
					}
				}
			})
		}
	})
	return r
}

func (r *dripRelay) url(path string) string { return "https://" + r.ln.Addr().String() + path }

// tlsFeedHost is an https:// feed host and a feed client that trusts its
// certificate, with the given timeout.
func tlsFeedHost(t *testing.T, timeout time.Duration, h http.HandlerFunc) (*httptest.Server, *http.Client) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	// The relays cut handshakes short on purpose; the server need not say so.
	srv.Config.ErrorLog = stdlog.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	return srv, newFeedClient(timeout, roots)
}

// downloadWithin runs download, failing the test if it has not returned
// within limit (a deadline per socket read would wait for a drip to end).
func downloadWithin(t *testing.T, limit time.Duration, client *http.Client, url string) (string, error) {
	t.Helper()
	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		body, err := download(t.Context(), client, url)
		done <- result{body, err}
	}()
	select {
	case r := <-done:
		return string(r.body), r.err
	case <-time.After(limit):
		t.Fatalf("download(%s) still running after %s", url, limit)
		return "", nil
	}
}

// Over TLS the timeout bounds the handshake as a whole and each TLS read as
// a whole (DeadlineConn over the *tls.Conn), however many socket
// reads either takes: a host dripping its handshake, or a record of the
// body, a byte at a time times out, where a deadline per socket read would
// hang a feed refresh, and the pass waiting for it, for as long as the host
// kept dripping. Records that each arrive in time are read however long the
// whole takes.
func TestFeedClient_BoundsTheTLSHandshakeAndEachTLSRead(t *testing.T) {
	body := `["http://h:80"]`
	var bodyRelay atomic.Pointer[dripRelay] // the relay /drip-body is fetched through
	srv, client := tlsFeedHost(t, 300*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/paced": // a record every 60 ms: each TLS read is quick
			for _, c := range []byte(body) {
				_, _ = w.Write([]byte{c})
				w.(http.Flusher).Flush()
				time.Sleep(60 * time.Millisecond)
			}
		case "/drip-body":
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			time.Sleep(150 * time.Millisecond) // the headers are through
			bodyRelay.Load().drip.Store(true)
			_, _ = w.Write([]byte(strings.Repeat(" ", 2048) + body)) // one record, dripped
		default:
			_, _ = w.Write([]byte(body))
		}
	})

	// Records arriving one by one, inside the timeout each, are served
	// however long the whole takes (about 1 s here, against 300 ms).
	if got, err := downloadWithin(t, 10*time.Second, client, srv.URL+"/paced"); err != nil || got != body {
		t.Errorf("a feed that keeps arriving: %q, %v", got, err)
	}

	// The handshake dripped from its first byte: a few kilobytes at 5 ms a
	// byte is many seconds, every socket read inside 5 ms.
	hs := newDripRelay(t, srv.Listener.Addr().String(), 5*time.Millisecond)
	hs.drip.Store(true)
	start := time.Now()
	if _, err := downloadWithin(t, 10*time.Second, client, hs.url("/")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("dripping handshake: %v, want a timeout", err)
	} else if took := time.Since(start); took > 3*time.Second {
		t.Errorf("dripping handshake timed out after %s; the bound is the handshake's 300 ms", took)
	}

	// One body record dripped at 2 ms a byte after the headers came through.
	rb := newDripRelay(t, srv.Listener.Addr().String(), 2*time.Millisecond)
	bodyRelay.Store(rb)
	start = time.Now()
	if _, err := downloadWithin(t, 10*time.Second, client, rb.url("/drip-body")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("dripping body record: %v, want a timeout", err)
	} else if took := time.Since(start); took > 3*time.Second {
		t.Errorf("dripping body record timed out after %s; the bound is one TLS read's 300 ms", took)
	}
}
