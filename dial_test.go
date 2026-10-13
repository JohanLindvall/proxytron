package proxytron_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func tunnelProxy(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			t.Errorf("method = %s", r.Method)
			w.WriteHeader(405)
			return
		}
		upstream, err := net.DialTimeout("tcp", r.Host, time.Second)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer upstream.Close()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		// A real compatibility quirk: a nonconforming response header does
		// not make an otherwise usable proxy tunnel fail.
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connected\r\nnonconforming header\r\n\r\n")
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close(); close(done) }()
		_, _ = io.Copy(conn, upstream)
		_ = conn.Close()
		<-done
	}))
}

func TestHTTPSProxyTrustDoesNotDisableDestinationVerification(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "verified") }))
	defer target.Close()
	proxy := tunnelProxy(t)
	defer proxy.Close()
	for _, trustDestination := range []bool{false, true} {
		roots := x509.NewCertPool()
		if trustDestination {
			roots.AddCert(target.Certificate())
		}
		tr, err := proxytron.NewTransport(proxy.URL, proxytron.TransportOptions{
			ProxyTLSConfig:  &tls.Config{InsecureSkipVerify: true},
			TLSClientConfig: &tls.Config{RootCAs: roots}, Timeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
		resp, err := client.Get(target.URL)
		if !trustDestination {
			if err == nil {
				resp.Body.Close()
				t.Fatal("untrusted destination was accepted")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || string(body) != "verified" {
				t.Fatalf("body %q, %v", body, err)
			}
		}
		tr.CloseIdleConnections()
	}
}

func TestDialerCancellationClosesNegotiatingConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	d := proxytron.Dialer{ProxyURL: "http://proxy.test:80", Timeout: time.Second,
		Dial: func(context.Context, string, string) (net.Conn, error) { return client, nil }}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := d.DialContext(ctx, "tcp", "target.test:443"); result <- err }()
	br := bufio.NewReader(server)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled negotiation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("negotiation ignored cancellation")
	}
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("connection still open: %v", err)
	}
}

func TestTunnelKeepsBufferedBytesAndBoundsHeaders(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nOdd header\r\n\r\npayload")
	}()
	conn, err := proxytron.ConnectTunnel(client, "target.test", 443, "")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(conn)
	if err != nil || string(body) != "payload" {
		t.Fatalf("buffered bytes %q, %v", body, err)
	}
	_, _, err = proxytron.ReadConnectReply(bufio.NewReader(strings.NewReader("HTTP/1.1 200 OK\r\nX: " + strings.Repeat("x", 65536) + "\r\n\r\n")))
	if err == nil {
		t.Fatal("unbounded CONNECT response accepted")
	}
}
