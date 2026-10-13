package proxytron

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// ErrTunnelRefused indicates that an HTTP proxy refused CONNECT.
var ErrTunnelRefused = errors.New("proxy refused the tunnel")

// ErrNoResponse indicates that a proxy closed before sending a status line.
var ErrNoResponse = errors.New("connection closed before a response")

// The limits on a CONNECT reply's head: the length of a line, and the
// number of lines after the status line, the blank one included.
const (
	maxLine    = 65536
	maxHeaders = 100
)

// connectTunnel asks an http(s) proxy for a tunnel: the CONNECT line,
// Proxy-Authorization when the proxy URL carries credentials, then Host.
// Only a 200 reply opens the tunnel; any other is ErrTunnelRefused.
func ConnectTunnel(c net.Conn, host string, port int, auth string) (net.Conn, error) {
	if port < 1 || port > 65535 || strings.ContainsAny(host+auth, "\r\n\x00") {
		return nil, errors.New("proxytron: invalid CONNECT target or authorization")
	}
	target := net.JoinHostPort(host, strconv.Itoa(port))
	var b strings.Builder
	b.WriteString("CONNECT " + target + " HTTP/1.1\r\n")
	if auth != "" {
		b.WriteString("Proxy-Authorization: " + auth + "\r\n")
	}
	b.WriteString("Host: " + target + "\r\n\r\n")
	if _, err := io.WriteString(c, b.String()); err != nil {
		return nil, err
	}
	br := bufio.NewReader(c)
	code, status, err := ReadConnectReply(br)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("%w: %s", ErrTunnelRefused, status)
	}
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: c, r: br}, nil
	}
	return c, nil
}

// readConnectReply reads a proxy's reply to CONNECT to the end of its head:
// the status line, whose code and status ("407 Proxy Authentication
// Required") it returns, then the header lines, skipped unread. It is
// lenient on purpose. Free proxies answer with header lines a strict reader
// such as http.ReadResponse refuses, and refusing them would bench routes
// that carry the tunnel fine; so only a status line that is not
// "HTTP/... <code> [reason]" fails, besides the limits (maxLine,
// maxHeaders). A connection that closes inside the header lines ends them.
func ReadConnectReply(br *bufio.Reader) (code int, status string, err error) {
	line, err := readReplyLine(br)
	if err != nil {
		return 0, "", err
	}
	if line == "" {
		return 0, "", ErrNoResponse
	}
	f := strings.Fields(line)
	if len(f) < 2 || !strings.HasPrefix(f[0], "HTTP/") {
		return 0, "", fmt.Errorf("malformed CONNECT status line %q", line)
	}
	code, err = strconv.Atoi(f[1])
	if err != nil || code < 100 || code > 999 {
		return 0, "", fmt.Errorf("malformed CONNECT status line %q", line)
	}
	for n := 1; ; n++ {
		line, err := readReplyLine(br)
		if err != nil {
			return 0, "", err
		}
		if n > maxHeaders {
			return 0, "", fmt.Errorf("CONNECT reply has more than %d lines after its status line", maxHeaders)
		}
		if line == "" || line == "\n" || line == "\r\n" {
			return code, strings.Join(f[1:], " "), nil
		}
	}
}

// readReplyLine reads one line of a CONNECT reply with its line ending: the
// rest of the stream when it ends without one, "" when it has ended.
func readReplyLine(br *bufio.Reader) (string, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxLine {
			return "", fmt.Errorf("CONNECT reply line longer than %d bytes", maxLine)
		}
		switch {
		case err == nil, errors.Is(err, io.EOF):
			return string(line), nil
		case !errors.Is(err, bufio.ErrBufferFull):
			return "", err
		}
	}
}

// bufferedConn hands out what a bufio.Reader already read before reading
// the connection again.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	if b.r.Buffered() > 0 {
		return b.r.Read(p)
	}
	return b.Conn.Read(p)
}

// resolveLocal resolves host on this machine, for a SOCKS proxy that is
// given an address (socks5, socks4): the first address, or the first IPv4
// one for SOCKS4.
func ResolveLocal(ctx context.Context, host, network string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		if network == "ip4" && ip.To4() == nil {
			return "", fmt.Errorf("socks4 cannot address IPv6 host %s", host)
		}
		return host, nil
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, network, host)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no address for %s", host)
	}
	return ips[0].String(), nil
}

// socks4Errors are the SOCKS4 reply codes that refuse a request.
var socks4Errors = map[byte]string{
	0x5B: "request rejected or failed",
	0x5C: "request rejected: the proxy cannot reach identd on the client",
	0x5D: "request rejected: identd reports a different user id",
}

// socks4Connect asks a SOCKS4 proxy to connect: by IPv4 address, or for
// socks4a by name (address 0.0.0.1 and the host after the user id).
func SOCKS4Connect(ctx context.Context, c net.Conn, scheme, user, host string, port int) error {
	if (scheme != "socks4" && scheme != "socks4a") || port < 1 || port > 65535 || strings.ContainsRune(user+host, '\x00') {
		return errors.New("proxytron: invalid SOCKS4 request")
	}
	var addr net.IP
	remote := false
	if ip := net.ParseIP(host).To4(); ip != nil {
		addr = ip
	} else if scheme == schemeSOCKS4A {
		addr, remote = net.IPv4(0, 0, 0, 1).To4(), true
	} else {
		resolved, err := ResolveLocal(ctx, host, "ip4")
		if err != nil {
			return err
		}
		addr = net.ParseIP(resolved).To4()
	}
	req := []byte{0x04, 0x01, byte(port >> 8), byte(port)}
	req = append(req, addr...)
	req = append(req, user...)
	req = append(req, 0)
	if remote {
		req = append(req, host...)
		req = append(req, 0)
	}
	if _, err := c.Write(req); err != nil {
		return err
	}
	var resp [8]byte
	if _, err := io.ReadFull(c, resp[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("connection closed unexpectedly")
		}
		return err
	}
	if resp[0] != 0x00 {
		return errors.New("socks4 proxy sent invalid data")
	}
	if status := resp[1]; status != 0x5A {
		msg, ok := socks4Errors[status]
		if !ok {
			msg = "unknown error"
		}
		return fmt.Errorf("%#02x: %s", status, msg) // "0x5b"
	}
	return nil
}
