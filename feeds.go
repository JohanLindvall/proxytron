package proxytron

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// The schemes a pool member may use: the ones the application transport can send
// a request through.
const (
	schemeHTTP    = "http"
	schemeHTTPS   = "https"
	schemeSOCKS4  = "socks4"
	schemeSOCKS4A = "socks4a"
	schemeSOCKS5  = "socks5"
	schemeSOCKS5H = "socks5h" // socks5 with the destination resolved by the proxy
)

// proxyScheme reports whether a pool member may use scheme.
func proxyScheme(scheme string) bool {
	switch scheme {
	case schemeHTTP, schemeHTTPS, schemeSOCKS4, schemeSOCKS4A, schemeSOCKS5, schemeSOCKS5H:
		return true
	}
	return false
}

// normalize turns one line of a list into a pool member: "" for a blank or
// comment line, Direct for "direct" in any letter case, a URL as it stands,
// and a bare host:port (the common open-proxy list shape) as a URL of
// scheme.
func normalize(line, scheme string) string {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return ""
	}
	if IsDirect(line) {
		return Direct
	}
	if strings.Contains(line, "://") {
		return line
	}
	return scheme + "://" + line
}

// staticEndpoints is the static endpoints cfg lists, normalised, in config
// order, each once.
func staticEndpoints(cfg *Config) []string {
	var out []string
	for _, line := range cfg.Endpoints {
		if u := normalize(line, cfg.DefaultScheme); u != "" && !slices.Contains(out, u) {
			out = append(out, u)
		}
	}
	return out
}

// uniqueURLs is urls with surrounding space trimmed, blanks dropped and
// each URL kept at its first place only.
func uniqueURLs(urls []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, u := range urls {
		if u = strings.TrimSpace(u); u != "" && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// Redact never lets proxy credentials into a log line or a page, if the
// endpoint carries any.
func Redact(url string) string {
	if strings.Contains(url, "@") {
		scheme, rest, found := strings.Cut(url, "://")
		if !found {
			scheme, rest = url, ""
		}
		if scheme == "" {
			return "***"
		}
		if i := strings.LastIndex(rest, "@"); i >= 0 {
			rest = rest[i+1:]
		}
		return scheme + "://***@" + rest
	}
	return url
}

// fingerprint is url's SHA-256 in hex: what marks an endpoint as retired
// without keeping its address or credentials.
func fingerprint(url string) string {
	sum := sha256.Sum256([]byte(url))
	return hex.EncodeToString(sum[:])
}

// -- feed bodies ------------------------------------------------------------------

// feedRecord is one endpoint a feed lists, with the feed's record of it
// ({} for a bare URL).
type feedRecord struct {
	url      string
	metadata json.RawMessage
}

// The ways a feed body can fail as a whole. A feed that fails keeps its
// previous list.
var (
	errFeedNotArray      = errors.New("proxy JSON must be an array")
	errNoUsableEndpoints = errors.New("proxy feed contains no usable endpoints for this country")
)

// parseFeed reads a feed body: a JSON array of endpoint URLs and Proxifly
// records, or plain text with one endpoint per line. It returns the usable
// endpoints in feed order, each with the record that listed it first; a
// later listing of the same endpoint is ignored. country, when it is not
// "", is the scope the feed serves, and a record that names another
// country is skipped. The result is never nil.
//
// Feed scores do not influence rotation. Malformed records cannot become
// proxy URLs. Errors are undecodable JSON, JSON that is not an array, and a
// non-empty feed with nothing usable for this country.
func parseFeed(body []byte, scheme, country string) ([]feedRecord, error) {
	items, err := feedItems(body)
	if err != nil {
		return nil, err
	}
	records := []feedRecord{}
	listed := map[string]bool{}
	for _, it := range items {
		url, ok := endpointURL(it.line, scheme)
		if ok && !listed[url] && it.inCountry(country) {
			listed[url] = true
			records = append(records, feedRecord{url: url, metadata: it.record})
		}
	}
	if len(items) > 0 && len(records) == 0 {
		return nil, errNoUsableEndpoints
	}
	return records, nil
}

// feedItem is one entry of a feed, not yet validated: the endpoint as the
// feed wrote it, the country its record names (nil when it names none) and
// the record itself ({} for a bare URL or a line of text).
type feedItem struct {
	line    string
	country json.RawMessage
	record  json.RawMessage
}

// noRecord is the record of an endpoint a feed lists without one.
func noRecord() json.RawMessage { return json.RawMessage("{}") }

// feedItems splits a feed body into its entries: the elements of a JSON
// array, or else the lines of plain text that are neither blank nor '#'
// comments, a line ending at a line feed, a carriage return or both. A
// leading byte-order mark or blank space is ignored. An element that is
// neither a URL nor a record with a "proxy" URL is an entry that names no
// endpoint.
//
// The body is read as UTF-8, a byte that is not becoming U+FFFD, so the
// records stored from it are valid text.
func feedItems(body []byte) ([]feedItem, error) {
	body = bytes.TrimLeft(bytes.ToValidUTF8(body, []byte("\uFFFD")), "\ufeff \t\r\n")
	if len(body) == 0 || (body[0] != '[' && body[0] != '{') {
		var items []feedItem
		for _, line := range strings.FieldsFunc(string(body), lineBreak) {
			if t := strings.TrimSpace(line); t != "" && !strings.HasPrefix(t, "#") {
				items = append(items, feedItem{line: t, record: noRecord()})
			}
		}
		return items, nil
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(body, &elements); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return nil, errFeedNotArray
		}
		return nil, err
	}
	items := make([]feedItem, 0, len(elements))
	for _, raw := range elements {
		items = append(items, jsonItem(raw))
	}
	return items, nil
}

func lineBreak(r rune) bool { return r == '\n' || r == '\r' }

// jsonItem is one element of a JSON feed: a bare URL, or a record whose
// "proxy" is the URL and whose "geolocation" may name its "country". Any
// other element yields an entry with no URL.
func jsonItem(raw json.RawMessage) feedItem {
	var url string
	if json.Unmarshal(raw, &url) == nil {
		return feedItem{line: url, record: noRecord()}
	}
	var record map[string]json.RawMessage
	if json.Unmarshal(raw, &record) != nil || json.Unmarshal(record["proxy"], &url) != nil {
		return feedItem{}
	}
	var geolocation map[string]json.RawMessage
	_ = json.Unmarshal(record["geolocation"], &geolocation) // none, or not an object: no country
	return feedItem{line: url, country: geolocation["country"], record: raw}
}

// inCountry reports whether the entry may serve the scope code (""
// for a feed every scope shares): it may unless its record names
// another country. null and "" name none; a country that is not a string
// is no scope's. Letter case does not matter.
func (it feedItem) inCountry(code string) bool {
	if code == "" || it.country == nil {
		return true
	}
	var named *string
	if err := json.Unmarshal(it.country, &named); err != nil {
		return false
	}
	return named == nil || *named == "" || strings.EqualFold(*named, code)
}

// endpointURL is the pool member a feed entry names, and whether it names a
// usable one: a URL, or a bare host:port taking scheme, whose scheme a pool
// member may use (proxyScheme), with a host and a port from 1 to 65535 and
// nothing after them but a "/". Space anywhere makes it unusable.
//
// The URL is rebuilt from the scheme, lowercased, and the authority exactly
// as the feed wrote it, credentials and letter case included, because it
// keys the endpoint's stored record and its pacing; net/url's own String
// would escape a non-ASCII host. A socks5:// endpoint becomes socks5h://:
// the proxy resolves the destination's name, as a configured socks5h
// tunnel does.
func endpointURL(line, scheme string) (string, bool) {
	raw := normalize(line, scheme)
	if raw == "" || strings.ContainsFunc(raw, unicode.IsSpace) {
		return "", false
	}
	u, err := neturl.Parse(raw)
	if err != nil || !proxyScheme(u.Scheme) ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" {
		return "", false
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return "", false
	}
	// The authority runs from the scheme's "://" to the first "/", "?" or
	// "#", as net/url split it.
	_, authority, _ := strings.Cut(raw, "://")
	if i := strings.IndexAny(authority, "/?#"); i >= 0 {
		authority = authority[:i]
	}
	if u.Scheme == schemeSOCKS5 {
		u.Scheme = schemeSOCKS5H
	}
	return u.Scheme + "://" + authority, true
}

// -- downloading ------------------------------------------------------------------

// feedTimeout bounds connecting to a feed host, the TLS handshake as a
// whole, and each read or write after that; never the whole download, so a
// feed that keeps arriving is never cut off.
const feedTimeout = 20 * time.Second

// newFeedClient is the HTTP client a pool downloads its feeds through,
// unless WithHTTPClient gives it another: net/http with the environment's
// proxy settings, speaking HTTP/1.1 only, with timeout bounding the
// connect, the TLS handshake and then each read and each write. Over TLS a
// read is one TLS record (DeadlineConn over the *tls.Conn), so a
// host dripping a record a byte at a time still times out. ALPN is set to
// http/1.1 explicitly: without it crypto/tls sends no ALPN extension, and
// the feed hosts have always seen one. roots verifies their certificates;
// nil is the system pool.
//
// The client is net/http's on purpose, not the application transport's: the feeds
// are GitHub files, and the browser identity the destination sees has no business
// on them.
func newFeedClient(timeout time.Duration, roots *x509.CertPool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	tlsConfig := &tls.Config{RootCAs: roots, NextProtos: []string{"http/1.1"}}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Plain http://, and the hop to an environment proxy.
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return DeadlineConn{Conn: conn, Timeout: timeout}, nil
	}
	// https:// straight to the host, or to an https:// environment proxy.
	tr.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		raw, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		cfg := tlsConfig.Clone()
		cfg.ServerName = host
		conn := tls.Client(raw, cfg)
		if err := handshake(ctx, conn, timeout); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return DeadlineConn{Conn: conn, Timeout: timeout}, nil
	}
	// The destination's handshake through a plaintext environment proxy,
	// which net/http makes itself.
	tr.TLSClientConfig = tlsConfig
	tr.TLSHandshakeTimeout = timeout
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetHTTP1(true)
	return &http.Client{Transport: tr}
}

// handshake runs conn's TLS handshake under one deadline, timeout from now,
// however many reads and writes it takes.
func handshake(ctx context.Context, conn *tls.Conn, timeout time.Duration) error {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if err := conn.HandshakeContext(ctx); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

// download is the body of the feed at url. A status of 400 or above is an
// error, and so is a failure to get or read the response.
func download(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		// The *url.Error repeats the URL the log line names already.
		var ue *neturl.Error
		if errors.As(err, &ue) {
			return nil, ue.Err
		}
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (32<<20)+1))
	if len(body) > 32<<20 {
		return nil, errors.New("proxy feed is too large")
	}
	return body, err
}

// fetchList downloads and parses one feed: its endpoints in feed order,
// each with its record's metadata. nil when the download or the parse
// failed, which is logged, since a bad source must not stop a pass; a
// download that ctx's end cut short is not logged.
//
// A panic is recovered and logged the same way: fetchList runs on a
// goroutine of its own, where it would otherwise take the server down.
func fetchList(ctx context.Context, client *http.Client, url, scheme, country string, logs ...*slog.Logger) (records []feedRecord) {
	log := slog.Default()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	defer func() {
		if r := recover(); r != nil {
			log.WarnContext(ctx, fmt.Sprintf("proxy refresh from %s failed: %v", Redact(url), r))
			records = nil
		}
	}()
	body, err := download(ctx, client, url)
	if err == nil {
		records, err = parseFeed(body, scheme, country)
	}
	if err != nil {
		if ctx.Err() == nil {
			log.WarnContext(ctx, fmt.Sprintf("proxy refresh from %s failed: %s", Redact(url), err))
		}
		return nil
	}
	return records
}
