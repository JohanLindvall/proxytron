package proxytron

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
)

// A line of a list becomes a pool member: a bare host:port takes the
// list's scheme, a URL stands, "direct" in any letter case is Direct, and
// blank and comment lines are nothing. A dotted capital "İ" has no simple
// case folding to "i", so "DİRECT" is a host name, not the home address.
func TestNormalize(t *testing.T) {
	for _, c := range []struct{ line, scheme, want string }{
		{"1.2.3.4:1080", defaultScheme, "socks5h://1.2.3.4:1080"},
		{"1.2.3.4:1080", "socks4", "socks4://1.2.3.4:1080"},
		{"http://1.2.3.4:3128", defaultScheme, "http://1.2.3.4:3128"},
		{"  # comment", defaultScheme, ""},
		{" \t", defaultScheme, ""},
		{"direct", defaultScheme, Direct},
		{"DIRECT", "http", Direct},
		{" Direct ", "http", Direct},
		{"D\u0130RECT", "socks5h", "socks5h://D\u0130RECT"},
		{"d\u0130rect", "http", "http://d\u0130rect"},
	} {
		if got := normalize(c.line, c.scheme); got != c.want {
			t.Errorf("normalize(%q, %q) = %q, want %q", c.line, c.scheme, got, c.want)
		}
	}
}

// Credentials never reach a log line or a page, whatever shape the
// endpoint has.
func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"socks5h://user:pw@h:1": "socks5h://***@h:1",
		"socks5h://h:1":         "socks5h://h:1",
		"socks5h://a:b@c@d:1":   "socks5h://***@d:1",
		"http://h:1":            "http://h:1",
		"user@host":             "user@host://***@",
		"://a@b":                "***",
		Direct:                  Direct,
	} {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

// The static endpoints are the config's lines normalised, in config order,
// each once.
func TestStaticEndpoints(t *testing.T) {
	cfg := proxyCfg(t, withEndpoints(" 1.2.3.4:1080 ", "# off", "socks5h://1.2.3.4:1080", "DIRECT", "direct",
		"http://b:3128", ""))
	if got, want := staticEndpoints(&cfg), []string{"socks5h://1.2.3.4:1080", Direct, "http://b:3128"}; !slices.Equal(got, want) {
		t.Errorf("staticEndpoints = %q, want %q", got, want)
	}
	if got := staticEndpoints(new(proxyCfg(t))); got != nil {
		t.Errorf("no endpoints: %q", got)
	}
}

func TestUniqueURLs(t *testing.T) {
	for _, c := range []struct{ in, want []string }{
		{nil, nil},
		{[]string{" ", ""}, nil},
		{[]string{" http://a:1 ", "http://b:2", "http://a:1", "", "http://b:2 "}, []string{"http://a:1", "http://b:2"}},
	} {
		if got := uniqueURLs(c.in); !slices.Equal(got, c.want) {
			t.Errorf("uniqueURLs(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// A feed entry is a usable endpoint only with a scheme the transport
// speaks, a host and a port in 1..65535, and nothing after them but a
// slash. The URL keeps the authority exactly as the feed wrote it, since it
// keys the endpoint's stored record and its pacing; socks5 becomes socks5h.
func TestEndpointURL(t *testing.T) {
	for _, c := range []struct {
		line, scheme string
		want         string // "" for unusable
	}{
		// usable
		{"1.2.3.4:1080", "socks5h", "socks5h://1.2.3.4:1080"},
		{"1.2.3.4:1080", "socks5", "socks5h://1.2.3.4:1080"},
		{"SOCKS5://h:1080", "http", "socks5h://h:1080"},
		{"socks5h://a:1080", "http", "socks5h://a:1080"},
		{"socks4://h:1080", "http", "socks4://h:1080"},
		{"socks4a://h:1", "http", "socks4a://h:1"},
		{"HTTPS://A.B:443/", "http", "https://A.B:443"},
		{"http://H.Example:0080", "http", "http://H.Example:0080"},
		{"http://h:65535", "http", "http://h:65535"},
		{"http://h:80?", "http", "http://h:80"},
		{"http://h:80#", "http", "http://h:80"},
		{"http://[::1]:8080", "http", "http://[::1]:8080"},
		{"http://[fe80::1%25en0]:80", "http", "http://[fe80::1%25en0]:80"},
		{"socks5://user:pa@ss@h:1080", "http", "socks5h://user:pa@ss@h:1080"},
		{"https://u@h:443", "http", "https://u@h:443"},
		{"http://\u00e4.de:80", "http", "http://\u00e4.de:80"},
		{"http://\u0131:80", "http", "http://\u0131:80"},
		// unusable
		{"", "http", ""},
		{"# comment", "http", ""},
		{"direct", "http", ""},
		{"ftp://h:21", "http", ""},
		{"http://h", "http", ""},
		{"1.2.3.4", "http", ""},
		{"http://h:", "http", ""},
		{"http://:80", "http", ""},
		{"http://h:0", "http", ""},
		{"http://h:65536", "http", ""},
		{"http://h:99999999999999999999", "http", ""},
		{"http://h:+80", "http", ""},
		{"http://h:\u0668\u0660", "http", ""},
		{"http://a:1:2", "http", ""},
		{"http://h:80/path", "http", ""},
		{"http://h:80?x", "http", ""},
		{"http://h:80#f", "http", ""},
		{"http://a :80", "http", ""},
		{"http://a\u3000b:80", "http", ""},
		{"\x01http://h:80", "http", ""},
		{"http://[::1:80", "http", ""},
		{"http://[1.2.3.4]:80", "http", ""},
		{"http://[v1.x]:80", "http", ""},
		{"http://%41:80", "http", ""},
		{"http://[::1%en0]:80", "http", ""},
		{"http://u^s:p@h:80", "http", ""},
	} {
		got, ok := endpointURL(c.line, c.scheme)
		if got != c.want || ok != (c.want != "") {
			t.Errorf("endpointURL(%q, %q) = %q, %v; want %q", c.line, c.scheme, got, ok, c.want)
		}
	}
}

// urlsOf is the endpoints of parsed feed records, in order.
func urlsOf(records []feedRecord) []string {
	urls := make([]string, 0, len(records))
	for _, r := range records {
		urls = append(urls, r.url)
	}
	return urls
}

// proxiflyBody is a Proxifly feed for amazon.se as such a feed mixes them:
// records with metadata, a bare URL repeating the first record's endpoint,
// a record of another country, and entries that are no usable endpoint.
const proxiflyBody = `[{"proxy": "socks5://192.0.2.7:1080", "protocol": "socks5", "https": false, ` +
	`"score": 1, "geolocation": {"country": "SE", "city": "Vaxjo"}}, ` +
	`{"proxy": "https://192.0.2.10:443", "protocol": "https"}, ` +
	`{"proxy": "http://192.0.2.8:80", "https": false}, ` +
	`"socks5h://192.0.2.7:1080", ` +
	`{"proxy": "http://other.example:80", "geolocation": {"country": "DE"}}, ` +
	`null, 7, {"proxy": null}, {}, {"proxy": ["http://bad.example:80"]}, ` +
	`"ftp://bad.example:21", "http://bad.example:99999", "http://:80", ` +
	`"http://bad.example:80/path", "http://bad.example", "direct", "not a proxy"]`

func TestParseFeed_ProxiflyJSONAndPlainLists(t *testing.T) {
	for _, c := range []struct {
		body, scheme, country string
		want                  []string
	}{
		{"\ufeff" + proxiflyBody, "socks5h", "se", []string{
			"socks5h://192.0.2.7:1080", "https://192.0.2.10:443", "http://192.0.2.8:80"}},
		{proxiflyBody, "socks5h", "", []string{
			"socks5h://192.0.2.7:1080", "https://192.0.2.10:443", "http://192.0.2.8:80",
			"http://other.example:80"}},
		{"# list\n\n1.2.3.4:1080\nhttp://h:80\n", "socks4", "", []string{"socks4://1.2.3.4:1080", "http://h:80"}},
		{`["http://u:p@h:80", "socks4://h:1080"]`, "http", "", []string{"http://u:p@h:80", "socks4://h:1080"}},
	} {
		got, err := parseFeed([]byte(c.body), c.scheme, c.country)
		if err != nil || !slices.Equal(urlsOf(got), c.want) {
			t.Errorf("parseFeed(%.40q, %s) = %q, %v; want %q", c.body, c.country, urlsOf(got), err, c.want)
		}
	}
}

// An endpoint keeps the record that listed it first, as the feed wrote it;
// a bare URL's record is {}.
func TestParseFeed_TheFirstListingWins(t *testing.T) {
	body := `[{"proxy": "socks5://a:1080", "score": 1}, {"proxy": "socks5h://a:1080", "score": 2}, "http://b:80",` +
		` {"proxy": "http://b:80", "score": 3}]`
	got, err := parseFeed([]byte(body), "http", "")
	if err != nil {
		t.Fatal(err)
	}
	want := []feedRecord{
		{"socks5h://a:1080", json.RawMessage(`{"proxy": "socks5://a:1080", "score": 1}`)},
		{"http://b:80", json.RawMessage(`{}`)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseFeed = %v, want %v", got, want)
	}
}

// A record that names a country keeps its endpoint only in the feed of
// that marketplace, in any letter case; null and "" name none, and a
// country that is not a string is none of them. "İT", "ıt", "ß" and "ﬆ" fold
// to no code. A shared feed ("") filters nothing.
func TestParseFeed_CountryFilter(t *testing.T) {
	record := func(geolocation string) string {
		return `[{"proxy": "socks5://a:1080", "geolocation": ` + geolocation + `}, "http://b:80"]`
	}
	both, onlyB := []string{"socks5h://a:1080", "http://b:80"}, []string{"http://b:80"}
	for _, c := range []struct {
		geolocation, country string
		want                 []string
	}{
		{`{"country": "SE"}`, "se", both},
		{`{"country": "se"}`, "se", both},
		{`{"country": ""}`, "se", both},
		{`{"country": null}`, "se", both},
		{`{"city": "Vaxjo"}`, "se", both},
		{`null`, "se", both},
		{`"SE"`, "se", both},
		{`{"country": "DE"}`, "se", onlyB},
		{`{"country": 46}`, "se", onlyB},
		{`{"country": ["SE"]}`, "se", onlyB},
		{`{"country": "\u0130T"}`, "it", onlyB},
		{`{"country": "\u0131t"}`, "it", onlyB},
		{`{"country": "\u00df"}`, "ss", onlyB},
		{`{"country": "\ufb06"}`, "st", onlyB},
		{`{"country": "DE"}`, "", both},
	} {
		got, err := parseFeed([]byte(record(c.geolocation)), "http", c.country)
		if err != nil || !slices.Equal(urlsOf(got), c.want) {
			t.Errorf("geolocation %s in the %q feed: %q, %v; want %q", c.geolocation, c.country, urlsOf(got), err, c.want)
		}
	}
}

// Plain text is one entry per line, with blank and '#' lines skipped. A
// line ends at LF, CR or CRLF; an entry with any other space inside it is
// no endpoint.
func TestParseFeed_PlainText(t *testing.T) {
	body := "# list\r\n\r\n1.2.3.4:1080\r\n  http://h:80  \n  # x\nb:2\vc:3\nnot a proxy\r\nd:4\re:5"
	got, err := parseFeed([]byte(body), "http", "")
	want := []string{"http://1.2.3.4:1080", "http://h:80", "http://d:4", "http://e:5"}
	if err != nil || !slices.Equal(urlsOf(got), want) {
		t.Errorf("parseFeed = %q, %v; want %q", urlsOf(got), err, want)
	}
	for _, r := range got {
		if string(r.metadata) != "{}" {
			t.Errorf("%s: metadata %s, want {}", r.url, r.metadata)
		}
	}
}

// A record keeps the bytes of a valid feed as they were, and a byte that
// is not UTF-8 is stored as U+FFFD.
func TestParseFeed_RecordsAreValidText(t *testing.T) {
	body := "[{\"proxy\": \"http://a:80\", \"city\": \"V\u00e4xj\xf6\"}]"
	got, err := parseFeed([]byte(body), "http", "")
	if err != nil || len(got) != 1 {
		t.Fatalf("parseFeed = %q, %v", urlsOf(got), err)
	}
	if want := "{\"proxy\": \"http://a:80\", \"city\": \"V\u00e4xj\ufffd\"}"; string(got[0].metadata) != want {
		t.Errorf("record %q, want %q", got[0].metadata, want)
	}
}

// A body that is no feed fails, and a failed feed keeps its previous list;
// an empty one is a feed that lists nothing.
func TestParseFeed_WhatFails(t *testing.T) {
	for _, c := range []struct {
		body    string
		wantErr error    // with want nil: the error, or any error when nil
		want    []string // non-nil: no error, and these endpoints
	}{
		{body: `[broken`},
		{body: `["http://a:80"] trailing`},
		{body: `[{"proxy": "http://a:80", "n": NaN}]`},
		{body: `{"error":"unavailable"}`, wantErr: errFeedNotArray},
		{body: `<html>Bad gateway</html>`, wantErr: errNoUsableEndpoints},
		{body: `[null, 1, {}]`, wantErr: errNoUsableEndpoints},
		{body: `["direct", "ftp://a:21"]`, wantErr: errNoUsableEndpoints},
		{body: `[]`, want: []string{}},
		{body: ``, want: []string{}},
		{body: " \r\n", want: []string{}},
		{body: "# nothing yet\n", want: []string{}},
	} {
		got, err := parseFeed([]byte(c.body), "http", "")
		switch {
		case c.want != nil:
			if err != nil || got == nil || !slices.Equal(urlsOf(got), c.want) {
				t.Errorf("parseFeed(%q) = %#v, %v; want an empty list", c.body, got, err)
			}
		case err == nil || got != nil:
			t.Errorf("parseFeed(%q) = %v, %v; want an error", c.body, urlsOf(got), err)
		case c.wantErr != nil && !errors.Is(err, c.wantErr):
			t.Errorf("parseFeed(%q): %v, want %v", c.body, err, c.wantErr)
		}
	}
}
