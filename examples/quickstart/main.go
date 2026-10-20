// Command quickstart demonstrates a proxy pool using only loopback servers.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := run(ctx, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	const message = "hello from the local destination\n"
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, message)
	}))
	defer destination.Close()
	target, err := url.Parse(destination.URL)
	if err != nil {
		return err
	}

	// These fixtures forward only to our local destination. Their transport
	// ignores environment proxy settings, so demo traffic stays on loopback.
	upstream := &http.Transport{}
	defer upstream.CloseIdleConnections()
	newProxy := func(name string) *httptest.Server {
		forward := httputil.NewSingleHostReverseProxy(target)
		forward.Transport = upstream
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Demo-Proxy", name)
			forward.ServeHTTP(w, r)
		}))
	}
	first, second := newProxy("proxy A"), newProxy("proxy B")
	defer first.Close()
	defer second.Close()

	// Round-robin gives repeatable output. Omit WithSettings to use the
	// default adaptive selector. LoadPool adds persistence to an application.
	settings := proxytron.DefaultConfig().Settings
	settings.Selection = proxytron.SelectionRoundRobin
	pool := proxytron.FromURLs([]string{first.URL, second.URL}, proxytron.WithSettings(settings))
	defer pool.Close()

	// Reuse one client per route. This example invokes Do sequentially;
	// concurrent callers need a synchronized client cache.
	clients := make(map[string]*http.Client)
	defer func() {
		for _, client := range clients {
			client.CloseIdleConnections()
		}
	}()

	for range 4 {
		var body []byte
		var via string
		err := pool.Do(ctx, "demo", proxytron.RequestOptions{}, func(ctx context.Context, endpoint string) proxytron.AttemptResult {
			client := clients[endpoint]
			if client == nil {
				transport, err := proxytron.NewTransport(endpoint, proxytron.TransportOptions{})
				if err != nil {
					return proxytron.AttemptResult{Err: err}
				}
				client = &http.Client{Transport: transport, Timeout: 5 * time.Second}
				clients[endpoint] = client
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, destination.URL, nil)
			if err != nil {
				return proxytron.AttemptResult{Err: err}
			}
			resp, err := client.Do(req)
			if err != nil {
				// Unclassified failures stay unscored. Real adapters decide
				// whether a failure belongs to the proxy or the destination.
				return proxytron.AttemptResult{Err: err}
			}
			defer resp.Body.Close()
			body, err = io.ReadAll(io.LimitReader(resp.Body, 1024))
			if err != nil {
				return proxytron.AttemptResult{Err: err}
			}
			if resp.StatusCode != http.StatusOK || string(body) != message {
				return proxytron.AttemptResult{Err: errors.New("unexpected demo response")}
			}
			via = resp.Header.Get("X-Demo-Proxy")
			return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
		})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "%s: %s\n", via, strings.TrimSpace(string(body))); err != nil {
			return err
		}
	}
	var successes int
	for _, health := range pool.SiteHealth("demo") {
		successes += health.Served
	}
	_, err = fmt.Fprintf(out, "Recorded successes: %d\n", successes)
	return err
}
