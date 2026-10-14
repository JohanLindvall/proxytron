// Command rotate fetches a URL using a persistent request pool and shared pacing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/signal"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func main() {
	var endpoints []string
	flag.Func("proxy", "proxy endpoint; repeat for multiple routes", func(s string) error {
		endpoints = append(endpoints, s)
		return nil
	})
	target := flag.String("url", "", "URL to fetch")
	database := flag.String("db", "requests.db", "request-pool database")
	direct := flag.Bool("allow-direct", false, "allow an explicitly configured direct route")
	interval := flag.Duration("interval", time.Second, "minimum spacing per proxy host")
	timeout := flag.Duration("timeout", time.Minute, "total operation timeout")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := fetch(ctx, *target, *database, endpoints, *direct, *interval); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func fetch(ctx context.Context, target, database string, endpoints []string, allowDirect bool, interval time.Duration) error {
	if target == "" || len(endpoints) == 0 {
		return errors.New("provide -url and at least one -proxy")
	}
	cfg := proxytron.DefaultConfig()
	for _, raw := range endpoints {
		endpoint, err := proxytron.ParseEndpoint(raw, "http")
		if err != nil {
			return err
		}
		cfg.Endpoints = append(cfg.Endpoints, endpoint)
	}
	pool, err := proxytron.LoadPool(ctx, &cfg, nil, database)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Each route owns a reusable transport and isolated cookies. Do invokes
	// this example's callbacks sequentially, so this map needs no mutex.
	clients := make(map[string]*http.Client)
	defer func() {
		for _, client := range clients {
			client.CloseIdleConnections()
		}
	}()
	pacer := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: interval}, nil)
	var body []byte
	err = pool.Do(ctx, "fetch", proxytron.RequestOptions{
		AllowDirect: allowDirect, Attempts: 2, MaxRouteWait: 10 * time.Second, Pacer: pacer,
	}, func(ctx context.Context, endpoint string) proxytron.AttemptResult {
		client := clients[endpoint]
		if client == nil {
			transport, err := proxytron.NewTransport(endpoint, proxytron.TransportOptions{})
			if err != nil {
				return proxytron.AttemptResult{Err: err}
			}
			jar, err := cookiejar.New(nil)
			if err != nil {
				transport.CloseIdleConnections()
				return proxytron.AttemptResult{Err: err}
			}
			client = &http.Client{Transport: transport, Jar: jar, Timeout: 15 * time.Second}
			clients[endpoint] = client
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return proxytron.AttemptResult{Err: err}
		}
		resp, err := client.Do(req)
		if err != nil {
			// A GET can be replayed on another route. An unclassified error
			// stays unscored: only the caller can distinguish a proxy failure
			// from a destination failure. Set TransportFailed when confirmed.
			return proxytron.AttemptResult{Err: err, Retry: true, RouteFailure: true}
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			// Real adapters decide whether this is a refusal, challenge,
			// operation error or usable response; a status alone is insufficient.
			return proxytron.AttemptResult{Err: fmt.Errorf("HTTP status %d", resp.StatusCode)}
		}
		const limit = 1 << 20
		body, err = io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if err != nil {
			return proxytron.AttemptResult{Err: err, Retry: true, RouteFailure: true}
		}
		if len(body) > limit {
			return proxytron.AttemptResult{Err: errors.New("response exceeds 1 MiB example limit")}
		}
		return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
	})
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(body)
	return err
}
