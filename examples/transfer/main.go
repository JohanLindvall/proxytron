// Command transfer confirms a reserved route's exit before streaming a response.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/JohanLindvall/proxytron/lease"
)

func main() {
	endpoint := flag.String("proxy", "", "proxy endpoint, or explicit direct routing")
	target := flag.String("url", "", "URL to stream")
	database := flag.String("db", "transfers.db", "lease-pool database")
	probe := flag.String("ip-check", "", "caller-selected IP-check URL")
	backup := flag.String("ip-check-backup", "", "optional second IP-check URL")
	timeout := flag.Duration("timeout", 2*time.Minute, "total operation timeout")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	probes := []string{*probe}
	if *backup != "" {
		probes = append(probes, *backup)
	}
	if err := transfer(ctx, *target, *database, *endpoint, probes); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func transfer(ctx context.Context, target, database, endpoint string, probes []string) error {
	if target == "" || endpoint == "" || probes[0] == "" {
		return errors.New("provide -url, -proxy and -ip-check")
	}
	client := lease.NewHTTPClient(nil)
	pool, err := lease.Open(database, []string{endpoint}, nil, client, nil)
	if err != nil {
		return err
	}
	defer pool.Close() // Also closes clients owned by the pool.
	route := pool.Acquire("transfer", "", 1)
	if route == nil {
		return errors.New("no transfer route available")
	}
	defer route.Release() // Runs before the pool is closed.
	prober := lease.ExitProber{URLs: probes, Timeout: 10 * time.Second}
	identity, err := prober.Claim(ctx, route)
	if err != nil {
		return err
	}
	log.Printf("reserved exit %s", identity.IP)
	// Probe and destination requests use the same transport and cookie jar.
	// Keep this context through any authentication or ticket setup as well.
	req, err := client.NewRequest(route.Context(ctx), http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.DoOnce(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() // Finish I/O before releasing the lease.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	// Reading to EOF reports the standard adapter's observation. Applications
	// with content-specific refusal/challenge rules provide their own Client.
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}
