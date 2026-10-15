package lease_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/JohanLindvall/proxytron"
	"github.com/JohanLindvall/proxytron/lease"
)

func ExampleWithProxyType() {
	dir, err := os.MkdirTemp("", "proxytron-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	pool, err := lease.Open(filepath.Join(dir, "leases.db"), []string{
		"http://http.example.test:8080",
		"socks5h://socks.example.test:1080",
	}, nil, nil, nil, lease.WithProxyType(proxytron.ProxyTypeSOCKS))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	route := pool.Acquire("downloads", "", 1)
	if route == nil {
		panic("no SOCKS route available")
	}
	defer route.Release()
	fmt.Println(route.Endpoint())
	// Requests use route.Context(ctx); upgrades retain the same type restriction.
	// Output: socks5h://socks.example.test:1080
}

func ExampleExitProber_Claim() {
	// A loopback proxy supplies a synthetic IP-check response in this example.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "198.51.100.8")
	}))
	defer proxy.Close()
	dir, err := os.MkdirTemp("", "proxytron-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	pool, err := lease.Open(filepath.Join(dir, "leases.db"), []string{proxy.URL}, nil, nil, nil)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	route := pool.Acquire("downloads", "", 1)
	if route == nil {
		panic("no route available")
	}
	defer route.Release()
	prober := lease.ExitProber{URLs: []string{"http://ip-check.example.test/ip"}}
	identity, err := prober.Claim(context.Background(), route)
	if err != nil {
		panic(err)
	}
	fmt.Println(identity.IP, route.Claimed())
	// Destination requests now use route.Context(ctx) until route.Release().
	// Output: 198.51.100.8 true
}
