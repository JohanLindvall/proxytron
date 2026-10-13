# proxytron

[![CI](https://github.com/JohanLindvall/proxytron/actions/workflows/ci.yml/badge.svg)](https://github.com/JohanLindvall/proxytron/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JohanLindvall/proxytron.svg)](https://pkg.go.dev/github.com/JohanLindvall/proxytron)

Persistent, scored proxy pools for Go with adaptive rotation, HTTP(S)/SOCKS
transport, transfer leases and real exit IP detection. Application policy stays
separate from inventory, discovery and transport.
Requires Go 1.26 or later. MIT licensed.

```sh
go get github.com/JohanLindvall/proxytron
```

| Package | Purpose |
| --- | --- |
| `proxytron` | Request rotation, discovery, HTTP(S)/SOCKS dialing and real exit IP detection |
| `proxytron/lease` | Reserved routes for setup and long transfers, address limits and adaptive upgrades |
| `proxytron/store` | Transactional Bolt persistence and an interface for existing databases |

## Persistent request rotation

```go
cfg := proxytron.DefaultConfig()
cfg.Endpoints = []string{"http://proxy.example.test:8080"}
// Optional: cfg.FeedURL = "https://feeds.example.test/{country}.json"
// Optional: cfg.Scopes = []string{"se", "de"}

pool, err := proxytron.LoadPool(ctx, &cfg, nil, "proxies.db")
if err != nil {
    return err
}
defer pool.Close()

route := pool.Acquire("catalog", false, nil, nil)
if route == "" {
    // No eligible route. WaitFor reports the next cooldown expiry.
    return errors.New("no proxy available")
}
transport, err := proxytron.NewTransport(route, proxytron.TransportOptions{})
if err != nil {
    return err
}
defer transport.CloseIdleConnections()
client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
// Make the request using client and classify its outcome in the application.
pool.ReportOK(ctx, "catalog", route, elapsed)
```

Reuse a transport/client per route in a running application. Configure its cookie
jar and browser identity in the application. Report `Demote` for a destination
refusal, `DemoteTransport` for a failed proxy connection, and `Punish` for a WAF
challenge. A response status alone cannot reliably classify these outcomes.
Exclude user cancellations from proxy penalties.

The default selector uses Thompson sampling to estimate successful responses per
second, including failure cost and optional caller-provided pacing. Round-robin
is also available. Scopes are arbitrary application keys: a refusal or challenge
in one scope does not penalize another. Broken transport health is endpoint-wide.
Never-successful feed entries receive longer cooldowns than configured routes.
Thousands of untried routes share one exploration draw.

`Live` retains the pool across passes, reconciles configuration, supports temporary
overrides and refreshes feeds between passes. Feed failures retain the last good
inventory. Country templates, JSON records, plain lists, feed metadata, retirement
and refresh timestamps survive restarts. Feed scores never override measured scores.

## Real exit IP detection

```go
detector := proxytron.Detector{
    URL: "https://ip-check.example.test/ip", // your chosen IP-check service
    Timeout: 10 * time.Second,
}
identity, err := pool.DetectEgress(ctx, detector, route)
// identity.IP is the externally observed IPv4 or IPv6 address, not the gateway's DNS/IP.
// The identity and observation time are saved with the route's scored state.

comparison, err := detector.Compare(ctx, route)
// comparison.Direct.IP and comparison.Proxy.IP measure both paths explicitly.
// comparison.SameIP() reports whether their observed exit addresses match.
```

No IP service is hard-coded or contacted automatically. Responses may be a plain
address or JSON with `ip` or `origin`; ambiguous lists and oversized responses fail.
Explicit proxies never fall back to direct egress. Supply `Detector.Client` to use
an application's own transport and session settings. A different exit address
does not prove anonymity; rotating gateways can change addresses between requests.

## Transfer leases

```go
pool, err := lease.Open("transfers.db", endpoints, feeds, nil, nil,
    lease.WithPolicy(lease.Policy{
        RecoveryScopes: []string{"challenge-recovery"},
        CappedScopes: []string{"rate-limited-service"},
    }))
if err != nil { return err }
defer pool.Close()
pool.Start(ctx)

route := pool.AcquireFor("downloads", preferredRouteID, 1, remainingBytes, false)
if route == nil { return errors.New("no transfer route available") }
defer route.Release()
requestContext := route.Context(ctx)
// Use lease.HTTPClient.DoOnce, or your Client adapter, with requestContext.
```

A lease spans authentication, CAPTCHA/ticket setup, redirects and the transfer.
Preferred route IDs allow reuse of unexpired IP-bound state. Address limits and
cooldowns are shared across proxy ports/protocols. Calling `pool.DetectEgress`
also groups different gateways by their observed public exit IP, including active
leases. Untested gateway ports can be tried independently; confirmed exit claims
enforce the per-address cap. Detection is
explicit because probing every public-feed entry is expensive and rotating
sessions require application-specific timing.

`Lease.ClaimExit` atomically claims a freshly observed IP before the destination
sees the request. A cached identity reserves capacity when a lease is acquired,
but `Claimed` remains false until a fresh observation confirms it. `ProbeContext`
counts transport failures without treating an IP service's refusal as a
destination refusal. `WithExitTTL` controls cached-identity expiry. Applications
choose probe fallbacks and decide how an outage of the IP services affects work.

`Progress` learns bandwidth and setup delay before completion, excluding constrained
windows. `Upgrade` atomically reserves a proven faster route when sustained expected
savings justify switching; the application cancels the old attempt and resumes on
the replacement. Challenge scores remain separate from ordinary service/transport
scores. `Policy` controls which scopes borrow measurements and which speeds reflect
service-imposed caps. `SetDemand` drives early replenishment using scored capacity.

Implement `lease.Client` to retain custom TLS, cookies, challenge classification
and request-context routing. `WithFeedFilter` limits discovery to protocols that
adapter supports. The provided HTTP adapter supports standard `net/http` clients.

## Persistence and compatibility

Both pools commit outcomes and cooldowns transactionally; lease progress samples
are batched by the refresh loop and flushed on close. Inventory includes learned
priors, discovery memberships and actual egress identities. Active leases are never
restored. Bolt files are created with mode `0600`, and lock acquisition is bounded.
Close a pool before another process opens its file.

Request pools use `store.Store` and `WithStoreOpener` to support existing databases;
the default is Bolt. Adapters can retain their existing tables and historical JSON
field names. `WithLegacyImport(true)` explicitly enables one-time migration from
an owned `proxy-memory.json`. Lease pools preserve their existing Bolt schema.
The two pool formats have separate namespaces; do not open the same file twice.
Unreadable state fails loading rather than silently resetting health.
`lease.WithHealthEpoch` lets applications explicitly invalidate health judged
under an older policy while retaining inventory; it is disabled by default.

The protocol layer preserves remote DNS, authentication, tolerant CONNECT replies,
buffered tunnel bytes and SOCKS4a support. HTTPS proxy TLS is separate from
destination TLS. Both verify certificates by default; opting into relaxed proxy
TLS does not relax destination verification. Browser fingerprinting and content
classification stay in the applications.

Outcome methods log persistence failures; request-pool `Save` returns a checkpoint
error to callers that require explicit durability confirmation. A request pool's
`Close` detaches its store; a lease pool's `Close` stops background work and closes
its clients and database. Release transfer leases before closing their pool.

## Development

```sh
go test -race ./...
go vet ./...
go test -cover ./...
```

Tests use loopback servers, fake clocks and synthetic endpoints. They cover
historical inventory migration, concurrency, selection, TLS separation, exit IP
grouping and corrupt-state handling without using live proxies. See
[CONTRIBUTING.md](CONTRIBUTING.md) for releases and compatibility policy.
