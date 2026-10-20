# User guide

[Back to the overview](../README.md) · [Runnable examples](../examples/README.md) ·
[Troubleshooting](troubleshooting.md)

Start with the [local quickstart](../README.md#quickstart) to see requests pass
through a pool. This guide covers configuration and the responsibilities of a
caller integrating Proxytron into an application. Snippets show parts of that
integration; the runnable examples include imports and complete cleanup.
This guide follows the repository revision you are reading. For a released
version, select that version in the
[API reference](https://pkg.go.dev/github.com/JohanLindvall/proxytron).

- [Persistent request rotation](#persistent-request-rotation)
- [Selecting proxy protocols](#selecting-proxy-protocols)
- [Request orchestration](#request-orchestration)
- [Real exit IP detection](#real-exit-ip-detection)
- [Transfer leases](#transfer-leases)
- [Persistence and compatibility](#persistence-and-compatibility)

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

## Selecting proxy protocols

Validate new user-supplied endpoints with `ParseEndpoint` at the application's
configuration boundary. Endpoints require an explicit port; URL-encode
credentials containing special characters. Use `Redact` before including an
endpoint in diagnostics.

`ParseEndpoint` and discovery normalize `socks5` to `socks5h` for remote
destination DNS. An explicit `socks5://` URL passed directly to `Dialer` or
`NewTransport` retains local DNS. `socks5h://` and `socks4a://` resolve at the
proxy. HTTP(S) proxy URLs support username/password authentication; SOCKS5
supports username/password and SOCKS4 uses the username as its user ID.

Choose SOCKS TCP routes with `WithProxyType`:

```go
pool := proxytron.FromURLs([]string{
    "http://proxy.example.test:8080",
    "socks5h://proxy.example.test:1080",
}, proxytron.WithProxyType(proxytron.ProxyTypeSOCKS))
defer pool.Close()

route := pool.Acquire("catalog", false, nil, nil)
// route is the SOCKS endpoint, or empty if no matching route is available.
```

| Type | Eligible protocols |
| --- | --- |
| `ProxyTypeAny` (default) | All configured protocols, with existing direct-route permissions |
| `ProxyTypeSOCKS` | `socks4`, `socks4a`, `socks5`, `socks5h` |
| `ProxyTypeHTTP` | `http`, `https` |

Pass the same option to `LoadPool` or `NewLive`. For transfers, pass
`lease.WithProxyType(proxytron.ProxyTypeSOCKS)` to `lease.Open`. The restriction
applies to static, discovered and restored routes, retries, cooldown waits,
preferred leases and adaptive upgrades. It survives inventory refreshes and live
overrides. A specific type excludes `direct` even with `AllowDirect: true`;
exhaustion returns an empty route or nil lease. Inventory and health remain stored
so reopening with another type preserves their history. Supply the option each
time a pool is created; the restriction itself is not persisted.

A lease pool can also restrict one scope on top of its own type, and change that
while it runs: pass `lease.WithScopeProxyType(scope, t)` to `lease.Open`, or call
`pool.SetScopeProxyType(scope, t)` at any time. A route must then be allowed by
both. The change holds from the next acquisition, preferred route, adaptive upgrade
and demand estimate on, and `Lease.Configured` reports a reserved route the scope
now excludes. Leases already granted are left alone, other scopes are unaffected,
and `proxytron.ProxyTypeAny` lifts the scope's restriction.

SOCKS does not add `Forwarded` or `X-Forwarded-For` headers as part of its protocol,
but choosing SOCKS does **not** guarantee anonymity. A proxy can still modify
plaintext HTTP. Use HTTPS destinations with certificate verification enabled to
protect requests from proxy header injection. HTTP(S) proxies also tunnel HTTPS
destinations using CONNECT; an `https://` proxy URL alone only secures the hop to
the proxy. Headers supplied by your application are sent unchanged. See
[SOCKS5](https://www.rfc-editor.org/rfc/rfc1928.html),
[CONNECT](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.3.6) and
[TLS integrity](https://www.rfc-editor.org/rfc/rfc8446.html#section-1).

## Request orchestration

`Pool.Do` owns route rotation, cooldown waits, pacing and outcome reporting for a
caller-supplied attempt. The callback constructs and finishes the request, closes
its body, and returns an `AttemptResult`; it can retain the response data in a
closure. Reuse your own clients and sessions across callbacks.

```go
pacer := proxytron.NewPacer(proxytron.PacerConfig{
    MinInterval: time.Second,
    SlowFactor: 2, MaxPenalty: 8, Decay: 0.9,
}, nil)

err := pool.Do(ctx, "catalog", proxytron.RequestOptions{
    Attempts: 3,
    RouteAttempts: 20,
    MaxRouteWait: time.Minute,
    Pacer: pacer,
    Backoff: func(n int) time.Duration { return time.Duration(n) * time.Second },
}, func(ctx context.Context, endpoint string) proxytron.AttemptResult {
    // Your adapter performs and classifies one complete attempt.
    return fetchAndClassify(ctx, endpoint)
})
```

Return `Succeeded`, `Refused`, `TransportFailed`, `Challenged` or `Unreported`
to choose how the runner scores the route. Do not report the same outcome again
inside the callback. `Retry` requests another attempt; `RouteFailure` spends the
separate route budget and skips operation backoff. Classification and the safety
of replaying an operation remain the caller's decisions. Cancellation is unscored.
`ErrAttemptsExhausted`, `ErrNoRoute` and `ErrRouteWait` distinguish why retries
stopped and preserve the last attempt error for `errors.Is`/`errors.As`.

Cooldown waits consume one cumulative `MaxRouteWait` budget, without spending
attempts. The context bounds the entire operation. Direct traffic requires both
`AllowDirect: true` and a configured `direct` pool member.

Share a pacer across workers to space starts on the same proxy host, regardless
of port, protocol or credentials. Refusals widen that host's floor and successes
ease it. Other hosts proceed independently; canceled waiters consume no slot.
`Pacer.ForRoute`, `Wait`, `Delay`, `Refused` and `OK` also work with custom request
loops. Pacers own no goroutines or network resources.

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
can use `ExitProber` to try configured IP services in order through the lease's
existing client and session, then claim the observed address:

```go
prober := lease.ExitProber{
    URLs: []string{"https://ip-check.example.test/ip", "https://backup.example.test/ip"},
    Timeout: 10 * time.Second,
}
identity, err := prober.Claim(ctx, route)
if err != nil { return err } // the deferred route.Release still applies
// Start destination work using route.Context(ctx).
```

A freshly claimed lease needs no second probe; a cached identity still needs
confirmation. `ErrExitInUse` signals a reserved or cooling exit without penalizing
the route. Invalid replies and probe-service refusals try the next URL; transport
failures stop immediately. The caller's cancellation is neutral, while a probe's
own timeout counts as a transport failure.

If every reply is unusable, the default is `ErrExitUnknown`, with no failure
penalty. An optional `Reachable(ctx)` callback can check probe availability:
`true` confirms and scores a route failure (`ErrExitProbeFailed`); `false`
explicitly permits proceeding without a known exit during an outage. The caller
chooses and may cache that diagnostic check. No direct check is implicit.
`IsResponseError` adapts clients that return HTTP refusals as errors. Probe bodies
and URLs are omitted from `ProbeResponseError` diagnostics. `ExitProber` owns no
clients and never releases the caller's lease.

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
