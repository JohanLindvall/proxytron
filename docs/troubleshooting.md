# Troubleshooting

[Overview](../README.md) · [User guide](guide.md) · [Get help](../SUPPORT.md)

## No route is available

`Pool.Acquire` returns `""`, and a lease acquisition returns `nil`, when there is
no eligible route. Check the endpoints, scope, protocol restriction, cooldowns
and, for leases, address capacity. `WithProxyType` and a lease scope's type
restriction must both permit a route. A specific proxy type excludes `direct`.

For request pools, `WaitFor(scope, allowDirect, nil)` reports the earliest
cooldown wait and whether any eligible route exists. `Health` and `SiteHealth`
help inspect the inventory and scored state. `Pool.Do` can wait within a
`MaxRouteWait` budget:

| Error from `Pool.Do` | Meaning | What to check |
| --- | --- | --- |
| `ErrNoRoute` | No route is eligible for this scope and routing policy | Inventory, scope and protocol settings |
| `ErrRouteWait` | The remaining cooldown wait exceeds the wait budget | `MaxRouteWait`, cooldowns and the operation deadline |
| `ErrAttemptsExhausted` | An operation or route retry budget was spent | `Attempts`, `RouteAttempts` and the last attempt's error |

Use `errors.Is` to inspect these errors; they can also wrap the last attempt's
error. Repeated immediate acquisition attempts do not clear a cooldown.

Always check for an empty acquisition before calling `NewTransport`. That
low-level API treats an empty endpoint as direct. To intentionally use direct
routing through a request pool, configure a `direct` member **and** set
`AllowDirect: true`. For a lease pool, explicitly configure a `direct` member.

## Which failures should affect proxy health?

Your adapter knows the destination's response semantics. A status code alone
cannot distinguish a usable response, refusal, challenge or destination outage.

| Observation | `AttemptResult.Outcome` | Effect |
| --- | --- | --- |
| Usable response | `Succeeded` | Records success for the scope and route |
| Destination refusal specific to this route | `Refused` | Scoped health and cooldown |
| Confirmed proxy connection failure | `TransportFailed` | Endpoint-wide health and cooldown |
| Destination challenge | `Challenged` | Separate scoped challenge state and cooldown |
| Local error or uncertain cause | `Unreported` | Leaves route health unchanged |

`Pool.Do` reports the returned outcome, so do not also call `ReportOK`, `Demote`,
`DemoteTransport` or `Punish` inside its callback. Cancellation of the operation
context or a `context.Canceled` attempt is unscored. With your own acquisition
loop, exclude user cancellations from penalties yourself.

`Retry` and `RouteFailure` control retry behavior independently of scoring.
Only retry an operation your application can safely replay.

## Requests use an unexpected route or DNS resolver

`NewTransport` ignores `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`; routing comes
from the endpoint you supply. It never falls back from an explicit proxy to a
direct connection. Reuse one transport/client per route and close response
bodies before returning from a `Pool.Do` callback.

`ParseEndpoint` and discovery normalize `socks5` to `socks5h` for remote
destination DNS. Passing an explicit `socks5://` endpoint directly to `Dialer`
or `NewTransport` retains local DNS; `socks5h://` and `socks4a://` resolve at the
proxy. Validate user-supplied endpoints at your application's boundary and
choose DNS behavior deliberately.

An HTTPS proxy URL secures the connection to the proxy. An HTTPS destination
secures the destination connection. Proxy and destination TLS configurations
are separate, and both verify certificates by default. See the
[protocol guide](guide.md#selecting-proxy-protocols).

## The database will not open

Close the pool before another process opens its Bolt file. Use separate paths
for request and lease pools, such as `requests.db` and `transfers.db`, and check
that the parent directory and file are accessible to the current process.

Corrupt state returns an error instead of resetting learned health. Preserve
the original file for diagnosis; do not overwrite it as a repair step. Database
files contain full proxy credentials, so never attach them to a public issue.
Use a synthetic reproducer instead. Files are created with mode `0600`.

Outcome methods log persistence errors; request-pool `Save` returns an explicit
checkpoint error. Release active leases before closing their pool. A lease
pool's `Close` flushes pending progress and closes its clients and database.
Active leases are not restored after restart.

## An exit probe fails or a lease cannot claim an address

Exit detection requires an IP-check URL you choose. It never contacts a default
service. Confirm that your probe returns a single IP address, either as plain
text or JSON with an `ip` or `origin` field.

| Error from `ExitProber.Claim` | Meaning | Next step |
| --- | --- | --- |
| `lease.ErrExitInUse` | Another lease or cooldown reserves the observed exit | Release the lease and select another eligible route or wait |
| `lease.ErrExitUnknown` | No configured probe returned a usable identity | Check the probe URLs and response formats |
| `lease.ErrExitProbeFailed` | An optional reachability check judged the route's probes to have failed | Inspect the route and your `Reachable` policy |

Invalid replies and probe-service refusals try the next configured URL.
Transport failures stop probing immediately. `ExitProber` never releases the
caller's lease. A cached identity reserves capacity but still needs fresh
confirmation before `Claimed` becomes true.

Probe and destination requests must use the same client/session. A different
observed exit does not establish anonymity, and rotating gateways may change
addresses between requests. See [transfer leases](guide.md#transfer-leases).

## Do I need a feed, database or public proxy to get started?

No. Use `FromURLs` for an in-memory request pool, and use the
[local demo](../examples/quickstart/main.go) to try it with loopback servers.
`LoadPool` adds persistence and optional feed discovery.

Proxytron does not supply proxy accounts, an anonymity service, browser
fingerprints or destination-specific response classification. Those belong
in your application or provider integration.

## Still stuck?

Check the [runnable examples](../examples/README.md) and
[API reference](https://pkg.go.dev/github.com/JohanLindvall/proxytron), then
[ask a question or report a bug](../SUPPORT.md).
