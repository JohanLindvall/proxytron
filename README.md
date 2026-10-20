# Proxytron

**Proxy pools for Go that learn from your requests and remember what works.**

[![CI](https://github.com/JohanLindvall/proxytron/actions/workflows/ci.yml/badge.svg)](https://github.com/JohanLindvall/proxytron/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JohanLindvall/proxytron.svg)](https://pkg.go.dev/github.com/JohanLindvall/proxytron)
[![Go version](https://img.shields.io/github/go-mod/go-version/JohanLindvall/proxytron)](go.mod)
[![Release](https://img.shields.io/github/v/release/JohanLindvall/proxytron)](https://github.com/JohanLindvall/proxytron/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Proxytron selects and rotates HTTP(S) and SOCKS proxies, learns which routes
deliver successful requests, and persists their health across restarts. For long
transfers, it reserves routes through setup and download, with limits shared by
observed exit IP.

[Quickstart](#quickstart) · [User guide](docs/guide.md) ·
[Examples](examples/README.md) · [API reference](https://pkg.go.dev/github.com/JohanLindvall/proxytron) ·
[Get help](SUPPORT.md) · [Contribute](CONTRIBUTING.md)

## Why Proxytron?

- **Learn from useful work.** Adaptive selection accounts for successful
  responses, elapsed time, failure cost and optional pacing. Round-robin is also
  available.
- **Keep what you learn.** Bolt persistence retains inventory, measured health,
  cooldowns and discovery state between runs.
- **Handle failures at the right scope.** A service refusal or challenge affects
  that scope; a broken proxy connection affects the endpoint. User cancellation
  does not penalize a route.
- **Reserve routes for transfers.** Keep a lease through authentication and
  download, enforce per-address limits, and reserve a faster replacement when
  measurements justify switching.
- **Control your routing.** Support for HTTP, HTTPS, SOCKS4(a) and SOCKS5(h),
  explicit direct routing, separate proxy/destination TLS, and caller-configured
  exit IP checks.

Proxytron is an embedded Go library. You supply proxies or a discovery feed and
decide what counts as a successful response, refusal or challenge. Your
application owns cookies, browser identity and retry safety. Coordination is
local to a pool; a custom store alone does not provide distributed reservations.
See the [comparison and tradeoffs](docs/comparison.md) when choosing a library.

## Quickstart

Requires **Go 1.26 or later**.

### Try it locally

No proxy account, credentials or external service is needed:

```sh
git clone https://github.com/JohanLindvall/proxytron.git
cd proxytron
go run ./examples/quickstart
```

The demo starts two loopback proxy fixtures and a local destination, sends four
requests through the pool, and prints:

```text
proxy A: hello from the local destination
proxy B: hello from the local destination
proxy A: hello from the local destination
proxy B: hello from the local destination
Recorded successes: 4
```

It uses round-robin to make the result repeatable. The default selector learns
from outcomes instead. All demo traffic stays on loopback, and the demo closes
its servers, clients and in-memory pool on exit. The first Go invocation may
download the toolchain and module dependencies.
Read the [complete source](examples/quickstart/main.go).

### Use your own proxies

Add the library from your application's module directory:

```sh
go get github.com/JohanLindvall/proxytron
```

From a checkout of this repository, run the persistent request example after
replacing the placeholder destination and proxies:

```sh
go run ./examples/rotate \
  -url 'https://destination.example.test/data' \
  -proxy 'http://proxy-a.example.test:8080' \
  -proxy 'socks5h://proxy-b.example.test:1080' \
  -db requests.db
```

This writes the response to stdout, reuses a client and cookie jar for each route,
and saves health to `requests.db`. Run it again to reuse the learned state.
Use `go run ./examples/rotate -h` for options, or see the
[transfer example](examples/README.md#transfer-lease-and-exit-confirmation) for
streaming a response through an exit-confirmed lease.

### Choose your API

| You need to… | Start here |
| --- | --- |
| Rotate an existing list in memory | `proxytron.FromURLs` |
| Restore health and discover routes | `proxytron.DefaultConfig` + `proxytron.LoadPool` |
| Handle retries, pacing and cooldown waits | `Pool.Do` with your request callback |
| Choose a route in an existing request loop | `Pool.Acquire`, then report the classified outcome |
| Reserve a route through setup and transfer | `lease.Open`, then `AcquireFor` and `Lease.Release` |
| Supply an existing persistence backend | `store.Store` + `proxytron.WithStoreOpener` |

An empty acquisition means **no eligible route**. Check it before constructing a
transport: the low-level transport API accepts an empty endpoint as direct.
Request-pool direct routing needs both a configured `direct` member and
`AllowDirect: true`; lease pools require an explicitly configured `direct` member.
Explicit proxies never silently fall back to direct egress.

## User guide

The detailed guides cover configuration, examples and resource ownership:

<!-- Keep the original README anchors usable for existing links. -->
<a id="persistent-request-rotation"></a>
<a id="selecting-proxy-protocols"></a>
<a id="request-orchestration"></a>
<a id="real-exit-ip-detection"></a>
<a id="transfer-leases"></a>
<a id="persistence-and-compatibility"></a>

- [Persistent request rotation and discovery](docs/guide.md#persistent-request-rotation)
- [Protocol selection, authentication and DNS](docs/guide.md#selecting-proxy-protocols)
- [Retries, pacing and outcome classification](docs/guide.md#request-orchestration)
- [Explicit exit IP detection](docs/guide.md#real-exit-ip-detection)
- [Transfer leases, exit reservations and upgrades](docs/guide.md#transfer-leases)
- [Persistence, compatibility and cleanup](docs/guide.md#persistence-and-compatibility)
- [Troubleshooting and common questions](docs/troubleshooting.md)

## Development

Run the same Go checks used in CI:

```sh
go test -race ./...
go vet ./...
```

`make help` lists optional shortcuts, including `make demo` and `make check`.
Tests use loopback servers, fake clocks and synthetic endpoints; they do not
require public proxies or live feeds. CI covers Linux, macOS and Windows.
See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, first contributions and releases.

## Community

Questions and bug reports are welcome through [GitHub Issues](https://github.com/JohanLindvall/proxytron/issues/new/choose).
Read the [support guide](SUPPORT.md) for what to include, the
[code of conduct](CODE_OF_CONDUCT.md) for community expectations, and
[SECURITY.md](SECURITY.md) to report a vulnerability privately.

If Proxytron helps your project, consider starring the repository so you can
find it again and sharing it with other Go developers. Reproducible bug reports,
documentation improvements and examples are useful contributions too.

## License

[MIT](LICENSE). See [GitHub Releases](https://github.com/JohanLindvall/proxytron/releases)
for version history and compatibility notes.
