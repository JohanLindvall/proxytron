# Comparison with other open source libraries

Proxytron's strongest niche is persistent proxy selection for Go applications,
especially long transfers that need to reserve an actual exit IP. It offers more
control than basic rotators, with more integration work for the caller.

This comparison was reviewed on 2026-10-14 against Proxytron's implementation
and upstream documentation and source. It covers architecture and documented
features; no comparative performance benchmarks were run. The Crawlee comparison
uses its JavaScript v3.18 documentation.

| Alternative | What it provides | How Proxytron compares |
| --- | --- | --- |
| [go-proxator](https://github.com/cpouldev/go-proxator) — Go | Selection weighted by success and latency, per-domain block tracking, retries, reusable sessions and optional browser-like TLS. | Closest overlap for ordinary HTTP requests. Proxytron adds persistent learned state, discovery and transfer reservations; go-proxator offers a more integrated HTTP client. |
| [proxyhive](https://github.com/divinedev111/proxyhive) — Go | Latency-based selection, background health checks, geo filters, sticky sessions and a pooled `http.RoundTripper`. | Easier integration with `http.Client` and geographic filtering. Proxytron's strengths are outcome-specific scoring, persistence and exit-address reservations. |
| [Colly's proxy switcher](https://github.com/gocolly/colly/blob/master/proxy/proxy.go) — Go | Small, concurrent round-robin switcher integrated with Colly. | Sufficient for rotating a reliable list. Proxytron supplies health learning, cooldowns and persistence that the switcher itself lacks. |
| [scrapy-rotating-proxies](https://github.com/TeamHG-Memex/scrapy-rotating-proxies) — Python | Customizable ban detection, retries, randomized recovery backoff and per-proxy concurrency controls. | Convenient inside Scrapy. Proxytron provides more explicit failure categories and reservations shared across gateways with the same observed exit. |
| [Crawlee](https://crawlee.dev/js/docs/guides/proxy-management) — JavaScript | Proxy/cookie sessions, browser integration and escalation between proxy tiers. Also supports [session persistence](https://crawlee.dev/js/api/core/class/SessionPool#persistState). | Broader capabilities for building a complete crawler. Proxytron concentrates on Go routing and transfer scheduling; browser identity and cookies remain application responsibilities. |
| [jhao104/proxy_pool](https://github.com/jhao104/proxy_pool) — Python service | Proxy discovery, periodic validation, Redis storage and an HTTP API. | Better suited to sharing proxy inventory across applications. Proxytron embeds directly into an application and learns from its actual operations. |

Three aspects particularly distinguish Proxytron:

- **Selection accounts for useful work.** Its Thompson sampling considers
  successful responses, attempt duration, failure cost and pacing. Untried feed
  entries share an exploration draw, preventing a large untested inventory from
  overwhelming proven routes. That is more nuanced than round-robin or selecting
  solely by latency. See the [selection implementation](../proxy.go).
- **Leases coordinate actual exit addresses.** Different proxy gateways can
  share one public IP. `Lease.ClaimExit` enforces capacity against that observed
  address for the service within the pool. Detection is explicit and uses
  caller-configured IP services; the application must preserve the session used
  for probing and destination work. This matters when destination limits apply
  per IP. See [exit reservations](../lease/exit.go) and the
  [transfer lease documentation](../README.md#transfer-leases).
- **Long transfers have their own scheduling model.** Selection includes setup
  time, bandwidth and remaining bytes. `Lease.Upgrade` reserves a replacement
  when sustained measurements justify switching; the application handles
  cancellation and resumption. No equivalent to this complete combination was
  documented in the alternatives reviewed. See
  [adaptive transfers](../lease/adaptive.go).

The main tradeoffs are integration effort and deployment scope. Callers must
classify outcomes, manage sessions and clean up responses. Proxytron keeps scoped
service refusals and challenges separate from endpoint-wide transport failures;
user cancellations are unscored. Coordination is local to a pool, and the default
Bolt store has one owning process. A custom persistence backend alone would not
provide distributed reservations. Active leases are not restored after restart.
The module requires Go 1.26 or later. See the
[request API responsibilities](../request.go), [storage model](../store/store.go)
and [Go version requirement](../go.mod).

Proxytron is a strong fit for a Go downloader with unreliable proxies, expensive
setup and IP-bound state. For ordinary proxied HTTP requests, go-proxator or
proxyhive are alternatives to evaluate. For browser crawling, Crawlee covers more
of the application. A simple switcher can be sufficient when the requirement is
just rotating a stable proxy list.
