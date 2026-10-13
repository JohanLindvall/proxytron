# Repository guidance

These instructions apply throughout this repository. Read [README.md](README.md)
for usage, [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and release policy,
and [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Project layout

Proxytron provides persistent, scored proxy pools for Go. The minimum Go version
is declared in `go.mod`; supported CI versions are in `.github/workflows/ci.yml`.

- The root `proxytron` package owns request rotation, discovery, live
  configuration, proxy transports and exit IP detection.
- `lease/` owns transfer reservations, per-address limits, throughput selection,
  adaptive upgrades and its own Bolt persistence.
- `store/` defines the request pool's persistence interface and Bolt backend.
- `internal/testutil/` and `internal/logtest/` contain shared test helpers.
- `testdata/` contains synthetic inventory, outcome and migration fixtures.

## Implementation constraints

- Keep application domains, configuration-file formats, browser fingerprints and
  response classification in caller adapters. Keep documentation focused on
  proxytron's behavior without naming consuming projects.
- Preserve the distinction between scoped service refusals, challenges and
  endpoint-wide transport failures. User cancellations must not penalize routes.
- An empty pool acquisition means no eligible route. Direct routing must be
  explicitly enabled; explicit proxies must never silently fall back to direct
  egress.
- Use `context.Context` for I/O and keep network calls outside pool locks.
  Document ownership and cleanup of clients, goroutines, stores and leases.
- Preserve protocol authentication and DNS behavior. Keep proxy TLS separate from
  destination TLS, with certificate verification enabled by default.
- Keep exit IP detection explicit and caller-configured. Preserve per-address
  limits and active exit reservations when updating lease or egress behavior.
- Preserve persisted schemas, JSON field names and transactional outcomes, or
  provide an explicit migration. Request and lease pools use separate storage
  namespaces; active leases are not restored after restart. Corrupt state must
  fail loading rather than silently reset health.
- Redact credential-bearing endpoints in diagnostics. Persistence files contain
  credentials and must retain restrictive permissions.
- Document exported APIs with Go comments and examples. Keep changes focused and
  update user-facing documentation when behavior changes.

## Validation

Format changed Go files with `gofmt`. For Go changes, run from the repository root:

```sh
go test -race ./...
go vet ./...
```

Use `go test -cover ./...` when assessing coverage. Run `go mod tidy` when changing
dependencies and review the `go.mod` and `go.sum` diff. Do not commit temporary Go
workspaces or local filesystem `replace` directives.

Selection, persistence, transport and concurrency changes need behavioral
regression coverage. Use loopback servers, fake clocks and synthetic endpoints;
tests must not depend on public proxies or live feeds. Review golden fixture
changes for compatibility rather than regenerating them blindly.

For documentation-only changes, check the diff, relative links and consistency
with the current API. Report validation performed and any checks not run.
