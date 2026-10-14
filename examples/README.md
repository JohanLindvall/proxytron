# Examples

Run these commands from the repository root with your own destination and proxy.
They make network requests only to the URLs you supply. Both stop on Ctrl+C or
their overall `-timeout`, and write response bytes to stdout.

## Persistent request rotation

```sh
go run ./examples/rotate \
  -url 'https://destination.example.test/data' \
  -proxy 'http://proxy-a.example.test:8080' \
  -proxy 'socks5h://proxy-b.example.test:1080' \
  -db requests.db
```

[rotate/main.go](rotate/main.go) restores a request pool, reuses a separate HTTP
client and cookie jar for each route, and runs a GET through `Pool.Do`. A shared
`Pacer` spaces requests to the same proxy host. The example buffers at most 1 MiB
and retries network errors using the route budget. Those errors remain unscored
until an application-specific classifier can identify their cause; successful
responses update the pool. Replace the marked classification points for your
service's refusal and challenge semantics.

For an explicitly direct request, supply both `-proxy direct` and `-allow-direct`.
Pacing and learned health persist for the lifetime of their owners: the pacer is
in memory, while pool health is saved to the database between runs.

## Transfer lease and exit confirmation

```sh
go run ./examples/transfer \
  -url 'https://destination.example.test/file' \
  -proxy 'http://proxy.example.test:8080' \
  -ip-check 'https://ip-check.example.test/ip' \
  -ip-check-backup 'https://backup.example.test/ip' \
  -db transfers.db > download.bin
```

[transfer/main.go](transfer/main.go) reserves one route, confirms and claims its
exit with `ExitProber`, then streams the response using that lease's context.
It demonstrates body, lease, client and pool cleanup in the required order.
Failed or shared exit checks stop before the destination request. No independent
direct probe or outage bypass is enabled. To choose a direct lease explicitly,
use `-proxy direct`.

Use different database paths for request and lease pools. The files contain
proxy credentials and are created with restrictive permissions. Applications
can use a custom `lease.Client` for their TLS, session and response policies.

## Executable API examples

The root [example_test.go](../example_test.go) also demonstrates outcome budgets,
pacing, feed parsing and credential redaction. The lease
[example_test.go](../lease/example_test.go) uses a loopback proxy to demonstrate
exit confirmation. Run them without contacting a public service:

```sh
go test ./... -run '^Example'
```
