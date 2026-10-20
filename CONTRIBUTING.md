# Contributing

Contributions are welcome: bug reports, clearer documentation, small examples,
regression tests and implementation changes all help. You do not need access to
public proxies to work on Proxytron.

Read the [code of conduct](CODE_OF_CONDUCT.md) and use the
[support guide](SUPPORT.md) for questions or bug reports. Report vulnerabilities
privately as described in [SECURITY.md](SECURITY.md).

## Your first contribution

- Try the [local demo](examples/quickstart/main.go) and improve any unclear step.
- Add a small example for an API you use.
- Reproduce a reported bug with a loopback server or synthetic fixture.
- Browse [good first issues](https://github.com/JohanLindvall/proxytron/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22)
  and [help wanted](https://github.com/JohanLindvall/proxytron/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22help%20wanted%22).

For a small fix, open a pull request directly. For a new public API, dependency or
change to persistence, start an issue explaining the problem and proposed
behavior so the design can be discussed before substantial work. Draft pull
requests are welcome when you want early feedback.

## Local setup

Install Git and Go 1.26 or later. The race detector also needs a working C
compiler. Fork the repository on GitHub and clone your fork, replacing
`YOUR-USERNAME`:

```sh
git clone https://github.com/YOUR-USERNAME/proxytron.git
cd proxytron
git switch -c describe-your-change
go mod download
go run ./examples/quickstart
```

No API keys, live feeds or external proxy services are required for the tests.
See [AGENTS.md](AGENTS.md) for the package layout and implementation constraints.
The [user guide](docs/guide.md) describes the behavior those packages expose.

## Validation

Format changed Go files with `gofmt`, then run from the repository root:

```sh
go test -race ./...
go vet ./...
git diff --check
```

Optional Make shortcuts are available on systems with Make:

| Command | Purpose |
| --- | --- |
| `make demo` | Run the local quickstart |
| `make test` | Run all tests with the race detector |
| `make vet` | Run Go's static checks |
| `make fmt` | Format Go files |
| `make check` | Check formatting, run race tests and run vet |
| `make coverage` | Report test coverage by package |

Use `go test ./... -run '^Example'` to run executable examples, or a package and
`-run` filter for focused iteration. Run `go test -cover ./...` when assessing
coverage. CI tests the minimum and current Go releases on Linux and the current
release on macOS and Windows; the exact matrix is in
[ci.yml](.github/workflows/ci.yml).

Run `go mod tidy` when changing dependencies and review the `go.mod` and `go.sum`
diff. Do not commit a temporary Go workspace or local filesystem `replace`
directive. For documentation-only changes, check relative links and examples
against the current API; report which checks you ran.

## Implementation and review

Keep application domains, configuration-file formats, browser fingerprints and
response classification in caller adapters. Selection, persistence, transport and
concurrency changes need behavioral regression tests. Keep fixtures synthetic and
tests independent of public proxy feeds. Preserve historical store formats or
supply an explicit migration; review golden fixture changes for compatibility.

Public APIs require Go documentation and examples. Use `context.Context` for I/O,
keep network calls outside pool locks, keep credential-bearing endpoints out of
logs, and report ownership of clients, goroutines and stores in API comments.

Keep pull requests focused. Explain the concrete problem, the resulting behavior,
any compatibility impact and the validation performed. Link a related issue when
one exists and update user-facing documentation when behavior changes. You can
submit a documentation or example improvement without changing library code.

Dependencies and GitHub Actions pins are maintained through Dependabot. Keep
unrelated dependency updates separate from a behavior change.

## Releases

Follow [Go's module release workflow](https://go.dev/doc/modules/release-workflow).
After CI passes for the exact commit, create an annotated semantic-version tag
such as `v0.1.0` and push it. The release workflow validates the tag and creates
GitHub release notes. This is a source library; there are no binary artifacts.
Do not move or reuse published tags. Breaking changes after v1 require a new major
module path. During v0, document API changes in release notes.

Work on multiple repositories locally using a temporary Go workspace or temporary
`replace` directives. Consumer commits must reference a published module version
and must not contain local filesystem replacements.
