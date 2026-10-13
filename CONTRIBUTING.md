# Contributing

Keep application domains, configuration-file formats, browser fingerprints and
response classification in adapters. Changes to selection or persistence need
behavioral regression tests. Keep fixtures synthetic and tests independent of
public proxy feeds. Preserve historical store formats or supply an explicit migration.

Run `gofmt -w .`, `go vet ./...` and `go test -race ./...`. Check module hygiene with
`go mod tidy` and review its diff. CI tests the minimum and current Go releases on
Linux and tests the current release on macOS and Windows.

Public APIs require Go documentation and examples. Use `context.Context` for I/O,
keep network calls outside pool locks, keep credential-bearing endpoints out of
logs, and report ownership of clients, goroutines and stores in API comments.

Use focused pull requests with the behavior changed and relevant validation.
Dependencies and GitHub Actions pins are maintained through Dependabot.

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
