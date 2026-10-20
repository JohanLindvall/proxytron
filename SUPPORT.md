# Getting help

Start with the [quickstart](README.md#quickstart), [runnable examples](examples/README.md),
[user guide](docs/guide.md) and [troubleshooting guide](docs/troubleshooting.md).
The [API reference](https://pkg.go.dev/github.com/JohanLindvall/proxytron) includes
executable examples.

## Questions, bugs and ideas

Search [existing issues](https://github.com/JohanLindvall/proxytron/issues) first.
If you do not find an answer, [open an issue](https://github.com/JohanLindvall/proxytron/issues/new/choose)
and choose a question, bug report or feature request. Questions about using the
library are welcome; you do not need to know which package causes a problem.

For a bug, include:

- The Proxytron version or commit: `go list -m github.com/JohanLindvall/proxytron`.
- Your Go version and platform: `go version` and `go env GOOS GOARCH`.
- What you expected and what happened, including the error text.
- The smallest reproducer you can provide, ideally using `httptest` or synthetic
  endpoints instead of a live proxy.

For a feature, describe the use case and what you have tried. This helps determine
whether it belongs in the library, an example or a caller adapter.

Remove credentials and sensitive destinations from code and logs. Use
`proxytron.Redact` for endpoint diagnostics. Do not upload database files or real
proxy inventories: they contain credentials. If a detail is sensitive, replace
it with a synthetic equivalent.

Support takes place in public issues so other users can learn from the answer.
Response times depend on maintainer availability.

## Private reports

Report vulnerabilities through the private channel in [SECURITY.md](SECURITY.md).
For conduct concerns, use the contact in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
Neither needs a public issue.
