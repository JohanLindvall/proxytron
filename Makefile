.PHONY: help demo test vet fmt fmt-check check coverage
.DEFAULT_GOAL := help

help:
	@printf '%s\n' \
		'make demo       Run the local quickstart' \
		'make test       Run all tests with the race detector' \
		'make vet        Run Go static checks' \
		'make fmt        Format Go files' \
		'make fmt-check  Check Go formatting without changing files' \
		'make check      Check formatting, run race tests and run vet' \
		'make coverage   Report test coverage by package'

demo:
	go run ./examples/quickstart

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files="$$(gofmt -l .)" || exit 1; \
	if [ -n "$$files" ]; then \
		printf 'Run gofmt on:\n%s\n' "$$files"; \
		exit 1; \
	fi

check: fmt-check test vet

coverage:
	go test -cover ./...
