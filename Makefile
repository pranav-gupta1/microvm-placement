GO      ?= go
GOBIN   ?= $(shell $(GO) env GOPATH)/bin
BIN     := bin
PKG     := github.com/pranav-gupta1/microvm-placement
BINARIES := placement-api vmhostd loadgen

.DEFAULT_GOAL := help

## help: list available targets
.PHONY: help
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F': ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## build: compile all binaries into ./bin
.PHONY: build
build: $(addprefix $(BIN)/,$(BINARIES))

$(BIN)/%: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $@ ./cmd/$*

FORCE:

## test: run the unit tests with the race detector
.PHONY: test
test:
	$(GO) test -race ./...

## cover: run tests and open a coverage report
.PHONY: cover
cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

## bench: run benchmarks for the hot-path packages
.PHONY: bench
bench:
	$(GO) test -run=XXX -bench=. -benchmem ./internal/scheduler/ ./internal/loadgen/

## lint: run golangci-lint
.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install it with:"; \
		echo "  brew install golangci-lint"; \
		exit 1; }
	golangci-lint run

## fmt: format the tree
.PHONY: fmt
fmt:
	$(GO) fmt ./...

## tidy: sync go.mod and go.sum
.PHONY: tidy
tidy:
	$(GO) mod tidy

## verify: everything CI runs, locally
.PHONY: verify
verify: fmt tidy test lint

## clean: remove build and test artifacts
.PHONY: clean
clean:
	rm -rf $(BIN) coverage.out coverage.html
