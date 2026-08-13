GO       ?= go
BIN      := bin
CLUSTER  ?= microvm
BINARIES := placement-api vmhostd loadgen

# Peak rate for `make load`.
PEAK_RPS ?= 200
RAMP     ?= 15s
CYCLES   ?= 2

.DEFAULT_GOAL := help

# # help: list available targets.
.PHONY: help
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F': ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# # build: compile all binaries into ./bin.
.PHONY: build
build: $(addprefix $(BIN)/,$(BINARIES))

$(BIN)/%: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $@ ./cmd/$*

FORCE:

# # test: unit tests with the race detector, including the full 1000 rps run.
.PHONY: test
test:
	$(GO) test -race ./...

# # test-short: skip the long end-to-end runs.
.PHONY: test-short
test-short:
	$(GO) test -race -short ./...

# # cover: test and write a coverage report.
.PHONY: cover
cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

# # bench: benchmark the placement and arrival-process hot paths.
.PHONY: bench
bench:
	$(GO) test -run=XXX -bench=. -benchmem ./internal/scheduler/ ./internal/loadgen/

# # lint: run golangci-lint.
.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "install with: brew install golangci-lint"; exit 1; }
	golangci-lint run

# # fmt: format the tree.
.PHONY: fmt
fmt:
	$(GO) fmt ./...

# # tidy: sync go.mod and go.sum.
.PHONY: tidy
tidy:
	$(GO) mod tidy

# # verify: everything CI runs, locally.
.PHONY: verify
verify: fmt tidy lint test

# # demo: bring up the whole stack and run a load test.
.PHONY: demo
demo: demo-up load

# # demo-up: bring up kind, Karpenter, CapacityBuffer, KEDA and the app.
.PHONY: demo-up
demo-up:
	@bash deploy/kind/up.sh

# # load: run the double-ramp load test against the local cluster.
.PHONY: load
load: $(BIN)/loadgen
	@mkdir -p results
	./$(BIN)/loadgen \
		-target http://127.0.0.1:18080 \
		-peak-rps $(PEAK_RPS) -ramp-up $(RAMP) -ramp-down $(RAMP) \
		-cycles $(CYCLES) -ttl 500ms \
		-results results/run.jsonl

# # guest: build the QEMU guest kernel and initramfs.
.PHONY: guest
guest:
	$(MAKE) -C images/guest

# # demo-status: show what the cluster is currently doing.
.PHONY: demo-status
demo-status:
	@echo "--- fleet ---"
	@curl -fsS http://127.0.0.1:18080/readyz || echo "placement API unreachable"
	@echo "\n--- capacity buffers ---"
	@kubectl -n microvm get capacitybuffer -o custom-columns=\
'NAME:.metadata.name,REPLICAS:.status.replicas,STRATEGY:.status.provisioningStrategy' 2>/dev/null || true
	@echo "--- karpenter nodes ---"
	@kubectl get nodeclaims --no-headers 2>/dev/null | wc -l | xargs echo "nodeclaims:"
	@echo "--- drops (must be zero) ---"
	@curl -fsS http://127.0.0.1:18080/metrics 2>/dev/null | grep '^microvm_requests_dropped_total' || true

# # demo-down: delete the kind cluster.
.PHONY: demo-down
demo-down:
	kind delete cluster --name $(CLUSTER)

# # clean: remove build and test artifacts.
.PHONY: clean
clean:
	rm -rf $(BIN) coverage.out coverage.html results
