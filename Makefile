SHELL := /bin/bash
ZIG_MAKE := $(MAKE) -C ./zig
ZIG_BUILD_FLAGS ?=
SCRIPTS_PY ?= uv run --project scripts --locked python
# ====================================================================================
# Go Version Configuration
# ====================================================================================
# Use Go 1.26 with SIMD experiment enabled for hardware SIMD acceleration
GO := GOWORK=off GOEXPERIMENT=simd go
ANTFLY_GO_MODULE := ./go/pkg/antfly

# Go modules outside of the Antfly product module
GO_SUBMODULES := \
	./go/e2e \
	./go/pkg/sdk \
	./go/pkg/proxy/antfly \
	./go/pkg/proxy/termite \
	./go/pkg/libaf \
	./go/pkg/operator \
	./go/pkg/docsaf \
	./go/pkg/generating \
	./go/pkg/evalaf \
	./go/pkg/evalaf/plugins/antfly \
	./go/pkg/genkit/antfly \
	./go/pkg/genkit/openrouter \
	./go/pkg/memoryaf \
	./go/pkg/termite

# ====================================================================================
# General Commands
# ====================================================================================

.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build              Build the Zig antfly binary"
	@echo "  build-go           Build the legacy Go antfly binary"
	@echo "  build-antfarm      Build the antfarm frontend (React admin UI)"
	@echo "  build-docs         Join OpenAPI specifications"
	@echo "  generate           Generate code, client SDKs, and all website documentation (API, config, changelog)"
	@echo "  lint               Run golangci-lint with auto-fix"
	@echo "  tidy               Run go mod tidy across root and Go submodules"
	@echo "  tidy-check         Verify go.mod/go.sum are tidy across root and Go submodules"
	@echo "  zig-build          Build the migrated Zig runtime"
	@echo "  zig-test           Run the migrated Zig test aggregate"
	@echo "  zig-generate       Regenerate migrated Zig generated sources"
	@echo "  zig-generated-check  Verify migrated Zig generated sources"
	@echo "  install-git-hooks  Configure Git to use the repository hooks in .githooks/"
	@echo "  update-deps        Update Go dependencies"
	@echo "  sim-validate       Run simulator-focused validation"
	@echo "  sim-validate-repo  Run broader repo validation including go test ./..."
	@echo "  sim-soak           Run simulator soak scenarios"
	@echo ""
	@echo "E2E Testing Commands:"
	@echo "  e2e                Run e2e tests with ONNX+XLA (downloads deps on first run)"
	@echo "                     Options: E2E_TEST=TestName E2E_TIMEOUT=60m"
	@echo "  e2e-deps           Download ONNX Runtime and PJRT for e2e tests"
	@echo ""
	@echo "ML Backend Commands:"
	@echo "  build-omni         Build antfly with ONNX + XLA backends (omni)"
	@echo ""
	@echo "Omni Cross-Compilation (for goreleaser):"
	@echo "  download-omni-deps     Download ONNX Runtime and PJRT to antfly root"
	@echo ""
	@echo "TLA+ Verification Commands:"
	@echo "  tla-tools          Download TLA+ tools (tla2tools.jar, CommunityModules)"
	@echo "  tla-check          Run TLC model checker on all Antfly TLA+ specs"
	@echo "  tla-check-txn      Model check transaction spec only (~10s)"
	@echo "  tla-check-split    Model check shard split spec only"
	@echo "  tla-check-snap     Model check snapshot transfer spec only (~90s)"
	@echo "  tla-trace-raft     Validate raft ndjson traces against etcd/raft TLA+ spec"
	@echo "                     Options: TRACE_FILES=path/to/*.ndjson"
	@echo "  tla-trace-txn      Validate transaction ndjson traces against AntflyTransaction"
	@echo "                     Options: TRACE_FILES=path/to/*.ndjson"
	@echo ""
	@echo "Minikube Commands:"
	@echo "  minikube-start     Start a Minikube instance"
	@echo "  minikube-delete    Delete the Minikube instance"
	@echo "  minikube-deploy    Deploy the application to Minikube"
	@echo "  minikube-status    Get the status of the Minikube deployment"
	@echo "  minikube-restart   Restart the Minikube instance"
	@echo "  show-ingress       Show the Ingress IP and example commands"


# ====================================================================================
# Build and Generation Commands
# ====================================================================================

.PHONY: build build-go build-docs generate lint license-headers license-check update-deps tidy tidy-check install-git-hooks build-antfarm sim-validate sim-validate-repo sim-soak
.PHONY: zig-build zig-test zig-unit-test zig-generate zig-generated-check zig-openapi-check zig-snowball-check zig-license-headers zig-license-check zig-tla-check

build-antfarm: build-antfarm-main

build-antfarm-main:
	@echo "Building antfarm frontend..."
	cd ts && pnpm install && pnpm --filter antfarm... build
	@echo "Copying dist files to go/pkg/antfly/src/metadata/antfarm..."
	rm -rf go/pkg/antfly/src/metadata/antfarm/*
	cp -r ts/apps/antfarm/dist/* go/pkg/antfly/src/metadata/antfarm/

build: build-antfarm
	$(ZIG_MAKE) build ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"
	cp zig/zig-out/bin/antfly ./antfly

build-go: build-antfarm generate
	(cd $(ANTFLY_GO_MODULE) && $(GO) build -tags "afrelease" -ldflags="-s -w" -o ../../../antfly ./cmd)

build-docs:
	uv run --project scripts --locked python scripts/join_public_openapi.py openapi.yaml

generate: build-docs tidy
	(cd $(ANTFLY_GO_MODULE) && $(GO) generate ./...)
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Generating in $$mod"; \
		(cd $$mod && $(GO) generate ./...) || exit 1; \
	done
	cd ts && pnpm --filter @antfly/sdk generate
	$(MAKE) -C ./py/packages/sdk generate

license-headers: ## Add first-party license headers.
	$(SCRIPTS_PY) scripts/license_headers.py

license-check: ## Check first-party license headers.
	$(SCRIPTS_PY) scripts/license_headers.py --check

zig-build:
	$(ZIG_MAKE) build ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"

zig-test:
	$(ZIG_MAKE) test

zig-unit-test:
	$(ZIG_MAKE) unit-test

zig-generate:
	$(ZIG_MAKE) generate

zig-generated-check:
	$(ZIG_MAKE) generated-check

zig-openapi-check:
	$(ZIG_MAKE) openapi-check

zig-snowball-check:
	$(ZIG_MAKE) snowball-check

zig-license-headers:
	$(ZIG_MAKE) license-headers

zig-license-check:
	$(ZIG_MAKE) license-check

zig-tla-check:
	$(ZIG_MAKE) tla-check

lint:
	$(GO) run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./...
	$(GO) run github.com/Antonboom/testifylint@latest --fix ./...
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Linting $$mod"; \
		(cd $$mod && $(GO) run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...) && \
		(cd $$mod && $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./...) && \
		(cd $$mod && $(GO) run github.com/Antonboom/testifylint@latest --fix ./...) || exit 1; \
	done
	cd ts && pnpm run lint

sim-validate:
	(cd $(ANTFLY_GO_MODULE) && $(GO) run ./cmd/sim -action validate -scope sim)

sim-validate-repo:
	(cd $(ANTFLY_GO_MODULE) && $(GO) run ./cmd/sim -action validate -scope repo)

sim-soak:
	(cd $(ANTFLY_GO_MODULE) && $(GO) run ./cmd/sim -action soak -json)


# ====================================================================================
# Omni Dependencies Download (for goreleaser CGO cross-compilation)
# ====================================================================================
#
# Downloads ONNX Runtime and PJRT libraries needed for goreleaser omni builds.
# Run this before: goreleaser release --snapshot --clean
#
# Downloads to ./onnxruntime and ./pjrt by default.
# Uses stamp files to skip if already downloaded.

ONNXRUNTIME_ROOT ?= $(CURDIR)/onnxruntime
PJRT_ROOT ?= $(CURDIR)/pjrt

ONNXRUNTIME_VERSION ?= 1.24.3
GENAI_VERSION ?= 0.12.1
PJRT_VERSION ?= 0.83.4

ONNXRUNTIME_STAMP := $(ONNXRUNTIME_ROOT)/.version-$(ONNXRUNTIME_VERSION)-$(GENAI_VERSION)
PJRT_STAMP := $(PJRT_ROOT)/.version-$(PJRT_VERSION)

$(ONNXRUNTIME_STAMP): scripts/download-onnxruntime.sh
	@echo "Downloading ONNX Runtime (version changed or first run)..."
	@rm -f $(ONNXRUNTIME_ROOT)/.version-*
	ONNXRUNTIME_ROOT=$(ONNXRUNTIME_ROOT) ./scripts/download-onnxruntime.sh $(ONNXRUNTIME_VERSION) $(GENAI_VERSION)
	@touch $@

$(PJRT_STAMP): scripts/download-pjrt.sh
	@echo "Downloading PJRT (version changed or first run)..."
	@rm -f $(PJRT_ROOT)/.version-*
	PJRT_ROOT=$(PJRT_ROOT) ./scripts/download-pjrt.sh $(PJRT_VERSION)
	@touch $@

.PHONY: download-omni-deps force-download-omni-deps

download-omni-deps: $(ONNXRUNTIME_STAMP) $(PJRT_STAMP) ## Download ONNX Runtime and PJRT (skips if up-to-date).

force-download-omni-deps: ## Force re-download of ONNX Runtime and PJRT.
	@rm -f $(ONNXRUNTIME_ROOT)/.version-* $(PJRT_ROOT)/.version-*
	$(MAKE) download-omni-deps

tidy:
	(cd $(ANTFLY_GO_MODULE) && $(GO) mod tidy)
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Tidying $$mod"; \
		(cd $$mod && $(GO) mod tidy) || exit 1; \
	done

tidy-check:
	(cd $(ANTFLY_GO_MODULE) && $(GO) mod tidy -diff)
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Checking tidy in $$mod"; \
		(cd $$mod && $(GO) mod tidy -diff) || exit 1; \
	done

install-git-hooks:
	git config core.hooksPath .githooks
	@echo "Configured Git hooks path to .githooks/"

update-deps:
	$(GO) get -u ./...
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Updating deps in $$mod"; \
		(cd $$mod && $(GO) get -u ./...) || exit 1; \
	done
	$(MAKE) tidy


# ====================================================================================
# ML Backend Build Targets
# ====================================================================================
# Build Antfly with various ML backend configurations

.PHONY: build-omni

build-omni: download-omni-deps
	@echo "Building antfly with ONNX + XLA backends (omni)..."
	@echo "Platform: $(E2E_PLATFORM)"
	export ONNXRUNTIME_ROOT=$(ONNXRUNTIME_ROOT) && \
	export PJRT_ROOT=$(PJRT_ROOT) && \
	export CGO_ENABLED=1 && \
	export LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$LIBRARY_PATH && \
	export LD_LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$LD_LIBRARY_PATH && \
	export DYLD_LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$DYLD_LIBRARY_PATH && \
	(cd $(ANTFLY_GO_MODULE) && $(GO) build -tags="onnx,ORT,xla,XLA" -ldflags="-s -w" -o ../../../antfly ./cmd)


# ====================================================================================
# E2E Testing Commands
# ====================================================================================
# End-to-end tests for docsaf that require ONNX/XLA backends and ML models

# Detect OS and architecture for library paths
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        E2E_PLATFORM := darwin-arm64
    else
        E2E_PLATFORM := darwin-amd64
    endif
else
    ifeq ($(UNAME_M),aarch64)
        E2E_PLATFORM := linux-arm64
    else
        E2E_PLATFORM := linux-amd64
    endif
endif

.PHONY: e2e e2e-deps

# E2E test configuration
E2E_TEST ?=
E2E_TIMEOUT ?= 60m
E2E_MEMLIMIT ?= 16GiB

e2e-deps: download-omni-deps

e2e: e2e-deps
	@echo "Running E2E tests with ONNX+XLA build (Termite provider)..."
	@echo "This will download models on first run (embedder, chunker, reranker, generator)."
	@echo "Platform: $(E2E_PLATFORM)"
ifdef E2E_TEST
	@echo "Test: $(E2E_TEST)"
endif
	@echo "Timeout: $(E2E_TIMEOUT)"
	@echo "Memory limit: $(E2E_MEMLIMIT)"
	export ONNXRUNTIME_ROOT=$(ONNXRUNTIME_ROOT) && \
	export PJRT_ROOT=$(PJRT_ROOT) && \
	export CGO_ENABLED=1 && \
	export GOMEMLIMIT=$(E2E_MEMLIMIT) && \
	export LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$LIBRARY_PATH && \
	export LD_LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$LD_LIBRARY_PATH && \
	export DYLD_LIBRARY_PATH=$(ONNXRUNTIME_ROOT)/$(E2E_PLATFORM)/lib:$$DYLD_LIBRARY_PATH && \
	export RUN_EVAL_TESTS=true && \
	export E2E_PROVIDER=termite && \
	cd go/e2e && $(GO) test -v -tags="onnx,ORT,xla,XLA" -timeout $(E2E_TIMEOUT) $(if $(E2E_TEST),-run '$(E2E_TEST)') ./...


# ====================================================================================
# TLA+ Verification Commands
# ====================================================================================

GOMODCACHE := $(shell go env GOMODCACHE)
RAFT_TLA := $(GOMODCACHE)/go.etcd.io/raft/v3@v3.6.0/tla

.PHONY: tla-tools tla-check tla-check-txn tla-check-split tla-check-snap tla-trace-raft tla-trace-txn

tla-tools:
	@bash scripts/tla-tools.sh

tla-check: tla-check-txn tla-check-split tla-check-snap

tla-check-txn: tla-tools
	@echo "==> Model checking transaction spec..."
	source scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflyTransaction.cfg specs/tla/MC.tla \
	  -workers auto -deadlock

tla-check-split: tla-tools
	@echo "==> Model checking shard split spec..."
	source scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflyShardSplit.cfg specs/tla/ShardSplitMC.tla \
	  -workers auto -deadlock

tla-check-snap: tla-tools
	@echo "==> Model checking snapshot transfer spec (safety only, ~90s)..."
	source scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflySnapshotTransfer-safety.cfg specs/tla/SnapshotTransferMC.tla \
	  -workers auto -deadlock

tla-trace-raft: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson)
endif
	@bash scripts/tla-validate-trace.sh -S \
	  -s "$(RAFT_TLA)/Traceetcdraft.tla" \
	  -c "$(RAFT_TLA)/Traceetcdraft.cfg" \
	  $(TRACE_FILES)

tla-trace-txn: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson)
endif
	@bash scripts/tla-validate-trace.sh -S \
	  -s specs/tla/TraceAntflyTransaction.tla \
	  -c specs/tla/TraceAntflyTransaction.cfg \
	  $(TRACE_FILES)


# ====================================================================================
# Minikube Commands
# ====================================================================================

.PHONY: minikube-start minikube-delete minikube-deploy minikube-status minikube-restart build-minikube show-ingress

minikube-start:
	minikube start --driver=vfkit --container-runtime containerd --cpus 3 --memory "7G" --disk-size "20G" --profile=minikube
	minikube addons enable metrics-server --profile=minikube
	minikube addons enable ingress --profile=minikube
	minikube addons enable ingress-dns --profile=minikube
	minikube addons enable registry --profile=minikube
	$(MAKE) minikube-deploy

minikube-delete:
	minikube delete --profile=minikube

minikube-deploy: build-minikube
	@echo "Waiting for Ingress controller deployment to be ready..."
	@kubectl wait --namespace ingress-nginx \
		--for=condition=available deployment \
		--selector=app.kubernetes.io/component=controller \
		--timeout=120s --context=minikube || \
		(echo "Error: Ingress controller deployment did not become ready." && exit 1)
	@echo "Applying Kubernetes manifests..."
	@kubectl --context=minikube apply -R -f ./devops/minikube/
	@echo "Waiting for ingress resource to be created and potentially assign IP..."
	@sleep 10 # Give ingress resource time to be processed
	$(MAKE) show-ingress

minikube-status:
	@echo "Pods:"
	@kubectl --context=minikube get pods
	@echo "\nServices:"
	@kubectl --context=minikube get services
	@echo "\nDeployments:"
	@kubectl --context=minikube get deployments

minikube-restart: minikube-delete minikube-start

build-minikube:
	minikube image build --profile=minikube -t localhost:5000/antfly:latest .

show-ingress:
	@echo "Fetching Ingress IP..."
	@INGRESS_IP=$$(kubectl --context=minikube get ingress antfly-ingress -o jsonpath='{.status.loadBalancer.ingress[0].ip}'); \
	if [ -z "$$INGRESS_IP" ]; then \
		echo "Ingress IP not available yet. Trying again after delay..."; \
		sleep 10; \
		INGRESS_IP=$$(kubectl --context=minikube get ingress antfly-ingress -o jsonpath='{.status.loadBalancer.ingress[0].ip}'); \
	fi; \
	if [ -z "$$INGRESS_IP" ]; then \
		echo "Error: Could not retrieve Ingress IP."; \
		echo "Ensure Minikube tunnel is running if needed (e.g., 'minikube tunnel --profile=minikube' in another terminal)"; \
		echo "and the ingress controller pod is running correctly ('kubectl --context=minikube get pods -n ingress-nginx')."; \
		exit 1; \
	fi; \
	echo "Ingress Controller IP: $$INGRESS_IP"; \
	echo ""; \
	echo "Example Access Commands:"; \
	echo "  # Access Leader API (replace /api/endpoint with actual path)"; \
	echo "  curl http://$$INGRESS_IP/leader/api/endpoint"; \
	echo ""; \
	echo "  # Access Worker 1 API (replace /api/endpoint with actual path)"; \
	echo "  curl http://$$INGRESS_IP/worker-1/api/endpoint"; \
	echo ""; \
	echo "  # Access Worker 2 API (replace /api/endpoint with actual path)"; \
	echo "  curl http://$$INGRESS_IP/worker-2/api/endpoint"; \
	echo ""; \
	echo "Note: If using Minikube Docker/Podman driver without LoadBalancer support, you might need 'minikube tunnel --profile=minikube' in a separate terminal."


# ====================================================================================
# Operator Commands
# ====================================================================================

.PHONY: operator-build operator-test operator-docker-build operator-lint \
        termite-build termite-test termite-lint \
        termite-client-test termite-client-lint

operator-build: ## Build the antfly-operator binary
	(cd ./go/pkg/operator && $(MAKE) build)

operator-test: ## Run antfly-operator tests
	(cd ./go/pkg/operator && $(MAKE) test)

operator-lint: ## Run linter on antfly-operator
	(cd ./go/pkg/operator && $(MAKE) lint)

operator-docker-build: ## Build antfly-operator Docker image
	docker build -t antfly-operator:latest -f ./go/pkg/operator/Dockerfile .

termite-build: ## Build the termite binary (pure Go)
	(cd ./go/pkg/termite && $(GO) build -o ../../termite ./cmd)

termite-test: ## Run termite unit tests (pure Go)
	(cd ./go/pkg/termite && $(GO) test ./...)

termite-lint: ## Run linter on termite
	(cd ./go/pkg/termite && $(GO) vet ./...)

termite-client-test: ## Run termite-client tests
	(cd ./go/pkg/sdk && $(GO) test ./...)

termite-client-lint: ## Run linter on termite-client
	(cd ./go/pkg/sdk && $(GO) vet ./...)
