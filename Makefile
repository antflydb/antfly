SHELL := /bin/bash
# ====================================================================================
# Go Version Configuration
# ====================================================================================
# Use Go 1.26 with SIMD experiment enabled for hardware SIMD acceleration
GO := GOEXPERIMENT=simd go

# Go modules outside of root
GO_SUBMODULES := \
	./e2e \
	./pkg/client \
	./pkg/antfly-operator \
	./pkg/antfly-proxy \
	./pkg/libaf \
	./pkg/docsaf \
	./pkg/generating \
	./pkg/evalaf \
	./pkg/evalaf/plugins/antfly \
	./pkg/genkit/antfly \
	./pkg/genkit/openrouter \
	./pkg/termite \
	./pkg/termite-client \
	./pkg/termite-operator \
	./pkg/termite-proxy

# ====================================================================================
# General Commands
# ====================================================================================

.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build              Build the antfly binary"
	@echo "  build-antfarm      Build the antfarm frontend (React admin UI)"
	@echo "  build-termite-dashboard  Build the termite dashboard (termite-only antfarm)"
	@echo "  build-docs         Join and lint OpenAPI specifications"
	@echo "  generate           Generate code, client SDKs, and all website documentation (API, config, changelog)"
	@echo "  lint               Run golangci-lint with auto-fix"
	@echo "  tidy               Run go mod tidy across root and Go submodules"
	@echo "  tidy-check         Verify go.mod/go.sum are tidy across root and Go submodules"
	@echo "  install-git-hooks  Configure Git to use the repository hooks in .githooks/"
	@echo "  update-deps        Update Go dependencies"
	@echo "  cleanup-goreman    Clean up goreman logs and data"
	@echo "  sim-validate       Run simulator-focused validation"
	@echo "  sim-validate-repo  Run broader repo validation including go test ./..."
	@echo "  sim-soak           Run simulator soak scenarios"
	@echo ""
	@echo "E2E Testing Commands:"
	@echo "  e2e                Run e2e tests with ONNX+XLA (downloads deps on first run)"
	@echo "                     Options: E2E_TEST=TestName E2E_TIMEOUT=30m"
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

.PHONY: build build-docs generate lint license-headers license-check update-deps tidy tidy-check install-git-hooks build-antfarm build-termite-dashboard sim-validate sim-validate-repo sim-soak

build-antfarm: build-antfarm-main build-termite-dashboard

build-antfarm-main:
	@echo "Building antfarm frontend..."
	cd ts && pnpm install && pnpm --filter antfarm build
	@echo "Copying dist files to src/metadata/antfarm..."
	rm -rf src/metadata/antfarm/*
	cp -r ts/apps/antfarm/dist/* src/metadata/antfarm/

build-termite-dashboard:
	@echo "Building termite dashboard (antfarm with VITE_PRODUCTS=termite)..."
	cd ts && pnpm install && VITE_PRODUCTS=termite pnpm --filter antfarm build
	@echo "Copying dist files to pkg/termite/dashboard..."
	rm -rf pkg/termite/dashboard/*
	cp -r ts/apps/antfarm/dist/* pkg/termite/dashboard/

build: build-antfarm generate
	$(GO) build -tags "afrelease" -ldflags="-s -w" -o antfly ./cmd/antfly

build-docs:
	npx @redocly/cli@latest join src/metadata/api.yaml src/usermgr/api.yaml
	npx @redocly/cli@latest lint openapi.yaml

generate: build-docs build-termite-dashboard tidy
	$(GO) generate ./...
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Generating in $$mod"; \
		(cd $$mod && go generate ./...) || exit 1; \
	done
	cd ts && pnpm --filter @antfly/sdk generate
	cd ts && pnpm --filter @antfly/termite-sdk generate
	$(MAKE) -C ./py generate

license-headers: ## Add ELv2 license headers to core files missing them
	$(GO) run github.com/google/addlicense@latest \
		-f .license-header.txt \
		-ignore 'lib/multirafthttp/**' \
		-ignore 'lib/types/**' \
		-ignore 'pkg/**' \
		-ignore 'ts/**' \
		-ignore 'py/**' \
		-ignore 'rs/**' \
		-ignore 'vendor/**' \
		-ignore '.github/**' \
		-ignore 'examples/**' \
		-ignore 'configs/**' \
		-ignore 'devops/**' \
		-ignore 'scripts/**' \
		-ignore '**/*.pb.go' \
		-ignore '**/zz_generated*' \
		-ignore '**/Dockerfile*' \
		-ignore '**/*.yaml' \
		-ignore '**/*.yml' \
		.

license-check: ## Check that all core files have license headers
	$(GO) run github.com/google/addlicense@latest \
		-check \
		-f .license-header.txt \
		-ignore 'lib/multirafthttp/**' \
		-ignore 'lib/types/**' \
		-ignore 'pkg/**' \
		-ignore 'ts/**' \
		-ignore 'py/**' \
		-ignore 'rs/**' \
		-ignore 'vendor/**' \
		-ignore '.github/**' \
		-ignore 'examples/**' \
		-ignore 'configs/**' \
		-ignore 'devops/**' \
		-ignore 'scripts/**' \
		-ignore '**/*.pb.go' \
		-ignore '**/zz_generated*' \
		-ignore '**/Dockerfile*' \
		-ignore '**/*.yaml' \
		-ignore '**/*.yml' \
		.

lint:
	$(GO) run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./...
	$(GO) run github.com/Antonboom/testifylint@latest --fix ./...
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Linting $$mod"; \
		(cd $$mod && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...) && \
		(cd $$mod && go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./...) && \
		(cd $$mod && go run github.com/Antonboom/testifylint@latest --fix ./...) || exit 1; \
	done
	cd ts && pnpm run lint

sim-validate:
	$(GO) run ./cmd/debugging/sim -action validate -scope sim

sim-validate-repo:
	$(GO) run ./cmd/debugging/sim -action validate -scope repo

sim-soak:
	$(GO) run ./cmd/debugging/sim -action soak -json


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
	$(GO) mod tidy
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Tidying $$mod"; \
		(cd $$mod && go mod tidy) || exit 1; \
	done

tidy-check:
	$(GO) mod tidy -diff
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Checking tidy in $$mod"; \
		(cd $$mod && go mod tidy -diff) || exit 1; \
	done

install-git-hooks:
	git config core.hooksPath .githooks
	@echo "Configured Git hooks path to .githooks/"

update-deps:
	$(GO) get -u ./...
	@for mod in $(GO_SUBMODULES); do \
		echo "==> Updating deps in $$mod"; \
		(cd $$mod && go get -u ./...) || exit 1; \
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
	$(GO) build -tags="onnx,ORT,xla,XLA" -ldflags="-s -w" -o antfly ./cmd/antfly


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
E2E_TIMEOUT ?= 30m
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
	cd e2e && $(GO) test -v -tags="onnx,ORT,xla,XLA" -timeout $(E2E_TIMEOUT) $(if $(E2E_TEST),-run '$(E2E_TEST)') ./...


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

.PHONY: operator-build operator-test operator-docker-build operator-lint proxy-docker-build \
        termite-operator-build termite-operator-test termite-operator-lint termite-operator-docker-build \
        termite-proxy-build termite-proxy-docker-build \
        termite-build termite-test termite-lint termite-docker-build termite-omni-docker-build \
        termite-client-test termite-client-lint

operator-build: ## Build the antfly-operator binary
	(cd ./pkg/antfly-operator && $(MAKE) build)

operator-test: ## Run antfly-operator tests
	(cd ./pkg/antfly-operator && $(MAKE) test)

operator-lint: ## Run linter on antfly-operator
	(cd ./pkg/antfly-operator && $(MAKE) lint)

operator-docker-build: ## Build antfly-operator Docker image
	docker build -t antfly-operator:latest -f ./Dockerfile.antfly-operator .

proxy-docker-build: ## Build antfly-proxy Docker image
	docker build -t antfly-proxy:latest -f ./Dockerfile.antfly-proxy .

termite-operator-build: ## Build the termite-operator binary
	(cd ./pkg/termite-operator && $(GO) build -o ../../termite-operator ./cmd/termite-operator)

termite-operator-test: ## Run termite-operator tests
	(cd ./pkg/termite-operator && $(GO) test ./...)

termite-operator-lint: ## Run linter on termite-operator
	(cd ./pkg/termite-operator && $(GO) vet ./...)

termite-operator-docker-build: ## Build termite-operator Docker image
	docker build -t termite-operator:latest -f ./Dockerfile.termite-operator .

termite-proxy-build: ## Build the termite-proxy binary
	(cd ./pkg/termite-proxy && $(GO) build -o ../../termite-proxy ./cmd/termite-proxy)

termite-proxy-docker-build: ## Build termite-proxy Docker image
	docker build -t termite-proxy:latest -f ./Dockerfile.termite-proxy .

termite-build: ## Build the termite binary (pure Go)
	(cd ./pkg/termite && $(GO) build -o ../../termite ./cmd)

termite-test: ## Run termite unit tests (pure Go)
	(cd ./pkg/termite && $(GO) test ./...)

termite-lint: ## Run linter on termite
	(cd ./pkg/termite && $(GO) vet ./...)

termite-docker-build: ## Build termite Docker image (pure Go)
	docker build -t termite:latest -f ./Dockerfile.termite .

termite-omni-docker-build: ## Build termite:omni Docker image (ONNX + XLA)
	docker build -t termite:omni -f ./Dockerfile.termite-omni .

termite-client-test: ## Run termite-client tests
	(cd ./pkg/termite-client && $(GO) test ./...)

termite-client-lint: ## Run linter on termite-client
	(cd ./pkg/termite-client && $(GO) vet ./...)


# ====================================================================================
# Cleanup Commands
# ====================================================================================

.PHONY: cleanup-goreman

cleanup-goreman:
	rm -rf goreman.log antflydb/metadata antflydb/store qlogdir; lsof -i udp:9021 -i udp:9022 -i udp:9023 -i udp:9024 -i udp:9025 -i udp:9026 -i udp:52380 -i udp:42380 -i udp:22380 -i udp:42380 -i udp:12380 -i udp:32380 -i udp:12277 -i udp:9018 -i udp:9019 -i tcp:8080 -i udp:9017 -i udp:4211 -i tcp:8080 -i tcp:9021 -i tcp:4201 -i tcp:4202 -i tcp:4203 -i tcp:4211 -i tcp:4212 -i tcp:4213 -i tcp:4201 -i tcp:9022 -i tcp:9025 -i tcp:9023 -i tcp:9024 -i tcp:11433 -i udp:11433 | awk '{print $2 }' | sed '1d' | xargs kill -9
