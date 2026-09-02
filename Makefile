SHELL := /bin/bash
ZIG_MAKE := $(MAKE) -C ./zig
ZIG_BUILD_FLAGS ?=
SCRIPTS_PY ?= uv run --project scripts --locked python
# ====================================================================================
# Go Version Configuration
# ====================================================================================
# Use Go 1.26 with SIMD experiment enabled for hardware SIMD acceleration
GO := GOWORK=off GOEXPERIMENT=simd go
GO_MODULES := \
	./go/pkg/antflylite \
	./go/pkg/sdk \
	./go/pkg/proxy \
	./go/pkg/operator \
	./go/pkg/docsaf \
	./go/pkg/evalaf \
	./go/pkg/evalaf/plugins/antfly \
	./go/pkg/genkit/antfly \
	./go/pkg/genkit/openrouter \
	./go/pkg/memoryaf

# ====================================================================================
# General Commands
# ====================================================================================

.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build              Build the Zig antfly binary"
	@echo "  build-antfarm      Build the antfarm frontend (React admin UI)"
	@echo "  build-docs         Join OpenAPI specifications"
	@echo "  generate           Generate Zig OpenAPI modules and client SDKs"
	@echo "  lint               Run linters across retained Go modules and TypeScript"
	@echo "  tidy               Run go mod tidy across retained Go modules"
	@echo "  tidy-check         Verify retained Go modules are tidy"
	@echo "  zig-build          Build the migrated Zig runtime"
	@echo "  zig-test           Run the migrated Zig test aggregate"
	@echo "  zig-generate       Regenerate migrated Zig generated sources"
	@echo "  zig-openapi-generate  Regenerate migrated Zig OpenAPI modules"
	@echo "  zig-generated-check  Verify migrated Zig generated sources"
	@echo "  release-scripting-test  Test release packaging, installers, and promotion scripts"
	@echo "  install-git-hooks  Configure Git to use the repository hooks in .githooks/"
	@echo "  update-deps        Update Go dependencies"
	@echo "  download-omni-deps Download ONNX Runtime and PJRT archives"
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

.PHONY: build build-docs generate graph-identifier-generate graph-identifier-check lint license-headers license-check update-deps tidy tidy-check install-git-hooks build-antfarm build-antfarm-main release-scripting-test
.PHONY: zig-build zig-test zig-unit-test zig-generate zig-openapi-generate zig-generated-check zig-openapi-check zig-snowball-check zig-license-headers zig-license-check zig-tla-check

build-antfarm: build-antfarm-main

build-antfarm-main:
	@echo "Building antfarm frontend..."
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm install --frozen-lockfile
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm --filter antfarm... build
	@echo "Copying dist files to zig/pkg/antfly/antfarm..."
	rm -rf zig/pkg/antfly/antfarm/*
	cp -r ts/apps/antfarm/dist/* zig/pkg/antfly/antfarm/

build: build-antfarm
	$(ZIG_MAKE) build ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"
	cp zig/zig-out/bin/antfly ./antfly

build-docs:
	uv run --project scripts --locked python scripts/join_public_openapi.py openapi.yaml

release-scripting-test:
	scripts/test_install_download_markers.sh
	scripts/test_quickstart_docs.py
	python3 -m unittest discover -s scripts/packaging -p 'test_*.py'
	python3 -m unittest discover -s scripts/release -p 'test_*.py'
	python3 scripts/release/validate_workflow_actions.py
	sh -n scripts/install.sh
	sh -n scripts/release/install_bootstrap.sh
	bash -n scripts/test_install_download_markers.sh

generate: graph-identifier-generate build-docs tidy
	$(MAKE) zig-openapi-generate
	@for mod in $(GO_MODULES); do \
		echo "==> Generating in $$mod"; \
		(cd $$mod && $(GO) generate ./...) || exit 1; \
	done
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk generate
	$(MAKE) -C ./py/packages/sdk generate
	$(MAKE) build-antfarm

graph-identifier-generate:
	$(SCRIPTS_PY) scripts/generate_graph_identifier_policy.py

graph-identifier-check:
	cd scripts && uv run --locked python -m unittest test_generate_graph_identifier_policy
	$(SCRIPTS_PY) scripts/generate_graph_identifier_policy.py --check

license-headers: ## Add first-party license headers.
	$(SCRIPTS_PY) scripts/license_headers.py

license-check: ## Check first-party license headers.
	$(SCRIPTS_PY) scripts/license_headers.py --check

zig-build:
	$(ZIG_MAKE) build ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"

zig-test:
	$(ZIG_MAKE) test

zig-unit-test:
	$(ZIG_MAKE) unit-test ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"

zig-generate:
	$(ZIG_MAKE) generate

zig-openapi-generate:
	$(ZIG_MAKE) openapi-generate

zig-generated-check: graph-identifier-check
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
	@for mod in $(GO_MODULES); do \
		echo "==> Linting $$mod"; \
		(cd $$mod && $(GO) run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...) && \
		(cd $$mod && $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./...) && \
		(cd $$mod && $(GO) run github.com/Antonboom/testifylint@latest --fix ./...) || exit 1; \
	done
	cd ts && pnpm run lint


# ====================================================================================
# Native Runtime Dependency Downloads
# ====================================================================================
#
# Downloads ONNX Runtime and PJRT libraries for native-runtime development.
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
	@for mod in $(GO_MODULES); do \
		echo "==> Tidying $$mod"; \
		(cd $$mod && $(GO) mod tidy) || exit 1; \
	done

tidy-check:
	@for mod in $(GO_MODULES); do \
		echo "==> Checking tidy in $$mod"; \
		(cd $$mod && $(GO) mod tidy -diff) || exit 1; \
	done

install-git-hooks:
	git config core.hooksPath .githooks
	@echo "Configured Git hooks path to .githooks/"

update-deps:
	@for mod in $(GO_MODULES); do \
		echo "==> Updating deps in $$mod"; \
		(cd $$mod && $(GO) get -u ./...) || exit 1; \
	done
	$(MAKE) tidy


# ====================================================================================
# TLA+ Verification Commands
# ====================================================================================

.PHONY: tla-tools tla-check tla-check-txn tla-check-split tla-check-snap tla-trace-raft tla-trace-txn

tla-tools:
	$(ZIG_MAKE) tla-tools

tla-check:
	$(ZIG_MAKE) tla-check

tla-check-txn:
	$(ZIG_MAKE) tla-check-txn

tla-check-split:
	$(ZIG_MAKE) tla-check-split

tla-check-snap:
	$(ZIG_MAKE) tla-check-snap

tla-trace-raft: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson)
endif
	$(ZIG_MAKE) tla-trace-raft TRACE_FILES="$(TRACE_FILES)"

tla-trace-txn: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson)
endif
	$(ZIG_MAKE) tla-trace-txn TRACE_FILES="$(TRACE_FILES)"


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
	docker pull ghcr.io/antflydb/antfly:latest
	docker tag ghcr.io/antflydb/antfly:latest antfly:latest
	minikube image load --profile=minikube antfly:latest

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

.PHONY: operator-build operator-test operator-docker-build operator-lint sdk-test sdk-lint

operator-build: ## Build the antfly-operator binary
	(cd ./go/pkg/operator && $(MAKE) build)

operator-test: ## Run antfly-operator tests
	(cd ./go/pkg/operator && $(MAKE) test)

operator-lint: ## Run linter on antfly-operator
	(cd ./go/pkg/operator && $(MAKE) lint)

operator-docker-build: ## Build antfly-operator Docker image
	docker build -t antfly-operator:latest -f ./go/pkg/operator/Dockerfile .

sdk-test: ## Run SDK tests
	(cd ./go/pkg/sdk && $(GO) test ./...)

sdk-lint: ## Run SDK linter
	(cd ./go/pkg/sdk && $(GO) vet ./...)
