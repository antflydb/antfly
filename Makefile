SHELL := /bin/bash
ZIG ?= zig
ZIG_MAKE := $(MAKE) -C ./zig
ZIG_BUILD_FLAGS ?=
SCRIPTS_PY ?= uv run --project scripts --locked python

# Use Go 1.26 with the SIMD experiment for hardware SIMD acceleration.
GO := GOWORK=off GOEXPERIMENT=simd go
GO_MODULES := \
	./go/pkg/embedded \
	./go/pkg/sdk \
	./go/pkg/docsaf \
	./go/pkg/evalaf \
	./go/pkg/evalaf/plugins/antfly \
	./go/pkg/genkit/antfly \
	./go/pkg/genkit/openrouter

# Run a command in every Go module: $(call go_each,<label>,<command>)
go_each = @for mod in $(GO_MODULES); do \
		echo "==> $(1) $$mod"; \
		(cd $$mod && $(2)) || exit 1; \
	done

.PHONY: help
help: ## Show this help.
	@echo "Usage: make <target>"
	@echo ""
	@grep -hE '^[a-zA-Z0-9_%-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN { FS = ":.*## " } { printf "  %-28s %s\n", $$1, $$2 }'

# ====================================================================================
# Build and Generation
# ====================================================================================

.PHONY: build generate graph-identifier-generate graph-identifier-check gc

# Uses the committed antfarm build in zig/pkg/antfly/antfarm; `make generate`
# refreshes it, and repository-check verifies it.
build: ## Build the Zig antfly binary and copy it to ./antfly.
	$(ZIG_MAKE) build ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"
	cp zig/zig-out/bin/antfly ./antfly

generate: graph-identifier-generate ## Regenerate openapi.yaml, Zig modules, SDKs, and the antfarm build.
	$(SCRIPTS_PY) scripts/generate_memory_openapi.py --zig $(ZIG)
	$(SCRIPTS_PY) scripts/join_public_openapi.py openapi.yaml
	$(SCRIPTS_PY) scripts/sync_rust_sdk_spec.py
	$(MAKE) tidy
	$(ZIG_MAKE) source-catalog-generate
	$(ZIG_MAKE) engine-errors-generate
	$(ZIG_MAKE) openapi-generate
	$(call go_each,Generating in,$(GO) generate ./...)
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk generate
	cd ts/apps/model-explorer && node --experimental-strip-types generator/index.ts
	$(MAKE) -C ./py/packages/sdk generate
	@echo "Building antfarm frontend into zig/pkg/antfly/antfarm..."
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm install --frozen-lockfile
	cd ts && node scripts/run-pinned-toolchain.mjs pnpm --filter antfarm... build
	rm -rf zig/pkg/antfly/antfarm/*
	cp -r ts/apps/antfarm/dist/* zig/pkg/antfly/antfarm/

graph-identifier-generate:
	$(SCRIPTS_PY) scripts/generate_graph_identifier_policy.py

graph-identifier-check:
	cd scripts && uv run --locked python -m unittest test_generate_graph_identifier_policy
	$(SCRIPTS_PY) scripts/generate_graph_identifier_policy.py --check

gc: ## Garbage-collect build caches across all worktrees (DRY_RUN=1 to preview).
	scripts/cache_gc.sh

# ====================================================================================
# Zig and TLA+ (delegated to zig/Makefile)
# ====================================================================================
#
# `make zig-<target>` runs `make -C zig <target>` (e.g. zig-test, zig-unit-test,
# zig-generate, zig-snowball-check) and `make tla-<target>` runs the matching
# TLA+ target (tla-check, tla-check-txn, tla-trace-raft TRACE_FILES=...). The
# explicit zig-* targets below add repository-level steps.

.PHONY: zig-checksums-check zig-generated-check zig-openapi-check

zig-%: ## Run a zig/Makefile target (zig-test, zig-unit-test, zig-generate, ...).
	$(ZIG_MAKE) $* ZIG_BUILD_FLAGS="$(ZIG_BUILD_FLAGS)"

tla-%: ## Run a TLA+ target (tla-check, tla-check-txn, tla-trace-raft TRACE_FILES=...).
	$(ZIG_MAKE) $@

zig-checksums-check:
	python3 -m unittest discover -s scripts/ci -p test_check_zig_checksum_usage.py
	python3 scripts/ci/check_zig_checksum_usage.py

zig-generated-check: graph-identifier-check
	$(SCRIPTS_PY) -m unittest scripts/test_openapi_build_paths.py scripts/test_graph_query_error_contract.py
	$(ZIG_MAKE) generated-check

zig-openapi-check:
	$(SCRIPTS_PY) -m unittest scripts/test_openapi_build_paths.py scripts/test_graph_query_error_contract.py
	$(ZIG_MAKE) openapi-check

# ====================================================================================
# Formatting, Linting, and Checks
# ====================================================================================

.PHONY: fmt fmt-check lint repository-check license-headers license-check apache-license-check release-scripting-test

fmt: ## Format Zig, Go, Python, TypeScript, and Rust sources.
	scripts/format.sh

fmt-check: ## Check formatting without changing files.
	scripts/format.sh --check

lint: ## Run linters across Go modules and TypeScript.
	$(call go_each,Linting,\
		$(GO) run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./... && \
		$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix ./... && \
		$(GO) run github.com/Antonboom/testifylint@latest --fix ./...)
	cd ts && pnpm run lint

repository-check: ## Run formatting, SDK, and release-tooling checks.
	scripts/ci/check.sh all

license-headers: ## Add first-party license headers.
	$(SCRIPTS_PY) scripts/license_headers.py

license-check: apache-license-check ## Check license headers and the Apache engine boundary.
	$(SCRIPTS_PY) scripts/license_headers.py --check

apache-license-check: ## Check Apache engine headers and the server dependency boundary.
	$(SCRIPTS_PY) scripts/check_apache_boundary.py

release-scripting-test: ## Test release packaging, installers, and promotion scripts.
	scripts/release/test.sh

# ====================================================================================
# Go Dependencies
# ====================================================================================

.PHONY: tidy tidy-check update-deps install-git-hooks

tidy: ## Run go mod tidy across Go modules.
	$(call go_each,Tidying,$(GO) mod tidy)

tidy-check: ## Verify Go modules are tidy.
	$(call go_each,Checking tidy in,$(GO) mod tidy -diff)

update-deps: ## Update Go dependencies across modules.
	$(call go_each,Updating deps in,$(GO) get -u ./...)
	$(MAKE) tidy

install-git-hooks: ## Use the repository hooks in .githooks/.
	git config core.hooksPath .githooks
	@echo "Configured Git hooks path to .githooks/"

# ====================================================================================
# Minikube
# ====================================================================================

.PHONY: minikube-start minikube-delete minikube-deploy minikube-status minikube-restart

minikube-start: ## Start Minikube and deploy devops/minikube.
	minikube start --driver=vfkit --container-runtime containerd --cpus 3 --memory "7G" --disk-size "20G" --profile=minikube
	minikube addons enable metrics-server --profile=minikube
	$(MAKE) minikube-deploy

minikube-delete: ## Delete the Minikube cluster.
	minikube delete --profile=minikube

minikube-deploy: ## Load the published antfly image and apply devops/minikube.
	docker pull ghcr.io/antflydb/antfly:latest
	docker tag ghcr.io/antflydb/antfly:latest antfly:latest
	minikube image load --profile=minikube antfly:latest
	kubectl --context=minikube apply -R -f ./devops/minikube/
	@echo "Public API: $$(minikube service public-api-svc --url --profile=minikube 2>/dev/null || echo 'run: minikube service public-api-svc --profile=minikube')"

minikube-status: ## Show Minikube pods, services, and StatefulSets.
	kubectl --context=minikube get pods,services,statefulsets -l app=antfly

minikube-restart: minikube-delete minikube-start ## Delete and recreate Minikube.

# ====================================================================================
# SQL Extraction Audits (intentionally outside default build/test gates)
# ====================================================================================

.PHONY: sql-parity-inventory-check sql-parity-evidence-check sql-parity-release-check sql-catalog-lifecycle-oracle-check sql-default-lifecycle-oracle-check sql-table-creation-oracle-check
.PHONY: sql-expression-index-oracle-check

sql-parity-inventory-check: ## Audit SQL parity inventory.
	python3 scripts/check_sql_parity_inventory.py

sql-parity-evidence-check: ## Audit SQL parity evidence.
	python3 scripts/check_sql_parity_inventory.py --evidence

sql-parity-release-check: ## Audit SQL parity release readiness.
	python3 scripts/check_sql_parity_inventory.py --release

sql-catalog-lifecycle-oracle-check: ## Check PostgreSQL catalog lifecycle contracts.
	uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_sql_catalog_lifecycle.py

sql-default-lifecycle-oracle-check: ## Check exact PostgreSQL default DDL and future mutation contracts.
	uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_sql_default_lifecycle.py

sql-table-creation-oracle-check: ## Check PostgreSQL generated/default CREATE and complete mutation postimages.
	uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_sql_table_creation_lifecycle.py

sql-expression-index-oracle-check: ## Check PostgreSQL expression/partial index volatility and mutation postimages.
	uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_sql_expression_index_lifecycle.py
