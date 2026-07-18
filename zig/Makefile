SHELL := /bin/bash

ZIG ?= zig
ZIG_BUILD_FLAGS ?=
UV ?= uv
GIT ?= git
SCRIPTS_PY := $(UV) run --project ../scripts --locked python
INFERENCE_BIN ?= ./zig-out/bin/antfly
OPENAPI_CODEGEN_SOURCES = build.zig build.zig.zon $(wildcard lib/openapi/src/*.zig)
OPENAPI_CODEGEN_CACHE_KEY = $(strip $(shell $(GIT) hash-object $(OPENAPI_CODEGEN_SOURCES) 2>/dev/null | $(GIT) hash-object --stdin 2>/dev/null))
OPENAPI_CODEGEN_CACHE_DIR ?= .zig-cache/openapi-generate-$(OPENAPI_CODEGEN_CACHE_KEY)
OPENAPI_ZIG_BUILD = $(if $(OPENAPI_CODEGEN_CACHE_KEY),$(ZIG) build --cache-dir $(OPENAPI_CODEGEN_CACHE_DIR),$(error failed to compute OPENAPI_CODEGEN_CACHE_KEY; ensure git is available))

# ====================================================================================
# Build
# ====================================================================================

INFERENCE_DIR := pkg/inference
ONNXRUNTIME_ROOT := $(INFERENCE_DIR)/onnxruntime
ONNXRUNTIME_VERSION ?= 1.24.3
GENAI_VERSION ?= 0.12.1
ONNXRUNTIME_STAMP := $(ONNXRUNTIME_ROOT)/.version-$(ONNXRUNTIME_VERSION)-$(GENAI_VERSION)

.PHONY: build test unit-test download-onnx force-download-onnx clean-onnx

$(ONNXRUNTIME_STAMP): $(INFERENCE_DIR)/scripts/download-onnxruntime.sh
	@test -d $(INFERENCE_DIR) || (echo "ERROR: inference package not found at $(INFERENCE_DIR)" && exit 1)
	@rm -f $(ONNXRUNTIME_ROOT)/.version-*
	cd $(INFERENCE_DIR) && ONNXRUNTIME_ROOT=onnxruntime ./scripts/download-onnxruntime.sh $(ONNXRUNTIME_VERSION) $(GENAI_VERSION)
	@touch $@

download-onnx: $(ONNXRUNTIME_STAMP) ## Download ONNX Runtime + GenAI (skips if up-to-date).

force-download-onnx: ## Force re-download of ONNX Runtime + GenAI.
	@rm -f $(ONNXRUNTIME_ROOT)/.version-*
	$(MAKE) download-onnx

build: download-onnx ## Build antfly with embedded inference + ONNX inference.
	$(ZIG) build install $(ZIG_BUILD_FLAGS)

test: ## Run the default Zig test aggregate.
	$(ZIG) build test

unit-test: ## Run hermetic Antfly unit tests.
	$(ZIG) build unit-test $(ZIG_BUILD_FLAGS)

clean-onnx: ## Remove downloaded ONNX Runtime files.
	rm -rf $(ONNXRUNTIME_ROOT)

.PHONY: inference-e2e-models

inference-e2e-models: ## Prefetch the curated inference E2E model set with `antfly inference pull`.
	cd pkg/inference && zig build
	ANTFLY_BIN=$(INFERENCE_BIN) $(if $(ANTFLY_INFERENCE_MODELS_DIR),ANTFLY_INFERENCE_MODELS_DIR=$(ANTFLY_INFERENCE_MODELS_DIR),) $(UV) run --project e2e/inference python e2e/inference/pull_models.py

# ====================================================================================
# TLA+ Verification Commands
# ====================================================================================

.PHONY: generate generated-check openapi-generate openapi-check snowball-generate snowball-check cabi-header-generate cabi-header-check license-headers license-check tla-tools tla-check tla-check-txn tla-check-split tla-check-snap tla-check-lsm tla-trace-raft tla-trace-txn

generate: openapi-generate snowball-generate cabi-header-generate ## Regenerate checked-in generated sources.

generated-check: openapi-check snowball-check cabi-header-check ## Check all checked-in generated sources are current.

openapi-generate:
	$(SCRIPTS_PY) ../scripts/join_public_openapi.py openapi.yaml
	$(OPENAPI_ZIG_BUILD) regen-openapi

openapi-check:
	$(SCRIPTS_PY) ../scripts/join_public_openapi.py --compare openapi.yaml
	$(OPENAPI_ZIG_BUILD) regen-openapi
	test -z "$$(git status --porcelain -- ../openapi.yaml pkg/antfly/src/openapi/generated pkg/inference/src/api/generated)"

snowball-generate:
	$(ZIG) build regen-snowball

snowball-check:
	$(ZIG) build check-snowball

cabi-header-generate:
	$(SCRIPTS_PY) ../scripts/packaging/sync_cabi_header.py

cabi-header-check:
	$(SCRIPTS_PY) ../scripts/packaging/sync_cabi_header.py --check

license-headers: ## Add license headers to first-party source files.
	$(SCRIPTS_PY) ../scripts/license_headers.py

license-check: ## Check first-party source files have license headers.
	$(SCRIPTS_PY) ../scripts/license_headers.py --check

tla-tools:
	@bash ../scripts/tla-tools.sh

tla-check: tla-check-txn tla-check-split tla-check-snap tla-check-lsm

tla-check-txn: tla-tools
	@echo "==> Model checking transaction spec..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflyTransaction.cfg specs/tla/MC.tla \
	  -workers auto -deadlock

tla-check-split: tla-tools
	@echo "==> Model checking shard split spec..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflyShardSplit.cfg specs/tla/ShardSplitMC.tla \
	  -workers auto -deadlock

tla-check-snap: tla-tools
	@echo "==> Model checking snapshot transfer spec (safety only, ~90s)..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflySnapshotTransfer-safety.cfg specs/tla/SnapshotTransferMC.tla \
	  -workers auto -deadlock

tla-check-lsm: tla-tools
	@echo "==> Model checking LSM lifecycle OOM safety..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/AntflyLsmLifecycle.cfg specs/tla/AntflyLsmLifecycle.tla \
	  -workers auto -deadlock

tla-trace-raft: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson)
endif
	@set -e; \
	segdir=$$(mktemp -d); \
	trap 'rm -rf "$$segdir"' EXIT; \
	for f in $(TRACE_FILES); do \
	  python3 ../scripts/tla-segment-raft-trace.py "$$f" "$$segdir"; \
	done; \
	segments=$$(find "$$segdir" -name '*.ndjson' -size +0c | sort); \
	if [ -n "$$segments" ]; then \
	  bash ../scripts/tla-validate-trace.sh -S \
	    -s specs/tla/Traceetcdraft.tla \
	    -c specs/tla/Traceetcdraft.cfg \
	    $$segments; \
	else \
	  echo "No non-empty trace segments found"; \
	  exit 1; \
	fi

tla-trace-txn: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson)
endif
	@set -e; \
	validated=0; \
	tmpfiles=""; tmpdirs=""; \
	trap 'rm -f $$tmpfiles; rm -rf $$tmpdirs' EXIT; \
	for f in $(TRACE_FILES); do \
	  filtered=$$(mktemp); \
	  tmpfiles="$$tmpfiles $$filtered"; \
	  python3 ../scripts/tla-filter-txn-trace.py < "$$f" > "$$filtered"; \
	  if [ -s "$$filtered" ]; then \
	    segdir=$$(mktemp -d); \
	    tmpdirs="$$tmpdirs $$segdir"; \
	    python3 ../scripts/tla-segment-txn-trace.py "$$filtered" "$$segdir"; \
	    segments=$$(find "$$segdir" -name '*.ndjson' -size +0c | sort); \
	    if [ -n "$$segments" ]; then \
	      bash ../scripts/tla-validate-trace.sh -S \
	        -s specs/tla/TraceAntflyTransaction.tla \
	        -c specs/tla/TraceAntflyTransaction.cfg \
	        $$segments; \
	      validated=$$((validated + 1)); \
	    else \
	      echo "SKIP $$f (no non-empty transaction trace segments)"; \
	    fi; \
	  else \
	    echo "SKIP $$f (no spec-compatible transactions after filtering)"; \
	  fi; \
	  rm -f "$$filtered"; \
	done; \
	if [ "$$validated" -eq 0 ]; then \
	  echo "No spec-compatible transactions found after filtering"; \
	  exit 1; \
	fi
