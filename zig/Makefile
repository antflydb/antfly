SHELL := /bin/bash

ZIG ?= zig
UV ?= uv
SCRIPTS_PY := $(UV) run --project scripts --locked python
TERMITE_BIN ?= ./pkg/termite/zig-out/bin/termite

# ====================================================================================
# Build
# ====================================================================================

TERMITE_DIR := pkg/termite
ONNXRUNTIME_ROOT := $(TERMITE_DIR)/onnxruntime
ONNXRUNTIME_VERSION ?= 1.24.3
GENAI_VERSION ?= 0.12.1
ONNXRUNTIME_STAMP := $(ONNXRUNTIME_ROOT)/.version-$(ONNXRUNTIME_VERSION)-$(GENAI_VERSION)

.PHONY: build test unit-test download-onnx force-download-onnx clean-onnx

$(ONNXRUNTIME_STAMP): $(TERMITE_DIR)/scripts/download-onnxruntime.sh
	@test -d $(TERMITE_DIR) || (echo "ERROR: termite package not found at $(TERMITE_DIR)" && exit 1)
	@rm -f $(ONNXRUNTIME_ROOT)/.version-*
	cd $(TERMITE_DIR) && ONNXRUNTIME_ROOT=onnxruntime ./scripts/download-onnxruntime.sh $(ONNXRUNTIME_VERSION) $(GENAI_VERSION)
	@touch $@

download-onnx: $(ONNXRUNTIME_STAMP) ## Download ONNX Runtime + GenAI (skips if up-to-date).

force-download-onnx: ## Force re-download of ONNX Runtime + GenAI.
	@rm -f $(ONNXRUNTIME_ROOT)/.version-*
	$(MAKE) download-onnx

build: download-onnx ## Build antfly with embedded Termite + ONNX inference.
	$(ZIG) build install-antfly

test: ## Run the default Zig test aggregate.
	$(ZIG) build test

unit-test: ## Run hermetic Antfly unit tests.
	$(ZIG) build unit-test

clean-onnx: ## Remove downloaded ONNX Runtime files.
	rm -rf $(ONNXRUNTIME_ROOT)

.PHONY: termite-e2e-models

termite-e2e-models: ## Prefetch the curated termite E2E model set with `termite pull`.
	cd pkg/termite && zig build
	TERMITE_PULL_BIN=$(TERMITE_BIN) $(if $(TERMITE_MODELS_DIR),TERMITE_MODELS_DIR=$(TERMITE_MODELS_DIR),) $(UV) run --project e2e/termite python e2e/termite/pull_models.py

# ====================================================================================
# TLA+ Verification Commands
# ====================================================================================

.PHONY: generate generated-check openapi-generate openapi-check snowball-generate snowball-check license-headers license-check tla-tools tla-check tla-check-txn tla-check-split tla-check-snap tla-trace-raft tla-trace-txn

generate: openapi-generate snowball-generate ## Regenerate checked-in generated sources.

generated-check: openapi-check snowball-check ## Check all checked-in generated sources are current.

openapi-generate:
	$(SCRIPTS_PY) scripts/join_openapi.py openapi.yaml
	$(ZIG) build regen-openapi

openapi-check:
	$(SCRIPTS_PY) scripts/join_openapi.py --compare openapi.yaml
	$(ZIG) build regen-openapi
	test -z "$$(git status --porcelain -- openapi.yaml pkg/antfly/src/openapi/generated pkg/termite/src/api/generated)"

snowball-generate:
	$(ZIG) build regen-snowball

snowball-check:
	$(ZIG) build check-snowball

license-headers: ## Add license headers to first-party source files.
	$(SCRIPTS_PY) scripts/license_headers.py

license-check: ## Check first-party source files have license headers.
	$(SCRIPTS_PY) scripts/license_headers.py --check

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
	@set -e; \
	segdir=$$(mktemp -d); \
	trap 'rm -rf "$$segdir"' EXIT; \
	for f in $(TRACE_FILES); do \
	  python3 scripts/tla-segment-raft-trace.py "$$f" "$$segdir"; \
	done; \
	segments=$$(find "$$segdir" -name '*.ndjson' -size +0c | sort); \
	if [ -n "$$segments" ]; then \
	  bash scripts/tla-validate-trace.sh -S \
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
	tmpfiles=""; \
	trap 'rm -f $$tmpfiles' EXIT; \
	for f in $(TRACE_FILES); do \
	  filtered=$$(mktemp); \
	  tmpfiles="$$tmpfiles $$filtered"; \
	  python3 scripts/tla-filter-txn-trace.py < "$$f" > "$$filtered"; \
	  if [ -s "$$filtered" ]; then \
	    bash scripts/tla-validate-trace.sh -S \
	      -s specs/tla/TraceAntflyTransaction.tla \
	      -c specs/tla/TraceAntflyTransaction.cfg \
	      "$$filtered"; \
	    validated=$$((validated + 1)); \
	  else \
	    echo "SKIP $$f (no spec-compatible transactions after filtering)"; \
	  fi; \
	  rm -f "$$filtered"; \
	done; \
	if [ "$$validated" -eq 0 ]; then \
	  echo "No spec-compatible transactions found after filtering"; \
	  exit 1; \
	fi
