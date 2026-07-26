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

.PHONY: generate generated-check openapi-generate openapi-check snowball-generate snowball-check cabi-header-generate cabi-header-check license-headers license-check tla-tools tla-check tla-trace tla-clean tla-viz tla-viz-check tla-viz-trace tla-viz-traces

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

# ---- TLA+ model checking -----------------------------------------------------
# Checks are discovered from specs/tla/**/<Model>.cfgs section files; see
# specs/tla/README.md (Layout) and scripts/tla-check.sh. Tiers (core, fast,
# heavy, manual) are annotated on positive sections; Bad* sections are the
# expected-failure mutants and need no wiring here.

# ---- TLA+ model checking -----------------------------------------------------
# All logic lives in scripts/tla-check.sh; checks are discovered from
# specs/tla/**/<Model>.cfgs section files. See specs/tla/README.md.

tla-tools:
	@bash ../scripts/tla-tools.sh

tla-check: tla-tools ## Full gate (audit+parse+core+fast+mutants). TIER=heavy|manual|core|fast for one tier; CHECK=<id> for one check.
ifdef CHECK
	@bash ../scripts/tla-check.sh run $(CHECK)
else ifdef TIER
	@bash ../scripts/tla-check.sh tier $(TIER)
else
	@bash ../scripts/tla-check.sh gate
endif

tla-trace: tla-tools ## Validate NDJSON traces: TRACE=<raft|txn|txn-session|ha|split-bridge|doc-identity-range-repair> TRACE_FILES=...
ifndef TRACE
	$(error TRACE is required, e.g. make tla-trace TRACE=ha TRACE_FILES="specs/tla/traces/ha_*.ndjson")
endif
	@TRACE_FILES="$(TRACE_FILES)" bash ../scripts/tla-check.sh trace $(TRACE)

tla-clean: ## Remove TLA+ model checker runtime artifacts.
	rm -rf specs/tla/states specs/tla/.generated states
	find specs/tla \( -name '*_TTrace_*.tla' -o -name '*_TTrace_*.bin' \) -delete
	find specs/tla -type d -name states -prune -exec rm -rf {} +

# ---- TLA+ visualizations -------------------------------------------------------
# Pedagogical views of the specs; see specs/tla/README.md (Visualizations).

tla-viz: ## Regenerate structural Mermaid diagrams (specs/tla/diagrams/).
	$(SCRIPTS_PY) ../scripts/tla-viz/gen_structural.py --specs-dir specs/tla --out-dir specs/tla/diagrams

tla-viz-check: ## Check the committed structural diagrams are up to date.
	$(SCRIPTS_PY) ../scripts/tla-viz/gen_structural.py --check --specs-dir specs/tla --out-dir specs/tla/diagrams

tla-viz-trace: ## Render NDJSON trace(s) to a self-contained HTML timeline: JSON="<files/dirs>" [OUT=<file.html>]
ifndef JSON
	$(error JSON is required, e.g. make tla-viz-trace JSON=specs/tla/traces/ha_rejoin.ndjson)
endif
	$(SCRIPTS_PY) ../scripts/tla-viz/render_trace.py $(JSON) $(if $(OUT),-o $(OUT),)

tla-viz-traces: ## Render every checked-in trace fixture into one browsable HTML file: [OUT=<file.html>]
	$(SCRIPTS_PY) ../scripts/tla-viz/render_trace.py specs/tla/example.ndjson specs/tla/traces \
	  -o $(if $(OUT),$(OUT),specs/tla/traces/all-traces.html)