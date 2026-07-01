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

.PHONY: generate generated-check openapi-generate openapi-check snowball-generate snowball-check cabi-header-generate cabi-header-check license-headers license-check tla-tools tla-list tla-clean tla-check tla-check-smoke tla-check-fast tla-check-heavy tla-check-trace tla-check-txn tla-check-split tla-check-snap tla-check-lsm tla-check-new tla-check-new-fast tla-check-new-heavy tla-check-negative tla-check-txn-negative-skip-intent-conflict tla-check-split-negative-premature-child-default tla-check-snap-negative-apply-without-put tla-check-lsm-negative-index-temp-leak tla-check-ha-replication-negative-stale-timeline-ack tla-check-ha-gates-negative-standby-runtime tla-check-ha-sync-wait-negative-move-target tla-check-ha-sync-wait-negative-wrong-timeline-ack tla-check-ha-sync-wait-negative-below-target-ack tla-check-ha-timeline-switch-negative-before-applied tla-check-ha-timeline-switch-negative-non-monotonic tla-check-ha-timeline-switch-negative-old-timeline tla-check-ha-timeline-switch-negative-recovery-previous tla-check-ha-standby-apply-negative-failure-advances tla-check-ha-standby-apply-negative-duplicate-effect tla-check-ha-standby-apply-negative-crash-loses-receive tla-check-ha-standby-apply-negative-client-write tla-check-ha-standby-apply-negative-background-runtime tla-check-ha-rejoin-negative-unfenced-rewind tla-check-ha-rejoin-negative-expired-wal-rewind tla-check-ha-rejoin-negative-forced-rewind tla-check-ha-rejoin-negative-identity-mismatch-rewind tla-check-ha-rejoin-negative-stale-assessment tla-check-ha-rejoin-negative-fork-mismatch tla-check-derived-replay-negative tla-check-enrichment-lease-negative-stale-publish tla-check-enrichment-lease-negative-empty-pending tla-check-lsm-wal-compaction-negative-checkpoint tla-check-lsm-wal-compaction-negative-corrupt-rotate tla-check-lsm-wal-compaction-negative-pinned-retire tla-check-lmdb-commit-negative-meta-before-data tla-check-lmdb-commit-negative-reader-reuse tla-check-transaction-session-negative-rollback tla-check-transaction-session-negative-recovery-decision tla-check-transaction-session-negative-cleanup tla-check-transaction-session-trace-negative-cleanup tla-check-document-identity-negative-reuse-ordinal tla-check-document-identity-negative-stale-filter tla-check-document-identity-negative-namespace-mismatch tla-check-document-identity-range-repair-negative-split-unhealthy tla-check-document-identity-range-repair-negative-split-dest-namespace tla-check-document-identity-range-repair-negative-merge-mismatch tla-check-document-identity-range-repair-negative-merge-active-reassign tla-check-document-identity-range-repair-negative-restore-namespace tla-check-document-identity-range-repair-negative-restore-early-clear tla-check-db-split-visibility-negative-parent-write tla-check-db-split-visibility-negative-child-serve tla-check-db-split-visibility-negative-merge-donor tla-check-db-split-visibility-negative-enrichment-owner tla-check-split-refinement-bridge-negative-route-before-db-serving tla-check-split-refinement-bridge-negative-db-serve-before-shard-cutover tla-check-split-refinement-bridge-negative-stale-fence-cutover tla-check-split-refinement-bridge-trace-negative-route-before-db-serving tla-check-managed-host-lifecycle-negative-premature-restore tla-check-managed-host-lifecycle-negative-stale-route tla-check-managed-host-lifecycle-negative-revive-removed tla-check-managed-host-lifecycle-negative-restore-cancel tla-check-lite-publication-negative-manifest-before-artifacts tla-check-lite-publication-negative-failed-head tla-check-lite-publication-negative-pinned-cleanup tla-check-lite-publication-negative-mixed-generation tla-check-ml-graph-passes-negative-dangling-cse tla-check-ml-graph-passes-negative-parameter-dedup tla-check-ml-graph-passes-negative-missing-lower-closure tla-check-ml-graph-passes-negative-fallback-runtime tla-check-ml-graph-passes-negative-partial-publish tla-check-ml-graph-dag-passes-negative-cse-miss-duplicate tla-check-ml-graph-dag-passes-negative-cse-no-consumer-remap tla-check-ml-graph-dag-passes-negative-dce-drop-reachable tla-check-ml-graph-dag-passes-negative-dce-non-topo-map tla-check-ml-compiler-publication-negative-stale-compile tla-check-ml-compiler-publication-negative-missing-input tla-check-ml-compiler-publication-negative-output-selection tla-check-ml-compiler-publication-negative-fallback-publish tla-check-ml-compiler-publication-negative-partial-artifact tla-check-openapi-codegen-negative-stale-package tla-check-openapi-codegen-negative-stale-root tla-check-openapi-codegen-negative-internal-leak tla-check-openapi-codegen-negative-partial-commit tla-check-ha-gates tla-check-ha-sync-wait tla-check-ha-timeline-switch tla-check-ha-standby-apply tla-check-ha-rejoin tla-check-ha-replication tla-check-derived-replay tla-check-derived-replay-heavy tla-check-derived-replay-heavy-depth tla-check-derived-replay-heavy-multi-index tla-check-derived-replay-heavy-full tla-check-enrichment-lease tla-check-lmdb-commit tla-check-lsm-wal-compaction tla-check-db-split-visibility tla-check-split-refinement-bridge tla-check-document-identity tla-check-document-identity-range-repair tla-check-transaction-session tla-check-managed-host-lifecycle tla-check-lite-publication tla-check-ml-graph-passes tla-check-ml-graph-dag-passes tla-check-ml-compiler-publication tla-check-openapi-codegen tla-trace-raft tla-trace-txn tla-trace-txn-session tla-trace-ha tla-trace-split-bridge
.PHONY: tla-check-critical tla-check-ha-failover-safety tla-check-ha-failover-negative-promote-missing-ack tla-check-ha-failover-negative-old-primary-write tla-check-document-identity-range-repair-trace-negative-restore-mismatch tla-check-document-identity-range-repair-trace-negative-restore-early-clear tla-trace-doc-identity-range-repair
.PHONY: tla-check-batcher-coalescing tla-check-batcher-coalescing-negative-delete-write-inversion tla-check-batcher-coalescing-negative-write-delete-inversion tla-check-batcher-coalescing-negative-partial-visibility tla-check-cdc-cutover tla-check-cdc-cutover-negative-boundary-duplicate tla-check-cdc-cutover-negative-checkpoint-ahead tla-check-cdc-cutover-negative-resume-replay tla-check-ha-gate-transitions tla-check-ha-gate-transitions-negative-stale-allow tla-check-ha-partition-fence tla-check-ha-partition-fence-negative-promote-before-fence tla-check-shard-split-seq tla-check-shard-split-seq-negative-keyset-cutover tla-check-snapshot-content tla-check-snapshot-content-negative-wrong-content tla-check-snapshot-content-negative-gc-needed-content tla-check-lsm-reserve-cleanup tla-check-lsm-reserve-cleanup-negative-publish-without-reserve tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp tla-check-query-completeness tla-check-query-completeness-negative-route-before-child-ready tla-check-query-completeness-negative-double-serve tla-check-query-completeness-negative-missing-doc

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

tla-list: ## List available TLA+ verification targets.
	@printf '%s\n' \
	  'TLA+ model checks:' \
	  '  make tla-check-smoke              # parse all checked-in TLA+ specs' \
	  '  make tla-check-fast               # alias for fast focused lower-level specs' \
	  '  make tla-check-heavy              # alias for heavy focused lower-level specs' \
	  '  make tla-check                    # transaction, split, snapshot safety, LSM lifecycle' \
	  '  make tla-check-new-fast           # fast focused lower-level specs' \
	  '  make tla-check-new-heavy          # heavy focused lower-level specs' \
	  '  make tla-check-new                # all focused lower-level specs' \
	  '  make tla-check-critical           # critique-repair critical safety subset' \
	  '  make tla-check-negative           # expected-failure mutant checks' \
	  '  make tla-check-txn-negative-skip-intent-conflict' \
	  '  make tla-check-split-negative-premature-child-default' \
	  '  make tla-check-snap-negative-apply-without-put' \
	  '  make tla-check-lsm-negative-index-temp-leak' \
	  '  make tla-check-ha-replication-negative-stale-timeline-ack' \
	  '  make tla-check-ha-gates-negative-standby-runtime' \
	  '  make tla-check-ha-sync-wait-negative-move-target' \
	  '  make tla-check-ha-sync-wait-negative-wrong-timeline-ack' \
	  '  make tla-check-ha-sync-wait-negative-below-target-ack' \
	  '  make tla-check-ha-timeline-switch-negative-before-applied' \
	  '  make tla-check-ha-timeline-switch-negative-non-monotonic' \
	  '  make tla-check-ha-timeline-switch-negative-old-timeline' \
	  '  make tla-check-ha-timeline-switch-negative-recovery-previous' \
	  '  make tla-check-ha-standby-apply-negative-failure-advances' \
	  '  make tla-check-ha-standby-apply-negative-duplicate-effect' \
	  '  make tla-check-ha-standby-apply-negative-crash-loses-receive' \
	  '  make tla-check-ha-standby-apply-negative-client-write' \
	  '  make tla-check-ha-standby-apply-negative-background-runtime' \
	  '  make tla-check-ha-rejoin-negative-unfenced-rewind' \
	  '  make tla-check-ha-rejoin-negative-expired-wal-rewind' \
	  '  make tla-check-ha-rejoin-negative-forced-rewind' \
	  '  make tla-check-ha-rejoin-negative-identity-mismatch-rewind' \
	  '  make tla-check-ha-rejoin-negative-stale-assessment' \
	  '  make tla-check-ha-rejoin-negative-fork-mismatch' \
	  '  make tla-check-ha-failover-negative-promote-missing-ack' \
	  '  make tla-check-ha-failover-negative-old-primary-write' \
	  '  make tla-check-ha-partition-fence-negative-promote-before-fence' \
	  '  make tla-check-ha-gate-transitions-negative-stale-allow' \
	  '  make tla-check-batcher-coalescing-negative-delete-write-inversion' \
	  '  make tla-check-batcher-coalescing-negative-write-delete-inversion' \
	  '  make tla-check-batcher-coalescing-negative-partial-visibility' \
	  '  make tla-check-cdc-cutover-negative-boundary-duplicate' \
	  '  make tla-check-cdc-cutover-negative-checkpoint-ahead' \
	  '  make tla-check-cdc-cutover-negative-resume-replay' \
	  '  make tla-check-shard-split-seq-negative-keyset-cutover' \
	  '  make tla-check-snapshot-content-negative-wrong-content' \
	  '  make tla-check-snapshot-content-negative-gc-needed-content' \
	  '  make tla-check-lsm-reserve-cleanup-negative-publish-without-reserve' \
	  '  make tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp' \
	  '  make tla-check-query-completeness-negative-route-before-child-ready' \
	  '  make tla-check-query-completeness-negative-double-serve' \
	  '  make tla-check-query-completeness-negative-missing-doc' \
	  '  make tla-check-enrichment-lease-negative-stale-publish' \
	  '  make tla-check-enrichment-lease-negative-empty-pending' \
	  '  make tla-check-lsm-wal-compaction-negative-checkpoint' \
	  '  make tla-check-lsm-wal-compaction-negative-corrupt-rotate' \
	  '  make tla-check-lsm-wal-compaction-negative-pinned-retire' \
	  '  make tla-check-lmdb-commit-negative-meta-before-data' \
	  '  make tla-check-lmdb-commit-negative-reader-reuse' \
	  '  make tla-check-transaction-session-negative-rollback' \
	  '  make tla-check-transaction-session-negative-recovery-decision' \
	  '  make tla-check-transaction-session-negative-cleanup' \
	  '  make tla-check-transaction-session-trace-negative-cleanup' \
	  '  make tla-check-document-identity-negative-reuse-ordinal' \
	  '  make tla-check-document-identity-negative-stale-filter' \
	  '  make tla-check-document-identity-negative-namespace-mismatch' \
	  '  make tla-check-document-identity-range-repair-negative-split-unhealthy' \
	  '  make tla-check-document-identity-range-repair-negative-split-dest-namespace' \
	  '  make tla-check-document-identity-range-repair-negative-merge-mismatch' \
	  '  make tla-check-document-identity-range-repair-negative-merge-active-reassign' \
	  '  make tla-check-document-identity-range-repair-negative-restore-namespace' \
	  '  make tla-check-document-identity-range-repair-negative-restore-early-clear' \
	  '  make tla-check-document-identity-range-repair-trace-negative-restore-mismatch' \
	  '  make tla-check-document-identity-range-repair-trace-negative-restore-early-clear' \
	  '  make tla-check-db-split-visibility-negative-parent-write' \
	  '  make tla-check-db-split-visibility-negative-child-serve' \
	  '  make tla-check-db-split-visibility-negative-merge-donor' \
	  '  make tla-check-db-split-visibility-negative-enrichment-owner' \
	  '  make tla-check-split-refinement-bridge-negative-route-before-db-serving' \
	  '  make tla-check-split-refinement-bridge-negative-db-serve-before-shard-cutover' \
	  '  make tla-check-split-refinement-bridge-negative-stale-fence-cutover' \
	  '  make tla-check-split-refinement-bridge-trace-negative-route-before-db-serving' \
	  '  make tla-check-managed-host-lifecycle-negative-premature-restore' \
	  '  make tla-check-managed-host-lifecycle-negative-stale-route' \
	  '  make tla-check-managed-host-lifecycle-negative-revive-removed' \
	  '  make tla-check-managed-host-lifecycle-negative-restore-cancel' \
	  '  make tla-check-lite-publication-negative-manifest-before-artifacts' \
	  '  make tla-check-lite-publication-negative-failed-head' \
	  '  make tla-check-lite-publication-negative-pinned-cleanup' \
	  '  make tla-check-lite-publication-negative-mixed-generation' \
	  '  make tla-check-ml-graph-passes-negative-dangling-cse' \
	  '  make tla-check-ml-graph-passes-negative-parameter-dedup' \
	  '  make tla-check-ml-graph-passes-negative-missing-lower-closure' \
	  '  make tla-check-ml-graph-passes-negative-fallback-runtime' \
	  '  make tla-check-ml-graph-passes-negative-partial-publish' \
	  '  make tla-check-ml-graph-dag-passes-negative-cse-miss-duplicate' \
	  '  make tla-check-ml-graph-dag-passes-negative-cse-no-consumer-remap' \
	  '  make tla-check-ml-graph-dag-passes-negative-dce-drop-reachable' \
	  '  make tla-check-ml-graph-dag-passes-negative-dce-non-topo-map' \
	  '  make tla-check-ml-compiler-publication-negative-stale-compile' \
	  '  make tla-check-ml-compiler-publication-negative-missing-input' \
	  '  make tla-check-ml-compiler-publication-negative-output-selection' \
	  '  make tla-check-ml-compiler-publication-negative-fallback-publish' \
	  '  make tla-check-ml-compiler-publication-negative-partial-artifact' \
	  '  make tla-check-openapi-codegen-negative-stale-package' \
	  '  make tla-check-openapi-codegen-negative-stale-root' \
	  '  make tla-check-openapi-codegen-negative-internal-leak' \
	  '  make tla-check-openapi-codegen-negative-partial-commit' \
	  '  make tla-check-txn                # distributed transaction model' \
	  '  make tla-check-split              # shard split model' \
	  '  make tla-check-snap               # snapshot transfer safety model' \
	  '  make tla-check-lsm                # LSM lifecycle OOM safety model' \
	  '  make tla-check-ha-gates           # HA gate decision model' \
	  '  make tla-check-ha-sync-wait       # HA sync wait target provenance' \
	  '  make tla-check-ha-timeline-switch # HA timeline switch boundary' \
	  '  make tla-check-ha-standby-apply   # HA standby apply and replay suppression' \
	  '  make tla-check-ha-rejoin          # HA former-primary rejoin and reseed' \
	  '  make tla-check-ha-replication     # HA replication/promotion/rejoin model' \
	  '  make tla-check-ha-failover-safety # HA acked-write failover and split-brain safety' \
	  '  make tla-check-ha-partition-fence # HA partition/fence propagation safety' \
	  '  make tla-check-ha-gate-transitions # HA gate transition stale-decision safety' \
	  '  make tla-check-batcher-coalescing # batcher per-key coalescing order/visibility' \
	  '  make tla-check-cdc-cutover       # CDC snapshot/stream cutover and checkpoint safety' \
	  '  make tla-check-shard-split-seq   # shard split sequence-level delta safety' \
	  '  make tla-check-snapshot-content  # snapshot content/index provenance' \
	  '  make tla-check-lsm-reserve-cleanup # LSM explicit reserve/fail cleanup safety' \
	  '  make tla-check-query-completeness # split query route completeness safety' \
	  '  make tla-check-derived-replay     # derived replay hint lanes and watermarks' \
	  '  make tla-check-derived-replay-heavy # split heavy derived replay confidence checks' \
	  '  make tla-check-derived-replay-heavy-depth # MaxSeq=3 single-index replay depth' \
	  '  make tla-check-derived-replay-heavy-multi-index # MaxSeq=2 two-index replay cross-product' \
	  '  make tla-check-derived-replay-heavy-full # MaxSeq=3 two-index manual confidence run' \
	  '  make tla-check-enrichment-lease   # generated enrichment lease publication' \
	  '  make tla-check-lmdb-commit        # LMDB commit/crash/readers' \
	  '  make tla-check-lsm-wal-compaction # LSM WAL/checkpoint/compaction' \
	  '  make tla-check-db-split-visibility' \
	  '  make tla-check-split-refinement-bridge' \
	  '  make tla-check-document-identity' \
	  '  make tla-check-document-identity-range-repair' \
	  '  make tla-check-transaction-session' \
	  '  make tla-check-managed-host-lifecycle' \
	  '  make tla-check-lite-publication' \
	  '  make tla-check-ml-graph-passes' \
	  '  make tla-check-ml-graph-dag-passes # arbitrary-DAG CSE/DCE remapping' \
	  '  make tla-check-ml-compiler-publication' \
	  '  make tla-check-openapi-codegen' \
	  '' \
	  'Trace validation:' \
	  '  make tla-check-trace TRACE=raft TRACE_FILES=/tmp/raft-trace.ndjson' \
	  '  make tla-check-trace TRACE=txn  TRACE_FILES=/tmp/txn-trace.ndjson' \
	  '  make tla-check-trace TRACE=txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"' \
	  '  make tla-check-trace TRACE=ha   TRACE_FILES="specs/tla/traces/ha_*.ndjson"' \
	  '  make tla-check-trace TRACE=split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"' \
	  '  make tla-check-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"' \
	  '  make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson' \
	  '  make tla-trace-txn  TRACE_FILES=/tmp/txn-trace.ndjson' \
	  '  make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"' \
	  '  make tla-trace-ha   TRACE_FILES="specs/tla/traces/ha_*.ndjson"' \
	  '  make tla-trace-split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"' \
	  '  make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"' \
	  '' \
	  'Housekeeping:' \
	  '  make tla-tools                    # install/check local TLC tools' \
	  '  make tla-clean                    # remove TLC runtime artifacts' \
	  '  make tla-audit                    # static hygiene audit (orphans, TTrace, unexercised mutants, Safety pins)'

tla-clean: ## Remove TLA+ model checker runtime artifacts.
	rm -rf specs/tla/states states
	find specs/tla -maxdepth 1 \( -name '*_TTrace_*.tla' -o -name '*_TTrace_*.bin' \) -delete

tla-audit: ## Static hygiene audit for the TLA+ suite (fails on orphans/TTrace/unexercised mutants; reports Safety-pin migration debt).
	@bash ../scripts/tla-audit.sh

tla-check: tla-check-txn tla-check-split tla-check-snap tla-check-lsm

tla-check-smoke: tla-tools ## Parse all checked-in TLA+ specs without model checking.
	@echo "==> Parsing checked-in TLA+ specs..."
	@source ../scripts/tla-tools.sh && \
	cd specs/tla && \
	for spec in *.tla; do \
	  case "$$spec" in *'_TTrace_'*) continue ;; esac; \
	  case "$$spec" in occ-2pc.tla) continue ;; esac; \
	  "$$TLA_JAVA" -cp "$$TLA2TOOLS" tla2sany.SANY "$$spec" >/dev/null; \
	done

tla-check-fast: tla-check-new-fast ## Alias for the fast focused lower-level specs.

tla-check-heavy: tla-check-new-heavy ## Alias for heavier focused lower-level specs.

tla-check-trace: tla-tools ## Dispatch trace validation with TRACE=raft|txn|txn-session|ha|split-bridge|doc-identity-range-repair and TRACE_FILES=...
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-check-trace TRACE=raft TRACE_FILES=/tmp/raft-trace.ndjson)
endif
ifndef TRACE
	$(error TRACE is required. Use TRACE=raft, TRACE=txn, TRACE=txn-session, TRACE=ha, TRACE=split-bridge, or TRACE=doc-identity-range-repair)
endif
	@if [ "$(TRACE)" = "raft" ]; then \
	  $(MAKE) tla-trace-raft TRACE_FILES="$(TRACE_FILES)"; \
	elif [ "$(TRACE)" = "txn" ]; then \
	  $(MAKE) tla-trace-txn TRACE_FILES="$(TRACE_FILES)"; \
	elif [ "$(TRACE)" = "txn-session" ]; then \
	  $(MAKE) tla-trace-txn-session TRACE_FILES="$(TRACE_FILES)"; \
	elif [ "$(TRACE)" = "ha" ]; then \
	  $(MAKE) tla-trace-ha TRACE_FILES="$(TRACE_FILES)"; \
	elif [ "$(TRACE)" = "split-bridge" ]; then \
	  $(MAKE) tla-trace-split-bridge TRACE_FILES="$(TRACE_FILES)"; \
	elif [ "$(TRACE)" = "doc-identity-range-repair" ]; then \
	  $(MAKE) tla-trace-doc-identity-range-repair TRACE_FILES="$(TRACE_FILES)"; \
	else \
	  echo "ERROR: TRACE must be raft, txn, txn-session, ha, split-bridge, or doc-identity-range-repair"; \
	  exit 1; \
	fi

define tla_check_module
	@echo "==> Model checking $(1)..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/$(2).cfg specs/tla/$(2).tla \
	  -workers auto -deadlock
endef

define tla_check_module_config
	@echo "==> Model checking $(1)..."
	source ../scripts/tla-tools.sh && \
	"$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/$(2).cfg specs/tla/$(3).tla \
	  -workers auto -deadlock
endef

define tla_check_expected_failure
	@echo "==> Model checking expected-failure $(1)..."
	@source ../scripts/tla-tools.sh && \
	status=0; \
	out=$$("$$TLA_JAVA" -XX:+UseParallelGC -cp "$$TLA2TOOLS" tlc2.TLC \
	  -config specs/tla/$(2).cfg specs/tla/$(3).tla \
	  -workers auto -deadlock 2>&1) || status=$$?; \
	echo "$$out"; \
	if [ $$status -eq 0 ]; then \
	  echo "ERROR: expected $(1) to fail, but TLC passed"; \
	  exit 1; \
	elif echo "$$out" | grep -qE "Invariant .* is violated|Temporal property .* (is |was )?violated"; then \
	  echo "OK: $(1) failed as expected"; \
	else \
	  echo "ERROR: $(1) failed, but not on an invariant/property violation (spec or config error?)"; \
	  exit 1; \
	fi
endef

tla-check-new: tla-check-new-fast tla-check-new-heavy ## Model check all newly-added focused specs.

tla-check-critical: tla-check-derived-replay tla-check-ha-failover-safety tla-check-ha-partition-fence tla-check-ha-gate-transitions tla-check-batcher-coalescing tla-check-cdc-cutover tla-check-shard-split-seq tla-check-snapshot-content tla-check-lsm-reserve-cleanup tla-check-query-completeness tla-check-split-refinement-bridge ## Run critique-repair critical safety subset.

tla-check-new-fast: tla-check-ha-gates tla-check-ha-sync-wait tla-check-ha-timeline-switch tla-check-ha-standby-apply tla-check-ha-rejoin tla-check-ha-failover-safety tla-check-ha-partition-fence tla-check-ha-gate-transitions tla-check-batcher-coalescing tla-check-cdc-cutover tla-check-shard-split-seq tla-check-snapshot-content tla-check-lsm-reserve-cleanup tla-check-query-completeness tla-check-node-drain-lifecycle tla-check-table-lifecycle tla-check-ha-retention-reseed tla-check-promotion-owner-handoff tla-check-index-lifecycle tla-check-derived-replay tla-check-enrichment-lease tla-check-lmdb-commit tla-check-lsm-wal-compaction tla-check-db-split-visibility tla-check-split-refinement-bridge tla-check-document-identity tla-check-document-identity-range-repair tla-check-transaction-session tla-check-managed-host-lifecycle tla-check-lite-publication tla-check-ml-graph-passes tla-check-ml-graph-dag-passes tla-check-ml-compiler-publication tla-check-openapi-codegen ## Model check the fast newly-added focused specs.

tla-check-new-heavy: tla-check-ha-replication tla-check-derived-replay-heavy ## Model check heavier newly-added focused specs.

tla-check-negative: tla-check-txn-negative-skip-intent-conflict tla-check-split-negative-premature-child-default tla-check-snap-negative-apply-without-put tla-check-lsm-negative-index-temp-leak tla-check-ha-replication-negative-stale-timeline-ack tla-check-ha-gates-negative-standby-runtime tla-check-ha-sync-wait-negative-move-target tla-check-ha-sync-wait-negative-wrong-timeline-ack tla-check-ha-sync-wait-negative-below-target-ack tla-check-ha-timeline-switch-negative-before-applied tla-check-ha-timeline-switch-negative-non-monotonic tla-check-ha-timeline-switch-negative-old-timeline tla-check-ha-timeline-switch-negative-recovery-previous tla-check-ha-standby-apply-negative-failure-advances tla-check-ha-standby-apply-negative-duplicate-effect tla-check-ha-standby-apply-negative-crash-loses-receive tla-check-ha-standby-apply-negative-client-write tla-check-ha-standby-apply-negative-background-runtime tla-check-ha-rejoin-negative-unfenced-rewind tla-check-ha-rejoin-negative-expired-wal-rewind tla-check-ha-rejoin-negative-forced-rewind tla-check-ha-rejoin-negative-identity-mismatch-rewind tla-check-ha-rejoin-negative-stale-assessment tla-check-ha-rejoin-negative-fork-mismatch tla-check-ha-failover-negative-promote-missing-ack tla-check-ha-failover-negative-old-primary-write tla-check-ha-failover-negative-ack-without-receipt tla-check-ha-partition-fence-negative-promote-before-fence tla-check-ha-partition-fence-negative-old-primary-write-after-promotion tla-check-ha-gate-transitions-negative-stale-allow tla-check-batcher-coalescing-negative-delete-write-inversion tla-check-batcher-coalescing-negative-write-delete-inversion tla-check-batcher-coalescing-negative-partial-visibility tla-check-batcher-coalescing-negative-delete-visible-without-delete tla-check-cdc-cutover-negative-boundary-duplicate tla-check-cdc-cutover-negative-checkpoint-ahead tla-check-cdc-cutover-negative-resume-replay tla-check-cdc-cutover-negative-stream-before-snapshot-complete tla-check-shard-split-seq-negative-keyset-cutover tla-check-shard-split-seq-negative-fenced-write-dropped tla-check-node-drain-negative-finalize-active tla-check-node-drain-negative-registration-clears-drain tla-check-node-drain-negative-safe-ignores-debt tla-check-table-lifecycle-negative-range-without-table tla-check-table-lifecycle-negative-intent-undesired-range tla-check-ha-retention-negative-truncate-unmarked tla-check-ha-retention-negative-backup-ignores-lost-wal tla-check-promotion-handoff-negative-attach-before-detach tla-check-promotion-handoff-negative-attach-before-transfer tla-check-promotion-handoff-negative-promote-unowned tla-check-index-lifecycle-negative-swap-incomplete tla-check-index-lifecycle-negative-recover-trusts-status tla-check-snapshot-content-negative-wrong-content tla-check-snapshot-content-negative-gc-needed-content tla-check-lsm-reserve-cleanup-negative-publish-without-reserve tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp tla-check-query-completeness-negative-route-before-child-ready tla-check-query-completeness-negative-double-serve tla-check-query-completeness-negative-missing-doc tla-check-derived-replay-negative tla-check-enrichment-lease-negative-stale-publish tla-check-enrichment-lease-negative-empty-pending tla-check-lsm-wal-compaction-negative-checkpoint tla-check-lsm-wal-compaction-negative-corrupt-rotate tla-check-lsm-wal-compaction-negative-pinned-retire tla-check-lmdb-commit-negative-meta-before-data tla-check-lmdb-commit-negative-reader-reuse tla-check-transaction-session-negative-rollback tla-check-transaction-session-negative-recovery-decision tla-check-transaction-session-negative-cleanup tla-check-transaction-session-trace-negative-cleanup tla-check-document-identity-negative-reuse-ordinal tla-check-document-identity-negative-stale-filter tla-check-document-identity-negative-namespace-mismatch tla-check-document-identity-range-repair-negative-split-unhealthy tla-check-document-identity-range-repair-negative-split-dest-namespace tla-check-document-identity-range-repair-negative-merge-mismatch tla-check-document-identity-range-repair-negative-merge-active-reassign tla-check-document-identity-range-repair-negative-restore-namespace tla-check-document-identity-range-repair-negative-restore-early-clear tla-check-document-identity-range-repair-trace-negative-restore-mismatch tla-check-document-identity-range-repair-trace-negative-restore-early-clear tla-check-db-split-visibility-negative-parent-write tla-check-db-split-visibility-negative-child-serve tla-check-db-split-visibility-negative-merge-donor tla-check-db-split-visibility-negative-enrichment-owner tla-check-split-refinement-bridge-negative-route-before-db-serving tla-check-split-refinement-bridge-negative-db-serve-before-shard-cutover tla-check-split-refinement-bridge-negative-stale-fence-cutover tla-check-split-refinement-bridge-trace-negative-route-before-db-serving tla-check-managed-host-lifecycle-negative-premature-restore tla-check-managed-host-lifecycle-negative-stale-route tla-check-managed-host-lifecycle-negative-revive-removed tla-check-managed-host-lifecycle-negative-restore-cancel tla-check-lite-publication-negative-manifest-before-artifacts tla-check-lite-publication-negative-failed-head tla-check-lite-publication-negative-pinned-cleanup tla-check-lite-publication-negative-mixed-generation tla-check-ml-graph-passes-negative-dangling-cse tla-check-ml-graph-passes-negative-parameter-dedup tla-check-ml-graph-passes-negative-missing-lower-closure tla-check-ml-graph-passes-negative-fallback-runtime tla-check-ml-graph-passes-negative-partial-publish tla-check-ml-graph-dag-passes-negative-cse-miss-duplicate tla-check-ml-graph-dag-passes-negative-cse-no-consumer-remap tla-check-ml-graph-dag-passes-negative-dce-drop-reachable tla-check-ml-graph-dag-passes-negative-dce-non-topo-map tla-check-ml-compiler-publication-negative-stale-compile tla-check-ml-compiler-publication-negative-missing-input tla-check-ml-compiler-publication-negative-output-selection tla-check-ml-compiler-publication-negative-fallback-publish tla-check-ml-compiler-publication-negative-partial-artifact tla-check-openapi-codegen-negative-stale-package tla-check-openapi-codegen-negative-stale-root tla-check-openapi-codegen-negative-internal-leak tla-check-openapi-codegen-negative-partial-commit ## Run TLA+ expected-failure mutant checks.

tla-check-txn-negative-skip-intent-conflict: tla-tools
	$(call tla_check_expected_failure,transaction OCC pending-intent conflict mutant,AntflyTransactionBadSkipIntentConflict,MC)

tla-check-split-negative-premature-child-default: tla-tools
	$(call tla_check_expected_failure,shard split premature child default mutant,AntflyShardSplitBadPrematureChildDefault,ShardSplitMC)

tla-check-snap-negative-apply-without-put: tla-tools
	$(call tla_check_expected_failure,snapshot apply without local archive mutant,AntflySnapshotTransferBadApplyWithoutPut,SnapshotTransferMC)

tla-check-lsm-negative-index-temp-leak: tla-tools
	$(call tla_check_expected_failure,LSM lifecycle index temp leak mutant,AntflyLsmLifecycleBadIndexTempLeak,AntflyLsmLifecycle)

tla-check-ha-replication-negative-stale-timeline-ack: tla-tools
	$(call tla_check_expected_failure,HA stale timeline ack mutant,AntflyHAReplicationBadStaleTimelineAck,AntflyHAReplication)

tla-check-ha-gates-negative-standby-runtime: tla-tools
	$(call tla_check_expected_failure,HA standby mutating runtime mutant,AntflyHAGatesBadStandbyRuntime,AntflyHAGates)

tla-check-ha-sync-wait-negative-move-target: tla-tools
	$(call tla_check_expected_failure,HA sync wait moving frozen target mutant,AntflyHASyncWaitBadMoveTarget,AntflyHASyncWait)

tla-check-ha-sync-wait-negative-wrong-timeline-ack: tla-tools
	$(call tla_check_expected_failure,HA sync wait wrong timeline ack mutant,AntflyHASyncWaitBadWrongTimelineAck,AntflyHASyncWait)

tla-check-ha-sync-wait-negative-below-target-ack: tla-tools
	$(call tla_check_expected_failure,HA sync wait below target ack mutant,AntflyHASyncWaitBadBelowTargetAck,AntflyHASyncWait)

tla-check-ha-timeline-switch-negative-before-applied: tla-tools
	$(call tla_check_expected_failure,HA timeline switch before parent apply mutant,AntflyHATimelineSwitchBadBeforeApplied,AntflyHATimelineSwitch)

tla-check-ha-timeline-switch-negative-non-monotonic: tla-tools
	$(call tla_check_expected_failure,HA timeline switch non-monotonic record mutant,AntflyHATimelineSwitchBadNonMonotonic,AntflyHATimelineSwitch)

tla-check-ha-timeline-switch-negative-old-timeline: tla-tools
	$(call tla_check_expected_failure,HA timeline switch old timeline accept mutant,AntflyHATimelineSwitchBadOldTimeline,AntflyHATimelineSwitch)

tla-check-ha-timeline-switch-negative-recovery-previous: tla-tools
	$(call tla_check_expected_failure,HA timeline switch recovery previous-LSN mutant,AntflyHATimelineSwitchBadRecoveryPrevious,AntflyHATimelineSwitch)

tla-check-ha-standby-apply-negative-failure-advances: tla-tools
	$(call tla_check_expected_failure,HA standby failed apply advances progress mutant,AntflyHAStandbyApplyBadFailureAdvances,AntflyHAStandbyApply)

tla-check-ha-standby-apply-negative-duplicate-effect: tla-tools
	$(call tla_check_expected_failure,HA standby duplicate apply side effect mutant,AntflyHAStandbyApplyBadDuplicateEffect,AntflyHAStandbyApply)

tla-check-ha-standby-apply-negative-crash-loses-receive: tla-tools
	$(call tla_check_expected_failure,HA standby crash loses durable receive mutant,AntflyHAStandbyApplyBadCrashLosesReceive,AntflyHAStandbyApply)

tla-check-ha-standby-apply-negative-client-write: tla-tools
	$(call tla_check_expected_failure,HA standby client write mutant,AntflyHAStandbyApplyBadClientWrite,AntflyHAStandbyApply)

tla-check-ha-standby-apply-negative-background-runtime: tla-tools
	$(call tla_check_expected_failure,HA standby background runtime mutant,AntflyHAStandbyApplyBadBackgroundRuntime,AntflyHAStandbyApply)

tla-check-ha-rejoin-negative-unfenced-rewind: tla-tools
	$(call tla_check_expected_failure,HA rejoin unfenced rewind mutant,AntflyHARejoinBadUnfencedRewind,AntflyHARejoin)

tla-check-ha-rejoin-negative-expired-wal-rewind: tla-tools
	$(call tla_check_expected_failure,HA rejoin expired WAL rewind mutant,AntflyHARejoinBadExpiredWalRewind,AntflyHARejoin)

tla-check-ha-rejoin-negative-forced-rewind: tla-tools
	$(call tla_check_expected_failure,HA rejoin forced promotion rewind mutant,AntflyHARejoinBadForcedRewind,AntflyHARejoin)

tla-check-ha-rejoin-negative-identity-mismatch-rewind: tla-tools
	$(call tla_check_expected_failure,HA rejoin identity mismatch rewind mutant,AntflyHARejoinBadIdentityMismatchRewind,AntflyHARejoin)

tla-check-ha-rejoin-negative-stale-assessment: tla-tools
	$(call tla_check_expected_failure,HA rejoin stale assessment truncate mutant,AntflyHARejoinBadStaleAssessment,AntflyHARejoin)

tla-check-ha-rejoin-negative-fork-mismatch: tla-tools
	$(call tla_check_expected_failure,HA rejoin fork mismatch truncate mutant,AntflyHARejoinBadForkMismatch,AntflyHARejoin)

tla-check-ha-failover-negative-promote-missing-ack: tla-tools
	$(call tla_check_expected_failure,HA failover promote missing acknowledged write mutant,AntflyHAFailoverSafetyBadPromoteMissingAck,AntflyHAFailoverSafety)

tla-check-ha-failover-negative-old-primary-write: tla-tools
	$(call tla_check_expected_failure,HA failover old primary post-promotion write mutant,AntflyHAFailoverSafetyBadOldPrimaryWrite,AntflyHAFailoverSafety)

tla-check-ha-failover-negative-ack-without-receipt: tla-tools
	$(call tla_check_expected_failure,HA failover sync ack without standby receipt mutant,AntflyHAFailoverSafetyBadAckWithoutReceipt,AntflyHAFailoverSafety)

tla-check-ha-partition-fence-negative-promote-before-fence: tla-tools
	$(call tla_check_expected_failure,HA partition promote-before-fence-delivery mutant,AntflyHAPartitionFenceBadPromoteBeforeFence,AntflyHAPartitionFence)

tla-check-ha-partition-fence-negative-old-primary-write-after-promotion: tla-tools
	$(call tla_check_expected_failure,HA partition old-primary normal write after promotion mutant,AntflyHAPartitionFenceBadOldPrimaryWriteAfterPromotion,AntflyHAPartitionFence)

tla-check-ha-gate-transitions-negative-stale-allow: tla-tools
	$(call tla_check_expected_failure,HA gate stale allow after role/fence transition mutant,AntflyHAGateTransitionsBadStaleAllow,AntflyHAGateTransitions)

tla-check-batcher-coalescing-negative-delete-write-inversion: tla-tools
	$(call tla_check_expected_failure,batcher delete/write inversion mutant,AntflyBatcherCoalescingBadDeleteWriteInversion,AntflyBatcherCoalescing)

tla-check-batcher-coalescing-negative-write-delete-inversion: tla-tools
	$(call tla_check_expected_failure,batcher write/delete inversion mutant,AntflyBatcherCoalescingBadWriteDeleteInversion,AntflyBatcherCoalescing)

tla-check-batcher-coalescing-negative-partial-visibility: tla-tools
	$(call tla_check_expected_failure,batcher partial visibility mutant,AntflyBatcherCoalescingBadPartialVisibility,AntflyBatcherCoalescing)

tla-check-batcher-coalescing-negative-delete-visible-without-delete: tla-tools
	$(call tla_check_expected_failure,batcher deleted value without trailing delete mutant,AntflyBatcherCoalescingBadDeleteVisibleWithoutDelete,AntflyBatcherCoalescing)

tla-check-cdc-cutover-negative-boundary-duplicate: tla-tools
	$(call tla_check_expected_failure,CDC snapshot/stream boundary duplicate mutant,AntflyCdcCutoverBadBoundaryDuplicate,AntflyCdcCutover)

tla-check-cdc-cutover-negative-checkpoint-ahead: tla-tools
	$(call tla_check_expected_failure,CDC checkpoint ahead of delivery mutant,AntflyCdcCutoverBadCheckpointAhead,AntflyCdcCutover)

tla-check-cdc-cutover-negative-resume-replay: tla-tools
	$(call tla_check_expected_failure,CDC resume replays checkpoint mutant,AntflyCdcCutoverBadResumeReplay,AntflyCdcCutover)

tla-check-cdc-cutover-negative-stream-before-snapshot-complete: tla-tools
	$(call tla_check_expected_failure,CDC stream phase before snapshot completion mutant,AntflyCdcCutoverBadStreamBeforeSnapshotComplete,AntflyCdcCutover)

tla-check-shard-split-seq-negative-keyset-cutover: tla-tools
	$(call tla_check_expected_failure,shard split key-set cutover loses second write mutant,AntflyShardSplitSeqBadKeySetCutover,AntflyShardSplitSeq)

tla-check-shard-split-seq-negative-fenced-write-dropped: tla-tools
	$(call tla_check_expected_failure,shard split cutover drops fenced write mutant,AntflyShardSplitSeqBadFencedWriteDropped,AntflyShardSplitSeq)

tla-check-node-drain-negative-finalize-active: tla-tools
	$(call tla_check_expected_failure,node drain finalize accepted while active mutant,AntflyNodeDrainLifecycleBadFinalizeActive,AntflyNodeDrainLifecycle)

tla-check-node-drain-negative-registration-clears-drain: tla-tools
	$(call tla_check_expected_failure,node re-registration clears drain intent mutant,AntflyNodeDrainLifecycleBadRegistrationClearsDrain,AntflyNodeDrainLifecycle)

tla-check-node-drain-negative-safe-ignores-debt: tla-tools
	$(call tla_check_expected_failure,node drain safe report ignores termination debt mutant,AntflyNodeDrainLifecycleBadSafeIgnoresDebt,AntflyNodeDrainLifecycle)

tla-check-table-lifecycle-negative-range-without-table: tla-tools
	$(call tla_check_expected_failure,desired range without table mutant,AntflyTableLifecycleBadRangeWithoutTable,AntflyTableLifecycle)

tla-check-table-lifecycle-negative-intent-undesired-range: tla-tools
	$(call tla_check_expected_failure,placement intent planned for undesired range mutant,AntflyTableLifecycleBadIntentUndesiredRange,AntflyTableLifecycle)

tla-check-ha-retention-negative-truncate-unmarked: tla-tools
	$(call tla_check_expected_failure,WAL truncation ignores unmarked lagging slot mutant,AntflyHARetentionReseedBadTruncateUnmarked,AntflyHARetentionReseed)

tla-check-ha-retention-negative-backup-ignores-lost-wal: tla-tools
	$(call tla_check_expected_failure,backup end ignores lost slot/truncated WAL mutant,AntflyHARetentionReseedBadBackupIgnoresLostWal,AntflyHARetentionReseed)

tla-check-promotion-handoff-negative-attach-before-detach: tla-tools
	$(call tla_check_expected_failure,promotion owner attach before detach mutant,AntflyPromotionOwnerHandoffBadAttachBeforeDetach,AntflyPromotionOwnerHandoff)

tla-check-promotion-handoff-negative-attach-before-transfer: tla-tools
	$(call tla_check_expected_failure,promotion owner attach before range transfer mutant,AntflyPromotionOwnerHandoffBadAttachBeforeTransfer,AntflyPromotionOwnerHandoff)

tla-check-promotion-handoff-negative-promote-unowned: tla-tools
	$(call tla_check_expected_failure,promotion without ownership predicate mutant,AntflyPromotionOwnerHandoffBadPromoteUnowned,AntflyPromotionOwnerHandoff)

tla-check-index-lifecycle-negative-swap-incomplete: tla-tools
	$(call tla_check_expected_failure,shadow index swap while behind mutant,AntflyIndexLifecycleBadSwapIncomplete,AntflyIndexLifecycle)

tla-check-index-lifecycle-negative-recover-trusts-status: tla-tools
	$(call tla_check_expected_failure,index recovery trusts stale fresh status mutant,AntflyIndexLifecycleBadRecoverTrustsStatus,AntflyIndexLifecycle)

tla-check-snapshot-content-negative-wrong-content: tla-tools
	$(call tla_check_expected_failure,snapshot wrong content for applied index mutant,AntflySnapshotContentBadWrongContent,AntflySnapshotContent)

tla-check-snapshot-content-negative-gc-needed-content: tla-tools
	$(call tla_check_expected_failure,snapshot GC of needed fetched content mutant,AntflySnapshotContentBadGcNeededContent,AntflySnapshotContent)

tla-check-lsm-reserve-cleanup-negative-publish-without-reserve: tla-tools
	$(call tla_check_expected_failure,LSM publish without cleanup reserve mutant,AntflyLsmReserveCleanupBadPublishWithoutReserve,AntflyLsmReserveCleanup)

tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp: tla-tools
	$(call tla_check_expected_failure,LSM failure leaks temporary allocation mutant,AntflyLsmReserveCleanupBadFailureLeaksTemp,AntflyLsmReserveCleanup)

tla-check-query-completeness-negative-route-before-child-ready: tla-tools
	$(call tla_check_expected_failure,query route-before-child-ready mutant,AntflyQueryCompletenessBadRouteBeforeChildReady,AntflyQueryCompleteness)

tla-check-query-completeness-negative-double-serve: tla-tools
	$(call tla_check_expected_failure,query parent/child double-serve mutant,AntflyQueryCompletenessBadDoubleServe,AntflyQueryCompleteness)

tla-check-query-completeness-negative-missing-doc: tla-tools
	$(call tla_check_expected_failure,query missing moved document mutant,AntflyQueryCompletenessBadMissingDoc,AntflyQueryCompleteness)

tla-check-derived-replay-negative: tla-tools
	$(call tla_check_expected_failure,derived replay latest-without-hint mutant,AntflyDerivedReplayBad,AntflyDerivedReplay)

tla-check-enrichment-lease-negative-stale-publish: tla-tools
	$(call tla_check_expected_failure,enrichment stale lease publish mutant,AntflyEnrichmentLeaseBadStalePublish,AntflyEnrichmentLease)

tla-check-enrichment-lease-negative-empty-pending: tla-tools
	$(call tla_check_expected_failure,enrichment hidden pending advancement mutant,AntflyEnrichmentLeaseBadEmptyPending,AntflyEnrichmentLease)

tla-check-lsm-wal-compaction-negative-checkpoint: tla-tools
	$(call tla_check_expected_failure,LSM checkpoint unsynced WAL mutant,AntflyLsmWalCompactionBadCheckpoint,AntflyLsmWalCompaction)

tla-check-lsm-wal-compaction-negative-corrupt-rotate: tla-tools
	$(call tla_check_expected_failure,LSM corrupt tail rotation mutant,AntflyLsmWalCompactionBadCorruptRotate,AntflyLsmWalCompaction)

tla-check-lsm-wal-compaction-negative-pinned-retire: tla-tools
	$(call tla_check_expected_failure,LSM pinned segment retirement mutant,AntflyLsmWalCompactionBadPinnedRetire,AntflyLsmWalCompaction)

tla-check-lmdb-commit-negative-meta-before-data: tla-tools
	$(call tla_check_expected_failure,LMDB meta-before-data mutant,AntflyLmdbCommitBadMetaBeforeData,AntflyLmdbCommit)

tla-check-lmdb-commit-negative-reader-reuse: tla-tools
	$(call tla_check_expected_failure,LMDB reader-visible page reuse mutant,AntflyLmdbCommitBadReaderReuse,AntflyLmdbCommit)

tla-check-transaction-session-negative-rollback: tla-tools
	$(call tla_check_expected_failure,transaction session rollback mutant,AntflyTransactionSessionBadRollback,AntflyTransactionSession)

tla-check-transaction-session-negative-recovery-decision: tla-tools
	$(call tla_check_expected_failure,transaction session recovery decision mutant,AntflyTransactionSessionBadRecoveryDecision,AntflyTransactionSession)

tla-check-transaction-session-negative-cleanup: tla-tools
	$(call tla_check_expected_failure,transaction session cleanup mutant,AntflyTransactionSessionBadCleanup,AntflyTransactionSession)

tla-check-transaction-session-trace-negative-cleanup: tla-tools
	@echo "==> Validating expected-failure transaction session cleanup trace..."
	@if bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyTransactionSession.tla \
	  -c specs/tla/TraceAntflyTransactionSession.cfg \
	  specs/tla/traces/negative/txn_session_bad_cleanup.ndjson; then \
	  echo "ERROR: expected transaction session cleanup trace to fail, but TLC passed"; \
	  exit 1; \
	else \
	  echo "OK: transaction session cleanup trace failed as expected"; \
	fi

tla-check-document-identity-negative-reuse-ordinal: tla-tools
	$(call tla_check_expected_failure,document identity ordinal reuse mutant,AntflyDocumentIdentityBadReuseOrdinal,AntflyDocumentIdentity)

tla-check-document-identity-negative-stale-filter: tla-tools
	$(call tla_check_expected_failure,document identity stale filter mutant,AntflyDocumentIdentityBadStaleFilter,AntflyDocumentIdentity)

tla-check-document-identity-negative-namespace-mismatch: tla-tools
	$(call tla_check_expected_failure,document identity namespace mismatch mutant,AntflyDocumentIdentityBadNamespaceMismatch,AntflyDocumentIdentity)

tla-check-document-identity-range-repair-negative-split-unhealthy: tla-tools
	$(call tla_check_expected_failure,document identity range repair unhealthy split mutant,AntflyDocumentIdentityRangeRepairBadSplitUnhealthy,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-negative-split-dest-namespace: tla-tools
	$(call tla_check_expected_failure,document identity range repair stale split destination mutant,AntflyDocumentIdentityRangeRepairBadSplitDestNamespace,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-negative-merge-mismatch: tla-tools
	$(call tla_check_expected_failure,document identity range repair merge namespace mismatch mutant,AntflyDocumentIdentityRangeRepairBadMergeMismatch,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-negative-merge-active-reassign: tla-tools
	$(call tla_check_expected_failure,document identity range repair unapproved merge reassignment mutant,AntflyDocumentIdentityRangeRepairBadMergeActiveReassign,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-negative-restore-namespace: tla-tools
	$(call tla_check_expected_failure,document identity range repair restore namespace mismatch mutant,AntflyDocumentIdentityRangeRepairBadRestoreNamespace,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-negative-restore-early-clear: tla-tools
	$(call tla_check_expected_failure,document identity range repair early restore-intent clear mutant,AntflyDocumentIdentityRangeRepairBadRestoreEarlyClear,AntflyDocumentIdentityRangeRepair)

tla-check-document-identity-range-repair-trace-negative-restore-mismatch: tla-tools
	@echo "==> Validating expected-failure document identity restore mismatch trace..."
	@if bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyDocumentIdentityRangeRepair.tla \
	  -c specs/tla/TraceAntflyDocumentIdentityRangeRepair.cfg \
	  specs/tla/traces/negative/doc_identity_restore_accept_mismatch.ndjson; then \
	  echo "ERROR: expected document identity restore mismatch trace to fail, but TLC passed"; \
	  exit 1; \
	else \
	  echo "OK: document identity restore mismatch trace failed as expected"; \
	fi

tla-check-document-identity-range-repair-trace-negative-restore-early-clear: tla-tools
	@echo "==> Validating expected-failure document identity restore early-clear trace..."
	@if bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyDocumentIdentityRangeRepair.tla \
	  -c specs/tla/TraceAntflyDocumentIdentityRangeRepair.cfg \
	  specs/tla/traces/negative/doc_identity_restore_early_clear.ndjson; then \
	  echo "ERROR: expected document identity restore early-clear trace to fail, but TLC passed"; \
	  exit 1; \
	else \
	  echo "OK: document identity restore early-clear trace failed as expected"; \
	fi

tla-check-db-split-visibility-negative-parent-write: tla-tools
	$(call tla_check_expected_failure,DB split parent child-range write mutant,AntflyDbSplitVisibilityBadParentWrite,AntflyDbSplitVisibility)

tla-check-db-split-visibility-negative-child-serve: tla-tools
	$(call tla_check_expected_failure,DB split premature child serve mutant,AntflyDbSplitVisibilityBadChildServe,AntflyDbSplitVisibility)

tla-check-db-split-visibility-negative-merge-donor: tla-tools
	$(call tla_check_expected_failure,DB merge donor post-handoff serve mutant,AntflyDbSplitVisibilityBadMergeDonor,AntflyDbSplitVisibility)

tla-check-db-split-visibility-negative-enrichment-owner: tla-tools
	$(call tla_check_expected_failure,DB split enrichment owner mutant,AntflyDbSplitVisibilityBadEnrichmentOwner,AntflyDbSplitVisibility)

tla-check-split-refinement-bridge-negative-route-before-db-serving: tla-tools
	$(call tla_check_expected_failure,split refinement route before DB serving mutant,AntflySplitRefinementBridgeBadRouteBeforeDbServing,AntflySplitRefinementBridge)

tla-check-split-refinement-bridge-negative-db-serve-before-shard-cutover: tla-tools
	$(call tla_check_expected_failure,split refinement DB serve before shard cutover mutant,AntflySplitRefinementBridgeBadDbServeBeforeShardCutover,AntflySplitRefinementBridge)

tla-check-split-refinement-bridge-negative-stale-fence-cutover: tla-tools
	$(call tla_check_expected_failure,split refinement stale fence cutover mutant,AntflySplitRefinementBridgeBadStaleFenceCutover,AntflySplitRefinementBridge)

tla-check-split-refinement-bridge-trace-negative-route-before-db-serving: tla-tools
	@echo "==> Validating expected-failure split bridge route-before-serving trace..."
	@if bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflySplitRefinementBridge.tla \
	  -c specs/tla/TraceAntflySplitRefinementBridge.cfg \
	  specs/tla/traces/negative/split_bridge_route_before_db_serving.ndjson; then \
	  echo "ERROR: expected split bridge route-before-serving trace to fail, but TLC passed"; \
	  exit 1; \
	else \
	  echo "OK: split bridge route-before-serving trace failed as expected"; \
	fi

tla-check-managed-host-lifecycle-negative-premature-restore: tla-tools
	$(call tla_check_expected_failure,managed host premature restore activation mutant,AntflyManagedHostLifecycleBadPrematureRestore,AntflyManagedHostLifecycle)

tla-check-managed-host-lifecycle-negative-stale-route: tla-tools
	$(call tla_check_expected_failure,managed host stale route mutant,AntflyManagedHostLifecycleBadStaleRoute,AntflyManagedHostLifecycle)

tla-check-managed-host-lifecycle-negative-revive-removed: tla-tools
	$(call tla_check_expected_failure,managed host removed replica revival mutant,AntflyManagedHostLifecycleBadReviveRemoved,AntflyManagedHostLifecycle)

tla-check-managed-host-lifecycle-negative-restore-cancel: tla-tools
	$(call tla_check_expected_failure,managed host restore cancellation mutant,AntflyManagedHostLifecycleBadRestoreCancel,AntflyManagedHostLifecycle)

tla-check-lite-publication-negative-manifest-before-artifacts: tla-tools
	$(call tla_check_expected_failure,Lite publication manifest-before-artifacts mutant,AntflyLitePublicationBadManifestBeforeArtifacts,AntflyLitePublication)

tla-check-lite-publication-negative-failed-head: tla-tools
	$(call tla_check_expected_failure,Lite publication failed-head mutant,AntflyLitePublicationBadFailedHead,AntflyLitePublication)

tla-check-lite-publication-negative-pinned-cleanup: tla-tools
	$(call tla_check_expected_failure,Lite publication pinned-cleanup mutant,AntflyLitePublicationBadPinnedCleanup,AntflyLitePublication)

tla-check-lite-publication-negative-mixed-generation: tla-tools
	$(call tla_check_expected_failure,Lite publication mixed-generation mutant,AntflyLitePublicationBadMixedGeneration,AntflyLitePublication)

tla-check-ml-graph-passes-negative-dangling-cse: tla-tools
	$(call tla_check_expected_failure,ML graph dangling CSE remap mutant,AntflyMlGraphPassesBadDanglingCse,AntflyMlGraphPasses)

tla-check-ml-graph-passes-negative-parameter-dedup: tla-tools
	$(call tla_check_expected_failure,ML graph parameter/constant dedup mutant,AntflyMlGraphPassesBadParameterDedup,AntflyMlGraphPasses)

tla-check-ml-graph-passes-negative-missing-lower-closure: tla-tools
	$(call tla_check_expected_failure,ML graph missing lower closure mutant,AntflyMlGraphPassesBadMissingLowerClosure,AntflyMlGraphPasses)

tla-check-ml-graph-passes-negative-fallback-runtime: tla-tools
	$(call tla_check_expected_failure,ML graph fallback runtime publish mutant,AntflyMlGraphPassesBadFallbackRuntime,AntflyMlGraphPasses)

tla-check-ml-graph-passes-negative-partial-publish: tla-tools
	$(call tla_check_expected_failure,ML graph failed-pass partial publish mutant,AntflyMlGraphPassesBadPartialPublish,AntflyMlGraphPasses)

tla-check-ml-graph-dag-passes-negative-cse-miss-duplicate: tla-tools
	$(call tla_check_expected_failure,ML graph DAG missed duplicate CSE mutant,AntflyMlGraphDagPassesBadCseMissDuplicate,AntflyMlGraphDagPasses)

tla-check-ml-graph-dag-passes-negative-cse-no-consumer-remap: tla-tools
	$(call tla_check_expected_failure,ML graph DAG stale consumer remap mutant,AntflyMlGraphDagPassesBadCseNoConsumerRemap,AntflyMlGraphDagPasses)

tla-check-ml-graph-dag-passes-negative-dce-drop-reachable: tla-tools
	$(call tla_check_expected_failure,ML graph DAG dropped reachable DCE mutant,AntflyMlGraphDagPassesBadDceDropReachable,AntflyMlGraphDagPasses)

tla-check-ml-graph-dag-passes-negative-dce-non-topo-map: tla-tools
	$(call tla_check_expected_failure,ML graph DAG non-topological DCE map mutant,AntflyMlGraphDagPassesBadDceNonTopoMap,AntflyMlGraphDagPasses)

tla-check-ml-compiler-publication-negative-stale-compile: tla-tools
	$(call tla_check_expected_failure,ML compiler stale compile mutant,AntflyMlCompilerPublicationBadStaleCompile,AntflyMlCompilerPublication)

tla-check-ml-compiler-publication-negative-missing-input: tla-tools
	$(call tla_check_expected_failure,ML compiler missing external input mutant,AntflyMlCompilerPublicationBadMissingInput,AntflyMlCompilerPublication)

tla-check-ml-compiler-publication-negative-output-selection: tla-tools
	$(call tla_check_expected_failure,ML compiler wrong output selection mutant,AntflyMlCompilerPublicationBadOutputSelection,AntflyMlCompilerPublication)

tla-check-ml-compiler-publication-negative-fallback-publish: tla-tools
	$(call tla_check_expected_failure,ML compiler fallback publish mutant,AntflyMlCompilerPublicationBadFallbackPublish,AntflyMlCompilerPublication)

tla-check-ml-compiler-publication-negative-partial-artifact: tla-tools
	$(call tla_check_expected_failure,ML compiler partial artifact mutant,AntflyMlCompilerPublicationBadPartialArtifact,AntflyMlCompilerPublication)

tla-check-openapi-codegen-negative-stale-package: tla-tools
	$(call tla_check_expected_failure,OpenAPI stale generated package mutant,AntflyOpenApiCodegenBadStalePackage,AntflyOpenApiCodegen)

tla-check-openapi-codegen-negative-stale-root: tla-tools
	$(call tla_check_expected_failure,OpenAPI stale root spec mutant,AntflyOpenApiCodegenBadStaleRoot,AntflyOpenApiCodegen)

tla-check-openapi-codegen-negative-internal-leak: tla-tools
	$(call tla_check_expected_failure,OpenAPI public client internal import mutant,AntflyOpenApiCodegenBadInternalLeak,AntflyOpenApiCodegen)

tla-check-openapi-codegen-negative-partial-commit: tla-tools
	$(call tla_check_expected_failure,OpenAPI partial generation commit mutant,AntflyOpenApiCodegenBadPartialCommit,AntflyOpenApiCodegen)

tla-check-ha-gates: tla-tools
	$(call tla_check_module,HA gate decision table,AntflyHAGates)

tla-check-ha-sync-wait: tla-tools
	$(call tla_check_module,HA sync wait target provenance,AntflyHASyncWait)

tla-check-ha-timeline-switch: tla-tools
	$(call tla_check_module,HA timeline switch boundary,AntflyHATimelineSwitch)

tla-check-ha-standby-apply: tla-tools
	$(call tla_check_module,HA standby apply and replay suppression,AntflyHAStandbyApply)

tla-check-ha-rejoin: tla-tools
	$(call tla_check_module,HA former-primary rejoin and reseed,AntflyHARejoin)

tla-check-ha-replication: tla-tools
	$(call tla_check_module,HA replication slot/fence/promotion/rejoin,AntflyHAReplication)

tla-check-ha-failover-safety: tla-tools
	$(call tla_check_module,HA failover acknowledged-write and split-brain safety,AntflyHAFailoverSafety)

tla-check-ha-partition-fence: tla-tools
	$(call tla_check_module,HA partition and fence propagation safety,AntflyHAPartitionFence)

tla-check-ha-gate-transitions: tla-tools
	$(call tla_check_module,HA gate transition stale-decision safety,AntflyHAGateTransitions)

tla-check-batcher-coalescing: tla-tools
	$(call tla_check_module,batcher per-key coalescing order and visibility,AntflyBatcherCoalescing)

tla-check-cdc-cutover: tla-tools
	$(call tla_check_module,CDC snapshot/stream cutover and checkpoint safety,AntflyCdcCutover)

tla-check-shard-split-seq: tla-tools
	$(call tla_check_module,shard split sequence-level delta safety,AntflyShardSplitSeq)

tla-check-snapshot-content: tla-tools
	$(call tla_check_module,snapshot content/index provenance,AntflySnapshotContent)

tla-check-lsm-reserve-cleanup: tla-tools
	$(call tla_check_module,LSM explicit reserve/fail cleanup safety,AntflyLsmReserveCleanup)

tla-check-query-completeness: tla-tools
	$(call tla_check_module,split query routing completeness safety,AntflyQueryCompleteness)

tla-check-node-drain-lifecycle: tla-tools
	$(call tla_check_module,node drain/scale-down lifecycle safety,AntflyNodeDrainLifecycle)

tla-check-table-lifecycle: tla-tools
	$(call tla_check_module,table create/drop desired-vs-committed lifecycle safety,AntflyTableLifecycle)

tla-check-ha-retention-reseed: tla-tools
	$(call tla_check_module,WAL retention floor/reseed/backup slot safety,AntflyHARetentionReseed)

tla-check-promotion-owner-handoff: tla-tools
	$(call tla_check_module,entity promotion single-owner handoff safety,AntflyPromotionOwnerHandoff)

tla-check-index-lifecycle: tla-tools
	$(call tla_check_module,index lifecycle/shadow swap freshness safety,AntflyIndexLifecycle)

tla-check-derived-replay: tla-tools
	$(call tla_check_module,derived replay hint lanes and watermarks,AntflyDerivedReplay)

tla-check-derived-replay-heavy: tla-check-derived-replay-heavy-depth tla-check-derived-replay-heavy-multi-index ## Split heavy derived replay confidence checks.

tla-check-derived-replay-heavy-depth: tla-tools
	$(call tla_check_module_config,derived replay hint lanes and watermarks depth-heavy,AntflyDerivedReplay-heavy-depth,AntflyDerivedReplay)

tla-check-derived-replay-heavy-multi-index: tla-tools
	$(call tla_check_module_config,derived replay hint lanes and watermarks multi-index-heavy,AntflyDerivedReplay-heavy-multi-index,AntflyDerivedReplay)

tla-check-derived-replay-heavy-full: tla-tools
	$(call tla_check_module_config,derived replay hint lanes and watermarks full heavy,AntflyDerivedReplay-heavy,AntflyDerivedReplay)

tla-check-enrichment-lease: tla-tools
	$(call tla_check_module,enrichment lease publication,AntflyEnrichmentLease)

tla-check-lmdb-commit: tla-tools
	$(call tla_check_module,LMDB commit crash and readers,AntflyLmdbCommit)

tla-check-lsm-wal-compaction: tla-tools
	$(call tla_check_module,LSM WAL checkpoint compaction,AntflyLsmWalCompaction)

tla-check-db-split-visibility: tla-tools
	$(call tla_check_module,DB split visibility,AntflyDbSplitVisibility)

tla-check-split-refinement-bridge: tla-tools
	$(call tla_check_module,split refinement bridge,AntflySplitRefinementBridge)

tla-check-document-identity: tla-tools
	$(call tla_check_module,document identity uniqueness,AntflyDocumentIdentity)

tla-check-document-identity-range-repair: tla-tools
	$(call tla_check_module,document identity range repair,AntflyDocumentIdentityRangeRepair)

tla-check-transaction-session: tla-tools
	$(call tla_check_module,transaction session savepoints,AntflyTransactionSession)

tla-check-managed-host-lifecycle: tla-tools
	$(call tla_check_module,managed host lifecycle,AntflyManagedHostLifecycle)

tla-check-lite-publication: tla-tools
	$(call tla_check_module,lite publication,AntflyLitePublication)

tla-check-ml-graph-passes: tla-tools
	$(call tla_check_module,ML graph pass ordering,AntflyMlGraphPasses)

tla-check-ml-graph-dag-passes: tla-tools
	$(call tla_check_module,ML graph arbitrary-DAG CSE/DCE remapping,AntflyMlGraphDagPasses)

tla-check-ml-compiler-publication: tla-tools
	$(call tla_check_module,ML compiler publication,AntflyMlCompilerPublication)

tla-check-openapi-codegen: tla-tools
	$(call tla_check_module,OpenAPI codegen publication,AntflyOpenApiCodegen)

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

tla-trace-txn-session: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson")
endif
	bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyTransactionSession.tla \
	  -c specs/tla/TraceAntflyTransactionSession.cfg \
	  $(TRACE_FILES)

tla-trace-ha: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-ha TRACE_FILES="specs/tla/traces/ha_*.ndjson")
endif
	bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyHA.tla \
	  -c specs/tla/TraceAntflyHA.cfg \
	  $(TRACE_FILES)

tla-trace-split-bridge: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson")
endif
	bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflySplitRefinementBridge.tla \
	  -c specs/tla/TraceAntflySplitRefinementBridge.cfg \
	  $(TRACE_FILES)

tla-trace-doc-identity-range-repair: tla-tools
ifndef TRACE_FILES
	$(error TRACE_FILES is required. Example: make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson")
endif
	bash ../scripts/tla-validate-trace.sh -S -p 1 \
	  -s specs/tla/TraceAntflyDocumentIdentityRangeRepair.tla \
	  -c specs/tla/TraceAntflyDocumentIdentityRangeRepair.cfg \
	  $(TRACE_FILES)
