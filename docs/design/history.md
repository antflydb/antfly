# Implementation history

This index preserves dated implementation records, investigations, and
qualification evidence beside the relevant design or operational topic. These
records describe work at the time it was performed; current contracts remain
in the linked design documents.

Proposed and active work lives in [plans](../plans/README.md). See the
[documentation placement rules](../README.md#placement-rules) before adding
or completing a plan. The [Zig roadmap](../../zig/ROADMAP.md) indexes colocated
subsystem designs.

## Records

### Ingestion

| Feature | Document | Summary |
|---------|----------|---------|
| DOCX/PPTX & Google Docs/Slides Support | [ppt-docx.md](ingestion/history/ppt-docx.md) | Structured extraction for Office and Google Workspace document formats in docsaf, using only the standard library |
| Reader Interface (OCR/Vision) | [reader-integration.md](ingestion/history/reader-integration.md) | A reusable `Reader` interface for OCR/vision integrations, replacing ad-hoc per-app implementations |

### Relocated Implementation Logs

Dated implementation logs, defect ledgers, benchmark diaries, and roadmap
restatements moved out of the Zig design docs on 2026-09-16. Durable decisions
were folded into the design doc first, and the design doc keeps a pointer.
Logs longer than ~300 lines were later condensed into summaries of decisions,
rejected approaches, and key measurements; each summary links to its full
verbatim text at a pinned commit. The placement rule is
recorded under Planning Rules in [`zig/ROADMAP.md`](../../zig/ROADMAP.md).

| Area | Document | Design doc | Summary |
|------|----------|------------|---------|
| VOPR | [vopr/status-history.md](vopr/history/status-history.md) | [zig/VOPR.md](../../zig/VOPR.md) | Scenario-version status preamble (v9–v53) and verification-audit narrative |
| VOPR | [vopr/defects-found.md](vopr/history/defects-found.md) | [zig/VOPR.md](../../zig/VOPR.md) | Per-defect ledger of production and harness bugs VOPR found |
| VOPR | [vopr/follow-ups-2026-09.md](vopr/history/follow-ups-2026-09.md) | [zig/VOPR.md](../../zig/VOPR.md) | Dated runtime-correctness, merge, and deadline follow-ups (2026-09-06/07) |
| VOPR | [vopr/full-cluster-scenario-narratives.md](vopr/history/full-cluster-scenario-narratives.md) | [zig/VOPR.md](../../zig/VOPR.md) | Per-version v11–v53 full-cluster narrative, gate-tier list, v13–v18 recap, and timer/deadline defect narrative |
| VOPR | [vopr/condensed-sections-2026-10.md](vopr/history/condensed-sections-2026-10.md) | [zig/VOPR.md](../../zig/VOPR.md) | Pre-condensation prose: extension-seam walkthroughs, scheduled-soak operations narrative, distributed coverage boundary, verification audit, defect classes, and long-form ongoing roadmap |
| Vector store | [vector-store/experiments-2026-09.md](vector-store/history/experiments-2026-09.md) | [zig/VECTOR_STORE.md](../../zig/VECTOR_STORE.md) | September 2026 experiment write-ups and 1M qualification tables |
| Vector store | [vector-store/vectordbbench-source-store-2026-09.md](vector-store/history/vectordbbench-source-store-2026-09.md) | [zig/VECTORDBBENCH_FINDINGS.md](../../zig/VECTORDBBENCH_FINDINGS.md) | Source-store memory, GC, and structural-experiment qualification narrative |
| Vector store | [vector-store/migration-qualification-findings.md](vector-store/history/migration-qualification-findings.md) | [zig/VECTOR_STORE.md](../../zig/VECTOR_STORE.md) | Former VECTOR_STORAGE_MIGRATION_FINDINGS.md: 50K/1M migration screens and review follow-ups |
| Vector store | [vector-store/review-2026-09-08.md](vector-store/history/review-2026-09-08.md) | [zig/VECTOR_STORE.md](../../zig/VECTOR_STORE.md) | Former VECTOR_STORE_REVIEW.md: correctness boundaries and design opportunities review |
| Dense indexing | [dense-indexing/vectordbbench-native-hbc-2026-08.md](dense-indexing/history/vectordbbench-native-hbc-2026-08.md) | [zig/VECTORDBBENCH_FINDINGS.md](../../zig/VECTORDBBENCH_FINDINGS.md) | Confirmed-finding evidence and native HBC measurements through r126 |
| Dense indexing | [dense-indexing/vectordbbench-2026-09-03-05.md](dense-indexing/history/vectordbbench-2026-09-03-05.md) | [zig/VECTORDBBENCH_FINDINGS.md](../../zig/VECTORDBBENCH_FINDINGS.md) | Bounded publication, r129, V2 directory experiments, next checks, audited baselines |
| Dense indexing | [dense-indexing/vectordbbench-2026-09-05-07.md](dense-indexing/history/vectordbbench-2026-09-05-07.md) | [zig/VECTORDBBENCH_FINDINGS.md](../../zig/VECTORDBBENCH_FINDINGS.md) | Prepared publication, recovery treatments, and subgroup routing experiments |
| Dense indexing | [dense-indexing/vectordbbench-2026-09-07-14.md](dense-indexing/history/vectordbbench-2026-09-07-14.md) | [zig/VECTORDBBENCH_FINDINGS.md](../../zig/VECTORDBBENCH_FINDINGS.md) | No-copy default, posting-row experiments, and source-ownership comparison |
| Dense indexing | [dense-indexing/lifecycle-implementation-log.md](dense-indexing/history/lifecycle-implementation-log.md) | [zig/DENSE_INDEXING_LIFECYCLE.md](../../zig/DENSE_INDEXING_LIFECYCLE.md) | Former implementation-status bullets, physical-versioning fix narratives, r128 qualification, completed phases 0/1/2/4/6, and rollout order |
| Dense indexing | [dense-indexing/lifecycle-condensed-sections-2026-10.md](dense-indexing/history/lifecycle-condensed-sections-2026-10.md) | [zig/DENSE_INDEXING_LIFECYCLE.md](../../zig/DENSE_INDEXING_LIFECYCLE.md) | Pre-condensation text: problem/terminology, HBC session capabilities, recovery owner, disk admission, repair scheduler, numbered repair test matrix, V1-mirror authority, rollout counters |
| Full text | [full-text/implementation-progress-2026-07.md](full-text/history/implementation-progress-2026-07.md) | [zig/FULL_TEXT.md](../../zig/FULL_TEXT.md) | v29–v38 posting-format increments and kernel qualification (2026-07) |
| Graph metrics | [graph-metrics/roadmap-restatements.md](graph-metrics/history/roadmap-restatements.md) | [zig/GRAPH_METRICS.md](../../zig/GRAPH_METRICS.md) | Per-phase progress blocks and successive remaining-roadmap restatements |
| Graph | [graph/edge-identity-investigation-2026-09.md](graph/history/edge-identity-investigation-2026-09.md) | [zig/GRAPH.md](../../zig/GRAPH.md) | 2026-09-30 relationship-identity investigation behind stable edge IDs; implemented contract in [artifact indexes](../guides/artifact-indexes.mdx) |
| Derived documents | [derived-documents/implementation-status-history.md](derived-documents/history/implementation-status-history.md) | [zig/DERIVED_DOCUMENT_HIERARCHY.md](../../zig/DERIVED_DOCUMENT_HIERARCHY.md) | Per-phase implementation-status bullets |
| LSM writes | [lsm-writes/follow-ups-2026-04.md](lsm-writes/history/follow-ups-2026-04.md) | [WRITES.md](../../zig/pkg/antfly/src/storage/lsm/WRITES.md) | 2026-04-16 write-amplification follow-ups and execution checklist |
| LSM writes | [lsm-writes/baseline-evidence-2026-06.md](lsm-writes/history/baseline-evidence-2026-06.md) | [LSM.md](../../zig/pkg/antfly/src/storage/lsm/LSM.md) | Sampled ns/op baseline (2026-06-02) |
| LSM publication | [lsm-version-publication/validation-2026-09.md](lsm-version-publication/history/validation-2026-09.md) | [lsm-version-publication.md](lsm-version-publication.md) | Dated validation runs and local ReleaseFast measurements |
| DOCID | [docid/query-bench-diary.md](docid/history/query-bench-diary.md) | [zig/DOCID.md](../../zig/DOCID.md) | Query and bulk-load optimization diary |
| DOCID | [docid/implementation-notes.md](docid/history/implementation-notes.md) | [zig/DOCID.md](../../zig/DOCID.md) | Per-change identity, filtering, namespace/split/merge notes and the Phase 1–8 roadmap narrative |
| Algebraic | [algebraic/churn-benchmarks-2026-05.md](algebraic/history/churn-benchmarks-2026-05.md) | [zig/ALGEBRAIC.md](../../zig/ALGEBRAIC.md) | May 2026 churn smoke and microbench chain |
| Algebraic | [algebraic/prototype-and-capability-log.md](algebraic/history/prototype-and-capability-log.md) | [zig/ALGEBRAIC.md](../../zig/ALGEBRAIC.md) | Prototype bootstrap milestones, success criteria, and the line-per-capability changelog |
| Algebraic | [algebraic/theory.md](algebraic/theory.md) | [zig/ALGEBRAIC.md](../../zig/ALGEBRAIC.md) | Extended sparse-token theory: derivations, worked tokenization and temporal-cylinder examples, COUNT/MAX maintenance example, original prototype sketch |
| Relational | [relational/benchmarks.md](relational/history/benchmarks.md) | [zig/RELATIONAL.md](../../zig/RELATIONAL.md) | Single-host LSM benchmark tables with repro commands |
| HTTP runtime | [http-runtime/implementation-checkpoint.md](../operations/http-runtime/history/implementation-checkpoint.md) | [zig/HTTP_API_RUNTIME.md](../../zig/HTTP_API_RUNTIME.md) | Route-by-route migration checkpoint |
| PDF | [pdf/render-control-verification-2026-09.md](pdf/history/render-control-verification-2026-09.md) | [zig/PDF.md](../../zig/PDF.md) | September 2026 render-control verification notes |
| PDF | [pdf/review-and-migration-ledger-2026.md](pdf/history/review-and-migration-ledger-2026.md) | [zig/PDF.md](../../zig/PDF.md) | Review-findings defect ledger (184 items) and migration/implementation-status ledger |
| PDF | [pdf/performance-and-ocr-verification-2026.md](pdf/history/performance-and-ocr-verification-2026.md) | [zig/PDF.md](../../zig/PDF.md) | Post-implementation performance-review change log and resident-OCR/scratch-retry/OCR-selection verification narrative |
| PDF | [pdf/shared-batching-and-runtime-notes-2026.md](pdf/history/shared-batching-and-runtime-notes-2026.md) | [BATCHING.md](../../zig/pkg/inference/BATCHING.md), [MANAGERS.md](../../zig/MANAGERS.md) | Former PDF.md shared model-forward batching, worker transport, maintenance scheduler, and lane-limit notes |
| Status | [status/dated-e2e-observations-2026-05.md](../operations/status/history/dated-e2e-observations-2026-05.md) | [zig/STATUS.md](../../zig/STATUS.md) | 2026-05-01 E2E observations |
| E2E | [e2e/resolved-failures-2026-05.md](../operations/e2e/history/resolved-failures-2026-05.md) | [zig/TODO.md](../../zig/TODO.md) | 2026-05-11 full-suite run and per-test resolutions |
| Inference | [inference/gemma4/a4b-perf.md](inference/history/gemma4/a4b-perf.md) | [CUDA.md](../../zig/pkg/inference/CUDA.md), [METAL.md](../../zig/pkg/inference/METAL.md) | Former PERF.md: Gemma 4 26B-A4B dated performance sessions |
| Inference | [inference/metal/status-history.md](inference/history/metal/status-history.md) | [METAL.md](../../zig/pkg/inference/METAL.md) | Metal backend bisection narrative and dated benchmark anchors |
| Inference | [inference/metal/slice-plans.md](inference/history/metal/slice-plans.md) | [METAL.md](../../zig/pkg/inference/METAL.md) | Four per-slice command-planner implementation plans |
| Inference | [inference/gemma4/metal-perf-plan.md](inference/history/gemma4/metal-perf-plan.md) | [GEMMA4.md](../../zig/pkg/inference/models/gemma4/GEMMA4.md#metal-performance-plan) | Plan §9–16 implementation and readiness ledgers |
| Inference | [inference/gemma4/mtp-cuda.md](inference/history/gemma4/mtp-cuda.md) | [GEMMA4.md](../../zig/pkg/inference/models/gemma4/GEMMA4.md) | MTP smoke transcripts and dated CUDA branch updates |
| Inference | [inference/gemma4/e2b-sm89.md](inference/history/gemma4/e2b-sm89.md) | [CUDA_TUNING.md](../../zig/pkg/inference/CUDA_TUNING.md) | E2B SM89 optimization status and split-KV validation |
| Inference | [inference/cuda/turboquant-l4.md](inference/history/cuda/turboquant-l4.md) | [CUDA.md](../../zig/pkg/inference/CUDA.md) | L4 TurboQuant qualification checklist |
| Inference | [inference/ggml-graph-execution-history.md](inference/history/ggml-graph-execution-history.md) | [GGML.md](../../zig/pkg/inference/GGML.md) | Partition executor and quant-matmul routing history |
| Inference | [inference/turboquant-history.md](inference/history/turboquant-history.md) | [TURBOQUANT.md](../../zig/pkg/inference/TURBOQUANT.md) | Compressed-KV history including the removed MLX provider |
| Inference | [inference/llms-plan.md](inference/history/llms-plan.md) | [LLMS.md](../../zig/pkg/inference/LLMS.md) | The original LLM plan: MLX-era status, KV cache design, delivery phases, and testing strategy |
| Inference | [inference/qwen/performance-evidence.md](inference/history/qwen/performance-evidence.md) | [PERFORMANCE.md](../../zig/pkg/inference/models/qwen/PERFORMANCE.md) | Dated campaigns, pass counts, and evidence-artifact ledger |
| Inference | [inference/qwen/qwen3vl-qualification-2026-08.md](inference/history/qwen/qwen3vl-qualification-2026-08.md) | [QWEN3VL.md](../../zig/pkg/inference/models/qwen/QWEN3VL.md) | August 2026 qualification runs |
| Inference | [inference/onnx-quantized-status-history.md](inference/history/onnx-quantized-status-history.md) | [ONNX.md](../../zig/pkg/inference/ONNX.md) | Quantized export proof runs and debugger bisection |
| Inference | [inference/finetuning-implementation-log.md](inference/history/finetuning-implementation-log.md) | [FINETUNING.md](../../zig/pkg/inference/finetuning/FINETUNING.md) | Session narrative and 37-item task changelog |
| Inference | [inference/graph-current-progress-history.md](inference/history/graph-current-progress-history.md) | [GRAPH.md](../../zig/pkg/inference/GRAPH.md) | Backend graph current-progress notes |
| Inference | [inference/wasm-status-history.md](inference/history/wasm-status-history.md) | [WASM.md](../../zig/pkg/inference/WASM.md) | Build-profile and GPU-resident-weight status bullets |
| Inference | [inference/ml-graph-ir-proposal.md](inference/history/ml-graph-ir-proposal.md) | [GRAPH.md](../../zig/pkg/inference/GRAPH.md) | Former ML.md: the superseded computation-graph IR proposal |
| Inference | [inference/pjrt-status-history.md](inference/history/pjrt-status-history.md) | [PJRT.md](../../zig/pkg/inference/PJRT.md) | PJRT whole-model artifact status bullets |
| Inference | [inference/gliner2/cuda-qualification-2026-07.md](inference/history/gliner2/cuda-qualification-2026-07.md) | [gliner2/CUDA.md](../../zig/pkg/inference/models/gliner2/CUDA.md) | July 2026 GLiNER2 CUDA environment, results, and route evidence |
| Inference | [inference/gliner2/gliner25-base-qualification-2026-09.md](inference/history/gliner2/gliner25-base-qualification-2026-09.md) | [GLINER25.md](../../zig/pkg/inference/models/gliner2/GLINER25.md) | GLiNER2.5 base qualification sections 2–15: correctness, geometry, long-document probe sweeps and throughput, batching incident, fp16 qualification |
| Inference | [inference/gliner2/gliner25-family-metal-followups-2026-10.md](inference/history/gliner2/gliner25-family-metal-followups-2026-10.md) | [GLINER25_FAMILY.md](../../zig/pkg/inference/models/gliner2/GLINER25_FAMILY.md#performance) | 2026-10-06 Metal packed-QKV/fused-FFN measurements and 2026-10-07 Decide-1B production-allocator exploratory comparison |
| Inference | [inference/gliner2/gliner25-cpu-benchmark-runs.md](inference/history/gliner2/gliner25-cpu-benchmark-runs.md) | [scripts/gliner25/BENCHMARK.md](../../zig/pkg/inference/scripts/gliner25/BENCHMARK.md) | 2026-09-09 macOS CPU run and 2026-10-01 Linux x86 diagnostic |
| Inference | [inference/gliner2/gliner25-cuda-l4-campaigns-2026-09.md](inference/history/gliner2/gliner25-cuda-l4-campaigns-2026-09.md) | [scripts/gliner25/CUDA.md](../../zig/pkg/inference/scripts/gliner25/CUDA.md) | L4 training v36–v39 runs, 2026-09-14 validation, inference and batch campaigns |
| Inference | [inference/gliner2/cuda-family-qualification.md](inference/history/gliner2/cuda-family-qualification.md) | [CUDA.md](../../zig/pkg/inference/CUDA.md#gliner25-multi-and-decide-1b) | GLiNER2.5 Multi/Decide-1B L4 campaigns, holdout match counts, and Q8 mirror long-context tables |
| Inference | [inference/metal/ggml-checklist-and-phases.md](inference/history/metal/ggml-checklist-and-phases.md) | [METAL.md](../../zig/pkg/inference/METAL.md) | Completed ggml-shaped checklist items, operator-family and Phase 1–6 ledgers, and dated performance diagnosis |
| Inference | [inference/metal/serving-defaults-m4-2026-10.md](inference/history/metal/serving-defaults-m4-2026-10.md) | [METAL.md](../../zig/pkg/inference/METAL.md#m4-serving-defaults) | 2026-10-02 Apple M4 Qwen3/Gemma4 E2B serving-default qualification receipt |
| Inference | [inference/gemma4/metal-perf-plan-phases.md](inference/history/gemma4/metal-perf-plan-phases.md) | [GEMMA4.md](../../zig/pkg/inference/models/gemma4/GEMMA4.md#metal-performance-plan) | Original 2026-08-26 Metal performance plan §1–8 (gap decomposition, Phase 0–4, process review) |
| Inference | [inference/antenna/2026-10-03-status.md](inference/history/antenna/2026-10-03-status.md) | [ANTENNA.md](../../zig/pkg/inference/models/antenna/ANTENNA.md#status) | Steps 0–2 dated status narrative (baselines, pilot collapse, native distillation, decision head, embeddings, fused attention) |
| Inference | [inference/qwen/qwen3-embedding-baseline-results.md](inference/history/qwen/qwen3-embedding-baseline-results.md) | [PERFORMANCE.md](../../zig/pkg/inference/models/qwen/PERFORMANCE.md#qwen3-embedding-reference-results) | Qwen3-Embedding M4 Q8_0 dated baseline, parity-plan, and gap-closure results (2026-09/10) |
| Audio | [audio/benchmark-baseline-2026-04.md](audio/history/benchmark-baseline-2026-04.md) | [AUDIO.md](../../zig/lib/audio/AUDIO.md) | 2026-04-14 single-run codec benchmark baseline |
| Compilation | [compilation/experiment-ledger-2026-08-to-10.md](compilation/history/experiment-ledger-2026-08-to-10.md) | [zig/COMPILATION.md](../../zig/COMPILATION.md) | Former COMPILATION_EXPERIMENTS.md: phase-by-phase compilation-unit experiments, rejected probes, and runner measurements through 2026-10-11 |
| SQL | [sql/reconciliation-and-adjudication-2026-10.md](sql/history/reconciliation-and-adjudication-2026-10.md) | [extraction.md](sql/extraction.md) | Capability reconciliation queue counts and the exact-source adjudication batch |
| SQL | [sql/lake-integration-log.md](sql/history/lake-integration-log.md) | [extraction.md](sql/extraction.md), [REMOTE_TABLE_SERVING.md](../../zig/REMOTE_TABLE_SERVING.md) | External lake SQL, lake index publication and cold-cache evidence |
| SQL | [sql/pre-merge-activation-2026-09.md](sql/history/pre-merge-activation-2026-09.md) | [extraction.md](sql/extraction.md) | Pre-merge FK/TRUNCATE/retirement activation runs and test totals |
| SQL | [sql/integration-status-log.md](sql/history/integration-status-log.md) | [extraction.md](sql/extraction.md), [ordered-artifacts.md](ordered-artifacts.md#distributed-online-merge) | Integration status, sessions, MERGE, TRUNCATE and online-merge narrative with per-case evidence |
| SQL | [sql/validation-checkpoints.md](sql/history/validation-checkpoints.md) | [extraction.md](sql/extraction.md) | Local validation checkpoints, initial slice and initial measurements |
| SQL | [sql/typed-values-log-2026-10.md](sql/history/typed-values-log-2026-10.md) | [extraction.md](sql/extraction.md) | Typed rows, arrays, precise types and exact NUMERIC increments with validation receipts |
| SQL | [sql/regex-implementation-log.md](sql/history/regex-implementation-log.md) | [regex-parity.md](sql/regex-parity.md) | Removed C-engine backend foundation, scalar activation and qualification progress |
| SQL | [sql/parity-inventory-batches.md](sql/history/parity-inventory-batches.md) | [parity-inventory.md](sql/parity-inventory.md) | Campaign batch narratives and per-case evidence prose |
| Ordered artifacts | [ordered-artifacts/activation-gate-log.md](ordered-artifacts/history/activation-gate-log.md) | [ordered-artifacts.md](ordered-artifacts.md) | Asynchronous-artifact protocol implementation log behind the activation gate |
| System catalog | [relational/namespace-relation-ownership-log.md](relational/history/namespace-relation-ownership-log.md) | [SYSTEM_CATALOG.md](../../zig/SYSTEM_CATALOG.md#namespace-relation-ownership) | Relation-name publication, reconciliation and coordinator log |


## Additional records

- [Gemma4 media throughput](inference/history/gemma4/media-throughput-2026-10.md) — residency, image/audio embeddings, PDF painter and JPEG/WebP qualification; current contracts in [BATCHING.md](../../zig/pkg/inference/BATCHING.md#gpu-residency-for-media-embeddings).
- [Gemma4 OCR throughput](inference/history/gemma4/ocr-throughput-2026-10.md) — rejected matrix/MTP trials, paragraph omission correction and canonical cohort gains; current [OCR design](inference/gemma4-ocr-optimization.md).

- [Apache Lite and inference licensing](../reference/licensing/history/apache2-lite-inference-2026-09.md) — implementation, review, packaging, and WASM validation.
- [Laya qualification](inference/history/laya/qualification.md) — measured CPU/Metal accuracy and performance.
- [Laya investigation](inference/history/laya/support-investigation.md) — original placement decisions and implementation follow-up.
- [Laya implementation logs](inference/history/laya/README.md) — dated training and runtime evidence.
- [Antenna implementation logs](inference/history/antenna/README.md) — baselines, pilot, and native distillation evidence.
- [Relational indexes extraction](relational/history/indexes-extraction.md) — extraction ledger; current restore design is [here](relational-restore-architecture.md).
