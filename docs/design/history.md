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
restatements moved out of the Zig design docs on 2026-09-16. The original
narrative is retained, with links updated for the consolidated documentation
layout. Durable decisions were folded into the design doc first,
and the design doc keeps a pointer. The placement rule is
recorded under Planning Rules in [`zig/ROADMAP.md`](../../zig/ROADMAP.md).

| Area | Document | Design doc | Summary |
|------|----------|------------|---------|
| VOPR | [vopr/status-history.md](vopr/history/status-history.md) | [zig/VOPR.md](../../zig/VOPR.md) | Scenario-version status preamble (v9–v53) and verification-audit narrative |
| VOPR | [vopr/defects-found.md](vopr/history/defects-found.md) | [zig/VOPR.md](../../zig/VOPR.md) | Per-defect ledger of production and harness bugs VOPR found |
| VOPR | [vopr/follow-ups-2026-09.md](vopr/history/follow-ups-2026-09.md) | [zig/VOPR.md](../../zig/VOPR.md) | Dated runtime-correctness, merge, and deadline follow-ups (2026-09-06/07) |
| Vector store | [vector-store/experiments-2026-09.md](vector-store/history/experiments-2026-09.md) | [zig/VECTOR_STORE.md](../../zig/VECTOR_STORE.md) | September 2026 experiment write-ups and 1M qualification tables |
| Full text | [full-text/implementation-progress-2026-07.md](full-text/history/implementation-progress-2026-07.md) | [zig/FULL_TEXT.md](../../zig/FULL_TEXT.md) | v29–v38 posting-format increments and kernel qualification (2026-07) |
| Graph metrics | [graph-metrics/roadmap-restatements.md](graph-metrics/history/roadmap-restatements.md) | [zig/GRAPH_METRICS.md](../../zig/GRAPH_METRICS.md) | Per-phase progress blocks and successive remaining-roadmap restatements |
| Derived documents | [derived-documents/implementation-status-history.md](derived-documents/history/implementation-status-history.md) | [zig/DERIVED_DOCUMENT_HIERARCHY.md](../../zig/DERIVED_DOCUMENT_HIERARCHY.md) | Per-phase implementation-status bullets |
| LSM writes | [lsm-writes/follow-ups-2026-04.md](lsm-writes/history/follow-ups-2026-04.md) | [WRITES.md](../../zig/pkg/antfly/src/storage/lsm/WRITES.md) | 2026-04-16 write-amplification follow-ups and execution checklist |
| LSM writes | [lsm-writes/baseline-evidence-2026-06.md](lsm-writes/history/baseline-evidence-2026-06.md) | [LSM.md](../../zig/pkg/antfly/src/storage/lsm/LSM.md) | Sampled ns/op baseline (2026-06-02) |
| LSM publication | [lsm-version-publication/validation-2026-09.md](lsm-version-publication/history/validation-2026-09.md) | [lsm-version-publication.md](lsm-version-publication.md) | Dated validation runs and local ReleaseFast measurements |
| DOCID | [docid/query-bench-diary.md](docid/history/query-bench-diary.md) | [zig/DOCID.md](../../zig/DOCID.md) | Query and bulk-load optimization diary |
| Algebraic | [algebraic/churn-benchmarks-2026-05.md](algebraic/history/churn-benchmarks-2026-05.md) | [zig/ALGEBRAIC.md](../../zig/ALGEBRAIC.md) | May 2026 churn smoke and microbench chain |
| Relational | [relational/benchmarks.md](relational/history/benchmarks.md) | [zig/RELATIONAL.md](../../zig/RELATIONAL.md) | Single-host LSM benchmark tables with repro commands |
| HTTP runtime | [http-runtime/implementation-checkpoint.md](../operations/http-runtime/history/implementation-checkpoint.md) | [zig/HTTP_API_RUNTIME.md](../../zig/HTTP_API_RUNTIME.md) | Route-by-route migration checkpoint |
| PDF | [pdf/render-control-verification-2026-09.md](pdf/history/render-control-verification-2026-09.md) | [zig/PDF.md](../../zig/PDF.md) | September 2026 render-control verification notes |
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
| Audio | [audio/benchmark-baseline-2026-04.md](audio/history/benchmark-baseline-2026-04.md) | [AUDIO.md](../../zig/lib/audio/AUDIO.md) | 2026-04-14 single-run codec benchmark baseline |


## Additional records

- [Apache Lite and inference licensing](../reference/licensing/history/apache2-lite-inference-2026-09.md) — implementation, review, packaging, and WASM validation.
- [Laya qualification](inference/history/laya/qualification.md) — measured CPU/Metal accuracy and performance.
- [Laya investigation](inference/history/laya/support-investigation.md) — original placement decisions and implementation follow-up.
- [Relational indexes extraction](relational/history/indexes-extraction.md) — extraction ledger; current restore design is [here](relational-restore-architecture.md).
