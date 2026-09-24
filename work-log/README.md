# Antfly Work Log

This directory tracks major features and architectural changes in Antfly. Each document preserves design decisions and implementation context for future reference.

Go-era design documents whose content now lives in the Zig design docs were removed from this directory; see [`zig/ROADMAP.md`](../zig/ROADMAP.md) as the index for those.

## Completed Features

### Ingestion

| Feature | Document | Summary |
|---------|----------|---------|
| DOCX/PPTX & Google Docs/Slides Support | [ppt-docx.md](completed/ingestion/ppt-docx.md) | Structured extraction for Office and Google Workspace document formats in docsaf, using only the standard library |
| Reader Interface (OCR/Vision) | [reader-integration.md](completed/ingestion/reader-integration.md) | A reusable `Reader` interface for OCR/vision integrations, replacing ad-hoc per-app implementations |

### Relocated Implementation Logs

Dated implementation logs, defect ledgers, benchmark diaries, and roadmap
restatements moved out of the Zig design docs on 2026-09-16. Each file is a
verbatim copy of the range it replaced; durable decisions were folded into the
design doc first, and the design doc keeps a pointer. The placement rule is
recorded under Planning Rules in [`zig/ROADMAP.md`](../zig/ROADMAP.md).

| Area | Document | Design doc | Summary |
|------|----------|------------|---------|
| VOPR | [vopr/status-history.md](completed/vopr/status-history.md) | [zig/VOPR.md](../zig/VOPR.md) | Scenario-version status preamble (v9–v53) and verification-audit narrative |
| VOPR | [vopr/defects-found.md](completed/vopr/defects-found.md) | [zig/VOPR.md](../zig/VOPR.md) | Per-defect ledger of production and harness bugs VOPR found |
| VOPR | [vopr/follow-ups-2026-09.md](completed/vopr/follow-ups-2026-09.md) | [zig/VOPR.md](../zig/VOPR.md) | Dated runtime-correctness, merge, and deadline follow-ups (2026-09-06/07) |
| Vector store | [vector-store/experiments-2026-09.md](completed/vector-store/experiments-2026-09.md) | [zig/VECTOR_STORE.md](../zig/VECTOR_STORE.md) | September 2026 experiment write-ups and 1M qualification tables |
| Full text | [full-text/implementation-progress-2026-07.md](completed/full-text/implementation-progress-2026-07.md) | [zig/FULL_TEXT.md](../zig/FULL_TEXT.md) | v29–v38 posting-format increments and kernel qualification (2026-07) |
| Graph metrics | [graph-metrics/roadmap-restatements.md](completed/graph-metrics/roadmap-restatements.md) | [zig/GRAPH_METRICS.md](../zig/GRAPH_METRICS.md) | Per-phase progress blocks and successive remaining-roadmap restatements |
| Derived documents | [derived-documents/implementation-status-history.md](completed/derived-documents/implementation-status-history.md) | [zig/DERIVED_DOCUMENT_HIERARCHY.md](../zig/DERIVED_DOCUMENT_HIERARCHY.md) | Per-phase implementation-status bullets |
| LSM writes | [lsm-writes/follow-ups-2026-04.md](completed/lsm-writes/follow-ups-2026-04.md) | [WRITES.md](../zig/pkg/antfly/src/storage/lsm/WRITES.md) | 2026-04-16 write-amplification follow-ups and execution checklist |
| LSM writes | [lsm-writes/baseline-evidence-2026-06.md](completed/lsm-writes/baseline-evidence-2026-06.md) | [LSM.md](../zig/pkg/antfly/src/storage/lsm/LSM.md) | Sampled ns/op baseline (2026-06-02) |
| LSM publication | [lsm-version-publication/validation-2026-09.md](completed/lsm-version-publication/validation-2026-09.md) | [lsm-version-publication.md](../docs/design/lsm-version-publication.md) | Dated validation runs and local ReleaseFast measurements |
| DOCID | [docid/query-bench-diary.md](completed/docid/query-bench-diary.md) | [zig/DOCID.md](../zig/DOCID.md) | Query and bulk-load optimization diary |
| Algebraic | [algebraic/churn-benchmarks-2026-05.md](completed/algebraic/churn-benchmarks-2026-05.md) | [zig/ALGEBRAIC.md](../zig/ALGEBRAIC.md) | May 2026 churn smoke and microbench chain |
| Relational | [relational/benchmarks.md](completed/relational/benchmarks.md) | [zig/RELATIONAL.md](../zig/RELATIONAL.md) | Single-host LSM benchmark tables with repro commands |
| HTTP runtime | [http-runtime/implementation-checkpoint.md](completed/http-runtime/implementation-checkpoint.md) | [zig/HTTP_API_RUNTIME.md](../zig/HTTP_API_RUNTIME.md) | Route-by-route migration checkpoint |
| PDF | [pdf/render-control-verification-2026-09.md](completed/pdf/render-control-verification-2026-09.md) | [zig/PDF.md](../zig/PDF.md) | September 2026 render-control verification notes |
| Status | [status/dated-e2e-observations-2026-05.md](completed/status/dated-e2e-observations-2026-05.md) | [zig/STATUS.md](../zig/STATUS.md) | 2026-05-01 E2E observations |
| E2E | [e2e/resolved-failures-2026-05.md](completed/e2e/resolved-failures-2026-05.md) | [zig/TODO.md](../zig/TODO.md) | 2026-05-11 full-suite run and per-test resolutions |
| Inference | [inference/gemma4/a4b-perf.md](completed/inference/gemma4/a4b-perf.md) | [CUDA.md](../zig/pkg/inference/CUDA.md), [METAL.md](../zig/pkg/inference/METAL.md) | Former PERF.md: Gemma 4 26B-A4B dated performance sessions |
| Inference | [inference/metal/status-history.md](completed/inference/metal/status-history.md) | [METAL.md](../zig/pkg/inference/METAL.md) | Metal backend bisection narrative and dated benchmark anchors |
| Inference | [inference/metal/slice-plans.md](completed/inference/metal/slice-plans.md) | [METAL.md](../zig/pkg/inference/METAL.md) | Four per-slice command-planner implementation plans |
| Inference | [inference/gemma4/metal-perf-plan.md](completed/inference/gemma4/metal-perf-plan.md) | [GEMMA4.md](../zig/pkg/inference/models/gemma4/GEMMA4.md#metal-performance-plan) | Plan §9–16 implementation and readiness ledgers |
| Inference | [inference/gemma4/mtp-cuda.md](completed/inference/gemma4/mtp-cuda.md) | [GEMMA4.md](../zig/pkg/inference/models/gemma4/GEMMA4.md) | MTP smoke transcripts and dated CUDA branch updates |
| Inference | [inference/gemma4/e2b-sm89.md](completed/inference/gemma4/e2b-sm89.md) | [CUDA_TUNING.md](../zig/pkg/inference/CUDA_TUNING.md) | E2B SM89 optimization status and split-KV validation |
| Inference | [inference/cuda/turboquant-l4.md](completed/inference/cuda/turboquant-l4.md) | [CUDA.md](../zig/pkg/inference/CUDA.md) | L4 TurboQuant qualification checklist |
| Inference | [inference/ggml-graph-execution-history.md](completed/inference/ggml-graph-execution-history.md) | [GGML.md](../zig/pkg/inference/GGML.md) | Partition executor and quant-matmul routing history |
| Inference | [inference/turboquant-history.md](completed/inference/turboquant-history.md) | [TURBOQUANT.md](../zig/pkg/inference/TURBOQUANT.md) | Compressed-KV history including the removed MLX provider |
| Inference | [inference/llms-plan.md](completed/inference/llms-plan.md) | [LLMS.md](../zig/pkg/inference/LLMS.md) | The original LLM plan: MLX-era status, KV cache design, delivery phases, and testing strategy |
| Inference | [inference/qwen/performance-evidence.md](completed/inference/qwen/performance-evidence.md) | [PERFORMANCE.md](../zig/pkg/inference/models/qwen/PERFORMANCE.md) | Dated campaigns, pass counts, and evidence-artifact ledger |
| Inference | [inference/qwen/qwen3vl-qualification-2026-08.md](completed/inference/qwen/qwen3vl-qualification-2026-08.md) | [QWEN3VL.md](../zig/pkg/inference/models/qwen/QWEN3VL.md) | August 2026 qualification runs |
| Inference | [inference/onnx-quantized-status-history.md](completed/inference/onnx-quantized-status-history.md) | [ONNX.md](../zig/pkg/inference/ONNX.md) | Quantized export proof runs and debugger bisection |
| Inference | [inference/finetuning-implementation-log.md](completed/inference/finetuning-implementation-log.md) | [FINETUNING.md](../zig/pkg/inference/finetuning/FINETUNING.md) | Session narrative and 37-item task changelog |
| Inference | [inference/graph-current-progress-history.md](completed/inference/graph-current-progress-history.md) | [GRAPH.md](../zig/pkg/inference/GRAPH.md) | Backend graph current-progress notes |
| Inference | [inference/wasm-status-history.md](completed/inference/wasm-status-history.md) | [WASM.md](../zig/pkg/inference/WASM.md) | Build-profile and GPU-resident-weight status bullets |
| Inference | [inference/ml-graph-ir-proposal.md](completed/inference/ml-graph-ir-proposal.md) | [GRAPH.md](../zig/pkg/inference/GRAPH.md) | Former ML.md: the superseded computation-graph IR proposal |
| Inference | [inference/pjrt-status-history.md](completed/inference/pjrt-status-history.md) | [PJRT.md](../zig/pkg/inference/PJRT.md) | PJRT whole-model artifact status bullets |
| Inference | [inference/gliner2/cuda-qualification-2026-07.md](completed/inference/gliner2/cuda-qualification-2026-07.md) | [gliner2/CUDA.md](../zig/pkg/inference/models/gliner2/CUDA.md) | July 2026 GLiNER2 CUDA environment, results, and route evidence |
| Audio | [audio/benchmark-baseline-2026-04.md](completed/audio/benchmark-baseline-2026-04.md) | [AUDIO.md](../zig/lib/audio/AUDIO.md) | 2026-04-14 single-run codec benchmark baseline |

## Planned Features

| Feature | Document | Summary |
|---------|----------|---------|
| Agentic Warehouse Memory | [agentic-warehouse-memory.md](planned/agentic-warehouse-memory.md) | Antfly as an agentic memory layer over BigQuery/Snowflake warehouses |
| Operator Standalone Mode | [operator-standalone-mode.md](planned/operator-standalone-mode.md) | Explicit operator-managed standalone mode for `AntflyCluster`, without breaking the existing clustered topology |
| Pipelined Query API | [pipelined-query-api.md](planned/pipelined-query-api.md) | Multi-stage query pipelines supporting delete-by-query, update-by-query, and cross-table joins |
| Query Sampler Feature | [query-samplers.md](planned/query-samplers.md) | Named query samplers that capture query embeddings and results for ML training |

## Quick Links

- **Completed Features**: [completed/](completed/)
- **Planned Features**: [planned/](planned/)
- **Main Documentation**: [../CLAUDE.md](../CLAUDE.md)
- **API Specification**: [../specs/openapi/](../specs/openapi/)
