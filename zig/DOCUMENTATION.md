# Zig Documentation Index

This is an index of first-party Markdown documentation under `zig/` and its
subpackages. Generated outputs, virtualenv/cache files, vendored license docs,
and fixture/testdata README files are intentionally omitted.

Use [README.md](README.md) for repository layout and day-to-day build commands.
Use [ROADMAP.md](ROADMAP.md) for the top-level execution map.

## Top-Level Guides

- [README.md](README.md) - Zig tree overview, package layout, common builds, and
  test entry points.
- [ROADMAP.md](ROADMAP.md) - Top-level execution map, major project lanes, and
  current test target split.
- [TODO.md](TODO.md) - Live bug list, parity gaps, and short-term follow-up
  work.
- [TESTING.md](TESTING.md) - Testing strategy, focused build targets, and local
  verification guidance.
- [ZIG.md](ZIG.md) - Zig language and toolchain notes used by this tree.
- [STATUS.md](STATUS.md) - Status subsystem design and runtime status shape.
- [STATUS_API.md](STATUS_API.md) - HTTP/API surface for status reporting.

## AntflyDB Product And API

- [A2A.md](A2A.md) - Native agents and Agent-to-Agent integration design.
- [ARD.md](ARD.md) - Agentic resource discovery design.
- [ARTIFACTS.md](ARTIFACTS.md) - Artifacts and enrichments API contract.
- [AUTH.md](AUTH.md) - Authentication and authorization design notes.
- [BACKUPS.md](BACKUPS.md) - Backup, restore, and snapshot plan.
- [CAPI.md](CAPI.md) - Antfly C API surface and embedding contract.
- [CDC.md](CDC.md) - Change data capture plan.
- [CONNECTIONS.md](CONNECTIONS.md) - Connection handling and client-facing
  connection semantics.
- [DATABASES.md](DATABASES.md) - Database, namespace, and table model.
- [DATA_DIR.md](DATA_DIR.md) - Local data directory layout.
- [EXTENSIONS.md](EXTENSIONS.md) - Postgres-style extension system design.
- [EXTRACT.md](EXTRACT.md) - Extraction API design.
- [GROUPS.md](GROUPS.md) - Group id model and group-aware routing notes.
- [HA.md](HA.md) - Hot standby WAL replication and HA ownership semantics.
- [LAKES.md](LAKES.md) - Lake query mode and lake-oriented execution surface.
- [LITE.md](LITE.md) - Antfly Lite mode, local file behavior, and embedded use.
- [MCP.md](MCP.md) - Zig MCP support.
- [METRICS.md](METRICS.md) - Metrics and profiling surfaces.
- [OPENAPI.md](OPENAPI.md) - OpenAPI generation and API documentation flow.
- [QUERY_BUILDER.md](QUERY_BUILDER.md) - Query builder agent design.
- [SCALING.md](SCALING.md) - Scaling, node shutdown, and distributed runtime
  behavior.
- [SECRETS.md](SECRETS.md) - Zig secrets store.
- [SERVERLESS.md](SERVERLESS.md) - Serverless execution plan.
- [SIM.md](SIM.md) - Product simulation testing plan.
- [STARTUP.md](STARTUP.md) - Startup status and provisioning behavior.
- [SWARM.md](SWARM.md) - Swarm runtime, providers, and shard DB access.
- [WEBSEARCH.md](WEBSEARCH.md) - Web search support and integration notes.

## DB, Storage, Query, And Indexing

- [ALGEBRAIC.md](ALGEBRAIC.md) - Algebraic sparse-token database theory,
  implementation state, guardrails, and production-hardening roadmap.
- [BATCH.md](BATCH.md) - Batch coalescing, bulk ingest scope, and write-path
  replay-window policy.
- [DB.md](DB.md) - DB-layer contract, storage backend boundary, and roadmap.
- [DERIVED_DOCUMENT_HIERARCHY.md](DERIVED_DOCUMENT_HIERARCHY.md) - Derived
  document hierarchy and generated document ownership.
- [DOCID.md](DOCID.md) - Document id, posting id, and doc identity semantics.
- [ENRICHMENTS.md](ENRICHMENTS.md) - Storage-side enrichment architecture and
  artifact identity contract.
- [FOREIGN_KEYS.md](FOREIGN_KEYS.md) - Foreign key behavior, integrity checks,
  and repair/action planning.
- [FULL_TEXT.md](FULL_TEXT.md) - Full-text indexing, visibility, and merge
  maintenance plan.
- [GRAPH.md](GRAPH.md) - Graph indexing design.
- [HBC.md](HBC.md) - HBC dense indexing, search/rerank behavior, and DB
  integration.
- [INFLIGHT.md](INFLIGHT.md) - Inflight batching plan.
- [JOINS.md](JOINS.md) - Join planning and execution notes.
- [KMEANS.md](KMEANS.md) - K-way k-means bulk build plan.
- [QUERY_STRING.md](QUERY_STRING.md) - Query string language syntax and
  semantics.
- [RELATIONAL.md](RELATIONAL.md) - Relational mode, SQL-facing relational
  syntax, and DB integration.
- [RESOLUTION.md](RESOLUTION.md) - Entity resolution design.
- [SCHEMA.md](SCHEMA.md) - Schema model and schema evolution behavior.
- [SPFRESH.md](SPFRESH.md) - SPFresh-style HBC refactor plan.
- [SQL.md](SQL.md) - SQL adapter design and parser/binder integration.

## Compatibility, E2E, And Specs

- [compat/README.md](compat/README.md) - Compatibility corpus and comparison
  harnesses.
- [compat/public_swarm/README.md](compat/public_swarm/README.md) - Public swarm
  search compatibility suite.
- [e2e/antfly/README.md](e2e/antfly/README.md) - Python E2E coverage for the
  Antfly product surface.
- [specs/tla/README.md](specs/tla/README.md) - TLA+ formal specifications and
  model-checking inputs.

## Shared Libraries

- [docs/ML.md](docs/ML.md) - Traditional ML inference notes shared by the Zig
  tree.
- [lib/audio/AUDIO.md](lib/audio/AUDIO.md) - Audio decoding, PCM boundary, and
  audio feature plan.
- [lib/audio/e2e/README.md](lib/audio/e2e/README.md) - Audio end-to-end test
  guidance.
- [lib/httpx/README.md](lib/httpx/README.md) - HTTP client/server helper
  library.
- [lib/image/IMAGE.md](lib/image/IMAGE.md) - Image decoding, encoding, and
  preprocessing support.
- [lib/image/e2e/README.md](lib/image/e2e/README.md) - Image end-to-end test
  guidance.
- [lib/image/src/jpeg2000/J2K.md](lib/image/src/jpeg2000/J2K.md) - JPEG 2000
  native conformance tracker.
- [lib/json/JSON.md](lib/json/JSON.md) - JSON parser/serializer plan.
- [lib/objectstore/README.md](lib/objectstore/README.md) - Object storage
  abstraction.
- [lib/openapi/INIT.md](lib/openapi/INIT.md) - OpenAPI 3.0.x to Zig code
  generator.
- [lib/raft/RAFT.md](lib/raft/RAFT.md) - Reusable raft library design.
- [lib/raft/ROADMAP.md](lib/raft/ROADMAP.md) - Reusable raft library roadmap.
- [lib/regex/REGEX.md](lib/regex/REGEX.md) - Regex library notes.

## Antfly Package Internals

- [pkg/antfly-embedded/WASM.md](pkg/antfly-embedded/WASM.md) - Embedded Antfly
  WASM support.
- [pkg/antfly/src/api/README.md](pkg/antfly/src/api/README.md) - API layer
  package overview.
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md) - Public/API-layer
  plan and DB boundary notes.
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md) - Zig LMDB engine
  design, verification, and performance roadmap.
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md) -
  Metadata control plane design.
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md) - Antfly raft
  integration.
- [pkg/antfly/src/sql/grammar/GRAMMAR.md](pkg/antfly/src/sql/grammar/GRAMMAR.md)
  - SQL grammar migration and generated parser plan.
- [pkg/antfly/src/storage/SIM.md](pkg/antfly/src/storage/SIM.md) - Storage
  simulation workflow.
- [pkg/antfly/src/storage/db/README.md](pkg/antfly/src/storage/db/README.md) -
  DB package implementation contract, module map, and cross-cutting semantics.
- [pkg/antfly/src/storage/lsm/CACHE.md](pkg/antfly/src/storage/lsm/CACHE.md) -
  LSM cache plan.
- [pkg/antfly/src/storage/lsm/LSM.md](pkg/antfly/src/storage/lsm/LSM.md) - LSM
  backend performance plan.
- [pkg/antfly/src/storage/lsm/READS.md](pkg/antfly/src/storage/lsm/READS.md) -
  LSM, HBC, and full-text read performance.
- [pkg/antfly/src/storage/lsm/WRITES.md](pkg/antfly/src/storage/lsm/WRITES.md)
  - LSM, HBC, and full-text write performance.

## Inference Package

- [pkg/inference/BITNET.md](pkg/inference/BITNET.md) - BitNet support.
- [pkg/inference/BUDGETS.md](pkg/inference/BUDGETS.md) - Inference budget and
  memory planning.
- [pkg/inference/CUDA.md](pkg/inference/CUDA.md) - NVIDIA/CUDA inference plan.
- [pkg/inference/GEMMA4.md](pkg/inference/GEMMA4.md) - Gemma 4 model support.
- [pkg/inference/GGML.md](pkg/inference/GGML.md) - GGML reference and
  quantization support.
- [pkg/inference/GGML_PLAN.md](pkg/inference/GGML_PLAN.md) - GGML-style graph
  execution plan.
- [pkg/inference/GRAPH.md](pkg/inference/GRAPH.md) - Inference graph IR.
- [pkg/inference/KVCACHE.md](pkg/inference/KVCACHE.md) - KV cache design.
- [pkg/inference/LLMS.md](pkg/inference/LLMS.md) - LLM runtime plan.
- [pkg/inference/METAL.md](pkg/inference/METAL.md) - Apple Metal inference
  backend plan.
- [pkg/inference/ML.md](pkg/inference/ML.md) - XLA-like computation graph IR.
- [pkg/inference/MULTIDEVICE.md](pkg/inference/MULTIDEVICE.md) - Multi-device
  inference design.
- [pkg/inference/NATIVE.md](pkg/inference/NATIVE.md) - Native CPU backend.
- [pkg/inference/NVME.md](pkg/inference/NVME.md) - NVMe weight tiering plan.
- [pkg/inference/ONNX.md](pkg/inference/ONNX.md) - ONNX import/runtime support.
- [pkg/inference/OPENAI.md](pkg/inference/OPENAI.md) - OpenAI-compatible
  inference API behavior.
- [pkg/inference/PARITY.md](pkg/inference/PARITY.md) - Inference parity status.
- [pkg/inference/PERF.md](pkg/inference/PERF.md) - Gemma 4 performance analysis.
- [pkg/inference/PJRT.md](pkg/inference/PJRT.md) - TPU/PJRT backend plan.
- [pkg/inference/QUANTIZE.md](pkg/inference/QUANTIZE.md) - Quantization layout.
- [pkg/inference/QWEN3_5.md](pkg/inference/QWEN3_5.md) - Qwen3.5 / Chandra OCR
  2 support.
- [pkg/inference/READERS.md](pkg/inference/READERS.md) - Reader model parity.
- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md) - Inference package
  roadmap.
- [pkg/inference/TODO.md](pkg/inference/TODO.md) - Inference package TODOs.
- [pkg/inference/TOOL_CALLING.md](pkg/inference/TOOL_CALLING.md) - Tool calling
  design.
- [pkg/inference/TURBOQUANT.md](pkg/inference/TURBOQUANT.md) - TurboQuant plan.
- [pkg/inference/UNSLOTH.md](pkg/inference/UNSLOTH.md) - Unsloth dynamic GGUF
  support.
- [pkg/inference/WASM.md](pkg/inference/WASM.md) - WASM and WebGPU backend.
- [pkg/inference/src/api/README.md](pkg/inference/src/api/README.md) -
  Inference OpenAPI generated source notes.

## Inference Task And Fine-Tuning Docs

- [pkg/inference/docs/NATIVE_MODELS.md](pkg/inference/docs/NATIVE_MODELS.md) -
  Native model runtime notes.
- [pkg/inference/docs/RERANKING.md](pkg/inference/docs/RERANKING.md) -
  Reranking support.
- [pkg/inference/docs/TASKS.md](pkg/inference/docs/TASKS.md) - Model task
  taxonomy.
- [pkg/inference/docs/finetuning/CLI.md](pkg/inference/docs/finetuning/CLI.md)
  - Fine-tuning CLI refactor.
- [pkg/inference/docs/finetuning/ENTITY_CLEANUP.md](pkg/inference/docs/finetuning/ENTITY_CLEANUP.md)
  - Learned entity cleanup.
- [pkg/inference/docs/finetuning/FINETUNING.md](pkg/inference/docs/finetuning/FINETUNING.md)
  - Fine-tuning workflows.
- [pkg/inference/docs/finetuning/GEMMA4.md](pkg/inference/docs/finetuning/GEMMA4.md)
  - Gemma4 single-device pilot.
- [pkg/inference/docs/finetuning/GLINER2.md](pkg/inference/docs/finetuning/GLINER2.md)
  - GLiNER2 fine-tuning state.
- [pkg/inference/docs/finetuning/PEFT.md](pkg/inference/docs/finetuning/PEFT.md)
  - PEFT support.
- [pkg/inference/docs/finetuning/RECURSIVE_LORA.md](pkg/inference/docs/finetuning/RECURSIVE_LORA.md)
  - Recursive LoRA compression plan.
