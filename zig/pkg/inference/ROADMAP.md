# antfly-inference-zig Roadmap

antfly-inference-zig is Antfly's Zig ML inference runtime: embeddings,
reranking, extraction/classification, chunking, rewriting, reading (OCR/VLM),
transcription, and generation across native CPU, Metal, and CUDA backends (ONNX
models run on Antfly's own ONNX implementation), plus LoRA/PEFT training. This file tracks open work only; how things
work today lives in the design docs linked below.

The HTTP surface is generated from
[specs/openapi/inference/api.yaml](../../../specs/openapi/inference/api.yaml)
(`src/api/generated/inference_api/server.zig`) and registered under `/ai/v1`
and `/ml/v1` by `Node.registerHttpRoutes` in `src/server/server.zig`. The
Kubernetes operator (`InferencePool`, `InferenceProxy`, `ExternalInferencePool`
CRDs) and the inference proxy are Go components owned by Colony.

Repository-wide open work lives in [zig/TODO.md](../../TODO.md) and
[zig/ROADMAP.md](../../ROADMAP.md); this file does not repeat it.

---

## Shipped (pointers only)

- Endpoints `/embed`, `/embeddings`, `/rerank`, `/generate`,
  `/generate/batch`, `/chat/completions`, `/chunk`, `/extract` (canonical
  NER/relations/text and token classification), `/decisions`, `/rewrite`,
  `/read`, `/transcribe`, `/dictate`, `/transcription/sessions/*`, `/models`,
  `/predict`, `/predictors`, and `/ml/v1/metrics`: all registered through the
  generated router and covered by handler tests in `src/server/server.zig`;
  most also have Python E2E in [zig/e2e/inference](../../e2e/inference).
- Backends: [NATIVE.md](NATIVE.md), [METAL.md](METAL.md), [CUDA.md](CUDA.md),
  [ONNX.md](ONNX.md), [WASM.md](WASM.md), [GGML.md](GGML.md),
  [GRAPH.md](GRAPH.md), [MULTIDEVICE.md](MULTIDEVICE.md).
- Generation, KV cache, tool calling, grammar/speculative decoding:
  [LLMS.md](LLMS.md), [KVCACHE.md](KVCACHE.md),
  [TURBOQUANT.md](TURBOQUANT.md), [TOOL_CALLING.md](TOOL_CALLING.md),
  [OPENAI.md](OPENAI.md); model families in [models/](models/README.md).
- Reranking (cross-encoder, ColBERT, ColQwen, Qwen3-VL): [RERANKING.md](RERANKING.md).
- Readers/OCR: [READERS.md](READERS.md). Training: [finetuning/FINETUNING.md](finetuning/FINETUNING.md).
- Hugging Face pull with resume, receipts, and integrity checks
  (`src/registry/download.zig`): [MODEL_COMPATIBILITY.md](MODEL_COMPATIBILITY.md), [TASKS.md](TASKS.md).
- Admission and backpressure: fail-fast weighted admission
  (`src/server/inference_admission.zig`, `max_concurrent_requests`) returning
  503 with `Retry-After`; requests are intentionally never queued. Model
  residency and budgets: [MANAGERS.md](../../MANAGERS.md), [BUDGETS.md](BUDGETS.md).
- Cross-request batching (instead of per-model session pools): [BATCHING.md](BATCHING.md).
- Binary dense embeddings: `/embed` returns a numeric frame for
  `Accept: application/vnd.antfly.numeric.v1` (`buildDenseNumericFrame`).
- Prometheus counters/gauges for requests, errors, embed batches, speculation,
  model count, and admission (`src/server/metrics.zig`).

---

## Open Work

### API and Spec Drift

- [ ] **Semantic chunking.** `/chunk` serves only the fixed chunker
  (`lib/chunker`, with overlap and separator fallback); semantic model names
  fail closed (test "fixed chunk model aliases canonicalize and semantic
  models fail closed"). The `/chunk` description in `api.yaml` still promises
  ONNX semantic chunkers from `models_dir/chunkers/` and 2-minute result
  caching; either implement them or correct the spec.
- [ ] **Result cache.** `ResultCache`/singleflight in `src/cache/` are not used
  by any handler; `Node.embed_cache` is allocated (`server.zig`, `init`) and
  freed but never read, and `/chunk` always reports `cache_hit = false`.
  Query-embedding caching is owned by Antfly core
  ([INFERENCE_CACHING.md](../../INFERENCE_CACHING.md)). Decide whether the
  server needs its own cache; otherwise delete the dead field and the
  `cache_hits`/`cache_misses` metrics, which are never incremented.
- [ ] **Native T5 rewriting.** `architectures/t5.zig` builds native T5
  sessions (also used by `web/t5_api.zig`), but `/rewrite` only resolves ONNX
  encoder/decoder pairs or the native Florence fallback
  (`findEncoderDecoderPaths` in `src/pipelines/encoder_decoder.zig`), so
  SafeTensors/GGUF T5 bundles are not reachable over HTTP.

### LayoutDoc

The runtime and checkpoint loaders are in place
([models/layoutdoc/LAYOUTDOC.md](models/layoutdoc/LAYOUTDOC.md)), but the HTTP
surface is not.

- [ ] **Register or remove the document classification handlers.**
  `Node.classifyDocument` and `Node.classifyDocumentTokens` in
  `src/server/server.zig` are not bound to any route, and `api.yaml` defines
  only their request schemas. The `/api/classify/document*` endpoints in
  LAYOUTDOC.md do not exist. The likely target is a document task on `/extract`.
- [ ] **Image bytes and content parts** instead of the local `image_path` input.
- [ ] **OCR-to-token flow** that produces the bbox tokens `classify_tokens`
  needs, from the readers in [READERS.md](READERS.md).
- [ ] **Parity fixtures** from real `gopeft-zig` checkpoints and example pages
  (the pipelines in `src/pipelines/document_*classification.zig` have no
  checkpoint-backed tests).

### Reranking

- [ ] Multimodal `/rerank` request-level coverage and reporting semantics: see
  [RERANKING.md#remaining-work](RERANKING.md#remaining-work). Today the
  multimodal tests are parser/admission unit tests in `src/server/server.zig`,
  and `zig/e2e/inference/test_rerank.py` sends no image content.

### GLiNER

Parity is qualified for the reviewed GLiNER2.5 rows; remaining model-level
work is in [models/gliner2/GLINER25.md](models/gliner2/GLINER25.md#open-work)
and [models/gliner2/CUDA.md](models/gliner2/CUDA.md#remaining-work).

- [ ] **Bounded BLAS-vs-tensor-parallel parity run** on a real local GLiNER2
  bundle. The only distributed test is the helper-level
  "gliner distributed GPU-hosted helpers mirror pipeline/session state" in
  `src/pipelines/gliner.zig`.

### Distributed Server Execution

- [ ] **Server-side multi-rank orchestration** for rerank and GLiNER
  `/extract`. Pipelines read `runtime.distributed.configFromEnv()`
  (`TERMITE_DISTRIBUTED_*`, `src/runtime/distributed.zig`) in
  `src/server/model_manager.zig` and `src/server/server.zig`, but no server
  path dispatches a request to peer ranks or reports per-rank results.

### Multimodal Generation

Native image/audio generation through the GGUF projector path is served. What
remains is throughput and parity on the faster paths:

- [ ] The CUDA row scheduler excludes multimodal requests
  (`enabledForRequest`, `src/server/server.zig`).
- [ ] The Metal whole-model executor excludes multimodal prompts because they
  prefill from projected embeddings.
- [ ] `/generate/batch` serializes multimodal items under the model lock until
  a model capability advertises native multimodal batching.

### Operations

- [ ] **Latency histograms and model load/unload events.** `/metrics` has
  counters and gauges only; per-endpoint latency histograms and load/evict
  events for AI models (as opposed to tabular predictors) are missing.
- [ ] **Graceful shutdown.** SIGINT/SIGTERM keep default OS behavior
  (`src/main.zig`), so in-flight requests are not drained and models are not
  unloaded on shutdown.
- [ ] **`SessionPool` cleanup.** `src/backends/session_pool.zig` is exported
  and unit-tested but never used; concurrency comes from cross-request
  batching ([BATCHING.md](BATCHING.md)). Delete it or document a use.

---

## Testing

Python E2E lives in [zig/e2e/inference](../../e2e/inference) (runnable via the
`inference-e2e` scope of `.github/workflows/zig-tests.yml`); the
cancellation E2E is described in [tests/README.md](tests/README.md). The gap
that inference E2E is not in normal PR CI is tracked in
[zig/TODO.md](../../TODO.md#ci-coverage-gaps).
