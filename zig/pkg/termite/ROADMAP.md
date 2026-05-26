# termite-zig Roadmap

termite-zig is a Zig reimplementation of the Go Termite ML inference service targeting full API parity with all 10 endpoints, plus native Metal/SafeTensors support for Apple Silicon. The Kubernetes operator and proxy stay in Go — only the inference binary is being rewritten.

## Architecture

```
.onnx files ──────► ONNX Runtime (CPU, CUDA, TensorRT, ROCm)
                         │
SafeTensors/GGUF ──► Hand-written forward pass ──► Metal (Metal, macOS only)
                         │                    └──► BLAS (CPU everywhere)
```

ONNX Runtime is the universal backend (loads `.onnx` directly). Metal and BLAS need SafeTensors weight loading + hand-written model architectures per model family.

---

## What Stays in Go

- `go/pkg/operator/` — Kubernetes operator (TermitePool, TermiteRoute CRDs)
- `pkg/proxy/` — load-balancing proxy with circuit breaker
- Dashboard HTML/JS serving

The Go binary continues to work — Zig is a drop-in replacement for the inference binary only.

---

## Shipped

- SentencePiece BPE tokenizer (full, tested)
- SafeTensors parser + MMapReader + ShardedIndex (full, tested)
- WeightSource abstraction (SafeTensors adapter, f16/bf16→f32)
- BERT config parsing + weight mapping (bert, roberta, distilbert)
- Tensor type (multi-dtype, shape, owned/borrowed)
- Session vtable (run, inputInfo, outputInfo, backend, close)
- Native math primitives (sgemm, l2Normalize, meanPool)
- TTL ResultCache with stats
- Model registry (local discovery, ModelRef parsing)
- HTTP server (httpx.zig, route stubs)
- CLI (run, list, pull, version)
- Build system (conditional `-Donnx`, `-Dmetal`, `-Dsystem-blas`)
- Working `/api/embed` via ONNX
- Reranking pipeline and `/api/rerank`
- Native BERT/RoBERTa cross-encoder path
- ColBERT late-interaction text reranker
- ColQwen multimodal reranker and `/rerank_multimodal`
- GLiNER2 native DeBERTa + span-head path with distributed Metal TP
- Document classification runtime (`/api/classify/document`, `/api/classify/document_tokens`)
- LayoutLMv3 PEFT surface (LoRA bootstrap, train, inspect, materialize)
- Autodiff and training loop (reverse-mode AD, VJP rules, FlatTrainingState, LoRA injection)
- Optimizers (SGD, Adam, AdamW, LLRD, Schedule-Free AdamW, gradient clipping)
- Distributed training (data-parallel via allReduceSum)
- Activation checkpointing
- Grammar masking + speculative decode parity on native generation
- Constrained decoding (native backend only)
- Tool-calling parity in `/api/generate` (FunctionGemma-style, streaming `tool_calls` deltas)
- Gemma4 LoRA training (surrogate-gradient, PEFT-compatible output)
- Text generation with autoregressive decoding, KV cache, SSE streaming
- ONNX `ortgenai` image-bearing generation path

---

## Active Work

### Metal Gemma Follow-Up

- [ ] **Budget accounting for native Metal Gemma**: large native Gemma 3 Metal runs exceed the intended runtime budget because resident Metal tensors and other backend allocations are not fully accounted for by the current host/backend/KV/scratch reservation system.
- [ ] **NVMe spill for native Metal Gemma**: current `disk`/`host`/`backend` budgeting is still tensor-store reload semantics, not a true NVMe-managed residency path. Cold dense weights should stay on disk/NVMe by default rather than being pulled resident too eagerly.
- [ ] **Clean up Metal Gemma debug scaffolding**: remove the temporary scheduler/KV/paged-attention debug env toggles and noisy Metal tied-logits logging added during the Gemma 3 decode investigation, while keeping the actual correctness fixes.

### Reranker and Multimodal Verification

- [ ] **Bounded BLAS-vs-Metal TP verification on a real local ColQwen2 bundle**
- [ ] **`/rerank_multimodal` smoke/regression surface** (request-level)
- [ ] **Verify native Qwen2-VL vision behavior under distributed Metal** on the larger machine
- [ ] **Unify text and multimodal late-interaction reporting semantics**
- [ ] **Broader multimodal server-path regression coverage**
- [ ] **Rank-aware Metal device/stream selection polish** for distributed reranker
- [ ] **Request orchestration semantics** for server-side distributed rerank execution

### Native GLiNER Parity

- [ ] **GLiNER parity validation**: GLiNER has a native DeBERTa + span-head path. Remaining work: prove parity with real GLiNER models across Metal/BLAS, add backend-specific tests, tighten performance gaps in the native head.
- [ ] **Bounded BLAS-vs-TP parity run** on a real local GLiNER2 bundle
- [ ] **Server-path orchestration** for distributed multi-rank GLiNER2 execution
- [ ] **Thread server/reporting semantics** through native `/classify` and `/recognize`

### LayoutDoc

- [ ] **Parity fixtures** using real `gopeft-zig` checkpoints and example pages
- [ ] **Image-byte / content-part input** for the sequence endpoint (vs. current path-based)
- [ ] **OCR extraction / bbox-producing flow** that can feed `classify_tokens` directly

---

## Remaining API Parity

### Endpoint Status

| Endpoint | Status |
|----------|--------|
| `/api/embed` | Working (ONNX) |
| `/api/rerank` | Working (ONNX + native BERT/RoBERTa + ColBERT) |
| `/api/rerank_multimodal` | Working end-to-end; verification ongoing |
| `/api/generate` | Working (autoregressive, streaming, tool-calling) |
| `/api/chunk` | Basic fixed chunking; semantic chunking pending |
| `/api/recognize` | GLiNER native path; parity validation pending |
| `/api/classify` | Stub |
| `/api/rewrite` | Stub |
| `/api/read` | Stub |
| `/api/transcribe` | Stub |
| `/api/models` | Stub |

### Remaining Endpoint Work

**Chunking (`/api/chunk`):**
- Improve fixed chunking: sentence boundary detection, overlap support
- Semantic chunking via optional token classification model

**Text classification (`/api/classify`):**
- Sequence classification: run BERT, extract `[CLS]` logits
- Multi-label support (sigmoid per class)
- Zero-shot via NLI model (hypothesis template)

**Text rewriting (`/api/rewrite`):**
- Seq2seq inference (T5/BART): encode input, decode output autoregressively
- Shares generation logic with `/api/generate`

**Document reading (`/api/read`):**
- Vision2Seq: image preprocessing (resize, normalize) → vision encoder → text decoder
- Florence2 support as primary target

**Audio transcription (`/api/transcribe`):**
- Audio preprocessing: resample to 16kHz, mel spectrogram
- Whisper-style encoder-decoder

**Model architectures needed:**

| Architecture | Endpoints | Status |
|---|---|---|
| BERT encoder | embed, rerank, recognize, classify | Done |
| GPT/Qwen decoder | generate, rerank (ColBERT) | Done |
| T5 encoder-decoder | generate, rewrite | Partial |
| CLIP vision+text | embed (multimodal) | Pending |
| Whisper | transcribe | Pending |
| Florence2 | read | Pending |

---

## Infrastructure Gaps

- [ ] **Result caching**: `ResultCache` and singleflight primitives exist in `src/cache/` but are not wired into any handler. Go termite has per-endpoint TTL caches with singleflight dedup for: embedding, sparse embedding, chunking, reranking, NER, reading, transcription.
- [ ] **Dense binary serialization**: Go termite defaults to `application/octet-stream` for `/api/embed` (LE float32 arrays). We always return JSON.
- [ ] **Multimodal generation parity**: native multimodal generation is missing for models like Gemma 3; multimodal success coverage and streaming behavior still lag Go termite.
- [ ] **HuggingFace Hub download**: `termite pull owner/model:variant` — HTTP client for hub.huggingface.co, token auth, variant selection, progress reporting, resume support. (`src/registry/download.zig`)
- [ ] **Session pooling**: pool of N sessions per model for concurrent inference. (`src/backends/session_pool.zig`)
- [ ] **Prometheus metrics**: request latency histograms, cache hit rates, model load/unload events. `/metrics` endpoint. (`src/server/metrics.zig`)
- [ ] **Request queue + backpressure**: configurable max queue depth, 503 with Retry-After when full.
- [ ] **Graceful shutdown**: drain in-flight requests on SIGTERM, unload all models cleanly.

---

## E2E Testing

**Structure:** Shell scripts in `e2e/` using curl + jq.

| Script | Model | Validates |
|---|---|---|
| `test_embed.sh` | bge-small-en-v1.5 | Embedding dimensions, cosine similarity |
| `test_rerank.sh` | ms-marco-MiniLM-L-6-v2 | Score ordering, relevant > irrelevant |
| `test_chunk.sh` | (no model) | Chunk boundaries, overlap |
| `test_blas.sh` | bge-small-en-v1.5 (SafeTensors) | Output matches ONNX within tolerance |
| `test_mlx.sh` | bge-small-en-v1.5 (SafeTensors) | Output matches ONNX within tolerance |
| `test_generate.sh` | small T5/GPT model | Generates coherent text |
| `test_ner.sh` | NER model | Correct entity spans |

**CI matrix:**
```
macOS arm64:  ONNX + BLAS + Metal
Linux x86_64: ONNX + BLAS
Linux arm64:  ONNX + BLAS
```

**Golden file approach:** Run Go termite on same model + inputs, save output. Zig tests compare against golden files (cosine similarity > 0.99 for embeddings, exact match for classification).

---

## Key Reference Files

| Zig File | Go Reference |
|---|---|
| `src/backends/onnx.zig` | `lib/hugot/backends/backend_onnx.go` |
| `src/pipelines/embedding.zig` | `lib/embeddings/embedder.go`, `lib/pipelines/embedding.go` |
| `src/pipelines/reranking.zig` | `lib/reranking/reranker.go` |
| `src/pipelines/chunking.zig` | `lib/chunking/fixed.go`, `lib/chunking/chunker.go` |
| `src/server/model_manager.zig` | `pkg/termite/registry_base.go`, `model_budget.go` |
| `src/cache/singleflight.zig` | `pkg/termite/result_cache.go` |
| `src/tokenizer/hf_tokenizer.zig` | `go-huggingface/tokenizers/` |
| `src/models/manifest.zig` | `lib/modelregistry/manifest.go` |
| `src/registry/download.zig` | `lib/modelregistry/huggingface.go` |
```

---

Here's a summary of what was done and key decisions made:

**docs/finetuning/FINETUNING.md** — merged TRAINING.md, TRAINING_FEATURES.md, LAYOUTLMV3_FINETUNE.md, and RUN_CONTRACT.md into a single document organized as: autodiff/IR architecture → core training primitives → optimizer features → feature matrix with per-feature explanations → SafeTensors checkpoint format → benchmarking → LayoutLMv3 PEFT surface (commands, runbook, artifact contracts) → CLI reference for all model families → run contract (JSON artifact schemas).

**docs/RERANKING.md** — merged COLBERT_RERANKING.md, COLQWEN_RERANKING.md, and NATIVE_CROSS_ENCODER_METAL.md. Organized as three top-level sections (cross-encoder, ColBERT late-interaction, ColQwen multimodal) with the native cross-encoder section expanded since it has the most concrete performance data and verification scripts. Eliminated the duplicate distributed Metal env var tables (one canonical table per section instead).

**docs/NATIVE_MODELS.md** — merged GLINER2_DISTRIBUTED_METAL.md and LAYOUTDOC_RUNTIME.md. Cut the "as of April 8" datestamp and "phase" language; kept all HTTP API contracts, probe commands, and env vars verbatim.

**ROADMAP.md** — merged ROADMAP.md (the old one was actually the full parity plan from early development) with TODO.md. Restructured as: architecture overview → what stays in Go → shipped items (consolidating the old "What's Done" list with items from TODO's checked boxes) → active work (Metal Gemma, reranker verification, GLiNER, LayoutDoc) → remaining API parity (endpoint status table + per-endpoint notes) → infrastructure gaps (from TODO unchecked items + Phase 6 items) → E2E testing. Removed the numbered Phase 1–6 planning structure and forward-looking phase language throughout.
