# antfly-inference-zig Roadmap

antfly-inference-zig is Antfly's Zig ML inference runtime: embeddings, reranking (cross-encoder, ColBERT, ColQwen), extraction/classification (GLiNER2, LayoutLMv3/LayoutDoc), chunking, and generative text (autoregressive decoding, KV cache, tool-calling, grammar-constrained decoding, SSE streaming) across native CPU, Metal, CUDA, and ONNX Runtime backends, plus LoRA/PEFT training for supported architectures. Most of the 10-endpoint API is already working — see "Shipped" below; "Remaining API Parity" tracks what's left. The Kubernetes operator and proxy stay in Go; only the inference binary itself is Zig.

## Architecture

```
.onnx files ──────► ONNX Runtime (CPU, CUDA, TensorRT, ROCm)
                         │
SafeTensors/GGUF ──► Hand-written forward pass ──► Metal (macOS only)
                         ├──► native (CPU everywhere, optional system BLAS)
                         └──► CUDA (see CUDA.md)
```

ONNX Runtime is the universal backend (loads `.onnx` directly). Metal, native, and CUDA need SafeTensors/GGUF weight loading plus hand-written model architectures per model family — see [NATIVE.md](NATIVE.md), [METAL.md](METAL.md), and [CUDA.md](CUDA.md) for each backend's design.

---

## What Stays in Go

- `go/pkg/operator/` — Kubernetes operator (InferencePool, InferenceProxy CRDs)
- `pkg/proxy/` — load-balancing proxy with circuit breaker
- Dashboard HTML/JS serving

The Go binary continues to work — Zig is a drop-in replacement for the inference binary only.

---

## Shipped

Backend and runtime design docs (linked below) describe how these work today; this list is what they add up to at the API/feature level.

- SentencePiece BPE tokenizer (full, tested)
- SafeTensors parser + MMapReader + ShardedIndex (full, tested)
- WeightSource abstraction (SafeTensors adapter, f16/bf16→f32)
- BERT config parsing + weight mapping (bert, roberta, distilbert)
- Tensor type (multi-dtype, shape, owned/borrowed)
- Session vtable (run, inputInfo, outputInfo, backend, close)
- Native CPU backend math primitives (sgemm, l2Normalize, meanPool) — see [NATIVE.md](NATIVE.md)
- Metal backend (packed/resident quantized weights, graph-planned command scopes) — see [METAL.md](METAL.md)
- CUDA backend — see [CUDA.md](CUDA.md)
- GGUF/GGML quantization format coverage and graph-execution partitioning — see [GGML.md](GGML.md)
- Generic graph IR (tracing, compiler passes, execution backends, partitioning, caching, offline artifacts) — see [GRAPH.md](GRAPH.md)
- TurboQuant KV cache codec — see [TURBOQUANT.md](TURBOQUANT.md)
- Gemma 4 support, including MTP speculative decoding — see [GEMMA4.md](GEMMA4.md)
- Document readers (OCR/layout/extraction pipelines) — see [READERS.md](READERS.md)
- TTL ResultCache with stats
- Model registry (local discovery, ModelRef parsing)
- HTTP server (httpx.zig, route stubs)
- CLI (run, list, pull, version)
- Build system (conditional `-Donnx`, `-Dmetal`, `-Dcuda`)
- Working `/api/embed` via ONNX
- Reranking pipeline and `/api/rerank`
- Native BERT/RoBERTa cross-encoder path
- ColBERT late-interaction text reranker
- ColQwen multimodal reranker and `/rerank_multimodal`
- GLiNER2 native DeBERTa + span-head path
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

### Reranker and Multimodal Verification

- [ ] **`/rerank_multimodal` smoke/regression surface** (request-level)
- [ ] **Unify text and multimodal late-interaction reporting semantics**
- [ ] **Broader multimodal server-path regression coverage**
- [ ] **Request orchestration semantics** for server-side distributed rerank execution

### Native GLiNER Parity

- [ ] **GLiNER parity validation**: GLiNER has a native DeBERTa + span-head path. Remaining work: prove parity with real GLiNER models across backends, add backend-specific tests, tighten performance gaps in the native head.
- [ ] **Bounded BLAS-vs-TP parity run** on a real local GLiNER2 bundle
- [ ] **Server-path orchestration** for distributed multi-rank GLiNER2 execution
- [ ] **Thread server/reporting semantics** through native `/classify` and `/extract`

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
| `/api/extract` | GLiNER native path; parity validation pending |
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

- [ ] **Result caching**: `ResultCache` and singleflight primitives exist in `src/cache/` but are not wired into any handler. Go antfly inference has per-endpoint TTL caches with singleflight dedup for: embedding, sparse embedding, chunking, reranking, NER, reading, transcription.
- [ ] **Dense binary serialization**: Go antfly inference defaults to `application/octet-stream` for `/api/embed` (LE float32 arrays). We always return JSON.
- [ ] **Multimodal generation parity**: native multimodal generation is missing for models like Gemma 3; multimodal success coverage and streaming behavior still lag Go inference.
- [ ] **HuggingFace Hub download**: `antfly inference pull owner/model:variant` — HTTP client for hub.huggingface.co, token auth, variant selection, progress reporting, resume support. (`src/registry/download.zig`)
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
| `test_generate.sh` | small T5/GPT model | Generates coherent text |
| `test_ner.sh` | NER model | Correct entity spans |

**CI matrix:**
```
macOS arm64:  ONNX + BLAS + Metal
Linux x86_64: ONNX + BLAS
Linux arm64:  ONNX + BLAS
```

**Golden file approach:** Run Go antfly inference on same model + inputs, save output. Zig tests compare against golden files (cosine similarity > 0.99 for embeddings, exact match for classification).

---

## Key Reference Files

| Zig File | Go Reference |
|---|---|
| `src/backends/onnx.zig` | `lib/hugot/backends/backend_onnx.go` |
| `src/pipelines/embedding.zig` | `lib/embeddings/embedder.go`, `lib/pipelines/embedding.go` |
| `src/pipelines/reranking.zig` | `lib/reranking/reranker.go` |
| `src/pipelines/chunking.zig` | `lib/chunking/fixed.go`, `lib/chunking/chunker.go` |
| `src/server/model_manager.zig` | `pkg/inference/registry_base.go`, `model_budget.go` |
| `src/cache/singleflight.zig` | `pkg/inference/result_cache.go` |
| `src/tokenizer/hf_tokenizer.zig` | `go-huggingface/tokenizers/` |
| `src/models/manifest.zig` | `lib/modelregistry/manifest.go` |
| `src/registry/download.zig` | `lib/modelregistry/huggingface.go` |
