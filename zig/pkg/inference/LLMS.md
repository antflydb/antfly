# Antfly inference LLM Design

This is the durable design for native local LLM support in antfly inference: GGUF ingestion, the storage/compute split, the paged KV cache, and MoE execution ownership. The subsystems it plans for now each have their own current design doc; this file states the decisions that still hold and what remains open. The original pre-implementation plan — a point-in-time status snapshot, the major refactor writeup, KV cache design narrative, delivery-phase tracking, and testing strategy — is preserved in the work-log for historical context.

> **Relocated:** The pre-implementation status snapshot, major refactor plan, KV cache design narrative, delivery phases, grammar-decoding notes, concrete code changes, and testing strategy that previously lived here (908 lines) are preserved verbatim in [work-log/completed/inference/llms-plan.md](../../../work-log/completed/inference/llms-plan.md). Durable decisions from it are in Core Decisions and Open work in this document.

## Goal

Add first-class local LLM support to antfly inference with:

- complete GGUF model/container support
- native Zig execution, not a llama.cpp wrapper
- Hypura-like storage-tier-aware inference
- compute backends (`native`, `metal`, `cuda`, `onnx`, `pjrt`, `wasm`) remaining separate from model format

The intended end state is:

`GGUF on disk -> tiered tensor store -> staged/dequantized working set -> backend compute (BLAS/Metal/CUDA)`

Not:

`GGUF -> convert to second full runtime copy -> infer from duplicate artifact`

## Core Decisions

### 1. GGUF Is Not A Backend

GGUF is:

- a container format
- a metadata format
- a tensor catalog
- a set of ggml tensor encodings / quantization layouts

GGUF is not the compute backend. The compute backends are `native`, `metal`, `cuda`, `onnx`, `pjrt`, and `wasm`.

### 2. Single Stored Model Artifact

The default design should match Hypura more closely:

- keep a single model artifact on disk
- page directly from that artifact
- avoid mandatory import/repack pipelines
- allow optional offline preparation later if benchmarks justify it

### 3. Two-Stage Native Runtime

The runtime should be split into:

1. storage/runtime layer
   - GGUF parsing
   - tensor metadata
   - residency planning
   - paging
   - prefetch
   - cache
2. compute layer
   - native BLAS execution
   - Metal, CUDA, WASM/WebGPU, and PJRT execution

This avoids baking GGUF assumptions into any compute backend directly.

### 4. Stage And Dequantize First, Fuse Later

The first working native implementation should:

- stage selected tensor blocks from disk/RAM
- dequantize active blocks into temporary `f16` or `f32` backend-native buffers
- execute attention/MLP/MoE math in the selected compute backend

Do not block the project on custom backend kernels for `Q5_K`, `Q6_K`, etc.

Later optimization phases can add:

- fused quantized matmul
- prepared weights / panel packing
- backend-specific quant kernels

### 5. Architecture Support Must Be Independent Of Weight Format

Today antfly inference mostly assumes:

- SafeTensors
- dense tensors
- dense transformer blocks

That needs to be inverted.

Target design:

- architecture code requests logical tensors by role
- tensor storage resolves where/how they live
- compute runtime decides whether they are already resident, staged, or need dequantization

### 6. MoE Execution Ownership Is Split

Sparse-MoE (Mixtral-style) execution is intentionally hybrid rather than
fully backend-native: expert routing, token-to-expert grouping, and output
scatter-merge run on the CPU, while only the expert gated-MLP matmul itself
executes on the active compute backend. Lazy expert weights stage through
byte-budgeted host/backend tiers with priority-ordered async prefetch, where
priority blends outstanding demand, recency, and MoE routing-prediction
confidence rather than strict FIFO or raw request count.

### 7. Fail Closed On Memory Budgets

Native generation (CLI `generate`/`smoke`, and the native `/generate` server
route) reserves KV and scratch memory budgets up front, before executing a
request, rather than allocating opportunistically and discovering pressure
mid-run. Exceeding the budget returns `MemoryBudgetExceeded` instead of
allocating past it. `--host-budget-mb`, `--backend-budget-mb`,
`--kv-budget-mb`, and `--scratch-budget-mb` override the defaults for local
tuning.

## End-State Feature Set

### GGUF

- parse GGUF header, metadata KV table, tensor directory
- support tensor offset lookup without loading the file eagerly
- support tokenizer/chat template/special token metadata from GGUF
- support sharded and multi-file model layouts if needed later
- design quant codec registration so new ggml types are additive

### Quantization

Support all GGUF tensor encodings required for practical llama.cpp ecosystem parity.

Implementation should be staged in waves:

- Dense/basic:
  - `F32`
  - `F16`
  - `BF16`
  - integer metadata/helper types as needed
- Legacy ggml quants:
  - `Q4_0`
  - `Q4_1`
  - `Q5_0`
  - `Q5_1`
  - `Q8_0`
  - `Q8_1`
- K-quants:
  - `Q2_K`
  - `Q3_K`
  - `Q4_K`
  - `Q5_K`
  - `Q6_K`
  - `Q8_K`
- IQ / newer families:
  - all currently relevant `IQ*` families
  - any ternary / mixed families needed for modern llama.cpp exports

Important:

- model variants like `Q5_K_M` are not primitive tensor types
- they are mixed quantization recipes across tensors
- antfly inference must support the underlying tensor types used by those recipes

### Generative Runtime

- KV cache
- paged KV cache
- incremental decode
- sliding-window / rolling cache support
- shared-prefix / prefix-cache reuse
- chunked prefill
- continuous batching
- prefill/decode scheduling
- optional KV-cache quantization later
- chat template application
- tokenizer support for GGUF metadata-driven models
- streaming generation on native backends

### Architecture Families

- LLaMA family
- Mistral family
- Mixtral / MoE
- Qwen2 / Qwen3 style decoder-only models
- Gemma family
- follow-up families via same runtime abstractions

### Hypura-Like Tiered Runtime

- GPU / RAM / NVMe placement planning
- direct paging from GGUF
- on-demand tensor staging
- expert-aware prefetch
- cache for staged/dequantized hot tensors
- MoE expert routing interception
- dense FFN streaming path for oversized dense models

## Where each subsystem is documented

| Subsystem | Design doc(s) |
|---|---|
| GGUF format and quantization | [GGML.md](GGML.md), [QUANTIZE.md](QUANTIZE.md) |
| KV cache | [KVCACHE.md](KVCACHE.md), [TURBOQUANT.md](TURBOQUANT.md) |
| Graph IR / runtime | [GRAPH.md](GRAPH.md) |
| Compute backends | [CUDA.md](CUDA.md), [METAL.md](METAL.md), [NATIVE.md](NATIVE.md), [WASM.md](WASM.md), [PJRT.md](PJRT.md) |
| MoE execution ownership | `src/runtime/moe/` (no design doc yet) |
| Tool calling | [TOOL_CALLING.md](TOOL_CALLING.md) |

## Open work

- Prefix cache integration across server requests: the server can activate a
  keyed `PromptPrefixCache` for direct generation requests (pool/storage
  setup, GPU device write hooks), and the `chat` CLI can drive it across REPL
  turns via `--prompt-cache`, but the attach path still degrades Metal decode
  and can hang native (see `models/gemma4/GEMMA4.md` "Chat REPL"); chat and
  affected streamed requests still default to full re-prefill each turn.
- Tiered expert streaming / NVMe tiering beyond mmap paging: `disk` is
  currently a spill/fallback marker in the tier planner and cache
  (`src/runtime/tier/planner.zig`, `cache.zig`), not an active NVMe streaming
  tier — there is no async NVMe read-ahead pipeline or true GPU/RAM/NVMe
  residency planner yet, and continuous batching does not yet account for MoE
  routing.
- Broader benchmark harness: only a single-purpose paged-attention benchmark
  (`src/bench/paged_attention.zig`) exists; scheduler, prefill, expert
  cache-hit-rate, and per-backend dashboards are still missing.
- Beam-search-heavy KV cache sharing is not implemented.

## External Design References

These are the main external designs worth tracking while implementing the antfly inference runtime:

- PagedAttention / vLLM paper:
  - "Efficient Memory Management for Large Language Model Serving with PagedAttention"
  - https://huggingface.co/papers/2309.06180
- vLLM automatic prefix caching docs:
  - https://docs.vllm.ai/features/automatic_prefix_caching.html
- TensorRT-LLM KV cache docs:
  - https://nvidia.github.io/TensorRT-LLM/latest/features/kvcache.html
  - https://nvidia.github.io/TensorRT-LLM/advanced/kv-cache-management.html
  - https://nvidia.github.io/TensorRT-LLM/advanced/kv-cache-reuse.html
- TensorRT-LLM attention and paged KV notes:
  - https://nvidia.github.io/TensorRT-LLM/advanced/gpt-attention.html
- ONNX Runtime GenAI generate / KV management docs:
  - https://onnxruntime.ai/docs/genai/
  - https://onnxruntime.ai/docs/genai/howto/past-present-share-buffer.html
  - https://onnxruntime.ai/docs/genai/reference/config.html
- FlashInfer paged KV and shared-prefix layouts:
  - https://docs.flashinfer.ai/tutorials/kv_layout.html
  - https://docs.flashinfer.ai/api/attention.html
  - https://docs.flashinfer.ai/api/cascade.html

These should inform the implementation, but antfly inference should keep its own backend-neutral interfaces rather than mirroring any one engine directly.
