# Antfly inference LLM Plan

> **Editorial note (2026 audit pass):** this document has not been fully
> reframed as a design doc — it is long (1300+ lines) and its "Delivery
> Phases" section leans heavily on an `mlx` compute backend that has since
> been removed from the codebase entirely (see `src/backends/backends.zig`'s
> `BackendType` enum: `native`, `onnx`, `metal`, `cuda`, `pjrt`, `wasm` — no
> `mlx`). Phase 5 and Phase 10, the sections that relied most heavily on that
> removed backend, have been relocated verbatim to
> [work-log/completed/inference/llms-stale-sections.md](../../../work-log/completed/inference/llms-stale-sections.md)
> with pointers left in place; their durable, backend-agnostic facts were
> folded into Core Decisions and Current Status above. The remaining phase
> content below still predates the current backend set and has not been
> individually re-verified against current source. Current, accurate design
> docs for the overlapping subsystems exist separately:
> [KVCACHE.md](KVCACHE.md) for the KV cache as implemented today,
> [GRAPH.md](GRAPH.md) for the graph IR/runtime, [GGML.md](GGML.md) for
> GGUF/quantization format coverage, and [CUDA.md](CUDA.md) for the CUDA
> backend that superseded the "future `cuda`" placeholder below. Treat any
> specific claim in this file as unverified unless cross-checked against
> current source or one of those docs.

## Goal

Add first-class local LLM support to antfly inference with:

- complete GGUF model/container support
- native Zig execution, not a llama.cpp wrapper
- Hypura-like storage-tier-aware inference
- compute backends remaining separate from model format (the backend set
  has since evolved to `native`, `metal`, `cuda`, `onnx`, `pjrt`, and `wasm`;
  the `mlx` backend named below was later removed — see the editorial note
  above)

The intended end state is:

`GGUF on disk -> tiered tensor store -> staged/dequantized working set -> MLX/BLAS/CUDA compute`

Not:

`GGUF -> convert to second full runtime copy -> infer from duplicate artifact`

## Current Status

The repo is no longer at the pure design stage.

Implemented now:

- GGUF parsing, metadata, tensor catalog, and manifest discovery
- GGUF-backed BLAS weight loading for native sessions, including LLaMA-style weight-name normalization
- storage-agnostic tensor access for SafeTensors and GGUF
- backend-agnostic paged KV manager and block tables
- request-scoped paged native decode wired into the live BLAS/MLX generation path
- query-only incremental decode with absolute-position-aware RoPE offsets
- sliding-window KV trimming with retained-window position tracking
- direct page-table attention for BLAS
- direct page-table attention for MLX using online blockwise softmax reduction
- native generation reserves KV and scratch memory budgets up front and fails
  closed with `MemoryBudgetExceeded` instead of allocating first

Still missing on the critical path:

- prefix cache integration across server requests, not just runtime support
  inside `KvManager` (the `chat` CLI can drive `PromptPrefixCache` across REPL
  turns via `--prompt-cache`, but the attach path currently degrades metal
  decode and can hang native — see GEMMA4.md "Chat REPL"; the server still
  re-prefills streamed requests)
- multi-request chunked prefill interleaving (single-request chunked prefill is done)
- tiered expert streaming and storage placement

## Core Decisions

### 1. GGUF Is Not A Backend

GGUF is:

- a container format
- a metadata format
- a tensor catalog
- a set of ggml tensor encodings / quantization layouts

GGUF is not the compute backend. Antfly inference backends remain `mlx`, `native`, and later `cuda`.

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
   - MLX execution
   - BLAS execution
   - future CUDA execution

This avoids baking GGUF assumptions into MLX or BLAS directly.

### 4. Stage And Dequantize First, Fuse Later

The first working native implementation should:

- stage selected tensor blocks from disk/RAM
- dequantize active blocks into temporary `f16` or `f32` backend-native buffers
- execute attention/MLP/MoE math in MLX or BLAS

Do not block the project on custom MLX kernels for `Q5_K`, `Q6_K`, etc.

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

## Major Refactor

## Current State

Current antfly inference native path is shaped roughly like:

- manifest discovers ONNX or SafeTensors
- session factory loads all weights eagerly
- GPT generation reruns full forward pass each token
- architecture code assumes dense FFN

That is enough for small dense HF-format models, but not for GGUF or Hypura-like execution.

## Target State

Introduce explicit layers:

### `src/gguf/`

- `format.zig`
  - header parsing
  - metadata parsing
  - tensor table parsing
- `metadata.zig`
  - typed accessors for tokenizer/chat template/model metadata
- `tensor_types.zig`
  - ggml quant type enum
  - block sizes
  - values per block
- `reader.zig`
  - mmap / file-backed random access
- `tensor_catalog.zig`
  - tensor names, shapes, dtype, offsets

### `src/tensors/`

- `logical_tensor.zig`
  - abstract tensor identity and role
- `tensor_store.zig`
  - unified interface over SafeTensors, GGUF, and future stores
- `quant_codec.zig`
  - registry of codecs per ggml type
- `dense_codec.zig`
- `kquant_codec.zig`
- `iquant_codec.zig`
- `staging.zig`
  - materialize blocks into temporary dense buffers

### `src/runtime/`

- `placement.zig`
  - decides GPU/RAM/NVMe residency
- `pager.zig`
  - page/block fetching
- `prefetch.zig`
  - lookahead and speculative fetch
- `residency_cache.zig`
  - hot block cache
- `decode_state.zig`
  - per-request state
- `kv_cache.zig`
  - backend-agnostic KV cache layout
- `scheduler.zig`
  - request admission
  - prefill vs decode scheduling
  - continuous batching
- `streaming.zig`
  - token streaming
  - native SSE / chunked response integration

### `src/runtime/moe/`

- `router.zig`
  - router logits and top-k expert selection
- `expert_store.zig`
  - expert tensor lookup/staging
- `expert_cache.zig`
  - hot experts / slices
- `expert_prefetch.zig`
  - speculative prefetch from routing history

### `src/backends/`

Keep backend-specific math here:

- `mlx.zig`
- `native.zig`
- future `cuda.zig`

Add backend staging helpers:

- `mlx_staging.zig`
- `blas_staging.zig`
- future `cuda_staging.zig`

### `src/models/`

Refactor model config parsing so architecture config is not tied to storage format:

- `gpt.zig` should grow support for:
  - sliding window
  - MoE parameters
  - rope variants/scaling
  - backend-agnostic KV/cache hints

### `src/architectures/`

Refactor architecture code to consume logical tensor handles instead of eager dense weight maps.

Add:

- `mixtral.zig`
  - MoE decoder block
  - router
  - top-2 dispatch
  - sparse combine

## KV Cache Architecture

## Goals

The KV cache subsystem should support:

- single-request low-latency decode
- multi-request continuous batching
- paged attention
- shared-prefix reuse across requests
- sliding-window / rolling-cache models
- tier-aware GPU/RAM/NVMe placement
- backend-neutral storage with backend-specific execution

The default architecture should be paged-first.

This matches the direction of the strongest current serving systems:

- PagedAttention / vLLM style block tables and non-contiguous physical allocation
- TensorRT-LLM style block pools, reuse, sliding-window-aware eviction, and secondary pools
- ORT GenAI style explicit KV cache management in the generation loop

## Why Paged KV Cache

A contiguous monolithic KV tensor is simple, but it causes:

- over-reservation
- fragmentation
- hard max-length allocation cliffs
- awkward prefix sharing
- awkward sliding-window reclamation

Paged KV cache should be termite's primary design, not a later optimization.

Antfly inference can still support a simpler contiguous mode for:

- testing
- debugging
- small local models
- backend bringup

But production native generation should target paged KV.

## KV Cache Layers

### 1. Logical Sequence View

Each request sees KV as:

- an ordered token sequence
- partitioned into fixed-size logical blocks
- with optional shared prefix blocks and unique suffix blocks

The logical view must not require contiguous physical storage.

### 2. Physical Block Pools

Each backend gets one or more physical KV pools.

At minimum antfly inference should support pools keyed by:

- backend type
- element type
- `num_kv_heads`
- `head_dim`
- attention window class

This mirrors current best practice for handling:

- MHA
- MQA
- GQA
- variable sliding-window sizes

### 3. Page Tables / Block Tables

Each active sequence owns a block table:

- logical block index -> physical block id

Each physical block tracks:

- pool id
- block id
- layer range or packed-layer layout
- token capacity
- filled token count
- residency tier
- refcount
- last access tick
- prefix-cache eligibility
- eviction priority

### 4. Shared Prefix Cache

The prefix cache is separate from the per-request decode state.

It should store reusable full blocks keyed by:

- model id
- tokenizer id / chat-template hash if relevant
- prefix token hash chain
- layer/window/pool configuration

The safe default is:

- only full KV blocks are reusable
- partial tail blocks are request-private

This avoids complicated correctness bugs while still capturing most of the value.

### 5. Sliding-Window / Rolling Cache

For models with limited attention windows, antfly inference should:

- keep a logical token cursor
- retire blocks that fall outside the effective window
- return retired blocks to reusable pools
- optionally publish retired full blocks to prefix cache if safe

This allows:

- much lower memory usage on long-running chats
- compatibility with Mistral/Mixtral-style sliding-window models

## KV Cache Data Structures

### `src/runtime/kv/block.zig`

- `KvBlockId`
- `KvPoolId`
- `KvResidency`
  - `gpu`
  - `ram`
  - `nvme`
- `KvBlockMeta`
  - token capacity
  - tokens written
  - refcount
  - last access
  - priority
  - model/pool compatibility

### `src/runtime/kv/pool.zig`

- `KvPoolConfig`
  - backend
  - dtype
  - page_size_tokens
  - num_layers_packed
  - num_kv_heads
  - head_dim
  - sliding_window_size
- `KvPool`
  - underlying storage buffers
  - free list
  - resident block table
  - secondary/offload link if present

### `src/runtime/kv/block_table.zig`

- `SequenceBlockTable`
  - ordered mapping from logical block index to physical block id
  - tail token count
  - prefix/shared split marker

### `src/runtime/kv/prefix_cache.zig`

- `PrefixCacheKey`
  - model id
  - block hash
  - prefix hash
  - pool config
- `PrefixCacheEntry`
  - physical block ids
  - refcount
  - last reuse
  - eviction priority

Recommended implementation:

- hash chained by prefix
- exact token identity
- full-block only reuse

### `src/runtime/kv/allocator.zig`

- allocates blocks from compatible pools
- supports copy-on-write for shared prefix blocks
- supports tail-block growth on decode
- supports pool-aware fallback if ideal pool is exhausted

### `src/runtime/kv/evictor.zig`

- prioritized LRU baseline
- protect active decode tail blocks
- prefer evicting cold non-shared blocks first
- sliding-window-retired blocks should be easiest to reclaim

### `src/runtime/kv/manager.zig`

Top-level API for:

- request attach
- prefill allocation
- decode append
- block sharing
- window retirement
- eviction
- stats

## KV Cache Execution Model

## Prefill

For prefill, antfly inference should support two modes:

### Full Prefill

- compute prompt in one pass
- allocate enough pages for the prompt
- materialize KV into block layout

### Chunked Prefill

- split large prompts into chunks
- interleave with decode work
- improve tail latency under mixed workloads

Chunked prefill should be part of the design from the start, even if disabled initially.

## Decode

For decode, each step should:

1. ensure writable tail block exists
2. compute one-token KV for active requests
3. append into tail page
4. allocate a new page when current page fills
5. update block table
6. emit token to streaming layer

## Shared Prefix Reuse

When a new request arrives:

1. tokenize and apply chat template
2. divide prompt into KV block-sized token groups
3. probe prefix cache for maximal reusable full-block prefix
4. attach shared blocks by refcount
5. prefill only the uncovered suffix

This is especially important for:

- repeated system prompts
- RAG/document-chat workloads
- multi-turn chat with static conversation prefixes

## Paged Attention API Shape

The architecture code should not manipulate raw cache pointers directly.

Instead, define a backend-agnostic attention interface along the lines of:

- `beginPrefill(request_batch, kv_plan)`
- `appendPrefillChunk(request_batch, kv_plan, chunk_tokens)`
- `beginDecodeStep(request_batch, kv_plan)`
- `runDecodeAttention(layer_ctx, q, kv_view)`
- `commitDecodeStep(request_batch, appended_tokens)`

Where `kv_view` is a logical descriptor:

- pool handle
- page indices
- last page length
- effective sequence length
- optional shared-prefix segment
- optional sliding-window mask info

Backends then choose how to execute:

- MLX:
  - direct block-table paged attention is now implemented for the native path
  - gathered contiguous fallback still exists for unsupported cases and debugging
- BLAS:
  - direct block-table paged attention is now implemented for the native path
- CUDA:
  - should eventually support true paged attention kernels

## Tiered KV Residency

Weights and KV cache should not be treated the same.

Recommended baseline:

- active decode KV remains GPU-resident whenever possible
- shared prefix KV prefers GPU, then RAM
- cold reusable prefix blocks may spill to RAM
- NVMe KV offload should be a later phase, not day-one

Reason:

- weight paging is already required for Hypura-like expert streaming
- KV offload is valuable, but much more latency-sensitive
- incorrect prioritization here will destroy decode latency

So antfly inference should implement:

1. paged GPU KV cache first
2. RAM spill / reusable-prefix spill second
3. NVMe KV offload only after the scheduler and reuse model are stable

## Streaming And Scheduler Interaction

Paged KV cache is most useful when paired with request scheduling.

Add a scheduler with:

- continuous batching
- prefill/decode separation
- starvation protection
- chunked prefill admission control
- stream-oriented request lifecycle

Suggested request states:

- `queued_prefill`
- `running_prefill`
- `ready_decode`
- `running_decode`
- `finished`
- `cancelled`

The scheduler should build microbatches by favoring:

- decode steps first for low interactive latency
- bounded prefill chunks second

This is the minimum architecture needed for native streaming to feel competitive.

## KV Cache Quantization

Do not make quantized KV cache a blocking requirement for first native release.

Plan it as a later optimization:

- initial KV dtypes:
  - `f16`
  - `bf16`
- later:
  - `int8`
  - `fp8`

Quantized KV cache is worth leaving room for in the API:

- pool dtype should not assume only `f16`
- dequant-on-read hooks should be possible

But correctness and scheduling should land first.

## Recommended First Implementation

The first complete KV implementation in antfly inference should be:

- paged KV cache
- fixed page size, likely 16 or 32 tokens
- GPU-resident primary pool
- full-block shared prefix caching
- continuous batching
- chunked prefill available behind a flag
- sliding-window retirement

Current state:

- paged KV cache: done
- fixed page size: done
- sliding-window retirement: done
- backend-native page-table attention:
  - BLAS: done
  - MLX: done for the current native path
- full-block shared-prefix reuse:
  - runtime support exists in `KvManager`
  - request-level/server integration is still TODO
- continuous batching: done (`claimStep`/`completeStep` in `scheduler/native_generate.zig`, wired in `generation.zig`; one fused forward pass per step packs decode tokens and prefill chunks against a step admission budget)
- chunked prefill:
  - native BLAS/MLX generation now supports chunked prompt prefill against the paged KV path
  - the server now turns it on with a fixed prefill chunk size for `/generate`
  - microbatching across multiple requests is still TODO
- speculative decoding: done (draft model loading, K-step draft, verify, KV rollback in `generation.zig`)
- grammar-constrained decoding: done (JSON FSM, GBNF parser, JSON Schema→GBNF compiler in `grammar.zig`)
- advanced sampling: done (min-p, repetition/frequency/presence penalties in `generation.zig`)
- benchmark-guided backend tuning: partial (`src/bench/paged_attention.zig` exists, broader harness TODO)

Still TODO:

- NVMe KV offload
- beam-search-heavy cache sharing

## Delivery Phases

## Phase 0: Design And Bench Harness

Goal:

- lock interfaces before implementation sprawl

Deliverables:

- tensor store abstraction
- quant codec abstraction
- placement abstraction
- benchmark harness for:
  - prompt processing
  - token decode
  - expert cache hit rate
  - NVMe bandwidth usage

Status:

- tensor store abstraction: in progress and already usable for SafeTensors + GGUF
- benchmark harness:
  - initial native paged-attention benchmark executable is landed
  - BLAS path is usable
  - MLX path is usable for native paged-attention measurement too
  - broader scheduler/prefill/expert benchmarks are still TODO

## Phase 1: GGUF Read-Only Infrastructure

Goal:

- antfly inference can inspect GGUF models and expose metadata without inference

Deliverables:

- GGUF parser
- metadata readers
- tensor catalog
- registry/manifest integration
- CLI support:
  - list model metadata
  - inspect tensor inventory

Acceptance:

- antfly inference can open a GGUF model directory or file
- tokenizer/chat template/special tokens can be surfaced

## Phase 2: Dense GGUF Execution

Goal:

- run dense F16/F32 GGUF models natively

Deliverables:

- dense codecs
- tensor store backed by direct file access
- staging into MLX/BLAS temporary buffers
- native generation path using GGUF metadata
- fix native generation parity gaps:
  - chat template usage
  - generic tokenizer abstraction
  - streaming on native path

Acceptance:

- small LLaMA/Mistral-style GGUF models run end-to-end

## Phase 3: KV Cache And Incremental Decode

Goal:

- move native generation from full-sequence recompute to real autoregressive decoding

Deliverables:

- backend-agnostic paged KV cache
- per-layer incremental attention path
- logical-to-physical block tables
- prefix cache for full blocks
- continuous batching scheduler
- chunked prefill support
- sliding-window support
- rolling cache support where needed

Status:

- backend-agnostic paged KV cache: done
- per-layer incremental attention path: done for the native BLAS/MLX path
- logical-to-physical block tables: done
- prefix cache for full blocks: runtime support exists, request-level reuse still TODO
- continuous batching scheduler: done (`claimStep`/`completeStep` in `scheduler/native_generate.zig`)
- chunked prefill support:
  - done for the single-request native generation path
  - scheduler-level multi-request interleaving still TODO
- sliding-window support: done
- rolling cache support where needed: partial, enough for retained-window decode but not yet a general scheduling feature

Acceptance:

- native decode complexity and latency are competitive for long generations
- repeated-prefix workloads can skip prompt recomputation for shared full blocks

## Phase 4: K-Quant Support

Goal:

- support `Q*_K` models, including the formats required by `Q5_K_M` variants

Deliverables:

- `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, `Q8_K` codecs
- block staging and dequantization
- tests against reference outputs

Status:

- initial native codec/dequant staging is now landed for:
  - `Q4_K`
  - `Q5_K`
  - `Q6_K`
  - `Q8_K`
- GGUF tensor materialization can now stage those formats into dense float32 tensors
- native BLAS sessions can now load GGUF tensors through the normal weight path for the current LLaMA/Mistral-style mapping layer
- remaining practical work:
  - `Q2_K`, `Q3_K`
  - backend-aware staging paths that avoid always fully dequantizing to float32
  - quantized matmul or prepared-weight paths for hot formats

Optimization strategy:

- first pass: scalar/SIMD decode into dense temp buffers
- second pass: panel packing / prepared weights
- third pass: fused kernels where justified

Acceptance:

- dense models using K-quants run correctly
- Mixtral `Q5_K_M` tensor inventory loads without unsupported-type failures

## Phase 5: Mixtral / MoE Native Support

> **Relocated:** This phase's MLX-era status/TODO narrative and E2E bring-up checklist (227 lines) are preserved verbatim in [work-log/completed/inference/llms-stale-sections.md](../../../work-log/completed/inference/llms-stale-sections.md). It predates the removal of the `mlx` backend; see the editorial note at the top of this document and MoE Execution Ownership Is Split under Core Decisions above for the durable architecture facts it contained.

## Phase 6: Tiered Storage Runtime

Goal:

- support models larger than comfortable unified memory residency

Deliverables:

- placement planner
- paging runtime
- resident/non-resident tensor states
- direct read path from GGUF on NVMe
- async prefetch queue
- hot tensor cache

Placement policy for first version:

- embeddings, norms, router, attention-critical tensors prefer GPU
- overflow dense weights spill to RAM
- cold expert weights spill to NVMe

Status:

- mmap / file-backed random access: done (`MmapRegion` in `util/c_file.zig`, wired in `models/safetensors.zig` and `models/tensor_store.zig`)
- placement planner: done (`runtime/tier/planner.zig`)
- paging runtime: partial (mmap paging works, full NVMe tiering TODO)
- async prefetch queue: done (MLX prefetch worker in `session_factory.zig`)
- hot tensor cache: partial (lazy weight loading with guard mutexes)

Acceptance:

- model can run without eager full-file load
- no mandatory duplicate model artifact

## Phase 7: Hypura-Like Expert Streaming

Goal:

- make Mixtral usable on constrained Apple Silicon

Deliverables:

- route-aware expert staging
- expert cache keyed by layer/expert/block
- speculative prefetch from recent co-activation history
- pool buffers for in-flight expert materialization

Acceptance:

- expert cache hit rate is measurable and high after warmup
- NVMe traffic falls after initial tokens

## Phase 8: Dense FFN Streaming

Goal:

- handle oversized dense models too, not just MoE

Deliverables:

- FFN streaming path
- layer/lookahead prefetch planner
- dynamic pool sizing based on available headroom

Acceptance:

- large dense GGUF models can run with tiered residency

## Phase 9: Full GGUF Quant Family Coverage

Goal:

- complete practical GGUF parity

Deliverables:

- remaining quant codecs
- codec registration tests
- compatibility matrix

Important:

- implement this phase only after the runtime abstractions are proven on dense + K-quant + MoE
- avoid front-loading every quant family before the runtime exists

## Phase 10: Backend Optimization

> **Relocated:** This phase's MLX-era status/TODO narrative (30 lines) is preserved verbatim in [work-log/completed/inference/llms-stale-sections.md](../../../work-log/completed/inference/llms-stale-sections.md). It predates the removal of the `mlx` backend; see the editorial note at the top of this document.

## Grammar-Constrained Decoding

Added post-Phase 10 as a cross-cutting feature.

Deliverables (all done):

- **JSON FSM** (`JsonGrammar` in `pipelines/grammar.zig`): finite state machine that constrains token-by-token generation to valid JSON. Tracks nesting depth, string/number/literal states, and structural transitions.
- **GBNF parser** (`GbnfGrammar` in `pipelines/grammar.zig`): full parser for the GBNF grammar format (used by llama.cpp). Supports character classes, alternatives, repetition, and rule references. Constrains generation to match arbitrary context-free grammars.
- **JSON Schema → GBNF compiler** (`buildJsonSchemaGrammar` in `pipelines/grammar.zig`): converts a JSON Schema object into a GBNF grammar string. Supports all JSON Schema types, `const`/`enum`, `allOf`/`anyOf`/`oneOf`, `required`/`additionalProperties`, `minItems`/`maxItems`/`minimum`/`maximum`. Precise property-order enumeration capped at 4 optional properties.
- **TokenByteTable** (`pipelines/grammar.zig`): pre-decodes all vocab tokens once at generation start (single-pass). `allowedTokenMaskFast()` on both grammars uses zero-alloc per-token byte lookup.
- **Server wiring** (`server/server.zig`): `response_format` field supports `json_object`, `json_schema` (with schema compilation), and `text`. `grammar` field accepts `"json"` or arbitrary GBNF strings. Grammar-constrained decoding requires the native backend.

## Concrete Code Changes

## Existing Files To Refactor

- `src/models/weight_source.zig`
  - replace SafeTensors-only assumptions with storage-agnostic tensor store interfaces
- `src/models/manifest.zig`
  - add GGUF discovery and metadata support
- `src/architectures/session_factory.zig`
  - stop eagerly loading all dense weights into memory
- `src/pipelines/generation.zig`
  - add incremental decode and KV cache path
- `src/architectures/gpt.zig`
  - split dense decode path from MoE decode path
- `src/models/gpt.zig`
  - add Mixtral/MoE/sliding-window config fields
- `src/server/model_manager.zig`
  - unify tokenizer/chat template retrieval for HF and GGUF models
- `src/server/server.zig`
  - native generation should use generic tokenizer + chat template, then streaming

## New Top-Level Components

- `src/gguf/`
- `src/tensors/`
- `src/runtime/`
- `src/runtime/moe/`
- `src/architectures/mixtral.zig`

## Testing Strategy

## Unit Tests

- GGUF parser correctness
- metadata parsing
- tensor offset/shape/type handling
- quant codec round-trips where possible
- dequantization vs reference
- KV cache correctness
- MoE router/top-k correctness

## Differential Tests

Compare antfly inference outputs against reference implementations for:

- dense GGUF models
- K-quant dense models
- Mixtral MoE models

Comparisons:

- logits on short fixed prompts
- next-token choices with deterministic sampling
- layer outputs for selected checkpoints

## E2E Tests

- `/api/generate` on GGUF model
- native streaming
- chat template correctness
- context extension / sliding window
- Mixtral expert routing sanity

## Performance Tests

- prompt tokens/sec
- decode tokens/sec
- p50/p95 token latency
- staged bytes/token
- NVMe bytes/token
- cache hit rates
- prefetch usefulness

## Risks

- implementing all quant types before the runtime abstractions settle will waste effort
- dequantize-into-temp approach may be too slow without careful caching
- MoE support without route-aware prefetch will be correct but disappointing
- MLX may need careful memory pressure controls on Apple Silicon
- GGUF metadata compatibility drifts over time, so parser/tests must be versioned and defensive

## Recommended Implementation Order

1. GGUF parser and metadata.
2. Tensor store abstraction.
3. Dense GGUF model execution.
4. KV cache and incremental decode.
5. K-quant support needed for `Q5_K_M`.
6. Mixtral/MoE support.
7. Tiered paging and expert cache.
8. Dense FFN streaming.
9. Full quant family completion.
10. Backend-specific optimization.

## Success Criteria

Antfly inference should eventually be able to:

- load GGUF directly with no mandatory duplicate runtime copy
- run small dense GGUF models natively on MLX/BLAS
- run quantized GGUF models including `Q5_K_M`
- run Mixtral natively with MoE-aware scheduling
- use GPU/RAM/NVMe tiers intentionally rather than relying on OS swap
- keep compute backend and model format cleanly separated

## Immediate Next Steps

If work starts now, the first concrete milestone should be:

- add `src/gguf/format.zig`
- add GGUF model discovery to `src/models/manifest.zig`
- replace `WeightSource` with a storage-agnostic `TensorStore`
- fix native generation to use generic tokenizer + chat template
- introduce `NativeDecodeState` as the home for paged KV work
- add `src/runtime/kv/manager.zig`, `pool.zig`, and `block_table.zig`
- define a backend-neutral `KvView` passed into attention kernels

That sequence unlocks the rest of the plan without forcing premature quant-kernel work.

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
