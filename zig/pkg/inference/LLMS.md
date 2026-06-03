# Antfly inference LLM Plan

## Goal

Add first-class local LLM support to antfly inference with:

- complete GGUF model/container support
- native Zig execution, not a llama.cpp wrapper
- Hypura-like storage-tier-aware inference
- compute backends remaining separate from model format:
  - `mlx`
  - `native`
  - future `cuda`

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

Still missing on the critical path:

- prefix cache integration across requests, not just runtime support inside `KvManager`
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

Goal:

- support Mixtral-family sparse MoE inference

Deliverables:

- MoE config support
- router projection
- top-2 expert selection
- sparse expert execution
- expert output merge

Current status:

- landed:
  - Mixtral-style config fields in native GPT config parsing:
    - `sliding_window`
    - `num_local_experts`
    - `num_experts_per_tok`
  - GGUF metadata fallback for expert/sliding-window fields
  - GGUF Mixtral weight-name normalization for:
    - router `ffn_gate_inp`
    - expert `ffn_gate.{n}` / `ffn_up.{n}` / `ffn_down.{n}`
  - first native MoE FFN path in `architectures/gpt.zig`
    - router projection
    - top-k selection
    - expert output merge
  - grouped backend-native expert execution
    - tokens are batched per selected expert
    - expert gated MLP matmuls run through the active BLAS/MLX backend
  - request-local MoE runtime cache/staging substrate
    - reusable per-layer expert batch buffers across decode steps
    - hot-expert tracking
    - co-activation based predicted expert set for future prefetch
  - BLAS GGUF lazy expert residency
    - dense/core weights stay resident
    - MoE expert tensors can remain non-resident until first touch
    - predicted experts are prefetched through the lazy cache path
  - MLX GGUF lazy weight loading
    - GGUF-native MLX sessions can materialize tensors on demand
    - predicted experts reuse the same lazy cache/prefetch path
    - eager full-model MLX upload is no longer required for GGUF models
  - model-scoped shared expert cache policy
    - hot-expert and co-activation state now survives across requests
    - new requests seed their predicted experts from the shared model profile
  - bounded shared expert residency policy
    - BLAS and MLX GGUF expert caches now track model-scoped hotness
    - per-layer resident expert capacity is bounded instead of unbounded
    - cold unpinned experts are evicted first, hot experts stay resident longer
  - first explicit tier planner and tier state
    - lazy GGUF weights now carry a placement plan instead of only loaded/not-loaded state
    - MLX lazy experts now transition through `disk -> host -> backend`
    - BLAS lazy experts now expose explicit `disk -> host` state using the same planner metadata
  - first byte-budgeted shared tier pools
    - host and backend bytes are now tracked separately
    - BLAS lazy expert eviction can trigger on host-byte pressure, not only resident expert count
    - MLX lazy expert eviction can trigger on either host-byte or backend-byte pressure
  - tensor-store-owned lazy tensor refs
    - lazy GGUF entries are now registered through `tensor_store.describeTensor(...)`
    - backend lazy promotion now reloads through `tensor_store.loadTensorRef(...)`
    - BLAS/MLX no longer reach back into `weightSource()` directly for lazy GGUF promotion
  - explicit backend prefetch API for lazy weights
    - predicted expert prefetch no longer uses `getWeight()+free` as a proxy
    - BLAS prefetch warms host-resident lazy tensors
    - MLX prefetch promotes lazy tensors to their planned preferred tier
  - queued lazy-weight prefetch requests
    - prefetch requests are now enqueued on the persistent weight store
    - native generation now drains queued requests with a small per-iteration budget instead of flushing the full queue at once
    - predicted expert selection and actual staging are now separate steps
  - first async prefetch workers for lazy GGUF weights
    - queue ownership and worker lifecycle now live in `src/runtime/tier/prefetch.zig`
    - BLAS and MLX now plug backend-specific lazy-tensor staging callbacks into that shared runtime queue
    - BLAS lazy stores now have a background worker that services the queued prefetch list off the decode thread
    - MLX lazy stores now do the same for `disk -> host` staging
    - MLX `host -> backend` promotion still happens on demand on the request thread
  - model-scoped shared prefetch state
    - `LoadedModel` now owns a `runtime/tier/shared.zig` prefetch state object alongside the shared MoE routing cache
    - native BLAS/MLX sessions now attach to that shared state explicitly after session creation
    - request/completion counts for lazy tensor prefetches now survive across requests at the model level
    - queue servicing can now prioritize repeated pending tensor requests using that shared state instead of strict FIFO
    - priority is now recency-windowed rather than purely cumulative, so near-term repeated expert requests win over stale historical hotness
    - GPT MoE predicted-expert prefetch now passes explicit priority hints based on prediction rank, so queue order reflects routing confidence/proximity instead of only observing demand after the fact
    - MoE prediction strength is now carried through runtime/shared-cache prediction paths and folded into those prefetch hints instead of using rank alone
  - server KV runtime now honors model `sliding_window` when present
  - first serving-side scheduler substrate for native generation
    - native BLAS/MLX generation now supports chunked prompt prefill over the paged KV runtime
    - the first decode step can reuse the final prefill chunk logits instead of rerunning the full prompt
    - `/generate` admission is now weighted by estimated prompt/decode cost rather than always consuming one flat queue slot
  - first model-scoped native generate coordinator
    - `LoadedModel` now owns a native generate coordinator for GPT-family models
    - concurrent native requests on the same model now share a simple prefill-chunk policy instead of each using a fixed chunk size in isolation
    - current policy only coordinates request pressure and chunk sizing; it does not yet execute true cross-request microbatches
  - model-scoped native waiting-room and phase tracking
    - native generate requests now register as explicit coordinator entries instead of only contributing anonymous pressure counts
    - the coordinator now distinguishes waiting, prefill, and decode phases
    - native generation reports prefill/decode progress back into that coordinator during execution
    - prefill chunk recommendations are now phase-aware, so decode activity forces smaller prefill chunks for later requests on the same model
  - first cooperative cross-request native turn scheduler
    - native BLAS/MLX generation now requests explicit prefill and decode turns from the model-scoped coordinator
    - waiting requests yield cooperatively through the request `io` until their model turn is available
    - decode turns are prioritized over prefill turns, with bounded prefill re-entry to avoid starvation
    - this gives real cross-request interleaving on the current fiber runtime, but still does not fuse multiple requests into one shared forward pass
  - first batch-capable paged-attention substrate for native microbatching
    - decode contexts can now carry per-item KV-manager/cache bindings for batched native requests
    - BLAS and MLX paged attention now accept `batch > 1` with per-item paged KV bindings instead of immediately falling back to dense attention
    - current backend behavior still resolves paged attention per item inside the batched call, so the main near-term win is shared upper-layer linear/FFN work rather than a fully fused paged-attention kernel
  - first fused native decode microbatch path
    - compatible decode waiters on the same model can now be claimed as one scheduled decode batch
    - one leader request executes a shared `gpt.forward(batch > 1)` and fan-outs per-request logits back to the waiting requests
  - first fused native prefill microbatch path
    - compatible paged-prefill chunks on the same model can now be claimed as one scheduled prefill batch
    - one leader request executes a shared `gpt.forward(batch > 1)` for those chunks and fans final-chunk logits back to the requests that need them
    - this removes the old “turn scheduling only” limitation for compatible prefill work
  - explicit native batch-formation policy and scheduler metrics
    - native scheduler policy now has explicit min/max prefill and decode batch sizes plus a bounded lead-wait deferral rule
    - undersized incompatible batches can now be deferred briefly instead of always flushing immediately
    - `/metrics` now exposes aggregate native scheduler queue depth, formed-batch counts, batch item counts, solo-batch counts, claim deferrals, and cooperative turn yields across loaded models
- still missing:
  - true tiered residency planner across GPU/RAM/NVMe
  - continuous batching interaction with MoE routing
  - stronger model-worker policy around batch formation, admission, and time/budget-based flush
  - benchmark-driven tuning and correctness validation for the new fused native batching paths
  - correctness/perf validation against reference Mixtral outputs
  - backend-native quantized execution kernels
    - current GGUF path now has direct BLAS quant matmul for `Q8_0`, `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, and `Q8_K`
    - packed MoE expert views can now preserve GGUF quant storage and use the direct BLAS path without dense expert slices first
    - MLX now has a direct quantized execution path for those same stored formats, including packed expert views, through the MLX backend wrapper
    - MLX now prefers backend-dense staged execution once a quantized GGUF weight has already been materialized as an MLX array, so hot quantized weights stay on-device instead of bouncing back through the CPU wrapper path
    - the wrapper-direct-quant path remains as a fallback mode, but the current default is to keep staged MLX weights on the MLX matmul path
    - the MLX quantized linear path is now isolated behind an explicit executor seam with `backend_dense`, `wrapper_direct_quant`, and future `device_native` modes, so a lower-level MLX/Metal kernel can slot in without rewriting `linearNoBias`
    - lazy quantized MLX weights now cache their transposed staged array on the backend-dense path, reducing repeated transpose overhead while the native device-side kernel path is still open
    - there is now an explicit MLX native-quant provider boundary under `src/backends/mlx_quant.zig`
    - `device_native` no longer hardcodes an inline stub in the MLX compute path; it dispatches through that provider interface, which currently defaults to a no-op implementation until a lower-level MLX/Metal kernel backend is added
    - first real MLX/Metal provider support is now landed for `Q8_0`, `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, and `Q8_K` linear, including packed expert views after Zig-side contiguous row extraction
    - the Metal path now borrows MLX input data directly and uses no-copy Metal buffers for input/weight staging where the current MLX C surface allows it
    - output still re-enters MLX through array creation, so this is reduced-copy interop rather than full zero-copy MLX/Metal tensor sharing
    - this still is not a true device-side MLX block-quant kernel; the remaining gap is native MLX/Metal execution over packed GGUF blocks without dense staging
    - true MLX-native block-quant kernels / packed-expert kernels are still open if we want llama.cpp-class efficiency on the MLX path
    - dequantize-on-demand remains the bring-up path, but quantized kernels are the long-term performance target

E2E bring-up checklist:

- landed now:
  - `antfly inference smoke <model-dir> <prompt>`
    - prints GGUF tensor-type coverage for the chosen artifact
    - loads the model through the native BLAS/MLX path
    - runs one real native generation pass with paged KV enabled
  - `antfly inference generate <model-dir> <prompt>`
    - is now the primary user-facing bring-up command once inspection is clean
    - supports `--print-chat-template-status`, `--print-prompt`, `--print-token-ids`, and `--print-finish-reason`
- next:
  - first, bring up a smaller GGUF on the exact MLX generate path before using Mixtral
    - recommended shape: Gemma-family or Qwen/LLaMA-family text-only GGUF in a single model directory
    - current GGUF inspection accepts `F16`, `F32`, `Q4_0`, `Q8_0`, `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, and `Q8_K`
    - native MLX/Metal quant linear now covers `Q4_0`, `Q8_0`, `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, and `Q8_K`
    - both `antfly inference smoke` and `antfly inference generate` expect a model directory with the `.gguf` plus tokenizer files, not an Ollama tag like `gemma3:4b-it-qat`
    - Gemma GGUF metadata with `general.architecture = gemma`, `gemma2`, or `gemma3` currently maps onto the native Gemma family path here
    - preferred first command for inspection:
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference smoke <gemma-gguf-dir> 'hi' --backend mlx --inspect-only`
    - first real-token check after inspection is clean:
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate <gemma-gguf-dir> 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --no-chat-template --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
    - validated MLX command for the small bring-up target:
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate /Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/termite-models/gemma-3-270m-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --no-chat-template --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
    - validated MLX command for the 4B QAT target:
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate /Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/termite-models/gemma-3-4b-it-qat-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
    - if the candidate Gemma export reports unsupported tensor types, pick a smaller dense GGUF that stays inside the current coverage set rather than debugging Mixtral first
  - then run the smoke path against the exact target Mixtral GGUF artifact
  - compare the reported GGUF tensor-type set with native quant coverage
  - fix any missing tensor-name/config mismatches exposed by real load
  - validate first-token and short decode output against a known reference
  - expand quant coverage only for formats the target artifact actually uses
  - validate `/generate` on the same model after the smoke path is clean
  - once the model is functionally runnable, replace dense-on-demand expert staging with quantized backend kernels in priority order:
    - BLAS `Q2_K` / `Q3_K`
    - BLAS `Q4_K` / `Q5_K` / `Q6_K` / `Q8_K`
    - BLAS packed-expert direct-quant execution
    - MLX packed-expert / direct-quant execution path through the backend wrapper
    - MLX device-side quant kernels

Implementation note:

- the current MoE execution path is hybrid:
  - routing is selected on CPU
  - tokens are grouped by expert on CPU
  - expert MLP execution runs natively on the active backend
  - expert outputs are scatter-merged on CPU
- expert caching/staging is currently request-local only:
  - request-local buffers still own per-request batching state
  - BLAS GGUF sessions now lazy-load expert tensors on first touch, can prefetch predicted experts, and now evict cold unpinned experts under a bounded model-scoped cache
  - MLX GGUF sessions now do the same through a two-stage lazy cache:
    - host `LoadedWeight` staging in RAM
    - backend `mlx_array` promotion on demand
    - backend eviction can now demote back to host instead of always dropping to disk
  - host and backend residency now have separate shared byte budgets
    - current budgets are heuristic defaults, not tuned per machine yet
  - native memory-safety guardrails now exist for `antfly inference generate`, `antfly inference smoke`, and the native `/generate` server path
    - each run estimates and reserves `kv` and `scratch` bytes up front
    - BLAS/MLX lazy weight promotion now checks tier budgets before allocating and fails with `MemoryBudgetExceeded` instead of allocating first
    - the tier planner now demotes large cold tensors toward disk more aggressively when budgets are tight
    - CLI native generation/smoke now expose explicit `--host-budget-mb`, `--backend-budget-mb`, `--kv-budget-mb`, and `--scratch-budget-mb` overrides
  - lazy tensor metadata is now owned by the tensor store layer rather than ad hoc backend strings
  - MoE predicted-expert prefetch now uses an explicit prefetch call path
  - queued prefetch is now decoupled from immediate staging and budget-drained on the generation thread
    - this amortizes prefetch cost across decode iterations
    - the queue and worker mechanics are now shared runtime infrastructure rather than backend-local lists
    - model-manager ownership now makes the prefetch tracking state explicit at the model level instead of only implicit in the cached session
    - repeated requests for the same lazy tensor now raise its queue priority at the model level
    - that priority now blends:
      - outstanding pending depth
      - recency of the last request
      - whether the tensor is still lagging behind recent demand
      - explicit MoE prediction-rank hints from the current decode step
      - predicted expert score/co-activation strength when the predictor can supply it
    - BLAS now has an off-thread worker for queued lazy loads
    - MLX now has an off-thread worker for host staging, but not for backend promotion
  - model-scoped routing hotness/co-activation state now survives across requests
  - the new planner is still heuristic and name-based
  - there is not yet a full NVMe placement planner
- this is enough to unblock native Mixtral support work, but not enough to claim Hypura-class performance

Acceptance:

- dense non-expert path and expert path both work natively
- correctness validated against known references

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

Goal:

- make native path fast, not just functional

Deliverables:

- MLX-specific staging optimizations
- BLAS prepared-weight paths
- future CUDA backend
- fused quantized matmul for hot formats
- per-backend benchmark dashboards

Status:

- flash attention: done (tiled online softmax for BLAS, native SDPA for MLX)
- advanced sampling: done (min-p, repetition/frequency/presence penalties)
- RoPE optimization: done (shared `ropeCore()` with flat position arrays for both `ropeOp` and `ropePerItemOp`)
- grammar constraint mask optimization: done (`TokenByteTable` for zero-alloc per-token lookup, `allowedTokenMaskFast()`)
- fused quantized matmul: done (SIMD vectorized dot product kernels for Q4_0/Q5_K/Q8_0, MLX Metal quantized kernels)
- paged-attention benchmark: done (`src/bench/paged_attention.zig`)

Remaining TODOs:

- extend benchmark into broader prompt/decode harness
- reduce per-step reshape and transpose churn in MLX paged attention
- per-backend benchmark dashboards
- future CUDA backend

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
