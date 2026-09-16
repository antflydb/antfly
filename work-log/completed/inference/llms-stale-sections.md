# LLM plan: MLX-era Phase 5 and Phase 10 narrative

> Relocated verbatim from `zig/pkg/inference/LLMS.md` (lines 890–1116 and 1202–1231 at commit 271838a195, prior to a lift pass that added minor context above these ranges) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`LLMS.md`](../../../zig/pkg/inference/LLMS.md). These sections predate the removal of the `mlx` compute backend (see `src/backends/backends.zig`'s `BackendType` enum, which now lists `native`, `onnx`, `metal`, `cuda`, `pjrt`, `wasm`) and describe a mix of dead MLX-specific code paths and native/BLAS behavior that was never re-verified against current source. Treat any specific claim below as unverified.

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
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate <models-dir>/gemma-3-270m-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --no-chat-template --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
    - validated MLX command for the 4B QAT target:
      - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate <models-dir>/gemma-3-4b-it-qat-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
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

