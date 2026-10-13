# Antfly inference LLM Plan

> Summary of a verbatim log. Full text: [docs/design/inference/history/llms-plan.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/llms-plan.md). Originally relocated verbatim from `zig/pkg/inference/LLMS.md` (lines 43–69 and 232–1112, based on commit 271838a195) on 2026-09-16 during the documentation cleanup, with the Phase 5 and Phase 10 MLX-era narrative from the removed `llms-stale-sections.md` folded in.

The original plan for native LLM generation: GGUF loading, a storage-agnostic tensor store, paged KV cache, delivery Phases 0–10, MoE/Mixtral support with tiered expert streaming, grammar-constrained decoding, and testing strategy. Much of it was written while the `mlx` backend still existed (since removed). Durable decisions are in [`LLMS.md`](../../../../zig/pkg/inference/LLMS.md) (Core Decisions, Open work).

## Decisions

- **Paged KV is the primary design, not a later optimization** — a contiguous monolithic KV tensor causes over-reservation, fragmentation, max-length cliffs, and awkward prefix sharing and sliding-window reclamation. A contiguous mode may remain for testing, debugging, and backend bring-up. Modeled on PagedAttention/vLLM block tables, TensorRT-LLM block pools, and ORT GenAI explicit KV management.
- **Physical KV pools keyed by** backend, element type, `num_kv_heads`, `head_dim`, and attention window class (handles MHA/MQA/GQA and mixed sliding windows). Each sequence owns a logical-block -> physical-block table.
- **Prefix cache reuses only full KV blocks;** partial tail blocks stay request-private (avoids correctness bugs while capturing most of the value). Keys: model id, tokenizer/chat-template hash, prefix hash chain, pool config.
- **Architecture code never manipulates raw cache pointers;** attention takes a backend-neutral `KvView` (pool, page indices, last page length, effective length, shared-prefix segment, sliding-window info).
- **Weights and KV are tiered differently:** active decode KV stays GPU-resident; order of work is paged GPU KV, then RAM spill for reusable prefixes, then NVMe KV offload only after scheduling and reuse are stable (KV offload is far more latency-sensitive than weight paging).
- **Scheduler favors decode steps first, bounded prefill chunks second,** with starvation protection. Request states: `queued_prefill`, `running_prefill`, `ready_decode`, `running_decode`, `finished`, `cancelled`.
- **Quantized KV is not a first-release requirement** (start `f16`/`bf16`; later `int8`/`fp8`), but pool dtype must not assume `f16`.
- **Quant format coverage follows runtime abstractions:** implement the full quant family only after dense + K-quant + MoE prove the runtime; expand coverage only for formats real target artifacts use. K-quant optimization path: scalar/SIMD decode into temp buffers, then prepared/panel-packed weights, then fused kernels where justified.
- **Native generation reserves KV and scratch budgets up front** and fails closed with `MemoryBudgetExceeded` instead of allocating first; lazy weight promotion checks tier budgets before allocating.
- **Grammar-constrained decoding** (JSON FSM, GBNF parser, JSON Schema -> GBNF compiler, `TokenByteTable` zero-alloc masks) requires the native backend; JSON Schema property-order enumeration is capped at 4 optional properties.

## Recommended implementation order (as planned)

1. GGUF parser and metadata. 2. Tensor store abstraction. 3. Dense GGUF execution. 4. KV cache and incremental decode. 5. K-quants needed for `Q5_K_M`. 6. Mixtral/MoE. 7. Tiered paging and expert cache. 8. Dense FFN streaming. 9. Full quant family. 10. Backend-specific optimization.

Success criteria: load GGUF with no mandatory duplicate runtime copy; run quantized GGUF including `Q5_K_M`; run Mixtral with MoE-aware scheduling; use GPU/RAM/NVMe tiers intentionally rather than OS swap; keep compute backend and model format cleanly separated.

## Status at the time of the move

| Area | Status recorded |
| --- | --- |
| GGUF parsing, tensor catalog, manifest discovery, GGUF-backed weight loading | done |
| Paged KV, fixed page size, block tables, sliding-window retirement | done |
| Direct page-table attention (BLAS; MLX via online blockwise softmax) | done |
| Continuous batching | done (`claimStep`/`completeStep` in `scheduler/native_generate.zig`; one fused forward pass per step) |
| Chunked prefill | done for single requests; multi-request interleaving TODO |
| Full-block prefix reuse | runtime support in `KvManager`; server-level reuse TODO (the `chat` CLI `--prompt-cache` attach path degraded metal decode and could hang native) |
| Speculative decoding, grammar decoding, min-p/penalty sampling, flash attention | done |
| K-quant codecs | `Q4_K`, `Q5_K`, `Q6_K`, `Q8_K` landed first; direct BLAS quant matmul later covered `Q8_0`, `Q2_K`–`Q8_K` |
| Placement planner, mmap paging, async prefetch | planner and mmap done; full NVMe tiering TODO |
| Benchmarks | `src/bench/paged_attention.zig`; broader prompt/decode harness TODO |

Still TODO at the move: NVMe KV offload, beam-search cache sharing, tiered expert streaming and storage placement, continuous batching interaction with MoE routing, correctness/perf validation against reference Mixtral outputs, per-backend benchmark dashboards, future CUDA backend.

## MoE / Mixtral (Phase 5, MLX-era)

- MoE execution was hybrid: routing and per-expert token grouping on CPU, expert MLP on the active backend, scatter-merge on CPU.
- GGUF expert tensors lazy-load on first touch with bounded, model-scoped hotness/co-activation caches that survive across requests; cold unpinned experts evict first; host and backend bytes have separate budgets (heuristic defaults, not tuned per machine).
- Lazy tensor refs are owned by the tensor store (`describeTensor`/`loadTensorRef`); prefetch is an explicit API with a shared queue/worker in `src/runtime/tier/prefetch.zig`, prioritized by pending depth, recency, and MoE prediction rank/strength.
- Native generate coordinator: model-scoped waiting room with phase tracking, decode-prioritized cooperative turns, then fused decode and prefill microbatches (one leader runs `gpt.forward(batch > 1)` and fans out logits), with `/metrics` scheduler counters.
- The planner was heuristic and name-based; the source states this was enough to unblock Mixtral work but not to claim Hypura-class performance.
- Bring-up guidance: validate a small dense GGUF (Gemma-family) first, then the target Mixtral artifact; compare its tensor-type set against native quant coverage before debugging.

## Risks recorded

Implementing all quant types before abstractions settle wastes effort; dequantize-into-temp may be too slow without caching; MoE without route-aware prefetch is correct but disappointing; GGUF metadata drifts over time, so parser/tests must be versioned and defensive.

## Testing strategy

Unit (GGUF parsing, codecs vs reference, KV correctness, router top-k), differential against reference implementations (logits on fixed prompts, deterministic next tokens, selected layer outputs for dense, K-quant, and Mixtral), E2E (`/api/generate`, streaming, chat templates, sliding window, expert routing), and performance (prompt/decode tok/s, p50/p95 token latency, staged and NVMe bytes/token, cache hit rates, prefetch usefulness).
