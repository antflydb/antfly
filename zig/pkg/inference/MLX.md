# MLX + Metal Quant Path

This document describes the intended runtime split for native GGUF execution on Apple Silicon.

## Goal

Use MLX as the main compute/runtime layer while adding a narrow Metal extension path for the quantized kernels MLX does not currently provide through the `mlxc` surface used here.

This is not a separate full Metal backend.

The design target is:

- antfly inference owns tensor storage, tiering, prefetch, routing, and execution-mode choice
- MLX owns the general tensor runtime and dense compute path
- Metal owns only the missing quantized kernels

## Runtime Split

### Antfly inference

- GGUF tensor store
- packed expert views
- tier planner across `disk -> host -> backend`
- route-aware expert prefetch
- execution-mode selection:
  - `device_native`
  - `backend_dense`
  - `wrapper_direct_quant`

### MLX

- dense tensor runtime
- attention / KV cache / norms / activations
- dense linear fallback
- CPU/GPU dependency handling for MLX-managed ops

### Metal Provider

- quantized linear kernels only
- starts with MoE expert linears
- later expands to more dense projection weights if worth it

## Data Flow

1. `TensorStore` returns a logical tensor ref or packed expert view.
2. `TierPlanner` ensures the weight is available in `host` or `backend`.
3. The MLX backend builds a quantized linear plan request.
4. The provider chooses:
   - `device_native`
   - `backend_dense`
   - `wrapper_direct_quant`
   - `unsupported`
5. If `device_native` is chosen, the Metal provider runs the quantized matmul.
6. Otherwise:
   - `backend_dense` uses staged MLX arrays
   - `wrapper_direct_quant` uses the existing shared quant wrapper path
7. The result rejoins the MLX graph.

## Current Code Boundary

- provider interface: `src/backends/mlx_quant.zig`
- MLX dispatch point: `src/ops/mlx_compute.zig`
- session wiring: `src/architectures/session_factory.zig`

The current default provider is intentionally a no-op. This keeps the interface stable while the implementation is still missing.

## Provider Contract

The provider receives:

- `LinearNoBiasPlanRequest`
  - quantized tensor type
  - packed-expert flag
  - whether a staged backend-dense MLX weight already exists
  - logical matrix shape
- `LinearNoBiasRequest`
  - MLX input array
  - MLX weight handle
  - quantized storage metadata
  - stream

This allows the provider to answer two separate questions:

1. Should this op use `device_native` at all?
2. If yes, can it produce an MLX-compatible output array?

## Fallback Order

Default order:

1. `device_native`
2. `backend_dense`
3. `wrapper_direct_quant`

This keeps Metal as an accelerator, not a replacement runtime.

## Why Not A Full Metal Backend

- we already have MLX attention, KV, and dense runtime support
- a full second Apple backend would duplicate too much logic
- the highest-value missing path is quantized linear on GPU

## Immediate Implementation Plan

1. Landed: a first real Metal-backed `mlx_quant` provider implementation.
2. Landed first scope: `Q4_0`, `Q8_0`, `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`, and `Q8_K` linear, including packed expert views after Zig-side row extraction.
3. The current Metal path now borrows MLX input data directly and uses no-copy Metal buffers for input/weight staging where the MLX C surface allows it.
4. Remaining interop gap: output still re-enters MLX through array creation, so this is reduced-copy interop, not full zero-copy MLX/Metal tensor sharing.
5. Keep outputs MLX-compatible so the rest of the graph stays unchanged.
6. Only after that, consider widening the Metal path beyond MoE expert linears.

## Bring-Up Order

Start MLX validation with a small GGUF before using Mixtral or other MoE artifacts.

- recommended first target:
  - a Gemma-family text-only GGUF such as local `gemma3` 270M first, then `gemma3` 4B QAT once the small path is clean
- first pass for inspection:
  - `ulimit -n 65536 && ./zig-out/bin/antfly inference smoke <gemma-gguf-dir> 'hi' --backend mlx --inspect-only`
- first token:
  - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate <gemma-gguf-dir> 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --no-chat-template --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
- validated small-model command:
  - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate /Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/termite-models/gemma-3-270m-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --no-chat-template --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
- validated 4B QAT command:
  - `ulimit -n 65536 && ./zig-out/bin/antfly inference generate /Users/ajroetker/go/src/github.com/antflydb/antfly-inference-zig/termite-models/gemma-3-4b-it-qat-gguf 'hi' --backend mlx --max-tokens 1 --prefill-chunk-size 64 --print-chat-template-status --print-prompt --print-token-ids --print-finish-reason`
- model layout requirement:
  - `antfly inference smoke` and `antfly inference generate` take a directory containing the `.gguf` and tokenizer files; an Ollama tag such as `gemma3:4b-it-qat` is only a source artifact identifier, not the direct CLI argument here
- reason for the ordering:
  - this isolates GGUF parsing, tokenizer discovery, MLX residency, and first-token generation without mixing in MoE routing, packed experts, or Mixtral-specific debugging

## Non-Goals

- replacing MLX as the default Apple runtime
- pushing tier-planning into MLX
- treating MLX as the NVMe/RAM/GPU scheduler

That scheduler remains termite's job.
