# TurboQuant Implementation History

> Relocated verbatim from `zig/pkg/inference/TURBOQUANT.md` (lines 376–636 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`TURBOQUANT.md`](../../../zig/pkg/inference/TURBOQUANT.md). Durable decisions from this log were folded into that document before the move.

## History

`polar4` and `turbo3` landed incrementally, in this order: codec module and
`KvDType` plumbing, native direct-key paged attention, native SIMD scoring,
QJL residual (`turbo3`), WebGPU shaders, and Metal kernels. The chronological
log below is kept as evidence for the measurements it contains.

Several entries below describe an interim Apple-GPU acceleration path built
on an "MLX provider" (`mlx_quant_metal.m`, `ANTFLY_INFERENCE_MLX_*` /
`TERMITE_MLX_*` env vars). That provider and the MLX backend it belonged to
have since been removed from the codebase — none of those env vars or files
exist today (`src/backends/backends.zig`'s `BackendType` enum has no `mlx`
variant). The MLX-era entries are kept as a historical record of how the
compressed-key scoring path was validated on Apple GPUs at the time; they do
not describe current runtime behavior. The Metal-native path that exists
today (`src/ops/metal_compute.zig`, `src/ops/metal/`) was built independently
of that MLX path.

- Done: `polar4` codec module with supported-shape checks, packing, fallback
  decode, and direct decoded-code dot product.
- Done: `KvDType.polar4` parsing, asymmetric K/V row sizing, pool allocation,
  write/read fallback, encoded-key reads, and V-only decode reads.
- Done: native paged attention dispatches `polar4` to a direct encoded-key
  scoring path while decoding only V for accumulation.
- Done: CLI usage and `KVCACHE.md` mention `polar4`.
- Done: paged-attention benchmark accepts `--cache-dtype polar4` and reports
  asymmetric K/V row bytes plus total per-token-pair bytes.
- Done: native coverage compares the `polar4` direct-key paged-attention path
  against the decode-fallback reference across GQA grouping and page boundaries.
- Done: first native packed-vector `polar4` dot helper for `head_dim=64` and
  `head_dim=128`, with scalar-reference coverage.
- Done: paged-attention benchmark dtype sweep mode reports f32, f16, int8, fp8,
  int4, and `polar4` side by side, and marks unsupported `polar4` head dims
  explicitly.
- Done: WebGPU cached-attention ABI has a format-aware entrypoint with key/value
  format enums, row-byte fields, and an auxiliary key buffer slot for `turbo3`;
  `f32` and `polar4` route to separate shaders behind that ABI.
- Done: WASM GPU KV cache creation/upload can allocate `polar4` packed key
  buffers with f32 values, encode K rows on upload, and expose `cacheDtype:
  "polar4"` through the GPT web cache wrapper.
- Done: standalone WebGPU browser numeric harness compares the `polar4`
  cached-attention shader against a JS packed-key reference on deterministic
  tensors.
- Done: Chrome browser run of the standalone WebGPU `polar4` numeric harness
  reported `maxAbs=1.192093e-7`, `rms=4.375033e-8`, and `PASS`.
- Done: initial shared `turbo3` base-key path has 3-bit packed key storage,
  fallback decode, native direct encoded-key scoring, parsing, CLI usage text,
  and paged-attention benchmark sweep coverage.
- Done: WASM/WebGPU can allocate and upload packed `turbo3` base-key GPU KV
  caches, route `GQA_K_FORMAT_TURBO3` through a dedicated cached-attention
  shader, and expose a standalone browser numeric harness.
- Done: codec-level QJL-style residual sketch support for `turbo3`: fixed
  one-bit projections per KV head, deterministic projection signs, sketch
  generation from base-key residuals, and a scalar query-dependent estimator
  with unit coverage.
- Done: `bench-turboquant-distortion` reports deterministic dot-product
  distortion for `polar4`, base `turbo3`, and residual-scale sweeps so residual
  correction can be calibrated before being wired into attention logits.
- Done: MLX provider ABI has compressed-key score dispatch, Metal fast kernels
  for `polar4` and base `turbo3`, scalar-reference tests, paged-attention
  dispatch checks, and decoded-key fallback when the provider is unavailable.
- Done: Chrome browser run of the standalone WebGPU `turbo3` numeric harness
  reported `maxAbs=1.490116e-7`, `rms=4.560416e-8`, `worstIndex=271`, and
  `PASS`.
- Done: native `.turbo3` KV rows append the residual sketch after the base
  3-bit key bytes, and native direct paged attention adds the calibrated
  residual estimate to key logits.
- Done: WASM/WebGPU `.turbo3` KV upload now stores base key bytes followed by
  the residual sketch, the WebGPU row checks accept the larger row, and the
  dedicated cached-attention shader adds the residual estimate before softmax.
- Done: MLX `.turbo3` compressed-key scoring now consumes the full key row,
  keeps fallback behavior when the provider declines a shape, and adds the Metal
  residual estimate to key logits.
- Done: Chrome browser run of the standalone WebGPU `turbo3` residual numeric
  harness reported `baseBytes=48`, `residualBytes=8`, `totalBytes=56`,
  `maxAbs=1.490116e-7`, `rms=4.972192e-8`, `worstIndex=5`, and `PASS`.
- Done: native residual scoring now precomputes the 32 query projections once
  per query/head instead of once per KV token. On the local native sweep
  (`prompt_len=128`, `decode_steps=32`, `heads=8/2`, `head_dim=64`), `turbo3`
  remained at `kv_pair_bytes=192` and improved from `23.813 ms/token` before
  the hoist to `1.962 ms/token`.
- Done: local MLX dtype sweep on the same shape reported `turbo3`
  `kv_pair_bytes=192` and `decode_paged_ms_per_token=0.312`, versus `polar4`
  `kv_pair_bytes=200` and `decode_paged_ms_per_token=0.201`.
- Done: Chrome browser rerun after the WebGPU projection hoist reported
  `baseBytes=48`, `residualBytes=8`, `totalBytes=56`,
  `maxAbs=1.490116e-7`, `rms=4.972192e-8`, `worstIndex=5`, and `PASS`.
- Done: local `antfly inference generate` smoke on GPT-2 with native `--cache-dtype
  turbo3` generated 64 tokens with `prefill=90 ms`, `decode=3731 ms`, and
  `generate=3822 ms`, or about `17.15 decode tokens/sec`. A matching native
  f32 run generated 64 tokens with `decode=4008 ms`, or about
  `15.97 decode tokens/sec`.
- Done: fixed MLX dense linear orientation for GPT-2 Conv1D-style `[in_dim,
  out_dim]` weights. Local GPT-2 MLX `--cache-dtype turbo3` generate now
  completes 64 tokens with `prefill=474 ms`, `decode=2886 ms`, and
  `generate=3361 ms`, or about `22.18 decode tokens/sec`. A matching MLX f32
  run generated 64 tokens with `decode=2449 ms`, or about
  `26.13 decode tokens/sec`.
- Done: dense MLX decode now defaults to a full decoder-stack eval stride,
  with `TERMITE_DENSE_DECODE_EVAL_STRIDE` as a rollback/tuning knob. On local
  GPT-2 MLX generate, the explicit eval count for 64 tokens dropped from `384`
  to `69`; f32 decode improved from `2449 ms` to `2209 ms`, and `turbo3`
  improved from `2886 ms` to `2081 ms`.
- Done: dense MLX decode now defaults to no explicit decoder-layer evals,
  letting the final token read force evaluation of the lazy full-stack graph.
  `TERMITE_DENSE_DECODE_EVAL_STRIDE=2` restores the old dense-decode barrier
  cadence, and positive values keep the layer-group tuning path available.
  Local GPT-2 MLX generate with the new default reported `eval_count=6`; f32
  decode was `2222 ms`, while `turbo3` decode improved to `1459 ms`, or about
  `43.9 decode tokens/sec`.
- Done: added an experimental MLX greedy decode token path behind
  `ANTFLY_INFERENCE_MLX_GREEDY_DEVICE_DECODE=1`. It uses the backend argmax path after
  the first generated token for pure greedy, grammar-free paged decode, avoiding
  full-vocab CPU logits downloads. On local GPT-2 this did not beat the default
  no-explicit-eval path: `turbo3` decode was `1558 ms` and f32 decode was
  `2270 ms`, so the path stays opt-in while deeper MLX decode ownership is
  investigated.
- Done: added `METAL.md` and an opt-in direct-Metal LM-head argmax hook behind
  `ANTFLY_INFERENCE_MLX_METAL_LM_HEAD_ARGMAX=1`. The Metal-provider unit compares the
  token id against scalar `hidden @ W^T` argmax. With
  `ANTFLY_INFERENCE_MLX_GREEDY_DEVICE_DECODE=1`, local GPT-2 MLX/turbo3 decode improved
  from the MLX-argmax opt-in result (`1558 ms`) to `1477 ms`, roughly matching
  the default no-explicit-eval path (`1487 ms` in the rerun).
- Done: added an opt-in MLX device-token handoff path behind
  `ANTFLY_INFERENCE_MLX_DEVICE_TOKEN_HANDOFF=1`. It can seed the generated token as an
  MLX integer tensor, feed backend tensor ids into the next embedding lookup,
  and preserve the direct-Metal LM-head token tensor across greedy paged-decode
  steps. The path remains opt-in because local GPT-2 MLX/turbo3 with
  `ANTFLY_INFERENCE_MLX_DEVICE_TOKEN_HANDOFF=1 ANTFLY_INFERENCE_MLX_METAL_LM_HEAD_ARGMAX=1`
  measured `decode=1543 ms`, slower than the default no-explicit-eval rerun
  (`1487 ms`) and slightly slower than a same-session direct LM-head greedy
  comparison without handoff (`1517 ms`).
- Done: added an opt-in compressed-KV decode attention block behind
  `ANTFLY_INFERENCE_MLX_METAL_COMPRESSED_ATTENTION_BLOCK=1`. For qLen=1 `polar4`/`turbo3`
  paged decode, the Metal provider can fuse compressed key scoring,
  causal/sliding masking, online softmax state update, and V accumulation for
  one KV block. Local GPT-2 MLX/turbo3 generated the same greedy stream, with
  `mlx_paged_decode.mask=0`, but measured `decode=3004 ms`; this remains
  opt-in while the next iteration removes per-block encoded-key uploads and
  per-block kernel launches.
- Done: cached encoded compressed-key MLX arrays on the per-block MLX KV cache
  entry. The opt-in compressed attention block path and the compressed-score
  fallback now reuse those arrays instead of rebuilding/uploading encoded key
  bytes for every block visit. Local GPT-2 MLX/turbo3 with
  `ANTFLY_INFERENCE_MLX_METAL_COMPRESSED_ATTENTION_BLOCK=1` improved slightly to
  `decode=2962 ms`, confirming that per-block kernel launch/object overhead is
  the larger remaining issue.
- Done: added a separate opt-in compressed-KV span path behind
  `ANTFLY_INFERENCE_MLX_METAL_COMPRESSED_ATTENTION_SPAN=1`. It maintains persistent
  per-layer gathered V and encoded-key arrays across qLen=1 decode steps, then
  runs one Metal kernel over the retained KV span instead of launching once per
  KV block. Local correctness smoke passed, but the warmed 64-token GPT-2
  MLX/turbo3 run measured `decode=4473 ms`. The paged-attention counters dropped
  (`mlx_paged_decode.total=15 ms`), but whole-token decode regressed because the
  span kernel serializes too much work per query head.
- Done: split the block and span toggles so the cheaper cached block experiment
  remains available independently. With
  `ANTFLY_INFERENCE_MLX_METAL_COMPRESSED_ATTENTION_BLOCK=1`, the latest local GPT-2
  MLX/turbo3 rerun measured `decode=2872 ms`, still correct and still slower
  than the default MLX/turbo3 path.
- Done: added chunked span partials under the existing
  `ANTFLY_INFERENCE_MLX_METAL_COMPRESSED_ATTENTION_SPAN=1` path for retained spans over
  32 tokens. The partial kernel computes per-head/per-chunk softmax state and
  weighted V, then a reduce kernel merges the chunks. The 64-token GPT-2
  MLX/turbo3 span run improved from `decode=4473 ms` to `decode=3673 ms`, but
  remains slower than the default and cached per-block paths.
- Done: hoisted turbo3 residual query projections out of the per-token scoring
  loops in the compressed attention block, span, and chunked-span kernels. The
  same 64-token GPT-2 MLX/turbo3 smoke now measures default `decode=1512 ms`,
  cached per-block `decode=1605 ms`, and chunked span `decode=1544 ms`
  (`1536 ms` warmed). This makes the span path roughly competitive for GPT-2,
  though still opt-in because it is not a consistent win yet.
- Done: hoisted the same turbo3 residual query projection work out of the
  default `compressedKeyScores` Metal kernel used by the non-span paged decode
  path. A warmed same-session 64-token GPT-2 MLX/turbo3 default rerun measured
  `decode=1240 ms`, which re-establishes the default path as the faster GPT-2
  option in the current code.
- Done: started the backend-owned raw-Metal whole-token bring-up with a
  session-owned decode runtime in `mlx_quant_metal.m`. It now owns persistent
  device/queue/library state plus reservable scratch/token buffers, so the next
  steps can build an actual GPT-2 greedy `qLen=1` decode loop below
  `mlx_fast_metal_kernel`.
- Done: threaded the whole-token bring-up flag through the MLX generation loop.
  `TERMITE_MLX_RAW_METAL_WHOLE_TOKEN=1` now prepares that runtime during the
  narrow decoder-only greedy paged-decode path and then falls back to the
  existing MLX token execution.
- Done: moved the absolute token-input slice to resident Metal-owned embedding
  tables. The raw whole-token runtime now uploads `wte`/`wpe` once, and the
  per-token entry point passes only `token_id` and `position_id` to produce the
  positioned hidden input inside Metal.
- Done: moved GPT-2 layer-0 attention pre-norm (`h.0.ln_1`) into the same
  raw-Metal runtime as a resident layer-norm slot. The whole-token bring-up now
  runs token embedding, absolute position add, and the first decoder pre-norm
  on Metal before falling back to MLX. The latest unsandboxed GPT-2 MLX/turbo3
  smoke with `TERMITE_MLX_RAW_METAL_WHOLE_TOKEN=1` showed
  `raw_whole_token_prepare_layer_norm_calls=1`,
  `raw_whole_token_apply_layer_norm_calls=3`, and
  `gpt_timing_ms.attn_norm=0`, confirming the override is live.
- Done: moved GPT-2 layer-0 fused attention projection (`h.0.attn.c_attn`) into
  the same raw-Metal runtime as a resident dense-linear slot. The whole-token
  bring-up now runs `embed -> ln_1 -> c_attn` on Metal for layer 0 before
  falling back to MLX. The latest unsandboxed GPT-2 MLX/turbo3 smoke showed
  `raw_whole_token_prepare_linear_calls=1`,
  `raw_whole_token_apply_linear_calls=3`,
  `input_successes=3`, and decode improved from `1054 ms` to `872 ms` for the
  4-token greedy check.
- Done: moved GPT-2 layer-0 attention output projection (`h.0.attn.c_proj`)
  into the same raw-Metal runtime as a second resident dense-linear slot. The
  latest unsandboxed GPT-2 MLX/turbo3 smoke showed
  `raw_whole_token_prepare_linear_calls=2`,
  `raw_whole_token_apply_linear_calls=6`, and the whole-token greedy check
  remained correct with `decode=918 ms`. That is still better than the earlier
  `1054 ms` baseline, though not better than the `872 ms` run that only owned
  `c_attn`.
- Done: moved the rest of the layer-0 GPT-2 MLP shell into the raw-Metal
  runtime: `h.0.ln_2`, `h.0.mlp.c_fc`, and `h.0.mlp.c_proj`. The latest
  unsandboxed GPT-2 MLX/turbo3 smoke showed
  `raw_whole_token_prepare_layer_norm_calls=2`,
  `raw_whole_token_apply_layer_norm_calls=6`,
  `raw_whole_token_prepare_linear_calls=4`,
  `raw_whole_token_apply_linear_calls=12`, and the whole-token greedy check
  remained correct with `decode=968 ms`.
- Done: moved the layer-0 GPT-2 activation and both residual adds into the
  raw-Metal whole-token runtime. The latest unsandboxed GPT-2 MLX/turbo3 smoke
  showed `raw_whole_token_apply_activation_calls=3`,
  `raw_whole_token_apply_add_calls=8`, preserved the greedy stream
  (`the!!!`), and measured `decode=1016 ms` for the 4-token check. This keeps
  more of layer 0 inside the raw-Metal strip, but the main remaining cost is
  still the MLX-owned attention core and later decoder layers.
- Done: routed qLen=1 paged decode through a whole-token backend attention op
  that updates KV once and then calls a raw-runtime compressed span kernel
  below `mlx_fast_metal_kernel`. The latest unsandboxed GPT-2 MLX/turbo3 smoke
  showed `raw_whole_token_attention_span_calls=48`, preserved the greedy stream
  (`the!!!`), and measured `decode=1026 ms` for the 4-token check. This proves
  the attention core moved onto the raw-runtime span path, but it is not yet a
  win on this short GPT-2 case; the remaining cost is likely the gathered-KV
  host/MLX boundary and later-layer fallback.
- Done: added a suffix-only resident-span update path for the raw whole-token
  attention runtime. When qLen=1 decode just appends one new retained KV row,
  it now slices only the suffix encoded-key row on the MLX side and asks the
  raw runtime to memmove the retained resident span and copy only the appended
  encoded-key/V rows. The latest unsandboxed GPT-2 MLX/turbo3 smoke preserved
  the greedy stream (`the!!!`) and moved `decode` from `1140 ms` to
  `1116 ms`, but `gpt_timing_ms.attn_qkv=854` still dominates. That means the
  next meaningful win is to move more than layer 0 off MLX, not another small
  span-upload tweak.
- Done: extended the raw whole-token GPT-2 slot preparation from layer 0 to
  layers 0 and 1 and added a positioned-embedding greedy override entry so the
  GPT path consumes those raw slots directly instead of generation manually
  applying layer-0 norm/QKV. The latest unsandboxed GPT-2 MLX/turbo3 smoke
  measured `decode=1093 ms`, `raw_whole_token_prepare_layer_norm_calls=4`,
  `raw_whole_token_prepare_linear_calls=8`,
  `raw_whole_token_apply_layer_norm_calls=12`,
  `raw_whole_token_apply_linear_calls=24`, and
  `raw_whole_token_attention_span_calls=48`. This does move more of the token
  loop under the raw-Metal whole-token entry, but it is still not numerically
  correct relative to default MLX greedy decode: raw whole-token emitted
  `the!!!` while the default path emitted `the the the the` for the same local
  command. So the next blocker is correctness of the raw whole-token math/path,
  not adding still more layers.
- Pending: model-level quality gates using real model weights.
