# Qwen3-VL qualification evidence (2026-08)

> Relocated verbatim from `zig/pkg/inference/QWEN3VL_SUPPORT.md` (lines 595–750 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`QWEN3VL_SUPPORT.md`](../../../../zig/pkg/inference/models/qwen/QWEN3VL.md). Durable decisions from this log were folded into that document before the move.

## Current status

The artifact catalog/downloader, configuration and projector validation,
request planner, Qwen projector, native and Metal M-RoPE/vision-RoPE kernels,
decoder/DeepStack integration, native generation entrypoint, and text-only
generative reranker path are implemented. Synthetic and differential unit
tests cover their strict contracts.

As of 2026-08-29, the pinned 2B landscape fixture passes exact rendered-prompt,
token expansion, 320x224 resize, `[1,14,20]` grid, 70 visual-token, complete
M-RoPE, delta, fallback-counter, resource, and greedy-token gates. Five serial
real-Metal processes produced one identical main-projector hash, one identical
three-tap DeepStack hash, one identical 151,936-value logit hash, and token
1986. Peak sampled RSS was 2.32 GiB, minimum free memory was 75 percent, and
swapout growth was zero. Against the official CPU BF16 checkpoint, Q4/Q8 kept
the same argmax with top-10 overlap 8/10, cosine similarity 0.9712, Pearson
correlation 0.9701, mean absolute logit error 0.8871, and RMSE 1.1253.

This lane also found and fixed a real stale-weight defect: the Metal dynamic
slot cache was keyed by transient projector tensor addresses, allowing allocator
reuse to execute vision layer 1 with an earlier layer's prepared weights. The
projector now retains a named request-scoped weight cache across every layer
and image, explicitly retires only those dynamic slots before freeing the
weights, and permits cleared slots to be reclaimed by later requests. The
correctness-first lifetime raises the observed 2B peak RSS from roughly 0.87
GiB to 2.32 GiB. Moving this cache to a model-scoped projector session and
eliminating redundant host mirrors is a required performance/residency follow-up,
not grounds to restore unsafe short-lived identities.

The standalone Transformers MPS comparator passes on the 16 GiB M4 Air. The
previous saturated-logit result was a host-transfer defect: copying the offset
final-row view directly from MPS returned corrupt values even though the device
tensor was valid. Cloning that row on-device before copying it to CPU restores
parity without the unsafe CPU-then-MPS duplicate-residency strategy.

As of 2026-08-30, the stage-profiled 2048x1416 photograph lane (532 visual
tokens) measures a 3.190-second FP16/SDPA Transformers MPS resident-forward
median after one warmup. Its median stages are 2.309 seconds in vision, 0.850
seconds in the decoder, 0.0065 seconds in the LM head, and 0.021 seconds
unattributed. The timed logits are bitwise deterministic and select token 1986.
The provenance-bound report is
`/private/tmp/qwen3vl-mps-fp16-sdpa-2mp-stage-profile-v1.json` with SHA-256
`e8002f3199068cca66be7a280c7bae9fa3bcc774efe36a508858d259febb9079`.

On the identical image, prompt, one-token cap, and Apple M4, the qualified
Antfly Q4_K_M decoder/Q8_0 projector profile now measures a 3.204-second
settled median from a freshly rebuilt production binary: 1.613 seconds in
vision/projector preparation and 1.590 seconds in decoder prefill. Antfly is
therefore 0.4 percent behind the matched MPS forward overall while its vision
lane is 30.1 percent faster; decoder prefill remains 1.87x slower and is the
next optimization boundary. Three settled runs selected token 1986 and
produced identical logits and image-patch hashes. Evidence lives under
`/private/tmp/qwen3vl-prepared-ffn-e2e-v2` through `v4`.

The dominant decoder shapes now use two model-neutral, opt-in matrix routes.
`TERMITE_METAL_ENABLE_Q4_K_HIGH_ROW_MM=1` accelerates the six Q4_K attention
and gate/up projections per layer, while
`TERMITE_METAL_ENABLE_Q6_K_HIGH_ROW_MM=1` accelerates the Q6_K FFN-down
projection. Force rollback with the corresponding `TERMITE_METAL_DISABLE_*`
switch. At the exact 558-row Qwen shape, one Q4_K gate/up projection
(`2048 -> 6144`) measures 10.426 ms GPU versus 33.710 ms on the register-tiled
fallback. A Q6_K attention-sized shape (`2048 -> 1024`) measures 2.864 ms versus
21.439 ms, and Q6_K FFN-down (`6144 -> 2048`) measures 17.005 ms versus
125.079 ms. The Q6 differential has maximum absolute error 1.62e-5, below the
existing 0.003 gate. Runtime telemetry reports 168 generated Q4_K dispatches
and 28 `metal.k_quant_dispatch.q6_high_row_mm_matrix` dispatches per request.

One Qwen-specific submission optimization reuses those same prepared kernels.
`TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN=1` executes the existing gated FFN
residual path after the whole-block fast path declines; settled evidence shows
28 direct successes and zero direct, backend, or runtime fallbacks. Enable and
rollback are controlled independently by
`TERMITE_METAL_DISABLE_QWEN3VL_PREPARED_FFN=1`, and compact timing JSON exposes
the `metal.prepared_gated_ffn` counters. The retained path preserves logits
SHA-256 `e752efd635b73d50661bb3d7be89ee0d370f19e0b219f143b19b9276d531499c`
and patch SHA-256
`5a4f565d34dad763af53b1094f693b521d376960f8eff34785c9d948aa8f466e`.

The complete opt-in qualification profile also requires
`TERMITE_METAL_ENABLE_VISION_SDPA_HD64_FLASH_Q32=1` for the Qwen vision
attention shape and `TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_SG_ATTENTION=1` for
direct prefill K/V. Prepared FFN additionally requires both
`TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_SLOTS=1` and
`TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_FRAME=1`; setting only the prepared-FFN
flag is intentionally a no-op. Each route retains its matching `DISABLE`
rollback. Qualification and performance reports must record the complete
environment and assert 28 direct-K/V calls, 28 prepared-FFN successes, 168
generated Q4_K dispatches, 28 generated Q6_K dispatches, and zero fallbacks.

The combined prepared Q/K/V submission was also evaluated but is not enabled
for Qwen3-VL. The corrected seven-sample benchmark hashes all three outputs and
reports the same `762cc188da6970a8` hash for both routes; combined submission
measures 7.134 ms GPU versus 7.105 ms for the existing three submissions. A
shorter sample had suggested a small win, but the hard-hash rerun reduced it to
noise and slightly favored the simpler existing path.

The pinned BF16 reranker text lane also passes the frozen Transformers and
real-Metal gate. The three-document oracle logits are `[1.171875, -1.0546875,
-0.890625]`, scores are `[0.7634837, 0.2583260, 0.29098085]`, and ranking is
`[0, 2, 1]`. Two independent Metal processes produced bitwise-identical logits
`[1.2202648, -1.0643585, -0.9209601]`, scores `[0.77211016, 0.25647742,
0.28476232]`, and the same ranking. Maximum absolute logit and score errors
were 0.0484 and 0.00863 respectively; prompt and active token IDs were exact.
The CPU oracle peaked at 3.18 GiB sampled RSS, Metal processes at 222 MiB
process RSS, minimum free memory was 73 percent, and swapout growth was zero.
All 34 gates passed. The local report is
`/private/tmp/qwen3vl-reranker-text-metal-v8.json` with SHA-256
`fa6b26051c93c9223ade35091513979ece26e329628d9fb0940be9e7cf5e66ce`.

That lane exposed and fixed two semantic defects rather than relaxing the
gate. The tokenizer now recognizes Qwen's exact isolated Split regex instead
of applying GPT-2 boundaries, preserving the official `?\n` and `<Document`
BPE merges. Batched reranking now supplies explicit three-axis positions;
the scalar RoPE API cannot infer batch size independently from head count in a
flattened tensor. A single-document diagnostic already matched Transformers,
and the explicit position contract brought the full batch within tolerance.

The Pillow-compatible antialiased bicubic path now repairs the high-resolution
resize/normalization failure. On the same 2048x1416 photograph, the development
qualification reports mean absolute patch error 0.0000244, RMSE 0.000437,
p99 absolute error 1.19e-7, and maximum absolute error 0.00784 across 1,634,304
values. Two complete opt-in Metal runs pass all 28 gates, select token 1986,
and produce identical projector, DeepStack, and logit hashes. Their median
generation time is 3.242 seconds: 1.632 seconds in vision/projector preparation
and 1.609 seconds in decoder prefill. Peak sampled RSS is 491 MiB and swapout
growth is zero. The report is
`/private/tmp/qwen3vl-2mp-pillow-parity-perf-v5.json` with SHA-256
`398be4795a859bb456d484d85946fe3dca758f2f55bfba72648a901c954dbbf3`.

The reranker now has a reproducible calibrated Q8 candidate. Two independent
BF16-to-GGUF passes produced decoder SHA-256
`77d166d8dba7f157b2c770db642b70ebc32dbdc8cf2d69aebdf44b3dfea24aef`
and projector SHA-256
`62135d45fbed2dfb3d047ef7a84eb04ed97b1721267bdea7e5a6185e08c95ba0`;
the managed receipt SHA-256 is
`0518cecea978bb0a1f71429ad4fb1c23ad553bee9e8c33058f8b72d6bb89046b`.
The live decoder catalog contains 311 tensors and preserves the `[2048,2]`
semantic classifier as F16. The projector contains 316 tensors. The standalone
conversion report is `/private/tmp/qwen3vl-reranker-q8-conversion-report.json`
with SHA-256
`a3cdf935ae893d88c147eb94c329d5804a214f4c21e8eef78e291b90a160713d`.

That exact receipt passes the new small-image and 2048x1416-image calibrated
gates. The 2 MP case matches the official `[1,38,56]` grid, 532 expanded visual
tokens, prompt IDs, visual mask, and axis-major M-RoPE positions. BF16
Transformers scored `0.52220219` from logit `0.08886719`; Q8 Metal scored
`0.53861177` from logit `0.15475526`. Two isolated Metal processes were
bitwise identical, used about 378 MiB sampled RSS, and observed zero swap
growth. Their 15.7-17.0 second guarded process times compare with 116.5 seconds
for the BF16 CPU oracle at the same end-to-end boundary. The report is
`/private/tmp/qwen3vl-reranker-2mp-q8-qualified-v3.json` with SHA-256
`51c07125ede7b0f6cafdd223350f18a4fff11961c553aa6db02bd08b558c3cd3`;
it binds ReleaseSafe+Metal binary SHA-256
`799fdb1d9a6d9851f95f9b7aa5e109844aa17877ff1d33e33985f2283ee3ddae`.
This is not a Transformers MPS comparison.

