# CUDA Tuning Profile

Gemma 4 QAT CUDA measurements must use the shared profile in
`scripts/gemma4/gemma4_qat_cuda_tuning.sh`. The CLI benchmark and server wrapper both
source it so production route settings stay aligned.

## EmbeddingGemma 2 CUDA image residency

Pure-image groups and the direct image embedding pipeline retain resized patches,
vision activations, projected soft tokens, and packed encoder inputs on CUDA.
Compressed image decoding and tokenization remain on CPU; decoded RGB uploads once
per image. CUDA performs Pillow-compatible resizing and patch extraction. Only
final vectors download (3,072 bytes per 768-dimensional FP32 vector). Grouped
requests truncate to the requested dimensions on CUDA; the direct pipeline
preserves its 768-dimensional return contract.
Mixed text/media groups retain the ordered-content frontend, including manual
placeholders, and compose text/media rows on device. Text lookup/scaling retains
the BF16 22.625 boundary. The ordered-content handoff no longer reads back
intermediate embeddings; transfer telemetry still exposes diagnostic or operation
fallbacks within the towers.

CUDA image cohorts default to one image to preserve the existing BF16 arithmetic.
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_IMAGE_BATCH=1` enables experimental cohorts
of up to four compatible images with bounded preparation lookahead. Attention is
isolated per image, but larger GEMMs can change final vectors; this option is not
qualified for exact-output workloads. Metal cohort policy is unchanged. Broker
keys include expanded token limits, so requests with different limits cannot share
a cohort. CUDA audio coalescing remains disabled.

The optional fused vision-attention path requires a discoverable
`libcudnn.so.9` (module minimum: cuDNN 9.13). Add its directory to
`LD_LIBRARY_PATH` when it is not installed system-wide. Plans that decline
retain the existing fallback. For diagnostics, set
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_STATS=1` and inspect
`embedding_gemma2_cudnn_stats` for `vision_executes` and `unavailable`.

## Production Defaults

The tuned profile currently uses these conservative defaults:

- Q4_0 x Q8_1 generated LM-head argmax: on.
- Generated Q6_K x Q8_1 LM-head argmax: off.
- Generated decode attention: off.
- Generated paged score-prework attention: evidence-bound automatic selection
  on qualified SM89 Gemma 4 F16 routes; explicit off remains the rollback.
- Generated E2B FFN coupled pair/down, exact-F32, and pair-only kernels: off.
- SM89 GGML-Q8_1 E2B FFN and BF16 cuBLASLt first-use tuning profiles: off.
- GQA prefill: typed `required-fast` profile; an unavailable or ineligible fast
  route fails the performance run instead of silently falling back. Set
  `ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT=1` (with no explicit profile) to keep
  the runtime's unset automatic selector, which engages the promoted SM89
  Flash route described below. Headline
  evidence must observe that route independently for Gemma 4's 256-d local
  heads and 512-d global heads (8 query heads / 1 KV head); route telemetry is
  deduplicated by exact `(route, head_dim)`, not by the first model-wide hit.
- Tiled F16 exact and warp-specialized GQA prefill profiles: experimental and
  off unless selected by their explicit typed profile.
- Q4_0 BF16 tensor-core (W4A16) prefill projections (`q4_0_tc_hmma_prefill`):
  a runtime default on SM89, not a wrapper setting. Multi-row prefill
  projections use BF16 tensor cores while single-row decode keeps DP4A,
  because BF16 activations track the F32-activation reference argmax and the
  DP4A Q8_1 path does not (96/96 vs 30/96 greedy-token match on a chunked
  prefill run). Rollback: `ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0`.
- Continuous generation batching: off.
- Request-scoped persistent CUDA graph replay: required.

Run the tuned server with:

```sh
zig/pkg/inference/scripts/gemma4/with_gemma4_qat_cuda_tuning.sh \
  zig/pkg/inference/zig-out/bin/antfly-inference run \
  --host 127.0.0.1 --port 8080 --models-dir .models --config server.json
```

With its default graph profile, the wrapper sets
`ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET=1` and disables continuous
batching so graph state cannot cross active requests. The default graph KV
capacity is 544 tokens. For longer requests, set a capacity covering prompt
and output tokens, rounded up to a KV page:

```sh
ANTFLY_CAPTURE_FORCE_KV_CAPACITY=1056 \
  zig/pkg/inference/scripts/gemma4/with_gemma4_qat_cuda_tuning.sh \
  zig/pkg/inference/zig-out/bin/antfly-inference run \
  --host 127.0.0.1 --port 8080 --models-dir .models --config server.json
```

The pinned temporary arena is planned at runtime. It observes two identical
eager decode iterations, pins the complete ordered byte-length layout, and
validates one pinned iteration before CUDA graph capture is allowed. A changed
allocation count or shape invalidates the plan and retrains it, so model,
architecture, kernel-schedule, and optional-fusion changes cannot silently
reuse stale graph pointers. `ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN=0`
disables this behavior. `ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD` and
`ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP` remain diagnostic-only fixed-schedule
overrides. Request reset retains the learned shape as a seed but requires each
new request to prove it once before pinning addresses.
Graph replay reconstructs the captured allocation interval from that learned
shape and validates the complete iteration, including uncaptured prefix and
tail work. A phase/count mismatch invalidates the graph before launch.

Before installing learned slots, CUDA sums the byte length of every distinct
pinned allocation ordinal. That sum is the current one-buffer-per-allocation
arena's simultaneous physical high-water request and must fit the request's
scratch, backend, and combined memory limits. With no request budget, the
ceiling defaults to 512 MiB. `ANTFLY_INFERENCE_CUDA_TEMP_ARENA_MAX_MB` may
narrow this ceiling but cannot widen request-level policy. An oversized or
overflowed plan is disabled before any new pinned allocation: automatic graph
mode remains eager, while required graph mode fails through its normal
`CudaGraphReplayRequired` path. The `cuda_temp_arena_plan` activation or
admission-denial log records slots, physical high-water bytes, and budget
bytes; runtime stats retain current/lifetime high-water and denial counts.

Q4_0 weight residency has two distinct experimental modes. Direct BF16
residency (`ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16=1`)
replaces the quantized execution copy and therefore affects both prefill and
single-row decode. Hybrid residency
(`ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL=1`) retains Q4_0 for decode
and adds BF16 mirrors used only for multi-row prefill. They are mutually
exclusive in the shared tuning wrapper; setting both is rejected instead of
silently selecting one policy. Neither is a production default. Candidate
runs must pass backend/combined admission, exact prompt-ID and semantic gates,
and separate prefill/decode measurements before promotion.

Six typed GQA prefill candidates are accepted for explicit experiments:
`tiled-f16-exact`, `required-tiled-f16-exact`, `tiled-f16-warp`, and
`required-tiled-f16-warp`, plus the SM89-only `flash-f16-sm89` and
`required-flash-f16-sm89`. The `required-` forms fail closed instead of using a
different prefill route. The shared wrapper validates these exact lowercase
values but continues to default to `required-fast`. Direct long-server
overrides remain collect-only for the warp profile, which has failed exact
generated-token parity.

The SM89 Flash prefill route is promoted: with
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE` unset, the runtime resolves the
automatic selector, which engages `flash-f16-sm89` whenever the full
eligibility contract holds (page-16 F16 KV, F32 query, GQA 8:1, the q512/q3
query-length policy, matching sliding-window/global geometry, SM89, and loaded
symbols) and otherwise silently keeps the previous unset launch topology.
Promotion evidence is the paged-prefill differential
(`zig build quant-kernel-cuda-paged-prefill-diff`), which passed all 90
guard/page-table/adversarial/determinism cases bitwise-identical, plus the
strict end-to-end candidate validator. Explicit profiles behave exactly as
before; `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off` is the rollback
switch that restores the pre-promotion unset behavior. The shipped q16/k16
Flash prefill kernel uses the compile-time `kThreads == 256` stride in its
hot cooperative K/V-load loops, which lets ptxas unroll those loads without
changing launch geometry, shared-memory layout, or occupancy.

`ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1=ggml-ffn-v1` selects the experimental
single-row E2B GGML-Q8_1 FFN route. Its default is `off`, and qualification is
scoped to SM89. Likewise,
`ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE=sm89-prefill` is an
experimental SM89-only BF16 prefill profile. That typed profile is fixed at 8
algorithm candidates, 3 warmups, 10 timed iterations, and a minimum of 32
rows; these are part of the profile contract rather than free tuning knobs.
Its default is `off` so normal inference uses cuBLASLt's first admissible
heuristic and never performs synchronous first-use algorithm timing. This is
also the repeatable retrieval path: timed selection is owned by each model
session and can otherwise choose numerically different algorithms for models
with identical shapes.

EmbeddingGemma 2 global text attention uses the resident SM89 BF16 flash
kernel for its exact 512-token, four-query-head, one-KV-head, 512-dimension
geometry. This route is independent of the optional cuDNN
policy. Other global shapes use the generic attention kernel. Sliding text
attention may use cuDNN for supported shapes; setting
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_CUDNN=0`, unavailable plans, and
workspace-budget limits retain its existing SM89 or generic fallback.

Continuous batching and request-scoped graph replay are mutually exclusive.
Set both server controls explicitly when running the batching profile:

```sh
ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING=0 \
ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off \
  zig/pkg/inference/scripts/gemma4/with_gemma4_qat_cuda_tuning.sh \
  zig/pkg/inference/zig-out/bin/antfly-inference run \
  --host 127.0.0.1 --port 8080 --models-dir .models --config server.json
```

Graph-off mode disables arena planning and forces the pinned slot period, skip,
and request reset to zero.
The wrapper rejects batching with any other graph mode, and the server retains
the model-wide lock for graph, speculative/MTP, and multimodal requests.
`generation_batching.mode: auto` also remains serialized until the batching
promotion gate passes; use `mode: on` only for an explicit row-2 run.

`scripts/gemma4/benchmark_gemma4_cuda_batching.py` uses distinguishable equal-length
prompts, alternating row order, and a staggered mixed-length/page-growth probe;
its CI floor and promotion threshold are under Server Batching.

## Model-Neutral Kernel Catalog

The checked-in AOT catalog selects kernels by semantic operation signature,
runtime shape, and target, not by model name or layer index. Inspect a GGUF from
the inference package directory:

```sh
cd zig/pkg/inference
ZIG=../../../.tools/zig-x86_64-linux-0.17.0/zig
MODEL=../../../path/to/model.gguf

$ZIG build quant-kernel-codegen -- --inspect-model "$MODEL"
$ZIG build quant-kernel-codegen -- --check-model "$MODEL"
```

Both model commands are read-only and emit
`antfly.quant_kernel.model_inventory.v1` JSON. `--inspect-model` always reports
the inventory. `--check-model` also exits nonzero when `coverage.complete` is
false, meaning a required quantized operation or attention topology is not
represented by the compiler's semantic catalog. Backend route fields are
reported separately.

Exact AOT status is intentionally a second dimension. `aot_resolutions` records
target-specific `aot_resolved`, `aot_missing`, or `semantic_unsupported` status,
the kernel and source fingerprints, and `production_enabled`. An exact AOT miss
is an optimization gap and does not fail the semantic model check. The
`aot_targets` summary counts exact hits and misses; `all_signatures_resolved`
means every emitted signature probe resolved, not that every possible runtime
route is promoted.

The checked-in exact catalog currently targets CUDA `sm_89` only. Its scope is
selected Q4_0 and Q4_K row-1 matmuls, Q4_0 x Q8_1 fused FFN shapes, Q6_K x
Q8_1 K=2560 and K=3840 tile-argmax candidates, and Gemma 4 decode-attention
topologies. In the merged unified generated-artifact registry, 21 CUDA entries
are non-promoted; no single Q4_K route represents the remaining candidate set.
The generated Q4 runtime routes are default-off. When a route is explicitly
enabled, it is target-guarded to SM89; other compute capabilities use the
handwritten fallback even when their code objects are present in the fatbin.
Development experiments can bypass this target and promotion guard with
`ANTFLY_INFERENCE_CUDA_ALLOW_UNPROMOTED_GENERATED_KERNELS=1`; results from that
mode are not production evidence.
`production_enabled` records benchmark qualification; it does not imply
runtime-default dispatch. The five qualified Q4_0 routes require their positive
runtime opt-ins, and the master disable gate remains authoritative. The
1536-wide FFN, Q4_K, Q6_K argmax, and generated-attention entries remain
candidates; the 2560 x 10240 FFN pair/down entries are production-qualified but
runtime-default-off. Q4_K candidates are not runtime-wired. The Q6_K argmax
stage-1 candidates are runtime-wired behind a disabled opt-in and must pass
device parity and target-specific performance gates before promotion. The dense split-KV attention route remains dev-only
because its reordered partial-softmax merge does not satisfy the long-output
parity contract. The paged score-prework route instead parallelizes only QK
scores and retains the production chronological softmax recurrence.

Generated attention remains opt-in through
`ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1`; the standard server and
tuned CLI profile leave that gate off. Its default split-KV threshold is 512,
and `ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS` can select
another threshold for an explicit experiment. Set
`ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SPLIT_KV_SPLITS` to `2`, `4`, or `8`
to select a dev-only reduction schedule; it defaults to `8`. Each split count
uses a distinct CUDA graph replay key, and none is production-enabled. Do not
enable split-KV in a production profile or promote the dense split-KV
generated-attention catalog entries until a long-output, multi-model parity
corpus passes; only the exact score-prework composites carry promotion.

The paged TurboQuant candidate uses a typed, fail-closed selector. With
`ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK` unset, automatic
selection is restricted to the qualified SM89 Gemma 4 geometry: batch-1
single-row decode, 8 query heads / 1 or 2 KV heads, F16 paged K and V, no mask or
bias, and either HD256 with a 512-token sliding window or HD512 global
attention. The automatic crossover is 512 logical KV tokens. Set the variable
to `0` for the emergency rollback; set it to `1` to qualify the same F16 routes
on other CUDA >= 7.5 targets or to run the F32-value control with Polar4/F16
keys. Every other geometry retains the established fast decode fallback.

The route keeps 128 score chunks but bounds score storage to the visible
policy: `8 * 512 * sizeof(float)` (16 KiB) for Gemma 4 local attention and
`8 * 4096 * sizeof(float)` (128 KiB) for global attention. Score indices are
relative to the visible window, so a logical context beyond 512 tokens does
not alias or truncate local scores. A non-identity page table proves logical
coverage independently from physical capacity, allowing ring-backed pages;
identity layouts still require one physical row per logical token. The
chronological F32 softmax/value recurrence and F32 output ABI are unchanged.
The consumer has two exact phases in one CTA: lane zero materializes the
chronological alpha/beta sequence in bounded shared memory, one block barrier
publishes it, and every output lane replays the original value recurrence. This
replaces two whole-block barriers per visible key with one barrier per head
without reordering any floating-point operation that contributes to the result.
The module-level buffer remains the larger legacy split-workspace reservation
so enabling a candidate cannot perturb allocation/graph topology; each exact
launch validates and exposes only its typed 16/128-KiB active score slice.
The split-K score/counter workspace is currently module-owned, which is valid
only for batch one with continuous batching disabled and a serialized stream;
a genuinely concurrent runtime must make that workspace request- or
stream-owned instead before split-K decode can be promoted.

Exact score-prework takes precedence over the numerically divergent
split-summary experiment. CUDA graph replay keys aggregate a route bit for
every observed attention policy (fallback, explicit F32 control, Gemma 4 F16
local, and Gemma 4 F16 global). The split schedule remains in the key whenever
any policy still uses it and is normalized away only when every policy uses
score-prework. This makes the 511-to-512 automatic crossover, F16/F32 storage
change, local/global workspace topology, and mixed local/global models explicit
replay boundaries. The shared tuning wrapper leaves the variable absent so the
qualified automatic selector remains authoritative; release controls can pin
it to `0`, and explicit candidate qualification can pin it to `1`.

For raw numerical diagnosis, use the differential harness. It bypasses runtime
routing and graph replay, launches the handwritten fast decode kernel alongside
generated serial and split-KV kernels directly, and reports bitwise mismatches,
maximum absolute/relative error, and maximum F32 ULP distance. The default
corpus covers head dimensions 256 and 512 with random, near-tie softmax, and
cancellation-heavy inputs:

```sh
cd zig/pkg/inference
../../../.tools/zig-x86_64-linux-0.17.0/zig build \
  quant-kernel-cuda-attention-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=fast -- --kv-len 1024
```

Use `--head-dim`, `--heads`, `--kv-heads`, `--pattern`, and
`--split-count 2|4|8|all` to isolate a dense topology and reduction schedule.
`--query-position` and `--sliding-window` isolate individual split boundaries.
`--max-abs`, `--max-ulp`, and `--require-bitwise` turn the report into a
failure gate; `--json` emits `antfly.cuda_attention_diff.v2` for automation.

For the production paged-KV ABI, use the score-prework differential. The build
step compiles both generated candidates with the pinned CUDA 13.2 toolkit,
checks fixed, reversed, and deterministic-permuted page tables, covers F32/F16
values with Polar4/F16 keys, poisons unused physical rows and score storage
with quiet NaNs, and fails on any bit mismatch against
`termite_gqa_attention_decode_turboquant_fast_f32`:

```sh
cd zig/pkg/inference
../../../.tools/zig-x86_64-linux-0.17.0/zig build \
  quant-kernel-cuda-paged-attention-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=fast -- \
  --head-dim all --kv-len 2003 --pattern all --key-format all \
  --value-format all --page-order all --heads 8 --kv-heads 2 \
  --iterations 100
```

`--json` emits `antfly.cuda_paged_attention_diff.v2`. On SM89 L4, both the F16
production geometry and the F32 control pass bitwise-exact at the production
selector boundaries and at long logical-token counts, across permuted pages,
cancellation-heavy inputs, and NaN-poisoned unused storage, and the
shared-recurrence consumer is meaningfully faster than the handwritten paged
baseline on the axes tested. Rerun the full differential and end-to-end gates
before changing the SM89 crossover or widening the automatic architecture set.

For paged-KV prefill, use the prefill differential. It launches the embedded
production prefill kernel (`termite_gqa_attention_prefill_turboquant_fast_f32`)
alongside both embedded F16-page candidates (exact and warp) with identical
buffers and launch arguments. The default matrix is a bounded, deterministic
pairwise sweep covering every supported axis — chunked query lengths across
the tile boundary, prefix lengths across the 511/512 crossover, global and
local windows, page sizes, page orders, and adversarial input patterns —
with NaN-poisoned unused storage and device-side argument audits:

```sh
cd zig/pkg/inference
../../../.tools/zig-x86_64-linux-0.17.0/zig build \
  quant-kernel-cuda-paged-prefill-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=fast -- --json
```

Use `--candidate exact --require-bitwise` for exact qualification and
`--candidate warp` for the numerically bounded warp route; `--matrix
cartesian` is available for exhaustive investigations. `--json` emits
`antfly.cuda_paged_prefill_diff.v1` for automation.

Freeze full-model parity, graph replay, route counters, and paired throughput
with the model-neutral candidate validator:

```sh
cd zig/pkg/inference
python3 scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --kernel-id cuda.attention.gqa.decode.score_prework \
  --cache-dtype polar4 \
  --prompt Ants \
  --prompt "Write one sentence about the night sky." \
  --prompt "List the first five prime numbers." \
  --lengths 256 1024 --repeats 1 --capture-kv-capacity 2048 \
  --output-dir /tmp/antfly-score-prework-final
```

> **Relocated:** The score-prework and split-KV decode validation campaign
> that previously lived here (34 lines) is preserved verbatim in
> [docs/design/inference/history/gemma4/e2b-sm89.md](../../../docs/design/inference/history/gemma4/e2b-sm89.md).
> Durable decisions from it are in Model-Neutral Kernel Catalog (the generated
> attention / split-KV promotion policy) in this document.

The raw harness isolates the source of dense split-KV drift: generated serial
matches the handwritten fast kernel bitwise, and each split partition's local
recurrence is also bitwise. The first mismatch occurs when stage 2 merges a
multi-token partition summary into the preceding online-softmax state. The
paged score-prework route avoids that merge by parallelizing only score
calculation, then applying the canonical chronological softmax/value recurrence.

Inventory `coverage.complete`, `cuda_route_complete`, and
`metal_route_complete` describe semantic/runtime-route availability, not exact
AOT promotion. The legacy `*_catalog_complete` fields are compatibility aliases
for route completeness. Exact target summaries split required and optional
signatures and expose `all_required_signatures_resolved` and
`all_optional_signatures_resolved`. `present_quantized_operation_count` counts
model tensors; `quantized_operation_count` counts active decode operations after
shared-KV and omitted-V rules. Every numeric fingerprint also has a canonical
16-digit `*_fingerprint_hex` field for lossless JSON consumers.

Enable catalog FFN candidates with the model-neutral gate:

```sh
ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES=1
```

`ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN` is the legacy alias. It supplies
the default only when the catalog gate is unset, so the catalog gate takes
precedence. Keep candidates disabled in production until parity and repeated
full-model performance gates pass.

With JSON timing enabled, use
`cuda.generated_kernel_catalog_resolve_attempts`,
`cuda.generated_kernel_catalog_resolve_misses`,
`cuda.generated_kernel_catalog_hits`, and
`cuda.generated_kernel_catalog_fallbacks`. Resolve misses identify unsupported
shapes or targets; fallbacks identify a resolved kernel that failed to launch.
The top-level
`quant_kernel_plan` object reports `planned`, `handwritten_production`,
`generated_production`, `generated_candidates`, `unsupported_routes`,
`fast_path_misses`, and granular fallback reasons. The E2B-named pair/down hit
and fallback counters remain compatibility telemetry.

The generated Q6_K x Q8_1 LM-head stage-1 candidates are guarded by:

```sh
ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX=1
```

They are deliberately limited to greedy, unsuppressed `rows=1`,
`out_dim=262144` LM heads with `in_dim=2560` or `3840`. The route retains the
existing Q8_1 quantizer, partial workspace, shared argmax reducer, token
handoff, and graph-replay behavior; only stage 1 is generated. The standard
tuning profile leaves this gate at `0`. The candidate validator requires an
explicit `--model` because the default E2B QAT fixture has a Q4_0 LM head and
cannot exercise this Q6_K route. Use a GGUF whose output tensor is Q6_K, then
validate exact tokens, replay, and route telemetry before comparing throughput:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --candidate q6-k-q8-1-lm-head-argmax \
  --model .models/google/gemma-4-12B-it-q4_k/gemma-4-12B-it-Q4_K_M.gguf \
  --config-label l4-sm89-q6-lm-head \
  --lengths 64 128 256 512 --repeats 5 \
  --min-candidate-ratio 1.00 --max-cv 0.02 \
  --output-dir /tmp/antfly-q6-lm-argmax-validation
```

The current generated body specializes Q6 sub-layout unpacking while
preserving the four dependent DP4A operations and the complete floating-point
reduction order, uses substantially fewer registers with no local memory
versus the handwritten Q6 stage-1 baseline, and is bitwise-equal at every
stage-1 partial value and index. This has not cleared the production gate: a
full Gemma 4 12B Q4_K_M graph-replay run had exact token IDs, required route
hits, and zero fallbacks, but only a marginal paired throughput ratio. Keep
the gate off until a larger end-to-end improvement is demonstrated.

Regenerate owned sources only after changing compiler or catalog inputs, then
check the result:

```sh
$ZIG build quant-kernel-codegen -- --write
$ZIG build quant-kernel-codegen -- --check
$ZIG build quant-kernel-codegen -- --check-metal  # Requires the Metal toolchain.
```

## Paired Comparison

This is the exact 256-token Antfly/llama.cpp collection command for the current
profile. The 255/256 request values are intentional benchmark accounting.

```sh
(cd zig/pkg/inference && \
  ../../../.tools/zig-x86_64-linux-0.17.0/zig build \
    -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize=fast)

OUT_DIR=/tmp/antfly-gemma4-l4-256 \
WARMUPS=1 REPEATS=3 PROMPT='Here is a sentence about ants:' \
ANTFLY_TOKENS=255 LLAMA_TOKENS=256 \
ANTFLY_CACHE_DTYPE=f32 LLAMA_CACHE_TYPE_K=f32 LLAMA_CACHE_TYPE_V=f32 \
ANTFLY_DECODE_GRAPH_REPLAY=required REQUIRE_GRAPH_REPLAY=1 \
ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=0 \
ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX=1 \
ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN=0 \
REQUIRE_GENERATED_ATTENTION=0 REQUIRE_LM_HEAD_ARGMAX=1 \
REQUIRE_GENERATED_E2B_FFN=0 \
MIN_LLAMA_THROUGHPUT_RATIO=0 MIN_COMPARABLE_THROUGHPUT_RATIO=0 \
MIN_ANTFLY_TOK_S=0 MAX_ANTFLY_TOK_S_CV=0.02 \
  zig/pkg/inference/scripts/gemma4/gemma4_qat_llamacpp_pair_benchmark.sh
```

Warmups and measured pairs use balanced AB/BA order: odd pairs run Antfly then
llama.cpp, and even pairs run llama.cpp then Antfly. Each raw row records the
actual order so drift cannot be mistaken for an engine effect.

On an NVIDIA L4, the verified ReleaseFast result put Antfly decode throughput
below llama.cpp eval throughput on this workload, and the comparable ratio
that also counts llama.cpp sampling time remains below parity.

## Output Matrix

Collect the current production matrix without asserting future throughput
targets:

```sh
zig/pkg/inference/scripts/gemma4/benchmark_gemma4_cuda_matrix.py \
  --lengths 64 128 256 512 --target-length 256 \
  --min-antfly-tok-s 0 --min-comparable-ratio 0 --max-cv 1 \
  --no-require-generated-attention --collect-only
```

The matrix still evaluates healthy persistent graph replay by default;
`--collect-only` records a failed check without exiting nonzero. Generated
attention is not required because the production profile keeps it off.

The following are aspirational gates, not current achieved values:

```sh
zig/pkg/inference/scripts/gemma4/benchmark_gemma4_cuda_matrix.py \
  --lengths 64 128 256 512 --target-length 256 \
  --min-antfly-tok-s 120 --min-comparable-ratio 0.95 --max-cv 0.02 \
  --no-require-generated-attention
```

Use `--require-generated-attention` only for an explicit attention candidate
run after exact-token parity has passed.

## Candidate Validation

The candidate validator checks every baseline/candidate pair for exact token
IDs, persistent graph replay without discards or capacity skips, zero baseline
route hits, required candidate route hits, and zero forbidden fallback counts.
With `--repeats`, it alternates baseline-first and candidate-first execution.

`passed` includes the configured performance gates. Defaults remain permissive
for compatibility; they are intended only for ad-hoc parity work. Reproducible
performance work must select `--qualification-profile screening` or
`--qualification-profile promotion`. Screening fixes five AB/BA pairs per case,
allows at most 3% CV, and rejects material TTFT, decode, or total-latency
regressions. Promotion fixes ten pairs, allows at most 2% CV, requires at least
a 2% decode-throughput and decode-latency improvement, and requires the paired
95% log-ratio CI for total and decode latency to close at or below parity.
Profile sample counts cannot be overridden, and their output directory must be
new and empty.

Every non-legacy profile uses the immutable catalog definition: the selected
gate, required and forbidden counters, baseline value, candidate value, and
declared prefill/decode phase cannot be replaced on the command line. The
qualification focus must match that declared phase. Legacy/ad-hoc runs retain
their override and positive-route-hit behavior for backwards compatibility.

Prefill candidates use the separate `prefill-screening` and
`prefill-promotion` contracts. Both require a versioned `--prompt-fixture`,
forbid unlocked `--prompt` cases, require exact prompt and generated token IDs,
and require every catalog route counter with zero forbidden fallbacks.
Screening fixes five AB/BA pairs with a 3% CV ceiling; promotion fixes ten with
a 2% CV ceiling. Both require at least a 2% paired-median improvement in the
CLI prefill/first-token proxy and fixed-work total latency. Decode is a control,
not an optimization target: screening permits at most 2% median latency or
throughput regression and promotion permits at most 1%. The screening 95% CI
upper limits are 1.03 for prefill/total and 1.05 for decode; promotion requires
prefill and total to close at or below parity and decode at or below 1.02.
Both profiles require at least 10,000 bootstrap resamples. These fixed
thresholds may be tightened but not loosened.

The typed catalog comparisons change only the selected gate while recording
all fixed comparison settings in evidence:

| Catalog ID | Baseline gate | Candidate gate | Required evidence |
| --- | --- | --- | --- |
| `cuda.attention.gqa.prefill.tiled_f16_exact` | `required-fast` | `required-tiled-f16-exact` | Exact-route launches for both HD256 and HD512 |
| `cuda.attention.gqa.prefill.tiled_f16_warp` | `required-fast` | `required-tiled-f16-warp` | Exactly 140 HD256 and 35 HD512 warp-route launches on the locked workload |
| `cuda.attention.gqa.prefill.flash_f16_sm89` | `required-fast` | `required-flash-f16-sm89` | Exact HD256/HD512 × q512/q3 route counts (112/28/28/7) and zero eligibility/symbol fallback |
| `cuda.attention.gqa.decode.score_prework.tiled64` | `serial` | `required-tiled64` | Serial HD256/HD512 baseline routes; tiled64 HD256/HD512 candidate routes; no serial, fast, or fallback candidate routes |
| `cuda.decode.q4_0.ggml_q8_1_e2b_ffn` | `off` | `ggml-ffn-v1` | E2B FFN hit and zero aggregate fallback |
| `cuda.cublaslt.bf16.prefill.sm89` | `off` | `sm89-prefill` | Tuned-plan hit and zero tuning-API fallback |

For cuBLASLt, `bf16_cublaslt_tuning_heuristic_calls` is diagnostic rather than
an error: a shape may legitimately expose only one usable algorithm. The
candidate must still report `bf16_cublaslt_tuning_tuned_calls > 0` across the
run and `bf16_cublaslt_tuning_api_fallbacks == 0`.

The CLI has no transport boundary, so its `ttft_ms` field is explicitly sourced
as `timing_ms.prefill_inner_proxy`; the warm-server harness below remains the
authority for true request-to-first-visible-token latency. Candidate evidence
contains the complete baseline and candidate timing payloads in
`candidate_samples.jsonl`, per-pair graph and route attestations, distribution
summaries and deterministic paired log-ratio confidence intervals for TTFT,
decode, and total latency in `candidate_summary.json`, and SHA-256s for every
artifact in `evidence_manifest.json`. The summary also content-addresses the
binary, model file or directory inventory, tuning wrapper, validator, and
shared pairing/statistics module, so a candidate result remains attributable
to its execution inputs rather than just its output files. Strict profiles
require every selected route in its declared prefill or decode phase. Decode
routes must be observed during graph construction; prefill routes are recorded
as prefill attestations and never mislabeled as graph-replay coverage. The
captured decode graph must still replay across all stable decode steps without
discard or capacity skip. Launch counters are not misinterpreted as per-replay
counters.

The warp-prefill and tiled64 score-prework catalog entries additionally bind
strict qualification to the checked-in `gemma4-search-retrieval-long-v1`
fixture by both file and rendered-prompt SHA-256: 2,051 prompt tokens, 300
output tokens, F16 KV, prefill chunk size 512, and capture capacity 2,432. On
that fixed 512+512+512+512+3 schedule, warp prefill must report exactly 140
HD256 launches and 35 HD512 launches (28 and 7 layers across all five chunks).
The exact totals plus the required runtime mode make the final q=3 tail fail
closed instead of accepting a partial route observation.

Strict candidate profiles also carry a release-grade provenance contract.
Before timing, the validator records the repository commit and Git dirty-state
status digest, hashes the validator and its shared provenance helpers, and
captures the exact Zig and NVCC executable hashes and version strings. Zig
0.17.0 and CUDA 13.2 are required. A dirty development tree is allowed for
candidate work, but its recorded state must remain identical through the
qualification; changing the commit or dirty-state digest invalidates the run.

Exactly one GPU must be selected. Evidence records its name, UUID, compute
capability, driver, persistence/MIG state, power limit, maximum graphics and
memory clocks, and application graphics and memory clocks. The selected GPU
must have no competing compute process at qualification preflight and directly
before and after every AB/BA pair. GPU identity, clock, or power drift and any
unexpected process fail the qualification. The resulting guards are retained
in `runtime_guards.json`, each raw pair, and the content-addressed summary
provenance.

The same preflight hashes the generated manifest, CUDA renderer and compiler
sources, canonical runtime CU, PTX, fatbin, and SM89 cubin. It then runs exactly
one untimed generated-source check
(`zig build quant-kernel-codegen -- --check`) and one canonical CUDA artifact check
(`regen-cuda-artifacts.sh --check --all`) before any benchmark pair. Hashes are
compared before and after those checks and again after the complete
qualification. This establishes freshness without recompiling inside measured
pairs. Missing, stale, or mutated artifacts fail closed; commands, return
codes, logs, and all before/after hashes are recorded in the versioned
`provenance.artifact_freshness` attestation. Legacy/ad-hoc qualification does
not run these checks or require GPU/toolchain provenance.

Screen the score-prework route on the same versioned 8,251-byte rendered prompt,
2,051-token prefill, and 300-token output shape used by the realistic-workload
program:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --kernel-id cuda.attention.gqa.decode.score_prework \
  --qualification-profile screening \
  --prompt-fixture zig/pkg/inference/scripts/gemma4/fixtures/gemma4_long_context_v1.json \
  --lengths 300 --prefill-chunk-size 512 \
  --cache-dtype f16 --capture-kv-capacity 2432 \
  --config-label l4-sm89-long-context-f16-decode-score-prework-screening \
  --output-dir /tmp/antfly-score-prework-screening
```

This isolates baseline and candidate Antfly decode routes while preserving the
production server's 512+512+512+512+3 prefill schedule. Its CLI TTFT field is a
declared `prefill_inner` proxy and is used only to reject incidental prefill
regressions between identical Antfly arms; it is not request-to-first-visible-
token latency. This is a decode-candidate promotion screen, not an
Antfly-versus-llama.cpp superiority claim; that claim belongs to the locked
warm-server gate.

To isolate the tiled64 score consumer from the serial implementation, use
`cuda.attention.gqa.decode.score_prework.tiled64` with the same command shape
and a decode `screening` or `promotion` profile. Its catalog fixes score
prework on in both arms, requires serial HD256 and HD512 evidence in the
baseline and tiled64 HD256 and HD512 evidence in the candidate, and rejects
candidate serial, fast-decode, or fallback launches.

Screen either tiled F16 GQA prefill candidate on the fixed realistic prompt by
selecting its catalog ID:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --kernel-id cuda.attention.gqa.prefill.tiled_f16_exact \
  --qualification-profile prefill-screening \
  --prompt-fixture zig/pkg/inference/scripts/gemma4/fixtures/gemma4_long_context_v1.json \
  --lengths 300 --prefill-chunk-size 512 \
  --cache-dtype f16 --capture-kv-capacity 2432 \
  --config-label l4-sm89-long-context-f16-prefill-tiled-exact-screening \
  --output-dir /tmp/antfly-gqa-prefill-tiled-exact-screening
```

Use `cuda.attention.gqa.prefill.tiled_f16_warp` for the warp-specialized arm.
Each case must observe its dedicated HD256 and HD512 timing counters; one
model-wide aggregate hit is insufficient. The same prefill profile qualifies
`cuda.cublaslt.bf16.prefill.sm89`; its catalog contract enables hybrid BF16
prefill mirrors identically in both arms, then changes only the typed tuning
gate. Use the ordinary decode `screening`/`promotion` profiles for
`cuda.decode.q4_0.ggml_q8_1_e2b_ffn`.

All five candidates in the table remain experimental and default-off. Candidate evidence
does not modify or substitute for the locked E2B llama.cpp superiority gate,
the E4B frozen-baseline regression lane, or the 12B control lane.

Run the repeated LM-head regression gate with:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --candidate q4-0-q8-1-lm-head-argmax \
  --lengths 64 128 256 512 --repeats 5 \
  --min-candidate-ratio 1.01 --max-cv 0.02 \
  --output-dir /tmp/antfly-lm-argmax-validation
```

The verified full LM-head matrix passed every case with exact tokens and zero
candidate fallback, with a consistent paired throughput improvement. This
evidence supports the current profile default of LM-head argmax on.

Generated attention remains off by default. An earlier divergence was traced
to candidate routing through the non-device-scalar warm-up path, whose
production kernel uses a different reduction contract; generated attention is
now restricted to the matching decode-scalar path. The serial candidate has
exact token parity and is throughput-neutral; split-KV remains opt-in because
its long-output parity gate still fails despite its material throughput
advantage.

The isolated E2B FFN chain kernels show a throughput edge in isolation that
does not carry over to the full-model gate: the isolated harness synchronizes
after each chain, while production replays a CUDA graph, so launch savings do
not translate directly. These kernels remain dev-only. Require repeated
full-model evidence before changing the profile default:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --candidate q4-0-q8-1-e2b-ffn \
  --lengths 64 128 256 512 --repeats 5 \
  --min-candidate-ratio 1.00 --max-cv 0.02 \
  --output-dir /tmp/antfly-e2b-ffn-validation
```

The generated E2B pair-only candidate isolates the numerically safe portion of
that chain. For rows=1, hidden width 1536, and intermediate widths 6144 or
12288, its gate/up kernel writes the Q8_1 activation directly. The isolated
microbenchmark compares every output byte with the existing F32
pair-activation followed by `quantize_f32_q8_1_rows`; that boundary is exact.
Runtime dispatch then unconditionally uses the existing handwritten Q8_1 down
projection, preserving its established reduction order. This removes the F32
activation intermediate and one quantization launch without inheriting the
coupled candidate's down-projection drift.

The route remains off by default until repeated full-model evidence shows both
exact token parity and a stable end-to-end win under persistent graph replay.
Its dedicated hit/fallback counters distinguish it from the coupled catalog
candidate, and the validator locks the coupled and exact-F32 routes off in both
arms:

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --candidate q4-0-q8-1-e2b-ffn-pair-only \
  --lengths 64 128 256 512 --repeats 5 \
  --min-candidate-ratio 1.00 --max-cv 0.02 \
  --output-dir /tmp/antfly-e2b-ffn-pair-only-validation
```

Direct experiments enable it with
`ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY=1`. The shared tuning
wrapper rejects simultaneous pair-only, coupled-catalog, and exact-F32 E2B FFN
candidates so a qualification run cannot silently measure a different route.

The exact F32 E2B FFN candidate is a separate default-off route. It preserves
the production F32 pair-activation boundary and reduction topology instead of
the Q8-intermediate contract above. Its validator pins both arms to raw-Q4_0
F32 pair activation and the handwritten tile4 down path: it disables the Q8
pair/down and Q8 precompute routes, catalog E2B FFN candidates, generated Q4_0
MMV, alternate F32 tile shapes, and Q4_0 dequant-on-upload. The resolved locks
are recorded in `candidate_summary.json`. It then requires exact token IDs,
persistent replay, nonzero exact pair and down hits only in the candidate, and
zero exact-route fallbacks:

The semantic AOT catalog records these as optional, production-disabled F32
entries for model inventory and validation. Runtime dispatch remains the
separate `ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT` direct gate;
cataloging the artifacts does not enable that route.

```sh
zig/pkg/inference/scripts/gemma4/validate_gemma4_cuda_candidate.py \
  --candidate q4-0-e2b-ffn-exact \
  --lengths 64 256 --repeats 3 --capture-kv-capacity 2048 \
  --min-candidate-ratio 1.00 --max-cv 0.02 \
  --output-dir /tmp/antfly-e2b-ffn-exact-validation
```

## L4 Release Evidence

`zig/e2e/inference/test_cuda_gemma4_l4.py` is the explicitly flagged GPU
evidence lane. Ordinary PR and scheduled CI do not run it. Select it on a
dedicated L4 with `ANTFLY_E2E_CUDA_GEMMA4_L4=1` and `pytest -m cuda_l4`.

The runner accepts local environment inputs rather than repository variables:

- `E2B_MODEL`, `GEMMA12B_Q4_MODEL`, and `LLAMA_CPP_BIN`: absolute target GGUF
  and pinned comparator paths.
- `LLAMA_SERVER_BIN`, `LONG_E2E_LOCK`, and `LONG_E2E_LOCK_SHA256`: pinned
  comparator inputs for the strict E2B warm-server headline. They are required
  as a complete set in release mode. The digest is reviewed and stored out of
  band from the lockfile; partial configuration, malformed digests, and
  mismatches fail closed.
- `E4B_QAT_MODEL` and optional `E4B_MODELS_DIR`: E4B regression model inputs.
- `E4B_BASELINE_EVIDENCE`, `E4B_BASELINE_SHA256`, `E4B_REGRESSION_LOCK`, and
  `E4B_REGRESSION_LOCK_SHA256`: the reviewed frozen E4B regression bundle.
  Release mode requires the complete bundle and enforces the 1.03
  TTFT/decode/total-latency regression ceiling.
- `CUDA_VISIBLE_DEVICES`: selected L4, defaulting to `0`.

The gate refuses a device other than a single NVIDIA L4 / SM89, checks generated
sources and checked-in CUDA artifacts, builds ReleaseFast, and runs
`cuda-info --smoke`. It then records the fixed 255/256-token E2B f32 paired
benchmark with persistent graph replay and candidate attention, Q6 LM-head,
Q8-intermediate E2B FFN, pair-only E2B FFN, and exact-F32 E2B FFN routes
disabled. It also runs
the 12B Q4_K_M f32 replay case twice and
requires identical token IDs, healthy replay, and zero disabled-candidate
counters. Every measured sample must hit the production Q8_1 DP4A linear and
pair prefill routes and report zero hits or fallbacks from the generated Q4_0
MMV/MM/pair/pair-Q8/down-Q8 routes. Generated-route hit and promotion evidence
remains a separate exact-token kernel-candidate gate because the fixed release
prompt does not exercise every generated row bucket.

The qualification also runs the fixed five-pair score-prework screening profile
with the checked-in long-context fixture, F16 KV, and 300 output tokens. The
fixture path materializes the locked 8,251-byte rendered chat prompt and passes
those bytes through the raw-prompt path, then requires both arms to report the
same 2,051 prompt token IDs. The screen is gating for the nightly candidate lane
and its raw samples, summary, logs, timing files, and content-addressed manifest
are written with the other L4 evidence. Release mode also uses the stronger locked
E2B warm-server lane below; the candidate screen is diagnostic kernel
qualification and does not substitute for the end-to-end comparator.

Those five generated Q4_0 routes therefore remain runtime opt-ins despite the
checked-in isolated-kernel benchmarks. Enable them individually with
`ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_{MMV,MM,PAIR,PAIR_Q8,DOWN_Q8}=1` only for
a model-level validation run. Per-route `DISABLE_` variables override the
matching opt-in, and `ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0=1` is the
single emergency rollback for all generated Q4_0 routes and candidates.

Run the release lane on a dedicated NVIDIA L4 for the commit intended for release:

```sh
ANTFLY_E2E_CUDA_GEMMA4_L4=1 \
CUDA_RELEASE_MODE=release \
CUDA_EVIDENCE_DIR=/tmp/antfly-cuda-gemma4-l4 \
E2B_MODEL=/path/to/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
GEMMA12B_Q4_MODEL=/path/to/gemma-4-12B-it-Q4_K_M.gguf \
LLAMA_CPP_BIN=/path/to/llama-completion \
LLAMA_SERVER_BIN=/path/to/llama-server \
LONG_E2E_LOCK=/path/to/headline.lock.json \
LONG_E2E_LOCK_SHA256=<reviewed-sha256> \
E4B_QAT_MODEL=/path/to/gemma-4-E4B-it-qat-q4_0.gguf \
E4B_BASELINE_EVIDENCE=/path/to/e4b-baseline.json \
E4B_BASELINE_SHA256=<reviewed-sha256> \
E4B_REGRESSION_LOCK=/path/to/e4b-regression.lock.json \
E4B_REGRESSION_LOCK_SHA256=<reviewed-sha256> \
  uv run --project zig/e2e/inference pytest -m cuda_l4 \
    zig/e2e/inference/test_cuda_gemma4_l4.py
```

`zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh --help` documents the complete
input contract.
Tag release archives are built with `-Dcuda=false`, so publication does not
implicitly claim qualification from this lane. Its `release_scope` is
`target_only`: CUDA MTP is not certified. Release mode requires an E2B
comparable llama.cpp ratio of at least `0.70` and token-throughput CV no higher
than `0.02`; `0.80` remains the future optimization target. Nightly mode keeps
the correctness/replay contract without enforcing that performance floor.

Each run writes `release_summary.json`, `release_provenance.json`, paired raw
logs/timing, and 12B timing evidence under `CUDA_EVIDENCE_DIR`. Locked headline
and E4B evidence are merged into the same fail-closed summary, so a nonzero E2E
result and its canonical JSON cannot disagree.

CUDA MTP diagnostics run only in `CUDA_RELEASE_MODE=nightly`. They are
experimental, optional, and non-gating, and they make no production-readiness
or llama.cpp-superiority claim. Strict CUDA MTP certification is follow-up work.

For an equivalent local evidence run after a ReleaseFast CUDA build:

```sh
python3 zig/pkg/inference/scripts/gemma4/gemma4_cuda_l4_release_gate.py \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference \
  --llama-cpp-bin /path/to/llama-completion \
  --e2b-model /path/to/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  --gemma12b-q4-model /path/to/gemma-4-12B-it-Q4_K_M.gguf \
  --output-dir /tmp/antfly-gemma4-l4-release \
  --enforce-performance --min-comparable-ratio 0.70 --verify-artifacts
```

## Gemma 4 E2B SM89 optimization status

The locked CUDA tuning workload is Gemma 4 E2B QAT on an NVIDIA L4: eight
query heads share one KV head (the 8:1 MQA endpoint of GQA), the rendered
prompt is 8,251 UTF-8 bytes / 2,051 tokens, prefill uses four 512-row chunks
plus a three-row tail, and decode is fixed at 300 tokens with an F16 paged KV
cache. This topology is an exact route constraint, not a model-neutral
assumption. The promoted SM89 flash-prefill route is the production default
on this workload; split-K decode and the BF16 mirror-first prefill profile
remain default-off collect-only candidates pending exact-output parity. TTFT
is the dominant remaining gap versus llama.cpp; decode throughput is close.

An Nsight Systems request-window trace on this workload attributes most GPU
busy time to Flash attention with GPU utilization near 100%, leaving only a
small device-idle upper bound, so CUDA Graph capture helps CPU concurrency but
is not the primary TTFT lever. Next production priorities, each independently
gated (projected savings overlap and must not be added without end-to-end
device-event evidence):

- an upload-packed SM89 W4A16 tensor-core projection engine with a documented
  numerical contract;
- model-shape cuBLASLt tuning with admitted persistent workspace;
- fused gate/up activation output;
- a genuinely concurrent GQA Flash redesign that reuses K/V without the
  current design's register/tail costs.

> **Relocated:** The dated SM89/E2B benchmark campaigns and candidate
> qualification history that previously lived here (102 lines) are preserved
> verbatim in
> [docs/design/inference/history/gemma4/e2b-sm89.md](../../../docs/design/inference/history/gemma4/e2b-sm89.md).
> Durable decisions from it are in Model-Neutral Kernel Catalog and Production
> Defaults in this document.

## Long-context llama.cpp Superiority Gate

`benchmark_gemma4_long_e2e_server.py` is the backend-neutral, warm-server
comparison used for a scoped llama.cpp superiority claim. Its checked-in
fixture renders an 8,251-byte SearchAF chat-template prompt and an identical
2,051-token model input after the tokenizer adds Gemma's BOS token. Both engines use
one byte-identical GGUF, a 4,096-token
context, equal F16 K/V caches, greedy sampling, and exactly 300 decode tokens.
This is an intentionally fixed-work end-to-end throughput lane: public output
must be non-empty and contain the fixture's required answer facts, while both
engines perform the same bounded decode work even if an EOS occurs early.
The measured requests set the typed chat-template option
`enable_thinking=false` for both engines, and the locked prompt opens Gemma 4's
public `final` channel directly. This keeps private-thought projection fail
closed while ensuring TTFT measures an observable response; it also avoids
grammar/sampler overhead that would disable the device-greedy CUDA path.

The harness runs only one engine process at a time on the selected GPU. Each
sample starts a resident server, performs two full-work warmups, and measures a
streaming request over the established keep-alive connection. Ten paired
samples alternate Antfly/llama.cpp then llama.cpp/Antfly. The measured boundary
is request submission through the final streamed event/response EOF; model
loading, server startup, and connection establishment are excluded. Total
latency ends only after the final SSE event/response EOF (including final usage and `[DONE]`),
while decode latency runs from first visible token through the terminal usage event so
hidden/private generation cannot be shifted into transport tail time. Visible-content time
is retained separately as a diagnostic. Antfly renders the measured user message
with `--disable-thinking`, requires the resulting bytes and SHA-256 to match the
locked public-channel prompt, and tokenizes that rendered prompt. Those token IDs
must exactly match llama-server's tokens before timing begins. For every llama.cpp server,
the harness first calls `/apply-template` with the exact measured messages, requires the
same typed chat-template options, requires the returned UTF-8 bytes and SHA-256 to equal the
locked reference prompt, and only then passes
that returned prompt to `/tokenize`. Measured server usage must also report the same
prompt-token count. Both request bodies disable prompt/KV reuse, so every warmup and measured
request performs the complete 2,051-token prefill.

Run the E2B QAT Q4_0 headline gate with a SHA-pinned GGUF and a pinned
`llama-server` build:

```sh
python3 zig/pkg/inference/scripts/gemma4/benchmark_gemma4_long_e2e_server.py \
  --profile headline \
  --backend cuda \
  --model /models/gemma-4-E2B-it-qat-Q4_0/gemma-4-E2B-it-qat-Q4_0.gguf \
  --llama-server-bin /opt/llama.cpp-pinned/bin/llama-server \
  --models-dir /models \
  --output-dir /tmp/gemma4-e2b-long-e2e \
  --require-lock \
  --lockfile /evidence/gemma4-e2b-long-e2e.lock.json \
  --lockfile-sha256 "$E2B_LOCK_SHA256"
```

The headline passes only when the median Antfly/llama.cpp total-latency ratio
is at most `0.95`, the paired bootstrap 95% confidence-interval upper bound is
below `1.0`, both total-latency CVs are at most `0.03`, Antfly p95 is no slower,
and neither median TTFT nor decode latency is more than 2% slower. Separate
paired log-ratio 95% confidence intervals for TTFT and decode must each have an
upper bound at or below `1.02`; a passing component median therefore cannot
hide a regressed tail of paired samples. Every warmup
and measured request must report the exact prompt/output counts and a length
finish; reconstructed output-content digests must be deterministic for each
engine. Empty visible deltas do not establish TTFT, and empty or semantically
incorrect visible responses fail the correctness gate even when terminal usage
claims 300 tokens. The harness requests terminal accounting from both engines with
`stream_options.include_usage=true`; that final SSE usage chunk is authoritative.
Headline enforcement requires the event from both engines. Content-event counts
remain only a non-gating collection-mode fallback because transport fragments
need not preserve token boundaries. Antfly omits the usage-only chunk for ordinary
streaming clients unless they explicitly request it, preserving the established
finish-chunk-to-`[DONE]` protocol.

Every run writes `evidence.json`, `samples.jsonl`, raw SSE events and server
logs, `paired_samples.tsv`, a prompt-token contract, `benchmark.lock.json`, and
`evidence_manifest.json`. The manifest content-addresses every other evidence
artifact, and `evidence.json` reports paired log-ratio confidence intervals for
TTFT, decode, and total latency in addition to the headline bootstrap result.
The evidence contract also records every applied ceiling in the versioned,
machine-readable `contract.performance_thresholds` object, including the
separate TTFT and decode CI limits and whether performance enforcement was
enabled.
The reviewed harness identity covers both the server benchmark and its shared
pairing/statistics/manifest module, so changing AB/BA ordering or evidence
hashing invalidates the comparator lock just like changing the launcher does.
Establish the comparator lock from a trusted collection,
review its model SHA, llama.cpp commit, fixture hash, backend, precision, and
context, then store its `sha256sum` in immutable release configuration outside
the evidence directory. Pass both the file and that reviewed digest with
`--require-lock --lockfile-sha256`; a lockfile by itself is not an authenticated
release input. The lock also contains a stable GPU/driver/CUDA/host-runtime
identity, GPU power limit,
maximum/application clocks, MIG mode, the exact non-hidden Antfly model-directory
inventory (decoder plus projector/config/tokenizer sidecars), and a llama.cpp runtime-bundle
digest. The latter covers the launcher, build/version metadata, and the resolved hashes of
non-system and material shared objects such as `libllama`, `libggml-cuda`, CUDA runtime, and
cuBLAS libraries; changing a shared implementation library therefore invalidates the lock
even when the small launcher binary is unchanged. Enforced CUDA evidence fails closed when
runtime identity or dynamic-dependency inspection is unavailable, or when another GPU
compute process is present before the benchmark starts. The Antfly
commit is deliberately evidence provenance rather than a lock field so new
candidate commits can be evaluated without changing the comparator.

E4B and F32 lanes compare current Antfly medians with a frozen evidence bundle:

```sh
# Record the frozen E4B F16 baseline before a runtime change.
python3 zig/pkg/inference/scripts/gemma4/benchmark_gemma4_long_e2e_server.py \
  --profile e4b-regression --collect-only \
  --model /models/gemma-4-E4B-it-qat-Q4_0/gemma-4-E4B-it-qat-Q4_0.gguf \
  --models-dir /models \
  --output-dir /evidence/e4b-f16-frozen

# Gate the candidate (3% maximum Antfly latency regression).
python3 zig/pkg/inference/scripts/gemma4/benchmark_gemma4_long_e2e_server.py \
  --profile e4b-regression \
  --model /models/gemma-4-E4B-it-qat-Q4_0/gemma-4-E4B-it-qat-Q4_0.gguf \
  --models-dir /models \
  --baseline-evidence /evidence/e4b-f16-frozen/evidence.json \
  --baseline-sha256 "$E4B_F16_BASELINE_SHA256" \
  --lockfile /evidence/e4b-f16-frozen/benchmark.lock.json \
  --lockfile-sha256 "$E4B_F16_LOCK_SHA256" --require-lock \
  --output-dir /tmp/e4b-f16-candidate

# F32 control (5% maximum Antfly latency regression).
python3 zig/pkg/inference/scripts/gemma4/benchmark_gemma4_long_e2e_server.py \
  --profile f32-control \
  --model /models/gemma-4-E4B-it-qat-Q4_0/gemma-4-E4B-it-qat-Q4_0.gguf \
  --models-dir /models \
  --baseline-evidence /evidence/e4b-f32-frozen/evidence.json \
  --baseline-sha256 "$E4B_F32_BASELINE_SHA256" \
  --lockfile /evidence/e4b-f32-frozen/benchmark.lock.json \
  --lockfile-sha256 "$E4B_F32_LOCK_SHA256" --require-lock \
  --output-dir /tmp/e4b-f32-control
```

Regression enforcement fixes the maximum ratios at 1.03 for E4B F16 and 1.05 for
the F32 control; command-line overrides may tighten but cannot loosen them. A
frozen baseline must itself have passed, use the same profile, contain at least
10 paired samples, keep both engines' total-latency CV at or below 0.03, and
match the candidate's GPU/driver/CUDA/host identity, Antfly model bundle, locked
server budgets, execution profile, and llama.cpp runtime bundle. Candidate runs
enforce the same 0.03 CV ceiling. Store the reviewed `sha256sum` of each frozen
`evidence.json` and `benchmark.lock.json` in immutable release configuration and
supply both through the corresponding environment variables above. Never
derive either expected digest from the candidate-time files: doing so records
corruption instead of authenticating the reviewed inputs. Candidate evidence
records the expected and observed frozen-evidence and lockfile SHA-256, lock
digest, timestamp, Antfly binary/commit identity, and sample count so the
regression result remains independently attributable.

Use `--collect-only` for profiling and exploratory kernels. It disables only
performance thresholds; fixture integrity, exact token accounting, prompt-ID
parity, deterministic output, and warm-connection checks remain mandatory.

CUDA runs apply a small model-neutral server profile directly: required decode
graph replay, typed `required-fast` GQA prefill, runtime temporary-arena
planning, prefill-first-token enabled, first-token coalescing disabled, a prompt/output-derived
`ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY`, and a 512-row idle prefill
chunk ceiling. The server is also started with explicit, lock-covered L4
admission limits (`8192` MiB host, `19000` MiB backend, `28000` MiB combined,
`1024` MiB KV, and `2048` MiB scratch). These are policy ceilings, not eager
allocations, and prevent machine-level auto-detection from changing which
model plan is admitted between candidate and comparator runs. The standalone
`run` command exposes the same `--host-budget-mb`, `--backend-budget-mb`,
`--combined-budget-mb`, `--kv-budget-mb`, and `--scratch-budget-mb` controls as
the other generation entry points. Collection runs may override them with the
corresponding `--antfly-*-budget-mb` harness options; headline runs require the
locked values above.

The benchmark locks two distinct environment views: Antfly's
effective environment after its explicit defaults and overrides, and the
material environment inherited unchanged by llama.cpp. Material variables
include the `ANTFLY_*`, `TERMITE_*`, `CUDA_*`, `NVIDIA_*`, `GGML_*`, and
`LLAMA_*` namespaces, the CUDA library namespaces, and common runtime/library
threading controls. This covers controls such as
`ANTFLY_CUDA_DISABLE_FAST_GQA_DECODE`, `ANTFLY_CUDA_QMATMUL_VARIANT`,
`TERMITE_CUDA_CUBLASLT_WORKSPACE_MB`, and `CUDA_VISIBLE_DEVICES`.

Both environment views, each Antfly value's source, the server chunk ceiling,
and canonical hashes are written to provenance and the comparator lock.
Credential-like values are represented by their SHA-256 digest in evidence;
their exact value still contributes to the lock hash. Override an Antfly value
explicitly with a canonical `ANTFLY_*` or `TERMITE_*` environment variable, or
a repeatable `--antfly-env NAME=VALUE`; either changes the lock hash. Strict
headline enforcement forbids opaque prefixes for both Antfly and llama.cpp,
forbids synchronization-heavy per-operation CUDA profiling, and non-headline
evidence locks prefix digests. The harness never sources the
E2B tuning wrapper: the headline execution profile is model-neutral, and
topology-specific schedules must never leak into either E2B headline evidence
or E4B regression evidence implicitly.

## Server Batching

Measure warmed HTTP server throughput and repeatability with:

```sh
zig/pkg/inference/scripts/gemma4/benchmark_gemma4_cuda_server.py \
  --tokens 256 --warmups 1 --repeats 5
```

Continuous batching remains off. Before enabling it, run the row-2 manual
promotion gate for dense and compressed KV storage:

```sh
zig/pkg/inference/scripts/gemma4/benchmark_gemma4_cuda_batching.py \
  --tokens 256 --cache-dtypes f32 polar4 \
  --concurrency 1 2 4 --min-c2-speedup 1.5
```

The gate requires exact response equality, positive scheduler row-2 counters,
at least 1.5x C2 aggregate throughput versus batching-off C1, and bounded C1
latency. Concurrency 4 and above remains diagnostic under the current two-row
bounded envelope.

Because continuous batching remains default-off, scheduled and release CI
use an explicit `0.40` C2 speedup floor as a correctness and catastrophic-
regression check. The benchmark's `1.5` default remains the manual promotion
gate before enabling batching by default.


### EmbeddingGemma2 video embeddings

Linux CUDA accepts one H.264 MP4 clip per embedding input, through `inference
embed <model-dir> --backend cuda --video <clip.mp4>` or a `video/mp4` media part:

```json
{"model":"embeddinggemma-2","input":[{"content":[{"type":"media","mime_type":"video/mp4","data":"<base64 MP4>"}]}]}
```

The checkpoint's video processor samples at 1 FPS and uniformly caps the sampled
sequence at 32 frames. Each frame contributes up to 140 visual tokens. Image
wrappers surround each frame's video markers, with BOS/EOS once around the clip;
the encoder pools this complete sequence into one normalized embedding. This
preserves temporal order and does not average independently embedded frames.
Expanded inputs exceeding the configured token limit fail without truncation.

NVDEC surfaces, RGB8 conversion, torchvision-compatible antialiased bicubic
resize, patch projection, frame packing, and encoder activations stay on CUDA.
The NV12 conversion uses nearest chroma, round-to-nearest RGB8, and BT.601
(default/unspecified) or BT.709 with the declared full/limited range. This is
an explicit decoder policy; FFmpeg/PyAV RGB24 may use different chroma filtering
and integer rounding. The CPU handles compressed MP4 packets, timestamps,
resize coefficients, and token IDs. Only the final embedding vector is downloaded. Selected surfaces are
synchronized before NVDEC unmaps them. Decoder errors discard cached surfaces;
successful clips reuse the decoder.

The supported envelope is progressive 8-bit SDR H.264 MP4, at most 60 seconds,
256 MiB, 4096 pixels per side, and 18,000 packets. HTTP/CLI request budgets can
be narrower. Rotation, non-square pixels, HDR, interlacing, other codecs, and
mixed text/image/audio/video groups are rejected. The existing Gemma4 generation
sampler and still-image preprocessing are unchanged. NVDEC's `libnvcuvid.so.1`
is required at runtime; no FFmpeg process or runtime CUDA compiler is needed.

HTTP reports corrupt or truncated clips as `400 INVALID_VIDEO`, unsupported
backends as `400 UNSUPPORTED_VIDEO_BACKEND`, and a missing NVDEC library as
`503 VIDEO_BACKEND_UNAVAILABLE`. With `error_policy: "per_item"`, failures retain
their input indexes and valid sibling inputs can still complete.

For HTTP serving, set `ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda` so ordinary
requests use CUDA. A CUDA-specific preload alone does not force that selection.
Configure backend and scratch budgets for the model plus video workspace.

The checked-in six-second fixture at `src/testdata/video/closed.mp4` and its
provenance record cover decoder, sampling, cancellation, and resident preprocessing
regressions. CUDA tests also compare RGB conversion and resized patches against the
CPU reference. Preserve those small fixtures when changing the decoder or sampler.

Both Gemma4 and EmbeddingGemma2 video projectors compact into an owned device
allocation before releasing their accumulation buffer. CUDA row slices borrow
their source, so returning a slice here would allow subsequent token lookup to
overwrite projected video features. A CUDA regression test overwrites the
accumulator and checks the returned rows remain intact.

Production changes to video decoding, sampling, color conversion, or token
packing require renewed endpoint parity and recovery checks on the target GPU.

## EmbeddingGemma 2 encoder optimization profiles

The checked-in artifacts use NVCC `V13.2.78`; regenerate with that compiler patch
to preserve existing kernel code generation. New inference-only kernels are
appended to the bundle so existing kernel numbering remains stable.

The session snapshots the following optional profiles at construction. Each
arithmetic profile (including image cohorts and graph bucket padding) has a
distinct CUDA `model_identity`; CPU/Metal and default CUDA pins remain stable.
Changing profiles requires reloading the model and qualifying the resulting
vectors against the matching pinned BF16 reference. These profiles default off
pending NVIDIA hardware qualification.

| Control (`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_` prefix) | Execution |
|---|---|
| `EXTENDED_FLASH=1` | cuDNN global/local plans when available, otherwise bundled BF16 WMMA tiles across supported sequence lengths and SM80–SM120; masks and scalar tile tails remain explicit. Portable SM75 PTX never enters an empty WMMA body. |
| `FUSED_VISION=1` | Fused F32 rotary/Q-scaling math directly into BF16 attention inputs, BF16 SDPA output directly into projection; one final widening at the tower boundary. Declined plans retain the existing path. |
| `PACKED_PROJECTIONS=1` | Pack immutable Q/K/V and gate/up weights once, execute one GEMM per group and consume packed rows directly in fused head RMS/rotary and GELU multiply kernels. Also pack immutable PLE projections into groups bounded by 16 MiB, preserving BF16 GEMM/scale/norm boundaries. |
| `GRAPH_REPLAY=1` | Four shared encoder/vision arenas keyed by tower kind, batch, geometry and mask kind. Encoder batches support four items, vision batches eight, with 4096 total rows and a 128-MiB per-arena ceiling. Each arena has a 128-MiB ceiling and retained admission. Fresh typed inputs, masks and prefix lengths are uploaded before replay; compact results are read afterward. |
| `IMAGE_BATCH=1` | Existing image cohort opt-in also permits up to four staged video pictures. cuDNN vision plans carry the full batch dimension. |
| `NATIVE_VISION=1` | Native batched head-64 tensor-core attention with BF16 inputs/output and online softmax. Fused rotary packing feeds attention and projection without whole-tensor F32 bridges. No score/probability arenas or cuDNN dependency. The typed BF16 implementation carries a new native-vision identity version. |
| `MEDIA_BATCH=1` | Up to four ordered text/image/audio groups share preparation waves and length-bucketed BF16 backbone calls. CUDA Pillow resize, reference audio resampling and manual placeholder ordering remain explicit. |
| `FUSED_BACKBONE=1` | Fuse post-attention norm/residual/pre-FFN norm, post-FFN norm/residual, PLE scale/norm, and PLE norm/residual/scalar. Explicit BF16 conversions preserve intermediate boundaries; six launches per encoder layer and three per vision layer are removed. |
| `SPATIAL_POOL=1` | Merge spatial patches and apply optional per-channel standardization in one F32 kernel, preserving dy/dx addition order and separate arithmetic boundaries; RMS uses the existing backend reduction. |
| `TUNED_GEMM=1` | Time up to eight validated cuBLASLt BF16-input/BF16-output algorithms (one warmup, three completed-device iterations), cache the fastest admissible plan, and retry ordinary heuristics on tuning decline before the scalar fallback. |

Packed weights retain up to 512 MiB, and each of the four graph arenas has a
128 MiB ceiling. Every retained allocation holds both physical context accounting
and shared backend scratch admission until synchronized eviction or teardown.
Admission-policy changes evict these buffers before switching controllers.
Video and mixed-input requests reclaim optional caches once on admission denial.
Failed admission declines packing/capture before queueing work. Model unload
releases graphs before their backing storage and weights.

Graph keys distinguish sequence buckets and unmasked, prefix-masked and arbitrary
mask layouts. A changed attention workspace pointer or packed-weight generation
invalidates replay. Cancellation is checked before submission and after completed
readback; captured driver execution follows the existing supervised CUDA
interruption contract. Capture failure cannot return stale output.

The comparison harness records CUDA controls and observed route counters. Use
`--require-route extended_flash`, `--require-route fused_vision`,
`--require-route packed_projections`, `--require-route graph_replay`,
`--require-route native_vision` or `--require-route media_batch` to reject a
qualification run where the requested optimization never executed. A workload
must exercise the route: e.g. short repeated queries for replay, media for vision,
and medium/long sequences for extended flash. `--require-route batched_vision`
requires actual multi-image/frame execution, not just an enabled environment flag.

NVIDIA qualification must run the native attention partial-tile/batch differential tests, mixed-cohort versus
singleton vectors and cancellation/retry, GPU split/rotary differential tests, ordered
resident composition versus legacy BF16 boundaries (with checkpoint/media oracle
environment paths), masked
local/global tile-tail tests, fresh-input graph replay test, pretrained matched
BF16 comparison, and completed-device performance/memory measurements before
promoting any profile. Compilation and portable policy tests do not establish a
GPU speedup or numerical parity.

CUDA resize preparation retains four immutable geometry/pixel-policy plans within
an 8 MiB coefficient ceiling. Two pinned source slots (at most 64 MiB each) wait
only for their own H2D completion events before reuse. A cache hit builds/uploads
no coefficients and imposes no resize completion barrier. Retained coefficient
host/device storage and pinned sources hold shared admission leases separately
from request peaks. Geometry eviction and teardown drain readers; admission or
pinned-memory declines retain the synchronous uncached path. Resize hit/miss and
transfer counters expose actual reuse.

Ordered CUDA admission inspects image headers before decode, bounds text lengths
and retained media rows, and reserves the maximum of tower and encoder peaks.
Once preparation completes, singleton mixed/video requests retain only the actual
sequence, host metadata and encoder workspace. Prepared cohort execution keeps
future preparation and projected peers admitted; it does not release their peak
while a subsequent producer wave may still need it. The final ready wave drops
completed preparation reservations before acquiring its encoder supplement.

The native BF16 vision kernel retains the existing tiled online-softmax reduction
and BF16 output rounding. Qualification includes bitwise comparison against the
previous F32-boundary native kernel for batched and partial-tile inputs.
`FUSED_BACKBONE` has a distinct identity bit and an observed
`--require-route fused_backbone` gate. Run the GPU fused-versus-unfused boundary
test and pinned pretrained vectors/rankings before enabling it in production.

CUDA singleton and cohort audio use the same model sample ceiling, reference
resampler and expanded-token limit. WAV/FLAC duration headers bound admission
without decoding PCM; unknown durations retain the full model token envelope.
Decoded samples, packed features and final encoder phases remain separately
bounded. A preparation-wave memory decline falls back through the existing
singleton route instead of imposing a different duration limit.


Packed-projection and graph-replay identities are versioned for grouped PLE and
cohort graphs. Packed PLE columns feed strided fused scale/RMS directly without
a per-layer slice allocation or copy kernel. Grouped PLE changes GEMM output geometry and requires pretrained
vector/ranking qualification; `--require-route packed_ple` requires observed
execution. The extended-attention identity also versions 64-row tail padding on
all architectures carrying a bundled BF16 WMMA image. Defaults retain the existing
qualified architecture restrictions. The CUDA attention differential test covers
1/2/4-item batches, dense/prefix/holey masks, partial tiles and contexts through
8192; it records synchronized exact/flash wall times. These are qualification
measurements, not a replacement for warmed full-pipeline benchmarks.

CUDA ordered-media composition uses BF16 row storage. Hard lookup rounds once;
F32 projected media converts during row insertion, and cohorts pack directly into
zero-padded BF16 storage. The encoder accepts either resident F32 or BF16 input,
so existing video producers remain compatible without a padded F32 staging arena.

Audio QKV packing uses a checked 64-MiB output envelope instead of a 512-row
cutoff. CUDA named audio packing follows `PACKED_PROJECTIONS`, retains F32 outputs
through the existing linear dispatch and shares the packed-weight admission cap. Segmented audio attention carries signed clip-start tags alongside the
ordinary validity mask; convolution shares per-row starts. One kernel writes the
whole cohort, with no per-clip output assembly. Invalid rows and finite mask
probabilities preserve isolated-clip semantics. Backends that decline segmented
execution retain the existing per-clip fallback.

BF16-output tuning caches at most 256 plans per cuBLASLt context. Keys include
operation, input/output types, shape, workspace size, all pointer alignments and
tuning policy; plans never cross device contexts or driver lifetimes. Tuning
misses are serialized separately from the short plan-map lock. Cache hits enqueue
the selected GEMM without retiming. Graph capture permits cache hits only; misses
decline before discovery or CUDA event timing. Warm the actual geometry and
workspace before capture. `--require-route tuned_gemm` requires an algorithm
selected by timing multiple admissible candidates; heuristic retries and tuning
declines do not satisfy that gate.

Packed head consumers preserve the existing 256-thread RMS reduction, BF16
normalization and text rotary multiply/add rounding points. Vision rotary fusion
applies only to routes already consuming BF16 rotary inputs; the F32 attention
fallback retains its arithmetic. Packed GELU rounds its activation before the
multiply. `--require-route packed_heads`, `--require-route packed_gelu` and
`--require-route vision_residual` attest execution of these consumers. GPU tests
compare them bitwise against the separate kernels across head sizes, masked
batches and launch tails, and compare tuned GEMM eager/cached/graph results with
fresh inputs. These tests still require NVIDIA execution before qualification.

`PACKED_PROJECTIONS` uses the `packed-consumers-v3` identity version;
`FUSED_BACKBONE` uses `vision-residual-v2`. `TUNED_GEMM` has a separate arithmetic
bit and `tuned-bf16-output-v1` version. Reload and qualify resulting model pins
before using them. Defaults, CPU and Metal profiles retain their identities.

The extended attention dispatcher validates official GQA/MQA geometry before
choosing candidates. Unmasked/right-padded sequences from 16 tokens can try
cuDNN; sequences from 64 tokens can use bundled tiles, including arbitrary masks
and scalar tails. Unsupported plans retain exact fallbacks. Hardware qualification
must include short queries, holes, prefixes, batches and each target architecture;
`--require-route short_attention` proves short cuDNN execution. `EXTENDED_FLASH`
now uses the `extended-dispatch-v3` identity and remains opt-in.

Graph mode retains exact sequences below 128 tokens and permits at most 12.5%
additional rows for larger tiled buckets. Once an encoder graph is warm, two
completed replay requests and two completed eager requests seed a per-arena
latency estimate. Cold setup/capture are excluded. Replay requires a 5% measured
advantage; one real request every 64 samples probes the alternate route. Estimates
use the existing completed readback boundary, introduce no duplicate inference
or new device barrier, and reset when graph dependencies change.

Vision uses the same four retained arena slots and aggregate cache ceiling as
the encoder. Keys include both grid axes even when patch counts match. Position
tables upload once per retained geometry. BF16 native/cuDNN tower routes warm
actual packed weights and GEMM/attention plans before capture; generic F32
attention stays eager because it owns transient tensors. Workspace pointer and
packed-weight generation changes invalidate graphs. Cancellation remains
retryable, and synchronized eviction destroys graphs before buffers and releases
shared admission. `--require-route vision_graph` requires actual graph submission.
`GRAPH_REPLAY` uses the `media-graphs-v3` identity.

`SPATIAL_POOL` is a separate CUDA arithmetic profile (`spatial-pool-v1`), with
`--require-route spatial_pool` execution evidence. Metal uses a precise-library
kernel tested bitwise against the previous gathers/additions, including channel
tails, multiple frames, merge sizes and standardization. Neither route downloads
activations or constructs gather indices. The existing RMS reduction follows the
fused merge/standardization so its numerical contract remains intact; supported
model constant caches retain the no-scale RMS ones vector under bounded admission.

Vision scratch sizes follow the lifetime of each buffer: `state` and `a` use
768 channels per row; only `b`, `c` and `d` reserve 3072. Four 480×480 frames
with packed projections and BF16 rotary require 133,747,200 arena bytes
(127.55 MiB), so they fit the existing 128 MiB limit. The geometry helper checks
arithmetic overflow and tests this exact envelope; no retention ceiling increases.

CUDA model constants have a separate 8 MiB / 128-entry ceiling, including
snapshots detached from the map but still borrowed by a request. Keys are bounded
and restricted to model preparation. Each snapshot owns fresh device storage and
shared admission, publishes after a completed copy, and returns independent
request handles. Warm hits perform no upload, readback or barrier. Eviction drains
readers' queued work and drops the cache reference; live borrowers keep storage
and admission until their last release. Admission changes clear snapshots before
rebinding, and lost-context teardown releases host metadata/admission without
calling the destroyed driver context. Capture misses and budget declines retain
uncached preparation. `--require-route audio_constants` requires actual cached
per-layer query-scale hits, rather than hits on unrelated constants.

The fixed 16-kHz, 320/512-point, 128-bin audio recipe uses less than 2 MiB of
process-owned storage for its window, filterbank and CPU/GPU DFT layouts. It
contains no clip/model data, has no dynamically growing key space, and initializes
once under a mutex. Prepared clips borrow immutable tables; unusual configurations
retain request-owned preparation. CPU BLAS keeps its previous matrix layout and
arithmetic. GPU basis/filter copies use the bounded model-constant cache. Existing
request estimates conservatively include the borrowed tables.

Deferred CUDA image batches resize/pack into disjoint row spans of one admitted
patch matrix, then execute one patch-projection GEMM. No progressively growing
hidden concatenations remain. Declined device preparation uploads/copies only
that item's rows; singleton preparation keeps its existing route. Destination
bounds are checked before submission and cancellation/error cleanup preserves
stream ownership. `--require-route patch_into` attests direct destination writes.
`IMAGE_BATCH` and `MEDIA_BATCH` carry a `batched-patches-v2` identity marker;
reload and qualify the resulting pins. NVIDIA tests compare batched projection
against singletons and verify warm audio constants perform no query-scale
readbacks. Compilation and counters still do not establish NVIDIA parity or speed.


Audio QKV packing checks backend eligibility before resolving clamp bounds.
CUDA uses the immutable scalar cache and Metal uses the model-bound cache;
a warmed twelve-layer traversal performs no clamp-weight readbacks, even when
packing is declined. Unsupported backends retain their existing scalar path.

Audio frontend device kernels window one PCM upload, calculate complex
magnitudes without squared/reduction/reshape temporaries, and fuse log-floor
with row masking. They preserve explicit multiply/add ordering and use the
precise Metal library. CUDA channel normalization and NCHW flattening remain
on device. Equal-length clips share both subsampling convolutions and their
projection; incompatible lengths form separate cohorts without extra padding.
A conservative 128-MiB optional batch envelope counts retained convolution and
transpose storage; large clips retain the singleton route. Per-clip time masks,
channel statistics, cancellation and caller ordering remain
unchanged. The CUDA recipe is `embeddinggemma2-cuda-bf16-mean-v2-audio-frontend`;
reload qualification pins before serving it. `MEDIA_BATCH` additionally versions
the subsampling GEMM geometry with `audio-subsample-batch-v2`.

Graph retention has eight lightweight descriptors under the prior aggregate
512 MiB arena ceiling; each arena remains limited to 128 MiB. Hits refresh
recency and reuse counts. Empty slots need no drain, recent reused shapes resist
one-off misses, and replacement waits for the victim's last-use event rather
than the entire stream. Failed event creation/recording conservatively falls
back to a stream drain. Event recording covers successful and failed execution
exits, including final output reads. Model unload and lost-context cleanup retain
their existing admission and ownership contracts.
