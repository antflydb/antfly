# CUDA Tuning Profile

Gemma 4 QAT CUDA measurements must use the shared profile in
`scripts/gemma4/gemma4_qat_cuda_tuning.sh`. The CLI benchmark and server wrapper both
source it so production route settings stay aligned.

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
  route fails the performance run instead of silently falling back. Headline
  evidence must observe that route independently for Gemma 4's 256-d local
  heads and 512-d global heads (8 query heads / 1 KV head); route telemetry is
  deduplicated by exact `(route, head_dim)`, not by the first model-wide hit.
- Tiled F16 exact and warp-specialized GQA prefill profiles: experimental and
  off unless selected by their explicit typed profile.
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
switch that restores the pre-promotion unset behavior.

`ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1=ggml-ffn-v1` selects the experimental
single-row E2B GGML-Q8_1 FFN route. Its default is `off`, and qualification is
scoped to SM89. Likewise,
`ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE=sm89-prefill` is an
experimental SM89-only BF16 prefill profile. That typed profile is fixed at 8
algorithm candidates, 1 warmup, 3 timed iterations, and a minimum of 128 rows;
these are part of the profile contract rather than free tuning knobs. Its
default is `off` so normal inference never performs synchronous first-use
algorithm timing.

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
prompts, alternating row order, and a staggered mixed-length/page-growth probe.
Scheduled and release CI enforce that correctness corpus with a `0.40` C2
regression floor. Manual promotion runs retain the `1.5x` throughput threshold.

## Model-Neutral Kernel Catalog

The checked-in AOT catalog selects kernels by semantic operation signature,
runtime shape, and target, not by model name or layer index. Inspect a GGUF from
the inference package directory:

```sh
cd zig/pkg/inference
ZIG=../../../.tools/zig-x86_64-linux-0.16.0/zig
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
uses a distinct CUDA graph replay key, and none is production-enabled.

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
../../../.tools/zig-x86_64-linux-0.16.0/zig build \
  quant-kernel-cuda-attention-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast -- --kv-len 1024
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
../../../.tools/zig-x86_64-linux-0.16.0/zig build \
  quant-kernel-cuda-paged-attention-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast -- \
  --head-dim all --kv-len 2003 --pattern all --key-format all \
  --value-format all --page-order all --heads 8 --kv-heads 2 \
  --iterations 100
```

`--json` emits `antfly.cuda_paged_attention_diff.v2`. On SM89 L4, the F16
production geometry is bitwise exact at the 511/512/513 selector boundary and
at 2,003 logical tokens for both HD256/SWA512 and HD512/global attention,
including permuted pages, cancellation-heavy inputs, and NaN-poisoned unused
storage. The F32 control is also bitwise exact at 2,003 tokens. Earlier F32
matrix measurements across HD256/HD512, Polar4/F16 keys, and all three input
patterns measured 1.43x to 1.83x faster than the handwritten paged baseline.
At 2,350 logical tokens with 8 query heads / 1 KV head and F16 K/V, the
shared-recurrence consumer remained bitwise exact for all three patterns and all
three page orders. It measured 2.90x to 2.92x faster than the handwritten
HD256/SWA512 baseline and 2.88x to 2.89x faster than the handwritten
HD512/global baseline over 100 raw launches. Rerun the full
differential and end-to-end gates before changing the SM89 crossover or
widening the automatic architecture set.

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
../../../.tools/zig-x86_64-linux-0.16.0/zig build \
  quant-kernel-cuda-paged-prefill-diff -Dcuda=true -Dmetal=false \
  -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast -- --json
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

The validator locks F32 values, disables the handwritten split experiment and
dense generated attention in both arms, alternates execution order, compares
every token ID, requires persistent graph replay, and requires the dedicated
`launch_attention_gqa_decode_score_prework` counter. On the E2B QAT model and
SM89 L4, the three-prompt corpus measured 98.918 versus 82.183 tok/s median at
256 outputs (1.1999x) and 67.422 versus 44.483 tok/s at 1024 outputs (1.5157x).
All six pairs had exact token IDs, persistent replay, zero fallback, and no
graph discard or capacity skip. The F32-cache production control remained
healthy at 91.103 tok/s with 251 persistent replays. These results justified
retaining the candidate; the later F16 bitwise-parity and paired-throughput
qualification promoted the route to the default automatic selector on the
qualified SM89 Gemma 4 F16 geometry. Enabling it beyond that geometry still
requires broader model/context coverage via the explicit gate.

The generated serial decode path now matches the production decode-scalar
contract exactly: it passed three prompts at 64 and 256 output tokens with a
0.9995x median paired throughput ratio, and three prompts at 1024 output
tokens with zero graph-capacity skips. Split-KV is substantially faster in the
current short-output corpus (about 1.28x at 256 tokens with a 128-token
threshold), and those runs had exact token IDs. It is still experimental: the
same split-128 candidate changes one token at position 915 for one prompt in a
three-prompt 1024-token corpus, despite a roughly 2x throughput gain. Do not
enable split-KV in a production profile or promote the dense split-KV
generated attention catalog entries until a long-output, multi-model parity
corpus passes; only the exact score-prework composites carry promotion.

The split-count sweep on the SM89 L4 keeps split-8 as the short-context winner:
at 256 output tokens it measured 115.8 tok/s versus 89.9 tok/s for the
baseline (1.288x median) with exact tokens on the three-prompt corpus. Split-2
and split-4 both changed the prime-number prompt at token 232. At 1024 outputs,
split-8 measured 103.3 tok/s versus 51.4 tok/s (2.01x median) but retained the
sky-prompt change at token 915; raising its threshold to 512 reduced throughput
to 1.54x without moving that divergence. These are development measurements,
not production tuning defaults.

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

The current generated body specializes Q6 sub-layout unpacking while preserving
the four dependent DP4A operations and the complete floating-point reduction
order. In the canonical SM89 artifact it uses 40 registers with no local memory,
versus 96 registers for the handwritten Q6 stage-1 baselines. The isolated
complete chain is bitwise-equal at every stage-1 partial value and index, and
measured 1.129x at K=2560 and 1.085x at K=3840 on the L4. This has not cleared
the production gate: a three-repeat Gemma 4 12B Q4_K_M graph-replay run at 64
and 256 tokens had exact token IDs, required route hits, and zero fallbacks, but
a 0.9992x median paired ratio and 0.9847x worst pair. Keep the gate off until a
larger end-to-end improvement is demonstrated.

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
  ../../../.tools/zig-x86_64-linux-0.16.0/zig build \
    -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast)

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

On an NVIDIA L4, the verified ReleaseFast result was 90.138 Antfly decode
tok/s with CV 0.0006, versus 134.34 llama.cpp eval tok/s. The raw throughput
ratio was 0.671. The comparable ratio, using llama.cpp evaluation plus
sampling time, was 0.728.

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
0.16.0 and CUDA 13.2 are required. A dirty development tree is allowed for
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

The verified full LM-head matrix covered three prompts and four lengths. All
12/12 cases had exact tokens and zero candidate fallback counts. Median paired
improvement was 2.52%; the worst case improved 1.59%. This evidence supports
the current profile default of LM-head argmax on.

Generated attention remains off by default. The earlier three-prompt divergence
was traced to candidate routing through the non-device-scalar warm-up path,
whose production kernel uses a different reduction contract. Generated
attention is now restricted to the matching decode-scalar path. The serial
candidate has exact token parity across the current three-prompt 64/256 corpus
and is throughput-neutral; split-KV remains opt-in because its long-output
parity gate still fails despite its material throughput advantage.

The corrected isolated E2B FFN chain measured 1.23x at width 6144 and 1.31x at
width 12288, but the five-repeat full-model gate improved only 0.21% at the
median and diverged deterministically on two of three prompts. The isolated
harness synchronizes after each chain, while production replays a CUDA graph,
so launch savings do not translate directly. These kernels remain dev-only.
Require repeated full-model evidence before changing the profile default:

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
query heads share one KV head (the 8:1 MQA endpoint of GQA), the rendered prompt
is 8,251 UTF-8 bytes / 2,051 tokens, prefill uses four 512-row chunks plus a
three-row tail, and decode is fixed at 300 tokens with an F16 paged KV cache.
This topology is an exact route constraint, not a model-neutral assumption.

The 2026-07-30 warm-server collection measured Antfly at 726.81 ms TTFT and
113.424 decode tokens/s, versus llama.cpp at 316.99 ms and 116.167 tokens/s.
Total latency was 3,362.98 ms versus 2,890.91 ms, a 1.1633 Antfly/llama.cpp
ratio. This was exploratory one-pair evidence, not a superiority result. It
shows that decode is within 2.4%, while the approximately 410 ms TTFT deficit is
the dominant remaining gap.

The 2026-07-31 ten-pair warm-server collection (`benchmark_gemma4_long_e2e_server.py
--cuda-execution-profile gemma4-e2b-sm89-flash-splitk-v1 --collect-only`, two
warmups, balanced AB/BA) confirmed that gap with paired statistics: Antfly
measured 740.8 ms median TTFT and 110.24 decode tokens/s versus llama.cpp at
329.5 ms and 114.38 tokens/s; total latency was 3,454.5 ms versus 2,945.1 ms, a
1.1730 median ratio with a [1.1700, 1.1753] paired bootstrap 95% CI. Decode is
within 4%; the 2.25x TTFT deficit is the entire remaining end-to-end gap. This
profile includes the collect-only split-K decode candidate, so it is a
performance-frontier measurement, not an exact-output configuration.

The promoted-routes-only production configuration is materially slower on the
same workload class. In the tuned pair-harness CLI config (F16 caches, 512-row
prefill chunks, a 1,457-token prompt, 511 greedy tokens) Antfly measured 76.7
decode tokens/s with 5,002 ms median prefill versus `llama-completion` at 131.3
tokens/s and 201 ms prompt eval. Those figures predate the 2026-07-31
flash-prefill promotion: the SM89 flash prefill route is now a production
runtime default through the automatic profile selector (unset
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE`; rollback `off`). The remaining
still-unpromoted surface is split-K decode,
`ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL`, and the capture/readback
extras in the reviewed flash profile environment. With F32 K/V
caches the same workload decodes at only 32 tokens/s: every score-prework
selector is F16-only, so the automatic route is ineligible and F32 long-context
comparisons measure the legacy decode path by construction.

Two operational notes for reproducing these numbers. The harness's default
`model-neutral-v3` execution profile is a deliberately untuned reviewed
baseline — it measures a 7.6x total ratio on this workload — so frontier
claims require the versioned flash profile. The pair harness requires the
`llama-completion` binary; current llama.cpp `llama-cli` builds ignore
`-no-cnv` and enter interactive conversation mode, which hangs the run, and the
harness's fixed `-c 2048` llama.cpp context caps prompt plus output at 2,048
tokens.

The SM89 split-K online decode prototype reduces the locked Antfly decode path
from approximately 74 to 114 tokens/s and replays safely through the persistent
CUDA graph. Its 64-way online-softmax/value regrouping is deterministic, but it
changes the generated token stream after output token 136 relative to the
legacy chronological recurrence. FP64 merge coefficients do not materially
reduce that drift. Consequently `splitk-online-sm89` remains default-off and
collect-only; exact-output policy must not be weakened just to promote it. The
exact alternative, parallel canonical-order score generation followed by the
canonical tiled64 consumer, is bitwise-identical but projects to only a 1.021x
attention speedup. A future concurrent runtime must also make split-K
score/counter workspace request- or stream-owned; the current module-owned
workspace is valid only for batch one with continuous batching disabled and a
serialized stream.

For Flash prefill, wider q32/q64 and serial two-/four-head grouped designs were
qualified and rejected: all were slower on the locked matrix. The retained
q16/k16 kernel now uses the compile-time `kThreads == 256` stride in its four
hot cooperative loops after launch validation. This lets ptxas unroll K/V
loads without changing launch geometry, shared-memory layout, or occupancy.
The regenerated production template passed all 90 guard, page-table,
adversarial-input, and determinism cases with bitwise-identical output. Its
alternating L4 A/B projects a 1.0355x attention speedup (8.81 ms over 35
layers), clearing the dedicated aggregate 1.02x micro-optimization gate. The
1.20x algorithmic gate for a new Flash design remains unchanged.
The follow-up concurrent q16/k16 two-head CTA was also bitwise-identical across
all 90 qualification cases, but its best launch-bounded build projects only a
1.0652x speedup and spills in HD512. It therefore remains standalone; a new
grouped design must reduce persistent-fragment register pressure and handle the
three-row tail separately before another production screen.

The bounded PLE-gate BF16 mirror-first profile is also implemented as a typed,
default-off SM89/E2B candidate. It routed all 175 eligible prefill projections
through the admitted BF16 mirror, preserved all 140 decode projections on the
fused Q4 path, and recorded no eligibility misses. On the locked 2,051-to-300
workload it reduced TTFT from 867 to 813 ms, but decode throughput regressed
slightly (114.811 to 114.460 tokens/s) and the generated stream first diverged
at zero-based output index 154, with 50 of 300 positions differing. Strict
parity therefore rejected promotion after the first pair. Keep
`ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE=off` in production; the
candidate exists for controlled numerical-quality experiments, not as a tuning
default.

An Nsight Systems request-window trace attributes 332.9 ms of 562.8 ms GPU
busy time to Flash attention and 229.8 ms to non-attention work. GPU utilization
is 97.7%, leaving only a 13.3 ms device-idle upper bound, so CUDA Graph capture
is useful for CPU concurrency but is not the primary TTFT solution. The next
production priorities are: an upload-packed SM89 W4A16 Tensor Core projection
engine with a documented numerical contract, model-shape cuBLASLt tuning with
admitted persistent workspace, fused gate/up activation output, and a genuinely
concurrent GQA Flash redesign that reuses K/V without the v3 register/tail
costs. Each remains independently gated; projected savings overlap and must not
be added without end-to-end device-event evidence.

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

Because continuous batching remains default-off, the GitHub release workflow
uses an explicit `0.40` C2 speedup floor as a correctness and catastrophic-
regression check. The benchmark's `1.5` default remains the manual promotion
gate before enabling batching by default.
