# OCR kernel saturation follow-up

Shared Apple M4 Max, 36 GiB unified memory, macOS 27.0.1. These exploratory
measurements use an occupied machine. Whole-reader parity is not established
for LightOn. Isolated kernel results must not be substituted for page throughput.

## Selected production changes

- Qualified small OCR cohorts fuse gate/up projections, bias and SiLU × up in
  one TensorOps dispatch. 0.8B serves 7–8 rows; 4B serves 4–8. Inputs,
  accumulation and output remain F32 with BF16 weights. Unsupported shapes,
  missing pipelines and explicit precision experiments retain prior paths.
- The Zig wrapper now delegates small-batch qualification to the C selector.
  A new regression test invokes the wrapper with real model dimensions. An
  earlier wrapper guard silently rejected the new batches despite direct C
  tests passing; intermediate reader measurements below did not exercise fusion.
- Recurrent prefill processes four value columns per SIMD group and launches
  one SIMD group per threadgroup. The two model head counts, sequences
  128–2048, and 128-dimensional states are qualified. Existing request-owned F32
  state is reused; no additional scratch or persistent buffer is allocated.
- Wide prefill RMS normalization distributes work across SIMD lanes. For aligned
  hidden widths 1024/2560, each thread retains input vectors in registers across
  the reduction instead of rereading them. Misaligned views and unavailable
  cached-input pipelines retain the existing reduction. Head-width normalization
  uses the ordinary SIMD reduction. Other models retain existing dispatch policy.
- Qualified four-row QKV/FFN projections and the four-row 4B down projection use
  eight-way split-K with a 32-column tile. Partials borrow the existing frame-owned
  attention scratch. The 0.8B four-row down projection retains its previous path:
  the split candidate was slower. Existing scratch-capacity, resource ownership,
  alignment, missing-pipeline and planned-encoder checks remain active.
- The 0.8B single-token down projection uses one output per SIMD group, improving
  parallelism on this geometry. Other down projections and vocabulary heads retain
  their existing launches.

All new production selection is scoped to the existing M4 Max OCR profiles.
Weight residency remains opt-in and admission limits remain unchanged.

## Paired kernel comparison

[Final resident sweep](paired_shipping_shared.json): 43 cases, 30 alternating
trials per participant in one process. Native and MLX synchronize every sample,
trial order reverses each time, and every output is numerically checked. Shared,
page-aligned no-copy native weight storage matches actual model weights. Both
participants use their bounded weight residency policies.

38/43 median native latencies are within 20% of MLX or faster; 20/43 are faster.
This is a bounded exploratory comparison, not universal kernel parity. Five
resident cases remain 22–34% slower: 0.8B four/eight-row FFN projections,
0.8B four-row down, 4B four-row FFN, and 0.8B eight-row fused gate/up.
The full samples and negative results are retained.

| Operation | Native median µs | MLX median µs | Native / MLX |
| --- | ---: | ---: | ---: |
| F32 RMS norm, 554 × 1024 | 26.37 | 28.94 | 0.91 |
| F32 RMS norm, 554 × 2560 | 54.50 | 54.50 | 1.00 |
| Recurrent prefill, 554 tokens, 16 value heads | 416.06 | 746.96 | 0.56 |
| Recurrent prefill, 554 tokens, 32 value heads | 814.31 | 1301.58 | 0.63 |
| Fused gate/up, 4 × 2560 → 9216 | 516.25 | 475.67 | 1.09 |
| Fused gate/up, 8 × 2560 → 9216 | 503.00 | 708.40 | 0.71 |

[Final cold-cache sweep](paired_shipping_cold.json): 25 cases, 30 trials,
64 MiB GPU read/write eviction before each participant, outside the timed
interval. All 25 cases are within 20% of MLX. Cache policy materially changes
small-matrix results; neither sweep alone models a complete OCR reader.

Projection and recurrent numeric inputs are identical BF16-representable values,
but native storage/output is F32 while MLX is BF16. BF16 weight bits match exactly.
Normalization is F32 against F32. Recurrent state and gates are F32; native QKV
is interleaved while MLX Q/K/V are separate contiguous tensors. Thus this is
not a fully precision/layout-matched comparison. Independent double-precision
CPU samples supplement full native/MLX comparisons; the production C checker
compares full outputs and recurrent continuation against existing paths.

The earlier separate-process claim of 29/30 projection cases within 20% did
not survive the first improved paired sweep (`paired_full_shared.json`). That
sweep exposed several larger gaps and motivated the selected changes above.
The vocabulary heads are included in the final 43-case sweep; they were excluded
from the earlier 30-case projection result.

## Reader measurements and build provenance

The [shipping build manifest](shipping_build_manifest.json) binds the stripped
ReleaseFast binary to the five runtime source files and the source diff relative
to `5826bf2320e29e3ebb431ea89fa82463b437f57e`. Its runtime fingerprints also
match the final paired sweeps. The historical manifests below identify earlier
builds and must not be substituted for this one.

All rows complete 16 mixed pages. Native uses concurrency/rows/window 8/8/8,
greedy generation and a 256-token ceiling. Loading and serial reference reads
are untimed. Native timing includes preprocessing, queueing and decode; vLLM
also includes loopback HTTP. Reference prefix/processor caches are disabled.

| Model | Native pages/s | Refreshed reference pages/s | Native / reference |
| --- | ---: | ---: | ---: |
| LightOn 0.8B, opt-in residency | [1.0785](native_08_final/report.json) | [2.5535, vLLM Metal](vllm_08_refresh/report.json) | 42% |
| LightOn 4B, opt-in residency | [0.3390](native_4b_final/report.json) | [0.6465, vLLM Metal](vllm_4b_refresh/report.json) | 52% |
| Florence Q4_K / FP16 | [3.5459](native_florence_f16_repeat/report.json) | [3.8490, MLX-VLM](mlx_florence_refresh/report.json) | 92% |

The new-feature rollback measured [1.0005](native_08_final_rollback/report.json)
and [0.3372](native_4b_final_rollback/report.json) pages/s. The 0.8B result is
about 8% higher with these changes; 4B is effectively tied. These successive
runs are exploratory, not a causal or statistically stable speedup estimate.
The preceding round's faster absolute numbers also fell for both refreshed
reference runtimes as machine load changed. Whole-reader parity remains open.

[Reader profile summary](reader_profile_summary.json) records eight-page
instrumented runs separately from throughput. Actual reader dispatches include
cached normalization, recurrent columns4, and the new batch gate/up pipelines.
Encoder timing assigns compound encoder time to its first kernel; it must not
be interpreted as isolated convolution or gate/up latency.

Cohort counters average 3.57 active pages for 0.8B and 3.48 for 4B, about 44%
of the eight-page batch capacity as short pages finish. This is a scheduling
counter, not measured GPU hardware utilization. Continuous cohort refill and
batched vision/prefill remain unimplemented; isolated kernel parity does not
resolve this utilization gap.

Validation on this shipping runtime:

- [74 focused GPU tests pass, zero skipped](shipping-gpu-tests.txt), including
  real-checkpoint/HTTP, wrapper reachability, recurrent histories, admission,
  cancellation, coalescing and existing Gemma source-policy regressions.
- [All C Metal conformance checks pass](shipping-conformance.txt): full outputs,
  independent CPU samples, recurrent continuation, nonzero offsets, output
  canaries, optional pipeline/rollback fallbacks, shared attention/projection
  scratch reuse and overlapping submitted frames. Cached normalization checks
  both public entry points, aligned/scalar-aligned views, and missing pipelines.
- [Output validation](output_validation.json) checks all 13 successful native
  reports against original native text exactly, timed own-serial output exactly,
  and declared cross-runtime reference content after markup/whitespace removal.
  Failed loads are recorded separately. Three synthetic fixtures are not a
  broad document quality evaluation.
- Python lint/syntax/formatting, license headers, Apache boundary (2423 files),
  whitespace checks and repository-required cache cleanup are checked separately.
- [Log normalization](log_normalization.json) preserves original/stored SHA-256
  values while normalizing line endings and trailing whitespace.

Historical intermediate reports are retained separately:

- `build_manifest.json`: intermediate build, recurrent launch still 128 threads
  per threadgroup, and the Zig wrapper still rejected small fused batches.
  `native_08_resident` / `native_08_rollback` measured 1.214 / 1.076 pages/s;
  `native_4b_resident_retry` / `native_4b_rollback_retry` measured 0.415 / 0.372.
  These are successive uncontrolled runs, and establish no gain from batch fusion.
- `native_4b_resident`, `native_4b_rollback` and `native_4b_bf16_prefill` failed
  live-memory admission. They are retained as failed loads, with no throughput
  assigned. Later ordinary retries were admitted; no limits were bypassed.
- `final_build_manifest.json`: corrected wrapper and 32-thread recurrent launch,
  preceding the final cached normalization and projection changes. Its 74 GPU
  tests passed. The filename is historical; it is not the shipping manifest.
- `native_4b_baseline_profile` uses the preceding committed runtime/build manifest
  from `../projection_parity_m4_max/`. It is an eight-page diagnostic and must
  not be included in final 16-page throughput comparisons.

Checkpoint pins and hashes are unchanged from
[the checkpoint manifest](../kernel_parity_m4_max/checkpoints.json). Three
synthetic invoice/receipt/shipping fixtures establish consistency, not broad
OCR quality. Florence native uses Q4_K weights with F16 encoder mirrors and
no-repeat n-gram size 3; MLX-VLM uses original FP16 weights and its greedy
policy. Installed vLLM Metal 0.32.0 has no Florence adapter.

## Rejected and diagnostic experiments

Retained `paired_*`, `controlled_*`, and `cold_splitprojections.json` reports
include explicit experimental variants. Only `candidate_applied: true` rows
measure the named candidate. They are benchmark-only and never linked into
Antfly. Their embedded source/library hashes identify the tested versions;
earlier source snapshots are retained as `.txt` files. Final production proof
uses `paired_shipping_*`, the shipping build manifest and reader reports.

Unrolled scalar/native-BF16 streaming, alternate SIMD segments, one-output
four-row GEMV, and static-K TensorOps descriptors did not improve the target
cases consistently. A maximum-threadgroup compiler hint also left the streaming
candidate more than 3× slower. Global BF16 prefill rounding did not improve the
0.8B page result and is not selected. Split-K fused gate/up is promising on
some cold-cache cases but has inconsistent cache behavior; it is not promoted.
The eight-row 4B split-fusion candidate exceeded the existing per-slot scratch
capacity and was rejected, with no extra allocation added.

Earlier MLX observer probes mishandled list outputs or empty synchronization
command buffers. Those are diagnostics, not production results. The qualified
observer self-test encodes work through both compute-encoder entry points and
blit, checks the output, and requires positive GPU timestamps. Empty completed
buffers without encoded work are counted separately; missing timestamps on
actual encoded work remain failures.

## Reproduction and rollback

Use the worktree's stripped ReleaseFast inference test binary, the local Zig
version required by `AGENTS.md`, and `benchmark_concurrency.py` with a matching
build manifest. `--weight-residency` remains an explicit option.

The paired benchmark compiles its own test-only bridge:

```sh
python benchmark_metal_pair.py --compile-native-library \
  --native-library /path/to/metal_pair_native.dylib \
  --timing-library /path/to/metal_command_timing.dylib \
  --suite full --trials 30 --output paired.json
```

Build the timing observer as an ARC dynamic library with Foundation and Metal.
The paired runner checks runtime and bridge source hashes against embedded
hashes before reuse. Add `--suite quick --cache-flush-mib 64` for the cold-cache
policy. `benchmark_mlx_primitives.py` provides separate-process diagnostics;
prefer paired measurements for direct comparisons.

New rollback switches (all set to `1`):

```text
TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_GATE_UP
TERMITE_METAL_DISABLE_QWEN35_PREFILL_COLUMNS
TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_NORM
TERMITE_METAL_DISABLE_LIGHTONOCR3_SINGLE_DOWN_TILE
TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_SPLIT
```

The concurrency harness exposes corresponding `--disable-*` switches. Existing
master, projection, normalization and precision rollback switches remain active.
