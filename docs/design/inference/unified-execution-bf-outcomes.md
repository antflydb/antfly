# BF: CPU online attention row programs

Baseline: `9eb7df85b4`. Four CPU attention consumers now share one inline row
program with three admitted numerical schedules. It removes 2,171 authored
production bytes / 32 lines after charging the complete new owner and policy.
GPU source and artifacts are unchanged.

## Shared owner and contracts

`linalg.attention.updateOnlineRow` owns maximum selection, output/statistic
rescaling, probability publication and running-statistic updates. Its closed
`OnlineRowSchedule` enum expresses numerical schedules rather than model names:

| Schedule | Retained contract |
|---|---|
| `flash` | Scalar conditional maximum, zero an entirely masked initial tile, skip unit rescaling, explicit SIMD output scaling, maximum store before sum store |
| `segment` | Scalar `@max` fold, zero an entirely masked initial tile, skip unit rescaling, original scalar-loop scaling, sum store before maximum store |
| `dense` | SIMD maximum plus scalar tail, original maximum-change rescale guard, original vector scaling, sum store before maximum store |

The dense schedule also retains its original **prior-maximum load before the
SIMD reduction**. Empty-row policy is not broadened to dense attention, and the
flash/segment unit-rescale guard is not substituted for dense's maximum-change
guard. These distinctions remain static; there is no runtime schedule switch.

Consumers are causal FlashAttention, bidirectional FlashAttention, grouped
segment attention and dense vision row normalization. Query/key admission,
GEMM implementations and boundaries, scratch ownership/alignment, worker grain,
BLAS threading, cancellation and final output normalization stay with callers.
The shared phase has no allocations or scheduling operations, and is forced
inline into the existing loops.

## Original-program and hardware qualification

The original linalg sources are frozen from the committed baseline. Original
row phases are extracted with slice/stat identifiers renamed into qualification
wrappers; no arithmetic, reduction or store order is changed in that reference.

All three original/candidate row exports collapse to the **same optimized
function** for every compiled profile:

- Native Apple ARM (hardware qualification on M4 Max).
- Linux x86 baseline.
- Linux x86 AVX2/FMA (`x86_64_v3`).
- Linux x86 AVX512 (`x86_64_v4`).
- WASM SIMD128.

That is **15/15 identical row-program pairs**. LLVM aliases and emitted assembly
are retained. The x86/WASM checks are compilation evidence, not hardware timing
on those targets. They preserve the target-selected vector widths, explicit
FMA/separate-product policy and scalar tails.

An initial prototype passed all timing gates but evaluated the dense maximum
load after the SIMD reduction. Its dense wrapper did not merge with the frozen
original. Restoring the native read order makes dense merge too. The initial
source, LLVM IR/assembly and timing receipts remain under `prototype-v1`; the
final timing assay follows that real source change.

On the local M4 Max, **125 complete-pipeline bitwise comparisons pass** across
all four consumers. Dimensions include 3/7/8/9/31/32/33/64/65/128; cases include
query/key tile tails, grouped heads, bias, local windows, independent segment
ranges, empty rows, initially empty key tiles and dense query/key boundaries.

The fixed final AA/AB/BA assay passes **12/12 gates**: 31 pairs, one full API
call per sample, median ratio <=1.05 and p95 ratio <=1.10. Each API call includes
its allocation, GEMM and release work; this is not a pure row-loop timer. Warmup
precedes sampling and outputs are consumed. Candidate median ratios range from
0.99882 to 1.00720 for AB/BA; no CPU or full-model speedup is claimed. The paired
local run is not an isolated-core throughput qualification.

The complete attention test binary reports **44 passed, three platform-specific
skips, zero failures**. This includes independent numerical references,
gradients, all-masked rows, allocator-failure rollback and cancellation/drain
checks. The original and initial candidate selected suites also pass 24/24.
Source-accounting regression tests pass all nine cases.

The reusable `check_cpu_attention_rows.zig` fixture imports frozen `baseline`
and current `candidate` linalg modules. It performs pipeline parity by default;
`--bench` adds the fixed assay. Evidence under
`.benchmark-assets/1166/bf-campaign/` retains the original sources, extraction
script and row exports, five-target IR/assembly, raw samples, suite logs,
qualification binaries and complete net source accounting.
