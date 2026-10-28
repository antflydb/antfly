# AJ: kernel-family consolidation

Baseline: `d89b8ad34fec1e79cea082ad4fb5faddbbb44dca`.

The six work packages are implemented. CPU and Metal qualification is complete;
fresh CUDA runtime qualification remains queued behind existing Glowworm work.

## Source result

Maintained production removes **25,324 bytes / 414 lines**, charging every new
owner, descriptor, catalog, ABI adapter and schedule policy.

| Maintained source | Bytes removed |
| --- | ---: |
| Metal shaders | 21,053 |
| CPU native kernels | 1,766 |
| Generator owners | 2,550 |
| CUDA device kernels | -44 |
| Metal host bindings | -1 |
| **Total** | **25,324** |

Generated production removes another 18 bytes with unchanged line count,
for a net production-text reduction of **25,342 bytes / 414 lines**. Inline
Zig tests add 12,265 bytes / 191 lines; maintained test/accounting tooling adds
1,125 bytes / 19 lines. Documentation and CUDA images are separate.

The campaign's overall 25–40 KB maintained-source target is met. Savings are
concentrated in Metal. CUDA's new common owners leave its source size
essentially flat; this is not a 20% reduction of every backend. The narrower
first-three-package 20 KB planning target is not claimed independently.

## Implemented owners

| Package | Final shape |
| --- | --- |
| AJ1: CPU projections | Q6/Q8 wide-column emission and grouped projection prefixes share owners. Both emitted grouped functions match the baseline byte for byte. |
| AJ2: GPU normalization | Metal shares tree RMS statistics and SIMD4 inverse phases. CUDA shares warp/block sum/max statistics with static plane counts and a compile-time epilogue inside the first-warp branch. |
| AJ3: GPU layouts/pointwise | CUDA shares head indexing, gather/scatter conversion policies and BF16 GELU-product forward arithmetic. Repeated-head gradient collapse and the analytic GELU gradient retain their distinct rounding orders. |
| AJ4: attention phases | Metal shares MPP prefill attention across 16/32 query tiles, preserving entry-scope scratch allocations. Florence HD32 shares score/value phases with separate serial/parallel denominator policies. |
| AJ5: descriptors/catalogs | Ten native MPP affine entries share a geometry/storage/unroll catalog and one numerical owner; split projection tiles share a separate partial-GEMM owner. CUDA projection metadata and CPU parameter/prefix declarations share construction. |
| AJ6: CPU scalar boundaries | Prepared Q4/Q5/Q6 scalar and four-column arithmetic share integer decode/scaling owners. Scalar Q6 retains its lexical traversal to preserve the exact compiled hot path. |

Existing exported names, dispatch decisions, eligibility, physical layouts,
training reduction algorithms, volatile FP32 staging and synchronization stay
intact. New policies specialize at compile time; the kernels acquire no
runtime function-pointer dispatch or allocation.

The attention checker also exposed a baseline bug: optimized MPP/direct paths
added uniform value mass when every query score was invalid, while the generic
path returned zero. Both legacy uniform fallbacks are removed, and the
maintained checker independently asserts exact zero for poisoned queries.

## CPU qualification

Native and generic AArch64 run on Apple M4 Max. Independent scalar/f64 and
codec-decoded references cover Q4/Q5/Q6 float arithmetic and Q6/Q8 grouped
projections, signed scales, extrema, bias, pair/triple projections, row tails,
column ranges and output guards.

All **48 distinct compiled driver/caller bodies and their complete static call
closures** satisfy the original <=1% instruction-growth and <=64-byte extra
fixed-frame bounds. Measured calls remain inside their loops; generic closures
contain no SDOT. Four composed projection contexts retain their resource
bounds. Five protected GEMM/attention/legacy bodies retain instructions,
resources and required alignment; relocated references resolve to identical
data. These checks cover the recorded callers rather than every possible
future inlining context.

All **114 separate-process caller/control profiles** pass fixed 32-arm
AA/AB/BA comparisons at the original 5% median / 10% p95 limits. Worst AB/BA
median and p95 ratios are 1.030872 and 1.034142.

All **12 scalar profiles** qualify with controlled same-process comparisons.
The actual closed compiled leaf instructions are linked from the retained
baseline/candidate images into one process. Only constant-pool page relocations
change. Inspection verifies every other instruction word, literal byte and
original entry alignment; independent f64/guard checks validate the ABI and
fixtures. Both arms share buffers and process, and direct calls stay in the
2,001-call loops. The original 25 ms warmup, 21 batches, 32-arm AA/AB/BA order
and 5%/10% limits remain unchanged. Worst native AB/BA median/p95 ratios are
1.005219/1.040501; generic ratios are 1.036056/1.003272.

The broader suite passes **125 tests with one existing optional-fixture skip**.
Both backend configurations compile; x86-64 v2/v3/v4 and WASM32/64 builds pass.
Generated-source freshness passes for **70 files and 129 owned regions**.
Only native/generic AArch64 execute locally; no x86 device timing is claimed.

## Metal qualification

All **110 direct profiles** pass AA calibration and both AB/BA pipeline creation
orders, including independent references, exact finite output parity, guards
and resource checks. Main-library safe/fast intrinsic policies match production;
MPP uses the production precise Metal 4 configuration. Fixed 500-dispatch and
500 ms warmups, 2,000 launches per arm, 32 balanced arms, and 5% median / 10%
p95 limits are retained, with no automatic timing retry.

Worst MPP AB/BA median ratios are 1.013195/1.012585 and p95 ratios
1.020848/1.007797. Worst main-library AB/BA medians are 1.037875/1.042655 and
p95 ratios 1.038559/1.036014.

The full production projection checker passes. The paged-attention checker
passes **158 cases**, including nonfinite Q/K/V, padded pages, short queries,
producer/consumer scopes, cancellation, fallback routes and the independent
invalid-query zero regression.

## Composed models and controls

All seven CPU models and five controls pass the original fixed 32-arm matrix.
Worst model/control median and p95 ratios are **1.017242 and 1.015152**.
All **384 retained outputs are finite and byte-identical** per case across arms.
Route checks and the independent decoder reference pass at the existing 0.002
tolerance; the decoder's worst sampled absolute error is below 0.000668.

## CUDA evidence and pending device work

CUDA 13.2 compiles portable PTX, SM89, SM121a and the seven-target fatbin.
Every supported target retains **454 exports**, identical register, stack,
local/shared-memory resources and the original <=1% instruction bound.
SM100/SM110/SM120 disassembly is unchanged for every export. SM75/SM80/SM89/SM90
change only RMSNorm's input VJP, with unchanged instruction count/resources.
The **SM121a cubin is byte-identical to baseline**. Checked-in PTX, fatbin and
SM89 images are regenerated from the final pinned inputs; source policy passes.

A final source-pinned runtime controller waits behind existing Glowworm jobs.
It contains 49 independent normalization forward/gradient, pooling, masked
contrastive, layout/repeated-head and GELU cases with fixed AA/AB/BA comparisons.
The earlier superseded controller was stopped while waiting, before GPU work.
No unrelated job is stopped. Static/identity evidence is separate from this
pending fresh CUDA runtime receipt.

## Retained findings and provenance

* The first shared native wide traversal increased some paired/triple caller
  instruction counts by up to 2.4%. It was replaced with shared emission that
  preserves the original generated schedules.
* The first CUDA statistics owner added a padded instruction group on
  SM75/SM80 RMSNorm and SM90 mean pooling. A redundant outer guard did not
  repair it. Keeping the compile-time epilogue inside the first-warp branch
  restores the seven-target instruction/resource bounds.
* The first separate-process scalar matrix failed generic Q6 MR1 BA median
  at 1.051922. An explicit decode boundary alone retained the same allocation
  and received no timing retry. The lexical traversal restores exact baseline
  instruction bytes. A second matrix still reports native Q4/Q6 MR1 AB
  medians of 1.051607/1.084592 despite exact instruction bytes and alignment.
  Both separate-process matrices remain failed and retained. The controlled
  protocol applies to all scalar profiles; it does not select passing samples
  from those failed matrices or relax their limits.
* The original baseline Metal poison failure, focused reproduction and final
  zero-mass regression are retained. Finite timing uses the original baseline.

Evidence resides in `.benchmark-assets/1166/aj-campaign/`, including source
pins, images, machine-code proofs, protocols, raw measurements and failed
prototypes. All **8,020 source pins** match the measured inputs. The comparison
checkout is audited byte-for-byte unchanged except for the two known appended
tests; their patch and all baseline test/model images are retained.
