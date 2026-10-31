# AI: shared prepared float four-column kernels

Baseline: `e7c850baaa`. The maintained float-input Q4/Q5 and Q6 four-column
kernels share one weight-load/decode/publication step and one compile-time row
owner. Four private MR1/MR2 ABI leaves retain their signatures. Direct paired and
triple callers now use the common owner; the maintained renderer is updated
and its owned triple regions regenerated.

Q4/Q5 keep unsigned values, eight scale/minimum subblocks, paired 32-value
traversal and column-first metadata reads. Q6 keeps signed values, sixteen
scale-only subblocks and unrolled 16-value steps. Both retain row-first output
publication and reuse decoded weights across the two-row tile. Format, row
count and half traversal specialize at compile time, with no allocation or
indirect call. Physical prepared layouts and GPU code are unchanged.

The same independent test overlay is installed in baseline and candidate.
Four fixtures cover zero inputs, weight extrema (Q4 0/15, Q5 0/31, Q6 -32/31),
mixed signed inputs and signed, nonuniform power-of-two scales/minimums.
Scalar f64 accumulation with literal prepared-layout offsets checks every
vector lane, including nonzero initial accumulators. Complete tiled callers
are checked independently after reduction. Existing composed Q4/Q5 tests
also exercise row/column tails, pair/triple projections and guarded outputs.

There are twelve changed direct/caller profiles and nine unchanged K16/Q8
controls in each CPU mode. Fixed 32-arm AA/AB/BA comparisons retain the existing
25 ms warmup, 21 measured batches and 5% median / 10% p95 limits. Host load is
recorded; no automatic timing retry is used. Changed bodies must pass the
pre-timing resource bounds of at most 1% instruction growth and 64 additional
fixed stack bytes. Every measured call must remain in its timing loop; the
generic AArch64 call closure must contain no SDOT. Complete static call
closures are included in instruction and fixed-frame resource accounting.
Four composed projection contexts also pass the same resource bounds, and
five untouched GEMM/attention/legacy bodies retain their strict instruction
and resource checks. Seven composed CPU models and five controls then run their existing fixed
32-arm comparison, output/route checks and independent decoder reference.

New owners and ABI leaves are charged to maintained source; tests and docs
are accounted separately. Generated production receives no reduction credit.
The initial driver vector-index syntax and a missed private-owner call-site
migration are retained as pre-qualification compile findings. Both are repaired
before any accepted timing. The first eager-weight prototype fails the native
resource gate (496 to 533 instructions, 160 to 288 fixed stack bytes in one
leaf). Its images and disassembly are retained in `eager-weight-prototype/`.
The revised step restores MR1 per-column decode/accumulate publication; the
resource and timing limits remain unchanged. Identical Q4/Q5 specializations
may fold into one compiled body while retaining all twelve measured profiles.
Evidence is retained in `.benchmark-assets/1166/ai-campaign/`.

## Results

Maintained production removes **6,916 bytes / 130 lines**, charging the new
owners, ABI leaves, direct call sites and renderer. Generated caller regions
add 546 bytes / 30 lines, leaving a net production-text reduction of
**6,370 bytes / 100 lines**. Inline tests add 6,917 bytes / 96 lines and are
charged separately; documentation is also separate.

All 21 native profiles pass every AA/AB/BA cohort. Worst AB/BA
median ratios are at most 1.02684/1.00967; p95 ratios
are at most 1.01348/1.01931. These are
recorded-load comparisons with no automatic timing retry or relaxed limits.

All 21 generic profiles pass every AA/AB/BA cohort. Worst AB/BA
median ratios are at most 1.01338/1.00909; p95 ratios
are at most 1.01877/1.01421. These are
recorded-load comparisons with no automatic timing retry or relaxed limits.

All seven composed CPU models and five controls pass the existing fixed
32-arm comparison. Worst model/control median and p95 ratios are at most
1.00325 and 1.02511. All 384 retained outputs are finite
and byte-identical per case across arms; route checks and the independent
decoder reference pass at the existing 0.002 tolerance.

All four direct/caller binaries pass 46 selected tests with zero skips.
The broader caller suite passes 123 tests with 1 existing optional-fixture
skip. Metal/CUDA compilation, x86-64 v2/v3/v4 oracle compilation,
WASM32/64 builds and generated-source freshness pass. Only native and generic
AArch64 execute locally; no fresh GPU or x86 device timing is claimed.

All 16 distinct changed compiled driver/caller bodies and their
static call closures pass the declared instruction and fixed-frame resource
bounds. All four composed projection bodies also retain instruction counts
and fixed stack sizes, including the Q4 fused triple path. Five protected
GEMM/attention/legacy bodies retain instructions, resources and required
alignment; changed address references resolve to identical data bytes. Equal
instruction counts alone are not treated as identity. Every measured call remains in its timing loop, and the complete measured generic
call closure contains zero SDOT. These tests do not claim identical resources
in every possible inlined caller context.

All 8,020 source pins match the measured inputs. Baseline production
is audited unchanged before removing its temporary worktree; its test overlay,
all four test executables and both model executables are retained. GPU source
is unchanged. No branch is pushed.
