# AD: native integer-dot tile ownership

Baseline: `d91c957c6a`, after the AC Q6 panel consolidation.

## Implementation

Eleven generated integer-dot wrappers and the handwritten two-row 16-byte leaf
now consume one native `integerDotTile` algorithm. Existing signatures,
inlining policies, tuple/array/vector/row-vector results and contiguous views
are retained. Geometry, result representation and SDOT versus portable integer
arithmetic specialize at compile time. There is no heap allocation or runtime
format dispatch.

The native owner keeps the qualified schedules: one-row SDOT consumes weight
loads directly, wider rows share weight views, and the two-row eight-column
16-byte SDOT leaf reduces immediately to retain its live-register bound. The
non-SDOT wrapper still composes two single-row tiles. Publication constructs
the existing result type directly; portable operands first load through their
unsigned byte-vector view before signed widening.

## Findings and retained prototypes

The first flat-result prototype passes its numerical oracles but grows the
protected Q4/Q5 triple/single projection bodies from 18,758/35,383 to
18,894/35,999 instructions. It is excluded. Direct result publication and the
retained immediate-reduction schedule restore the protected bodies.

An intermediate typed-result implementation formats field names at compile
time. The model build exceeds its existing 20,000-branch evaluation budget;
static field names remove that cost without increasing the budget.

The next variant passes the protected-code checks and all consumed Q8/Q6/K16
comparisons, but the two direct portable 32-byte cases regress by about
4.85x/3.34x in AB and 4.83x/3.31x in BA. Disassembly shows byte-extraction and
shuffle sequences replacing the baseline's widening multiply loads. A direct
array-to-signed-vector bitcast caused this lowering. Restoring the explicit
unsigned vector load before the signed cast removes the regression. These
failed samples are retained, not described as benchmark noise or baseline wins.

The prototype patches, binaries, compiler failures and raw timings are kept
under `flat-result-prototype/`, `typed-field-format-prototype/` and
`array-bitcast-prototype/` in the campaign bundle. `vector-load-native/` retains
the passing native variant before the explicit non-SDOT tiling guard.

## Qualification protocol

Four signed-byte fixtures include zero inputs, both signed extrema and mixed
patterns. Twelve leaf entries match an independent scalar I64 oracle exactly.
The consumed Q8, Q6 and K16 families retain their independent scalar references.
Baseline and candidate use identical drivers.

The fixed native matrix contains 36 profiles: twelve integer leaves, nine Q8,
six Q6 and nine K16/Q8 panel cases. A second build targets generic AArch64 and
runs on the same Apple M4 Max with SDOT disabled. Its 30 profiles include all
portable/fallback entries; the six SDOT-only entry points are ineligible there.
This is execution of the fallback code, not merely cross compilation.

Each family uses 32 fixed arms in each AA/AB/BA cohort, ten warmup batches and
21 measured batches of 20,001 calls. Whole batch durations and fractional
per-call timings are retained. Admission limits remain 5% median / 10% p95,
with no automatic timing retries. Disassembly checks every distinct non-inlined
kernel call inside its timing loop. Compiler-folded identical profiles may
share a body; logical profile counts are not unique machine-function counts.
The generic build is also checked to contain no SDOT in these call closures.

## Direct results

All 36 native and 30 non-SDOT profiles pass every AA/AB/BA cohort. Native
AB/BA worst median ratios are 1.00292/1.01195 and worst p95 ratios are
1.00477/1.01283. Non-SDOT worst median ratios are 1.00684/1.01773 and p95
ratios are 1.00561/1.03388. The controls remain within the original gates.
These are upper bounds rounded outwards from the retained raw receipts.

The corrected vector-load boundary also improves existing 16-byte fallback
code relative to baseline. The one-row/eight-column integer leaf measures
AB/BA median ratios 0.25972/0.25485 (about 3.9x faster); Q6 MR1 is about 0.313
and K16 MR1 without minimum correction about 0.271–0.273. These are isolated
non-SDOT AArch64 results on the M4 Max, not native-SDOT, x86 or model-wide
speedup claims. Immediate integer reduction restores the original Q6/K16
native frame bounds. The native protected GEMM/attention/projection bodies
retain all seven required instruction/resource/alignment checks.

## Source budget

All new numerical, load and publication helpers are charged. Maintained
production shrinks by **2,636 bytes / 40 lines**, generated production by
**19,277 bytes / 481 lines**, for **21,913 bytes / 521 lines** of total production
text removed. The renderer sheds 3,769 bytes, offset by 1,133 bytes added to
native numerical/result ownership. Inline tests add 8,795 bytes / 139 lines and
receive no production reduction credit. This batch does not establish a new
20% whole-kernel reduction.

CUDA and Metal source/images are unchanged. Glowworm's earlier AA queue remains
a separate qualification of its pinned snapshot, waiting behind existing work;
no other job was stopped and no fresh CUDA timing is claimed here. Receipts are
retained under `.benchmark-assets/1166/ad-campaign/`.

## Final model, build and cleanup results

Seven CPU composed model cases and five controls pass their fixed 32-arm
comparison. All outputs are finite and byte-identical. Worst median/p95 ratios
are below 1.02273/1.05698, with the same 5%/10% limits.
Model route checks and the independent tiny-decoder reference pass. The three
small norm controls use the existing 8,192-iteration warmup protocol, and the
decoder uses 100 warmup / 201 measured iterations; both images use identical
arguments declared before timing. No model-wide speedup is inferred from the
non-SDOT microbenchmarks.

The final caller selection passes 115 tests; its one skip requires the optional
`ANTFLY_LAYA_REFERENCE` fixture. Combined Metal/CUDA compilation, x86-64
v2/v3/v4 direct-oracle test-binary builds, WASM32/64 product builds, and all 70
generated files / 129 owned regions pass. The x86 builds check compilation;
only native and generic AArch64 execute locally. Every file in the final Zig
source manifest matches the measured snapshot.

The task-created baseline worktree was verified to contain only the recorded
independent test overlay, with production bytes identical to the baseline. Its
patch and both native/non-SDOT binaries are retained before worktree removal.
Task-scoped cache GC considers only this worktree's cache/output; other
worktrees and user caches are untouched. No branch is pushed.
