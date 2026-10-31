# AE: Q4/Q5 panel accumulation ownership

Baseline: `2e37a81c48`, after the AD integer-dot tile consolidation.

## Implementation

Seven handwritten Q4/Q5 panel16 accumulation entries and two generated cached
result entries now share `q4q5Panel16Accumulate`. Compile-time row geometry,
cached sums/scales and contiguous versus separate weight views retain the
existing leaf selection. MR8 keeps two MR4 dot tiles within each subblock.
There is no heap allocation or runtime format dispatch.

The owner preserves coefficient-load order, low-before-high publication and
the original floating-point expression. Direct entries obtain activation sums
from the Q8 block; cached entries consume their supplied lane vectors. The two
entries accepting external scale vectors retain that behavior. Generated result
wrappers retain their existing array layout and publication code. Removing the
Q4/Q5 branch also simplifies the remaining Q6 renderer; its generated regions
are unchanged.

## Qualification protocol and findings

Four fixtures cover zeros, signed extrema, mixed byte patterns, Q4/Q5 weight
ranges, signed scales and nonuniform cached lane sums/scales. All nine entries
are checked bit-exactly against an independent scalar I64/F64 reference. The
result-returning entries start from zero; accumulating entries start with
nonzero lane/row-dependent biases. Both images use the identical test driver.

The initial seven-entry prototype fails the earlier seven-body identity probe:
the two projections being refactored change their machine code. That failed
receipt remains under `initial-seven-body-identity-failure.log`. Before timing,
AE explicitly declares five unchanged controls and two changed targets. The
three protected GEMM/attention bodies and two untouched legacy projections must
retain their instructions, resources and required alignment. The changed Q4/Q5
single/triple projections must stay within 1% instruction growth and 64 extra
bytes of fixed stack. These resource bounds do not replace performance checks.

The fixed direct matrix covers 45 native profiles and 39 generic-AArch64
profiles, executing the latter without SDOT on the same M4 Max. Every family
uses 32 arms per AA/AB/BA cohort, ten warmup batches and 21 measured batches.
The new panel entries measure 1,001 calls per batch; the previously qualified
integer/Q8/Q6/K16 drivers retain 20,001. Whole-batch durations and fractional
per-call timings are retained. Admission remains 5% median / 10% p95 with no
automatic timing retries. Machine-code inspection checks measured calls remain
inside their timing loops and generic call closures contain no SDOT.

Fresh composed validation covers seven CPU model cases and five controls using
the existing fixed 32-arm protocol, finite byte-identical output checks, route
checks and an independent decoder reference. Native and generic direct timing,
compilation and model timing run sequentially on the host.

## Source budget

Including every replacement helper and renderer, maintained production shrinks
by **14,003 bytes / 180 lines** and generated production by **5,582 bytes / 52
lines**: **19,585 bytes / 232 lines** of production text removed. Inline tests
add 9,811 bytes / 143 lines, charged separately. This is a CPU family reduction,
not a new 20% claim across all kernel source.

CUDA and Metal source are unchanged. The older CUDA queue remains a separate
qualification of its pinned snapshot; AE claims no fresh CUDA device timing.
Receipts and the earlier seven-entry passing native prototype are retained in
`.benchmark-assets/1166/ae-campaign/`.

## Final results

All 45 native and 39 non-SDOT profiles pass every AA/AB/BA cohort. Native
AB/BA worst median ratios are below 1.00440/1.00895 and p95 below
1.06007/1.03312. Non-SDOT worst median ratios are below 1.04100/1.03207
and p95 below 1.03652/1.08307. These are outward-rounded bounds from the
retained receipts, not speedup claims. Timing-loop inspection passes for both
images; no generic call closure contains SDOT.

The five unchanged machine-code controls pass. The changed triple projection
uses 18,685 instructions versus 18,758, with the same 36,272-byte fixed stack.
The single projection uses 35,487 instructions versus 35,383 and 71,344 stack
bytes versus 71,328. Their static stack-memory instruction counts change from
5,250 to 5,257 and 7,697 to 7,783 respectively. Both targets meet their
predeclared resource bounds. The initial seven-entry prototype and final
nine-entry version are separately retained. The inherited native-proof stdout
label says seven; its structured receipt and actual script inspect five, as
recorded in `native-proof-log-note.txt`.

The caller selection passes 116 tests with one existing optional
`ANTFLY_LAYA_REFERENCE` fixture skip. Combined Metal/CUDA compilation, x86-64
v2/v3/v4 oracle test-binary builds, WASM32/64 product builds and freshness of
all 70 generated files / 129 owned regions pass. The x86 checks compile the
new panel oracle as well as the integer oracle; they do not execute x86 code.

All seven composed CPU models and five controls pass their fixed 32-arm
comparison, with finite byte-identical outputs. Worst median/p95 ratios are
below 1.04167/1.08929. Route checks and the independent tiny-decoder
reference pass with the existing 0.002 tolerance. No automatic retry or
performance-limit change was used.

All 8,019 final Zig source pins match the measured snapshot in both CPU modes.
The temporary baseline was audited to contain only the independent test
overlay before removal; its patch and both qualified CPU-mode binaries remain
in the campaign bundle. Task-scoped cache GC considers only this worktree's
cache/output. No branch is pushed.
