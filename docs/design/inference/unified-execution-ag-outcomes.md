# AG: shared ordinary and packed Q4/Q5 subblocks

Baseline: `0bfe6e2627`, after AF's panel and block consolidation.

## Implementation

Ordinary Q4/Q5 panels and packed QKV now share `q4q5PanelSubAccumulate`.
Compile-time sum policies retain block-derived sums, cached lane vectors and
already prepared vectors. Row tiles, contiguous versus separate-row dot leaves,
coefficient reads and low-before-high publication retain their existing order.
MR8 still uses two MR4 dot tiles within each subblock. The eight-column MR1 path
retains its unrolled traversal and early sum read.

The common physical subblock view records fixed-size coefficient and weight
pointers. Packed views shrink from six slices (96 bytes on this host) to six
pointers (48 bytes). This is descriptor storage, not a claim that tensor storage
or runtime stack frames halve. The packed tensor byte layout is unchanged.

Five packed-specific helper functions are removed. Six groups of three handwritten
projection calls become six compile-time loops, preserving Q/K/V order. Public
operator interfaces are unchanged; no heap allocation, runtime format dispatch
or function-pointer callback is added. A small shared adapter packs pointers to
existing low/high accumulators.

## Qualification protocol

The baseline and candidate receive identical new drivers. An initial driver
mistakenly selects the new private interface while building the baseline; its
compiler error and driver remain in `private-declaration-prototype/`. Selecting
the known slice-view versus pointer-view interface fixes the driver. Numerical
references remain independent of either interface.

Four fixtures cover zero inputs, signed activation extrema, mixed byte patterns,
Q4/Q5 weight ranges, signed coefficients, nonzero accumulators and nonuniform
cached sums/scales. Twenty new profiles cover MR1/MR2/MR4/MR8: three individual
projections, all three projections together, and the complete packed projection
caller. The last group explicitly selects packed row-tile flags and checks
internal dispatch, Q8 block headers, cached sums, bias publication and
simultaneous projection accumulation. Existing caller tests separately check
automatic route admission and larger row groups/tails. Every output
must match an independent scalar I64/F64 reference bit-exactly.

The full direct matrix contains 79 native profiles and 73 generic-AArch64
profiles without SDOT. Each family runs fixed 32-arm AA/AB/BA cohorts with the
25 ms warmup established in AF and 21 measured batches. New packed profiles use
1,001 calls per batch; existing drivers retain their established counts. Limits
remain 5% median / 10% p95 with no automatic timing retries. Host load is sampled
every ten seconds; these comparisons are reported under recorded load.

Five untouched GEMM/attention/legacy bodies retain strict instruction/resource
checks and required alignment. The ordinary single/triple projections and the
packed MR4/MR8 row-tile functions are changed targets: before timing, each must
stay within 1% instruction growth and 64 extra fixed stack bytes. Timing-loop
inspection checks the measured calls and the generic build's absence of SDOT.
Caller tests, both backend builds, x86 and WebAssembly compilation, generated
source freshness, seven CPU models and five controls follow sequentially.

## Source budget

All replacement numerical, descriptor and pointer adapters are charged.
Maintained production removes **6,910 bytes / 164 lines**. Generated production
is unchanged; these savings do not come from relocating stored expansions.
New inline tests are charged separately. This is an additional CPU-family
reduction, not a new whole-kernel percentage claim. Metal/CUDA source is
unchanged, and no fresh CUDA device timing is claimed.

Evidence is retained under `.benchmark-assets/1166/ag-campaign/`.

## Results

All 79 native profiles pass each AA/AB/BA cohort. Worst AB/BA median
ratios are at most 1.02099/1.02839, and p95 ratios at most
1.05513/1.04600. These are recorded-load comparisons;
no automatic retry or performance-limit change is used.

All 73 fallback profiles pass each AA/AB/BA cohort. Worst AB/BA median
ratios are at most 1.02546/1.04747, and p95 ratios at most
1.03545/1.02492. These are recorded-load comparisons;
no automatic retry or performance-limit change is used.

All seven CPU model cases and five controls pass their fixed 32-arm
comparison. Every retained output is finite and byte-identical across arms.
Worst model/control median and p95 ratios are at most 1.00700
and 1.00472. Route checks and the independent decoder reference
pass with the existing 0.002 tolerance.

Five unchanged machine-code controls and all four changed-target resource
bounds pass. Packed views report 96/48 bytes in baseline/candidate respectively.
The four full-caller packed profiles pass as well as individual and simultaneous
projection profiles. Matching instruction counts alone are not treated as
instruction identity.

The selected caller suite passes 122 tests with 1 existing optional-fixture
skip. Combined Metal/CUDA compilation, x86-64 v2/v3/v4 oracle builds,
WASM32/64 product builds and all 70 generated files / 129 owned regions pass.
Only native and generic AArch64 execute locally; x86 checks compilation.

All 8,020 final Zig source pins match both measured snapshots. The baseline
worktree is removed after verifying its production is unchanged and retaining
its test-only patch plus both CPU-mode binaries. Cache GC considers only this
worktree's caches/output. No branch is pushed.
