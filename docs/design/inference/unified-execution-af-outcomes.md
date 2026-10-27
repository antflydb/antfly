# AF: shared CPU panel and block traversal after the main merge

Baseline: merge `4ed0d8bf7f`, incorporating `origin/main` at `7dabd028d6`.
The merge is clean. Upstream inference changes update build dependency wiring
and exhaustive engine-error handling; no new kernel file is introduced.
Upstream changes receive no consolidation credit.

## Implementation

The two handwritten Q4/Q5 panel8 entries now use the same native accumulation
owner as the nine panel16 entries consolidated in AE. Column width specializes
at compile time. The eight-column single-row entry retains its fully unrolled
subblocks, and the two-row entry retains two single-row dot calls with shared
weight views. Existing panel16 entries retain their leaf policies, cached sums,
external scales and low-before-high accumulation expression.

Three Q6 panel16 entries now share `q6KPanel16Rows`. The numerical owner retains
sixteen 16-byte subblocks, typed MR1/MR2/MR4 dot results, both dot halves before
coefficient loads and the original floating-point expression. Existing result
publication helpers remain in use. No heap allocation or runtime format switch
is introduced.

Three K16 four-column block entries also share `k16Block4Rows`. MR4 keeps
its two MR2 dot tiles, multirow coefficient loads remain before updates, and
input scaling still occurs after all sixteen subblocks. Tuple/array wrappers
retain their original result ABI. Nine new direct profiles cover no minimum,
minimum from block sums and minimum from externally supplied sums for each row
geometry. The independent reference preserves final scaling, including signed
zero, and covers full signed-byte weight extrema.

The first K16 compile infers an integer type too narrow for tile-size
multiplication. An explicit `usize` tile dimension fixes it; the failure and
patch remain under `k16-index-type-prototype/`.

The first compile detects that the existing 16-byte and 32-byte row-vector dot
results have distinct Zig types. The shared type selector now includes byte
width rather than assuming structural type equivalence. The compiler failure
and prototype patch are preserved in `result-type-prototype/`.

## Qualification

Baseline and candidate are both built from the merged revision with identical
new test drivers. Four independent scalar I64/F64 fixtures cover zeros, signed
activation extrema, mixed patterns, Q4/Q5 unsigned weight ranges, signed Q6
weights, signed scale coefficients and minimum correction. All five new entries
must match the scalar result bit-exactly. Existing integer/Q8/Q6/K16 and cached
panel16 oracles remain part of the matrix.

The fixed direct matrix contains 59 native profiles and 53 generic-AArch64
profiles without SDOT. Every family uses 32 arms in each AA/AB/BA cohort, a fixed 25 ms
warmup and 21 measured batches. New panel geometry and existing AE
accumulation profiles use 1,001 calls per batch; older leaf drivers retain
20,001. Admission remains 5% median / 10% p95, without automatic retries.
Machine-code checks require measured calls inside timing loops and no SDOT in
generic call closures. Five untouched GEMM/attention/legacy bodies retain strict
identity checks; the two Q4/Q5 projections retain AE's explicit bounds of at
most 1% instruction growth and 64 extra fixed stack bytes against the fresh
post-merge baseline. Full composed-model checks follow the direct matrix.

The first native timing run is unqualified: an integer AA control has a
1.10630 p95 ratio, and an unchanged Q8 BA control has a 1.06180 median ratio.
All five new geometry entries and the nine existing panel16 accumulation
profiles pass. Inspection confirms identical normalized kernel bodies for all
integer and Q8 controls. Host inspection during the run observes fseventsd at
167.6% CPU and mediaanalysisd at 88.4%; activity drops afterward. This is
interference evidence, not proof of a cause for individual samples. The failed
run remains under `first-native-run/`.

The intended manual requalification reaches its quiet-window check but never
starts timing. While macOS media analysis remains busy, the three K16 block
entries are added to the consolidation. Only this task's idle waiter is
stopped; no service or other job is interrupted. The final combined snapshot
therefore receives fresh baseline/candidate builds and the complete 59/53
profile matrices, rather than reusing the earlier prototype's acceptance.

All compilation finishes before timing. The strict idle-host attempt requires
three samples ten seconds apart with no process above 50% CPU and aggregate
usage at most 150%. Other debug library builds keep that condition unavailable.
Its complete observations are retained under `strict-idle-*`; no other job or
service is interrupted.

The first combined snapshot then receives one fixed comparison under recorded
host load, without changing source, drivers or limits. It is also unqualified:
integer AB/BA controls exceed the median limit, K16 AA controls exceed it too,
and one K16 BA case exceeds it. The binaries, source pins and raw rows remain
under `recorded-load-short-warmup/`. Integer kernel and timing-loop alignment
and sizes match between images; alignment is not established as the cause.

Inspection shows that ten warmup batches take about 0.3 ms for the fastest
integer and K16 cases. Both drivers now use a fixed 25 ms warmup per profile;
measured loop bodies, iteration counts, 21 samples, arm order, every AA control
and the original 5%/10% limits stay unchanged. This is a declared measurement
setup change, not proof that short warmup caused the earlier failures. Fresh
baseline/candidate builds and full qualification follow it. No automatic timing
retry is enabled. Host activity is sampled every ten seconds through direct
and model timing; any accepted comparison is reported under recorded load,
not as an isolated-host result.

The projection targets retain their instruction counts and stack usage. An
additional strict seven-body probe finds the triple projection's normalized
instructions unchanged; the single projection, which includes the narrow-panel
path being refactored, changes control flow. Matching counts are not described
as instruction identity. The five unchanged controls retain their strict
checks; the declared two-target resource and performance gates remain in force.

## Source budget

All replacement numerical/type owners and renderer changes are charged.
Maintained production removes **2,991 bytes / 73 lines**; generated production
removes **15,627 bytes / 304 lines**, totaling **18,618 production bytes / 377
lines**. Native handwritten ownership grows by 2,487 bytes while the renderer
shrinks by 5,478 bytes. Inline tests add 13,030 bytes / 187 lines, separately
charged. This does not establish a new whole-kernel percentage reduction.

Metal/CUDA source is unchanged. The earlier CUDA queue is a separate pinned
qualification; no fresh device timing is claimed. Evidence is retained under
`.benchmark-assets/1166/af-campaign/`.

## Results

All 59 native profiles pass each fixed AA/AB/BA cohort. Worst AB/BA
median ratios are below 1.01474/1.00335, and p95 below
1.05936/1.03237.

All 53 non-SDOT profiles pass each fixed AA/AB/BA cohort. Worst AB/BA
median ratios are below 1.02615/1.03500, and p95 below
1.04976/1.03636.

The caller selection passes 118 tests with its one existing optional
`ANTFLY_LAYA_REFERENCE` skip. Combined Metal/CUDA compilation, x86-64
v2/v3/v4 oracle builds, WASM32/64 products and all 70 generated files / 129
owned regions pass. x86 validation checks compilation, not execution.

All seven composed CPU models and five controls pass their fixed 32-arm
comparison, with finite byte-identical outputs. Worst median/p95 ratios are
below 1.00514/1.03850. Route checks and the independent tiny-decoder
reference pass with the existing 0.002 tolerance. No automatic retry or
performance-limit change was used.

All 8,020 final Zig source pins match the measured snapshot in both CPU modes.
The temporary baseline was audited to contain only the independent test
overlay before removal; its patch and both qualified CPU-mode binaries remain
in the campaign bundle. Task-scoped cache GC considers only this worktree's
cache/output. No branch is pushed.
