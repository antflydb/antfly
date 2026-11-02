# Q8K grouped projection loop screen

Status: numerical and emitted-work screening passed; production optimization
not enabled. This investigates P6 in [the production plan](../production-cost-plan.md).
It does not establish a smaller installed product or a faster cold build.

`linearQ8KQ8KActivationGroupedRange` combines four-, two- and single-row work,
16/8/4-wide prepared panels, raw-weight fallback and tails, separately expanded
for two and three projections. Replacing projection `inline for` loops with
runtime `for` loops leaves SIMD lane unrolling and each projection's accumulation
order intact. This can substantially reduce LLVM's expanded body without
introducing calls inside quantized-block loops.

Several candidates were screened in private overlays, leaving production sources
unchanged:

| Candidate | Changed projection loops | Original two/three-instance bytes | Candidate bytes |
| --- | ---: | ---: | ---: |
| All row paths | 36 | 397,228 | 136,936 |
| Four-row path only | 12 | 397,228 | 231,932 |
| Four-row helper split | 12 | 397,228 | 246,352 |
| Four-row helper split, guarded call | 12 | 397,228 | 246,272 |
| Four-row path, no dot-product CPU feature | 12 | 648,464 | 419,600 |
| All row paths, no dot-product CPU feature | 36 | 648,464 | 262,672 |

These are **estimated Mach-O object text extents**, including possible padding,
not final linked size savings. They are the sum of this function's two concrete
instances, not an entire inference owner. Externally callable dynamic request
wrappers prevent a constant-shaped test caller from eliminating unused paths.
The raw results retain individual symbols and their estimated-extent flags.

## Numerical and runtime coverage

The repeatable [screening tool](../../../../zig/tools/screen_q8_grouped_projection_loops.py)
clones the existing independent sibling-matmul oracle, uses varied signed input
and weights, and adds 288 comparisons between unchanged and candidate dynamic
worker calls. These cover:

- Two and three projections; bias and no bias.
- Row counts 1, 2, 3, 4, 9 and 33, exercising MR1/MR2/MR4 and row tails.
- Raw weights and panel4/panel8/panel16.
- Three valid Q8K activation scales.
- Full ranges and unaligned partial ranges ending in column tails, with sentinel
  checks outside the requested range and beyond the requested rows.

Packing stays outside the timed region. Samples alternate implementation order,
rotate valid activation buffers, and consume output checksums. Short row cases
run more iterations to reduce timer/scheduling noise. Original numerical
assertions and tolerances are retained; no reference comparison was relaxed.

On native macOS ARM64 with the pinned Zig 0.17 compiler, the varied-input,
four-row-only screen with fused SDOT enabled had candidate/baseline median
ratios of 0.62–0.75 for rows 4/9/33. Rows 1/2/3 ranged from 0.87 to 1.055.
With fused SDOT explicitly disabled, the range for rows 4/9/33 was 0.64–0.95;
small-row cases reached 1.062. Each comparison used four alternating repeats.
These are synthetic observations on a shared busy host. They do not qualify
model latency, throughput or an absence of regressions. The all-row varied-input
no-dot-product screen passed too, with observed ratios of 0.92–1.015; that
profile offers less synthetic runtime gain despite its smaller emitted body.

The split variants place the four-row body in a separate non-inlined function,
called once per worker range. The guarded variant skips this call when there
are fewer than four rows. Neither adds calls inside the quantized-block loops.
Both passed numerical checks at 1536×512 or 3072×1536 projection shapes. The
unguarded split showed small-row ratios up to 1.028 at the smaller shape and
1.038 at the wider shape, with multi-row ratios of 0.62–0.75. The guarded wide
screen used six repeats and reached 1.054 for one small-row/panel case, while
multi-row cases remained at 0.63–0.70. These measurements still leave a serving
regression question open. The emitted sizes match across the two shapes.

Each completed varied screen reports 23 successful build steps and 37 passing
tests, including two selected kernel tests with no skips. The remaining tests
are dependencies; they are not 37 different kernel-shape suites. An intentional
one-second timeout also verified failed-run receipts and overlay cleanup.

The initial all-row candidate had a 7–11% raw-weight slowdown at one/two rows.
The narrower candidate improves multi-row work but still has mixed small-row
results, so neither was applied to production. Keeping a source loop unchanged
does not guarantee identical small-row machine code: LLVM optimizes the whole
surrounding function.

Zig's `baseline` CPU on macOS ARM64 is Apple M1 and already includes dot product.
Adding `+dotprod` is not a fallback-versus-SDOT comparison. The tool records the
actual compiled `native_aarch64_dotprod` flag and the effective fused-SDOT switch.
Use both `--fused-sdot off` and `--cpu baseline-dotprod` to test their distinct
fallback paths.

## Reproduction and next gate

From the repository root:

```sh
python3 zig/tools/screen_q8_grouped_projection_loops.py \
  --mode mr4 --report .benchmark-results/compile-costs/q8-mr4.json
python3 zig/tools/screen_q8_grouped_projection_loops.py \
  --mode mr4 --fused-sdot off \
  --report .benchmark-results/compile-costs/q8-mr4-unfused.json
python3 zig/tools/screen_q8_grouped_projection_loops.py \
  --mode all --cpu baseline-dotprod --fused-sdot off \
  --report .benchmark-results/compile-costs/q8-all-fallback.json
python3 zig/tools/screen_q8_grouped_projection_loops.py \
  --mode mr4-split-guarded --in-dim 3072 --out-dim 1536 \
  --iterations 15 --repeats 6 \
  --report .benchmark-results/compile-costs/q8-split-wide.json
```

The tool runs from its overlay's `zig/` directory, uses worktree-local caches,
retains the exact source patch/build log/report and deletes its overlay on exit.
It never installs another production binary. The current tool records compiler
and script hashes; a timeout/interruption terminates only its own process group
and retains the failed-run receipt.

Next, qualify the split four-row body and single-row decode separately;
then compare representative Q8K model decode/prefill, threading/cancellation and
real inference-owner compiler time/RSS. Only a candidate passing that gate should
be applied and measured in both stripped production artifacts. Do not infer a
compiler-time improvement from object text extents.

Ignored receipts under `.benchmark-results/compile-costs/` include
`q8-grouped-screen`, `q8-grouped-matrix-screen`, `q8-grouped-mr4-screen`,
`q8-grouped-mr4-dotprod`, `q8-grouped-mr4-fallback`,
`q8-grouped-mr4-varied`, `q8-grouped-mr4-varied-unfused`,
`q8-grouped-all-varied-fallback`, `q8-grouped-mr4-split-varied`,
`q8-grouped-mr4-split-wide`, and `q8-grouped-mr4-split-guarded-wide` JSON/build logs.
The first three used local precursor scripts and constant-per-row activation
fixtures; the varied-input runs use the checked-in tool. Do not mix their
runtime ratios as a controlled comparison.
