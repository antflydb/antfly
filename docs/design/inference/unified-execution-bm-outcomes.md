# BM: Row-complete Metal projections and bounded CUDA readback

This batch merges `origin/main` at `63cf18d980` into the combined Tile/generate
branch (`b0210343a0`). The three intervening main commits introduce no native
kernel changes. The PR is ready for review; its full CI gate requires a human
approval tied to the final head SHA and selected suites.

## Isolating main's Metal decoder defect

The BL numerical failure is now isolated. Layer traces agree through attention,
then main's gate/up projections leave later prompt rows zero. Its
`termite_metal_decode_runtime_apply_linear_pair_slots_device` dispatches
`rows * out_dim` invocations of `termite_apply_linear_1x`; that kernel returns
for `gid >= out_dim`, reads only the first input row, and writes only the first
output row. This is a shape/dispatch defect, not a permissible rounding delta.

Clearing inherited optional CPU callbacks alone does not fix main: the independent
F64 prefill error remains 0.0187094. An isolated main build that bypasses only the
multi-row paired route and uses the ordinary two-projection fallback instead
passes all five decoder logit checks (maximum 0.00075894, unchanged 0.002 budget)
and full/chunk prefill becomes byte-identical (unchanged 0.0001 gate).
Its complete five-step output is also byte-identical to the branch candidate.

The branch already corrects this through its shared row-aware dense pair encoder;
this batch adds a regression rather than a redundant production workaround.
The same test fails against main's original encoder (expected -0.15429688,
actual zero) and passes against this branch. It checks both outputs against
independent F64 accumulation for rows 1/2/8/17, widths 63/64/65, a nonzero input
byte offset, and ordinary/planned frame execution. All 24 combinations pass;
the filtered suite reports 40 passed, zero skipped. Test setup explicitly keeps
F32 slots dense so automatic Q8 staging cannot bypass the affected program.

## Generic CUDA readback

A 48 KiB output previously missed the 64 KiB bulk-pinning threshold. The generic
synchronous readback path now admits bulk pinning from 32 KiB and retains only
the next power-of-two size class, bounded at the existing 4 MiB maximum. A 48 KiB
output newly uses 64 KiB of pinned staging (the old path used pageable memory
without a pinned allocation). An existing 192 KiB bulk download now retains
256 KiB instead of an unconditional 4 MiB, a 93.75% reduction. No model name or
shape-specific dispatch is introduced.

Growth synchronizes prior stream users and frees the old allocation before
allocating the next class, bounding peak staging. A failed free retains ownership;
allocation denial or a missing host-free entry point takes the existing pageable
fallback. Bounds and transfer-audit admission precede staging and driver access.
Empty downloads return immediately. Existing scalar pinning and graph-capture
guards remain in place.

Four targeted CUDA regressions pass physically on Glowworm's GB10 (43 filtered
suite tests passed, zero skipped): size-class boundaries/overflow, preflight audit
and bounds rejection, byte/guard accuracy across growth/reuse/over-limit fallback,
and failed-free/allocation-denial ownership recovery. Native GPU programs and
CUDA images are unchanged by this host-side batch. It earns no kernel source
reduction credit.

## Qualification and retained controls

Raw patches, binary/source hashes, logs, independent-reference outputs and timing
receipts live in `.benchmark-assets/1166/bm-campaign`. No numerical tolerance is
relaxed. The initial 16-arm, four-case shared-GPU AB matrix is bit-identical and
passes all median/p95 gates. Its identical-binary AA control fails the batch-two
p95 gate (1.13233); that invalidates an unconditional speedup claim. The GPU had
another checkout's test context during part of the run. Both successes and
failures are retained, including the initial non-dense regression-test setup.

The final rebuilt executable is qualified separately with predeclared eight-arm
balanced AA/AB/BA matrices, 25 warmups and 301 measured iterations per process.
These remain shared-host comparisons; physical L4/A100 testing is deferred at
the user's direction. The original BL unpinned main-versus-branch tail failure
is retained and is not silently replaced by a comparison against the previous
branch head.

Local verification also passes canonical license checks, engine-error table
freshness, and enabled Metal/CUDA cache identity invalidation (217.6 seconds).
The cache test uses a worktree-owned global cache. A first sandbox invocation
without that override was denied before compilation; it is not a product failure.

Final exact-binary results pass all 24 timing gates (eight AA, eight AB, eight
reversed BA), with bit-identical outputs in every process. Ratios below are
new/old in both orders, using the unchanged 1.05 median / 1.10 p95 limits:

| CUDA case | AB median / p95 | BA median / p95 |
|---|---:|---:|
| MiniLM batch 1, sequence 16 | 0.99734 / 0.96133 | 0.99631 / 1.00872 |
| MiniLM batch 1, sequence 128 | 0.99895 / 1.00017 | 1.00020 / 1.00007 |
| MiniLM batch 2, sequence 16 | 0.38954 / 0.37370 | 0.39294 / 0.38701 |
| Q4_K reranker | 1.00046 / 1.00020 | 0.99901 / 0.99881 |

The affected 48 KiB output is 60.7–61.0% faster in median time and 61.3–62.6%
faster at p95 in this GB10 comparison against the previous branch head. Other
cases have a maximum median ratio of 1.00046 and maximum p95 ratio of 1.00872.
New pinned staging has a first-use allocation cost: batch-two first-warmup ratios
are 1.017 and 1.111, reported separately from warmed steady-state acceptance.
This is an execution benchmark claim, not server/tokenizer or large-model
throughput qualification.

The generic runner's reversed invocation writes old/new ratios and exits 1 when
the old binary is much slower. Raw receipts and exit status are retained;
`summarize.py` inverts BA ratios to new/old and applies the same limits. That is
order normalization, not a relaxed gate. Final AA passes all cases (maximum
median ratio 1.00483, p95 1.06890). The GPU process snapshots are empty at the
start of each final matrix, but host/GPU exclusivity is not enforced or claimed.
All transferred binary hashes match local frozen executables. The first prototype
and its final rebuild have distinct hashes and separately retained measurements;
final acceptance uses only the final rebuilt executable.
