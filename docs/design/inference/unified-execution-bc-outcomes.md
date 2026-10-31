# BC: source-preserving segmented-attention phases

Baseline: `87a1ab1f13`. This wave audits larger native bodies, then consolidates
seven phases across the wide, blocked and grouped 16-key Metal attention
programs. Five native entrypoints consume the shared phases. CUDA and CPU
programs are unchanged.

## Authored ownership

`metal_kernels.m` now owns range envelopes, coarse window/key staging, unsigned
position-distance visibility, 8-key QK multiplication, 8-key probability/value
multiplication, two-weight softmax and blocked output publication once each.
The `ANTFLY_MSL_SEGMENT_*` fragments accept only the native storage, acquisition
or publication seams that differ between their consumers.

The entrypoints retain all shared-array extents, register arrays, SIMD-group
geometry, explicit scales, GQA indexing, barriers and reduction associations.
The 8-key and 16-key softmax/MMA schedules remain distinct. There is no runtime
interpreter or second generator, and no broad cross-product of kernel traits.
New owners describe operations rather than models.

The source ledger includes every replacement fragment. These three native
bodies originally occupied 22,521 bytes; together with all replacement owners
they now occupy 18,544 bytes: **3,977 fewer authored shader bytes, or 17.7% of
this family**. The whole production change removes 3,977 bytes / 28 lines.
Qualification tooling grows separately. Expanded shader text and compiled
program size receive no reduction credit. This is not a 17.7% reduction of the
whole kernel tree.

## Qualification and retained prototype

The final entire main shader is byte-identical to the baseline. This is checked
against an independently compiled factory from the committed baseline native
file, with its original source macros intact, rather than relying on an older
snapshot or candidate macro definitions. All includes used for that compilation
are unchanged by this wave. Identical compiler source and options preserve the
native program and resource inputs; no new performance speedup is claimed.

The Apple M4 Max passes **118 frozen-original bitwise and independent F64
reference cases** under fast and safe math. Cases cover all five entrypoints,
64/128/256/512 head dimensions where admitted, grouped ratios, explicit scale,
128/256/512-thread blocked launches, single queries, query/key tails, three
separate key ranges, empty rows, signed positions, local windows and nonzero
output canaries. The wide entrypoint retains its 128-thread launch. Independent
reference tolerance is `3e-4 * (1 + abs(reference))`, declared before execution.
All ten baseline/candidate pipeline comparisons (five entrypoints, two math
modes) retain the same static threadgroup memory, SIMD width and maximum
threads per threadgroup. The source-accounting regression suite passes all
nine tests.

An earlier native-template prototype removed 4,188 bytes and passed 108
correctness cases. Its fixed 31-pair, 32-dispatch AA/AB/BA assay passed 323/324
gates (median <=1.05, p95 <=1.10). Safe-math grouped-blocks256, case 10, BA missed
the median gate: **1.05559137**, p95 **1.03886276**. That version is discarded.
The final source-phase implementation preserves the original shader bytes;
the prototype's timings are not presented as final-candidate measurements and
were not resampled until passing.

Evidence is retained under `.benchmark-assets/1166/bc-campaign/`: baseline and
prototype source, source hashes, family/whole-production ledgers, final parity
and reference results, raw prototype timings, and qualification binaries.
The shipping fixture can freeze a source with `--dump-source PATH`, compare a
frozen original normally, or assert full shader identity with
`PATH --require-source-identity`. Use `PATH --bench` only when actually measuring
a changed numerical program.

## Audit findings

The expanded paired Q4/Q8 projection bodies initially appeared 96% alike, but
the authored code already shares their 32x64 staging and traversal owner. They
earn no new reduction credit. Dense Q16 causal variants likewise already share
accumulator setup; their remaining one-head/two-head acquisition and scratch
contracts need a separate qualified phase migration. The narrower Q4 32x32
matrix schedule uses different participating lanes and storage geometry, so it
is retained. Similarity in emitted code alone is not an authored-duplication
estimate.
