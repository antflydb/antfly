# V campaign outcomes

This records V01–V40 for issue 1166, following completed U commit
4c460dfc15e458c68c177b28095190f83e0fda87. Merge
4eedf4729182e0f1b0a4da5517aec180f94d217f includes origin/main
4eedf9add77fc54cdba27ec32e88deb9ebb1e547. Source accounting uses that clean merge
as its baseline, before applying the V working changes, so upstream and earlier
campaigns receive no new reduction credit. Performance references remain the
previously qualified corrected U executables.

Implementation commit: `9345b6b743d34c6575faa0a1cd34531c9fa9c1bb`.

The final 6,020-input source fingerprint is
`5799054f027289787095211a4535fe60b9fc4ba335cce4f6b328f53efd09bdcf`. The Mac executable SHA256 is
`5294545dbf15e8d3b69729427612e13650045f7991d060311ed4fe670648e867`; CUDA is
`4d61c50c99da9ba62c5ed5da9d3b41f2dd3e5ae753e3af57ea68758e7466b183`.

Final qualification passes all 19 supported model cases, 11 required controls,
31 additional CPU widths, 1,554 paired Metal and 558 CUDA direct cases, seven
120-second soaks, independent decoder references and five portable compile
configurations. The Mac affected suite has 489 passes and 18 unchanged expected
skips; the CUDA affected suite has 146 passes, four unchanged baseline skips and six additional disabled-Metal tests selected by the broader consuming-operation filter.
The dedicated GLiNER2 suites execute all supported cases across the two hosts;
GLiNER2.5, lifetime, training/replay and allocation-failure coverage also pass.
The 12 shared flash-attention tests and all 23 qualification-tool tests pass. x86 runtime/performance is not claimed.

Raw source identities, failed prototypes, individual receipts, fixture/tool
identities and the disposition register remain under
`.benchmark-assets/1166/v-campaign/`. The combined acceptance record is
`final-evidence.json`. CPU control receipts carry forward only after both
final source and executable identities match. CUDA's final source reruns the
build, model, soak and portable gates after the attention storage change.
The earlier missing-route-helper interruption and its guarded recovery remain
archived with the previous run. No timing thresholds or numerical tolerances
were relaxed. One unchanged tiny training-flash kernel initially failed the
Metal paired timing gate at 1.057. Identical-program controls passed at 1.008
and 1.010, and a targeted reference/candidate comparison passed at 1.009. One
complete 64-case attention confirmation then passed with the original protocol
(maximum paired ratio 1.010). The initial failure and all diagnostic and
confirmation receipts are retained; this is evidence of a non-reproducing timing
failure, rather than a kernel performance fix.

The portable PTX 9.2 and TinyLlama CUDA profiles retain
their previously documented device/driver limitations.

## Shared owners

Metal Q4 default/short row variants now share source while preserving their
different unroll directives. Q8 ordinary/RMS scalar pairs share a static input
policy; SG-v2 and M64 share a matrix body with explicit tile, staging-stride,
guard and publication policies. Q4K ordinary/RHS projections share the codec dot
leaf. Packed-pair folds and matching SIMD publication also have shared owners.
Legacy float gather/bias-gather share traversal, broadcast and generic reduction
share affine indexing, and binary shaders share arithmetic while preserving their
different row-bias policies. Five attention consumers use the renderer-owned
online-row helper; their masks, barriers and publication schedules remain local.

CUDA matching accumulator initialization, reduction trees and projection epilogues
have native-scope phase owners. Eight additional projection consumers reuse Q8
acquisition. Q4K/Q8 fast projections share a static codec schedule. The three new
headers participate in the JIT dependency identity. All 445 exports retain their
compiled instructions and resource use on SM75/80/89/90/100/110/120.

CPU allocating add/multiply share allocation and shape orchestration. Allocating,
assigned and six consuming binary paths share static arithmetic; consuming paths
share donation orchestration while preserving SIMD/tail operand order. The
existing binary descriptor enum remains authoritative. No additional registry,
format vocabulary or unused descriptor layer is introduced.

CPU causal and bidirectional flash attention now share one checked, 64-byte
aligned score/statistics workspace. Their tile arithmetic and lane extents stay
unchanged; the shared owner replaces three temporary allocations and cleanups.

## Correctness findings

Consuming arithmetic, less-than, LayerNorm and RMSNorm now propagate cold-read
allocation failures before output writes. Forward normalization and residual
normalization prepare their fallible reads before allocating output. Regression
tests cover integer-backed reads, real strided-view materialization, both donation
directions, unchanged inputs after failure, retry, reference counts and independent
numerical references. Binary arithmetic covers SIMD boundaries, tails, cyclic
inputs and empty inputs across six donation routes.

LayerNorm backward validates `rows * dim` with checked multiplication. An isolated
baseline Debug witness reproduced the overflow trap. The fixed public API rejects
overflow and malformed geometry before any allocation.

The first Q8 matrix-sharing prototype incorrectly reused the SG-v2 activation
staging stride for M64. Its failed source and tests are preserved. An explicit
8-versus-4 stride policy repairs that prototype; this is not credited as an
upstream bug fix. Qualification tooling also caught the initially missing CUDA
header identities before final validation.

## Measured retentions

CPU LayerNorm backward retains the existing shared Into arithmetic and the
packed allocation schedule for larger rows. Checked-once, forced-inline and
explicit alignment prototypes failed unchanged latency gates and were rejected.
After the main merge, the original short-row allocation schedule also failed
width 13's p95 gate (77 versus 69 ns), with numerical and p50 checks passing.
Those failed arms and prototype executables remain available.

Short rows now use two bounded stack planes for parameter gradients and write
`dx` directly into the packed output through the same Into arithmetic. This
removes three temporary allocations/frees and one copy for every short row,
without duplicating the math. The prototype passes all five original controls,
three warm controls and 31 short-width controls with p50 <= 1.05 and p95 <= 1.10.
An exhaustive allocation-failure test covers partial output ownership. Final
composed qualification includes that additional test.

The first composed model sweep also reproduced a native MiniLM sequence-128
p50 regression: 1.0616 in the full matrix and 1.0603 in a confirmatory run, while
reference/reference and candidate/candidate controls stayed within 0.1%.
Sampling pointed to flash attention. The aligned shared workspace restores the
original gate (prototype p50 1.0012, p95 1.0012), preserves the seven protected
CPU instruction streams and passes the existing flash reference, cancellation
and allocation-failure tests. All failed arms and diagnostic samples remain.

Metal AdamW arithmetic, LayerNorm parameter folds, CPU projection workers,
descriptor workspace planning and several layout Into paths were already shared.
They receive explicit already-shared dispositions rather than new consolidation
credit. Codec-specific accumulator layouts, ARM DOT/SDOT schedules, scatter
atomics, attention masks/publication, and small measured-negative launch tuples
retain their specialized native schedules.

## Source accounting

The net maintained production reduction is **242 lines / 19,948 bytes**, after
charging all replacement helpers and build metadata. Tests, generated artifacts,
documentation, earlier campaigns and native image sizes are separate.

| Maintained source | Net lines removed | Net bytes removed |
| --- | ---: | ---: |
| Metal shader | -5 | 9,412 |
| Metal host binding | -1 | -1 |
| Generator owners | -12 | -599 |
| CUDA device | 163 | 7,849 |
| CPU and result ownership | 114 | 4,322 |
| CPU linalg workspace owner | -17 | -1,087 |
| Shared contracts and build identity | 0 | 52 |
| **Total** | **242** | **19,948** |

Expanded Metal source decreases from 926,411 to 919,692 bytes; all 525 kernel
entry names remain. This expanded-source difference is not added to maintained
source savings. Native image and executable sizes are reported independently;
source consolidation does not imply smaller compiled kernels.

## Compiled artifacts

Sizes below are separate from maintained-source reduction. CUDA instruction and
resource identity is verified independently of container and PTX text size.

| Artifact | Reference bytes | Candidate bytes |
| --- | ---: | ---: |
| Mac model benchmark | 18,904,520 | 18,904,760 |
| CUDA model benchmark | 132,523,760 | 132,529,896 |
| CUDA fatbin | 56,323,216 | 56,323,360 |
| CUDA ptx | 11,506,113 | 11,516,069 |

## Package dispositions

| ID | Disposition | Outcome |
| --- | --- | --- |
| V01 | audit | Metal projection inventory separates authored bodies from compiler-owned expansions. |
| V02 | implemented | Q4 packed pair fold and SIMD publication are shared; default/short row bodies share exact source with distinct unroll policy. |
| V03 | implemented | Q8 scalar pair projections share ordinary/RMS input policy; Q4K single and RHS projections share one codec dot leaf. |
| V04 | implemented | Q8 SG-v2/M64 share a static matrix body with explicit activation staging stride, guarded weight staging, tile width, and publication stride. |
| V05 | audit | CUDA inventory and seven-target native comparison use the completed U baseline, without crediting prior templates. |
| V06 | implemented | Eight more projection consumers reuse existing Q8 scale/packed-word acquisition; matching accumulator setup is shared. |
| V07 | implemented | Q4K/Q8 fast projections share a static codec schedule; exact shared trees and mode epilogues have one phase owner. |
| V08 | implemented | Both old fast projection bodies and repeated phases are retired; all 445 exports retain native instructions/resources on all seven targets. |
| V09 | already shared | Metal in-place/Into AdamW already consumes termite_training_adamw_impl; no duplicate math body remains. |
| V10 | retained | Accumulation contracts differ: grad*scale versus ordered previous+grad/divisor followed by renormalization. Preserve their arithmetic order and flags. |
| V11 | already shared | Direct/chunked LayerNorm parameter gradients already share termite_layer_norm_parameter_fold. Keep dx reductions and saved statistics in their own schedules. |
| V12 | already shared | AdamW destination binding is already separate from shared arithmetic through PublishFlag and source/destination pointers; no extra adapter is required. |
| V13 | implemented | Metal ordinary and bias-fused legacy gather consume one static traversal and preserve truncation, negative normalization, invalid-row zero and guarded bias reads. |
| V14 | retained | Metal scatter uses ordered matching tiles and typed/rounded index policies; CUDA uses duplicate-index atomics. Keep distinct physical schedules and float rounding. |
| V15 | implemented | Metal broadcast and generic axis reduction consume one affine offset loop; transpose keeps its validated permutation mapping. |
| V16 | already shared | Logical reshape, slice, gather, broadcast and transpose admission already use kernel_geometry/slice_plan owners. No second geometry vocabulary added. |
| V17 | audit | Assigned consumers retain target-specific representation/lease admission and side-effect-free query behavior; a common storage adapter would erase necessary policies. |
| V18 | implemented | CPU allocating, consuming and assigned binary paths consume denseArithmetic; six donation adapters use a static operation/destination owner. |
| V19 | already shared | CPU unary and existing Metal/CUDA layout/reduction Into bodies are already consumed by ordinary/assigned paths. Distinct reductions stay local. |
| V20 | implemented | CPU allocating add/multiply share geometry, allocation cleanup, integer admission and dense arithmetic through allocatingArithmetic. |
| V21 | already shared | K16 acquisition and Q4K/Q4Q5 MR/panel phase owners were consolidated in U; their function bodies remain byte-identical. |
| V22 | retained | Distinct codec layouts, MR accumulator planes, ARM DOT/SDOT schedules and unroll policies retain their native leaves; no new unavailable-x86 performance claim. |
| V23 | implemented | CPU LayerNorm backward retains one Into arithmetic owner. Short norm rows use two bounded stack planes and write dx directly into packed output. CPU causal/bidirectional attention share one aligned score/stat owner, removing two temporary allocations/frees. Checked multiplication rejects geometry overflow; earlier inlining/alignment prototypes are rejected. |
| V24 | already shared | Pair/triple projection workers already have static arity and typed contexts; their callbacks, partitioning and cancellation/drain bodies remain unchanged. |
| V25 | implemented | Metal online attention row math has one renderer-owned helper consumed by generated/compatibility and native dense/Q16 routes. |
| V26 | retained | Attention masks, diagonal layouts, changed-state atomics and matrix PV publication remain in their matching schedules. No full flash/paged/scalar unification. |
| V27 | already shared | Existing descriptor workspace requirements and paged launch wire owners remain authoritative; actual scalar addresses and scratch extents are preserved. |
| V28 | implemented | Replaced online row arithmetic is retired at actual consumers; compatibility source and standalone output are regenerated from their owners. |
| V29 | audit | Existing CUDA signature and fieldsFor owners retain checked stack argument tables. Add all three new phase headers to the JIT dependency identity; qualification tooling caught the initial omission. |
| V30 | already shared | Existing kernel catalog, format recipes and shared descriptors own identities. No new registry, second format enum or unconsumed descriptor added. |
| V31 | already shared | Metal runtime output finishing/allocation helpers and CUDA owned launch adapters already centralize compatible setup; different native APIs retain their bindings. |
| V32 | implemented | Retired only replaced bodies/phases. Public kernel entry points, routes and specialized schedules remain available. |
| V33 | validation | Freeze 4c460dfc15 as new baseline, origin/main 4eedf9add7, source inputs and both prior qualified executables. |
| V34 | implemented | Propagate consuming arithmetic and normalization materialization OOM before writes; reject LayerNorm geometry overflow before allocation. Preserve and repair the M64 prototype staging-stride failure. |
| V35 | validation | Independent arithmetic references cover six donation routes across SIMD/tail/cyclic/empty cases, retained owners and integer source OOM/retry; exhaustive short-norm output allocation failures and training/replay/lifetime suites cover ownership. |
| V36 | validation | Audit seven CUDA architectures, protected CPU source/instructions and Metal pipeline resources; preserve exact Q4 unroll bodies. |
| V37 | validation | Run complete paired Metal and CUDA direct matrices with fixed fixtures, guards, numerical and resource gates. |
| V38 | validation | Run composed 19-model, 11-control, 31-width, independent decoder and seven-soak matrix. |
| V39 | validation | Account net maintained source after charging helpers, separating generated output, tests and native image/binary sizes. |
| V40 | validation | Finish freshness/product/portable checks, record final outcomes and commit locally without pushing. |
