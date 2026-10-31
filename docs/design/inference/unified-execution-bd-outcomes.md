# BD: dense decoder and vision attention phases

Baseline: `4a42220ad8`. This wave consolidates thirteen authored phases across
four native bodies serving seven Metal entrypoints. The full emitted main
shader remains byte-identical to the committed baseline. CPU/CUDA kernels,
artifacts and backend dispatch policies are unchanged.

## Shared programs and numerical contracts

Dense Q16 inference shares QK multiplication, causal/window score admission,
probability/value multiplication, diagonal rescaling, output normalization and
K/V staging. Single-query-head and paired-query-head schedules retain their
own launch geometry, scratch layout, head indexing and publication. Direct
half-device loads and staged tails retain their original barriers and casts.
The existing accumulator/admission owner was already shared and earns no new
reduction credit.

Head-64 vision inference shares K/V staging, QK multiplication, online softmax,
probability/value multiplication, denominator-diagonal publication, tail
scatter and accumulator scaling. Q16 and Q32 retain their distinct row
ownership and register arrays. The Q32 full variant keeps float Q/K/V and
probability tiles and reuses Q storage for tail output; the narrower variants
keep their half staging and separate tail storage.

All new `ANTFLY_MSL_DENSE_*` and `ANTFLY_MSL_VISION64_*` owners identify the
operation and physical contract. Their source arguments specify only the
native addresses, typed publication or schedule that differ. They do not add
runtime dispatch or a general-purpose generator.

## Qualification and performance parity

Before editing, the committed factory was compiled and its source frozen.
The final entire main shader is byte-identical: 959,323 bytes, SHA-256
`75afc3a0c2b1b859709c5af59705098caaf3b3e7a14ec4420513aada66d88dfa`.
The native baseline file is also checked against `git show 4a42220ad8`.
No native schedule or compiler input is replaced by an unqualified template.
With unchanged compiler options, the source-phase change preserves the
numerical/performance program. It claims no kernel or end-to-end speedup;
unchanged inputs do not establish a new isolated tail-latency measurement.

On the Apple M4 Max, **216 original-bitwise cases and 216 independent F64
references pass** in fast and safe math. Dense cases cover head dimensions
32/64/96/128; query lengths 1/15/16/17/33/65; key lengths
1/31/32/33/64/257; grouped heads; position offsets; initially empty causal
rows; local windows; full direct-half tiles and staged tails. The fixed-HD128
and paired-head entrypoints are both exercised.

Vision cases cover all three half/full entrypoints, batches one/two, heads
one/two/four, and sequence lengths 1/15/16/17/31/32/33/65/129/257. Full and
partial output tiles exercise the separate tail storage and float Q-storage
reuse paths. Every output has nonzero canaries.

The F64 oracle rounds scaled Q and acquired K/V for the half-stage contracts;
it evaluates the masked softmax independently. Its predeclared half bound is
`2e-3 * (1 + abs(reference))`, accounting for probability-tile rounding; full
F32 uses `3e-4`. The worst observed absolute reference difference is
`9.06139612e-5`. Original/candidate output parity is bitwise for every case.

All **14 pipeline comparisons** retain static shared memory, SIMD width and
maximum threads per threadgroup (seven entrypoints, two math modes). All nine
source-accounting regression tests pass. All fourteen compiler-owned generated
regions in the native file are unchanged; no generated text or image deletion
is credited.

The shipping `check_metal_dense_tiles.m` fixture supports `--dump-source PATH`,
a required frozen source path for parity/reference checks,
`PATH --require-source-identity`, and the existing fixed AA/AB/BA assay through
`PATH --bench`. Evidence, frozen native/shader sources, binaries, hashes and
source receipts are retained under `.benchmark-assets/1166/bd-campaign/`.

## Source accounting

The four bodies plus all new phase owners go from 26,339 to 22,424 authored
bytes: **3,915 fewer production bytes (14.9% of these owners)**. This is the
whole production byte saving for the wave; tests/docs are charged separately.
Physical source lines increase by 34 because the named phases are laid out
separately. Expanded shader text and image size do not shrink, and this is not
a 14.9% reduction of the whole kernel tree.

Together BC and BD remove **7,892 authored shader bytes** while preserving the
same full main-shader bytes. Remaining similar kernels must be audited against
their existing owners and actual numerical/storage contracts before estimating
further savings.
