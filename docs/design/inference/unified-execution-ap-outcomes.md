# AP: shared CPU axis rotation and vision phase reuse

Baseline: `3b067076e7`.

## Implementation and source accounting

CPU M-RoPE and vision RoPE now use one private, forced-inline axis-rotation
owner with compile-time axis and single-head policies. Each public entry keeps
its existing shape, position and section validation. Vision restarts its
frequency exponent per spatial axis; M-RoPE retains its interleaved temporal/
height/width section rules and separate frequency-scale multiplication.

Vision phases are evaluated once per token/pair and reused across heads,
matching the existing M-RoPE schedule. Each head still reads and publishes its
two components in the original order, supporting exact in-place operation.
The single-head specialization removes the otherwise unnecessary dynamic head
loop. There is no phase cache, additional allocation or public API change.

Including helper comments and specialization, production source **grows by
28 bytes / seven lines**. This is a performance and shared-ownership change,
not a source-budget reduction. The independent original-order inline oracle
adds **1,907 test bytes / 32 lines**, accounted separately. The qualification
driver contains extracted immutable baseline/candidate functions; production
continues consuming the implementation in `native_compute.zig`.

## Numerical and performance acceptance

- The direct driver checks exact output bits and exact in-place aliasing for
  both variants across 48 profiles: tokens 1/16, heads 1/4/16/32, dimensions
  8/64/128. The repository oracle also covers vision dimension four and
  independent per-head arithmetic; existing M-RoPE tests cover section policies,
  partial rotation tails and architecture position contracts.
- The final fixed protocol uses 21 paired samples in each AA/AB/BA order for
  every profile, with one million target pair operations per interval and a
  262,144-call cap. All **144 profile/order checks pass**, requiring candidate/
  baseline median <= 1.05 and p95 <= 1.10, including the AA controls.
- AA maximum median/p95 ratios: **1.026739 / 1.024395**.
- M-RoPE maximum AB/BA median ratio: **1.014661**. LLVM folds the extracted
  before/after M-RoPE implementations into one function in the final benchmark.
- Vision AB/BA median ratios: **0.160498–1.018114**, up to **6.23x faster** in
  this direct CPU matrix. This is not an end-to-end model speedup claim.
- Final repository rotary tests: **16 passed / eight skipped**, with GPU
  execution disabled. Both-backend compilation and generated-source freshness
  (**70 files + 129 native owned regions**) pass against the accepted source.
- All final source, fixture, binary, raw-sample and check-log hashes are pinned
  in `pins-final.json`; `verified.json` validates all 3,024 paired samples.

## Retained findings

The first shared owner moved the position load across `pow`, adding a section-
bound spill/reload in M-RoPE. A small profile failed the median limit at
1.052840; that source, disassembly and complete failed matrix are retained.
Restoring the original uint position-load order makes LLVM fold M-RoPE's
before/after functions together. Its second short-interval matrix nevertheless
fails two M-RoPE ratios and a vision baseline-versus-itself control; it remains
rejected rather than supplying selected passing samples.

The final source additionally specializes single-head vision execution.
Its new protocol lengthens every timing interval uniformly, addressing the
short-interval AA control failure. Limits, workload coverage, sample counts and
orders are unchanged; both earlier failed matrices remain preserved. The
accepted result is the complete third matrix, not a retry of selected failures.

Evidence: `.benchmark-assets/1166/ap-campaign/`, including `prototype-v1/`,
`prototype-v2/`, the final source snapshot and retained benchmark binary,
`protocol-v3.json`, `performance-v3.json`, raw samples and source accounting.
