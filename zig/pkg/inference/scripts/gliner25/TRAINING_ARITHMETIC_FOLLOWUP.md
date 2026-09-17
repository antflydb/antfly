# CUDA training arithmetic follow-up

Training qualification remains closed. This follow-up isolates arithmetic
differences identified after the [loss corrections](LOSS_PARITY_FOLLOWUP.md).
Existing optimizer defaults and acceptance tolerances are unchanged.

## Findings and implementation

The opt-in diagnostic worker compares loss inputs and replays individual
Linear/LayerNorm modules using the same native inputs in pinned Python CUDA.
It reuses the existing worker supervisor, source fixtures, snapshot comparator
and configuration. Tracing is excluded from timed steps. Module tracing is
bounded to 128 MiB and requires retained activations; repeated module calls
without unambiguous alignment are skipped. Comparisons include inactive
storage, so padding differences alone are not functional failures.

The original trace-enabled small/full run reproduced both untraced tensor
comparison reports exactly. The loss-input comparison found larger errors
upstream of the loss derivatives. Changing the native cuBLASLt library from
CUDA 13 to Python's CUDA 12 library did not change those results.

Two arithmetic paths have been addressed:

- Small training dots previously fell below the serving BLAS dispatch
  threshold. Resident primitive dots now use a stream-bound standard FP32
  cuBLAS SGEMM path, including strided batches and both right-hand contraction
  orientations. Handle reuse always supplies the current tensor buffers.
  The trainer explicitly enables this profile before uploading weights;
  serving and generic primitive dispatch retain their existing policy. An
  unavailable cuBLAS library or a `cuda-libs=off` build rejects this training
  profile at startup instead of silently choosing different arithmetic.
- CUDA training LayerNorm now preserves fused forward and backward operations
  with Welford statistics and deterministic reductions matching the pinned
  Python implementation. Shared instruction admission checks input/output
  shapes, epsilon, scratch and work limits. CPU/Metal retain their existing
  graph behavior. The upstream reduction license is included with the kernel
  and in the repository's third-party notices.

The v6 CUDA trainer fingerprint is `cuda_f32_loss_seeds_v6_sgemm_norm`; checkpoints
from earlier arithmetic profiles intentionally fail compatibility checks.
The new LayerNorm symbol is required at CUDA trainer startup.

## Evidence and limits

All hardware checks here use the NVIDIA L4 and PyTorch 2.9.1+cu128. They do not
establish identical arithmetic across other GPU architectures or library
versions.

| Check | Result |
| --- | --- |
| Production SM89 LayerNorm artifact: 80 shape/offset cases | Outputs, input/parameter gradients, means and reciprocal standard deviations match raw FP32 bits |
| NVIDIA Compute Sanitizer memcheck, same LayerNorm probe | Zero reported errors |
| Standard SGEMM: 14 dense and 16 batched cases, CUDA 12/13 libraries | All match Python raw FP32 bits |
| Intermediate v5 full-model run, small/B2/epsilon `1e-6` | First two microbatches and first update pass existing complete state checks; same-input LayerNorm replay is exact |
| Initial v6 SGEMM + normalization full-model run, same configuration | First two microbatches and first update pass complete state checks; all replayed linear modules exactly match Python's separate matrix-product-then-bias expression |
| Initial v6, small heads/B2/default epsilon `1e-8` | Two first-update weight tensors fail, maximum absolute difference `7.34255e-6` |
| Initial v6, small full/B2/epsilon `1e-6`, 100 updates | First loss failure at update 37; 280 final weight tensors fail, maximum absolute difference `0.00665884` |
| Shared LayerNorm admission tests | Two passed, including module import |
| Python CUDA contract suite | 33 passed |

The intermediate v5 run uses cuBLASLt plus fused normalization; it is not
evidence for the subsequent SGEMM integration. Its first-step pair-logit
maximum difference is about `1.05e-5`, so the complete forward pass is still
not bitwise equal. Local exactness does not establish sustained weight parity,
default-epsilon parity, extraction quality or throughput parity. The updated
native GPU regression gate remains incomplete; the sustained v6 campaign fails.

The initial v6 trace reduces the first-step pair-logit maximum difference to
`9.54e-6`. Python's fused linear-with-bias operation still differs from the
native separate bias addition, even though the separate Python expression
matches exactly. This is a remaining arithmetic difference to investigate.
The v6 trace binary predates the subsequent explicit BLAS startup opt-in;
the startup policy and native regressions require their own rebuilt checks.

The v6 sustained run repeats the previous 100-update configuration: one
initial update, two measured pairs, and 97 additional untimed updates. It uses
diagnostic mode so all failed checks and final state are retained. CPU
compilation overlapped this run, so its two timing pairs are excluded from
performance conclusions regardless of the parity result. Both worker trees
were cleaned up successfully. The largest final weight discrepancy remains in
`relation_scorer.content_linear.weight`.

A separate bias-initialized SGEMM probe matches Python in 12 of 14 cases but
fails both library versions at shape `[26,384] @ [192,384]^T`. It is not an
accepted fix. The pinned reference selects a cuBLASLt bias epilogue when its
linear dispatch conditions allow it; reproducing that path is the next
investigation. See the pinned upstream
[dispatch](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/native/cuda/Blas.cpp)
and [BLAS wrapper](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/cuda/CUDABlas.cpp).

The resulting candidate dispatch (cuBLASLt bias epilogue with 1 MiB workspace,
SGEMM with bias-initialized output for one-column/one-contracting-element
shapes) matches Python raw FP32 bits in 36 isolated cases across both installed
library versions. The probe includes small non-aligned dimensions, larger
encoder widths and actual head dimensions. The candidate is now integrated
into the training graph as v7; this isolated probe is not a sustained-parity result.

## Native regression follow-up

The rebuilt v6 startup-profile suite selected 55 checks: 52 passed, none skipped
or leaked, and three newly added low-level tests failed. Both published small
heads/full checkpoint tests passed, with exact resumed optimizer state and
exported model identity. Synthetic materialized/replay and layer-recomputation
resume checks also passed, including allocation-failure handling.

Two new dot tests invoked the low-level primitive API and then used the strict
resident download API. Resident ownership is attached by the instruction
adapter, which those direct tests bypass. Their readbacks now explicitly copy
the primitive result and synchronize. Production ownership checks are unchanged.

The new high-offset normalization test incorrectly required closeness to FP64
mathematical results beyond what the pinned Python FP32 implementation achieves.
Replaying its exact 65-by-4 input at offset 10000 gives bitwise-equal native and
Python outputs, gradients and statistics, while both differ from FP64 by up to
about `2.42e-4`. The corrected test compares hashes of every output and packed
gradient bit against pinned Python FP32 instead of widening a tolerance. These
three corrected tests subsequently passed the actual v7 kernel rerun. The first narrow rebuild
reported 33 passes but discovered only module-import tests; it is not validation
of these corrections. The root CUDA test-discovery block now explicitly imports
the resident adapter and instruction descriptor modules. Subsequent focused runs
must use runtime filters and verify that every intended test name appears.

## v7 fused-bias integration (validation in progress)

The candidate bias dispatch is now integrated behind the CUDA trainer's explicit
fusion profile. The shared strict autodiff rule retains the fused FP32 forward
operation and expresses its three gradients through existing matmul, transpose,
reshape and reduction primitives. CPU/Metal's default lowering remains unchanged.
Matrix and flattened-input VJP shape/rejection tests pass in the ML unit suite.
The v7 CUDA build and trainer build succeeded. Six explicitly selected GPU and
admission tests passed, with zero skips: small and batched dots, fused linear
forward/backward and cached bias refresh, degenerate biased matrices, pinned
FP32 normalization, and fused linear admission/workspace limits. The archived
log records the actual test names; this run resolves the discovery gap above.
Published-model resume tests still require a v7 rerun.

The CUDA forward adapter reuses the existing cuBLASLt plan cache for biased
non-degenerate matrices. A distinct cache key and serialized descriptor update
ensure each invocation uses its current bias buffer. Matrix/bias alignment,
disjoint output/workspace storage and bounded cache capacity remain checked.
The instruction planner reserves the whole 1 MiB workspace before execution.
One-column or one-contracting-element matrices use existing bias broadcast and
SGEMM with beta one, with zero declared workspace. The trainer requires both
BLAS libraries at startup and fingerprints this arithmetic as
`cuda_f32_loss_seeds_v7_fused_bias`, rejecting older-profile checkpoints.

The v7 full-model trace passes both initial microbatch state comparisons and
the first optimizer update at epsilon `1e-6`. All 22 unambiguously replayed
linear modules and 28 normalization modules exactly match pinned Python on
identical inputs. Repeated modules remain explicitly skipped. The complete
forward pass still differs: first-step pair logits differ by up to `1.53e-5`.

The default-epsilon (`1e-8`) small heads run fails three first-update weight
checks, with a maximum difference of `9.96143e-6`. The v7 small/full/B2 campaign
finishes all 100 updates but fails sustained parity: the first loss failure is
qualification update 32 (total update 35), and 282 final weight tensors fail.
The largest weight difference is `0.00668228`, again in
`relation_scorer.content_linear.weight`. Both worker trees were cleaned up.
The tiny held-out fixture has equal task F1 for both implementations, including
a relation F1 regression from 1 to 0 in both. This does not establish production
quality. The two timing pairs are diagnostic only and do not qualify throughput.

An isolated follow-up identifies another arithmetic mismatch in the existing
materialized-attention path: the primitive softmax sums rows sequentially.
A warp-reduction candidate following pinned Python's reduction and backward
arithmetic matches all 114 cases bitwise for both forward and backward; the
existing production forward kernel differs in 76 cases. Cases cover widths
1 through 1024, single and multiple rows, random data and finite masked rows.
This isolated probe predates the v8 integration below and does not resolve the
sustained-parity blocker by itself.

## v8 softmax integration (sustained parity still fails)

The CUDA training fusion profile now selects the candidate for FP32 softmax
rows up to 1024 elements. A shared `fused_softmax_backward` operation takes
the separately rounded cotangent-times-probability and the probabilities,
then computes the row reduction and final subtraction in the pinned order.
Autodiff selection is explicit; existing CPU/Metal differentiation defaults
and longer-row CUDA paths retain their previous behavior. The resident planner
checks both gradient input shapes, work limits and the supported row extent;
the warp kernel needs no additional workspace. The new symbol is required at
trainer startup, and checkpoint arithmetic is fingerprinted as
`cuda_f32_loss_seeds_v8_softmax`.

The focused ML autodiff check passes, including the default path, explicit
fusion, gradient shape and malformed dimension rejection. All three production
artifacts have been regenerated. The production SM89 artifact matches all 114
forward/backward probe cases bitwise and passes NVIDIA memcheck with zero errors.
Its SHA-256 is `5155039dd4ac5cfcbce100da65822a70736da86cb961feea15a2b7bfbb63cb76`.
The three artifact-script contract tests pass. The integrated trainer and CUDA
test builds succeed. Nine explicitly selected CUDA tests pass with zero skips,
including complete softmax output/gradient hashes, shape admission, linear and
normalization regressions, synthetic materialized/replay/recomputation resume,
allocation failures, and published small heads/full durable resume with exact
state/model identity. The initial build is identified by `v8-softmax-sources.json`.
A subsequent validation-only adjustment limits the new autodiff shape checks
to explicit fused-backward selection, preserving generic softmax behavior;
its focused ML check passes. The GPU reports describe the preceding build.

The initial v8 full-model trace passes both microbatch state checks and the
first update at epsilon `1e-6`. First-step pair-logit maximum error is `1.33514e-5`.
Default-epsilon small heads training still fails two first-update weight checks:
`boundary_head.shared_pool_scorer.query_projection.weight` (`9.96143e-6`) and
`relation_scorer.mlp.3.weight` (`5.15580e-6`).

The 100-update campaign completes but fails: the first loss failure is at total
update 35, and 257 final weight tensors exceed tolerance. The largest error is
`0.00661878` in `relation_scorer.content_linear.weight`. Both worker trees clean
up successfully. On the tiny held-out fixture, final relation F1 differs
(native 0.5, Python 0); both regress from their initial score of 1. Other task
F1 scores match. Neither strict trajectory parity nor production quality is
established, and the two timing pairs remain diagnostic.

## Remaining arithmetic evidence

An exact-GELU probe over 110,001 values finds 9,131 forward and 8,479 backward
bit differences with the current CUDA 13.2 production artifact, each bounded
by `4.76837e-7`. A separate CUDA 13.2 kernel using the pinned reference expression
reproduces the same discrepancy. Compiling that expression with the installed
NVRTC 12.8 library instead gives bitwise-equal forward and backward results in
every case. The compiler library hash and source/commands are archived. This
demonstrates a CUDA math-version effect for GELU, not a proof that all remaining
training drift has that cause. The reference expression is in pinned PyTorch's
[GELU implementation](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/native/cuda/ActivationGeluKernel.cu).
A follow-up compiled as portable compute-75 PTX also matches all 110,001
forward/backward values bitwise when loaded on the L4. Its SHA-256 is
`2de3cfb7cf372428403a9cbdbc0f0280f9301a9736bf83a31668a750b10ff756`.
This verifies the local driver JIT case, not other GPU architectures. The
compatibility candidate has not been integrated into the production trainer.

The materialized DeBERTa graph also scales `(C2C + (C2P + P2C))` after summing,
whereas the installed Python reference scales K before the content contraction
and scales the two relative terms before their addition. The graph is
mathematically equivalent, but this is another concrete FP32 ordering difference.
The v9 integration below preserves existing backend defaults while matching
this source order in the CUDA training profile and rechecks the first attention
layer and sustained state parity.

Raw probe results, scripts and the intermediate full-model trace are in the
[evidence directory](evidence/2026-09-15-training-arithmetic/README.md).


## v9 attention arithmetic integration (sustained parity still fails)

The CUDA materialized-training profile now scales K before the content
contraction and scales C2P/P2C before adding them. It retains transposed RHS
storage through the existing batched-dot operator instead of copying K into
a transposed contiguous buffer. Other architecture callers keep their existing
arithmetic defaults. The arithmetic profile is explicit in the graph plan and
checkpoint identity (`cuda_f32_loss_seeds_v9_attention_order`). Replay attention
rejects this profile; it continues to use its existing versioned operation.

An isolated Python probe compares full relative projection followed by gather
with the existing gathered-relative optimization. Retaining RHS storage is
bitwise exact in all five tested head/sequence/dimension configurations. At
sequence seven, copying the transposed matrices changes up to 236 content
values (maximum `2.38419e-7`) and 215 relative values (maximum `4.76837e-6`).
These are local arithmetic results, not end-to-end training qualification.

The first finite-difference check exposed an unsupported layout in the existing
batched-dot VJP. The corrected rule chooses contractions based on both input
storage layouts, reuses the shared graph builder, and preserves the original
standard-layout construction. All 32 graph gradient tests pass, including a
new independent FP64 reference for strict seeded gradients across all four
supported storage layouts. Resident admission now checks transposed RHS shape
and work limits; the Metal adapter forwards the existing runtime transpose
flag rather than silently treating the new layout as standard storage.
Both final CUDA builds succeed. Eight explicitly selected CUDA regressions
pass with zero skips: resident batched-dot forward and strict gradients,
softmax, dot admission, encoder arithmetic/resource bounds, synthetic
materialized/replay/recomputation resume and allocation failures, and published
small heads/full durable resume with exact state/model identity. The Metal
adapter forwarding change has not been run on Metal hardware here. The v9
trainer SHA-256 is
`0ce9069d6686a01b8ad83f5b25eb290c5d906235b8d333bbe92f5eb3ffcc1e44`.
The full-model trace passes both initial microbatch state comparisons and
the first optimizer update at epsilon `1e-6`. The first attention output
LayerNorm input/output and first intermediate dense input/output now match
Python exactly over all traced storage. Differences first reappear later in
the feed-forward block: first output LayerNorm input maximum `1.90735e-6`.
The final pair-logit maximum error is `1.52588e-5`, versus `1.33514e-5` for v8;
the local attention fix does not by itself improve this end-to-end maximum.
The default-epsilon (`1e-8`) heads run now has one first-update weight
failure, down from two in v8: `relation_scorer.mlp.3.weight`, maximum
`5.15580e-6` versus allowed `4.96550e-6`. Initial state and first microbatch
checks pass. The 100-update full-model campaign completes but still fails sustained parity.
The first loss failure remains qualification update 32 (total update 35), and
243 final weight tensors fail tolerance. The maximum difference is
`0.00664854` in `relation_scorer.content_linear.weight`; v8 was `0.00661878`.
Both worker trees clean up successfully. On the tiny held-out fixture, native
relation F1 is 0.5 and Python is 0, with other task F1 scores equal to 1. This
is not a production-quality evaluation. Two diagnostic timing pairs do not
qualify performance, and the report explicitly leaves performance unqualified.

A separate content-contraction backward probe shows that copying the LHS
transpose in the K gradient still differs at sequence seven: 1,258 values,
maximum `8.94070e-8`. Keeping a transpose view matches Python bitwise. All
four larger tested cases (sequence 59/118/256, head dimension 64/128) match
with either layout. The v9 VJP is mathematically validated but still copies
this LHS transpose. This short-sequence rounding difference remains open;
it is not hidden by the finite-difference tolerance.

The remaining default-epsilon first-update miss is explained numerically by
Adam's sensitivity near zero. At weight index eight, stored first moments
imply clipped gradients of `1.374107e-9` (native) and `1.242281e-9` (Python).
Reconstructing their first Adam updates in FP64 with the unchanged optimizer
configuration predicts a weight gap of `5.154715e-6`; the measured gap is
`5.155802e-6`, leaving only `1.087124e-9` residual. This calculation assumes the
verified equal initial weights, first update, and common decoupled weight decay.
It accounts for this weight miss, not the origin of its gradient difference,
and does not establish that the long-run drift is harmless.


The next bounded arithmetic change is to integrate and test the demonstrated
CUDA 12.8 GELU compatibility artifact in the explicit CUDA training profile,
with reproducible artifact generation and checkpoint identity. Preserve the
main CUDA 13.2 build contract and avoid a Python/NVRTC runtime dependency.
The short-sequence copied-LHS backward mismatch also needs a retained-storage
contraction path. Reuse the existing graph/backend operators and fixtures,
then repeat strict first-update and sustained state qualification before
making throughput or production-readiness claims.


## v10 CUDA 12.8 GELU integration (strict heads passes; sustained full parity fails)

The explicit resident CUDA training backend now loads a small embedded PTX
module generated with NVRTC 12.8.93 before weight upload. Only the selected
training backend uses it; generic CUDA primitives retain their previous path.
The module uses the existing driver, stream, tensor ownership and resource
admission, with no Python or NVRTC runtime dependency. The main CUDA 13.2
artifact bundle remains unchanged. Checkpoint identity is versioned as
`cuda_f32_loss_seeds_v10_gelu_cuda128` and incorporates the PTX SHA-256.

The separate generator verifies NVRTC 12.8.93 and explicit math options,
produces a source/compiler/PTX manifest, and supports reproducible `--check`.
All seven old/new artifact script contract tests pass; the new manifest tests
are included in the existing CI suite. Actual regeneration reproduces the
checked-in bytes. Production PTX SHA-256 is
`4d6dd38023a6db073313f5fe9e8987e1a8867f2ea4e524674f639d7bb7fcc1f4`.
The production artifact matches all 110,001 forward/backward probe values
bitwise on L4 and passes NVIDIA memcheck with zero errors. The integrated
graph test checks full forward and non-unit seeded-gradient hashes over 1,025
values, including tails and a partial block. Both integration builds succeed.
Ten explicitly selected CUDA regressions pass with zero skips, including the
new GELU hash and bounds checks, generic GELU compatibility, matrix/bias/softmax
regressions, synthetic resume/recomputation/allocation failures, and published
small heads/full exact durable resume. Trainer SHA-256 is
`f850c36d58fbe122b09528f5551349e42fdc23c67bea4fd49816c224f48024de`.
Both initial microbatch state comparisons and the first full-model update
pass at epsilon `1e-6`. All traced encoder-layer inputs/outputs now match
Python exactly, including the feed-forward blocks. The boundary encoder
output projection, layer norm and first attention-block pre-norm also match.
The first observed forward difference appears after that boundary attention
block (next pre-norm input maximum `4.76837e-7`). Pair-logit maximum error is
`5.72205e-6`, down from v9 `1.52588e-5`. Repeated modules remain explicitly
unmatched, and trace comparisons include inactive storage.

The strict default-epsilon (`1e-8`) heads run now passes every initial,
microbatch and first-update state gate with zero parity failures. Tolerances
are unchanged. This clears the prior first-update weight miss.

The full-model 100-update run completes but fails sustained parity: first loss
failure is again at total update 35, and 271 final weight tensors fail. Maximum
weight error is `0.00625285`, now in `relation_scorer.mlp.0.weight`. The first
failed term is pair loss at microbatches 69/70: native `10.0989571`, Python
`10.1134071`, error `0.0144501` versus allowed `0.0103134`. The intermediate
state check at total update 33 passes; total update 43 fails. Both workers
clean up successfully. On the tiny held-out fixture, both relation F1 scores
are 0 and other task F1 scores are 1; output parity still fails. Two diagnostic
timing pairs do not qualify performance.

A separate linear-backward probe extends the retained-LHS-storage finding:
copying `dy.T` changes FP32 weight gradients for rows 2, 7, 16 and 118 with
384 input/output dimensions, while retaining its view matches Python exactly
in all eleven tested shapes. For the seeded random row-118 case, the maximum
difference is `3.05176e-5`. Other tested shapes are exact with either layout.
This is a local diagnostic, not a measurement of actual training-gradient
error. The current linear VJP still copies that transpose; it is the next
shared graph/BLAS change after measuring the isolated GELU integration.


The boundary-head probe uses the actual packed QKV layout, boolean padding
and diagonal masks, FP32/no-TF32, and deterministic execution. CPU profiling
identifies `aten::_scaled_dot_product_efficient_attention` /
`aten::_efficient_attention_forward` in pinned Python. For B2/H4/D32 and
sequence 11/22/59, none of the materialized candidates (scale after dot,
split Q/K scaling, or Q-only scaling) is bitwise exact; maxima range from
`7.15256e-7` to `1.78814e-6`. This confirms that a scaling-only head change is
insufficient to reproduce the reference. The default Python backend must stay
unchanged for the comparison. The next implementation work is retained
matrix storage for backward contractions and a native fused boundary-head
attention path matching the actual reference kernel's arithmetic and gradients.


## Direct backward cuBLAS storage follow-up

A direct SGEMM probe now reproduces both copies in the current dense backward
path, rather than only the graph's copied LHS. It uses identical seeded inputs
for the installed CUDA 12 and CUDA 13 cuBLAS libraries across eleven shapes
each. The copied input-gradient path matches Python in 14/22 cases, with
maximum error `2.09808e-5`; the copied weight-gradient path matches in 13/22,
with maximum error `3.05176e-5`. Retaining original matrix storage and selecting
BLAS transpose flags matches both Python gradients bitwise in all 22 cases.
Retaining only one weight-gradient operand is insufficient (14/22 exact for
RHS only, 15/22 for LHS only). The probe is archived with its source hash.

This strengthens the case for a shared contraction path that retains both
operands and removes the redundant copies. It does not establish the size of
the corresponding errors in actual training, or resolve the separate fused
boundary-attention difference. No v11 implementation or throughput result is
implied by this isolated diagnostic.

## v11 dense storage integration (validation in progress)

The CUDA training dense-dot adapter now passes both contraction orientations
straight to the stream-bound SGEMM wrapper. It checks shapes and element counts
before dispatch and allocates only the output. The existing linear wrapper
reuses this implementation with its original flags, preserving fused-bias
behavior. Non-training dispatch retains its previous policy.

An explicit `retain_backward_storage` fused-linear attribute selects the weight
VJP through a shared dense-layout builder. Existing default differentiation
keeps its construction. The new nonstandard dense layouts have strict VJPs;
all four layouts pass finite differences and independent FP64 seeded-gradient
checks. All 34 ML gradient checks pass, along with the fused-linear matrix and
flattened-input shape/rejection check for both storage profiles. Resident
instruction admission describes both orientations and checks work limits.
Metal forwards its existing RHS flag and explicitly rejects the newly admitted
LHS layout instead of executing it incorrectly. No Metal hardware validation
is claimed.

The trainer identity is `cuda_f32_loss_seeds_v11_dense_storage`, and the CUDA
fusion-plan identity is bumped to v4. Previously issued arithmetic-profile
checkpoints are incompatible by design. The new native regression hashes every
input/weight gradient bit against pinned Python on a fixture where copied
storage changes both gradients. Trainer/GPU integration and sustained parity
results are pending. Batched backward LHS copies and fused boundary-head
attention remain separate outstanding work.

### v11 integrated results

Both builds succeeded. Twelve explicitly selected CUDA/admission tests pass
with zero skips: the new full-gradient hash, small/batched dots, fused-bias
regressions, GELU/softmax, both dot admission tests, synthetic full/heads and
materialized/replay/recomputation durable resume with allocation failures, and
published small heads resume with exact state/model identity. NVIDIA memcheck
of the actual new gradient regression reports zero errors. Published full-model
resume was not selected in this build; its previous v10 result must not be
attributed to v11. Trainer SHA-256 is
`1c735bb97faffd602e669afff7aa1617df6f3c3dbcd4692a451c525bc77382d9`.

The trace passes both initial state comparisons, including the first optimizer
update at epsilon `1e-6`. All 35 unambiguously traced encoder-layer modules have
exact inputs/outputs in both microbatches. First-step pair-logit maximum error
is unchanged at `5.72205e-6`. Strict default-epsilon heads training still passes
all initial/microbatch/first-update state gates. Worker cleanup succeeds.

Sustained full-model parity still fails. The first loss failure remains update
35 (microbatch 69), with pair loss native `10.1015186`, Python `10.1134071`:
error `0.0118885`, allowed `0.0103134`. The intermediate state check at total
update 33 passes, while total update 43 fails. At 100 updates, 252 weight tensors
fail (v10: 271); maximum weight error is `0.00661910` in
`relation_scorer.content_linear.weight` (v10 maximum: `0.00625285`). Thus the
local exactness fix does not materially resolve the accumulated drift. Both
workers clean up successfully. The tiny held-out fixture has relation F1 zero
for both and other task F1 one, with output parity still failing.

The first native microbatch launches 6,897 kernels versus v10's 7,328, with
identical reported transfer bytes and zero host fallbacks. Two uncontended
but diagnostic timing pairs give 15.28 examples/second native versus 8.84
Python (native/Python latency ratio 0.5785). They do not qualify performance:
strict parity fails and the sample count is insufficient. The next work remains
retained batched backward storage and the actual fused boundary-attention
forward/backward arithmetic; changing reference backends or relaxing gates
would not resolve these differences.

## v12 retained batched gradients

Dense and batched layout helpers now share one contraction builder. An explicit
`dot_general.retain_backward_storage` profile selects storage-preserving VJPs;
default differentiation keeps its construction. The CUDA batched SGEMM wrapper
accepts both transpose flags, validates physical element counts, and restores
the output dimensions. Shared resident admission validates both operand layouts
for ranks two and three. Metal continues to reject an unsupported LHS transpose
explicitly. Checkpoint arithmetic is
`cuda_f32_loss_seeds_v12_batched_storage_layout_restore`, with CUDA fusion-plan
identity v6.

All 34 ML gradient checks passed with both storage profiles across all four
layouts. The initial native regression exposed a real FP32 issue in the first
candidate: computing a transposed-LHS input gradient as the reversed matrix
product changed 244/278 values in the two affected fixture layouts. Both CUDA
12 and CUDA 13 cuBLAS reproduced the same result. The corrected VJP retains its
input operands, computes the reference product, and then restores the gradient
output layout with a transpose when necessary. This is not a tolerance change.
The corrected focused mathematical check passes. The initial GPU suite is
archived as 14 passes and one failure, including passing published heads/full
resume; it is not a passing integration gate.

Both final builds succeed. All 15 selected CUDA/admission checks now pass with
zero skips, including complete forward/gradient hashes for all four batched
layouts, dense/matrix/activation regressions, synthetic full/heads and
materialized/replay/recomputation resume with allocation failures, and published
small heads/full durable resume with exact state/model identity. This also
closes the missing published-full resume rerun noted for v11. Trainer SHA-256:
`5dda0bb8f2fea9d1c9ea2a9e06012489790296a518c44d1d92880fdd79632a3a`.
The initial full-model trace passes both state comparisons and first update at
epsilon `1e-6`; first pair-logit error remains `5.72205e-6`. Strict heads
first-update parity at default epsilon `1e-8` also passes. NVIDIA memcheck of
the integrated batched-gradient regression reports zero errors.

The completed 100-update full-model campaign still fails. The first loss
failure is total update 35 (microbatch 69): pair loss native `10.1014556885`,
Python `10.1134071350`, absolute error `0.0119514465` against allowed
`0.0103134071`. The state check at total update 33 passes; total update 43
fails. At the end, 252 weight tensors exceed tolerance, with maximum absolute
error `0.0066192069` in `relation_scorer.content_linear.weight`. Both workers
clean up successfully. The tiny held-out fixture still has relation F1 zero
for both implementations and other task F1 one; held-out parity fails.

The initial microbatch uses 6,813 kernel launches, 84 fewer than v11, with
unchanged transfer bytes and zero host fallbacks. Two uncontended diagnostic
timing pairs measure 15.65 examples/second native versus 9.02 Python, with a
native/Python latency ratio of 0.5764. This is not performance qualification:
sustained parity fails and only two timing pairs were collected. The full
campaign, failed initial candidate, corrected tests and source identities are
preserved in the hash-verified evidence directory.

### Standalone fused boundary-attention candidate

GPU profiling identifies the actual pinned Python kernels as
`fmha_cutlassF_f32_aligned_64x64_rf_sm80` and
`fmha_cutlassB_f32_aligned_64x64_k32_sm80`. The installed PyTorch headers select
`OpMultiplyAddFastF32` tensor-core arithmetic for this path. Ordinary materialized
SGEMM plus softmax cannot be assumed to reproduce that ordering, even with the
Python matmul TF32 flag disabled. The pinned
[forward implementation](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/native/transformers/cuda/mem_eff_attention/kernel_forward.h)
and its matching backward header provide the candidate's implementation.

A standalone CUDA 13.2 build reuses the installed PyTorch 2.9.1+cu128 kernel
headers and its pinned
[CUTLASS commit](https://github.com/NVIDIA/cutlass/tree/e51efbfe18fe4f4cbb66ab814c55bf4aa0185491).
The initial host-wrapper prototype matches forward output, saved log-sum-exp,
and Q/K/V gradients exactly at sequence lengths 11/22/59/65/118/129, B2/H4/D32,
with zero dropout and boolean padding/diagonal masks. A separate native FP32
multiply-and-ascending-warp-reduction kernel replaces the initial diagnostic
Torch delta reduction and retains exactness in every case.

The same kernels now work as driver-loaded CUBINs with flat pointer/scalar
arguments: no C++ host wrapper or Torch runtime is needed to execute them.
The build-only Philox header copy adds host/device constructor annotations so
constructing the upstream parameter object inside the device wrappers is legal.
The source/patch/compiler/dependency hashes are recorded. All six driver-loaded
cases are bitwise exact for forward, log-sum-exp, delta and complete packed
Q/K/V gradients. Forward dynamic shared memory is 36,352 bytes; backward is
53,504 bytes. Backward workspace for these B2/H4 cases is 131,200/262,400/393,600
bytes for one/two/three query blocks. NVIDIA memcheck of the complete six-case
driver probe reports zero errors. Prototype sources, dependency identities,
build logs and results are archived with the integration evidence.

This is an isolated component candidate, not an integrated trainer or production
artifact. It covers only L4/SM89, FP32 head dimension 32, zero dropout and the
listed layouts/masks. The integration must add bounded resident descriptors,
shared graph/strict VJP selection, reproducible artifact generation and licensing,
startup capability checks, checkpoint identity, and whole-training validation.
Existing benchmark tolerances and the Python backend remain unchanged.

## v13 fused boundary attention (validated integration, sustained parity open)

The graph now has a versioned boundary-attention operation with a strict VJP.
Its ordinary saved tensor packs attended output and padded log-sum-exp; backward
references that exact forward value. There is no activation cache outside the
tape. The immutable binary mask is nondifferentiable. Checked descriptors cover
physical shapes, signed device indexing, padded bias, aligned delta and query
accumulation workspace, and work limits. CUDA executes the pinned forward,
backward and delta kernels with GPU-built padding/window/diagonal bias.

An explicit boundary arithmetic profile selects this path for D32 attention
without probability dropout. Other dimensions and nonzero probability dropout
retain materialized execution. The compiled SM89 and SM80 images both match
Python forward and packed QKV-gradient bits across eight cases: batches 1/2/3,
heads 1/4, sequence lengths 1/11/22/59/64/65/118/129, global/local windows and
padding including an entirely invalid single-token mask. The regression hash
is `0da0df5697bf7070bbf20286f50a597f838546a7dad87dba8b513fa18633004e`.
The artifact probe deliberately poisons accumulation workspace before launch;
both images still pass, and NVIDIA memcheck reports zero errors. The strict
VJP structural test verifies saved-forward reuse and frozen-mask behavior.

The initial PTX portability probe failed with CUDA driver status 222 on this
host. The implementation therefore loads compiled SM80/SM89 images on compute
capability 8.x; other architectures retain the existing path. Shared-memory
admission is checked by the driver before training weights are uploaded.
All three published model variants use D32 boundary attention, but their usual
nonzero dropout remains outside this new fused arithmetic profile.

The artifact generator verifies a transitive dependency lock and uses pinned
CUDA 13.2.78, PyTorch and CUTLASS headers, with the build-only Philox annotation
patch. NVCC also incorporates the physical source path into private symbol names.
The generator now uses an owner-only, locked stable build directory and verifies
all transitive headers. Repeated and relocated-checkout regeneration matches.
Actual embedded artifact bytes are hashed once at trainer startup for checkpoint
identity, avoiding excessive compiler memory from comptime hashing.

Final trainer SHA256 is
`2970f1ee04f3b65c31548ebd685f4e90c115154a2f63f8ff430ec55abc36f30f`.
All 18 selected CUDA tests pass, including explicit fused-attention durable resume,
partial-window/fault recovery and recomputation. The integrated gradient test and
both compiled-image probes report zero NVIDIA memcheck errors. The ML/import
suite passes 89 checks; the added strict-VJP structural check also passes.
Strict heads validation at default Adam epsilon 1e-8 passes. The full trace has
bit-exact attention-block and refinement-normalization inputs/outputs on both
initial microbatches. Initial state, gradients and the first update pass.

The final 100-update full-model campaign with epsilon 1e-6 still fails sustained
parity. First loss failure is total update 37 (qualification update 34). The state
at total update 33 passes; update 43 fails. At completion, 272 weight tensors
exceed tolerance; maximum absolute weight error is 0.00666746 in
`relation_scorer.content_linear.weight`. Both workers clean up successfully.
Two uncontended diagnostic timing pairs give 15.110 native vs 9.064 Python
examples/second, paired latency ratio 0.5999. This is not performance qualification.
The final-build results are archived separately from the earlier v13 candidate.

### Remaining activation and prefix-sum arithmetic

A standalone CUDA 12.8 SiLU candidate follows the pinned PyTorch forward division
and direct backward formula. All 110,001 forward values and non-unit seeded
gradients match bitwise. The current reciprocal/multiply decomposition differs
in 24,701 forward and 67,976 backward values; its decomposed derivative also
produces 564 nonfinite results for large finite negative inputs. The candidate
is not yet integrated. The existing training-math module will host this kernel.

Prefix sums are another demonstrated ordering difference. The boundary graph
uses a doubling tree; pinned PyTorch uses a serial accumulator for outer-axis
scans, Sklansky scans for innermost dimensions, and CUB for a single vector.
The serial candidate exactly matches forward and reverse seeded gradients for
tested multi-channel content layouts; it does not match the single-vector CUB
case. Inside-prefix tensors use the innermost algorithm. A blanket replacement
with a serial scan would therefore be incorrect. The isolated order probe and
SiLU probe are preserved with the final v13 evidence. No tolerances were relaxed.

## v14 SiLU integration (validated component, sustained parity open)

CUDA training now explicitly retains FP32 SiLU nodes and differentiates them
with a direct seeded VJP. Ordinary graphs retain their decomposition. The new
kernel shares the existing CUDA 12.8 activation module, launch validation and
artifact generator with GELU; it needs no Torch runtime. The fusion identity is
v7 and the trainer arithmetic identity is v14, binding the regenerated PTX hash.
The strict VJP/default-lowering checks pass, and the production artifact matches
all 110,001 reference values and gradients exactly. Whole-trainer results remain
pending; this does not yet close the sustained-parity release gate.

A separate scan prototype matches both outer-axis serial and innermost Sklansky
forward/reverse-gradient arithmetic in ten cases, including partial blocks and
widths above 1,024. Single-vector CUB dispatch is explicitly excluded. This code
is isolated and is not part of v14.

The first integrated run passes 18 of 19 selected checks, including exact GELU
and SiLU seeded bits, attention, dense gradients, explicit fused resume/recompute
and published heads resume. Published full-model resume is not yet passed:
concurrent trainer compilation reduced available host RAM, so live-memory
admission returned `ResourceTemporarilyUnavailable` before the intentional
restore-hash rejection assertion. The test will be repeated after compilation
without changing its admission policy. The initial failed log is retained.

Final v14 trainer SHA256:
`ce6b8b3f71b392ce8f3f9b9737b72b5d5f7bf59774a1b97b12466247099cdd25`.
The uncontended published full-model resume rerun passes with exact resumed
model and optimizer state. Thus all 19 selected checks have passing results
(the initial combined suite was 18/19, followed by the one-test successful rerun).
Explicit child-process NVIDIA memcheck of the SiLU strict-VJP regression reports
zero errors. The selected ML/import suite passes 31 checks; four artifact
contract tests and byte-identical regeneration also pass.

The complete full-model trace passes both initial microbatch state comparisons
and cleanup. The boundary shared-pool builder and scorer start/end projection
inputs and outputs are now bit-exact, closing the first post-refinement mismatch.
Content-pooling normalization input still differs by at most 2.86102e-6, and
pair logits by 5.72205e-6. Trace comparisons include inactive storage; large relation-input differences
require active-row alignment before attributing them to active relation-output
errors. Strict default-epsilon heads validation passes.
The unchanged 100-update full-model campaign is running; no sustained parity or
performance qualification is claimed for v14 yet.

The final v14 100-update run is diagnostic_complete, with parity and performance
qualification both false. First loss failure is qualification update 32 / total
update 35 / microbatch 69: pair loss 10.0995168686 native vs 10.1134071350 Python,
error 0.0138902664 vs allowance 0.0103134071. State parity passes at total update
33 and fails at 43. At completion 280 weight tensors exceed tolerance, maximum
absolute error 0.00672614 in `relation_scorer.content_linear.weight`. Both workers
clean up. The tiny held-out fixture has relation F1 zero in both arms and other
task F1 one; it does not establish trained quality equivalence.

The first microbatch uses 6,739 kernel launches (13 fewer than v13), unchanged
transfer bytes, and zero host fallbacks. Two uncontended diagnostic timing pairs
measure 14.940 native vs 8.934 Python examples/second; paired native/Python latency
ratio 0.5977. The SiLU correction closes a component arithmetic mismatch but does
not improve the first sustained-loss failure relative to v13. Remaining unmatched
arithmetic must be corrected before making a training-performance claim.
All final source/binary hashes, test logs, traces and campaign outputs are archived.

## v15 prefix-sum integration (validated component, sustained parity open)

All three pinned Python scan paths now have driver-only FP32 kernels in the
existing CUDA 12.8 training-math module: serial outer-axis scans, shared-memory
Sklansky innermost scans, and deterministic single-vector scans. The latter
specializes PyTorch's CUB block ordering with 512 threads and 16 values/thread,
including block aggregate reduction, multiple tiles per CTA, and device-SM-based
partitioning. It uses no CUB headers or runtime library. Thirty isolated cases
match forward and seeded backward exactly; production-artifact memory checking
reports zero errors. Single-vector coverage reaches 8192*(SM count+1)+7 elements.
The pinned deterministic single-vector path starts with a zero prefix, including
for one-element vectors. The VJP preserves the seed for width one, as Python does.

The shared versioned graph operation records the original reference axis layout
and reverses scan direction in its VJP without retaining activation values.
Content and relation pooling select the outer profile; inside-prefix scoring
selects the innermost profile explicitly. Zero padding and downstream pooling
remain shared. CPU/Metal defaults retain their previous tree graph. Admission
checks exact physical shape, nonzero dimensions, bounded device indexing, scratch
and work limits. The plan hashes its prefix profile; the v15 trainer identity
binds embedded artifact bytes and SM count, since single-vector ordering depends
on it. CUB licensing is recorded with the source and repository notices.

The selected ML/import suite passes 32 checks, and four artifact contract tests
plus regeneration pass. The first integration build found a helper-name shadow,
a test fixture shape-width mismatch and a comptime hashing quota limit. These
were corrected; artifact hashing now runs once at startup as with the attention
module. The corrected GPU test build is in progress. No whole-training parity or
performance claim is made yet.

An independent marginal-score probe rejects a proposed operand-order change:
PyTorch's `einsum("bld,bqd->bql", positions, queries)` matches the graph's
`queries @ positions.transpose` forward and seeded gradients exactly in four
layouts. Computing `positions @ queries.transpose` then transposing the result
introduces backward differences and, in larger cases, forward differences.
The existing graph order is retained; this probe does not explain the remaining
marginal-logit discrepancy.

The first integrated scan fixture failed only on the sign of a scalar zero.
The earlier width-one exploratory probe had omitted deterministic-algorithm mode;
ordinary CUB copies a one-element vector, while the pinned deterministic scan
adds its zero prefix. The exact regression correctly caught that distinction.
The inappropriate scalar copy fast path has been removed; the seeded VJP's
width-one identity remains correct. The failing integrated and direct-replay
results are preserved rather than replacing their expected fixture hash.

Final trainer SHA256:
`b53b69b1c6609aa8a55a79ad4a1c4d81315cb18efc20930c0aa22457a62f3f94`.
All 21 selected CUDA checks pass with zero skips, including the unchanged
16-case exact scan fixture, explicit fused-path resume/recomputation, and
published heads/full durable resume and portable reload. The integrated scan
VJP test and final production-artifact probe report zero NVIDIA memcheck errors.
Both initial full-model microbatch state checks and default-epsilon heads parity
pass. Content-pooling normalization input and output now match Python exactly
in both traced microbatches. FiLM input still differs by at most 9.53674e-6,
and pair logits by 7.62939e-6; marginal-logit discrepancies remain.

The completed 100-update campaign still fails sustained parity. First loss
failure is total update 35 / microbatch 69: pair loss 10.1017513275 native vs
10.1134071350 Python, error 0.0116558075 vs allowed 0.0103134071. State parity
passes at update 33 and fails at 43. At completion 272 weight tensors exceed
tolerance, maximum absolute error 0.00665256 in
`relation_scorer.content_linear.weight`. Both workers clean up successfully.
The tiny held-out fixture still fails the quality gate; it cannot establish
trained quality equivalence.

The first microbatch uses 6,649 kernel launches (90 fewer than v14), H2D
13,741,484 bytes, D2H 669,952 bytes, and zero host fallbacks. Two uncontended
diagnostic timing pairs measure 15.829 native vs 9.062 Python examples/second,
paired native/Python latency ratio 0.57243. Performance remains unqualified
because sustained parity fails and only two timing pairs were collected.

### Next demonstrated differences: reductions and score association

The current generic CUDA reduction is serial FP32 accumulation. Pinned PyTorch
uses its configured reduction tree, vectorization and per-thread accumulators.
An isolated probe finds differences in endpoint compatibility (336/384 values),
boundary bias gradients (94/128), encoder bias gradients (645/768), and a
FiLM broadcast-gradient reduction (1610/1792), plus a scalar sum. These are
arithmetic diagnoses, not integrated replacement kernels. The pinned Reduce.cuh
hash and cases are recorded. Both forward and AD-generated reductions need the
reference profile; changing only forward graph nodes would miss bias gradients.

Two shared-pool scoring expressions also associate their three additions
differently from Python: proposal compatibility plus start/end logits, and final
scores plus start/end logits. The isolated addition-order probe differs in
3157/10000 values. No reduction or score-association changes are integrated yet.
The next implementation must preserve existing CPU/Metal arithmetic identities,
bind CUDA arithmetic changes to checkpoints, cover row/column/broadcast layouts,
and admit any large-reduction scratch explicitly.


## v16 CUDA reduction and sequential score integration (validation in progress)

The isolated reduction prototype matches pinned PyTorch CUDA exactly in 148
sum/mean cases. The production artifact passes 150 cases, adding a large odd
mean whose total count exceeds exact FP32 integer representation. Mean scaling
uses FP32(output count) / FP32(input count), as the reference does, rather than
assuming it is always identical to FP32(1 / reduction width). NVIDIA memcheck
reports zero errors across the production cases. The 32 selected shared ML graph
checks and byte-identical artifact regeneration also pass.

The new pure host reduction planner handles dense row-major tensors, coalesced
size-one dimensions, input/output vectorization, four per-thread accumulators,
warp/block reduction order, and device-dependent CTA partitioning. Global sums
use an explicitly admitted staging allocation and a second kernel; the fixed CTA
index order is preserved without persistent workspace or semaphore state.
Resident instruction limits carry an explicit optional CUDA reduction device
profile. The same checked planner supplies both program memory/work admission
and dispatch, covering reductions generated by strict autodiff. The default
profile retains generic backend arithmetic. CUDA checkpoints bind maximum
threads per SM as well as SM count and the embedded PTX hash.

Sequential score addition has a separate graph profile, preserving CPU/Metal's
existing grouped expressions and plan identities. CUDA training selects the
reference order for proposal compatibility plus start/end logits, and for final
scores plus start/end logits. The trainer arithmetic identity advances to v16.
An integrated regression will check sum, mean and autodiff-generated broadcast
reductions against the pinned Python fixture; trainer/test binaries are currently
building. These isolated results do not yet establish sustained training parity.

Primary arithmetic references are the installed pinned `Reduce.cuh` and
`SharedReduceOps.h`, plus PyTorch's
[sum dispatch](https://github.com/pytorch/pytorch/blob/5811a8d7da873dd699ff6687092c225caffcf1bb/aten/src/ATen/native/cuda/ReduceSumProdKernel.cu)
and [mean factor](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/native/cuda/ReduceMomentKernel.cu).


Final v16 trainer SHA256:
`4b6261ea695c99a2e9f375b91c557e7672b24eafc3e60ddcb6a99276ec24e49f`.
Test binary SHA256:
`e81bcb2ed64370580a3d708ff49458bc4ef973b2fbf8e75f68114b10dec9a6ca`.
Both builds pass. All 24 selected CUDA checks pass with zero skips, including the
15-layout sum/mean/generated-broadcast-VJP fixture, exact workspace/work admission,
and published heads/full durable resume and portable reload. The integrated
reduction test has zero NVIDIA memcheck errors. Eight isolated shared-memory and
multi-block sum/mean cases have zero racecheck hazards. The attempted separate
generic-reduction regression was excluded from this test binary and matched no
tests; it is explicitly pending inclusion in the next build, not counted as a pass.

Default-epsilon heads parity passes. Both initial full-model microbatch state
checks pass. Traced content-pooling normalization stays exact. FiLM input error
falls from 9.53674e-6 to 3.81470e-6, pair-logit error from 7.62939e-6 to
5.72205e-6, and proposal-logit error from 2.62260e-6 to 1.19209e-6. Marginal
start/end errors remain 9.53674e-7 and inside errors 1.90735e-6.

The 100-update campaign remains diagnostic-only. First failure is qualification
update 32 / total update 35 / microbatch 69: pair loss 10.1008758545 native vs
10.1134071350 Python, absolute error 0.0125312805 vs allowed 0.0103134071.
State parity passes at update 33 and fails at 43. Final weight failures decrease
from 272 to 258 tensors; maximum absolute weight error is 0.00660725054 in
`relation_scorer.content_linear.weight`. On the tiny held-out set relation F1 is
0.5 native vs 0.0 Python, both below the initial 1.0; the quality gate still fails.
Both workers clean up successfully. Two uncontended diagnostic timing pairs give
15.2767 native vs 9.22848 Python examples/second, paired latency ratio 0.60419
(about 1.66x faster). First microbatch: 6,650 launches, H2D 13,741,692 bytes,
D2H 669,952 bytes, zero host fallback calls. Sustained parity and performance
qualification remain open.

### Next demonstrated mismatch: forward transpose storage

The earlier v15 probe compared logical operand order. A new probe isolates a
different issue: `queries @ positions.transpose(...)` versus the same transpose
materialized with `.contiguous()`. The retained view matches Python's marginal
einsum exactly in forward and both seeded gradients across five layouts. The
materialized form differs in every forward and query gradient. For [B=2,N=11,
Q=7,D=128], 117 forward outputs differ (maximum 3.81470e-6) and 1,115 query
gradient elements differ. Native forward graphs still materialize these
transposes even though AD-created gradient dots already retain operand storage.
The next CUDA-only fusion change should absorb direct matrix transpose inputs
into dot contracting axes before differentiation, preserving logical operand
order and binding the new arithmetic to checkpoint identity.


## v17 forward transpose storage (component validation complete)

A new opt-in training helper in the shared fusion module absorbs direct rank-2
or rank-3 matrix transposes on either dot operand, remapping its contracting axis
while retaining the original buffer. Batch-moving permutations remain explicit.
The helper validates shapes/topology before matching, handles nested transposes,
retains strict VJP storage, allocates no nodes, and is idempotent. The default
inference fusion pipeline and CPU/Metal training profiles do not invoke it.
CUDA training selects this rewrite before differentiation; fusion identity is
v8 and trainer arithmetic is v17. Existing matrix kernels and VJP rules are reused.

All 34 selected ML/import checks pass, including both rewrite regressions. A
pinned Python fixture covers forward and both non-unit seeded gradients for 20
head-dot layouts, including D128 marginal and candidate scoring shapes. The
integrated forward/seeded-gradient fixture now matches the pinned hash exactly,
with zero NVIDIA memcheck errors. The initial GPU suite passed 25 of 26 selected
tests, including the generic-reduction regression omitted from v16 and published
heads/full resume/reload checks. Its only failure was an overly broad structural
assertion in the new fixture: retained VJP deliberately transposes the final left
gradient when the forward left operand is transposed. After correcting that
assertion, the focused fixture passes without changing its numerical golden.
The corrected test build produced a complete executable but failed installation
when the disk filled; after removing completed compiler intermediates, that exact
executable was installed and tested. The trainer rebuilt successfully and its
hash is unchanged from the trace binary.

The Python diagnostic trace now compares retained module inputs even when their
outputs are unavailable. Output replay remains conditional on an actual native
output record; no output is inferred or invented. This exposes boundary query
projections, candidate normalization and prior projection without extra native
activation captures. All 18 Python contract checks pass, including input-only
hook registration/comparison/cleanup. This instrumentation remains outside timed
benchmark steps.

Both initial traced microbatches pass state parity. Starts, ends, inside logits,
proposals, counts and nulls now match Python exactly; maximum pair-logit error is
1.9073486328125e-6. Candidate-normalization input error is 4.76837158203125e-7;
FiLM input error remains 3.814697265625e-6, while same-input linear replay is exact.
These are initial-state diagnostics. No v17 sustained 100-update campaign has
run; the last sustained result remains the failed v16 campaign above.

The next confirmed arithmetic mismatch is host span-length feature generation.
Across lengths 1..4096, actual Zig ReleaseFast host values differ from pinned
Python CUDA in 130 log1p results and 1,264 reciprocal-square-root results.
A CUDA 12.8 prototype matches all three Python features bit-for-bit over 65,537
lengths, including clamped lengths/counts. This prototype is not integrated or
production-qualified. A separate cuBLAS probe ruled out the suspected degenerate
projection layout mismatch. Host accumulation of detached inside means is also
under investigation; no mismatch has yet been demonstrated for that path.

## v18 detached candidate features (initial forward parity exact; sustained gate fails)

The CUDA shared-candidate graph now generates its three span-length features
with a versioned frozen-metadata operation in the existing CUDA 12.8 module.
Lengths are [B*C,1], per-sample counts [B,1], and the checked output [B*C,3].
Shape/work admission and device-buffer validation precede dispatch; the kernel
needs no scratch or host readback. Strict differentiation explicitly detaches
both metadata inputs and rejects malformed geometry. CPU/Metal keep their host
feature path. Unreachable host feature descriptors are omitted by the existing
required-input transfer logic, avoiding a second upload implementation.

A diagnostic using actual v17 inside logits also identifies three of fourteen
serial host means differing from Python CUDA by up to 4.76837158203125e-7.
The CUDA shared-pool graph now computes masked inside sums with its existing
reference reduction profile, divides by clamped per-sample counts and explicitly
stops gradients through that mean. Prefix centering and interval restoration use
this same value. Candidate arithmetic is bound into plan identity and trainer
identity is v18; the training math artifact is profile v5 and its actual bytes
remain part of checkpoint compatibility.

All 35 selected ML/import checks pass, including detached metadata gradients and
invalid geometry. Four artifact contract checks pass and regeneration matches.
The actual production span-feature PTX matches pinned Python over 589,833 feature
values (three sample counts, lengths -1..65535, clamping and a partial block),
with zero memory-check errors. Its forward digest is
`9b5055a00ce7fce0061fcfe1f1e51046dc0ffff674799c66128706d6ebb1a9bb`.
All 28 selected CUDA checks pass with zero skips, including the integrated
feature golden, exact work admission and published-small heads/full durable
resume and portable reload. The integrated feature/admission fixture also has
zero memory-check errors. The successful trainer hash is
`3eb938ee622905cda411fd7e6feae2b3b0c526bd904af1a5ecbd38cea9b2c9ab`;
the test binary is
`ce8c005e9ccdc4822418e826c0d03f07ae09d51ace1256d40df324969d74e020`.

Both initial traced full-model microbatches now have zero error in every compared
forward output: starts, ends, inside, nulls, counts, proposals and pairs. The
length-projection input, candidate-normalization input and FiLM inputs/outputs
also match exactly. Both initial state checks pass. Separate heads and full-model
validation at the unchanged default Adam epsilon 1e-8 passes all initial,
gradient and first-updated-state comparisons. These remain short validations.

The matched 100-update full-model campaign at epsilon 1e-6 still fails. First
failure is total update 35 / qualification update 32 / microbatch 69: native pair
loss 10.10056400299 vs Python 10.11340713501, error 0.01284313202 vs allowed
0.01031340714. State checks pass through total update 33 and fail at 43. Final
weight checks fail for 256 tensors; maximum error is 0.00660372549 in
`relation_scorer.content_linear.weight`. Final tiny-held-out metrics agree, but
both arms' relation F1 falls from 1.0 to 0.0, so quality remains unqualified.
Both workers clean up successfully.

Two uncontended diagnostic timing pairs give 15.84864 native vs 8.90597 Python
examples/second, paired latency ratio 0.561815 (about 1.78x). This is not a
qualified performance result because sustained parity and quality fail. First
microbatch has 6,650 launches, H2D 13,736,820 bytes, D2H 669,952 bytes and zero
host fallback calls. No CPU compilation overlapped this campaign.

### Remaining demonstrated mismatch: loss-gradient seed arithmetic

Despite exact initial forward logits, boundary seed errors remain up to
1.67638e-8. Actual Zig ReleaseFast sigmoid/exp probes on the captured logits
differ from pinned Python CUDA; baseline x86-64 and native CPU targets give the
same probe bits. For example, sigmoid differs in 31/154 starts, 31/154 ends,
46/140 inside values and 204/2688 pair values. The probes isolate scalar math,
not the entire loss derivative. Scaling, reduction and accumulation order also
need examination: current host loss code can combine FP64 scale factors and
apply loss weights after computing an FP32 seed, while Python differentiates
individual FP32 tensor operations. The next change must reproduce the complete
backward expression, including listwise reductions and other task losses, rather
than assuming a sigmoid substitution alone closes sustained parity. No loss-seed
fix is integrated in v18 and tolerances remain unchanged.


## v19 gather-backward diagnosis and arithmetic profile

Two diagnostics on the v18 trainer narrow the remaining differences. Replacing
only Python's boundary-loss cotangents with captured native cotangents makes
22/24 shared-pool scorer parameter gradients exact; only the start projection
weight and bias remain different. This is an intervention, not a production
parity test: other task losses are unchanged and the surrogate backward may
alter accumulation order. The scorer gradient L2 difference drops from
2.60914e-6 to 9.05143e-7, but encoder gradient L2 barely changes.

The actual start-index pattern has up to 156 duplicate indices. Native's serial
grouped scatter differs from deterministic Python Tensor.gather backward in 160
feature elements (maximum 9.53674e-7 with padding masked). Python's CUDA
`Indexing.cu` stride-one reduction accumulates complete 32-element chunks by
lane, performs a shuffle tree, then accumulates the tail. Advanced row indexing
with feature width greater than one takes a different path; matching the
Tensor.gather path globally would incorrectly change relation-head arithmetic.

An explicit gather-backward profile now travels from the architecture builder
through strict autodiff into scatter dispatch and plan identity. The CUDA
trainer opts in for actual batched Tensor.gather operations. Relation advanced
indexing and query expansion keep their previous path. CPU/Metal defaults are
unchanged. The implementation reuses existing validated integer grouping,
zero initialization, module ownership and launch accounting. Admission charges
for the extra warp reduction work, with the same bound checked at dispatch.
Trainer arithmetic identity is v19 and the CUDA 12.8 training-math artifact is
profile v6.

The actual production PTX matches pinned Python on all 12 isolated layouts,
including captured start/end indices, >32 duplicates, scalar features and long
runs. NVIDIA memory checking reports zero errors. The initial fixture digest is superseded by the identical-input correction
described below.
The 36 selected ML/import checks and four artifact-contract checks pass;
regeneration matches. Integrated trainer results are recorded below when run.

Separately, advancing the unchanged v18 workers to update 35 confirms identical
valid candidate memberships (55 and 45 spans). Only two spans exchange slots in
the second sample. After aligning by span identity, pair-logit maximum errors
are 0.00284672 and 0.00251675. Large unaligned candidate-module differences must
not be interpreted as new arithmetic failures. The loss and sustained-state
failures remain real and the release is still unqualified.


### Integrated v19 diagnosis and sustained run

The v19 trainer is
`123cbc635499a6874ab3599fd3c2549fef687ebb28e962f6550eee56c001d3e5`.
Repeating the native-boundary-seed intervention now makes **all 24/24**
shared-pool scorer parameter gradients exact on the first microbatch. The two
start-projection residuals disappear, confirming the intended kernel fix.
The number of exact parameter gradients increases from 87 to 92; boundary
encoder and encoder gradients still differ, and this intervention does not
establish normal training parity.

The first integrated gather regression failed its digest with zero memory-check
errors. This was a fixture mismatch: constructing float32 inputs with Python
GPU scalar division multiplies by a rounded reciprocal, whereas the Zig fixture
uses float32 division. A standalone Zig ReleaseFast probe records the actual
input bits. Feeding those identical bits to Python yields exactly the observed
integrated digest:
`acfc4ae085b97d38a9f4d22667e7781352af3adb7551973df7001cfbe2dab3c2`.
The corrected regression uses this independently reproduced reference. The
original failed test and reference are retained in evidence, along with the
matched-input generator and input bits. No numerical tolerance changed.

The uncontended 100-update full-model rerun still fails, with first loss failure
at the same total update 35 / qualification update 32 / microbatch 69. Native
pair loss is 10.10155773163 versus Python 10.11340713501, error 0.01184940338
(allowed 0.01031340714). State checks pass through total update 33 and fail at
43. Final weight checks fail for 248 tensors (previously 256), with maximum
error 0.00662085344 in `relation_scorer.content_linear.weight`. These numbers do
not establish an overall reduction in sustained drift. Final held-out metrics
agree but both arms' relation F1 regresses from 1.0 to 0.0. Both workers clean up.

Two diagnostic timing pairs measure 15.48844 native and 8.16629 Python
examples/second, paired latency ratio 0.524978. No compilation overlapped this
campaign. Timing remains unqualified because sustained parity and quality fail.
Default optimizer settings and all acceptance tolerances remain unchanged.


A small focal-derivative formula probe removes the sigmoid difference by
substituting the exact CUDA probability bits into the host FP64 expression.
On captured start/end/pair logits, derivatives still differ from Python CUDA by
up to 5.96046e-8 before task weighting. This uses NumPy to evaluate the host
formula, not the production Zig loss, and uniform positive/negative targets,
not the full training labels. Bit counts include signed-zero differences. It
supports investigating the complete derivative operation sequence, not claiming
that a sigmoid replacement alone will fix the remaining trajectory drift.


The corrected test binary
`e09fbc3c55a3b85511855b3098e0259998e93460d3e97abb9b435e345b1f890b`
passes all 31 selected CUDA checks with zero skips. This includes the pinned
gather golden, work admission, legacy scatter and published-small heads/full
durable resume and portable reload; resumed model and optimizer state are exact.
Only the regression golden and explanatory comment changed after the trainer
build. Production source and trainer binary remain the v19 identity above.

The corrected integrated gather/admission/legacy-scatter checks also pass under
NVIDIA memory checking with zero errors. Separate heads and full-model
validation at the unchanged default Adam epsilon 1e-8 passes all initial,
gradient and first-updated-state comparisons with no parity failures. These
short checks do not supersede the failed sustained campaign.


## v20 binary-loss derivative arithmetic

An independent tensor-operation probe identifies the source focal VJP's branch
accumulation order, including soft labels where all branches contribute. The
CUDA prototype then matches all 12 random BCE/focal cases bit-for-bit (65,539
values each). The production CUDA 12.8 artifact reproduces those results with
zero NVIDIA memory-check errors, and four further cases combine saturated
logits with actual published-model start/end/inside/pair values. They also match
bit-for-bit. These are component checks, not sustained training qualification.

The implementation introduces a small optional binary-loss arithmetic callback.
Shared losses still build labels, apply masks, compute reduction denominators
and report scalar objectives. They pass already reduced loss cotangents to the
backend, allowing task weights to enter before the derivative's FP32 operations.
The returned loss records whether that weight is already present, preventing
accidental double weighting. CPU/Metal calls retain their previous behavior.
The callback reuses the normal CUDA allocator, tracked transfers and owned
training-math module. Shape/settings/finiteness checks and a conservative work
charge precede device execution; zero cotangents explicitly disable elements.

The CUDA kernel keeps individual tensor-operation rounding and the pinned
backward addition order, rather than simplifying the analytic derivative in
FP64. Scalar-power and derivative ordering were checked against PyTorch 2.9.1
[PowKernel.cu](https://raw.githubusercontent.com/pytorch/pytorch/v2.9.1/aten/src/ATen/native/cuda/PowKernel.cu)
and [FunctionsManual.cpp](https://raw.githubusercontent.com/pytorch/pytorch/v2.9.1/torch/csrc/autograd/FunctionsManual.cpp).
The trainer identity is v20 and the math artifact profile is v7. This change
covers boundary BCE/focal, soft-IoU BCE, inside BCE and supervised classification/
relation BCE. Listwise, consistency, query/count and record arithmetic remain
separate and require their own evidence.

The integrated derivative fixture uses exactly representable inputs and non-unit
cotangents across 24 cases, with reference digest
`046f83ccef3841eebf6ac2035079adc54bd3bbc37c528a90934c075f315b08f4`.
Its trainer and regression validation results follow below when available.


### v20 integrated results

Trainer binary:
`1ea9df97a44f3cdb2d23d2d4befa2017587e6d0ae6afdffa20f6f9cffb5fb962`.
Test binary:
`39a2aed72fea89a6ab81bf152c5f0f4dac626e2e7a3283ddacbf7b91663769de`.
All four focused integrated binary tests pass under NVIDIA memory checking with
zero errors. They cover the independent golden, masked nonfinite values,
settings/shapes/limits, exact reduction/work admission and applying supervised
loss weight exactly once. Four artifact-contract checks pass.

Both normal initial full-model traces retain exact forward logits. Start, end
and inside loss cotangents now match Python exactly on both traced microbatches,
without a gradient substitution. Remaining first-microbatch seed differences:
pairs 237 values (max 1.49011612e-8), proposals 59 (max 1.67638063e-8), nulls 4
(max 4.65661e-10) and counts 13 (max 9.31323e-10). Both initial state checks pass.
First-microbatch scorer gradient L2 error is 2.29111e-6 versus v19's 2.48975e-6;
encoder L2 is 5.25739e-5 versus 5.07506e-5. This does not establish an overall
parameter-gradient improvement. Gradient slots are cleared after the second
microbatch's optimizer step; their zero values are not backward parity evidence.

The uncontended 100-update campaign still first fails at total update 35 /
qualification update 32 / microbatch 69. Native pair loss is 10.10081768036 versus
Python 10.11340713501: difference 0.01258945465, allowed 0.01031340714. State
checks pass through total update 33 and fail by 43. Final weight checks fail for
257 tensors, maximum 0.00660658185 in `relation_scorer.content_linear.weight`.
Both workers clean up. Final tiny-held-out relation F1 differs: native 0.5
versus Python 0.0, both regressing from 1.0. Other final task metrics agree.
Keep the release unqualified.

Two uncontended diagnostic timing pairs give 15.49381 native versus 9.31431
Python examples/second, paired latency ratio 0.601147. No compilation overlapped
the campaign. These timings remain unqualified because sustained parity and
quality fail. No tolerance or optimizer default was changed.

The first microbatch records 6,657 launches, H2D 13,808,292 bytes, D2H
693,776 bytes and zero host fallback calls; the added staging is visible in
the existing transfer diagnostics.


All 35 selected CUDA regression checks pass with zero skips, including exact
published-small heads/full durable resume and portable reload. Separate heads
and full-model validations at the unchanged default Adam epsilon 1e-8 pass all
initial, gradient and first-updated-state comparisons with no parity failures.
Artifact regeneration checks match. These validations do not supersede the
failed sustained campaign or differing final held-out relation scores.

### Next confirmed formula concern: decomposed graph sigmoid

`Builder.sigmoid` emits `1/(1+exp(-x))`; generic strict autodiff differentiates
its division using `-a/b²`. A CUDA tensor-formula probe of these checked-in
expressions yields a nonzero derivative at x=17 (4.13994e-8) where pinned
`torch.sigmoid` has rounded its output to one and returns zero derivative.
At x=-90/-100 the decomposed formula produces nonfinite derivatives while the
saved-output reference gives zero. This is a formula diagnostic, not yet a test
executing the production Zig graph. Relation content gates and explicit-candidate
query gates use this builder. A retained sigmoid VJP preserving the reference
forward result is a concrete next check, alongside the remaining listwise loss
reductions; no sigmoid graph fix is included in v20.


## v21 retained sigmoid forward and saved-output backward

CUDA relation content gates and explicit-candidate query gates now select an
explicit FP32 sigmoid profile. The new strict VJP retains the rounded forward
output and evaluates `(upstream * (1 - output)) * output` in the pinned CUDA
12.8 math module. This removes the decomposed exponential/division derivative
and its saturation instability. Default CPU/Metal builder graphs retain their
existing decomposition. The graph profile is bound into the plan hash, the
trainer identity is v21, and the artifact profile is v8. Existing activation
launch validation, resident allocation and golden-test infrastructure are reused.

Trainer binary:
`2a0e05120bcf282b8669ec79cbece5ac76b62df99b7144579f045ef516c5c0cd`.
Test binary:
`82c2f1db02e871b469902062a92760b0d12635801b22a3a9e9dbf2cecfebac40`.

Three independent randomized production-PTX cases, each with 65,539 values and
input scales 1, 10 and 100, match pinned PyTorch forward and seeded backward
bit for bit. The integrated Zig graph checks all 2,049 exact eighth-step inputs
from -128 to 128 and non-unit cotangents against independent golden
`5207c722e249d72ef49116efec157718d729086fb0c6bfef26bfbcbbe52fc2bb`.
Both the direct probe and integrated graph pass NVIDIA memcheck with zero errors.
All 38 selected ML checks and 36 selected CUDA checks pass; CUDA has zero skips.
Heads/full durable resume and portable reload remain exact. Both default-epsilon
(1e-8) first-update comparisons pass with no parity failures. Four artifact
contract checks pass and regeneration matches.

Both normal initial full-model traces retain exact forward logits and passing
state checks. On the first microbatch, relation gradient tensors improve from
8/12 exact to 10/12 exact: both relation-content-gate gradients now match.
Remaining relation residuals are content_linear.weight (max 5.96046e-8) and
mlp.3.weight (max 2.98023e-8). Relation gradient L2 error falls from 2.26820e-7
to 1.45198e-7; encoder L2 falls from 5.25739e-5 to 4.98591e-5. Boundary loss
seed residuals remain unchanged: start/end/inside exact, pair/proposal/null/count
still differ. Optimizer-cleared second-microbatch gradient slots do not establish
backward parity.

The uncontended 100-update run still first fails at total update 35 / qualification
update 32 / microbatch 69. Pair loss is 10.10081958771 native versus
10.11340713501 Python: error 0.01258754730, allowed 0.01031340714. State checks
pass through total update 33 and fail by 43. Final weight comparisons fail for
257 tensors; maximum error is 0.00660663028 in relation_scorer.content_linear.weight.
Final tiny-held-out relation F1 remains native 0.5 versus Python 0.0, both down
from 1.0; other final task scores agree. Both worker cleanups complete. This fix
closes an isolated derivative mismatch, not the sustained release blocker.

Two diagnostic timing pairs give 15.40522 native versus 9.10198 Python examples/s,
paired latency ratio 0.590942. No build or other GPU workload overlaps the
campaign. These numbers remain unqualified because sustained parity/quality fail.
No tolerance or optimizer default was changed. No commit or push was made.

The remaining listwise derivative uses host exp/log, a serial sum, and applies
loss weight after differentiation; the reference uses CUDA logsumexp and weights
the cotangent before its derivative. A separate pinned layout probe confirms
that masked_fill materializes contiguous [B,Q,C] tensors even from candidate-major
inputs. A CUDA listwise implementation can therefore reuse the existing
contiguous-row reference reduction planner after canonicalizing its input; it
must preserve masks, active-query denominators and saved rounded logsumexp values.
Query/null/count, consistency and record objectives still need independent checks.


## v22 listwise backward and weighted pair accumulation

The CUDA loss callback now computes masked listwise logsumexp backward using
pinned CUDA 12.8 exp/log arithmetic and the existing reference reduction planner.
It accepts both candidate layouts and reduces shared proposal cotangents using
the same planner. Shared loss code retains target/mask construction and scalar
loss calculation. CPU/Metal defaults are unchanged. The trainer identity is v22;
the artifact profile is v9. Binary and listwise callbacks now pass through the
step's transfer admission adapter, including device temporary accounting.

The integrated 28-case golden covers both layouts, shared/nonshared logits and
widths 1, 7, 31, 129, 1025, 65539 and 262147. Oversized stress cases explicitly
raise their fixture limit; the production default stays 65536. Five focused
contracts/goldens pass under Compute Sanitizer with zero errors. All 41 selected
CUDA regressions pass with zero skips, including exact durable resume and
portable reload. Python harness contracts pass (18), artifact contracts pass
(4), and regeneration matches. Heads/full first-update comparisons pass at the
unchanged default Adam epsilon 1e-8.

At common initial weights, all forward logits, start/end/inside/pair/proposal
loss seeds and all 24 shared-scorer parameter gradients are now bitwise exact.
Null/count seeds still differ; encoder gradient L2 error is 4.790896e-5.
The four-microbatch trace passes state tolerances, but after the first optimizer
update the pair loss seed differs by up to 5.960464e-8 on identical logits.
Post-update cleared gradient slots are explicitly excluded as backward evidence.

The unchanged 100-update full-model campaign at epsilon 1e-6 still fails first
on pair loss entering total update 35: native 10.0999355316 versus Python
10.1134071350, error 0.0134716034 against allowance 0.0103134071. At the end,
270 weight tensors fail; maximum absolute error is 0.00666475785 in relation
content_linear.weight. Final held-out metrics agree, but both arms' relation F1
falls from 1 to 0 on this tiny fixture. Diagnostic throughput is 15.7287 native
versus 9.27895 Python examples/second; it is not qualified performance evidence.
This integration does not close sustained-training or release qualification.

A follow-up replay isolates the nonzero consistency branch's accumulation order.
The initial v22 ordering adds consistency before listwise/soft-IoU/BCE. Captured
Python components instead reproduce its active-consistency result exactly when
consistency is added last. A CPU replay reproduces the native aggregate exactly;
moving only this contribution last reduces the microbatch-3 discrepancy from
111 values / 5.960464e-8 to 15 values / 2.842171e-14. This is a diagnostic,
not a production fix or proof of long-run parity. Remaining consistency math
uses host FP64; query/count and other backward accumulation differences remain.

The source/binary manifests, initial fixture/compile failures, successful checks,
normal traces, campaign and replay evidence are archived as `v22-*` under
`evidence/2026-09-15-training-arithmetic/`. Tolerances, training defaults and
frozen fixture oracles were not changed.


## v23 active consistency accumulation order

The production CUDA objective now adds weighted pair contributions in the
observed reference order: listwise, soft-IoU, BCE, then consistency. This moves
an existing accumulation loop and adds no device allocations or kernels.
The native arithmetic identity is v23; the pinned v9 PTX is unchanged.
A regression with cancelling large branch seeds checks that the small active
consistency contribution survives. CPU/Metal default paths remain unchanged.

Both ReleaseFast builds pass. All 43 selected CUDA checks pass, zero skipped,
including the new regression, exact durable resume and portable reload.
Heads/full first-update validation still passes at default epsilon 1e-8.
In the four-microbatch normal trace, initial pair/proposal and marginal seeds
remain exact, as do all 24 initial shared-scorer parameter gradients. With
nonzero consistency on microbatches 3/4, pair seeds now differ in only 15 values,
maximum absolute error 2.842170943e-14, versus 111 / 5.960464478e-8 in v22.
All four state comparisons pass. Cleared gradient slots after optimizer steps
are not evidence of backward arithmetic parity.

The unchanged 100-update full-model comparison still fails first entering
update 35. Pair loss is 10.1007528305 native versus 10.1134071350 Python;
error 0.0126543045 exceeds allowance 0.0103134071. There are 255 failing final
weight tensors, worst error 0.00660578278 in relation content_linear.weight.
Held-out entity/classification/record F1 is 1 in both arms; relation F1 is 0.5
native versus 0 Python, both below the initial 1. Diagnostic throughput is
15.5069 native versus 9.20407 Python examples/second. These remain unqualified
timings; improved local derivatives do not resolve sustained parity or learning
quality. No tolerance or optimizer default was changed.

The source snapshots, binary identities, successful builds/checks, normal trace
and failed sustained comparison are archived as `v23-*` in the arithmetic
evidence directory. Production qualification remains closed.


A narrow diagnostic then replaces only Python null/count tensor cotangents with
native seeds, divided by the configured accumulation factor, while retaining
the original loss graph. At common initial weights both count-head tensors and
both null-projection tensors become bitwise exact. Boundary/candidate encoder
errors are unchanged; the main encoder stack still has L2 gradient error
4.705698e-5. Query-loss arithmetic therefore explains those head differences,
but does not explain all initial backward differences. This is an intervention,
not a production fix or evidence of sustained training parity. The first hook
attempt omitted the accumulation divisor; its `unscaled` artifacts are marked
invalid and excluded from conclusions. The corrected rerun and source hashes
are preserved separately.

Record scalar-loss derivatives still use host FP32 serial log-softmax/logsumexp
arithmetic and apply outer record weighting after differentiation. Remaining
record/candidate encoder gradients merit a same-logit task-loss trace; this is
a next diagnostic target, not a demonstrated cause of the update-35 failure.


## v24 record forward and derivative isolation

This iteration adds bounded record-logit observation to the diagnostic worker
and `diagnose_training_cuda.py --records`. The existing decision event borrows
logits already downloaded by the shared loss controller; it adds no training
transfer or arithmetic. The opt-in trace serializes semantic metadata only.
Compact JSON keeps the two captured dense groups within the unchanged 2 MiB
receiver limit. Initial pretty-printed and oversized module-response attempts
failed closed; those failed artifacts are retained. Large diagnostic module
arrays are subsequently stored in local files rather than enlarging response
limits. The trainer rebuild passes, as do all 18 Python harness contracts.
The arithmetic identity is still v23; no new sustained qualification is claimed.

The first diagnostic feeds the actual Zig `record_loss.compute` function with
captured Python CUDA logits and matching decisions, using a small standalone
ReleaseFast executable targeting the trainer's x86_64-linux-gnu baseline.
There are 69 differing record assignment derivatives across two groups, with
maximum absolute error 4.470348358e-8. This isolates a real loss-arithmetic
difference, but does not assume the two arms' forward logits are identical.

The new native trace then shows that record logits themselves differ before
the loss: 32,457 candidate values and 381 null values differ. Maximum candidate
error is 1.71661377e-5 and maximum null error is 2.86102295e-6. Candidate spans,
membership, gold masks, instance masks and matching decisions agree. The
standalone Zig loss on actual native logits reproduces its reported record
field scalar exactly (0.023120248690247536). The Python field scalar is
0.023120328783988953. The scalar target experiment maps required/optional single
fields to the same scalar branch; it does not change matching or supervision.

A fixture-specific module trace aligns the two per-sample record invocations
in graph-construction order, requiring one record group per sample and exact
slice extents. Inputs to inst_proj, field_proj and cand_proj are bitwise equal;
the retained cand_proj outputs are also equal. Replaying all three projections
with both CUDA 12 and CUDA 13 cuBLASLt, per-sample and combined, matches Python
exactly. Applying the native final SGEMM calls to those exact projected inputs
reproduces every native record logit. The projection library version is therefore
not the source of this captured forward discrepancy.

The distinguishing operation is actual batch geometry. A two-sample batched
candidate product matches all Python candidate scores exactly. Separate GEMMs,
and separate one-sample batched calls, reproduce the 32,457 differences. For
null scores, a single product over all 768 instance/field rows matches Python
exactly; separate per-sample products differ in 381 values. These are causal
same-input probes, not a production graph fix or proof of sustained parity.

A CUDA scalar-record-loss prototype reuses the reference reduction planner and
adapts the existing warp softmax implementation to log-softmax. It matches all
nonzero forward/backward values on captured rows and random widths 1, 7, 31,
128, 129, 193, 513 and 1024. Remaining bit differences are signed zeros in
inactive entries. Compute Sanitizer reports zero errors. It remains a prototype:
validity masking is applied by the probe, widths above 1024 are not implemented,
and list-valued fields/object objectives are outside its scope. It is not a
replacement for the production record loss yet.

The next production change should assemble padded record queries over the full
batch, use the existing batched matrix product for candidate scores, and flatten
the complete batch for null scores. Missing groups/fields must preserve reference
positions while remaining masked; a per-group batch-of-one wrapper is proven
insufficient. Reuse shared preparation, matching and loss code, bind the new
CUDA arithmetic profile into plan/checkpoint identity, and exercise mixed group
counts, field counts, record modes and batch one before sustained comparison.
Then integrate the record-loss derivative with admitted transfers/temporaries
and full-width behavior. The original parity/performance goal remains open.


## v25 full-batch record graph

CUDA training selects `RecordProfile.pytorch_batch_v1`; the existing per-group
profile remains the CPU/Metal default. Shared preparation, matching, loss code,
parameters, graph primitives, autodiff and BLAS dispatch are reused. The new
builder pads group/field positions over the full batch, performs global linear
projections, uses a true batch-B candidate contraction and globally flattened
null contraction, then returns views of actual groups and fields. Mixed modes
use masks to select pool or learned instance states. Shape/node/constant budgets
are checked and the profile enters plan/checkpoint identity as
`cuda_f32_loss_seeds_v25_batched_records`.

The final trainer SHA-256 is
`e03b1696879972c8672b35d4744208bdfa03cda4b8e3044c1852c8bc54900207`.
The mixed-mode graph regression compares forward outputs and every parameter
VJP with the established per-group path, including uneven groups/fields, an
empty sample, batch one, candidate/learned-instance padding, and rejected
layouts/budgets. Its first version supplied constant cotangents to an API that
requires runtime parameters and failed `InvalidGradientSeed`; the corrected
focused test passes. The same broad run passes all 43 existing selected CUDA
checks with zero skips, including exact published-model heads/full resume and
portable reload. The corrected test changes no production code. Both builds,
both test identities and the failed initial test source are preserved.

All 18 Python harness contracts pass in the pinned environment (a preliminary
system-Python invocation lacks NumPy and is excluded). Heads/full first-update
comparisons pass with default epsilon 1e-8. Four normal full-model microbatches
pass state tolerances. The captured initial model record output now has zero
numerical differences across 148,224 assignment logits and 384 object logits;
candidate geometry and matching agree. This establishes the predicted batch
fix for this fixture, not universal bit identity for mixed-mode CUDA execution.

Sustained qualification remains closed. The unchanged small/B2/epsilon-1e-6
100-update campaign first fails pair-loss tolerance at update 37, microbatch 73:
native 6.994833469390869 versus Python 6.9815168380737305, absolute error
0.013316631317138672 versus allowance 0.00718151683807373. There are 279 final
weight tensors outside tolerance, worst 0.00666316156 in
`relation_scorer.content_linear.weight`. Both arms retain entity/classification/
record F1 1 on the tiny fixture but relation F1 falls from 1 to 0. This local
forward correction is not an improvement in final sustained weight parity.
Two uncontended diagnostic timing pairs measure 15.4925 native versus 8.5490
Python examples/s; parity/quality failure prevents a performance claim.

A subsequent diagnostic keeps Python's original graph but substitutes only
record-output cotangents computed by the actual native host loss, divided by
the configured accumulation factor. It requires exact initial record outputs
and unchanged weighting. All 18 record-decoder, 2 candidate-encoder and 28
boundary-encoder parameter gradients then become numerically exact (normal
L2 errors 3.014809e-6, 2.871084e-6 and 8.426916e-6 respectively). The main encoder
still differs, L2 4.620872e-5; relation gradients remain unchanged. This isolates
record-loss arithmetic for the first three components, without claiming it
explains the full trajectory failure. The hook is diagnostic-only and never
part of the production worker.

Next integrate the admitted CUDA record-loss derivative across scalar/list
fields, object supervision and supported widths, reusing existing reduction,
softmax and transfer primitives. Preserve original matching, weighting and
optimizer defaults, and validate initial gradients plus sustained state and
quality again. Query-loss and remaining main-encoder arithmetic also remain
open; the production-parity and performance goal is not complete.


## v26 complete CUDA record-loss derivatives

The scalar record objective now uses admitted CUDA log-softmax, alternative-
target logsumexp and their VJPs. The same shared softmax implementation supplies
persistent and wide-row paths; reduction geometry reuses the reference planner.
Object BCE and list fields reuse the existing binary-loss callback. Matching,
labels, masks, cardinality, normalization and objective reporting stay shared.
Task weights enter cotangents before differentiation exactly once. CPU/Metal
retain their existing default path. Transfer/temporary admission precedes
dispatch, controls and finite checks remain active, and allocations have bounded
ownership. Checkpoint identity is `cuda_f32_loss_seeds_v26_record_loss` and binds
training PTX plus reduction/shared-memory device geometry.

The NVRTC 12.8 generator now hashes the resolved shared-header source; its five
contracts pass. Training PTX and all general CUDA artifacts were regenerated.
Final trainer SHA-256:
`92103c373bddea588a68cbccc26eef8ca573293b83ba4cb1edd24e8d603101b8`.
Final broad test SHA-256:
`469cc7202c604c120c9139280b0af1831d65ea46d01a2e1e2559be2af09629da`.
All 52 selected regressions pass with zero skips, including published heads/full
exact checkpoint resume/reload and transfer-budget rejection before callbacks.
Nine focused checks cover record input/memory admission, allocation failure,
mixed modes and graph behavior. Both default-epsilon first-update checks pass.

Independent Python CUDA goldens cover 63 log-softmax width/alignment cases,
including widths 1 through 262147, and 16 complete scalar-record pipeline widths.
Final forward/backward primitive bits match; complete record gradients/losses
match numerically with canonical-zero SHA checks. A mixed scalar/list/object
fixture covers natural, latent and anchorless modes. Compute Sanitizer reports
zero memory errors for primitives, complete pipeline and integrated native
record tests; log-softmax race checking reports zero hazards/errors/warnings.
An initial wide-row summation-order mismatch was fixed before these final
checks. Earlier compile attempts and failed probe are retained as superseded
evidence, not passing results.

All four initial full-model microbatch state comparisons pass. At common
initial weights, record-decoder 18/18, candidate-encoder 2/2, boundary-encoder
28/28 and shared-pool-scorer 24/24 gradients are now numerically exact. Cleared
gradient storage after optimizer updates is not gradient-parity evidence.
Main encoder remains nonexact (combined gradient L2 error 4.64221e-5); relation
and classification gradients also contain small differences. Initial null/count
loss seeds differ by at most 4.65661e-10 and 9.31323e-10 respectively. Replacing
only those seeds in Python via diagnostic hooks makes count/null heads exact,
but main-encoder L2 remains about 4.5849e-5. This rules out those two loss seeds
as the sole source of the encoder discrepancy. Hooks do not qualify production.

The unchanged small/B2/epsilon-1e-6 100-update campaign fails first at update 35,
microbatch 69: pair loss 10.1014738083 native versus 10.1134071350 Python,
absolute error 0.0119333267 versus allowance 0.0103134071. There are 252 final
weight tensors outside tolerance, maximum 0.00661898521 in
`relation_scorer.content_linear.weight`. Both arms have entity/classification/
record F1 1 and relation F1 0, regressing from initial relation F1 1. Two timing
pairs measure 14.7721 versus 8.4746 examples/s, diagnostic only. This is not a
sustained-parity or performance qualification. Tolerances and optimizer defaults
were not relaxed. Evidence is limited to the L4 and published-small fixtures.

Next isolate remaining encoder cotangents/backward arithmetic and classification/
relation gradient differences, while correcting the demonstrated query-loss
rounding mismatch through shared scalar-loss machinery. Broader model/hardware,
quality and performance gates remain open. No commit or push was made.


## v27 encoder cotangent isolation

Added `--trace-backward FILE` to the diagnostic native worker. Only validation
`trace_step` installs a borrowed CUDA LayerNorm backward observer; ordinary and
timed commands do not. The observer writes existing x/gamma/beta/dy and packed
dx/dgamma/dbeta to a bounded 128 MiB file, with the existing 2 MiB host allocator
and a 1024-invocation cap. It restores previous observers on every exit. Resident
instructions reject stream capture before dispatch. No training arithmetic or
optimizer defaults changed. Trainer SHA-256:
`bf1cd05de689bb6401b56c5b78a0ca320f7f0b4e4a1ff9bd4d52bc19d9fb3383`.
Initial compile attempts failed on reserved/shadowed identifiers; corrected
compilation succeeded, but installation exhausted the filesystem. Obsolete
reproducible compiler-cache executables were hashed and reclaimed; a cache-hit
installation succeeded and the final installed hash was verified. Sources and
model/evidence files were retained.

The one-microbatch normal probe exactly reproduces v26 parameter-gradient
comparison metrics. All 32 LayerNorm invocations have exact forward inputs and
exact dx/dgamma/dbeta when independently replayed in Python with native inputs
and cotangents. No native invocation was unmatched. The final encoder output
cotangent already differs in 3187 elements (maximum 5.96046448e-8, L2 3.29938e-7).
Text and query routes differ; classification routes are exact.

A diagnostic Python hook substitutes only the native final encoder cotangent,
divided by accumulation two. Then 194/198 encoder parameter gradients become
exact; remaining combined L2 is 1.67172e-6. Also substituting the native
relative-position LayerNorm cotangent makes 197/198 exact: only token embedding
weights remain nonexact (maximum 4.47035e-8, L2 3.02448e-7). These hooks isolate
upstream joins, relative-position accumulation and token-embedding reduction;
they do not change or qualify the production reference.

Measured Python input cotangents from left/right boundary projections, inside
projection, content pooling and relation scoring reproduce the text mismatch
exactly. Python equals sequential `(((relation + content) + inside) + right) +
left`; native equals `((relation + content) + inside) + (right + left)`. There are
2167 different valid text-state elements, maximum 2.98023e-8. The native graph
shares a reshaped text node between its two boundary shifts, joining those two
adjoints before other text consumers. The Python shifts consume text separately.
Adding branch observation leaves the entire first-microbatch tensor comparison
unchanged. A CUDA-only graph profile preserving separate views is now being
validated as v28. Query joins, relative embeddings, token embeddings and the
remaining task gradients still require correction. Sustained qualification
remains failed at v26; this diagnostic work is not a new 100-update gate.


## v28 independent boundary text views

CUDA selects `InputGradientProfile.pytorch_views_v1`; CPU/Metal retain shared
views. The right boundary shift receives its own reshape of the same original
text, so its adjoint joins the other text consumers separately from the left
shift. No forward payload is copied. Existing graph/activation admission accounts
for the additional node, and both plan and trainer identity bind the arithmetic
(`cuda_f32_loss_seeds_v28_text_views`). Final trainer SHA-256:
`c6e81fb7a4a1acf78e3c1f399797b3b2bb7961c65e6715c9376496274a06d4bc`.
An initial build launch followed a failed relative-path edit and was deliberately
interrupted; the corrected source was then built successfully and its installed
hash verified. The interrupted attempt is not a passing build.

The normal initial state comparison passes and all valid text cotangents now
match Python exactly: the predicted 2167 mismatches are gone. Classification
cotangents remain exact. There are 1020 remaining query cotangent differences;
main-encoder gradient L2 is 4.19959e-5. Record/candidate/boundary parameter matches
are preserved. No new sustained qualification or throughput claim is made.

A query-input diagnostic substitutes native null/count loss seeds in Python and
captures individual linear input cotangents. All four count/null parameter
gradients become exact. Python query gradients equal the sequence record,
count, null, shared scorer, inside, end, start, then relation. Native instead
creates shared-scorer inputs after count/null heads, so reverse accumulation
places shared-scorer contributions before count/null. That ordering reproduces
all non-relation query rows exactly. Only 94 element differences remain in the
four relation-role rows (maximum 7.45058e-9), indicating a separate relation
input-gradient difference. The earlier hypothesis that independent encoder
relation gathers necessarily explain this fixture's query mismatch is not
established; both reconstructed paths add relation contributions last.

With the final encoder cotangent matched by diagnostic hook, captured relative
Q/K input cotangents reproduce both frameworks exactly. Native sums all 24
contributions in reverse layer order; Python first sums each layer's Q/K pair
and then sums the 12 pairs. The resulting relative LayerNorm cotangent differs
in 31329 elements (maximum 2.08616e-7, L2 1.43901e-6). Python's per-layer slice/
unsqueeze creates that adjoint join; the native shared table currently lacks it.
Next preserve the per-layer join and move auxiliary query-head construction
after pool scoring, then check the same boundaries again. Token-embedding,
query-loss and remaining task-gradient arithmetic still remain open.

## v29 query and relative-position input-gradient joins

CUDA now selects `InputGradientProfile.pytorch_v1`; the default CPU/Metal
profile is `grouped_v1`. The shared query-head builder permits CUDA to create
auxiliary count/null heads after pool scoring, while retaining output seed
order. Each DeBERTa layer receives a separate identity view of the shared
relative-position table, joining its Q/K adjoints before other layers. These
views do not copy forward payloads. The trainer arithmetic identity is
`cuda_f32_loss_seeds_v29_input_joins`; final trainer SHA-256 is
`686e60e2fa96a5eee01f6a755c46f7b92fedc79c8b312d408a84436b0db1d43a`.

All 55 selected regressions pass, none skipped. Coverage includes a new
cancellation-sensitive test through actual boundary-shift VJPs, CUDA resident
normalization, descriptor cleanup under allocation failure, and exact published
checkpoint resume/reload. Heads/full first-update checks pass unchanged at
default Adam epsilon 1e-8. No new sustained or performance qualification is
claimed.

Normal initial text and classification cotangents remain exact. The final
encoder cotangent now differs in 170 elements (maximum 1.49012e-8, L2
3.44240e-8), versus 1020 at v28. This does not materially reduce aggregate
encoder parameter-gradient error: L2 remains 4.23999e-5. Under native query-loss
seeds, all non-relation query rows match exactly; 91 different elements remain
in relation-role rows, maximum 7.45058e-9.

With only the final encoder cotangent replaced by the native value in a Python
diagnostic hook, 197/198 encoder parameter gradients match exactly. The relative
LayerNorm cotangent now matches without its own intervention. The remaining
word-embedding gradient differs by at most 5.96046e-8 (L2 3.16974e-7).
An independent embedding-backward probe reproduces this error exactly using
serial accumulation. Grouping captured input rows into 32-index chunks matches
Python CUDA exactly for this 118-row fixture. PyTorch's separate large-input
path uses partial segments of ten occurrences; a production implementation
must cover both paths. The embedding probe is not yet integrated.

A separate CUDA 12.8 query-loss prototype matches Python numerically in all 60
tested shape/weight/mask cases and both captured initial null/count seeds.
Poisson backward requires separately rounded `seed * exp(x)` and `-seed * target`
products before addition. This prototype remains unintegrated. Remaining work
includes that loss path, embedding accumulation, relation input/parameter
gradients and classification parameter gradients, followed by unchanged
sustained state/loss/quality and performance gates. The failed v26 100-update
campaign remains the latest sustained result.

## v30 shared elementwise query-loss integration

The existing binary-loss backend is generalized to `elementwise_loss_math`;
BCE, focal and Poisson derivatives reuse the same admitted three-input/one-output
transfer path and temporary allocations. `queryLossWithBackend` constructs
targets/masks through the shared loss implementation, applies the scalar weight
before active-query normalization, and marks the resulting cotangent so callers
do not apply that weight twice. Count backward separately rounds the two product
adjoints before adding them. CPU/Metal retain the existing host arithmetic.
Query-head transfers and peak device scratch are included in admission; work,
finite-value and cancellation checks remain enforced.

The artifact profile is `gliner25_training_elementwise_cuda128_v11` and trainer
identity is `cuda_f32_loss_seeds_v30_query_loss`. Final trainer SHA-256:
`089e60c5093dc06637b4300d3647c7a52506ff054f5f2dff1f81023f9bcb5226`.
Final test SHA-256:
`c3e9d66c30479db8eb4b791702f0e6d892bf85cec146d519cfd4a504fb3a7901`.
All 57 selected regressions pass without skips, including published-model
checkpoint resume/reload. Heads/full first-update parity passes unchanged at
default Adam epsilon 1e-8. The complete Zig query-loss path matches independent
Python CUDA goldens for 180 size/weight/objective/mask cases. Device memcheck
reports zero errors; all five artifact-generator tests and regeneration check
pass. The initial test installation failed with a full filesystem; obsolete
generated caches were reclaimed and installation retried from the completed
cache. The failed attempt and successful retry are both retained.

All 102 boundary-head parameter gradients and all 18 record-decoder gradients
now match exactly in the normal first microbatch. Text and classification
cotangents remain exact. Only 91 relation-query cotangent elements differ at
the final encoder output (maximum 7.45058e-9, L2 2.32537e-8). The aggregate normal
encoder parameter-gradient error remains L2 4.17942e-5. A final-encoder-seed
intervention still gives 197/198 exact encoder gradients; the word embedding
remains nonexact (L2 2.88731e-7). These are initial diagnostics, not sustained
qualification.

Additional controlled probes establish the remaining changes to make:

- **Embedding accumulation:** an unintegrated CUDA kernel reuses stable grouped
  integer routing and follows 32-original-index chunks for small inputs and
  ten-occurrence partials above 3072 inputs. All 144 Python CUDA comparisons
  match numerically, including repeated-token cancellation, widths 1/33/384,
  and the small/large-path boundary. Padding semantics still need to be handled
  explicitly when integrating the arithmetic profile.
- **Classification gradients:** replaying two separate classification groups
  matches every Python classifier parameter gradient exactly; concatenating
  those groups into one matrix product/reduction matches every native gradient
  exactly. This reproduces all three nonexact classifier parameter tensors.
- **Relation input joins:** measured content-linear, gate and MLP input
  cotangents reproduce Python with `(linear + gate) + mlp`, and native with
  `(linear + mlp) + gate`. Reassembling the query root explains all 91 differences
  exactly. A shared identity view for the two content branches can preserve
  the required join. Serial versus indexed relation reduction is exact on this
  fixture and does not explain the discrepancy.
- **Relation row placement:** native compacts the two active pairs into rows
  0/1, while Python keeps per-batch padded rows 0/64. After aligning rows, active
  inputs to all inspected relation linears are exactly equal. Replaying the
  two layouts reproduces the full native `content_linear.weight` gradient and
  the recorded maximum/L2/worst-element mismatch of `mlp.3.weight`. The latter
  full native tensor was not captured. Equivalent cuBLAS layouts, versions
  12/13 and workspace configurations do not explain these differences. Preserve
  Python's padded relation grouping in the CUDA arithmetic profile, including
  heterogeneous relation counts, routing, masks and admission bounds.

Task observers leave all compared non-encoder parameters and the original
encoder-input cotangent unchanged. The successful forward-only trace leaves
the complete parameter comparison unchanged. An initial attempt to combine
forward replay with the backward observer failed because no-grad replay tensors
cannot accept gradient hooks; the corrected trace uses the standard forward
worker. Preserve that failed diagnostic separately from successful evidence.
Next integrate the profiled changes above, then rerun the unchanged sustained
state/loss/quality and performance gates. The failed v26 100-update campaign
remains the latest sustained result.

## v31: exact initial gradients; sustained parity still fails

The four v30 diagnoses are now integrated under CUDA input-gradient profile
`pytorch_v2`, checkpoint identity `cuda_f32_loss_seeds_v31_task_gradients`, and
NVRTC artifact `gliner25_training_embedding_cuda128_v12`:

- Word-embedding backward shares the existing stable integer grouping and
  scratch allocations. Small inputs preserve the reference's 32-row chunks;
  larger inputs preserve ten-occurrence partial reductions. Only the word
  embedding selects this profile, and its configured padding row receives no
  weight gradient. Forward still reads the row normally. Admission charges the
  additional reduction work and rejects invalid padding metadata. Unsupported
  interpreters reject the new backward profile explicitly.
- A shared relation-content view joins content-linear and gate adjoints before
  the MLP contribution, without changing forward values or duplicating weights.
- Classification builds each sample's label groups through the same classifier
  builder and shared parameters, with distinct deterministic dropout sites.
  Concatenation and masked padding preserve the existing output contract.
- CUDA relation rows preserve `[batch, relation, pair_capacity]` slots, including
  empty groups. Existing pair-count, transfer and device limits apply to the
  padded allocation. A checked routing helper rejects per-group overflow.

The trainer SHA-256 is
`1458d48b2a2371482cc22ec977df4d4a2a62a1bb819d72c7ac8b8db344fd6ebe`;
the selected-test binary is
`469efe547157a5cd583c058170f5f9932ccd9175ef75dfa9cd8f09259cb2014d`.
The final source manifest matches the compiled implementation. The first build
failed on a schema field path; its source, log and result are retained separately.
The corrected build and all **62 selected regressions pass, zero skipped**,
including exact published-small head/full durable resume and portable reload.
New tests exercise heterogeneous classification groups, empty relation groups,
checked slot packing, embedding padding and reduction admission. Five artifact
contract tests pass and regeneration matches. The production embedding PTX
matches Python on **192 cases** (small/large input paths, repeated/sparse/unique
indices, widths 1/33/384, padding enabled/disabled), with zero NVIDIA memory-check
errors. The full Zig graph/VJP/CUDA test matches the independent golden digest
`d33f5c0216a6a32af04cb556efd21c698c12169042a5e7a5d6fce3c77f0ca421`.

### Initial comparison and trained-weight intervention

On the ordinary first diagnostic microbatch, **334/334 parameter-gradient
tensors, comprising 73,881,879 elements, match Python CUDA exactly**: boundary
102/102, classifier 4/4, encoder 198/198, records 18/18, relations 12/12. All routed
encoder-input cotangents are exact. No native gradient, loss seed or weight is
substituted into Python in this run. The optional LayerNorm replays are separate
observations and do not alter the real backward pass.

Heads/full first-update state comparisons pass at the unchanged default epsilon
`1e-8`. After the full-model update, maximum weight difference is
`2.384185791015625e-7`; moments also differ slightly. The zero gradient slots
at this point are cleared optimizer buffers, not backward-parity evidence.

A separate diagnostic advances both ordinary workers by two microbatches,
then loads native post-update weights into Python before microbatch three.
All 334 parameter gradients again match exactly, as do all observed boundary
forward logits. This is an explicit common-weight intervention; it is not
normal training or sustained qualification. It separates first-update weight
rounding from backward arithmetic at that particular trained state. Small
same-logit auxiliary replay differences (~1e-15) do not affect the exact real
parameter gradients in this probe.

### Unchanged 100-update gate

The small/full/B2/accumulation-two run retains epsilon `1e-6`, two diagnostic
timing pairs, no warmup and 97 additional updates, matching the previous failed
campaign. All loss/state tolerances and production defaults remain unchanged.
It **fails**: the first component-loss error is entering total update 35,
microbatch 69 (qualification update 32), with pair loss `10.099387168884277`
native versus `10.113407135009766` Python. Absolute error `0.014019966125488281`
exceeds the declared allowance `0.010313407135009767`. Full-state snapshots pass
through update 33 and first fail at the next sampled state, update 43.

At update 100, **261 weight tensors, 259 first moments and 43 second moments
fail**. Maximum weight error is `0.006536979228258133` in
`relation_scorer.mlp.0.weight`. Final held-out output confidence parity fails.
Both arms' entity/classification/record F1 is 1, while relation F1 falls from 1
to 0 on the tiny fixture. This is insufficient learning-quality evidence.

Uncontended diagnostic throughput is 14.3193 native versus 8.48238 Python
examples/s, over only two pairs; paired native/Python latency ratio is 0.59258.
These numbers remain **unqualified** because sustained state/loss/quality parity
fails. The planned 100-update default-epsilon run was not started after this
failure. All workers were cleaned up with no survivors or resource violations.

Keep the release and performance gates closed. The exact initial and one-update
common-weight gradients narrow the next investigation to optimizer/clipping
arithmetic and any differences that appear at later common trained weights;
they do not establish that optimizer rounding alone explains the full failure.

The same explicit common-weight intervention after **34 ordinary optimizer
updates**, immediately before failing microbatch 69, again yields **334/334
exact parameter-gradient tensors** and exact compared boundary forward logits.
Candidate geometry also agrees. This is retained separately as
`v31-update35-nativeweights-*`, with the weight substitution, actual worker
command, driver/worker hashes and 68 advancing microbatches recorded. Python's
preexisting moments remain untouched; this comparison concerns the forward and
backward calculation at common weights, not a valid continued optimizer state.
The result prioritizes clipping/optimizer arithmetic over more backward-kernel
changes for the next isolation step.

Source inspection identifies concrete arithmetic differences to test next:
CUDA's shared transaction computes a scaled-sum norm and final host FP64 norm,
whereas the first observed Python norm is FP32; native AdamW receives FP32
hyperparameters and bias corrections and combines decay with its update, while
the pinned Python fused implementation receives double hyperparameters and
rounds a separate decay before its adaptive update. These are candidates for
causal probes, not yet a proven full-trajectory explanation or integrated fix.
Preserve shared transaction ownership, atomic publication and admission if a
profiled optimizer correction is introduced. Do not change the reference's
optimizer settings to manufacture parity.

The independent `v31-adam-production-probe` now confirms a local optimizer
mismatch without any model-gradient or norm differences. It launches the actual
production SM89 AdamW artifact and Python fused AdamW from identical weights,
already-clipped gradients and zero moments, over eight first-update cases
(33/8193 elements, both learning rates and epsilons). Weight differences reach
`2.384185791015625e-7`; both moments also differ. In an explicitly diagnostic
Python arm using the native-rounded scalar values, first moments become exact
in all eight cases, while second moments and weights still differ. Thus scalar
precision explains that isolated first-moment difference, and operation
ordering/precision remains relevant for the rest. The real benchmark reference
and production implementation are unchanged by this probe. It does not yet
prove that correcting AdamW alone will close the 100-update gate.


## V32: fused AdamW integration and clipping isolation (2026-09-16)

The trainer SHA-256 is
`efee12e90fce34b1577cf8515e1d005afbd8f2c8352cd0c8141e66d0ed93d890`;
its checkpoint profile is `cuda_f32_training_v32_fused_adamw` and the regenerated
training-math artifact is `gliner25_training_adamw_cuda128_v13`.
`v32-trainer-sources.json` binds the trainer and broad regression executable;
`v32-sources.json` additionally includes a counter-bound test and explicit test
module discovery added afterward. Those later edits change no production
function bodies. No commit or push was made.

### Shared architecture and optimizer arithmetic

Learning-rate schedules and AdamW configurations now share generic scalar-type
implementations with existing FP32 and new FP64 aliases. CUDA's explicit fused
profile retains parsed double scalars, applies the precise schedule, and passes
per-parameter update counters into the existing device transaction. The profile
is rejected on non-CUDA backends. Legacy CPU/Metal arithmetic remains selected
by default. Existing transaction ownership, staged snapshots, atomic publication,
rollback, absent-gradient handling and gradient clearing remain shared.

The CUDA artifact reproduces the pinned fused AdamW's separate decay rounding,
double scalar arithmetic, explicit fused moment updates and FP32 adaptive
update. An initial literal implementation matched first updates but failed a
repeated-update mean at step nine; the archived FMA variants isolate the
required operation order. Production artifact tests cover 12 cases of 100
updates each. Raw weight/first-moment/second-moment bytes have the same SHA-256
as Python:
`587f6f17e80e1c8b4bf1bb3df10c78793521eae501dd1efdee78aaf94a4f78a0`.
NVIDIA memory checking reports zero errors. Fourteen independent nonzero-moment
cases also match at counters through 16,777,216. The profiled transaction rejects
stepping beyond the exact consecutive-integer range of Python's FP32 counter;
legacy counter admission remains unchanged.

The broad regression executable
`b1a08179c4636ed96e2ea700e15ce0a9cb565dd11d8d093c157d478c1f06d673`
selected 79 tests: 77 passed and two Metal-only tests skipped. CUDA tests were
not skipped. This includes the integrated 1,200-update golden, controller
lifecycle/admission tests, published-small heads/full durable resume and reload.
After the additional counter-bound test, four focused admission checks pass;
14 standalone optimizer tests, five artifact generator tests and regeneration
verification also pass. A failed initial shadowing build and an initially empty
focused discovery run are preserved, not counted as passes.

### Model results and remaining release failure

Default-epsilon (`1e-8`) first-update checks pass for heads and full training.
Full training has exact initial gradients and **all 334 weights, first moments
and second moments exact after the first update**. Heads still has small
within-tolerance differences, including maximum weight error
`1.4901161193847656e-8`; its clipping multiplier differs by one FP32 unit.

The ordinary 100-update run keeps the previous fixture, small/full/B2,
accumulation two, epsilon `1e-6`, two timing pairs, no warmup and 97 additional
updates. First-update full state is exact here too. Loss first fails entering
total update 46 (qualification update 43); state passes through sampled update
43 and fails at sampled update 53. At the final update, **272 weights, 262 first
moments and 76 second moments fail**. Worst weight error is
`0.011759473942220211` in `relation_scorer.mlp.0.weight`. This is worse final
weight drift than v31, despite the later first failure. Held-out confidence
parity fails. Entity/classification/record F1 is 1 in both arms, while relation
F1 drops from 1 to 0 in both. These two synthetic validation examples cannot
establish learning quality.

Ordinary diagnostic throughput is 14.3466 native versus 8.96978 Python
examples/s; paired native/Python latency ratio is 0.625103. Only two timing
pairs were collected. **Parity and performance remain unqualified.** The
benchmark's diagnostic subprocess exit zero is not a parity pass: the wrapper
checks the report flags and exits one. The planned sustained default-epsilon
run was not started after this failure.

### Clipping intervention: causal contribution, incomplete explanation

Native clipping currently uses a scaled per-tensor reduction and host FP64
combined norm; Python uses FP32 tensor norms and a second FP32 reduction. The
first full update happens to have the same rounded clipping multiplier.
By ordinary update three the multipliers differ. Merely casting the final
native norm to FP32 does not fix that case; the reductions themselves differ.

A separate 100-update diagnostic makes Python apply the native clipping
multiplier on every update. Python still computes and records its real norm,
uses its own gradients and its unchanged fused AdamW, and receives no weight
or gradient substitution. Native runs first to provide the scalar receipt;
these timings are excluded from performance evidence. Driver/worker sources,
actual commands and all 100 receipts are retained. The initial worker startup
failure (Torch imported before runtime policy) is preserved separately from
the corrected completed experiment.

This intervention still **fails**. First loss failure moves to total update
56 (qualification update 53). State passes through sampled update 53 and fails
at sampled update 63. At update 100, 152 weights, 241 first moments and 100
second moments fail; maximum weight error is `0.0006935819983482361` in
`boundary_head.shared_pool_scorer.film_output.0.weight`. It reduces final weight
drift in this fixture, but does not isolate clipping as the sole cause. The
report explicitly sets production/parity qualification false. Cleared zero
gradient buffers after optimizer steps are not backward-parity evidence.

Next, locate the first non-exact gradient or optimizer state with a common
clipping multiplier, then reproduce that operation from identical inputs.
A matching initial update and isolated AdamW goldens do not establish exact
forward/backward arithmetic at every later trained state. Keep the ordinary
reference, tolerances, and release gates unchanged during this investigation.

The early-state diagnostic now narrows that next step: with a common clipping
multiplier, weights and both moments remain exact through two updates, and
all 334 gradients are exact on microbatch three. The first non-exact gradients
appear on microbatch five (entering update three): only three tensors,
`shared_pool_scorer.film_output.0.{weight,bias}` and `.3.weight`, differ. Maximum
absolute gradient difference is `8.673617379884035e-19`, far within tolerance.
After update three all weights remain exact; two first-moment tensors and one
second-moment tensor have tiny differences. These observations do not establish
that this earliest tiny discrepancy causes the later material drift.

A separate module/loss trace advances two **ordinary** updates and verifies all
weights and moments exact before tracing microbatch five, without any state
substitution. This preserves the first differing gradient state for local
arithmetic isolation. Its module comparisons include inactive storage and
explicit unfused replays; neither should be mistaken for active forward-output
failures. See `v32-update3-trace-*` and `v32-early-clip-*` evidence.


## V33 investigation: consistency-loss derivative isolated

This is an isolated prototype; the production trainer and its failed sustained
qualification remain v32. No v33 optimizer/model run is claimed.

At common exact weights after two ordinary updates, reconstructing the
microbatch-five weighted pair-gradient branch sum with Python's consistency
contribution removes all 20 pair-seed differences. Their maximum absolute error
was `7.105427357601002e-15`. This identifies a local loss-derivative discrepancy;
it does not prove that this discrepancy alone causes the later material drift.

There are two distinct differences. First, native stores the consistency weight
and warmup scale in FP32 before multiplying them. Python multiplies its double
scalars and then converts at the tensor operation. At update three, these are
`0.00010000000474974513` native versus `0.0001` Python. Changing only the weight
on identical CUDA inputs changes 506 consistency-component values. A separate
host replay with the correct scalar still leaves all 20 aggregate pair-seed
differences. Second, the host derivative simplifies the chain rule in FP64 and
rounds afterward; Python uses separate FP32 operations and deterministic scatter
reductions with the scalar cotangent incorporated earlier.

The isolated CUDA prototype now matches the captured probabilities,
log-survivals, start/end scatter sums and all three consistency gradients exactly.
It reuses the production gather-scatter kernel and stable grouping; no duplicate
scatter implementation is introduced. A first serial-sum prototype failed eight
start sums and 33 pair-gradient values, and is retained as failed evidence.
The reused reduction follows the pinned
[PyTorch Indexing.cu stride-one implementation](https://raw.githubusercontent.com/pytorch/pytorch/v2.9.1/aten/src/ATen/native/cuda/Indexing.cu).

The broader probe passes **82/82 comparisons**: the captured state plus 81 cases
covering widths 1 through 1025, warp boundaries, random/repeated/all-masked
indices, saturated logits and three weights. All compared probabilities,
log-survivals, scatter sums and derivatives are numerically exact. Inactive
signed zeros are not required to have identical bits. NVIDIA memory checking
reports zero errors. Source snapshots, failed prototype, scalar isolation,
combined production/prototype hashes and complete results are archived as
`v33-*`; `v33-integration-notes.md` records the remaining integration work.

Next integrate the derivative through the shared loss/backend callback and
admission/transfer ownership, retaining precise authoritative scalar fields.
Then rerun the ordinary training gate. The separately demonstrated gradient
norm/clipping mismatch remains unresolved; prototype parity is not sustained
training or performance qualification.


## V34: integrated consistency VJP and scheduled loss-weight precision

The CUDA trainer now selects the consistency-gradient callback through the
shared loss controller. Host code still owns scalar loss reporting, masking,
reached counts, integer routing and work/cancellation admission. CUDA computes
FP32 probabilities and derivatives using the existing deterministic gather
scatter for log-survival sums. It preserves masked entries in the grouping,
including clamped padding indices, because removing zero-valued entries can
change the reduction association. The backend uses tracked temporary tensors,
transfer accounting, stream-capture rejection and cleanup. The shared admission
plan includes descriptors, all 23 device buffers and three gradient readbacks.
Malformed routing, masks, active nonfinite inputs and transfer overruns fail
before dispatch.

The authoritative consistency and soft-IoU weights/schedules retain FP64 until
their final FP32 cotangent conversion. CPU/Metal explicitly retain their legacy
FP32 products. A scalar replay found soft-IoU premature-rounding differences in
27 of steps 0–100, first at step three, so both scheduled losses receive this
correction. No reference setting or tolerance was changed. The head parser and
validation support these doubles, and context fingerprints retain their bits.
The outer weight is incorporated once, before the CUDA derivative.

Trainer SHA-256:
`17657b5c169cc92e5a2a9183ee3ab6736dd73f8ad06e661dcabfb50c5d5f8150`.
Checkpoint arithmetic profile: `cuda_f32_training_v34_weighted_losses`.
Artifact profile: `gliner25_training_consistency_cuda128_v14`.
Regression executable SHA-256:
`e4f8982bd05e7b393e6445dbd9cdc404ab20eddff8ab751371d8975b79d36582`.
`v34-trainer-sources.json` identifies the trainer build;
`v34-sources.json` also includes two subsequent test-only additions. An initial
compile failure identified parser/fingerprint assumptions about FP32, which
were fixed. A later intermediate build was deliberately stopped to incorporate
the demonstrated soft-IoU precision correction; its cancellation is preserved
separately from the successful final build.

### Validation

The actual production artifact passes all 82 isolated Python CUDA comparisons,
with zero NVIDIA memory-checking errors. Five artifact-generation tests and
reproducible regeneration checks pass. The integrated backend's independent
63-case weighted-derivative golden passes with SHA-256
`8fc41553899b09e0a5fa2fb797468ec002c5e4609d5a2eb4672c445621f35fdc`;
it includes warp/reduction boundaries, colliding indices, all-masked cases and
clamped invalid padding. Inactive signed zeros are canonicalized in both arms.
The regression suite selects **90 tests: 88 pass and two Metal-only tests skip**.
No CUDA tests skip. Published-small heads/full durable resume and portable
reload remain exact. New checks cover scalar parsing/scheduling, callback
weighting, malformed routing, cancellation and transfer/work admission.

Default-epsilon (`1e-8`) heads/full first-update comparisons pass. All 334 full
weights and both moments remain exact. Heads retains the previously observed
maximum weight difference `1.4901161193847656e-8`, within tolerance; clipping is
still different there.

### Ordinary 100-update gate still fails

The unchanged small/full/B2/accumulation-two/epsilon-1e-6 comparison first fails
entering total update **44**, microbatch 87 (qualification update 41). Pair loss
is `7.1362624168396` native versus `7.1268415451049805` Python; error
`0.00942087173461914` exceeds allowance `0.00732684154510498`. State snapshots
pass through total update 43 and fail at the next sampled state, update 53.
At update 100, **272 weights, 260 first moments and 70 second moments fail**.
Maximum weight error is `0.007234878838062286` in
`relation_scorer.mlp.0.weight`. This is smaller final weight error than v32,
but the first loss failure is earlier; sustained parity remains unqualified.
Held-out confidence parity fails. Both arms retain entity/classification/record
F1 of 1 and lose relation F1 from 1 to 0 on the two-example synthetic fixture.

Diagnostic throughput over only two pairs is 13.8400 native versus 9.09488
Python examples/s; paired native/Python latency ratio is 0.657481. These are
**not qualified performance results**. The default-epsilon sustained comparison
was not started after this failure. The next causal check holds the clipping
multiplier equal while retaining the integrated loss corrections; its results
must remain separate from the ordinary benchmark.


### Completed clipping intervention and integrated memory check

The v34 native-clipping diagnostic completed all 100 updates with
`intervened_parity_passed=true` and no parity failures. All 334 weights, first
moments and second moments are bitwise exact at every sampled state, including
update 100. Python still computes its own gradients and optimizer updates but
applies the native clipping multiplier. This is strong causal evidence that
clipping arithmetic explains the remaining divergence in this tested trajectory
with the integrated loss corrections. It is not an independent production gate:
`parity_validated=false` and `performance_qualified=false` remain explicit.
Intervention timings must not be used as performance evidence.

The integrated Zig consistency golden also passes NVIDIA Compute Sanitizer
memcheck: one selected test, one passed, zero skipped and zero errors. Its
63 weighted derivative cases exercise the actual backend; this supplements the
82-case standalone production-artifact memory check. The sanitizer process has
terminated.

Remaining work is native reproduction of the pinned Python CUDA per-tensor norm
reductions, final norm reduction and FP32 clipping arithmetic, preserving actual
parameter registration order and filtering absent gradients. The ordinary
100-update gate and default-epsilon sustained comparison must then pass without
intervention before performance qualification. Final-cast-only correction was
already disproved by the earlier update-three replay.

## V35 — explicit Python CUDA norm backend (trainer integration pending)

The remaining clipping investigation now has a production CUDA norm primitive
behind the explicit `NormLimits.profile = .pytorch_f32` option. The shared trainer
still selects its legacy norm path; the installed trainer executable remains
v34. No v35 model-level parity or throughput result is claimed.

The new profile follows the pinned foreach norm's 65,536-element chunks,
512-thread reductions, four separate accumulators, fused square accumulation,
and FP32 square root. It then reduces the ordered per-tensor norms with the
existing CUDA block-reduction helper and performs a final FP32 square root.
A reusable partial buffer replaces padded per-tensor scratch without changing
nonzero addition order. All gradients stay on device; the backend downloads one
FP32 result. Admission covers the partials, per-tensor norms and scalar output;
shape, extent, cancellation and stream-capture checks precede dispatch. The
legacy scaled norm remains available for shared finite/state validation, and
non-CUDA backends reject this new explicit arithmetic profile.

The isolated candidate matches Python CUDA in 132 cases. The regenerated
production artifact matches **138/138** comparisons, including unaligned input,
chunk boundaries, more than 512 chunks, large tensors and final norm vectors
through 16,384 entries. These are arithmetic tests, not throughput measurements.
Artifact profile is `gliner25_training_norm_cuda128_v15`; regeneration checks
and all five artifact-generator tests pass.

An independent integrated golden contains 72 cases, with expected SHA-256
`7b97d0c17c2c14c769e4c39d2fdc1ad51ef8e1c7bf48acc00029775476531e53`.
It covers dense/sparse/large-magnitude inputs, chunk boundaries, and ordered
lists around warp and power-of-two boundaries through all 334 model parameters.
The integrated test also checks empty lists, admission, malformed extents,
nonfinite gradients and overflow. Its runtime result is recorded below after
completion of the build and regression run.

### Parameter order and scalar rounding

A candidate extension of the existing authoritative `capture_inventory.py`
generates a registration ordinal from the pinned modules' `named_parameters()`.
The small-model order matches the actual Python snapshot exactly for all 334
parameters; all three variants retain their exact existing tensor shapes.
This extension is staged in diagnostic evidence, not yet applied to the
repository generator or its hash pin. It avoids a second hardcoded name list.

The pinned Python tensor reverse division computes reciprocal then
multiplication. Thus the clipping coefficient requires separate rounded FP32
addition, reciprocal, multiplication and clamp. In **55 scalar cases**, this
sequence matches Python CUDA exactly, while replacing reciprocal/multiplication
with direct division fails four cases. A standalone strict-mode Zig ReleaseFast
helper matches all 55 raw FP32 results. This helper is not yet selected by the
shared training transaction.

Remaining integration must preserve absent-gradient filtering, parameter order,
transaction ownership and rollback, bounded admission, and checkpoint identity.
It must then pass independent sustained training without clipping intervention;
the ordinary v34 100-update failure remains the applicable model-level result.


### Completed v35 verification

The final regression executable SHA-256 is
`ce2dc8b69b5fc2eccd207cfce85f8bbf6cee647fdad6d9605f39ac1a05c09f88`.
Its exact sources are identified by `v35-sources.json` and preserved as source
snapshots. The build passes; **91 tests selected, 89 passed, two Metal-only
skipped**. The new 72-case integrated norm golden passes, including its rejection
checks. Existing published-small heads/full durable resume and portable reload
remain exact. Two initial compile failures were confined to the new test's
backend adapter call; both failed logs are retained separately from the final
successful build and runtime results.

NVIDIA Compute Sanitizer reports **zero errors** for both the integrated norm
test (one selected, one passed, zero skipped) and the 138-case production-artifact
probe, including the large and unaligned cases. All processes have completed.
The previous v34 test executable is preserved in shared memory with its hash;
only verified redundant generated build-cache outputs were removed for space.
The installed trainer remains SHA-256
`17657b5c169cc92e5a2a9183ee3ab6736dd73f8ad06e661dcabfb50c5d5f8150`.
No new sustained-training run has been performed with the norm profile selected.

## V36 — independent trainer clipping integration

CUDA full/heads training now selects the verified FP32 norm profile and strict
clipping scalar sequence. The shared controller owns an explicit permutation
of parameter slots, validates it, preserves it across staged restore, and binds
it to checkpoint identity. The transaction filters absent gradients, retains
explicit zero gradients, and reduces all participating accumulators in one
ordered call. Existing finite/state validation remains on its scaled path.
New scratch, host metadata, scalar transfer and work are admitted explicitly;
the backend and admission code share the checked norm-scratch formula.

The existing authoritative inventory generator now emits each parameter's
registration ordinal alongside its name and shape. No tensor shape, oracle
value or numeric tolerance changed. Only that generator's hash/size pin changed
in the reference manifest; the complete reference verifier passes. Resolving
native aliases through the existing canonical-name map reproduces Python's
actual order for all **136 heads** and **334 full-model** parameters, including
200 native aliases in the full inventory.

The new profile currently applies to CUDA full/heads. LoRA/DoRA retain their
existing clipping profile: their PEFT registration order requires independent
qualification and cannot be inferred from the base inventory. CPU/Metal retain
their existing arithmetic.

Trainer build passes, with SHA-256
`797b9a8280862839d1b92b2f03f9c7aa655559350a387b3dd09bf82682505630`.
Checkpoint arithmetic identity is `cuda_f32_training_v36_clipping`.
The norm artifact remains v15. `v36-trainer-sources.json` identifies the build;
`v36-sources.json` additionally includes a subsequent test-only fixture update
for classifier canonical aliases and the explicit one-scalar norm readback.
The initial compile failure from a mode-selection variable's scope is retained
separately from the successful build.

Added regressions cover independent scalar goldens, malformed permutations,
admission boundaries, canonical aliases, absent versus explicit-zero gradients,
owned order metadata, durable partial resume and wrong-order checkpoint rejection.
Runtime regression and independent model comparison results follow when complete;
this integration alone does not establish sustained parity or performance.


### Completed v36 qualification on the existing fixture

The expanded suite selects **106 tests: 103 pass and three Metal-only tests
skip**. No CUDA test skips. The regression executable SHA-256 is
`0e1f3524d975c2753b3d89c49efd233a9a27e1e96ea4d358d71070f5ba2e2ae4`.
The integrated clipping transaction and norm tests both pass NVIDIA Compute
Sanitizer memcheck: two selected, two passed, zero skipped, **zero errors**.
Published-small heads/full durable resume and portable reload remain exact.

Both default-epsilon first-update comparisons pass with bitwise-exact state:
all 136 heads parameters and all 334 full-model parameters, including first and
second optimizer moments. The previous heads clipping-rounding differences are
gone.

**Both independent 100-update full-model campaigns pass.** The existing
small/B2/accumulation-two comparison passes at epsilon `1e-6` and at the unchanged
default `1e-8`. All 334 weights and both moment fields are bitwise exact at every
sampled checkpoint, including update 100; there are no component-loss or
held-out confidence parity failures. Each implementation computes its own
clipping multiplier. No native scalar or state is supplied to Python training;
no tolerance was relaxed. This closes the sustained parity failure demonstrated
by v34 on this fixture.

The scope remains the pinned small checkpoint, FP32, disabled dropout/query
sampling/augmentation, and short repeated synthetic examples. Both weight sets
produce identical held-out task metrics on the two existing validation examples.
Relation F1 regresses from 1 to 0 at epsilon `1e-6` and from 1 to 0.5 at `1e-8`
in both implementations; the other task F1 scores remain 1. These results prove
implementation equivalence on this trajectory, not general learning quality.

The campaigns contain only two timing pairs and no warmup. Their timings remain
unqualified for performance claims. Larger-batch, warmed paired comparisons and
the broader model/task/hardware release matrix are still required.

### Build-space recovery

The first regression-install attempt exhausted the filesystem after compilation.
The qualification supervisor exited with ENOSPC; no numerical test failed in
that attempt. Five generated hexadecimal install-temporary files were copied
to shared memory, verified by SHA-256, and removed from the constrained build
filesystem, recovering 305,692,672 bytes. Sources, fixtures and evidence were
preserved. After confirming the original processes were terminal, the cached
build installed successfully and the complete qualification chain passed.
The failed logs, reversible backup receipts, successful receipts and final
source identities are archived separately.

## V37: warmed throughput and sustained batch coverage

The unchanged v36 trainer was compared with pinned eager Python CUDA on the
existing small-model synthetic fixtures, FP32, default epsilon `1e-8`, and
accumulation two. Each row used three warmup updates, 12 balanced timing pairs,
and 84 additional qualification updates: 100 total updates including initial
validation. All four rows pass; weights and both optimizer moments have zero
absolute numerical difference at all 12 sampled states per row (48 states in total). All component-loss
comparisons pass under the unchanged tolerances.

| Mode / microbatch | Native examples/s | Python examples/s | Paired native/Python latency (95% bootstrap interval) |
| --- | ---: | ---: | ---: |
| Heads / 2 | 46.27 | 13.94 | 0.3066 (0.2964–0.3143) |
| Heads / 8 | 84.32 | 50.43 | 0.5956 (0.5915–0.6134) |
| Full / 2 | 14.44 | 8.88 | 0.6183 (0.6074–0.6223) |
| Full / 8 | 32.74 | 33.71 | 1.0313 (1.0080–1.0402) |

Throughput columns are medians; latency uses the existing median paired log
ratio estimator. Full/B8 remains approximately 3.1% slower. These measurements
support this matched eager comparison only; they do not qualify production
learning quality, other checkpoints, adapter training, reduced precision,
longer inputs, another GPU architecture, or the fastest Python candidate.
`performance_qualified` remains false.

A fresh Nsight trace of the same trainer identifies substantial launch and
allocation overhead. The trace includes initialization, three warmup updates
and two observed updates; it is diagnostic, not another throughput result.
The host API summary includes 85,073 launches and 8,608 allocations/frees.
The shared boundary dispatcher accounts for 36.6% of device kernel time, and
AdamW for 13.1%. The largest embedding update alone takes approximately 12.7 ms.
The new clipping-norm chunks and finish kernels account for approximately 3.8%
of device kernel time, so norm batching alone is not an established solution
to the full-model gap. Physical cache retention and repeated safety-validation
launches are investigation targets; admission and finite checks remain required.

The first compiled-Python candidate attempt stopped before training because
Fastino wraps individual submodules, adding `_orig_mod` name components to
snapshots. The benchmark now retains the original parameter inventory and
checks identical parameter objects and registration order after compilation.
Both snapshots and held-out weight loading use those canonical names. It does
not strip arbitrary strings or relax the tensor inventory or numerical gates.
All 19 Python contract tests pass, including wrapper names, replaced/reordered/
missing/extra parameters, duplicate names and loading compiled-model weights.
The exact pre-fix helper sources used for the four measured rows are preserved
and verified against the hashes in their reports. Compiled numerical and
performance qualification is separate from these eager results.

### V37 candidate and cache follow-up

After correcting compiled-module snapshot identities, the initial compiled
candidate passes all original numerical gates through three updates. It is not
numerically identical to eager execution: final maximum weight difference is
`3.498280420899391e-6`. Its two timing pairs suggest native full/B8 latency is
approximately 19.1% higher. This short run has no warmup or held-out evaluation
and does not qualify sustained parity or comparative performance. A separate
100-update warmed candidate campaign follows.

A serial native-only ABBA cache sensitivity probe (1 GiB, 2 GiB, 2 GiB, 1 GiB)
uses the unchanged executable, 20 updates per run, and excludes three warmup
updates. Median update times are 492.01, 445.80, 448.15 and 482.91 ms,
respectively. Every run peaks at 3,045,609,144 device bytes under the same
4,160,749,568-byte physical allocation ceiling. This supports investigating
cache retention; it does not establish cross-framework parity or justify a
production-default change on its own. No production cache default changed.

The original tensor comparator converts values to float64 and measures numeric
error; zero error alone does not distinguish positive and negative zero.
References to bitwise equality for v36/v37 full-model reports should therefore
be read as zero numerical difference, unless a raw-bit metric is present.
The comparator now additionally counts differing raw FP32 bit patterns, without
changing any acceptance tolerance. A signed-zero regression verifies that the
new diagnostic distinguishes bit equality from numerical acceptance. All 20
Python contract tests pass. Earlier archived reports retain their original
schema and exact helper sources.

### Completed compiled-candidate sustained check

The unchanged v36 trainer versus compiled Python completes 100 updates as
`diagnostic_complete`, with parity false. The first loss failure occurs at
qualification update 17, entering total optimizer update 33. The final maximum
weight difference is `0.01106894458644092`; 573 tensor-field/metadata checks
fail. There are 142 recorded parity failures across losses, sampled state and
held-out comparisons. The warmed paired native/Python latency ratio is 1.1871
(95% interval 1.1384–1.2002), but its interpretation is explicitly diagnostic
because sustained equivalence fails. Final two-example relation F1 is 0 for
native weights and 0.5 for compiled-Python weights; both regress from 1. The
other three task F1 scores remain 1. This does not establish comparative
learning quality on real data. The matching eager-Python 100-update results
remain valid and separate from this failed candidate.

A v38 cache-default change is staged for build and regression validation:
CUDA resident training retains up to 2 GiB of idle exact-size buffers under the
existing physical memory ceiling, while inference keeps its 1 GiB default.
Explicit environment overrides and eviction on allocation-budget pressure are
preserved. Arithmetic and checkpoint identity remain v36. The native-only cache
probe motivates this change; production-default paired measurements are still
pending. Generated v36 binaries were backed up and verified before removal to
make room for the build; source files, fixtures and evidence were preserved.

## V38: bounded cache retention closes the eager full/B8 gap

Resident CUDA training now defaults to a 2 GiB idle-buffer cache, retaining its
exact-size reuse policy, explicit environment override, and physical allocation
ceiling. Other CUDA owners retain the 1 GiB default. The change reuses the
existing allocator and pressure-eviction path; no kernel arithmetic, clipping,
optimizer operation, or checkpoint identity changed.

The trainer build passes with SHA-256
`a46641b0bb2fdd104b5d6eb30a6c105fb4758b37ece7ead43886f09d118b2b86`.
The regression executable is
`8355f281ac251911a2f09d3d5de1a3639b56d455cfbc69b9b17a679eead4f849`.
The suite selects 110 tests: **107 pass and three Metal-only tests skip**.
The denominator of 143 in individual log lines is the compiled test inventory,
not the selected count. Physical cache charging/eviction and allocation-limit
checks pass. Both published-small heads/full durable resume and portable reload
remain exact; model and optimizer-state file hashes match their v36 results.

The production-default full/B8 eager-Python comparison passes all 100 updates,
using three warmup updates, 12 balanced timing pairs and accumulation two.
The new raw-bit diagnostics confirm that **all 334 weights and both moments
are bitwise identical at all 12 sampled states**, including update 100.
Component losses pass unchanged tolerances. No cache environment override,
clipping intervention, state injection, or numerical tolerance change is used.

Native median throughput is **35.5068 examples/s**, versus **33.6949** for
Python. The paired native/Python latency ratio is **0.942748**, with 95% bootstrap
interval **[0.923666, 0.970332]**: 5.7% lower native latency, closing the earlier
3.1% disadvantage on this row. Timed native peak physical allocation is
3,046,546,616 bytes, below the unchanged 4,160,749,568-byte ceiling. This is a
matched eager comparison on the existing small-model synthetic fixture, not a
claim about the fastest quality-qualified implementation on real data.

Held-out metrics remain identical between arms. Relation F1 falls from 1 to 0
on the two-example validation fixture; classification/entity/record F1 stays 1.
This preserves implementation equivalence without establishing general learning
quality. The other warmed rows still refer to v37/v36 binaries until rerun.
Compiled-candidate follow-up and broader model/task/hardware qualification remain
separate open work. Generated binaries were backed up with verified hashes before
rebuilding to avoid exhausting the constrained filesystem.

### V38 memory safety and compiled follow-up

NVIDIA Compute Sanitizer reports **zero errors** for three integrated checks:
physical cache charging/pressure eviction, the pinned CUDA clipping norm, and
clipping accumulation/resume/absent-gradient/order identity. All three selected
tests pass without skips.

The compiled-Python follow-up completes 100 updates with the same sustained
parity failure as v37: first loss failure at total update 33, final maximum
weight difference `0.01106894458644092`, and 142 recorded parity failures.
Its paired native/Python latency ratio is 1.114314 (95% interval
1.095848–1.130247). This remains a diagnostic result, separate from the passing
eager-Python campaign. The supervisor's nonzero qualification exit records the
failed compiled candidate; the trainer build, regressions and eager gate passed.

The other three warmed eager rows are being repeated on the v38 binary rather
than combining versions into a new matrix claim.

### Completed v38 eager matrix

All four rows now use the same v38 binary and pass 100 updates each. Across all
48 sampled states, every selected weight and both optimizer moments are raw-bit
identical to eager Python CUDA. All four paired latency intervals favor native.

| Mode / microbatch | Native examples/s | Python examples/s | Paired native/Python latency (95% interval) |
| --- | ---: | ---: | ---: |
| Heads / 2 | 48.09 | 14.74 | 0.31183 (0.30672–0.31643) |
| Heads / 8 | 86.34 | 51.54 | 0.60225 (0.58922–0.60565) |
| Full / 2 | 15.43 | 9.04 | 0.59199 (0.58516–0.61158) |
| Full / 8 | 35.51 | 33.69 | 0.94275 (0.92367–0.97033) |

The geometric mean of the four paired speed estimates is 1.7575x. This is a
descriptive result for the tested small-model synthetic matrix; it does not
satisfy the broader real-data/model/adapter/hardware release matrix by itself.

## V39: isolated optimizer-validation batching investigation

The optimizer currently uses scaled norms both for clipping and for finite/
zero-state checks. Actual clipping arithmetic must remain unchanged. Separate
boolean validation can batch the latter checks without dropping them or
pretending that a boolean flag is a numeric norm.

Two standalone CUDA candidates classify raw FP32 bits, including subnormals,
signed zero, infinity and NaN payloads. Each uses at most 128 by-value pointer/
length descriptors (2,048 bytes), 65,536-element chunks and a four-byte result.
The first atomically merges flags into an explicitly cleared result. The second
writes each chunk's flags to private scratch and reduces them in a final kernel;
it needs no initialized result and avoids contention on a shared atomic.

Both pass 117 independent NumPy/raw-bit cases, preserve input bytes, and pass
Compute Sanitizer memcheck with zero errors. The two-stage version additionally
passes initcheck with zero errors and racecheck with zero hazards/errors/warnings,
with PyTorch allocation caching disabled. These are isolated kernel checks;
production ownership, cancellation, admission and transaction integration remain
required before using the capability in training.

Using real parameter shapes and synthetic finite inputs, a Python/ctypes
benchmark measures about 21.0 versus 2.22 ms for one field per parameter, and
103.9 versus 11.13 ms for five fields, legacy scaled checks versus the two-stage
candidate. Both comparisons include host dispatch and final readback. They are
not predictions of production trainer speed, since Python launch overhead and
allocation paths differ from Zig. The profiler evidence and this result justify
an integration experiment; the production trainer remains v38.

A suitable shared interface must return explicit finite/all-zero booleans,
preserve the existing norm fallback for other backends, keep all pre/post/absent-
gradient checks and transactional publication, and account for scratch, metadata,
work and scalar readback. The actual FP32 clipping norm and registration order
are separate and must not change. No candidate kernel has been integrated yet.

## V39 integration under verification — 2026-09-16

The two-stage boolean validator is now wired into the shared resident optimizer
through an optional `residentTrainingValidate` capability. CUDA classifies FP32
bits in batches of 128 descriptors and returns one four-byte flags word. Other
backends retain their scaled-norm validation. Actual clipping continues to use
the existing norm primitive and arithmetic profile; the CUDA 12.8 training-math
artifact and checkpoint fingerprint are unchanged.

Pre-state, incoming-gradient, absent-accumulator, post-AdamW and cleared-gradient
checks remain in place. Ownership, shape, byte extent, dtype, capture, control
and resource admission precede dispatch. Shared scratch planning proves the new
buffer fits within the conservative legacy allowance; the 2KiB descriptor fits
within the existing 256KiB fixed metadata allowance. No full tensor is downloaded
and no descriptor payload is uploaded.

Added integration tests exercise raw IEEE patterns, chunk/tensor batch tails,
input preservation, scalar-only transfer accounting, optional-backend fallback,
malformed inputs, cancellation and active graph capture. **These source changes
are not yet qualified:** artifact regeneration, builds, regression tests,
sanitation and sustained paired comparisons are pending. The last completed
production measurements remain v38; isolated v39 prototype speedups are not
trainer throughput claims.

The first integrated test build succeeds. The new raw-bit/batch-tail and
admission/cancellation/capture CUDA tests pass, as does the shared scratch
planner test. The full suite then exposes a test-fixture initialization defect:
`seeded_device_transaction_test.Fake` deliberately leaves unused vtable hooks
undefined, including the new optional validator. A full-suite debugger run
confirms an indirect call to address `0x30` through that slot. The fake now sets
it explicitly to null to exercise the legacy norm fallback. A standalone
cancellation test had passed before the full-suite crash, so that isolated pass
was insufficient. Failed build/run evidence is retained; the corrected full
suite is rebuilding. No production parity or throughput claim is made for v39
from these partial results.

The second gate completes with 114 selected: 109 passed, three Metal-only skips,
and two failures. The callback-initialization fix clears the cancellation and
rollback tests. The remaining failures are (1) the shared CPU/Metal/CUDA optimizer
fixture still asserting twelve-byte alignment for CUDA's new four-byte flags,
and (2) the published-full resume test encountering live-memory admission before
its expected malformed-checkpoint rejection. The published-heads model/state
hashes remain exactly those of v38. The CUDA alignment assertion is corrected
without changing any numerical tolerance or transfer upper bound. Verified
redundant compiler outputs were removed and old generated binaries losslessly
compressed with SHA-256 round-trip verification, reclaiming 1,068,326,119 bytes
of shared memory. The production memory guard remains unchanged. A third gate
is rebuilding, with duplicate linker outputs removed before GPU tests.

The third corrected gate passes: **114 selected, 111 passed, three Metal-only
skips**. Both published-small heads/full durable-resume and portable-reload
checks pass; their complete model and state file hashes are unchanged from v38
(and therefore v36). The installed candidate test binary SHA-256 is
`5185117f8a434bcc0450c1ef09f776311f5eeb600c28abf6a648ab30363def1d`.
The memory issue is resolved without changing admission: redundant compiler
outputs are removed before running the gate. The production trainer is now
building, and isolated integrated sanitizer checks are running; sustained paired
training measurements remain pending.

Integrated sanitizer qualification also passes on that same test binary:
memcheck runs six selected validator/clipping/optimizer/cache cases with zero
errors; initcheck and racecheck each run the two validation integration tests,
with zero errors and zero race hazards/warnings. Sanitizer timings are excluded
from throughput results. Exact source/test hashes are recorded in the sanitizer
identity receipt, and the paired-run driver requires those hashes to remain
unchanged before it starts. Trainer compilation and the four sustained paired
comparisons are the next gates.


The v39 trainer build now passes (SHA-256
`2280f44d671b9296df07190ab4a6b2fece4b4418ee5123798bcd7d507250ed13`).
Its first sustained row, small/full/B8/eager-FP32, passes 100 updates with no
loss or state parity failures. All 334 weights and both Adam moments are raw-bit
identical at all 12 sampled states. Native median throughput is 39.2006 versus
33.3061 examples/s; paired native/Python latency is 0.853621 (95% interval
0.839835–0.876409), approximately 1.17x paired speedup. This remains a short,
repeated synthetic fixture with deterministic overrides, not broad performance
or learning-quality qualification. The other three small-model rows are running.


The completed v39 small-model eager-FP32 matrix passes all four 100-update
campaigns on the same trainer binary. All 48 sampled states have raw-bit
identical weights and Adam moments (136 heads / 334 full parameter tensors).
There are no loss/state parity failures; numerical tolerances are unchanged.

| Mode / microbatch | Native median examples/s | Python median examples/s | Paired native/Python latency [95% CI] |
| --- | ---: | ---: | --- |
| full-b8-eager_fp32 | 39.2006 | 33.3061 | 0.853621 [0.839835, 0.876409] |
| heads-b8 | 90.7502 | 50.2430 | 0.555960 [0.526401, 0.569785] |
| heads-b2 | 51.8581 | 14.9015 | 0.276167 [0.265756, 0.304071] |
| full-b2 | 18.5605 | 9.0956 | 0.491688 [0.480735, 0.509436] |

The descriptive geometric mean paired speed estimate is 1.9848x.
These are accumulation-two, deterministic FP32, short repeated synthetic
examples on one L4. They do not qualify other models, real-data learning quality,
stochastic policies, adapters or other GPUs. The fastest quality-qualified
Python candidate and wider release matrix remain open. `performance_qualified`
remains false, and no production registry entry is added.


V39 broader-model first-update preflights also pass the unchanged parity gates:
base and multilingual, heads-only, batch two, accumulation two and default
epsilon `1e-8`. Base has raw-bit identical weights and both Adam moments in all
three sampled states. Multilingual weights and second moments are also raw-bit
identical; 12 first-moment elements in `relation_scorer.mlp.0.weight` differ by
at most `1.401298464324817e-45` after the first update (one FP32 subnormal step).
Thus multilingual state passes numerical parity but is not fully bit-exact.
Component-loss and held-out confidence checks pass. These close the earlier
first-update discrepancies under the existing tolerances on this fixture;
sustained larger-model training and throughput remain unqualified.


## V40 wider heads-training qualification

The unchanged v39 trainer was tested for 100 updates at base/multilingual,
batches two/eight, using the same deterministic fixture, default epsilon and
unchanged numerical tolerances. Base/B2 and multi/B2 pass all comparisons. All
sampled weights and second moments are bit-exact; first-moment differences are
at most 4.71e-38 for base and 7.35e-40 for multilingual. Native medians are 50.1109 versus 14.7979 examples/s for
base/B2 and 47.6823 versus 14.9840 for multi/B2. Paired latency ratios are
0.303331 [0.289824, 0.309981] and 0.317182 [0.312653, 0.320849], respectively.

Base/B8 fails sustained parity. Its initial gradient difference is 5.72205e-6,
first-update maximum weight difference 9.06875e-8, and first sampled failed state
is at total update 56 (qualification-40). Losses first fail at total update 59.
At update 100, maximum weight/m/v errors are 0.00549456 / 0.00961337 / 0.000174517,
and held-out relation extraction differs. Its apparent speed advantage is
strictly diagnostic. Multi/B8 fails ResourceLimitExceeded before its first step
returns; initialization, inventory and inputs passed. Both use the existing
benchmark resource configuration. Do not infer a CUDA memory shortage until
the exact admission rejection is identified.

An initial base/B8 module/loss trace fails the benchmark's 2 MiB trace allocator
with OutOfMemory; this is a diagnostic-path limit, separate from the ordinary
training run. An explicit bounded 2–8 MiB trace JSON allowance and error-phase
receipt are being built in the benchmark worker, without optimizer/kernel
changes. The paged snapshot extension is deferred while these concrete blockers
are investigated; its unintegrated prototype is retained only in evidence and
/tmp, not in production sources.


The v40 runtime controls identify the base/B8 source: native SGEMM uses cuBLAS
13.4, while the pinned PyTorch wheel uses cuBLAS 12.8. Isolated 128x768
weight-gradient products at 56/80/88 rows reproduce the difference; the 12.8
products match PyTorch exactly. A native-only library substitution, with the
v39 trainer and all tolerances unchanged, passes 100 base/B8 updates. All sampled
weights/second moments are bit-exact; maximum first-moment error is 9.40395e-38.
Native/Python medians are 81.5757 / 54.0446 examples/s; paired latency is 0.666037
[0.652231, 0.684751]. Loaded-library hashes record the diagnostic substitution.

The multilingual/B8 rejection is the encoder logical-forward estimate: its
per-layer lower bound alone is 5,124,685,824 bytes, above the default 4 GiB
logical-work allowance. With only that allowance set to 8 GiB, the first step
succeeds at 1,736,141,120 tracked GPU bytes under the unchanged 4,160,749,568-byte
physical cap. The benchmark now exposes `--encoder-forward-bytes` and records
its resource limits. With the matching cuBLAS12.8 control, multi/B8 also passes
100 updates, all sampled weights/second moments bit-exact, maximum first-moment
error 7.52316e-37. Medians are 77.3921 / 51.6285 examples/s, paired latency
0.667716 [0.652252, 0.684348]. Both controls remain separate from production
qualification because they used a temporary native-only runtime wrapper.

## V41 explicit training runtime and checkpoint identity

The production loader now accepts an explicit absolute cuBLAS library via
`ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY`. An invalid configured library
fails closed; serving-library selection and the unconfigured lookup remain
unchanged. The loaded vendor version is queried and included in CUDA trainer
fingerprint `cuda_f32_training_v41_blas_identity`, since the control demonstrates
that changing it changes training arithmetic. Runtime-unbound older CUDA trainer
checkpoints therefore fail identity validation; portable weights remain loadable.
The benchmark exposes `--cublas-library`, records its file hash, and both workers
report their runtime versions. Native readiness also reports the trainer
fingerprint. No caller can silently select a different library after a path or
symbol error. Twenty-three Python contract tests and the Zig test-binary build pass. The
CUDA suite reports 112 passes, three Metal-only skips and one full-model
checkpoint test stopped by live-memory admission before its expected rejection
check. The same test passes in an isolated process after verified generated-artifact
reclamation, with exact resumed state/model and unchanged published hashes.
Together, 113 selected tests pass and three Metal-only tests skip. The focused
CUDA memcheck passes all three selected training cases with zero errors.
Integrated heads/B8 passes 100 updates (90.16 vs 52.75 examples/s native/Python
median); integrated full/B2 passes 100 updates across all 334 trainable tensors
(18.77 vs 9.03 examples/s), with both rows reporting cuBLAS 12.8 and no parity
failures. Integrated full/B8 now also passes 100 updates across all 334
trainable tensors (40.01 vs 33.84 examples/s), with a paired latency ratio of
0.850 and no parity failures. The earlier stopped run was a command-session
interruption; the retry completed under unchanged memory guards. Memory guards
and assertions are unchanged; failed attempts are preserved. The explicit
runtime and checkpoint identity path is integrated, while older runtime-unbound
CUDA checkpoints intentionally fail identity validation.
The integrated multilingual heads/B8 row also passes 100 updates across all
136 trainable head tensors (74.70 vs 52.22 examples/s), using the documented
8 GiB logical encoder-forward allowance and unchanged physical CUDA cap.
