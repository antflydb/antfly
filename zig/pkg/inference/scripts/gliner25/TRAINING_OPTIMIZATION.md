# CUDA training optimization and parity investigation — 2026-09-15

This investigation uses the published small checkpoint, pinned Fastino checkout
and existing supervised CPU/Metal fixture adapter. The comparison remains strict
FP32 with dropout and augmentation disabled, accumulation two, constant learning
rates and the upstream fused AdamW optimizer. It does not qualify production
training, other models, reduced precision or convergence.

**Current arithmetic status (v36):** the independent small-model full-training
100-update comparisons now pass at both epsilon `1e-6` and the default `1e-8`,
with bitwise-exact weights and optimizer moments at every sampled checkpoint.
See [the arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md). The numerical
failures described below are historical; broader performance and production
qualification remain open.

**Subsequent numerical investigation:** a Python-versus-Python control with
one-ULP initial parameter perturbations also develops comparable 100-update
drift. Three CUDA loss-seed discrepancies have been corrected, but neither
default-epsilon first-update parity nor sustained cross-framework parity is
resolved. See [the loss-parity follow-up](LOSS_PARITY_FOLLOWUP.md); the original
short-run measurements below are historical evidence, not qualification of the
updated numerical contract.

**Later release follow-up:** extending full-model batch-two training to 100
updates at matched epsilon `1e-6` fails strict loss, weight and moment checks.
The first loss failure is at microbatch 73, entering optimizer update 37. A
control without held-out evaluation produces an identical final tensor
comparison, excluding that instrumentation as the cause. The short passing
campaign below remains valid evidence for its 14 updates only. See
[PRODUCTION_REVIEW.md](PRODUCTION_REVIEW.md#release-blocker-follow-up) for the
new memory enforcement, checkpoint tests and qualification limits.

## Implementation

- Dense repeated routing indices now use stable counting sort. Sparse high
  destination ranges retain heap sort. Both produce exactly the same ascending
  destination and original-ordinal order, including negative aliases. The dense
  path fits the existing conservative metadata and work admission limits.
- Contiguous-axis reductions decode their indexing once. They retain the prior
  serial FP32 accumulation order; disjoint axes retain the general path.
- FP32 cuBLASLt calls reuse the existing bounded, alignment-aware plan cache.
  The datatype is part of the cache key; computation remains FP32.
- CUDA norms batch per-tensor summaries into one readback. Inactive optimizer
  accumulators are also checked in bounded batches, before replacements or
  updates. Finite checks, zero-state validation, cancellation and transactional
  ownership remain enforced.
- Scalar constants use a fill launch instead of synchronous host uploads.
  CUDA training owners enable exact-size temporary reuse and a default maximum
  of 4,096 cached buffers, within the existing 1 GiB byte budget. Other owners
  retain their prior defaults; the existing buffer-count environment override
  still applies. Physical pool retention remains part of memory qualification.
- CUDA BCE cotangents use FP32 sigmoid/subtraction semantics, including zero
  residuals at saturated positive logits. Focal losses use FP32-rounded sigmoid
  probabilities. CPU/Metal retain their existing reference loss precision.
- Exact GELU now calls CUDA `erff` instead of a polynomial approximation in both
  primitive and boundary kernels. CUDA 12.8 and 13.2 libdevice can still differ
  by one FP32 ULP near the erf endpoints. Operator regression bounds account for
  the resulting CDF cancellation; full-model parity thresholds are unchanged.

The exact GELU expression follows the
[PyTorch 2.9.1 CUDA implementation](https://github.com/pytorch/pytorch/blob/v2.9.1/aten/src/ATen/native/cuda/ActivationGeluKernel.cu).
That shared expression does not imply bitwise agreement across toolchains.

CUDA training fingerprints now include `cuda_f32_loss_seeds_v2` in place of
`cuda_f32_v1`. Earlier CUDA checkpoints are intentionally incompatible with the
new numerical contract. CPU/Metal fingerprint markers are unchanged.

The later loss-parity follow-up advances this marker to
`cuda_f32_loss_seeds_v3`. CUDA checkpoints from either earlier marker must not
be resumed under the new derivative contract.

## Diagnostic evidence

The original full-model campaign measured 4.84 versus 9.04 examples/s at
microbatch two, and 7.65 versus 32.17 at eight, for Zig versus Python. Those
timings were explicitly diagnostic because strict weight parity failed.

A CUDA trace exposed repeated approximately 30 ms host gaps before grouped
scatter launches. The full batch-eight attention workload sorts 167,088 routing
indices into 331,344 destination rows repeatedly. Replacing heap sort reduced
steady diagnostic forward/backward host intervals from approximately 890 ms to
215–225 ms. These phase samples overlapped compilation and are explanatory,
not an end-to-end speedup claim.

The benchmark worker has an explicit `profile_step` command that reports host
phase intervals and decision-event timestamps. GPU work may cross host phase
boundaries. Ordinary paired `step` commands do not install these observers.
The Python snapshot comparator now reports the worst element's flat index,
both values and the associated Adam state for failed weights, without retaining
complete snapshots or changing acceptance.

First-update diagnostics show that AdamW applied to each arm's own gradients
predicts its weights to approximately FP32 rounding precision. Some failing
coordinates have clipped gradients around `1e-9`, below epsilon `1e-8`. Small
absolute gradient differences can therefore produce much larger update
differences. Matching broad gradient tolerances alone does not establish weight
parity. BCE saturation correction by itself did not remove the observed
full-model discrepancies.

Matching Python's bundled cuBLASLt library instead of the system library left
the first-update errors unchanged in the heads/batch-two experiment. Loader
diagnostics confirmed the alternate library was loaded.

An explicit `--adam-epsilon 1e-6` first-update experiment passed all existing
checks. Both arms receive this value through the same hashed configuration;
the original `1e-8` default is unchanged. This tests optimizer sensitivity to
small gradients. It does not establish that the changed hyperparameter preserves
convergence or resolves parity under the original optimizer configuration.

## Regression validation

- Required-hardware ReleaseFast gate: 46 passed, zero skipped. This includes
  module-discovery tests, reused optimizer/attention fixtures and new routing,
  reduction, scalar, norm, matmul-plan and loss/GELU regressions.
- Python comparison contract tests: 24 passed.
- Rebuilt inference worker: all ten frozen cases on each of small, base and
  multi passed against pinned Python CUDA (30 cases).
- NVIDIA Compute Sanitizer memcheck: zero errors across four full-model
  microbatches and two optimizer updates at batch two using the final worker.
  Sanitizer and compilation runs are excluded from throughput measurement.
- Nsight allocation traces, four microbatches/two updates: heads/batch two
  peaks at 549,179,164 dynamic device bytes versus a 960,507,432-byte reported
  bound; full/batch eight peaks at 3,227,872,676 versus 4,247,988,248. All traced
  dynamic device allocations are freed at shutdown. Each trace additionally
  records 152 bytes of module static storage. These observations cover the
  named workloads and cache configuration, not every shape or driver-private
  allocation. Traces and JSON summaries are
  `/tmp/gliner25-training-opt-memory-{heads-b2,full-b8}.*`.

## Final default-epsilon measurements

NVIDIA L4, strict FP32, published small, pinned Fastino/PyTorch 2.9.1+cu128
eager CUDA and fused AdamW. Each row has one validation update, three warmup
updates and ten measured paired updates, with accumulation two. Compilers,
profilers and other GPU workers were stopped before measurement. The default
epsilon is `1e-8` in both arms. All four rows still fail strict weight parity;
these timings remain diagnostic.

| Mode | Microbatch | Zig examples/s | Python examples/s | Zig speedup (paired 95% interval) |
| --- | ---: | ---: | ---: | ---: |
| Heads | 2 | 29.00 | 14.75 | 2.01x (1.95–2.07x) |
| Full | 2 | 11.28 | 9.01 | 1.25x (1.23–1.29x) |
| Heads | 8 | 95.31 | 53.70 | 1.77x (1.73–1.79x) |
| Full | 8 | 32.79 | 33.59 | 0.98x (0.97–1.00x) |

Full-model Zig throughput improved approximately 2.33x at batch two and 4.29x
at eight versus the original campaign. Python retains a 2.46% paired latency
advantage at batch eight (95% interval 0.17–3.19%); the upper rounded speedup
endpoint in the table is below one before rounding. This is not a demonstrated
batch-eight win.

The first/final failing weight-tensor counts are heads/2: 4/9, full/2: 3/5,
heads/8: 3/5 and full/8: 2/3. Original thresholds remain in force. The largest
final weight error is `8.39e-5` in heads/2.

Raw reports, paired observations, per-tensor failures and cleanup receipts:
`/tmp/antfly-gliner25-cuda-training-campaign-v2/`. Its `summary.json` records
per-report hashes. Final training executable SHA-256:
`210da9d70b82bdba4ebf3778dc0e1dc5855ab3ea21c786921925020941a0d57e`.

## Explicit epsilon `1e-6` comparison

All four configurations pass the unchanged loss and tensor checks: initial
state, accumulated gradients after the first microbatch, complete state after
the first update, every observed component loss, and complete final state after
14 optimizer updates. The same final executable and campaign methodology apply;
both arms use epsilon `1e-6`. This is a separate optimizer configuration, not
a repair of default-epsilon parity.

| Mode | Microbatch | Zig examples/s | Python examples/s | Zig speedup (paired 95% interval) | Largest final weight error |
| --- | ---: | ---: | ---: | ---: | ---: |
| Heads | 2 | 29.55 | 14.82 | 1.98x (1.93–2.06x) | 4.53e-6 |
| Full | 2 | 11.25 | 8.84 | 1.26x (1.22–1.28x) | 6.29e-6 |
| Heads | 8 | 94.98 | 52.72 | 1.78x (1.74–1.82x) | 9.83e-7 |
| Full | 8 | 32.53 | 32.32 | 1.00x (0.97–1.03x) | 1.67e-6 |

Rates are separate-arm medians; speedups use paired log ratios. Batch-eight
full-model performance is statistically indistinguishable in this experiment.
These results do not demonstrate a full-model batch-eight speed win over Python
or superiority to reduced-precision/compiled Python training candidates.

Reproduce using the existing training command in [CUDA.md](CUDA.md), adding
`--adam-epsilon 1e-6` for **both** arms through the shared configuration. An
explicit production job can represent this choice as `run.epsilon = 1e-6`, but
this investigation does not change the production default or qualify that job.
Keep the original-epsilon campaign as a separate regression.

Evidence: `/tmp/antfly-gliner25-cuda-training-campaign-eps1e6/`, including
`summary.json`, per-report hashes, complete comparison failures/summaries,
timing pairs, worker provenance and cleanup receipts. Preserve both campaigns
in release evidence storage before cleaning the local temporary directory.

## Acceptance

Keep the existing weight, gradient, moment and loss thresholds. Preserve failed
checks in diagnostic reports and leave the production qualification registry
empty. A throughput improvement must not be represented as numerical or
learning equivalence. Long-run and held-out quality qualification remains
necessary after the coordinate-level numerical investigation.

Remaining work is specifically the default-epsilon numerical mismatch, learning
quality for the explicit epsilon choice, and a clear full-model batch-eight
speed win. The final trace still shows many small boundary/norm launches and
allocation/free calls. It includes initialization, so its aggregate API totals
are not steady-step timings. Further fusion or allocation planning should
preserve per-tensor reduction order, transactional state validation and the
physical memory bounds established here.
