# CUDA loss parity follow-up — 2026-09-15

The observed long-run weight difference can arise from amplified FP32 rounding.
It is nevertheless substantial by the end of training, and **production
qualification remains closed**. Correcting identified loss derivatives has
not resolved the default-epsilon or sustained weight-parity failures.

## Rounding control

Two pinned Fastino/PyTorch 2.9.1+cu128 CUDA workers trained the same published
small model in full mode for 100 updates, using the existing batch-two fixture,
accumulation two and epsilon `1e-6`. Both used the existing training worker,
with dropout, augmentation, query sampling and shuffling disabled. The second
worker shifted every initial parameter one FP32 representable value toward
positive infinity using `torch.nextafter`. No native model, native derivative
or alternative optimizer participated in this control.

Initial weights passed the existing tolerances. Component loss first failed
the same comparison rule at microbatch **79**, entering update 40. At update
100, **218 weight tensors failed**, with a largest absolute difference of
**0.00821560**. Both worker process trees were cleaned up successfully.

This demonstrates that rounding-sized initial perturbations alone can produce
drift comparable to the original Zig/Python maximum of 0.00785755. It does not
prove that every native discrepancy is caused by rounding, nor establish that
the two training procedures learn equivalent models. Perturbing every initial
weight is a sensitivity experiment, not a reproduction of native arithmetic.
Long-run state comparisons remain recorded failures; no tolerance or release
requirement is waived.

## Corrected loss derivatives

The CUDA trainer already selects a tensor-FP32 derivative profile. Three paths
did not fully honor it:

- **Listwise ranking:** derivatives normalized exponentials in FP64 instead
  of using the rounded FP32 forward `logsumexp`. For logits `[20, 0, -20]`
  with the first candidate gold, Torch gives that gold logit exactly zero
  gradient; the FP64 normalization leaves a small negative residual. Both
  reranker and proposal objectives now use the FP32 profile. Tests also cover
  non-saturated logits, several active queries and an inactive query.
- **Inside-span BCE:** the mixed objective omitted the selected precision
  when calling inside supervision. It now propagates it, preserving Torch's
  zero residual when a positive sigmoid saturates.
- **Poisson count:** the rate now rounds to FP32 before subtracting the count
  target. At the stored FP32 value of `log(2)`, the FP32 residual is exactly
  zero; the prior FP64 intermediate leaves a residual. Overflow still fails
  with `NonFiniteBoundaryTraining` and releases the allocated gradient.

The existing shared loss implementation and source fixtures are reused.
CPU/Metal retain their reference-FP64 derivative contract. No optimizer default,
parity tolerance, source checkpoint or serving qualification entry changed.
The CUDA training fingerprint advances to `cuda_f32_loss_seeds_v3`, deliberately
rejecting resume from checkpoints made with earlier CUDA loss derivatives.
This does not claim bitwise equivalence between host FP32 loss arithmetic and
every CUDA reduction/transcendental implementation.

## End-to-end results

The corrected derivative campaign retains all original acceptance thresholds.
These are diagnostic qualification runs, not throughput measurements.

| Check | Result |
| --- | --- |
| Small heads, default epsilon `1e-8`, first update | Four weight tensors still fail; maximum absolute difference 3.65973e-5 |
| Base heads, epsilon `1e-6`, first update | State and held-out fixture parity pass |
| Multilingual heads, epsilon `1e-6`, first update | Three weight tensors still fail; maximum absolute difference 1.18576e-5 |
| Small full, epsilon `1e-6`, 100 updates | First component-loss failure at microbatch 73/update 37; 266 weight tensors fail at update 100, maximum difference 0.00660358 |

The smaller final maximum in this one corrected run is not evidence of a
general reduction in drift: the rounding control shows that this trajectory
is sensitive to small perturbations. After 100 updates, both corrected native
weights and Python weights produce the same extraction decisions on the two
synthetic validation examples, but confidence parity fails. Both also regress
from initial relation F1 of 1.0 to 0.0. A representative training/held-out split
and an agreed quality target are still required.

At the worst default-epsilon first-update weight coordinate, the clipped
gradients inferred from first Adam moments are approximately `-6.34e-10`
(native) and `-1.53e-9` (Python). Those tiny absolute differences produce a
weight difference of `3.66e-5`. Matching broad gradient tolerances therefore
does not imply matching near-zero-gradient Adam updates.

Raw reports are retained in the accompanying evidence bundle. The production
review remains the release checklist; these corrections close identified loss
implementation discrepancies, not the outstanding training-parity or quality
requirements.

## Validation and evidence

- 45 selected CPU/CUDA operator, trainer and closed-qualification checks passed,
  with zero skipped.
- Both published small-model heads/full checkpoint tests passed: resumed
  optimizer state and portable model match uninterrupted training exactly.
- All 42 standalone source tests passed, including the new listwise and
  inside-supervision regressions, the Poisson zero-residual and overflow cases,
  masking, cancellation and allocation-failure cleanup. This count overlaps
  the selected regression suite and includes its module-import test.

The GPU/checkpoint test binary contains the corrected finite-input derivatives.
The final source-unit tests additionally cover the subsequently added Poisson
overflow guard and expanded scalar fixtures. The final rebuilt training worker
repeats all 100 updates and produces an identical final tensor comparison to
the earlier corrected run. Its SHA-256 is
`f63462d8c78aa78b2603ee3d2d71abf233c3698b7af2583d1067cc086aae15c8`.

See the [evidence bundle](evidence/2026-09-15-loss-parity/README.md) for raw
reports, diagnostic control scripts, implementation patch and source/binary
hashes. No commit or push was performed during this follow-up.
