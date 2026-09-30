# Laya training memory and device-memory admission: 2026-09-25

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md)
(Training memory, under Trainer throughput).

Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0, ReleaseFast.
Machine was shared with ~8 other agents' training/eval/build jobs throughout;
step times below are slower than the uncontended numbers in the sibling
trainer-throughput log for that reason. `~/bin/zig build -Doptimize=ReleaseFast
--prefix .tmp/release` (release binary), then `.tmp/release/bin/antfly-inference
finetune train laya <job.json>` under `/usr/bin/time -l` and the shared
`with-lock gpu`/`with-lock build` wrappers.

Job: released `laya` checkpoint (28 layers, hidden 1024, 16 heads,
intermediate 2624, vocab 50368, ~394.7M trainable f32 parameters at
`freeze_layers: 0`), `train_file`/`eval_file` from the shared step-0 subset
(`td/s0-train.jsonl`, `td/s0-eval-tiny.jsonl`), `packing: question`,
`batch_size: 1`, seed 42, `stop_after_microbatches: 1` and `14`.

## Peak footprint, 1 vs 14 microbatches

```
1 microbatch:  16.99s real, 9,008,431,104 B max RSS, 25,192,161,632 B peak footprint
14 microbatches: 32.13s real, 9,008,316,416 B max RSS, 25,895,149,992 B peak footprint
```

Losses at each step (`step step_ms loss`) matched the sibling
trainer-throughput log's framed-trainer run exactly (same seed, data, job
shape), confirming this checkout reproduces that trainer bit for bit:

```
1 2246 1.6272865533828735;2 1379 1.4147964715957642;3 1452 1.9195972681045532;4 1279 1.155087947845459;5 1030 0.7116259336471558;6 1382 0.6765822768211365;7 1260 0.2295519858598709;8 1521 1.1614408493041992;9 1026 0.923608124256134;10 1394 0.5738025903701782;11 1034 0.6766258478164673;12 1298 1.4977619647979736;13 1521 1.8385034799575806;14 1390 0.8050568103790283
```

Max RSS is identical (8.39 GiB) between the two runs; peak footprint grows
702,988,360 B (2.8%) from 1 to 14 microbatches. Step time stays in a 1.0-2.5 s
band with no widening trend (the 1st step is slowest in both runs: one-time
`training.Program.initFrozen` graph construction for that bucketed shape).
Conclusion: the small footprint growth tracks a later, larger bucketed
sequence length among the 14 shuffled training examples (bucketing rounds up
to 64 tokens; `Cache` holds exactly one compiled `Program` at a time, freeing
the old one before building a new one on a shape change), not an unbounded
per-step leak.

The ~15 GiB gap between max RSS (8.39 GiB) and peak footprint (23.5-24.1 GiB)
is Metal/unified device memory: shared-storage-mode Metal buffers and MPS
allocations are not attributed to the process's RSS sample the way ordinary
heap pages are, but they are real physical memory macOS's "peak memory
footprint" (`task_info`'s phys_footprint) does count. This matches the
motivating report ("wires ~15 GB of GPU/unified memory, while the process
RSS looks small") almost exactly.

## Reduction attempt

No additional safe reduction was found beyond what the trainer-throughput
work already did (device slices, device-resident gradients, one command
frame per forward/backward, the batched optimizer transaction, runtime
inputs uploaded once). Candidates considered and set aside:

- **Optimizer transaction snapshot-then-swap:** the dominant transient
  (weight/m/v snapshotted into new buffers before the old ones are freed,
  ~4.7 GiB at this model size) is the mechanism that keeps the transaction
  all-or-nothing (`seeded_gradient_trainer.zig:updateResident` frees the old
  device buffers only after every fallible step of `prepare` succeeds).
  Changing it needs a transaction-contract change (skip re-validating
  already-committed state, write AdamW out of place) that LAYA.md already
  flags as open; out of scope here per the task brief.
- **Per-step growth:** investigated above; explained by bucket-size
  variation, not a leak, so there is nothing to free between steps.
- **`Cache`'s compiled `Program`:** already holds only one program at a time
  (old one `deinit`'d before the new one is built), so no double residency
  across a bucket-shape change.
- **Trunk cache (`laya_trunk_cache.zig`):** serving-only; the training graph
  (`finetune/laya/graph.zig`) does not use it, so there is nothing to bound
  or evict here.

## Admission estimate derivation

`zig/pkg/inference/src/finetune/laya/job.zig`: `estimateBackendBytes` (called
from `execute` right after selecting trainable parameters, before any device
allocation). Components, evaluated for this job's config and a representative
~448-token bucketed packed row (batch 1, `freeze_layers: 0`):

```
weightStateBytes(394,723,328 trainable elements, 0 frozen)
  = 394,723,328 * 4 * 7               = 11,052,253,184 B  (10.30 GiB)
activationBytes(seq=448, batch=1, 28 fwd + 28 bwd layers)
  hidden: 448*1024*12*4 per layer     =     22,020,096 B
  mlp:    448*2624*2*4  per layer     =      9,404,416 B
  attn:   448^2*16*8*4  per layer     =    102,760,448 B
  per layer                            =    134,184,960 B
  * 56 layers                          =  7,514,357,760 B  (7.00 GiB)
fixed_backend_overhead_bytes                                4,294,967,296 B  (4.00 GiB)
---------------------------------------------------------------------
estimate                                                 22,861,578,240 B  (21.29 GiB, 22.86 GB)
```

Weight/optimizer-state arithmetic is exact, derived from
`seeded_device_transaction.zig`'s replacement contract (4x resident + 3x
transient on a stepped update). The activation term is an approximate upper
bound (twelve hidden-width and two GeGLU-width buffers per layer, plus eight
`[batch,heads,seq,seq]`-sized buffers for the three tree/padding biases,
forward scores/probabilities, and their backward cotangents); the 4 GiB fixed
term is calibrated, not derived, to close the gap to the measurement above:
device estimate (22.86 GB) plus an unmeasured few GB of host-side model
loading and dataset/tokenizer state plausibly accounts for the measured
25.2-25.9 GB combined peak footprint. `max_backend_bytes` defaults to 24 GiB,
just above this workload's estimate.

The estimate is intentionally not tight at longer sequences: activation cost
is quadratic in sequence length (dense, non-segment attention with no
gradient checkpoint in the training graph), so a packed row of ~1,024 tokens
already estimates to ~49 GB and is refused, well before
`architecture.validate`'s existing `batch*sequence^2*heads <= 64M`-element
bound would trip (that bound covers one attention tensor, not the sum of
buffers across every layer, and admits sequences past 1,024 tokens at batch
1/16 heads).

**Cross-check (coordinator-reported, 2026-09-26):** an unpacked run
(`s0-train`, `gradient_accumulation: 5`) showed a 17 GB process footprint in
`top` and ~20 GB of system wired memory. The estimate for that layout
(sequence 512, the full unpacked budget, same trainable-element count) is
24.9 GB, above the observed figures as expected: the estimate assumes every
microbatch is a stepped optimizer update (the default, `gradient_accumulation:
1`), so it charges the transaction's transient snapshot on every step, while
four of five microbatches in that run were accumulation-only and never paid
it.

## Verification

- `~/bin/zig ast-check` on the two changed files
  (`src/finetune/laya/job.zig`, `src/finetune/train/train_laya.zig`) after
  every edit.
- `~/bin/zig build test -- --test-filter "laya training job"` compiled the
  whole package (same dependency graph as the `antfly-inference` binary) with
  zero errors and passed the one matching pre-existing test, confirming the
  new admission code, `memory.AdmissionController` wiring, and new unit tests
  type-check.
- A second, broader `--test-filter laya` run (to execute the new unit tests
  and the existing Laya regression suite) was queued behind 8-12 other
  agents' `with-lock build` jobs on this shared machine and did not return
  within this session; it should be re-run before merge.
- New unit tests added to `job.zig`: backend-memory-ceiling validation
  bounds, `weightStateBytes`'s exact 7x/1x accounting, `activationBytes`
  monotonicity in layout size and reduction under `freeze_layers`, and
  `estimateBackendBytes` composing all of the above with the fixed overhead
  floor. These compiled cleanly but were not confirmed to pass at runtime
  before this report, for the same lock-contention reason.
