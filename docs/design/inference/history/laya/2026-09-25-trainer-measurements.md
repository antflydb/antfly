# Laya trainer throughput, half precision, LoRA, and memory measurements (2026-09-25/26)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1499–1767 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### Trainer throughput

Measured 2026-09-25 on the same machine: packed question mode, batch size 1,
14 microbatches of the step-0 training subset (`td/prof.json` from
`scripts/laya/prepare_laya_training_data.sh`), median step after two warm-up
steps, ReleaseFast.

| Trainer | Median step | Speedup |
| --- | ---: | ---: |
| Step-0 trainer (per-op synchronization, host slicing, gradient round trip) | 8.55 s | 1× |
| + strided slices on the device | 6.43 s | 1.3× |
| + gradients kept on the device (unframed) | 5.34 s | 1.6× |
| + one command frame per forward and per backward | 2.02 s | 4.2× |
| + one command batch per optimizer transaction | 1.68 s | 5.1× |
| + runtime inputs uploaded once; gradients handed over without a copy | **1.38 s** | **6.2×** |

Frozen layers, measured on the 2.02 s trainer:

| Trainer | Median step |
| --- | ---: |
| `freeze_layers: 11` (half the encoder) | 1.41 s |
| `freeze_layers: 18` | 1.17 s |

The first four losses are identical in every configuration without frozen
layers. Frozen runs share the first loss and then diverge, as expected.
Freezing saves less than its share of layers, because the forward and the
decision head still run in full.

- **Device slices:** strided slices of device tensors now run on the GPU
  (`slice_plan` on `decoderRuntimeSliceTypedDevice`) instead of downloading.
- **Gradients on the device:** the resident optimizer takes a read-only view
  of each dense device gradient (`resident_training.Request.adopt_f32`), with
  no copy. It falls back to download and upload only for host-backed
  gradients.
- **Optimizer batch:** the resident AdamW transaction
  (`seeded_device_transaction.prepare`) used to submit and wait after every
  snapshot, elementwise op and zero fill. It now owns one command batch
  (`ComputeBackend.residentTrainingBeginBatch`/`EndBatch`). Only the
  finiteness and norm reductions synchronize it, and it commits before the
  transaction returns. A failed or cancelled transaction discards the batch;
  anything already executed wrote only replacement buffers. Resident ops
  still refuse any frame they do not own.
- **Device inputs:** attention biases, RoPE tables, type masks and dropout
  masks are uploaded once per step. As host tensors, every op that read them
  uploaded them again, in every layer of both graphs. This was ~95% of the
  host-side encoding time.
- **Command frames:** `training.executeFramed` runs each forward and backward
  graph in one Metal command frame and synchronizes once at the end.
  `ANTFLY_LAYA_TRAIN_UNFRAMED=1` restores per-op submission.
- **Frozen lower layers:** `freeze_layers: N` in the job keeps the token
  embeddings and encoder layers `0..N-1` at their source values. They are not
  differentiated, have no optimizer state, and are exported unchanged. The
  backward graph stops at layer N. The forward still runs every layer.

Framing exposed a bug in the Metal runtime's in-frame buffer reuse. Buffers
freed during a frame went into a reuse pool whatever their storage mode, and a
later private allocation could receive a shared one. An upload into shared
storage is an immediate host copy, so the previous owner's still-queued GPU
writes landed on top of the new data when the frame ran. In the trainer this
corrupted the index arrays of the embedding-gradient scatter and produced
NaN updates. It showed up only in framed runs, and flushing after almost any
op hid it. The pool now takes private buffers only
(`metal_runtime.zig`, "metal in-frame buffer reuse never hands a
host-writable buffer to a private request").

What remains per step at 1.38 s: GPU work of ~0.13 s forward and ~0.34 s
backward, ~0.21 s in the optimizer transaction (mostly its snapshot copies
and full-state finiteness reads), ~0.07 s building inputs (dropout random
numbers on the host), and ~0.05 s encoding. Cutting the optimizer further
means changing the transaction contract: skip re-validating state the
previous commit already validated, and write AdamW out of place instead of
snapshotting weights and moments first.

Raw per-step logs and the investigation are in
[`docs/design/inference/history/laya/2026-09-25-trainer-throughput.md`](../../../../../docs/design/inference/history/laya/2026-09-25-trainer-throughput.md).

Segment attention in the training graph landed with step 2c (below). Batching still helps little, because cost is per token. LoRA is
implemented ([LoRA](../../../../../zig/pkg/inference/models/laya/LAYA.md#lora)); its own step-time, memory, and accuracy numbers
are below.

### Half-precision training (not adopted)

An opt-in f16 path (half operands with f32 accumulation through MPS for
every linear's forward and backward matmuls, f32 master weights and
optimizer state) was built and measured on 2026-09-26 and **not merged**:

- **Not faster:** 1.357 s against 1.312 s median step on `td/prof.json`
  (3-4% slower); the per-matmul casts cost more than the half GEMMs save.
- **Not correct at full depth:** worst relative gradient error over
  `encoder.layers.*` rose from 2.25% (f32) to 353% against float64 PyTorch,
  although each isolated half GEMM was within 0.1%. Without loss scaling the
  error compounds over ~340 matmuls per step.
- **Caching the half weights per step was unsafe:** keyed on buffer address,
  it served stale casts after Metal reused a freed activation's address
  within a frame. Half operands for the batched attention matmuls diverged
  within four steps.

A future attempt needs loss scaling or bf16, per-layer precision choices
validated against the float64 gradient check, and a weight cache keyed on
the binding rather than the buffer address. Branch
`laya/half-precision-training` keeps the attempt.

### LoRA throughput and accuracy

LoRA does not shrink the forward graph (every targeted linear still computes
its base `x·Wᵀ` beside the adapter's `scale·(x·Aᵀ)·Bᵀ`), so a step still runs
the full encoder and head plus a small extra matmul pair per targeted linear;
step time should be at or slightly above a full fine-tune's, not below it.
What it removes is optimizer state: a targeted linear's `[out, in]` weight and
its two AdamW moments drop out, replaced by `A` (`[rank, in]`) and `B`
(`[out, rank]`) and their own moments, which is smaller whenever
`rank < out·in/(out+in)`. At rank 16 on the released 421M checkpoint's largest
linears (`hidden 1024`, `mlp 6144`) that is decisively true, so the expected
win is optimizer-transaction bytes and peak memory, not wall time per step.

Measured 2026-09-26 on an Apple M4 Max, ReleaseFast, Metal trainer, released
`convaiinnovations/laya`, `targets: ["encoder","head"]`, `alpha` 32,
`dropout` 0.

**Step time and memory.** `td/prof.json`-style job (packed question mode,
batch size 1, 14 microbatches of the step-0 training subset), median step
after two warm-up steps, peak `/usr/bin/time -l` footprint. The full
fine-tune row is remeasured here (same job, same machine state) rather than
reusing the 1.38 s/step figure from [Trainer
throughput](#trainer-throughput), so the two rows are directly comparable:

| Trainer | Median step | Peak footprint |
| --- | ---: | ---: |
| Full fine-tune (remeasured) | 1.31 s | 25.88 GB |
| LoRA rank 16 | 1.23 s | 14.43 GB |

LoRA is not slower despite the extra adapter matmuls: it tracks far fewer
optimizer-managed parameters (two small `A`/`B` matrices per targeted linear
instead of the full `[out, in]` weight and its two AdamW moments), and the
optimizer transaction was ~15% of full fine-tune's step time (its own
[Trainer throughput](#trainer-throughput) breakdown), so the saving there
outweighs the small added forward/backward cost. Peak memory drops 44%
(1.79x).

**Accuracy (step 0, packed question mode).** Same recipe, data, and serving
evaluator as [Accuracy (step 0)](../../../../../zig/pkg/inference/models/laya/LAYA.md#accuracy-step-0) and [Run-to-run
variance](2026-09-24-packed-accuracy-qualification.md#run-to-run-variance), rank 16:

| Trainer | Seed 42 | Seed 43 | Seed 44 | Mean | SD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current trainer (full fine-tune) | 0.434 | 0.461 | 0.455 | 0.450 | 0.014 |
| LoRA rank 16 | 0.480 | 0.478 | 0.454 | 0.471 | 0.014 |

LoRA rank 16 scores at or above the full fine-tune baseline, with the same
seed-to-seed spread (SD 0.014 in both). It lands between the unfrozen
baseline (0.450) and `freeze_layers: 11` (0.512): training only a small
delta on top of frozen base weights plausibly regularizes this small
(400-case) recipe similarly to freezing lower layers, without freezing any
layer outright. Three seeds is still not enough to resolve a difference this
size against the 0.450 baseline ([Run-to-run
variance](2026-09-24-packed-accuracy-qualification.md#run-to-run-variance) makes the same point about the existing
rows), so read this as "not worse," not as a confirmed improvement. Rank 64
was not measured (time did not allow it in this session).

### Training memory

Measured 2026-09-25 on an Apple M4 Max (36 GiB), ReleaseFast, `backend: metal`,
`packing: question`, batch 1, seed 42, released `laya` (28 layers, hidden
1024, 16 heads, intermediate 2624, ~394.7M trainable parameters, 0 frozen
layers), the same step-0 subset as [Trainer throughput](#trainer-throughput).
`/usr/bin/time -l` around the whole process, one run stopped after 1
microbatch and one after 14:

| Microbatches | Max RSS | Peak footprint |
| --- | ---: | ---: |
| 1 | 8.39 GiB | 23.46 GiB |
| 14 | 8.39 GiB | 24.11 GiB |

RSS is flat; peak footprint (macOS's combined host-plus-device high-water
mark) grows about 703 MB (2.8%) from 1 to 14 microbatches. The ~15 GiB gap
between RSS and footprint is Metal/unified memory: shared-storage buffers a
kernel-level RSS sample does not attribute to the process the way ordinary
heap pages are. The small growth across steps tracks bucket-size variation
among shuffled training examples (`training.bucketedLayout` rounds each
batch's sequence up to 64 tokens; a later, longer example raises the
high-water mark), not an unbounded per-step leak: step time also stays in a
narrow 1.0-2.5 s band with no widening trend. No safe reduction was found
beyond what [Trainer throughput](#trainer-throughput) already did. The
resident AdamW transaction's weight/m/v snapshot-then-swap
(`seeded_device_transaction.prepare`) is the dominant transient, and changing
it is explicitly out of scope: it exists to keep the transaction all-or-
nothing, and cutting it needs a transaction-contract change (skip
re-validating already-committed state, write AdamW out of place) that is
still open.

**Admission.** The Laya job Config gained `max_backend_bytes` (default 32
GiB; an initial 24 GiB default refused the standard packed recipe, whose
estimate is above that and whose measured peak is ~25.9 GB), a
device/unified-memory ceiling alongside the existing `max_host_bytes`. A
refusal logs the estimate and the ceiling, and every Metal job logs its
estimate. The admission controller's live system-memory check is opt-in
(`check_live_memory`): it reserves serving headroom of a quarter of physical
memory on top of the request, so on a 36 GB laptop it refuses every full
Laya fine-tune (a ~25 GB job would need ~34 GB free). Run one training job at
a time on a small machine.
`execute` estimates the run's device need right after selecting trainable
parameters and before any device weight or optimizer-state allocation (well
before creating the output directory), and refuses with
`LayaBackendMemoryLimitExceeded` if the estimate exceeds the ceiling:

- **Weights and optimizer state (exact).** The resident AdamW transaction
  keeps weight, m, v, and the gradient accumulator resident (4x trainable f32
  bytes) and, on every optimizer step, additionally snapshots weight/m/v into
  new buffers before freeing the old ones, so the transient peak is 7x
  trainable bytes; frozen tensors add 1x with no optimizer state.
- **Activations (approximate).** Per encoder layer: about twelve hidden-width
  buffers, two GeGLU intermediate-width buffers, and eight dense
  `[batch, heads, seq, seq]`-sized buffers (three tree/padding biases, forward
  scores and softmax probabilities, and their backward cotangents), summed
  over the run's admitted layouts and over forward-pass layers plus
  backward-retained (unfrozen) layers.
- **Fixed overhead (measured).** 4 GiB for MPS kernel/temporary caches, the
  in-frame buffer reuse pool, per-step RoPE/bias/dropout runtime inputs, and
  Metal driver bookkeeping, calibrated against the run above.

For this run's layout (~448-token packed rows after bucketing, batch 1): the
estimate is **22.9 GB** (device only). Adding a few GB for host-side model
loading and dataset/tokenizer state (not separately isolated in this
measurement) lands close to the **25.2-25.9 GB** measured combined peak
footprint above, so the estimate is a reasonable, if approximate, stand-in for
the real number. It is not tight: a training graph with dense (non-segment,
non-checkpointed) attention scales the activation term with `sequence^2`, so a
longer packed row than this workload's costs much more (the estimate exceeds
`max_backend_bytes` well before `architecture.validate`'s existing
`batch*sequence^2*heads <= 64M`-element bound would, which only bounds one
attention tensor, not the sum of buffers across every layer).

A separate, unpacked run (`s0-train`, `gradient_accumulation: 5`, so only one
in five microbatches is a stepped optimizer update) showed a 17 GB process
footprint in `top` and ~20 GB of system wired memory. The estimate for that
layout (sequence 512, the full unpacked budget) is 24.9 GB: higher than
observed, as expected, since the estimate assumes every microbatch is a
stepped update (worst case, matching the default `gradient_accumulation: 1`)
while most of that run's microbatches were accumulation-only and never paid
the transaction's transient snapshot.

**Process-wide admission.** `execute` also acquires a lease from the
inference runtime's `memory.AdmissionController` (`.gpu` backend class for
`backend: metal`, `.cpu` otherwise), charging the device estimate as
`backend_scratch_bytes` and enabling the controller's live-memory check only
for Metal. On macOS, `AdmissionController` charges backend bytes to the same
live system-memory sample as host bytes (`memory.zig`'s `liveHostBytes`,
because Metal draws from unified memory), so this check is real cross-process
protection: two concurrent `antfly-inference finetune train laya` processes
each request their own device estimate against one shared, live view of
system memory, and the second is refused once the two would not fit, rather
than both proceeding into OOM. `max_host_bytes` is deliberately not also
charged to this live sample: it is an existing generous safety-net ceiling on
allocator growth (enforced independently by `BoundedAllocator`), not a
measurement of actual usage, and charging the full default there as well
would make a single default job's own declared envelope exceed most machines.
The standalone CLI (`train_laya.zig`) creates one `AdmissionController` per
process; a caller embedding the trainer in a longer-lived process (for
example, alongside serving sessions) can share its own controller instead so
resident models and training compete for the same ledger.

The final `report.json` records `backend_estimated_bytes` (the admission
estimate) and `backend_peak_bytes` (the Metal runtime's own
`device_owned_peak_live_bytes`, reset at admission so it reflects this run
alone), alongside the existing `host_peak_bytes`.

Segment attention in the training graph landed on CPU (step 2c, see
[Long states](../../../../../zig/pkg/inference/models/laya/LAYA.md#long-states-step-2c)); a device kernel remains open (LoRA is implemented, see [LoRA](../../../../../zig/pkg/inference/models/laya/LAYA.md#lora)).
Batching still helps little, because cost is per token.
