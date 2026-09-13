# Inactive adapter training oracle

`capture_training_inactive_adapters.py` captures the pinned Fastino trainer's
behavior when a valid microbatch has no live call to a selected adapter. It
uses the immutable tiny mixed-step baseline, not a published checkpoint or a
corpus. This source fixture is separate from the trained-artifact inference
proof and does not qualify native updates by itself.

The important distinction is gradient **presence**. The pinned
[`_backward_one`](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/trainer.py#L1182)
uses `_zero_loss` when the total loss is absent or does not require gradients.
For a non-differentiable loss it replaces the reported objective with a zero
term touching every trainable parameter. The model's
[`_head_touch` and `_zero_loss`](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/boundary/model.py#L1099)
also explicitly touch enabled optional record/relation heads. Therefore:

| Selected adapters; current task | Reported loss | Selected gradient presence |
|---|---|---|
| Classifier only; entity-only batch | Zero, although the frozen entity objective is positive | Every selected A/B/magnitude slot is present zero |
| Record or relation only; entity-only batch | Original positive frozen-task objective | Optional-head slots are present zero |
| Encoder and classifier; entity-only batch | Original positive objective through the live encoder | Encoder slots are present; classifier slots remain `None` |

Successful inactive batches still count toward accumulation. Present zero
slots participate in AdamW: weight decay, moment decay and slot-step counters
advance at a flush; the trainer advances its scheduler/global step. `None`
slots stay absent for that microbatch. They can still have an accumulated
gradient from an earlier active microbatch in the same window. Partial-window
renormalization counts all accepted microbatches, including inactive ones.

The generator compiles seven exact AST-selected source trainer methods,
preserving their decorators and calls: backward, finite-gradient hooks,
optimizer construction, optimizer update, partial-window correction, and both
boundary loss/injection schedules. It does not rewrite fallback or AdamW math.
The versioned contract binds each method hash, the clean source commit,
unchanged oracle helper and four tiny baseline files.

## Fixture scope

The baseline is H=16, two DeBERTa layers, vocabulary 192, with 66,724 bytes of
original parameters. Encoder, head and PEFT dropout are zero. Inputs are one
short document, at most 128 encoded tokens, eight words and eight queries.
Each mode uses rank 2, alpha 3, constant learning rate `5e-4`, AdamW weight
decay `0.01`, accumulation two and clipping norm `0.7` in the exact pinned
Torch/PEFT environment.

Eight profiles cover LoRA and DoRA with four selections. Classifier-only runs
active → inactive → active, flushes the partial epoch, then runs an additional
two-inactive window after moments have been learned. Flush sizes are `[2,1,2]`.
A fresh model/optimizer owner restores the unfinished first window and exactly
reproduces the remaining state. Record-only and relation-only profiles each
flush one inactive microbatch. Encoder-plus-classifier profiles run an active
classification batch followed by an entity-only batch.

The capture records the model objective and its `requires_grad` state, reported
loss, every selected slot's scaled/unscaled/accumulated gradient or `None`,
prepared token/routing tensors, and every flush's weights, moments, per-slot
steps, global/scheduler counters and gradient norm. Unscaled gradients are an
explicit factor-two reversal of the source backward hook's configured loss
division. Source parameter names and exact shapes are retained. The baseline
parameters are included once; tokenizer files remain pinned references to the
existing tiny fixture.

The four mixed-step, dropout, and inactive captures store exact repeated input
bindings, encoder configuration, initial bindings, and dropout maps in local
`shared_metadata` tables. References are zero-based indices into typed tables
limited to 64 entries each. Readers reject missing, ambiguous, and out-of-range
references before running numerical checks. Tensor names, scalar expectations,
`None` gradients, mask values and resume assertions retain their original
meaning; no metadata reference crosses a capture file.

Fresh-owner resume compares full model parameters, gradient presence and
accumulators, optimizer moments/state membership, scheduler/counters and RNG
bytes. This is an owned in-memory source snapshot. Durable native checkpoint
publication and native CPU/Metal numerical consumers are separate proofs.

## Capture and evidence

Metadata-only validation does not import a numerical runtime:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 \
  zig/pkg/inference/scripts/gliner25/capture_training_inactive_adapters.py \
  --preflight-only
```

An explicitly scheduled serial numerical capture uses a fresh directory:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/capture_training_inactive_adapters.py \
  --upstream /private/tmp/antfly-gliner25-upstream \
  --output-dir /private/tmp/gliner25-training-inactive-adapters-new
```

The actual probes ran under an outer 180-second process-group supervisor,
2 GiB child-tree RSS ceiling, 16 MiB artifact ceiling and 4 MiB combined log
ceiling. TERM/KILL/reap completed before releasing the compute lane. The
generator also checks 120 seconds cooperatively around its tiny forwards,
2 GiB RSS, 8 MiB tensor data and 4 MiB JSON metadata. Output publication uses
an exclusive atomic directory. A first sandbox attempt failed to inspect
process-tree RSS before model execution; its child was reaped and its receipt
is preserved at `/private/tmp/gliner25-training-inactive-adapters-probe-v1/`.
No guard or numerical policy was removed. The successful v2 probe used the
authorized process-inspection context; a deliberate v3 repeat produced both
fixture files byte-identically.

| Probe | Duration | Peak child RSS | Result |
|---|---:|---:|---|
| `...inactive-adapters-probe-v2/process.json` | 4.04 s | 494,469,120 B | Eight profiles, exit zero, reaped |
| `...inactive-adapters-probe-v3/process.json` | 2.80 s | 492,027,904 B | Byte-identical repeat, exit zero, reaped |

The external files restore under `testdata/gliner25/training_inactive_adapters/`:

| File | Bytes | SHA-256 |
|---|---:|---|
| `capture.json` | 420,293 | `d0d5f8dbbd39446cf8050021623f2eda7db6992447bd65029970e93a3a1eff53` |
| `tensors.safetensors` | 207,375 | `9394123bccbaa959e6aa2b4a32905208e3f2df5917626824edff7af741e0192a` |

The generator SHA-256 is
`8f63e2ccb46fcf2ee00033ea3577c02615d59c07b233da7ed9303de0fea45380`;
the v1 contract SHA-256 is
`d88ece37ce0873baf481d957a4e30c53b5fb35e5780ea542dc360931d5e810cd`.
Pure Python tests verify bounded tensor headers, provenance and the recorded
presence/objective/counter distinctions without a model import. Supplemental
native CPU and resident Metal `Controller` consumers now pass this exact
control fixture, including its `[2,3,5]` flush schedule. They replay source
cotangents to qualify accumulation and optimizer ownership; the complete
native forward/backward/update proof uses the companion below. All source,
baseline numerical values and expected results remain unchanged. See the
[fixture metadata policy](../../testdata/gliner25/README.md) for exact tensor deduplication
and updated writer pins.

## Native epoch companion

`capture_training_inactive_native_epoch.py` is an additive companion. The
original control profile and captured tensor bytes above remain fixed.
An ordinary immutable five-row native training epoch with accumulation two
flushes after rows `[2,4,5]`; the control fixture's `[2,3,5]` schedule represents
a three-row partial epoch followed by a separate inactive window. Comparing
those different schedules would compare different optimizer updates.

The new `training_inactive_native_epoch_contract_v1.json` preserves every
source, baseline, optimizer and model setting, changing only the classifier
flush positions to `[2,4,5]`. Its window sizes are `[2,2,1]`, ending with one
wholly inactive partial window. The other six mode/target profiles already
follow their ordinary native epoch schedule and remain unchanged. A private
module owner binds this declared profile to the byte-pinned control helper;
it does not replace source trainer or optimizer equations. Output records
both generator identities and the profile change explicitly.

Input-only preparation emits four version-1 dataset JSONL files plus a
`native_settings.json` binding recipe. Every row includes its fixed schema,
original `Ada.` text, explicit byte span `Ada` at `[0,3)`, and supervised
classification labels only on the active rows. The classifier dataset is
`active_good, inactive, active_bad, inactive, inactive`, with unique stable
row IDs. Native settings use one epoch, batch size one, no shuffle, constant
schedule and no warmup. The consumer must copy only the captured initial
A/B/magnitude parameters before microbatch zero; it must not inject source
post-update parameters or alter the native cursor. The JSON binding recipe
is synthetic test configuration, not a published-model job manifest.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 \
  zig/pkg/inference/scripts/gliner25/capture_training_inactive_native_epoch.py \
  --preflight-only

PYTHONDONTWRITEBYTECODE=1 python3 \
  zig/pkg/inference/scripts/gliner25/capture_training_inactive_native_epoch.py \
  --inputs-only --output-dir /private/tmp/gliner25-inactive-native-inputs-new
```

The companion numerical capture and its deliberate repeat both passed under
the same scheduled outer process/RSS/output guards. V1 completed in 4.21 s
with 494,944,256 bytes peak child-tree RSS; v2 completed in 2.99 s with
503,988,224 bytes. Both exited zero, were reaped, and emitted no stderr. Every
one of their seven output files is byte-identical. Process receipts are under
`/private/tmp/gliner25-training-inactive-native-epoch-probe-v{1,2}/process.json`.

The small input files remain checked in; the external capture and tensors restore under
`testdata/gliner25/training_inactive_native_epoch/`, totaling 634,237 bytes.
The 423,554-byte `capture.json` has SHA-256
`24cefafa4dde1f7067b3ba81e7b9209495a58a0cb31620c5805b00bec9abdc24`;
the 207,343-byte tensor file has SHA-256
`eb8d7939308642c9587c73789070516e632078850473feecd3416f57d665246c`.
Capture metadata binds each exact JSONL/settings file. Its wrapper SHA-256 is
`7a38842add551cad17397fcaefb8f3b2bb00ecc163afb8afb72a1a66c0fd9944`
and companion contract SHA-256 is
`b901882abbb3842d6f3eda9554b583e78b686411e218acca3499262b4e2516d8`.

Six additional pure tests verify profile isolation, exact JSONL/schema/
annotation mapping, tamper rejection, atomic publication, fixture provenance,
and the captured schedule/presence/state distinction. Independent comparison
also confirms all six non-classifier profile reports and 1,552 unrelated or
initial tensors remain byte-identical to the original control fixture. Both
classifier modes reproduce the fresh-owner source resume exactly. Native
CPU and resident Metal `NativeTrainer` consumers now pass all eight companion
profiles. They compare exact prepared IDs and routing tensors, model and
fallback objectives, every global gradient's `None`/zero/value presence,
numerical VJPs, AdamW state after native updates, observer-failure retry and
durable fresh-owner resume. The ordinary native `[2,4,5]` epoch cursor remains
in control throughout.

The constructor used only by compiled tests admits the tiny H=16 source
layout and copies captured initial A/B/magnitude slots before microbatch zero.
It does not relax published inventories, expose a public configuration seam,
or inject source post-update parameters. Comparisons retain the established
mixed-step tolerances: losses `1e-3 + 2e-5*abs(reference)`, VJPs
`5e-4 + 8e-4*abs(reference)`, and updated weights
`2e-5 + 2e-5*abs(reference)`. Gradient presence, exact zero states and
same-backend fresh-owner resume remain exact.

The focused CPU/Metal source, supplemental Controller, synthetic-descriptor,
tape and managed-owner checkpoint passed **17/17 tests, zero skips** in
`/private/tmp/gliner25-source-socket-metal-v1.log`, SHA-256
`7960df3ce368de2937e397b819f4e5f239845287fa8b422ff7b3b8c3cbf8481b`.
This establishes the declared tiny inactive-adapter update behavior. The
separate published classifier CLI evidence below does not extend these source
numerical comparisons to a published checkpoint. Convergence and release
qualification remain open requirements.

## Published-artifact scope

Separate local published-small classifier-only LoRA/DoRA jobs exercised the
ordinary CPU and resident-Metal CLI with five active/inactive microbatches,
three updates and exact fresh-owner resume within each backend. Their final
adapters also underwent strict upstream CPU reload and ten fixed requests.
Those process receipts and output artifacts are campaign data, not fixtures.

The source fixtures above remain pinned for restoration as the numerical regression
contract. Published restart/reload consistency does not establish published
Fastino VJP parity, equality of CPU/Metal updates, broader ranks/backbones,
full-context training, useful trained quality or release readiness. See
[training](../../../../../work-log/completed/gliner2.5.md#training-and-export) and
[export compatibility](../../../../../work-log/completed/gliner2.5.md#export-and-adapter-materialization).
