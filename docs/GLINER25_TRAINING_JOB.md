# GLiNER2.5 native training jobs

The version-1 job owns source loading, complete dataset preflight, CPU or
resident Metal training, durable checkpointing and final model/adapter export. It accepts the
small, base and multilingual boundary architectures with original FP32
weights. The four training modes are `full`, `heads`, `lora` and `dora`.
`execution` defaults to `native`; `resident_metal` requires a build with Metal
support and the strict resident training backend. Quantized training is rejected.
Tiny managed Metal full/head jobs and composed training steps have focused
validation; published-model Metal job qualification remains open.

The top-level JSON profiles are independent: `attention_profile` defaults to
`"materialized_v1"` and accepts `"replay_tiled_v1"`; `activation_profile`
defaults to `"retained_v1"` and accepts `"layer_recompute_v1"`. Both are part
of durable run identity. Omitting them preserves the retained, materialized
path used by the earlier published training evidence. The
[regional training guide](GLINER25_RECOMPUTED_TRAINING.md) describes the separate
CPU/Metal fixture evidence and the completed published-small native all-target
LoRA/DoRA campaign. That campaign uses both opt-in profiles with the ordinary
production constructor and proves five-microbatch/three-update partial resume:
final result, checkpoint and four adapter-export files are byte-exact against
uninterrupted execution. It does not qualify published regional Metal or
published-model PyTorch numerical parity.

The [recorded native jobs](../zig/pkg/inference/testdata/gliner25/recomputed_training_published_small_execution_v1/README.md)
retain explicit 768 MiB host, 1 GiB backend, 3 GiB combined and 32 MiB optimizer
transaction caps. An earlier LoRA resume failed cleanly because restore charged
the default 64 MiB header-parser ceiling before reading the checkpoint. The
successful revision reserves staged state plus the actual immutable snapshot,
then clamps parser capacity to the transaction remainder. All job memory caps
remain unchanged; the ledger preserves both the failure and all six successful
LoRA/DoRA phases. These are short authored examples, not a full-context or
performance qualification.

The attempted published-small Metal head job was denied by live-memory
admission before source loading: it requested 2,216,519,709 bytes against a
current capacity of 601,295,421 bytes under the dynamic macOS pressure policy.
No model training ran and no allocation leaked. This is a resource-admission
failure, not evidence of model numerical failure or successful GPU training.
The log is `/private/tmp/gliner25-real-small-training-job-metal-v1.log`, SHA-256
`b1d30c377b06950db5e0966fbf66ee5527a38998960334d14799b8aa65b4c3c0`.

The source owner, native runner, CLI and exporter are implemented. All three
published source snapshots and the tiny composed CPU training profiles have
focused validation evidence. The real-small head-only library job passed four
microbatches/two optimizer updates, pause-after-one/resume, exact final state
and model agreement, all 334 exported tensors and the four unchanged sidecars.
A persistent native CLI job then completed with identical final state/model
hashes; its export passed strict pinned Fastino loading and all ten bounded
task requests. A fresh supervised CPU CLI also passed pause-after-one/resume
with the same final hashes and every redirected stdout event retained. See
[training evidence](GLINER25_TRAINING.md) and [release gates](GLINER25_IMPLEMENTATION.md).

That successful four-row integration probe produced model SHA-256
`ed1c88487e6b1936f0586e3868c4b944af9707b5a31ea0834156f60d354cefd7`
and canonical optimizer-state SHA-256
`508c7369ee604274321ae5666c88d57eb9dbae1efe881118b96ff86dd04bdf3c`.
These identities belong to that probe, not arbitrary runs of the example below.
The test process exited successfully with four selected tests passing and no
skips; its log is `/private/tmp/gliner25-real-small-training-job-v2.log`, SHA-256
`93b5add68241adc4736d85ad818199dbe2dfb137524fe1a0b342f30e65628e0f`.
The persistent CLI output is
`/private/tmp/antfly-gliner25-training-cli-v1/uninterrupted`, produced by binary
SHA-256 `aee7147d1724b093768dbaba2a2e992d42b29c3e09ef8fc3e1f1cbe7275956a0`.
The upstream checker report is
`/private/tmp/gliner25-training-export-heads-check-v1/report.json`, SHA-256
`5692fd22c94069de7d96135668705e0dca70a9ca79810921dc60f03ebff3e841`.
This proves this head-only artifact loads and runs; same-trained-artifact
native/Python output parity, extraction quality and convergence remain open.

## A concrete bounded job

Save the following JSON as `/private/tmp/gliner25-heads-job-v1.json`. Paths are
absolute and refer to the current checkout and previously downloaded pinned
small model. The output directory must not exist; its parent must exist.
Every row already carries the complete schema described in
[the JSONL contract](GLINER25_TRAINING_DATA.md).

```json
{
  "version": 1,
  "source_dir": "/private/tmp/antfly-gliner25-models/small",
  "train_file": "/Users/timkaye/Documents/af/antfly/zig/pkg/inference/testdata/gliner25/training_job_small_v1/train.jsonl",
  "calibration_file": "/Users/timkaye/Documents/af/antfly/zig/pkg/inference/testdata/gliner25/training_job_small_v1/validation.jsonl",
  "output_dir": "/private/tmp/gliner25-heads-paused-v1",
  "expected_source": {
    "backbone": "small",
    "precision": "fp32",
    "weight": {
      "size_bytes": 295567700,
      "sha256": "4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b"
    },
    "sidecars": [
      {"size_bytes": 3152, "sha256": "0b7d9e1401ceeb83e992ec66d2f93bff7e5646428f1b4706ec527cf88f53578a"},
      {"size_bytes": 856, "sha256": "db837d0dc587f5858687ef860c1f400de10f3c3e44f88daef8cbda80d74e4c9c"},
      {"size_bytes": 8341713, "sha256": "cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3"},
      {"size_bytes": 645, "sha256": "0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1"}
    ]
  },
  "run": {
    "mode": "heads",
    "epochs": 2,
    "batch_size": 1,
    "accumulation": 2,
    "encoder_lr": 0.00001,
    "task_lr": 0.0005,
    "weight_decay": 0.01,
    "scheduler": "constant",
    "warmup_steps": 0,
    "seed": 42,
    "shuffle": false
  },
  "tokenization": {
    "max_text_words": 128,
    "max_sequence_tokens": 512,
    "max_queries": 64,
    "word_splitter": "whitespace"
  },
  "checkpoint_every_microbatches": 1,
  "timeout_seconds": 3600
}
```

The sidecar order is `config.json`, `encoder_config/config.json`,
`tokenizer.json`, `tokenizer_config.json`. These are the small revision
`cab1bddfd30fda7b803a4691c41f90378a2d517a` pins in
[the oracle manifest](../zig/pkg/inference/scripts/gliner25/oracle_manifest.json).
For another variant, use its complete identity and source directory together.
The job performs no downloads. Without `expected_source`, it still validates
the architecture and records consumed hashes, but does not require a specific
externally approved checkpoint.

This fixture has two training examples and two validation examples. The latter
are supplied as `calibration_file` only to exercise split ownership/preflight;
the job does not calibrate anything. The complete two-epoch run should consume
four microbatches and perform two optimizer updates. These authored rows test
plumbing, not model quality. Their exact bytes and limitations are recorded in
[the fixture manifest](../zig/pkg/inference/testdata/gliner25/training_job_small_v1/manifest.json).

The package build graph compiles and runs the dedicated command. Running from
the package makes the requested optimization mode and single build job apply
to its dependency graph:

```sh
cd /Users/timkaye/Documents/af/antfly/zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build train-gliner25 -Doptimize=ReleaseFast -Dmetal=false -Dcuda=false -j1 -- /private/tmp/gliner25-heads-job-v1.json --stop-after-microbatches 1
```

The command is also registered as `antfly-inference finetune train gliner25 <job.json>`
(or `antfly inference finetune train gliner25 <job.json>` through the parent
CLI). The top-level Zig graph exposes `inference-train-gliner25`, explicitly
delegated with `ReleaseFast` and `-j1` for the whole package:

```sh
cd /Users/timkaye/Documents/af/antfly/zig
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build inference-train-gliner25 -Dmetal=false -Dcuda=false -j1 -- /private/tmp/gliner25-heads-job-v1.json --stop-after-microbatches 1
```

Choose one invocation route; running both against the same output is rejected.
`train-gliner25` is a build-and-run target, not an installed executable target.
Serial execution is appropriate on the development machine; do not overlap
this job with model benchmarks or GPU training probes.

The job JSON is limited to 64 KiB and nesting depth 32. Unknown fields fail.
All artifact/data/output paths must be absolute, contain no `.`/`..` path
components or control characters, and fit the platform path limit. There is
no `backend` field; select `execution: "resident_metal"` explicitly for Metal.
Library supervisors can use `loadConfigSnapshot` to bind the raw SHA-256 and
size of the exact bounded bytes parsed by each parent/worker. Parsed strings
remain owned by that snapshot even if the configuration path changes.

## Modes and schedules

| Mode | Mutable parameters | Final `model/` artifact |
| --- | --- | --- |
| `full` | Original encoder and task parameters; unused parameters preserve absent-gradient behavior | Complete 334-tensor FP32 model plus original four sidecars |
| `heads` | Original task parameters; the outer `model.encoder` is frozen | Complete 334-tensor FP32 model, with frozen encoder bytes preserved |
| `lora` | Complete resolved A/B adapter layout; original source weights are frozen | Standard PEFT adapter weights/config plus provenance |
| `dora` | Complete resolved A/B/magnitude layout; original source weights are frozen | Standard PEFT DoRA adapter weights/config plus provenance |

For adapter jobs, set the matching `run.mode` and add this top-level object
(change both modes to `dora` for DoRA):

```json
{
  "peft": {
    "kind": "lora",
    "mode": "train",
    "rank": 8,
    "alpha": 16,
    "dropout": 0.125,
    "targets": ["encoder"]
  }
}
```

Targets resolve against the complete model once. Supported aliases include
`encoder`, `encoder.query`, `encoder.key`, `encoder.value`, `encoder.dense`,
`all_task_heads`, `extractive_head`, `classification_head`, `record_head` and
`relation_head`; exact supported biased Linear module paths are also accepted.
The resolved layout is fingerprinted. Do not set
`run.adapter_config_sha256`: the job derives it from that verified layout.

The optimizer retains the pinned upstream grouping: for ordinary full/head
training, any canonical parameter name containing `encoder` receives
`encoder_lr`, including `boundary_encoder` and `candidate_encoder` task
modules. Head-only freezing separately matches the outer `encoder.` prefix.
Adapter parameters use `task_lr`. Biases and normalization parameters remain
in their source-compatible weight-decay groups.

Schedulers are `linear`, `cosine`, `cosine_restarts` and `constant`.
`max_optimizer_steps`, when supplied, sets the optimizer horizon; otherwise it
is derived from epochs, batch size and accumulation. Partial accumulation
flushes at each epoch boundary using the actual number of contributing
microbatches. Batch size, accumulation, schedule, seed, order, tokenizer,
capacity and objective choices bind resume identity. Pool/gold-capacity
overrides must match the serialized head; they cannot silently reduce targets.

## Admission and ownership

Let `S` be the source reservation, `J` the job envelope, `D` one dataset's
configured host ceiling, `N` the number of declared splits, `H` the trainer
host ceiling, `B` its backend ceiling, and `M` its backend metadata ceiling.
Admission reserves separate host and device maxima:

```text
preflight = S + J + N * D
training  = S + J + D + H + B

native.host = max(preflight, training)
native.device = 0

resident.host = max(preflight, training - B + M)
resident.device = B - M

admitted = host + device
```

Metal requires `0 < M < B`. In a preflight-dominated run, the resident lease
still reserves both all host dataset snapshots and the later device maximum;
device capacity cannot cover those host snapshots. Every sum/product uses
checked arithmetic. The reservation must fit `memory.combined_bytes` and the
process-wide admission controller's current physical-memory policy.
Calibration/test snapshots
are released after preflight, before mutable trainer construction; their
lifetimes are not added to the optimizer phase. The CLI owns one admission
controller. Embedded callers must use their process-wide controller and retain
the lease until source, dataset, trainer and export teardown complete.

| Default ceiling | Value and scope |
| --- | --- |
| `source_limits.max_source_bytes` | 2 GiB; owned file snapshots, tokenizer, metadata and validation scratch. Reservation uses actual file sizes plus the auxiliary allowance, capped by this limit. |
| `dataset_limits.max_host_bytes` | 512 MiB per declared dataset; default raw file limit 256 MiB and row limit 2 MiB |
| `memory.job_bytes` | 128 MiB for orchestration, receipts and export scratch |
| `memory.host_bytes` | 6 GiB minus 128 MiB for trainer host ownership |
| `memory.backend_bytes` | 4 GiB for backend tensor payloads and metadata |
| `memory.backend_metadata_bytes` | 128 MiB host metadata sublimit; separated from resident device payload admission |
| `memory.optimizer_state_bytes` | 4 GiB optimizer-state sublimit; resident initialization counts four host mirrors, four device tensors and upload staging |
| `memory.optimizer_transaction_bytes` | 4 GiB transaction/restore sublimit |
| `memory.combined_bytes` | 12 GiB conservative total admission ceiling |

These are enforceable allocation ceilings, not measured RSS or a guarantee
that every model/profile fits. Full-parameter training, larger batches and
longer schemas can require different explicitly reviewed budgets. The job
rejects insufficient admission or inner budgets; it does not switch backend,
precision, schema or supervision to fit. The source reservation must include
its tokenizer and alignment/metadata costs, not just model file size.

### Explicit training resource limits

The optional `training_limits` object has its own version, currently `1`.
Omitting it or any child field preserves the existing limit; a larger memory
reservation does not automatically raise an inner limit. Values must be
positive JSON integers. Unknown fields, quoted numbers, floating-point values,
negative/zero values, overflow and values above the versioned ceilings fail.
No execution mode, dtype, dropout, loss, proposal or matching control is
accepted inside this object.

**`encoder.max_forward_tensor_bytes` is a conservative sum of logical forward
intermediate sizes across all layers. It is not peak device memory or RSS.**
The default encoder uses materialized training attention with explicit
replayable dropout. The separate `replay_tiled_v1` training profile uses its
dedicated forward/backward primitive; inference's tiled attention does not
qualify it. Encoder admission, differentiated graph/tape admission, actual
resident lifetimes, host allocation ceilings and process-wide reservations
are separate checks.

Regional activation replay must still fit the explicit `memory.host_bytes`,
`memory.backend_bytes`, `memory.optimizer_transaction_bytes` and
`memory.combined_bytes` ceilings. Its aggregate additionally reserves caller
batch scratch, the largest local tape, head execution and pending optimizer
updates. The native API's caller-scratch cap defaults to 512 MiB and is
reserved in full; the tiny regional test uses 16 MiB. These reservations are
allocation limits, not measured peak savings. See the
[regional admission contract](GLINER25_RECOMPUTED_TRAINING.md#admission-contract).
The new caller-scratch and `step.recomputation`/`step.replay` JSON mappings,
including the 6 GiB regional aggregate host default, were added after the
passing native integration-v3 checkpoint. Their seven numeric-wire tests and
job mapping test passed in the later 43-test Metal-enabled integration run.

For example, this fragment explicitly raises selected limits that otherwise
reject full base/multilingual gradients. It is not a complete hardware profile
or a claim that the default memory reservation will admit that training job:

```json
{
  "training_limits": {
    "version": 1,
    "encoder": {"max_forward_tensor_bytes": 17179869184},
    "differentiation": {"max_tape_bytes": 2147483648},
    "resident": {
      "program": {
        "max_working_bytes": 2147483648,
        "max_capture_bytes": 2147483648,
        "instruction": {
          "primitive": {"max_scatter_work": 268435456}
        }
      }
    }
  }
}
```

Here are the relevant default and hard ceilings. All byte units are binary;
JSON uses their integer byte counts. The complete numeric contract and every
field's hard ceiling are in
[`gliner_boundary_training_limits.zig`](../zig/pkg/inference/src/finetune/gliner_boundary_training_limits.zig).

| Field under `training_limits` | Default | Version-1 hard ceiling |
| --- | --- | --- |
| `max_recomputed_batch_scratch_bytes` | 512 MiB | 8 GiB |
| `encoder.input.max_batch` | 8 | 64 |
| `encoder.input.max_sequence_tokens` | 16,384 | 16,384 |
| `encoder.input.max_batch_tokens` | 65,536 | 262,144 |
| `encoder.input.max_text_words` | 8,192 | 8,192 |
| `encoder.input.max_queries` | 256 | 1,024 |
| `encoder.input.max_classification_labels` | 512 | 4,096 |
| `encoder.input.max_groups`, `max_relations` | 64 each | 256 each |
| `encoder.input.max_encoder_output_bytes`, `max_routed_bytes` | 128 MiB each | 2 GiB each |
| `encoder.max_attention_score_elements` | 67,108,864 | 268,435,456 |
| `encoder.max_forward_tensor_bytes` | 4 GiB logical sum | 1 TiB logical sum |
| `encoder.max_dropout_mask_bytes` | 512 MiB | 8 GiB |
| `encoder.max_constant_bytes` | 512 MiB | 2 GiB |
| `head_graph.max_nodes` | 100,000 | 1,000,000 |
| `head_graph.max_tensor_elements` | 67,108,864 | 268,435,456 |
| `head_graph.max_graph_elements` | 536,870,912 | 8,589,934,592 |
| `head_graph.max_constant_bytes` | 64 MiB | 2 GiB |
| `head_graph.max_attention_elements` | 16,777,216 | 268,435,456 |
| `step.max_step_host_bytes`, `max_total_host_bytes` | 512 MiB / 2 GiB | 8 GiB / 32 GiB |
| `step.max_step_work` | 524,288,000 | 1,099,511,627,776 |
| `step.transfers.max_upload_bytes`, `max_readback_bytes` | 256 MiB / 128 MiB | 8 GiB / 1 GiB |
| `step.recomputation.max_regions`, `max_source_nodes` | 64 / 1,000,000 | 256 / 1,000,000 |
| `step.recomputation.max_plan_host_bytes`, `max_compile_bytes` | 256 MiB / 1 GiB | 8 GiB each |
| `step.recomputation.max_checkpoint_bytes`, `max_gradient_bytes` | 256 MiB / 1 GiB | 16 GiB each |
| `step.recomputation.max_backend_bytes`, `max_host_bytes` | 8 GiB / 6 GiB | 128 GiB / 32 GiB |
| `step.recomputation.max_total_work` | 2^42 bounded work items | 2^48 |
| `step.replay.max_source_nodes`, `max_recipes` | 1,000,000 / 16,384 | Same |
| `step.replay.max_candidates`, `max_name_bytes` | 65,536 / 1,024 | Same |
| `step.replay.max_host_bytes`, `max_mask_bytes` | 64 MiB each | 1 GiB each |
| `step.replay.max_explicit_bytes` | 512 MiB | 1 GiB |
| `step.replay.max_work_items` | 2^28 | 2^30 |
| `differentiation.max_tape_bytes`, `max_cotangent_bytes` | 256 MiB / 64 MiB | 16 GiB / 1 GiB |
| `differentiation.gradient.max_forward_nodes`, `max_gradient_nodes` | 1,000,000 / 4,000,000 | Same |
| `resident.max_compile_bytes` | 1 GiB across programs | 8 GiB |
| `resident.max_device_bytes` | 8 GiB | 128 GiB |
| `resident.max_host_metadata_bytes` | 256 MiB | 2 GiB |
| `resident.program.max_source_nodes`, `max_program_nodes` | 262,144 / 131,072 | 1,000,000 each |
| `resident.program.max_outputs` | 4,096 | 4,096 |
| `resident.program.max_compile_bytes`, `max_constant_bytes` | 256 MiB / 64 MiB | 4 GiB / 2 GiB |
| `resident.program.max_binding_bytes`, `max_working_bytes` | 8 GiB / 512 MiB | 64 GiB / 32 GiB |
| `resident.program.max_capture_bytes` | 1 GiB aggregate snapshots | 16 GiB |
| `resident.program.max_device_bytes`, `max_host_metadata_bytes` | 10 GiB / 256 MiB | 128 GiB / 2 GiB |
| `resident.program.max_total_work` | 2^42 scalar iterations | 2^48 |
| `resident.program.instruction.max_instruction_work`, `max_scratch_bytes` | 2^40 iterations / 512 MiB | 2^46 / 8 GiB |
| `resident.program.instruction.primitive.max_tensor_bytes` | 1 GiB per tensor | 1 GiB |
| `resident.program.instruction.primitive.max_scatter_work` | 67,108,864 elements | 536,870,912 |
| `resident.program.instruction.primitive.max_index_metadata_bytes` | 128 MiB | 128 MiB |

The input word ceiling counts routed body/prefix words; the artifact's
`max_len=4096` independently limits body words. Encoded-token limits count
subwords and schema tokens. `run.batch_size` and declared tokenization maxima
must fit the configured encoder input ceilings together. Physical i32 index,
tensor rank/stride and per-tensor bounds remain mandatory at every geometry;
raising a resource ceiling cannot introduce float indices or a host fallback.

Scatter work counts dense zero writes plus every submitted input element.
For batch 1 at 512 encoded tokens, the published base embedding gradient
requires 98,705,664 scatter elements; multilingual requires 192,479,232.
The multilingual embedding alone occupies 768,344,064 FP32 bytes, and all 334
parameter gradients occupy 1,149,420,636 bytes. The default 512 MiB working
and 1 GiB aggregate capture limits therefore cannot admit that full-gradient
path even though its individual embedding tensor fits the 1 GiB tensor limit.
These are inventory/geometry calculations, not measured memory peaks.

For resident training, the backend reservation must cover frozen resident
weights, four tensors for each mutable parameter (weight, accumulation, first
moment and second moment), metadata, retained forward/backward execution,
bounded transfers and a separately staged atomic optimizer update. Four host
mirrors coexist with those device tensors. Restore additionally retains the
old owner while constructing a complete replacement from a bounded checkpoint
snapshot. `memory.optimizer_state_bytes` and
`memory.optimizer_transaction_bytes` must admit those phases, independently
of `training_limits`; raising only the scatter or tape ceiling is insufficient.
The current dense full-multilingual ownership/transaction lower bound already
exceeds 15 GiB before graph/tape/gradient/dataset/driver overhead. Its admission
requires larger hardware than the 16 GiB development machine.

The normalized requested limits are recorded in each invocation's `run.json`.
Resource-only limits are excluded from semantic run/optimizer fingerprints;
a resume may raise them if the new process passes all admission checks.
Model/data/schema identities, execution profile, adapter layout, optimizer,
batching, schedules, dropout and objective semantics remain fingerprinted.
A resource rejection publishes no new optimizer/data epoch and never changes
sampling, truncates supervision or silently selects a different kernel profile.

The focused resource/configuration/state checkpoint passed 14 CPU tests with
one intentional Metal-only test skip and no leaks. It covers every numeric
field's lower/upper bound, unchanged defaults, strict JSON types, all parser
allocation failures, exact consumed config hashes, path replacement, admission
split/overflow, cooperative pause selection and global optimizer-counter
consistency. Its log is `/private/tmp/gliner25-training-limits-state-v2.log`,
SHA-256 `8b15b92e889b0cc63b10c8f10e72ca758ca9f426ad8b570c91bc64378a9163d0`.
This checkpoint did not execute a full base/multilingual training job or a
supervised subprocess pause/resume scenario.

Source files are hashed and consumed through the same open descriptors into
immutable aligned snapshots. All 334 tensor names/shapes and FP32 encodings are
validated before training. Each dataset similarly owns its exact consumed
bytes and ordered schema fingerprints. Mutating an input path afterward cannot
alter the live job. Holdouts must have disjoint IDs and exact UTF-8 texts;
this check does not detect semantic duplicates or pretraining contamination.
Every declared split is tokenized and target-checked. Holdout preflight does
not run inference, compute metrics, tune thresholds or choose models.

Restore reads a bounded immutable checkpoint snapshot and stages complete
weights, moments, accumulation, presence bits and exact counters in a fresh
owner. It validates the run contract, expected state pin and derived dataset
cursor before publishing the replacement owner. File bytes and staged arrays
are accounted for by the restore transaction limit. There is no separate
mutable cursor file.

Initial disk admission conservatively allows an old and replacement checkpoint
to coexist plus the staged final export, with a default 256 MiB free-space
reserve. Checkpoint size is estimated as 16 bytes per enrolled FP32 parameter
element plus 8 MiB overhead. Save/export repeat disk checks. The parent output
directory must exist on a filesystem supporting the publication operations.

## Exact partial-window resume

The earlier command pauses after one new microbatch, preserving its unfinished
two-microbatch accumulation window. `--stop-after-microbatches` is an invocation
limit; it does not change the training schedule or run fingerprint. A planned
pause writes `latest.safetensors` and a `result.json` with `status: "paused"`.

Generate the resume configuration from that exact receipt. This normalizes
Zig's raw 32-byte JSON digest to an integer array; it does not treat it as a
64-character hex string:

```sh
python3 - <<'PY'
import json
from pathlib import Path

original = Path("/private/tmp/gliner25-heads-job-v1.json")
config = json.loads(original.read_bytes())
paused = Path(config["output_dir"])
receipt = json.loads((paused / "result.json").read_bytes())
if receipt["status"] != "paused" or receipt["accumulated_microbatches"] != 1:
    raise SystemExit("expected the saved one-microbatch partial window")
raw = receipt["state_sha256"]
if isinstance(raw, str):
    raw = raw.encode("utf-8")
elif isinstance(raw, list) and all(type(x) is int and 0 <= x <= 255 for x in raw):
    raw = bytes(raw)
else:
    raise SystemExit("invalid state digest encoding")
if len(raw) != 32:
    raise SystemExit("expected exactly 32 state digest bytes")
config["resume_from"] = str(paused / "latest.safetensors")
config["expected_restore_state_sha256"] = list(raw)
config["output_dir"] = "/private/tmp/gliner25-heads-resumed-v1"
with Path("/private/tmp/gliner25-heads-resume-v1.json").open("x") as output:
    json.dump(config, output, indent=2)
    output.write("\n")
PY
```

```sh
cd /Users/timkaye/Documents/af/antfly/zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build train-gliner25 -Doptimize=ReleaseFast -Dmetal=false -Dcuda=false -j1 -- /private/tmp/gliner25-heads-resume-v1.json
```

Keep all semantic settings and consumed source/train/holdout bytes unchanged.
The new output path and source/data locations may differ when their verified
bytes remain identical. The resume receipt records both the actual checkpoint
file digest and canonical staged-state digest. A caller-supplied state pin
detects unexpected checkpoint state before publication; these receipts are
integrity evidence, not publisher signatures.

For unplanned cancellation, timeout or process failure, recover from the last
successfully published checkpoint. The current CLI does not promise a fresh
checkpoint on every failure or signal. A failure before the first checkpoint
can leave no resumable state. Progress lines may be ahead of the durable
checkpoint; they must not be used to reconstruct or advance its cursor.

## Progress, receipts and export

| Output | Meaning |
| --- | --- |
| `run.json` | Immutable normalized job configuration, native math policy, observed executable path/hash, Zig/OS/architecture, source/data/schema/holdout identities, initial counters, optional restore receipt and memory admission. It explicitly records `evaluation_performed: false`. |
| `progress.jsonl` | Per-microbatch or partial-flush losses, coverage, optimizer result, decision fingerprint and host/backend peaks. It is synced at checkpoints and completion. |
| `latest.safetensors` | Authoritative durable optimizer state, atomically replaced on each successful checkpoint |
| `result.json` | Atomically published paused/complete result, counters, accumulated count, run fingerprint and canonical state digest. Complete results also pin the portable export. |
| `model/` | A complete FP32 model or standard PEFT adapter, published only after completed training; contains `antfly_gliner25_training.json` provenance |

Stdout emits JSON `event: "step"` and `event: "result"` records. Progress is
bounded to 64 MiB by default and each encoded entry to 8 KiB. Configured timeout
checks flow through source/preflight/training/checkpoint work. A completed
result is published after the final checkpoint, export and durability checks;
an exported directory alone does not prove that the whole invocation returned
success. Failed run directories are retained for diagnosis and recovery.
Manifest/result files are staged privately, synced and published without
replacing an existing receipt. Training never resumes into an existing output
directory.

The CLI supervisor now gives each invocation one disposable worker and does
not restart it. The worker rechecks exact configuration bytes and the shutdown
policy before execution. SIGINT/SIGTERM requests a safe-boundary checkpoint;
a second signal or exhausted shutdown grace kills and reaps the worker.
`--shutdown-grace-seconds` defaults to 30 and is capped at 300; timeout exits
124, interruption exits 130/143. The command timeout is capped at seven days.
The supervised deadline begins after the parent's bounded configuration read;
help and setup failures before supervision retain ordinary CLI returns.
An independent worker lifeline remains active through teardown, including
blocking exit handlers. Once its child is reaped, the parent drains its
command-owned arguments and configuration, then exits immediately for every
completion status. This bypasses unmonitored outer-runtime teardown and C
exit handlers. The latest process suite passes four existing server tests and
nine one-shot test methods. The parent-only blocking-exit method covers eight
completion/error/signal scenarios and verifies command-owned cleanup before
immediate exit. Its receipt is
`/private/tmp/gliner25-process-supervision-parent-v2.log`, SHA-256
`3bd2970748b3c763b84602b457c09cd17926fe49b240ff5cb5dfd323206d5052`.
The actual-model binary receipt below predates this parent completion change.
Four existing server
process tests and eight new one-shot process tests passed in
`/private/tmp/gliner25-process-supervision-v1.log`, SHA-256
`26eb686dd6481f3256ca8f2ec20f74dcb1701a36a4dacf89b67c69d6971bfb1a`.
These tests cover blocked work, teardown, owner loss after monitor startup,
and safe signal handling. Published-model GPU jobs remain a separate gate.

The first persistent CLI binary's redirected stdout overwrote earlier records
because it used positional writers. Its durable `progress.jsonl`, result and
export were complete and passed the checks above. The streaming-writer fix is
now checked with a fresh supervised CPU head-only pause/resume run. Both
commands exited zero. The pause preserved one unfinished microbatch, and the
resumed invocation completed three more microbatches and two optimizer
updates. All two paused-invocation and four resumed-invocation stdout events
survived redirection. The final optimizer-state and exported-model identities
match the earlier uninterrupted run's hashes recorded above. The earlier
binary's stdout file is still incomplete and is not progress evidence.

The new binary has SHA-256
`73695721460619998009dc6ea10e8cd436616c36897e1732d99cab57009d1c0f`
and size 5,277,776 bytes. Its evidence receipt is
`/private/tmp/antfly-gliner25-training-cli-v2/validation.json`, SHA-256
`7be46425288ddb96f8e1daf7f530cc052b3243d97f3e38a07ebd5a84bd729cca`.
The receipt pins both configurations, all stdout files, and the two
`run.json`/`result.json` pairs. Their sizes and hashes and the binary identity
were independently rechecked. Three focused tests also pass for redirected
JSON lines, exact config/pause/grace fingerprinting, and every CLI argument
adapter allocation failure.

The three focused CLI tests also pass after the parent completion change in
`/private/tmp/gliner25-native-memory-cli-v1.log`, SHA-256
`f023f3a3f3e1ffff8e54d50f41d6cd1b868b12ca7bca2fc57ab9c2c971262757`.
That checkpoint selected nine total tests, also covering native view readback,
budget accounting and trainer failure/recovery. It does not replace the exact
published-model binary receipt or qualify the runtime-artifact entrypoint.

The original 512 MiB host/backend profile was denied before source loading:
1,679,648,757 requested bytes exceeded the live 1,116,691,496-byte capacity.
The successful run used a separate explicit small-heads profile with 256 MiB
trainer-host and 128 MiB backend ceilings. It preserved all model/data/run
semantics and the unchanged live-memory policy. Its resumed allocation peaks
were 155,626,435 host bytes and 63,338,112 backend bytes. These are tracked
allocator peaks, not process RSS or a published-model GPU qualification.
The exact resource decision is recorded in the receipt's `resource-profile.json`.

Full/head exports stream the locked current parameter epoch into the original
334-name layout and retain the four source sidecars. Adapter exports retain
every resolved A/B/magnitude slot, including modules inactive in the last
microbatch; they bind the original frozen source and exact layout. Optimizer
state is in the checkpoint, not the portable model. Use
[the independent export checker](../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md)
to compare source/parameter/receipt bytes and, in a separately admitted run,
load the artifact with pinned Fastino/PEFT code.

Resident Metal encoder/PEFT programs and Session ownership have component
evidence. Two actual GPU optimizer Controller tests also passed the existing
three-parameter Torch fixture with unchanged CPU tolerances, including both
updates, exact fresh-owner partial-window resume, scalar-only submission
readbacks and recovery after a cancelled partial host-state copy. All six
existing native Controller regressions passed in that checkpoint. The overall
mixed-step checkpoint still had ten separate fixture/observer failures; see
[the scoped training evidence](GLINER25_TRAINING.md).

The current managed job remains native CPU. Mixed-task GPU gradients feeding
the resident optimizer and a complete full-model GPU job remain unqualified.
Neither the component tests nor four fixture rows qualify all-model training
recovery, merged/unmerged upstream inference, held-out improvements,
multi-seed convergence, throughput or release readiness.
