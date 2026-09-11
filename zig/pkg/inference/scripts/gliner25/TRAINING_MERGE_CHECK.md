# Trained adapter merge compatibility check

`check_training_merge.py` checks a native-materialized LoRA or DoRA export
against its original published source, completed native training run and exact
materialization job. The immutable contract is
[`training_merge_contract.json`](training_merge_contract.json), scope
`gliner25_training_merge_compatibility/v1`. This checker does not enable a model,
measure task quality, or establish convergence or performance.

The native producer and its resource/publication contracts are documented in
[GLINER25_MERGE.md](../../../../../docs/GLINER25_MERGE.md). This checker currently
requires an Antfly training export and its completed run receipt. The native
materializer also accepts explicitly bound standard adapters without that
training receipt; those artifacts are outside this checker version's scope.

## Fixed comparison rules

All 334 original parameter names, shapes and FP32 dtypes must be present. The
checker binds the original five source files, adapter configuration/weights/
receipt, final optimizer state, schema/target/parameter fingerprints, exact raw
materialization job bytes, merge receipt and all five output model files.
Recorded job paths are provenance locators: a relocated artifact is checked
against the paths explicitly supplied to the checker.

Every untouched tensor, every bias and all four sidecars must retain exact
source bytes. An adapted tensor must be finite. These static checks stream
file contents and do not execute a model. They cannot establish that an adapted
matrix implements the intended mathematical update.

With `--runtime`, the independent reference is the unmodified official PEFT
0.18.0 release in the separately pinned export profile. Its released loader
fixes the `inside_weight` key insertion defect in PEFT 0.17.1. The original
0.17.1 oracle, golden training fixtures and failed loader evidence remain
unchanged; see [the export checker](TRAINING_EXPORT_CHECK.md).

Every native adapted element is compared with the official PEFT merged FP32
element using the predeclared rule:

```text
abs(native - peft) <= 1e-6 + 1e-5 * abs(peft)
```

The reference magnitude is the PEFT value. The native DoRA policy computes the
norm of rounded FP32 directions in FP64; the checker makes no claim of
byte-identical adapted arithmetic across implementations. It records every
matrix's element count, violations, maximum absolute/relative error and up to
eight first failing indices. Untouched tensors remain subject to exact byte
comparison after PEFT merging, including all biases.

The fixed ten-request fixture is executed in three forms: unmerged PEFT
adapter, official PEFT merge, and the independently loaded native-materialized
model. All three pairwise comparisons require exact encoder token IDs and
discrete outputs, including labels, values, order, spans and public solver
metadata. Confidence and complete classification probability maps use the
existing absolute `5e-4` bound. The classification solver's numeric objective
is retained in raw evidence but is not treated as a discrete decision. These
limits cannot be changed through CLI flags. Empty task outputs still count as
executed requests; ten authored examples are not a quality evaluation.

## Invocation

Use absolute paths and a new evidence directory for each check. `--run-dir`
is the completed training run whose `model` contains the adapter;
`--job-config` is the exact JSON consumed by materialization. The following
paths illustrate a small LoRA check; substitute the actual completed merge
paths from its receipt.

```sh
GLINER25_CHECKER=zig/pkg/inference/scripts/gliner25/check_training_merge.py
GLINER25_ADAPTER_RUN=/private/tmp/antfly-gliner25-training-runtime-artifact-v1/lora-all-host128-backend384-uninterrupted
GLINER25_MERGE_JOB=/absolute/path/lora-all-merge.json
GLINER25_MERGED_MODEL=/absolute/path/lora-all-merged

python3 "$GLINER25_CHECKER" \
  --variant small \
  --source-dir /private/tmp/antfly-gliner25-models/small \
  --adapter-dir "$GLINER25_ADAPTER_RUN/model" \
  --run-dir "$GLINER25_ADAPTER_RUN" \
  --merged-dir "$GLINER25_MERGED_MODEL" \
  --job-config "$GLINER25_MERGE_JOB" \
  --output-dir /private/tmp/gliner25-merge-static-check-v1
```

Only run the numerical form in the serialized model execution lane. It uses
the invoked virtual environment path, records and hashes the resolved Python
executable separately, and verifies the environment configuration before
imports. The upstream checkout must remain at
`3c913c7369301133d3b7699252074c4303ada50e`.

```sh
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 TOKENIZERS_PARALLELISM=false \
  /private/tmp/antfly-gliner25-oracle-venv/bin/python "$GLINER25_CHECKER" \
  --variant small \
  --source-dir /private/tmp/antfly-gliner25-models/small \
  --adapter-dir "$GLINER25_ADAPTER_RUN/model" \
  --run-dir "$GLINER25_ADAPTER_RUN" \
  --merged-dir "$GLINER25_MERGED_MODEL" \
  --job-config "$GLINER25_MERGE_JOB" \
  --output-dir /private/tmp/gliner25-merge-runtime-check-v1 \
  --runtime \
  --upstream /private/tmp/antfly-gliner25-upstream \
  --peft-wheel /private/tmp/antfly-gliner25-export-peft018-v1/peft-0.18.0-py3-none-any.whl \
  --max-rss-mib 6144 --runtime-deadline-seconds 600 \
  --event-deadline-seconds 180 --max-runtime-copy-bytes 4294967296
```

The checker forces one Torch/inter-op thread, blocks remote access and uses
strict local loads. Missing adapter keys are errors, and every loaded tensor
is verified against the admitted bytes. It never retrains, omits a requested
target, patches a loader, or downloads a checkpoint. Only one model owner is
resident at a time: the PEFT wrapper is released after merging, and all source
model/state-dictionary views and hooks are released before the native export
is loaded. Weak-reference checks enforce this lifecycle.

The private-copy budget covers the original, adapter, materialized model and
official wheel overlay together. Copies are hashed before and after use.
The parent supervises the worker's RSS, absolute/event deadlines, stderr and
event volume; default limits are 6 GiB RSS, 600 seconds, 180 seconds per event,
8 MiB stderr, 128 events, 32 MiB event output and an 8 MiB final report.
Tensor comparison reads 1 MiB file chunks. After reaping the worker, the
parent removes only its newly created private scratch directory, including
copies left by forced termination. Input artifacts and evidence are retained.

## Receipts and current evidence

`report.json` distinguishes `static_verified`, `verified`, and
`comparison_failed`; every result retains `qualification:false`. A completed
numerical mismatch writes diagnostics and exits nonzero. Incomplete,
cancelled, resource-limited or malformed runs retain `failure.json`, received
`runtime.events.jsonl`, worker stderr and `runtime.process.json` as available.
An incomplete transport uses an unknown numerical-execution marker instead
of claiming that no model ran. Existing output directories are rejected.

The new checker passed 13 lightweight contract tests, within all 123 GLiNER2.5
Python tests, with no Torch/model execution. Coverage includes full-inventory
synthetic LoRA/DoRA artifacts, exact job/tensor/sidecar identities, rehashed
mutations, fixed tolerances, all three comparison denominators, model-owner
release, cancellation/deadlines, substituted protocol events and scratch
cleanup. The log is `/private/tmp/gliner25-training-merge-contract-v4.log`,
SHA-256 `01ac46adb3dbd2707958955112daf33c234d86deaab57a3cec68be1c1f59bf8c`.
The contract SHA-256 is
`b18312e337302e7c0c1bf2b0ec5c4679ade912f0ec5029905d94131f43e89149`.
The subsequent published-small all-131-target LoRA and DoRA runs both passed
the complete numerical check. Each verified 131 adapted matrices with zero
violations and 203 exact untouched tensors, including every bias; all ten
requests matched across all three pairwise comparisons. Maximum adapted
absolute errors were `2.9802322387695312e-8` and `7.152557373046875e-7`,
respectively. Maximum output confidence/probability differences were
`1.7881393432617188e-6` and `2.205371856689453e-6`. Both completed 37 events
and cleaned their owned scratch. The exact artifact/report/build digests and
resource observations are in the
[actual materialization evidence](../../../../../docs/GLINER25_MERGE.md#actual-small-all-target-materialization).
These three forms execute through Python; a native-produced artifact is not
evidence of trained-artifact Zig CPU/Metal inference. That execution proof,
other variants/ranks, quality and release qualification remain separate.

```sh
python3 -m unittest discover \
  -s zig/pkg/inference/scripts/gliner25 \
  -p 'test_*.py' -v
```
