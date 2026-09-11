# GLiNER2.5 trained adapter export evidence

The published small CPU checkpoint now has bounded native LoRA and DoRA job,
unfinished-accumulation resume, and standard PEFT loading evidence for two
separate target sets: **59 task-head Linear modules** and **all 131 encoder
plus task-head Linear modules**. Both use rank 2, alpha 4, adapter dropout zero,
two training rows and two disjoint validation rows. Each job completed four
microbatches and two AdamW updates. Validation was immutable-data preflight;
no held-out quality evaluation or convergence measurement occurred.

Separate published-small classifier-only LoRA/DoRA jobs now add a third
target scope: `classifier.0` and `classifier.3`, rank 2 and alpha 3, five
active/inactive microbatches and three updates. The four CPU-trained and
Metal-trained final adapters all pass actual upstream CPU reload and ten
requests with the same isolated PEFT 0.18 profile. Their exact per-backend
resume and reload evidence is separate from the earlier two-row jobs.

The complete [training scope](GLINER25_TRAINING.md) and
[job ownership/limits](GLINER25_TRAINING_JOB.md) remain applicable. Other
ranks/target sets/variants and broader published GPU jobs remain separate qualification
requirements. The all-131-target small adapters additionally have independent
merged/unmerged Python and materialized-artifact Zig CPU/Metal execution
comparisons below. Inactive-adapter update behavior has separate tiny source
and published classifier-only policy/continuity evidence; broader numerical
training and quality scopes remain open.

## Exact source and task-head job identities

All source files were checked against the pinned small revision
`cab1bddfd30fda7b803a4691c41f90378a2d517a`. Its original
`model.safetensors` contains 295,567,700 bytes with SHA-256
`4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b`.
Fastino source remains commit `3c913c7369301133d3b7699252074c4303ada50e`.
The supervised native executable was
`/private/tmp/antfly-gliner25-training-cli-v2/train-gliner25`, SHA-256
`73695721460619998009dc6ea10e8cd436616c36897e1732d99cab57009d1c0f`.
Later executable revisions are not covered by this artifact receipt.

The job family is under
`/private/tmp/antfly-gliner25-training-head-adapters-v1/`. The original
encoder-plus-head job configurations remain unchanged elsewhere. These jobs
use the fixed [four-row fixture](../zig/pkg/inference/scripts/gliner25/TRAINING_JOB_FIXTURE.md)
and native CPU execution. PEFT mode gives every trainable adapter tensor
`task_lr`; the source's `"encoder" in name` grouping applies to ordinary
full/head training and does not split these adapters into encoder/task rates.

| Measured integration property | LoRA | DoRA |
|---|---:|---:|
| Adapted Linear modules | 59 | 59 |
| Adapter tensors / final owned optimizer parameters | 118 | 177 |
| Adapter Safetensors bytes | 242,464 | 285,924 |
| Host allocator peak bytes | 36,273,111 | 38,373,962 |
| Backend allocator peak bytes | 50,741,560 | 60,666,312 |
| Microbatches / optimizer updates | 4 / 2 | 4 / 2 |

Host/backend ceilings stayed at 256/128 MiB, with live admission and all
individual caps enforced. These allocator observations are neither process
RSS nor encoder-adapter memory bounds. Every training row retained all six
gold mentions and its one gold relation. The declared natural-record and
classification tasks remained in the full schema.

Both modes paused after microbatch one, before the first optimizer update,
then restored into a fresh output directory. Full final results, checkpoint
bytes, adapter bytes and all adapter sidecars match uninterrupted execution.
Redirected stdout contained 5 uninterrupted, 2 paused and 4 resumed events.
The resume helper compares native `f32` settings by exact IEEE-754 binary32
encoding; it does not use epsilon tolerances or relax the `f64` settings.

| Receipt or artifact | SHA-256 |
|---|---|
| `lora-resume-validation-v1.json` | `442fc8d0c9cc388e6e6ec6ba3e86c5113af8f2f7f2eb6cfe2f1a96e1d06c477e` |
| `dora-resume-validation-v1.json` | `e118d2f5500ef86581e8c2dbc607c38339e61d3a11023f464b1c6f8361ee192c` |
| LoRA final canonical state | `8b80c1128cbdd7523dcd3e16404358278ccff4f1cc3f194eb2755728fd40ad37` |
| DoRA final canonical state | `a5a2ea57b6b1c778becf1478c18bdf809043e9cf0ea4a6c06c20c1fbeaf46650` |
| LoRA `adapter_model.safetensors` | `05cbf792a600609eb4c8b34ba4ea8322ac78bdf4f3263650c96a4a739589b7b8` |
| DoRA `adapter_model.safetensors` | `2003f170990a74219d76d5bad401b35e6c0eaa578bdc404797de3ef7708089fe` |

The receipt files above were reread and every referenced file's size and
SHA-256 independently recomputed. Complete state receipts include pending
gradients, moments, counters, presence and enrolled parameter identities.

## Complete encoder-plus-head jobs

Separate native CPU jobs adapted all 72 encoder and 59 task-head Linear
modules using the same fixed four-row dataset and PEFT settings. These ran
through the public `antfly-inference finetune train gliner25` route, executable
SHA-256 `6cd281cf04e326aab187647173ab645780f2a789621d6dc4607bf39fb91f5fa5`
(25,854,800 bytes). The run family is
`/private/tmp/antfly-gliner25-training-runtime-artifact-v1/`.

| Measured all-target integration property | LoRA | DoRA |
|---|---:|---:|
| Adapted Linear modules | 131 | 131 |
| Adapter tensors / owned optimizer parameters | 262 | 393 |
| Adapter Safetensors bytes | 926,488 | 1,146,572 |
| Trainer host ceiling | 128 MiB | 128 MiB |
| Backend ceiling | 384 MiB | 512 MiB |
| Maximum observed trainer host bytes | 44,640,688 | 50,930,139 |
| Maximum observed backend bytes | 282,550,704 | 484,999,240 |
| Microbatches / optimizer updates | 4 / 2 | 4 / 2 |

These two owner ceilings are not the complete process reservation. The
configuration separately admits immutable source, optimizer, transaction,
metadata and job owners under a 2 GiB combined ceiling. Smaller backend
profiles returned typed resource denials: all-target LoRA did not complete
with 128/256 MiB backend, and DoRA did not complete with 384 MiB backend.
The successful profiles retained live admission and every individual bound;
the table reports measured allocator peaks, not peak RSS or a general model
memory requirement. DoRA's paused host peak was 43,602,189 bytes, while its
resumed and uninterrupted peaks were 50,930,139 bytes.

Each mode completed uninterrupted execution and a separate pause after the
first microbatch followed by restoration into a new output directory.
The entire final result, latest checkpoint and all four exported files were
independently compared byte for byte. Redirected stdout contained 5/2/4
uninterrupted/paused/resumed events. The exact run directories are
`lora-all-host128-backend384-{uninterrupted,paused,resumed}` and
`dora-all-host128-backend512-{uninterrupted,paused,resumed}`.

| All-target receipt or artifact | SHA-256 |
|---|---|
| `lora-all-resume-validation-v1.json` | `134217d8238e5b00c7ab2366c94761082d1b417197c1cb13a7692550967f7e35` |
| `dora-all-resume-validation-v1.json` | `2b81f3f9aaf30be10d4f80f80507e9186e3afc042c7c4b65eda5695a34656a62` |
| LoRA final canonical state | `8bcc1782053b0dfb350ee4d8de685d9d100918f744bcabf60685824009b0261a` |
| DoRA final canonical state | `6afe534e64da1f3d9aa847e0c1b0f7cba88d708148fc094393ada2bc99694d47` |
| LoRA `adapter_model.safetensors` | `e1f2f9484941938b48d726cbfd1dd4b200cc9f5920cdff64f4b4f95f3a04de2e` |
| DoRA `adapter_model.safetensors` | `e1a0d9ddcdd1267ee2e0438b6083789d3cbd04ccaa595587069add631055f3c8` |
| `lora-all-peft018-check/report.json` | `2d377386c32c6da31e98f73dfc671fd8891c8ff046ba1154bd73584219d59275` |
| `dora-all-peft018-check/report.json` | `08e8164401ad8b0ed524bc168cb8e75fe90dce50335c918873fe1ad1ae4faf6d` |

Both independent upstream checks used the same isolated official PEFT 0.18.0
profile described below. They verified all 334 original source tensors plus
all 262 LoRA or 393 DoRA tensors byte-exactly, then executed all ten fixed
requests without remote access or missing-weight fallback. Each resume
receipt's executable and 19 referenced files was rehashed during review.
This extends the small CPU training/load proof to encoder adapters; it does
not extend it to other variants, ranks, GPU model jobs or task quality.

## Published classifier-only export reloads

Four separate bounded processes loaded the final classifier-only artifacts
from the published-small CPU and resident Metal jobs. Each used the exact
original five-file small source and verified all 334 base tensor names,
shapes, FP32 dtypes and bytes before and after adapter attachment. LoRA has
four adapter tensors and DoRA six; every adapter byte matched its export and
completed three-update/five-microbatch checkpoint. Loaded exports also match
the separately resumed artifacts byte for byte.

All four processes used actual pinned Fastino and official PEFT 0.18.0 on
**CPU**, including those loading Metal-trained adapters. All ten fixed
requests ran per export, with no missing-key fallback, omitted targets or
network access. This is 40 execution requests, not an output-equality
comparison between differently trained CPU and Metal artifacts.

| Training artifact | Adapter tensors | Reload report SHA-256 |
| --- | ---: | --- |
| CPU LoRA | 4 | `2728c6883f236e5689abb8fc0c661e55cc8275145ed02049f5db5296354f3299` |
| CPU DoRA | 6 | `23c5de608f6a1698c9dd0add32ba2aeadf29929f9b929e0e6a36bd42a87ddc07` |
| Metal LoRA | 4 | `0fefdefdce51a85630b3f0619e37aace5f68d968790d6a3db54766ea0ede24e4` |
| Metal DoRA | 6 | `06d993e134719aa65aeac0079e58f0df96401667c3662eb0baa602ea0ea6eb3f` |

The [additive reload ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_export_reload_v1/manifest.json)
preserves all complete reports/process receipts, the four small portable
adapter artifacts, and exact helper/loader/input pins. An independent
read-only audit rechecked the original files, checkpoint digests, all saved
capture hashes and complete process/cleanup records. Eight preparation tests
passed before execution. The 180-second/2 GiB RSS guards, 512 MiB private-copy
cap and all output bounds stayed unchanged; maximum observed RSS was
1,356,742,656 bytes. Every process was reaped and its private copies removed.
The earlier Metal-LoRA disk-preflight denial is retained separately; no model
child ran in that denied attempt.

These jobs used materialized training attention. Reload compatibility adds
no published Fastino loss/VJP or CPU–Metal update equality, trained quality,
convergence, replay-tiled training, performance or GA claim.

## Released PEFT loader compatibility

The original pinned PEFT 0.17.1 loader cannot load every standard adapter key
for this model. Its `_insert_adapter_name_into_state_dict` globally replaces
the suffix text `weight`, also rewriting the module name `inside_weight`.
For example, the correct `boundary_head.pair_scorer.inside_weight.lora_A.weight`
key becomes a nonexistent `inside_default.weight` module path. The native
artifact agrees with PEFT's canonical saved keys. The actual first LoRA
load failed strict missing-key/loaded-byte verification, preserved in
`lora-peft-check.stderr.log`; no success receipt was published. No target was
omitted or renamed. See the official
[0.17.1 loader](https://github.com/huggingface/peft/blob/v0.17.1/src/peft/utils/save_and_load.py).

The separate `peft-0.18.0-export-v1` profile uses the official released
[0.18.0 loader's suffix-anchored substitution](https://github.com/huggingface/peft/blob/v0.18.0/src/peft/utils/save_and_load.py).
The original oracle environment, PEFT 0.17.1 training fixtures and frozen
evaluation helpers remain unchanged. The checker verifies the original
dependency contract first, verifies the official wheel and all 190 entries,
then imports PEFT 0.18.0 from a private temporary overlay. It changes no
package code and installs nothing into the existing venv. Imports must come
from the verified overlay; all other pinned numerical dependencies remain
the same, with `tqdm==4.67.1` explicitly recorded. The wheel's declared
dependencies are recorded from the official [PyPI release metadata](https://pypi.org/pypi/peft/0.18.0/json).

| Export profile identity | Value |
|---|---|
| Official wheel | `peft-0.18.0-py3-none-any.whl`, 556,427 bytes |
| Wheel SHA-256 | `624f69ca6393b765ccc6734adda7ca57d80b238f0900a42c357d8b67a03d62ff` |
| `training_export_peft018.json` | `08905e3428c9d4a45a5e6a2c54677427f727cee1aa605647bc0b613567000968` |
| `training_export_runtime.py` used | `b6d15a935454a84f7df18eb394e4d6dd9c1dc8a988426e93169d596236d42d63` |
| `check_training_export.py` used | `11c5aac29919b21eba0e66aa1ab96d6bae0591d1cc154c5c980f703a001f91d5` |

Six pure tests cover closed wheel inventory, path/symlink and expansion
limits, substituted bytes, imports outside the overlay, dependency overrides
and CLI profile admission. They passed as part of the 108-test checkpoint
`/private/tmp/gliner25-export-profile-contract-v1.log`, SHA-256
`c76ed9024dda0ab23c7b0c5af7448168b59458143d57c06b8941d80e5d26c790`.

## Actual upstream load and bounded requests

Both runtime checks exited successfully using the original small extractor
and official PEFT 0.18.0. They verified exact names, shapes, dtype and bytes
of all 334 frozen source tensors and every adapter tensor; every owned
parameter also matched the final optimizer checkpoint. Missing-weight fallback
and network access remained disabled. Each check used CPU FP32, one Torch
thread, one inter-op thread and bounded private artifact copies.

| Runtime receipt under the job family | SHA-256 |
|---|---|
| `lora-peft018-check/report.json` | `5f12d9d9c7c3c920e667fc949684f87ea9db4cdedd449ae1182618f4e8ec963e` |
| `dora-peft018-check/report.json` | `8e8b921fc9b5fe1c76fdaa9b5732a410398ca149eaa512f3c674e261fbd5155f` |

Each loaded model executed all ten requests from the immutable fixture
SHA-256 `030c979419cee5fd209ba6a6f23f2e0cc7744e396d4bf3357925651293f5fe5f`,
covering the mixed task, Unicode, attributes, legacy/natural/latent/anchorless
records, enum, constrained-classification and JointIE paths. This proves
loading and execution. The anchorless result is empty, and ordinary/JointIE
relations can be empty; these are not quality success criteria. The checker
did not compare trained native outputs against Python or compare merged and
unmerged inference. Every result retains `qualification:false`.

For a separately authorized serial compute slot, run from the repository
root using a **new** report directory. Use `dora` independently for its job:

```sh
GLINER25_EXPORT_PYTHON=/private/tmp/antfly-gliner25-oracle-venv/bin/python
GLINER25_ADAPTER_JOBS=/private/tmp/antfly-gliner25-training-head-adapters-v1
GLINER25_PEFT_WHEEL=/private/tmp/antfly-gliner25-export-peft018-v1/peft-0.18.0-py3-none-any.whl

PYTHONDONTWRITEBYTECODE=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
VECLIB_MAXIMUM_THREADS=1 BLIS_NUM_THREADS=1 \
"$GLINER25_EXPORT_PYTHON" zig/pkg/inference/scripts/gliner25/check_training_export.py \
  --variant small --source-dir /private/tmp/antfly-gliner25-models/small \
  --export-dir "$GLINER25_ADAPTER_JOBS/lora/model" \
  --run-dir "$GLINER25_ADAPTER_JOBS/lora" \
  --output-dir /private/tmp/gliner25-lora-export-new-check \
  --runtime --runtime-profile peft-0.18.0-export-v1 --peft-wheel "$GLINER25_PEFT_WHEEL" \
  --requests zig/pkg/inference/testdata/gliner25/requests.json \
  --max-runtime-copy-bytes 536870912
```

The default `oracle-0.17.1` profile remains available for original oracle
compatibility checks and retains the documented failing behavior for these
task-head adapters. The [checker contract](../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md)
describes immutable reads, complete checkpoint verification and the original
head-only export proof. The separate all-target jobs above establish their
own small CPU encoder-adapter evidence. Published full-parameter jobs,
held-out improvement, convergence and production release remain open. The
subsequent [native materialization and independent three-form comparison](GLINER25_MERGE.md#actual-small-all-target-materialization)
passed for both all-target adapters: all 334 tensors, exact untouched/bias/
sidecar bytes, zero adapted numerical violations and ten matching requests
across unmerged PEFT, official PEFT merge and the Python-loaded native-produced
model. The [merge checker](../zig/pkg/inference/scripts/gliner25/TRAINING_MERGE_CHECK.md)
used its original fixed tolerances. The subsequent separate
[trained-execution checker](../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md)
also passed both materialized small artifacts on Zig CPU and Metal against
all three Python forms: 120 token/decision/confidence comparisons and 20
CPU–Metal comparisons with native output metadata. This is specific to the
all-131-target merged FP32 files; it does not expand the task-head-only export
checks above or prove arbitrary heterogeneous training updates. The new audit
receipt is `trained-execution-validation-v1.json` under the materialization
directory, SHA-256
`ea3c2fb430d10d68fd5a0db0e5449ebae76008600698868147508db4f4e826eb`.
