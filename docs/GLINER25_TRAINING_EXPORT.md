# GLiNER2.5 trained adapter compatibility

Exports retain original source identities, canonical tensor names and complete
parameter inventories. Native full/head model exports contain all 334 FP32
source tensors and four exact sidecars. LoRA/DoRA adapters contain exactly the
configured targets, A/B tensors and optional DoRA magnitudes; checkers also
bind the completed optimizer checkpoint and durable run layout.

The [static/runtime checker](../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md)
rejects missing/extra tensors, altered frozen parameters, source substitution,
malformed layouts and loaded-byte mismatches. Native
[materialization](GLINER25_MERGE.md) and
[three-form merge comparison](../zig/pkg/inference/scripts/gliner25/TRAINING_MERGE_CHECK.md)
are separate steps. Reloading an artifact does not prove training-update parity
or useful model quality.

Local published-small jobs exercised task-head, all-131-target and
classifier-only LoRA/DoRA artifacts, including CPU and Metal classifier-only
exports. Strict upstream reloads used CPU, even for Metal-trained artifacts.
The two all-target materialized models additionally underwent ten-task native
CPU/Metal comparisons against the Python forms. These narrow authored recipes
do not qualify other ranks/backbones/target sets, full-parameter published jobs,
convergence or held-out improvement. See the
[training contract](GLINER25_TRAINING.md) and
[immutable job limits](GLINER25_TRAINING_JOB.md).

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

Pure regression tests cover closed wheel inventory, traversal/symlink and
expansion limits, substituted bytes, imports outside the verified overlay,
dependency overrides and CLI profile admission.

## Validate an export

Use the [checker commands](../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md)
with explicit original source, exported model/adapter, completed run, pinned
wheel and a fresh output directory. Static checks run without loading a model;
`--runtime` additionally loads the actual pinned Fastino/PEFT implementation.
No network or missing-weight fallback is allowed.

Runtime validation uses one Torch and one inter-op thread, finite outputs and
bounded private copies. All ten fixed requests execute per artifact, including
empty outputs where the source produces them. This proves loading and
execution, not task quality or equality between differently trained artifacts.
The [trained-execution checker](../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md)
provides separate token/decision/confidence comparisons for materialized files.
Preserve raw failures, process cleanup and exact input/build/report identities
outside the source tree; every result retains `qualification:false`.
