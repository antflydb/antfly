# Training export compatibility checker

`check_training_export.py` checks a newly exported native training artifact
against its original published source. It is separate from the frozen
CrossNER/evaluation helpers. The default path uses only Python's standard
library and never constructs a model. Fifteen focused pure tests cover all four
artifact modes, source substitution, missing/extra tensors, frozen encoder
mutation, final checkpoint weight/moment/gradient mismatch, PEFT metadata and layout hashes, malformed
Safetensors, FIFO rejection, network denial, runtime-copy admission and the
native architecture/config version contract.

The first actual small head-only export passed strict pinned-upstream loading
and all ten bounded extraction requests, as recorded below. Separate small
CPU task-head LoRA/DoRA jobs now also have exact partial-resume and standard
PEFT 0.18.0 loading evidence in the
[adapter export guide](../../../../../docs/GLINER25_TRAINING_EXPORT.md).
The original PEFT 0.17.1 loader fails on the valid `inside_weight` target;
the official fixed loader uses an explicit isolated profile with unchanged
adapter keys and strict byte checks. `qualification`
is always false. Four authored training rows or ten curated extraction requests
cannot establish task quality or convergence.

The separate published-small classifier-only CPU/Metal training artifacts
now also pass four actual upstream CPU reloads with this unchanged checker:
334 base tensors and four LoRA or six DoRA tensors byte-exact, then all ten
requests per export. Complete process cleanup and unchanged input pins were
independently rechecked. The
[additive reload ledger](../../testdata/gliner25/published_inactive_classifier_export_reload_v1/manifest.json)
archives the reports, portable adapter bytes and fixed supervisor/profile.
This proves reload interoperability; it does not compare training gradients
or updates between CPU and Metal, or establish useful trained quality.

## Static checks

The source directory must match all five pins of the selected small, base or
multi checkpoint. Complete model exports must contain exactly the pinned
334-name inventory with original shapes and FP32 dtype. All four sidecars must
remain byte-identical. A head-only export must preserve every original encoder
tensor; the checker reports every changed task parameter without requiring
nonzero updates.

LoRA/DoRA exports must contain exactly the configured biased Linear targets,
A/B tensors, and DoRA magnitude vectors. The checker independently reproduces
the native target, parameter and complete run-layout digests. It validates
both training and adapter receipts against source/tokenizer/schema identities.
Unrecognized PEFT behaviors or extra loader files fail.

With `--run-dir`, the checker also verifies the durable completed result,
training provenance, final optimizer counters, and every exported trainable
against the final checkpoint's owned weight slots. It recomputes the canonical
owned-state digest from weight/moment/gradient payloads, presence bits,
per-parameter Adam counters, enrolled names/shapes and persisted optimizer
contract, then compares that digest with `result.json`. This checks internal
agreement; receipts are not publisher signatures.

Reads use one regular descriptor with size and change checks. Safetensors
ranges must cover the complete payload without gaps or overlaps. Limits are
2 GiB for a full source/export, 512 MiB for adapter weights, 8 GiB for a final
checkpoint and 4 MiB for tensor headers. Static mode checks inventory and bytes;
runtime mode additionally scans every loaded FP32 tensor for finite values.

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/check_training_export.py --variant small --source-dir /private/tmp/antfly-gliner25-models/small --export-dir /private/tmp/example-training-run/model --run-dir /private/tmp/example-training-run --output-dir /private/tmp/example-export-static-check
```

Use new report directories outside the input artifacts. The command publishes
the report only after all requested checks succeed.

## Optional upstream loading

Add `--runtime` only when the shared compute lane is assigned. The checker
copies the previously verified artifacts into a private temporary directory,
rehashing copied bytes, with an aggregate 3 GiB disk-copy ceiling and 256 MiB
free-space reserve. These copies are removed after loading/capture. It loads
one model, on CPU, FP32, with one Torch thread, no quantization or compilation,
and no FlashDeBERTa. Hugging Face offline settings and denied socket connection
entry points prevent network retrieval. Missing weights are never filled as a
fallback: the pinned loader uses strict full-state loading, and the checker
then compares the shape, dtype and exact bytes of every loaded tensor.

Full/head artifacts use the pinned `AutoExtractor.from_pretrained`. Adapters
use `PeftModel.from_pretrained` over a separately loaded complete original
extractor; the adapter's locator is recorded metadata and cannot choose a
different base. Loaded A/B/magnitude bytes and every frozen base tensor are
checked independently. `training_export_contract.json` pins PEFT 0.17.1's
loader and LoRA/DoRA sources; `oracle.prepare_runtime` additionally checks the
immutable Fastino checkout and exact numerical package versions.
That default is preserved for the original oracle. Task-head adapter loading
uses `--runtime-profile peft-0.18.0-export-v1 --peft-wheel <pinned-wheel>`;
`training_export_peft018.json` fixes the official 190-file wheel, exact
dependency versions and import origins without changing the existing venv
or the training oracle. This profile is separately recorded in each receipt.

These paths follow the pinned source
[boundary loader](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/boundary/model.py)
and [PEFT integration](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/lora.py).

`--requests` optionally runs a version-1 oracle request fixture with at most
32 fixed requests. The existing ten-request fixture is suitable for a bounded
load/inference probe; it is independent of the four-row training dataset.
The report records exact request bytes, token/routing tensors and upstream
outputs. It does not compare those outputs to gold or claim improvement.

```sh
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/check_training_export.py --variant small --source-dir /private/tmp/antfly-gliner25-models/small --export-dir /private/tmp/example-training-run/model --run-dir /private/tmp/example-training-run --output-dir /private/tmp/example-export-python-check --runtime --requests zig/pkg/inference/testdata/gliner25/requests.json
```

Native-versus-upstream inference comparison on the same trained export,
representative held-out quality, real-run recovery and all-model/backend
qualification remain separate evidence requirements.

## Actual small head-only export evidence

The native CLI job at
`/private/tmp/antfly-gliner25-training-cli-v1/uninterrupted` completed four
microbatches and two optimizer updates. Its export checker then exited
successfully on 2026-09-10 with the exact pinned CPU/FP32/one-thread upstream
loader and denied network access. The checker verified all 334 exported and
loaded parameter byte sequences, every unchanged encoder tensor and sidecar,
all 136 final owned parameter slots, and the reconstructed optimizer-state
digest. There were 108 changed task tensors. All ten request-fixture entries
executed, including attributes, all record modes, constrained classification
and JointIE. This is load/inference execution evidence; it does not compare
trained native outputs to Python or evaluate gold labels.

| Evidence | SHA-256 |
| --- | --- |
| `model/model.safetensors` | `ed1c88487e6b1936f0586e3868c4b944af9707b5a31ea0834156f60d354cefd7` |
| Final canonical optimizer state | `508c7369ee604274321ae5666c88d57eb9dbae1efe881118b96ff86dd04bdf3c` |
| `latest.safetensors` | `88e44f88c56ad23ae4baf25166a070b16bc8c050d29fa8d51ef9dd09f0303f75` |
| `/private/tmp/gliner25-training-export-heads-check-v1/report.json` | `5692fd22c94069de7d96135668705e0dca70a9ca79810921dc60f03ebff3e841` |
| Checker source used for this run | `ead1cd614bcde3595074cd24193875838b700615d8d906fd616b3bb60177d849` |
| Loader contract | `b164ae2fe9aef1506f179fc554ae956826d782c0ff0ca7d4685d747882687978` |
| Ten-request fixture | `030c979419cee5fd209ba6a6f23f2e0cc7744e396d4bf3357925651293f5fe5f` |

An initial checker attempt rejected its own stale expectation of native
`config_version: 1` before loading a model. The checker was corrected to the
actual native contract, `config_version: 3` with architecture/tensor policy 1,
and a source-version regression was added. The successful receipt above uses
that corrected checker. The only loader warning was the pinned compatibility
normalization of legacy `extra_special_tokens` metadata.

Full-parameter loading, other variants, same-trained-artifact native/Python
output comparison and held-out improvement remain unqualified by this run.
The later small task-head LoRA/DoRA evidence is separately versioned in the
linked adapter guide; it does not expand this original heads receipt.
The observed load duration is diagnostic,
not a latency or memory benchmark.
