# Published classifier-only adapter reload evidence

All four completed published-small classifier-only artifacts passed the
unchanged Fastino/official PEFT 0.18 export checker: CPU-trained LoRA/DoRA and
Metal-trained LoRA/DoRA. Each reload used one upstream **CPU** model owner,
verified every original FP32 base tensor and adapter tensor byte, then ran all
ten fixed requests. All four process trees exited successfully, were reaped,
removed their private model/wheel copies, and left original inputs unchanged.

[manifest.json](manifest.json) binds 54 archived files: the four complete
reports and process records, exact small portable adapter files, frozen
checker/contract/supervisor copies, preparation tests and the independent
read-only audit. The archive payload is 1,068,334 bytes, excluding the manifest
and this README. It is execution evidence outside the upstream numerical
reference-fixture manifest. Original base weights, checkpoints and saved
intermediate request tensor payloads remain external and hash-bound; the four
adapter artifacts themselves are preserved under `exports/`.

| Artifact's training backend | Mode | Base tensors | Adapter tensors | Requests | Sampled reload RSS bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| Native CPU | LoRA | 334 | 4 | 10 | 1,245,577,216 |
| Native CPU | DoRA | 334 | 6 | 10 | 1,040,465,920 |
| Resident Metal | LoRA | 334 | 4 | 10 | 1,356,742,656 |
| Resident Metal | DoRA | 334 | 6 | 10 | 1,148,469,248 |

All adapters target only `classifier.0` and `classifier.3`, rank 2, alpha 3,
adapter dropout zero. Their ordinary published-source training jobs retained
materialized attention and published encoder/head dropout 0.1; they completed
five active/inactive microbatches and three updates. The loaded artifact bytes
also match the separately resumed exports. Training policy and exact
same-backend resume evidence remain in the separate
[CPU](../published_inactive_classifier_cpu_v1/manifest.json) and
[Metal](../published_inactive_classifier_metal_v1/manifest.json) ledgers.

The loader was isolated official `peft-0.18.0-export-v1`, over Fastino commit
`3c913c7369301133d3b7699252074c4303ada50e`. The original training oracle's
PEFT 0.17.1 environment and historical `inside_weight` loader failure remain
unchanged. There was no missing-key fallback, target omission, network access
or model download. The exact original source, virtualenv invocation/resolved
executable, wheel, helper closure and final checkpoint identities are in the
manifest and raw reports.

Each process retained a 180-second deadline, 2 GiB sampled process-tree RSS
guard, 4 MiB per stream, 512 MiB copy admission and 576 MiB active output-tree
ceiling; final captures were capped at 64 MiB. Actual private model/adapter/
wheel copies were about 305.9 MB. Hashing and audits are bounded by fixed file
sizes; the child deadline covers the model process, not an interruptible
whole-audit promise. RSS may count shared pages twice. These are guarded
correctness observations, not throughput measurements.

The earlier Metal-LoRA launch failed disk preflight before child creation;
its stderr is retained under `history/`. After the root reclaimed only five
exact completed test object files, the unchanged command and limits passed.
There was no failed numerical model run or tolerance adjustment in this set.

This qualifies artifact interoperability and bounded request execution for
these four exports. It does not establish published Fastino training-loss/VJP
or CPU–Metal update equality, useful trained quality, convergence, replay-tiled
published training, other variants/targets/ranks, or release readiness. No
classification probability-map or all-label calibration claim is made.
