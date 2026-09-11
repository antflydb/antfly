# Tiny real-source training job fixture

[`training_job_small_v1`](../../testdata/gliner25/training_job_small_v1) contains
two authored training examples and two validation examples. Every row carries
the same complete entity, single-label classification, natural-record and
ordinary-relation schema, including an entity type absent from all positives.
Entity/record values, IDs, exact texts and normalized texts are disjoint across
splits. Shared task templates and labels are intentional. These four examples
test integration and cannot qualify extraction quality or convergence.

`prepare_training_job_fixture.py` uses the exact pinned upstream word pattern
to check every UTF-8 byte span. It never opens a model or tokenizer. All four
prepared artifacts reproduced byte-for-byte. Actual native Source/Dataset
preflight remains a separate test, followed by admitted optimizer execution,
durable resume, final export and validation inference.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `train.jsonl` | 2,192 | `47a7b27b46bbfa25c77754c4e88e97f82530b73db9e80f8cb918f64b8d04b517` |
| `validation.jsonl` | 2,238 | `638bd525f38eb071c4fbd84bd2726a1bbf020d1a05694b0934e03e392f20f531` |
| `schema.json` | 395 | `1eb946c6b140fba2ef24ee4e84c85536153ebcc933aadbca36bb290a68b1bd41` |
| `manifest.json` | 5,788 | `e4f7ab891ab01cba48798c784a0cb5b1d120109f0b05c113336dd65243ae85c6` |

The manifest references all five immutable files of
`fastino/gliner2.5-small-v1` revision
`cab1bddfd30fda7b803a4691c41f90378a2d517a`. Its preparation evidence is separate
from the numerical oracle inventory. The source/model pins do not claim a
model was executed during preparation.

The bounded job probe uses head-only mode, two epochs, batch size one,
accumulation two, constant learning rate, no warmup, no shuffle and seed 42.
First compare an uninterrupted run with a run paused after one microbatch,
restored into a new output directory using identical semantic settings. Both
paths should complete two optimizer updates. The restored checkpoint must
bind the consumed source, training and validation snapshots and exact pending
optimizer state. Compare final weights/counters and inference outputs;
validation examples must never enter the optimizer or select a tuned threshold.

Preparation into a new directory:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/prepare_training_job_fixture.py --output-dir /private/tmp/gliner25-training-job-fixture-new
```

The repeat used `/private/tmp/gliner25-training-job-fixture-repeat-v1`.
`qualification` and `heldout_quality_claim` remain false.
