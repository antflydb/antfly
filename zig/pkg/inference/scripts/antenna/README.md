# Antenna scripts

Tools for the Antenna encoder plan
([`models/antenna/ANTENNA.md`](../../models/antenna/ANTENNA.md)).

## Datasets

`antenna_datasets.py` is the single place that loads the classification and
NER datasets and names their labels, so pilot training rows and baseline
evaluation see the same text, labels and offsets.

- `load_classification(name, split)` returns `{id, text, label}` records, with
  `label` in natural language (Banking77 `card_arrival` is "card arrival",
  CLINC150 `oos` is "out of scope", AG News `Sci/Tech` is "science and
  technology"). `label_names(name)` lists the labels in a fixed order.
  `typed_decisions` has one record per case and question, each with its own
  `task`, `labels`, `descriptions`, `kind` and gold `target` distribution
  (score levels are named by their descriptions); its `label_names` is `None`.
- `load_ner(name, split)` returns `{id, text, tokens, entities}` records. The
  text is the tokens joined by single spaces, and entities carry UTF-8 byte
  offsets and natural-language types (`programlang` is "programming
  language", `Restaurant_Name` is "restaurant name"). `entity_types(name)`
  lists the types.

Names: `banking77`, `clinc150` (the `plus` configuration, 150 intents and
"out of scope"), `ag_news`, `sst5`, `typed_decisions`,
`crossner_{ai,literature,music,politics,science}`, `mit_restaurant`,
`mit_movie` (MIT Movie trivia). Every dataset has train and test splits;
CLINC150, SST-5, CrossNER and MIT also have validation.

Sources are fixed revisions: Hugging Face dataset commits, and, for Banking77
and CrossNER (whose Hub repositories are loading scripts), the GitHub commits
those scripts download from. `antenna_datasets.sha256.json` pins every file;
a changed file fails loading. No dataset script runs. Downloads are cached
under `$ANTFLY_ANTENNA_DATA` (default `~/.cache/antfly/antenna-datasets`).

```sh
python antenna_datasets.py --summary   # record, label and entity counts per split
python antenna_datasets.py --pin       # re-pin after deliberately changing a source
```

The module needs only the standard library and `pyarrow` (for Parquet).

## Baselines

`baselines.py` evaluates one GLiNER2 checkpoint directory (span or boundary,
including students exported by the native trainer) with the pinned upstream
(`scripts/gliner25/oracle.py`, commit `3c913c7`):

- classification accuracy, macro-F1 and ECE (15 bins, top-label confidence)
  on Banking77, CLINC150, AG News, SST-5 and typed-decisions. Every label is
  scored in one pass with upstream's `Classifier`; single-label tasks use its
  softmax probability. Typed-decisions is scored per question (choice and
  noul as single-label with label descriptions, score as ordinal) and also
  reports soft cross-entropy against the gold distribution.
- zero-shot NER exact span-and-type micro precision, recall and F1 on CrossNER
  and MIT at threshold 0.5.

Each test split is a fixed seeded subsample (seed 20260925; 500
classification records, 300 NER sentences, all 2,000 typed-decisions
questions) unless `--full`. The report records the sampled ids, source pins,
weight SHA-256, device and versions, and groups datasets into in-domain (the
pilot's training datasets: Banking77, AG News, CrossNER AI/literature/music,
MIT restaurant) and held-out (CLINC150, SST-5, typed-decisions, CrossNER
politics/science, MIT movie).

```sh
uv venv --python 3.12.3 .venv
uv pip install --python .venv/bin/python -r ../gliner25/requirements.txt \
  pyarrow==21.0.0 protobuf==6.32.1 sentencepiece==0.2.1
.venv/bin/python baselines.py --model-dir <checkpoint> --upstream <GLiNER2 checkout> \
  [--model-id <hub id> --revision <sha>] [--device mps|cpu] --output report.json
```

`protobuf` and `sentencepiece` are needed only to load the DeBERTa tokenizers
of released GLiNER2.5 checkpoints. MPS and CPU agree to 1.5e-6 in label
probabilities and give identical entity spans; MPS is the default.

`laya_baselines.py` scores a Laya checkpoint on the same classification
subsamples through the native serving evaluation
(`antfly-inference finetune eval laya`, models/laya/LAYA.md). Datasets with
more labels than the model admits (20, or 255 candidate-packed) are marked not
applicable. Typed-decisions states over 316 Laya tokens are dropped, as in
LAYA.md's step-0 evaluation. `baselines_tables.py` renders reports as
Markdown tables, and can score every model's typed-decisions predictions on
the subset another model admits.

Encoder latency for the GLiNER2.5 and Laya encoders is a ReleaseFast test,
`src/bench/antenna_encoder_timing_test.zig`. Results and commands:
[work-log/completed/inference/antenna/2026-09-25-baselines.md](../../../../../work-log/completed/inference/antenna/2026-09-25-baselines.md).

## Student and teacher targets

Both scripts run on the pinned GLiNER2.5 oracle
([`../gliner25/oracle.py`](../gliner25/oracle.py)), in a uv venv made from
`../gliner25/requirements.txt` plus `pyarrow`, with the upstream checkout at
the pinned commit.

- `init_student.py` builds the starting checkpoint: a boundary extractor on a
  pinned pretrained ModernBERT (`answerdotai/ModernBERT-base` by default) with
  freshly initialized published heads. The native training source loads it
  unchanged, and its `processor.json` pins upstream's token ids for that
  tokenizer (`ANTFLY_GLINER25_MODERNBERT_STUDENT=<dir>` runs the check).
- `teacher_targets.py` writes boundary training rows from the train splits of
  Banking77 and AG News (classification) and CrossNER ai/literature/music and
  MIT Restaurant (entities). Classification rows carry per-label
  `probabilities`, `w * gold + (1 - w) * sigmoid(Decide logit)`, over a
  sampled label subset; entity rows carry gold spans, or an extraction
  teacher's spans with `--teacher-entities`. Rows are deduplicated by text and
  split into train and validation.

```sh
python init_student.py --upstream <GLiNER2> --output <student>
ANTFLY_ANTENNA_DATA=<cache> python teacher_targets.py --upstream <GLiNER2> \
  --classifier <GLiNER2.5-Decide dir> --output <data>
antfly-inference finetune train gliner25 <job.json>
```

A ModernBERT-base job on resident Metal needs larger budgets than the job
defaults, which are sized for the small DeBERTa checkpoint: for example
`"memory": {"host_bytes": 6 GiB, "backend_bytes": 14 GiB, "combined_bytes":
22 GiB, "optimizer_state_bytes": 8 GiB, "optimizer_transaction_bytes": 8 GiB}`
and `"training_limits": {"differentiation": {"max_tape_bytes": 12 GiB}}`
(values in bytes). The exported `model/` directory loads with upstream
`AutoExtractor.from_pretrained`, so the baseline harness evaluates it.
