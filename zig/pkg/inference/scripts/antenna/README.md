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
