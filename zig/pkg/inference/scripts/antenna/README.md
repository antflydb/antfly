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
