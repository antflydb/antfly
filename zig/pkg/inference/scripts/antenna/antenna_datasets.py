#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Pinned classification and NER datasets for Antenna baselines and training.

One module decides how every dataset is loaded and how its labels are named,
so training rows and evaluation see the same text, labels, and offsets
(zig/pkg/inference/models/antenna/ANTENNA.md).

Sources are fetched from pinned revisions (Hugging Face dataset commits or
GitHub commits) and verified by SHA-256; no dataset loading script is run.
Downloads are cached under ``$ANTFLY_ANTENNA_DATA`` (default
``~/.cache/antfly/antenna-datasets``).

Classification records: ``{"id", "text", "label"}``, where ``label`` is the
natural-language label name that evaluation passes to a model;
``label_names(name)`` lists them. ``typed_decisions`` has one record per
(case, question) and a label set per question, so its records also carry
``task`` (the question instruction), ``labels``, ``descriptions``, ``kind``
(choice/score/noul), ``target`` (the gold distribution over ``labels``) and
``group_id``; its ``label_names`` is ``None``.

NER records: ``{"id", "text", "tokens", "entities": [{"start", "end", "type"}]}``
with UTF-8 byte offsets into ``text`` (tokens joined by single spaces) and
natural-language entity types; ``entity_types(name)`` lists them.

Names: banking77, clinc150, ag_news, sst5, typed_decisions,
crossner_{ai,literature,music,politics,science}, mit_restaurant, mit_movie.
Splits: train and test everywhere; validation where the source has one.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

HF = "https://huggingface.co/datasets"
GITHUB = "https://raw.githubusercontent.com"
CLINC_REV = "155b9c710419136e17307b80d0a13e68cd46b4ec"
AG_NEWS_REV = "eb185aade064a813bc0b7f42de02595523103ca4"
SST5_REV = "e51bdcd8cd3a30da231967c1a249ba59361279a3"
TYPED_REV = "f7a2487edd7a043a5441a5e9ccc7fe5ddbd9ebe8"
MIT_RESTAURANT_REV = "538663410a86a70f788b0c193d42320de330cc0d"
MIT_MOVIE_REV = "d35f3cd11c9c5c1754ef66bfcbcb6a8e632216a6"
# PolyAI/banking77 and DFKI-SLT/cross_ner on the Hub are loading scripts that
# download these files; read the same files at pinned commits instead.
BANKING77_COMMIT = "57ec275d8078af65b7731c2a98be812d844a6d6b"
CROSSNER_COMMIT = "2e7ba2a7798c961e3f29fbc51252c5a8d40224bf"

CLASSIFICATION = ("banking77", "clinc150", "ag_news", "sst5", "typed_decisions")
CROSSNER_DOMAINS = ("ai", "literature", "music", "politics", "science")
NER = tuple(f"crossner_{domain}" for domain in CROSSNER_DOMAINS) + ("mit_restaurant", "mit_movie")


def _sources(name: str) -> dict[str, str]:
    """Split -> pinned URL."""
    if name == "banking77":
        base = f"{GITHUB}/PolyAI-LDN/task-specific-datasets/{BANKING77_COMMIT}/banking_data"
        return {"train": f"{base}/train.csv", "test": f"{base}/test.csv"}
    if name == "clinc150":
        base = f"{HF}/clinc/clinc_oos/resolve/{CLINC_REV}/plus"
        return {split: f"{base}/{split}-00000-of-00001.parquet" for split in ("train", "validation", "test")}
    if name == "ag_news":
        base = f"{HF}/fancyzhx/ag_news/resolve/{AG_NEWS_REV}/data"
        return {split: f"{base}/{split}-00000-of-00001.parquet" for split in ("train", "test")}
    if name == "sst5":
        base = f"{HF}/SetFit/sst5/resolve/{SST5_REV}"
        return {"train": f"{base}/train.jsonl", "validation": f"{base}/dev.jsonl", "test": f"{base}/test.jsonl"}
    if name == "typed_decisions":
        base = f"{HF}/LocalLLaMA/typed-decisions/resolve/{TYPED_REV}/all"
        return {split: f"{base}/{split}-00000-of-00001.parquet" for split in ("train", "test")}
    if name.startswith("crossner_") and name[len("crossner_"):] in CROSSNER_DOMAINS:
        base = f"{GITHUB}/zliucr/CrossNER/{CROSSNER_COMMIT}/ner_data/{name[len('crossner_'):]}"
        return {"train": f"{base}/train.txt", "validation": f"{base}/dev.txt", "test": f"{base}/test.txt"}
    if name in ("mit_restaurant", "mit_movie"):
        repo, rev = ("tner/mit_restaurant", MIT_RESTAURANT_REV) if name == "mit_restaurant" else ("tner/mit_movie_trivia", MIT_MOVIE_REV)
        base = f"{HF}/{repo}/resolve/{rev}/dataset"
        return {"train": f"{base}/train.json", "validation": f"{base}/valid.json", "test": f"{base}/test.json", "labels": f"{base}/label.json"}
    raise KeyError(f"unknown dataset: {name}")


# SHA-256 of every pinned file, recorded on first download (see `pin`).
SHA256: dict[str, str] = {}
_PINS = Path(__file__).with_name("antenna_datasets.sha256.json")
if _PINS.exists():
    SHA256.update(json.loads(_PINS.read_text()))


def cache_dir() -> Path:
    return Path(os.environ.get("ANTFLY_ANTENNA_DATA", Path.home() / ".cache" / "antfly" / "antenna-datasets"))


def _fetch(url: str) -> bytes:
    path = cache_dir() / hashlib.sha256(url.encode()).hexdigest()
    if path.exists():
        data = path.read_bytes()
    else:
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        staging = path.with_suffix(".partial")
        staging.write_bytes(data)
        staging.replace(path)
    digest = hashlib.sha256(data).hexdigest()
    expected = SHA256.get(url)
    if expected is not None and expected != digest:
        raise ValueError(f"SHA-256 mismatch for {url}: {digest} != {expected}")
    return data


def source_pins(name: str) -> dict[str, dict[str, str]]:
    """Split -> {url, sha256} for provenance in reports."""
    return {split: {"url": url, "sha256": hashlib.sha256(_fetch(url)).hexdigest()} for split, url in _sources(name).items()}


def splits(name: str) -> list[str]:
    return [split for split in _sources(name) if split != "labels"]


def _url(name: str, split: str) -> str:
    table = _sources(name)
    if split not in table or split == "labels":
        raise KeyError(f"{name} has no {split!r} split (available: {splits(name)})")
    return table[split]


def _parquet(url: str) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(_fetch(url)))
    return table.to_pylist()


def _parquet_label_names(url: str, column: str) -> list[str]:
    import pyarrow.parquet as pq

    metadata = pq.read_schema(io.BytesIO(_fetch(url))).metadata or {}
    features = json.loads(metadata[b"huggingface"])["info"]["features"]
    return list(features[column]["names"])


def _natural(identifier: str) -> str:
    return identifier.replace("_", " ").strip().lower()


_AG_NEWS = {"World": "world", "Sports": "sports", "Business": "business", "Sci/Tech": "science and technology"}
_SST5 = ["very negative", "negative", "neutral", "positive", "very positive"]


def label_names(name: str) -> list[str] | None:
    """Natural-language labels in a fixed order; None for typed_decisions."""
    if name == "banking77":
        rows = csv.DictReader(io.StringIO(_fetch(_url(name, "train")).decode("utf-8")))
        return sorted({_natural(row["category"]) for row in rows})
    if name == "clinc150":
        return ["out of scope" if raw == "oos" else _natural(raw) for raw in _parquet_label_names(_url(name, "test"), "intent")]
    if name == "ag_news":
        return [_AG_NEWS[raw] for raw in _parquet_label_names(_url(name, "test"), "label")]
    if name == "sst5":
        return list(_SST5)
    if name == "typed_decisions":
        return None
    raise KeyError(f"not a classification dataset: {name}")


def _typed_decisions(split: str) -> list[dict[str, Any]]:
    records = []
    for case in _parquet(_url("typed_decisions", split)):
        state = case["state"]
        try:
            state = json.loads(state)
        except (TypeError, json.JSONDecodeError):
            pass
        text = state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)
        questions, gold = json.loads(case["questions"]), json.loads(case["gold"])
        for qid, question in questions.items():
            kind, criteria = question["type"], question.get("criteria")
            # Same label conventions as scripts/laya/prepare_laya_finetune.py.
            if kind == "choice" and isinstance(criteria, dict):
                keys = list(criteria)
                descriptions = [str(criteria[key]) for key in keys]
            elif kind == "score" and isinstance(criteria, list):
                keys = [str(i) for i in range(len(criteria))]
                descriptions = [str(value) for value in criteria]
            elif kind == "noul" and (criteria is None or isinstance(criteria, dict)):
                keys = ["false", "true"]
                descriptions = [str((criteria or {}).get(key) or "") for key in keys]
            else:
                raise ValueError(f"unsupported typed-decisions question {case['id']}/{qid}")
            probabilities = gold[qid]["probabilities"]
            target = [float(probabilities[key]) for key in keys]
            # Score levels are named by their description; choices and noul by key.
            labels = descriptions if kind == "score" else [_natural(key) for key in keys]
            best = max(range(len(keys)), key=lambda i: target[i])
            records.append({
                "id": f"{case['id']}/{qid}", "group_id": case["id"], "text": text,
                "task": question["instructions"], "kind": kind, "keys": keys, "labels": labels,
                "descriptions": descriptions, "target": target, "label": labels[best],
            })
    return records


def load_classification(name: str, split: str) -> list[dict[str, Any]]:
    """Records ``{id, text, label}`` (see the module docstring for typed_decisions)."""
    url = _url(name, split)
    if name == "typed_decisions":
        return _typed_decisions(split)
    if name == "banking77":
        rows = csv.DictReader(io.StringIO(_fetch(url).decode("utf-8")))
        return [{"id": f"{name}/{split}/{i}", "text": row["text"], "label": _natural(row["category"])} for i, row in enumerate(rows)]
    if name == "clinc150":
        names = label_names(name)
        return [{"id": f"{name}/{split}/{i}", "text": row["text"], "label": names[row["intent"]]} for i, row in enumerate(_parquet(url))]
    if name == "ag_news":
        names = label_names(name)
        return [{"id": f"{name}/{split}/{i}", "text": row["text"], "label": names[row["label"]]} for i, row in enumerate(_parquet(url))]
    if name == "sst5":
        lines = [json.loads(line) for line in _fetch(url).decode("utf-8").splitlines() if line.strip()]
        return [{"id": f"{name}/{split}/{i}", "text": row["text"], "label": _SST5[row["label"]]} for i, row in enumerate(lines)]
    raise KeyError(f"not a classification dataset: {name}")


_CROSSNER_TYPES = {
    "ai": ["algorithm", "conference", "country", "field", "location", "metrics", "misc", "organisation", "person",
           "product", "programlang", "researcher", "task", "university"],
    "literature": ["award", "book", "country", "event", "literarygenre", "location", "magazine", "misc",
                   "organisation", "person", "poem", "writer"],
    "music": ["album", "award", "band", "country", "event", "location", "misc", "musicalartist",
              "musicalinstrument", "musicgenre", "organisation", "person", "song"],
    "politics": ["country", "election", "event", "location", "misc", "organisation", "person", "politicalparty",
                 "politician"],
    "science": ["academicjournal", "astronomicalobject", "award", "chemicalcompound", "chemicalelement", "country",
                "discipline", "enzyme", "event", "location", "misc", "organisation", "person", "protein",
                "scientist", "theory", "university"],
}
_CROSSNER_NATURAL = {
    "academicjournal": "academic journal", "astronomicalobject": "astronomical object",
    "chemicalcompound": "chemical compound", "chemicalelement": "chemical element",
    "literarygenre": "literary genre", "misc": "miscellaneous", "musicalartist": "musical artist",
    "musicalinstrument": "musical instrument", "musicgenre": "music genre", "organisation": "organization",
    "politicalparty": "political party", "programlang": "programming language",
}


def _raw_entity_types(name: str) -> list[str]:
    if name.startswith("crossner_"):
        return list(_CROSSNER_TYPES[name[len("crossner_"):]])
    if name in ("mit_restaurant", "mit_movie"):
        table = json.loads(_fetch(_sources(name)["labels"]))
        return sorted({tag[2:] for tag in table if tag != "O"})
    raise KeyError(f"not an NER dataset: {name}")


def _natural_type(name: str, raw: str) -> str:
    if name.startswith("crossner_"):
        return _CROSSNER_NATURAL.get(raw, raw)
    return _natural(raw)


def entity_types(name: str) -> list[str]:
    """Natural-language entity types in a fixed order."""
    return [_natural_type(name, raw) for raw in _raw_entity_types(name)]


def _bio_sentences(name: str, split: str) -> list[tuple[list[str], list[str]]]:
    """(tokens, BIO tags with raw type names)."""
    data = _fetch(_url(name, split)).decode("utf-8")
    if name.startswith("crossner_"):
        sentences, tokens, tags = [], [], []
        for line in data.splitlines():
            if not line.strip():
                if tokens:
                    sentences.append((tokens, tags))
                tokens, tags = [], []
                continue
            token, tag = line.rsplit("\t", 1) if "\t" in line else line.rsplit(" ", 1)
            tokens.append(token)
            tags.append(tag.strip())
        if tokens:
            sentences.append((tokens, tags))
        return sentences
    table = json.loads(_fetch(_sources(name)["labels"]))
    by_id = {index: tag for tag, index in table.items()}
    rows = [json.loads(line) for line in data.splitlines() if line.strip()]
    return [(row["tokens"], [by_id[tag] for tag in row["tags"]]) for row in rows]


def load_ner(name: str, split: str) -> list[dict[str, Any]]:
    """Records ``{id, text, tokens, entities: [{start, end, type}]}``, UTF-8 byte offsets."""
    known = set(_raw_entity_types(name))
    records = []
    for index, (tokens, tags) in enumerate(_bio_sentences(name, split)):
        starts, cursor = [], 0
        for token in tokens:
            starts.append(cursor)
            cursor += len(token.encode("utf-8")) + 1
        entities, current = [], None
        for i, tag in enumerate(tags + ["O"]):
            inside = tag.startswith("I-") and current is not None and current[2] == tag[2:]
            if current is not None and not inside:
                first, last, raw = current
                entities.append({"start": starts[first], "end": starts[last] + len(tokens[last].encode("utf-8")),
                                 "type": _natural_type(name, raw)})
                current = None
            if tag.startswith("B-") or (tag.startswith("I-") and current is None):
                if tag[2:] not in known:
                    raise ValueError(f"{name}/{split}/{index}: unknown entity type {tag[2:]!r}")
                current = [i, i, tag[2:]]
            elif inside:
                current[1] = i
        records.append({"id": f"{name}/{split}/{index}", "text": " ".join(tokens), "tokens": tokens, "entities": entities})
    return records


def pin(output: Path = _PINS) -> dict[str, str]:
    """Download every source and record its SHA-256 (run once when adding a source)."""
    names = CLASSIFICATION + NER
    pins = {url: hashlib.sha256(_fetch(url)).hexdigest() for name in names for url in _sources(name).values()}
    output.write_text(json.dumps(dict(sorted(pins.items())), indent=2) + "\n")
    return pins


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pin", action="store_true", help="download every source and write the SHA-256 pin file")
    parser.add_argument("--summary", action="store_true", help="print record and label counts for every split")
    args = parser.parse_args()
    if args.pin:
        print(json.dumps({"pinned": len(pin())}))
    if args.summary:
        for name in CLASSIFICATION:
            labels = label_names(name)
            for split in splits(name):
                records = load_classification(name, split)
                print(json.dumps({"name": name, "split": split, "records": len(records),
                                  "labels": None if labels is None else len(labels)}))
        for name in NER:
            for split in splits(name):
                records = load_ner(name, split)
                print(json.dumps({"name": name, "split": split, "records": len(records),
                                  "entities": sum(len(r["entities"]) for r in records),
                                  "types": len(entity_types(name))}))
