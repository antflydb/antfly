#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Pinned, permissively licensed training-only datasets for Antenna.

``antenna_datasets.py`` defines the evaluation suite (and the pilot's training
splits). These sets are for training only and never evaluated on. They replace
the pilot sets whose licenses do not permit redistributable weights: AG News
(its corpus page restricts it to non-commercial use) and MIT restaurant (no
license). Records use ``antenna_datasets``' shapes:

- classification ``{"id", "text", "label"}``, with ``label_names(name)``;
- NER ``{"id", "text", "tokens", "entities": [{"start", "end", "type"}]}``,
  UTF-8 byte offsets into ``text`` (tokens joined by single spaces), with
  natural-language ``entity_types(name)``.

| Name | Source | License |
| --- | --- | --- |
| huffpost | HuffPost News Category (heegyu/news-category-dataset), 42 topics | CC BY 4.0 |
| dbpedia | DBpedia-14 (fancyzhx/dbpedia_14), 14 topics | CC BY-SA 3.0 |
| massive_intent | MASSIVE en-US (mteb/amazon_massive_intent), 60 intents | Apache 2.0 |
| massive_slots | MASSIVE 1.1 en-US slot annotations, 55 types | CC BY 4.0 |
| fewnerd | Few-NERD supervised (DFKI-SLT/few-nerd), 66 fine types | CC BY-SA 4.0 |
| multiconer | MultiCoNER v2 English (MultiCoNER/multiconer_v2), 33 types, lowercased | CC BY 4.0 |
| snips_restaurant | SNIPS NLU benchmark BookRestaurant (sonos/nlu-benchmark), 14 slot types | CC0 1.0 |

Every file is fetched from a pinned revision through ``antenna_datasets``'
cache and checked against its SHA-256 here.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
from typing import Any

import antenna_datasets as datasets

HF = "https://huggingface.co/datasets"
SOURCES = {
    "huffpost": (
        f"{HF}/heegyu/news-category-dataset/resolve/304a05a55bc6abc0446d8fae0d0771716b6a271a/data.json",
        "2cfc51fa4a82a96ecbe251fa383737aa1dea2e0a517f61dece633fcc25a4a065",
    ),
    "dbpedia": (
        f"{HF}/fancyzhx/dbpedia_14/resolve/9abd46cf7fc8b4c64290f26993c540b92aa145ac/dbpedia_14/train-00000-of-00001.parquet",
        "0640e4664a99cc94c47db1d7b2e01c14455d5bbecb8183ad1f93bde59f3f28ee",
    ),
    "massive_intent": (
        f"{HF}/mteb/amazon_massive_intent/resolve/940fd47a81eaa7f2cc7b129674d945d618ac38c2/train/en.json.gz",
        "65e77f0f2596931671074e3d031a481bc798e5800daa2c886ce7aa26cc16cf22",
    ),
    "massive_slots": (
        "https://amazon-massive-nlu-dataset.s3.amazonaws.com/amazon-massive-dataset-1.1.tar.gz",
        "4cba5faa11c71437928e17cb1b9b3d8b8e727e7ea363a3a9a8045e19c0491577",
    ),
    "fewnerd": (
        f"{HF}/DFKI-SLT/few-nerd/resolve/205f3e9c9f3577ea2561d43f2f62dc249ab92d5b/supervised/train-00000-of-00001.parquet",
        "6ccb192b1accd3d1754db2244d18ddf64040357fb5b9076a338ec421a72d7d61",
    ),
    "multiconer": (
        f"{HF}/MultiCoNER/multiconer_v2/resolve/4be2d62c912977ee26ed14d2553a4fe17ca3d980/EN-English/en_train.conll",
        "1e1af77f95e92aa287c40feb7c10b384192c8ff026c916114a54713f331138ea",
    ),
    # The repository is CC0 1.0; only BookRestaurant is used, so MIT movie
    # stays as far out of domain as before.
    "snips_restaurant": (
        "https://raw.githubusercontent.com/sonos/nlu-benchmark/b86ac7f1577868c42158d0dec77db50956046696/2017-06-custom-intent-engines/BookRestaurant/train_BookRestaurant_full.json",
        "7677e82cd6e9a8191f0a4502568c786278c984900b4796d3628e46f1abf73ef4",
    ),
}
CLASSIFICATION = ("huffpost", "dbpedia", "massive_intent")
NER = ("massive_slots", "fewnerd", "multiconer", "snips_restaurant")

_FEWNERD = {
    "broadcastprogram": "broadcast program",
    "writtenart": "written work",
    "other": None,  # resolved with the coarse type below
    "sportsfacility": "sports facility",
    "attack/battle/war/militaryconflict": "military conflict",
    "sportsevent": "sports event",
    "GPE": "geopolitical entity",
    "bodiesofwater": "body of water",
    "road/railway/highway/transit": "road or railway",
    "education": "educational institution",
    "government/governmentagency": "government agency",
    "media/newspaper": "media organization",
    "politicalparty": "political party",
    "showorganization": "show organization",
    "sportsleague": "sports league",
    "sportsteam": "sports team",
    "astronomything": "astronomical object",
    "biologything": "biological entity",
    "chemicalthing": "chemical",
    "educationaldegree": "educational degree",
    "livingthing": "living thing",
    "medical": "medical term",
    "artist/author": "artist or author",
}
_SNIPS = {
    "party_size_number": "party size",
    "party_size_description": "party description",
    "timeRange": "time range",
    "served_dish": "dish",
    "poi": "point of interest",
    "sort": "ranking preference",
}
_MULTICONER = {
    "AerospaceManufacturer": "aerospace manufacturer",
    "AnatomicalStructure": "anatomical structure",
    "ArtWork": "artwork",
    "CarManufacturer": "car manufacturer",
    "HumanSettlement": "human settlement",
    "MedicalProcedure": "medical procedure",
    "Medication/Vaccine": "medication or vaccine",
    "MusicalGRP": "musical group",
    "MusicalWork": "musical work",
    "ORG": "organization",
    "OtherLOC": "location",
    "OtherPER": "person",
    "OtherPROD": "product",
    "PrivateCorp": "private company",
    "PublicCorp": "public company",
    "SportsGRP": "sports team",
    "SportsManager": "sports manager",
    "VisualWork": "visual work",
    "WrittenWork": "written work",
}


def _fetch(name: str) -> bytes:
    url, expected = SOURCES[name]
    data = datasets._fetch(url)
    digest = hashlib.sha256(data).hexdigest()
    if digest != expected:
        raise ValueError(f"SHA-256 mismatch for {url}: {digest} != {expected}")
    return data


def _parquet(data: bytes) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import pyarrow.parquet as pq

    features = json.loads(pq.read_schema(io.BytesIO(data)).metadata[b"huggingface"])[
        "info"
    ]["features"]
    return pq.read_table(io.BytesIO(data)).to_pylist(), features


def _camel(identifier: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", identifier).replace("_", " ").lower()


def _huffpost() -> list[dict[str, Any]]:
    rows = [
        json.loads(line) for line in _fetch("huffpost").decode("utf-8").splitlines()
    ]
    # HuffPost renamed and merged sections over the years; fold the old names
    # into the current ones so no two labels mean the same thing.
    merged = {
        "the worldpost": "world news",
        "worldpost": "world news",
        "arts": "arts & culture",
        "culture & arts": "arts & culture",
        "style": "style & beauty",
        "parents": "parenting",
        "taste": "food & drink",
        "green": "environment",
        "healthy living": "wellness",
        "college": "education",
    }
    records = []
    for i, row in enumerate(rows):
        text = " ".join(
            part.strip() for part in (row["headline"], row["short_description"]) if part
        )
        label = row["category"].strip().lower()
        if text:
            records.append(
                {
                    "id": f"huffpost/train/{i}",
                    "text": text,
                    "label": merged.get(label, label),
                }
            )
    return records


def label_names(name: str) -> list[str]:
    if name == "huffpost":
        return sorted({record["label"] for record in _huffpost()})
    if name == "dbpedia":
        _, features = _parquet(_fetch("dbpedia"))
        return [_camel(n) for n in features["label"]["names"]]
    if name == "massive_intent":
        return sorted({record["label"] for record in load_classification(name)})
    raise KeyError(f"not a training classification set: {name}")


def load_classification(name: str) -> list[dict[str, Any]]:
    if name == "huffpost":
        return _huffpost()
    if name == "dbpedia":
        rows, features = _parquet(_fetch("dbpedia"))
        names = [_camel(n) for n in features["label"]["names"]]
        return [
            {
                "id": f"dbpedia/train/{i}",
                "text": row["content"].strip(),
                "label": names[row["label"]],
            }
            for i, row in enumerate(rows)
        ]
    if name == "massive_intent":
        import gzip

        rows = [
            json.loads(line)
            for line in gzip.decompress(_fetch("massive_intent"))
            .decode("utf-8")
            .splitlines()
        ]
        return [
            {
                "id": f"massive_intent/train/{i}",
                "text": row["text"],
                "label": _camel(row["label_text"]),
            }
            for i, row in enumerate(rows)
        ]
    raise KeyError(f"not a training classification set: {name}")


def _massive_rows() -> list[dict[str, Any]]:
    with tarfile.open(
        fileobj=io.BytesIO(_fetch("massive_slots")), mode="r:gz"
    ) as archive:
        member = archive.extractfile("1.1/data/en-US.jsonl")
        lines = member.read().decode("utf-8").splitlines()
    return [row for row in map(json.loads, lines) if row["partition"] == "train"]


def _massive_tokens(annotated: str) -> tuple[list[str], list[str]]:
    """``wake me up at [time : nine am]`` -> tokens with IO tags."""
    tokens, tags = [], []
    for match in re.finditer(r"\[(\w+) : ([^\]]+)\]|(\S+)", annotated):
        if match.group(3) is not None:
            tokens.append(match.group(3))
            tags.append("O")
        else:
            for word in match.group(2).split():
                tokens.append(word)
                tags.append(match.group(1))
    return tokens, tags


def _fewnerd_type(raw: str) -> str:
    coarse, fine = raw.split("-", 1)
    if fine == "other":
        return {
            "art": "work of art",
            "building": "building",
            "event": "event",
            "location": "location",
            "organization": "organization",
            "person": "person",
            "product": "product",
        }[coarse]
    return _FEWNERD.get(fine) or fine


def _io_sentences(name: str) -> list[tuple[list[str], list[str]]]:
    """(tokens, IO tags with natural type names); runs of one type are one entity."""
    if name == "massive_slots":
        return [
            (tokens, [tag if tag == "O" else _camel(tag) for tag in tags])
            for tokens, tags in map(
                _massive_tokens, (row["annot_utt"] for row in _massive_rows())
            )
        ]
    if name == "snips_restaurant":
        sentences = []
        for row in json.loads(_fetch("snips_restaurant").decode("utf-8"))[
            "BookRestaurant"
        ]:
            tokens, tags = [], []
            for chunk in row["data"]:
                entity = chunk.get("entity")
                kind = _SNIPS.get(entity, _camel(entity)) if entity else "O"
                for word in chunk["text"].split():
                    tokens.append(word)
                    tags.append(kind)
            sentences.append((tokens, tags))
        return sentences
    if name == "fewnerd":
        rows, features = _parquet(_fetch("fewnerd"))
        names = features["fine_ner_tags"]["feature"]["names"]
        return [
            (
                row["tokens"],
                [
                    "O" if t == 0 else _fewnerd_type(names[t])
                    for t in row["fine_ner_tags"]
                ],
            )
            for row in rows
        ]
    raise KeyError(name)


def _bio_multiconer() -> list[tuple[list[str], list[str]]]:
    sentences, tokens, tags = [], [], []
    for line in _fetch("multiconer").decode("utf-8").splitlines():
        if line.startswith("# id"):
            continue
        if not line.strip():
            if tokens:
                sentences.append((tokens, tags))
            tokens, tags = [], []
            continue
        parts = line.split()
        tokens.append(parts[0])
        tags.append(parts[-1])
    if tokens:
        sentences.append((tokens, tags))
    return sentences


def entity_types(name: str) -> list[str]:
    if name == "multiconer":
        raw = sorted(
            {tag[2:] for _, tags in _bio_multiconer() for tag in tags if tag != "O"}
        )
        return sorted({_MULTICONER.get(kind, _camel(kind)) for kind in raw})
    return sorted(
        {tag for _, tags in _io_sentences(name) for tag in tags if tag != "O"}
    )


def _spans(
    tokens: list[str], kinds: list[str | None], starts_entity: list[bool]
) -> list[dict[str, Any]]:
    offsets, cursor = [], 0
    for token in tokens:
        offsets.append((cursor, cursor + len(token.encode("utf-8"))))
        cursor += len(token.encode("utf-8")) + 1
    entities, current = [], None
    for i, kind in enumerate(kinds + [None]):
        if current is not None and (
            kind != current["type"] or (i < len(tokens) and starts_entity[i])
        ):
            entities.append(current)
            current = None
        if kind is not None and current is None:
            current = {"start": offsets[i][0], "end": offsets[i][1], "type": kind}
        elif kind is not None:
            current["end"] = offsets[i][1]
    return entities


def load_ner(name: str) -> list[dict[str, Any]]:
    records = []
    if name == "multiconer":
        sentences = [
            (
                tokens,
                [
                    None if tag == "O" else _MULTICONER.get(tag[2:], _camel(tag[2:]))
                    for tag in tags
                ],
                [tag.startswith("B-") for tag in tags],
            )
            for tokens, tags in _bio_multiconer()
        ]
    else:
        sentences = [
            (
                tokens,
                [None if tag == "O" else tag for tag in tags],
                [False] * len(tokens),
            )
            for tokens, tags in _io_sentences(name)
        ]
    for i, (tokens, kinds, starts) in enumerate(sentences):
        if not tokens:
            continue
        records.append(
            {
                "id": f"{name}/train/{i}",
                "text": " ".join(tokens),
                "tokens": tokens,
                "entities": _spans(tokens, kinds, starts),
            }
        )
    return records
