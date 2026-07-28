#!/usr/bin/env python3
"""Prepare deterministic, query-explicit WANDS attribute associations.

This sidecar augments the pinned embedding fixture without changing its human
relevance judgments. Product attributes come from WANDS ``product_features``;
query attributes are emitted only when a supported catalog value occurs as a
literal, token-bounded phrase in the published query.

Binary layout (all integers little-endian):

    8s magic ("AFHDCAS1")
    u32 version
    u32 product count
    u32 query count
    u32 field count
    u32 product association count
    u32 query association count
    u32 product_offsets[product_count + 1]
    { u32 field_id, u32 value_id } product associations
    u32 query_offsets[query_count + 1]
    { u32 field_id, u32 value_id } query associations
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import struct
from collections import Counter
from pathlib import Path
from typing import Iterable

MAGIC = b"AFHDCAS1"
VERSION = 1
HEADER = struct.Struct("<8s6I")
PAIR = struct.Struct("<II")
WANDS_REVISION = "3b74dcf4ba29ab8ff3e6a50b5b09fc627cb882b5"
EXPECTED_DATASET_SHA256 = {
    "product.csv": "d993926254572e6eba96c8fd87cc549a17fb91ad3748308036eee4cf92b10ac6",
    "query.csv": "63b61660560fecc33ec490804c7e2b81402ee3e7c31a9cbb5e03736639f68e95",
    "label.csv": "c11fe81ad62f17f56f316b0ec9630ebe8fbe1393578cb0ca4f05c17253a180ef",
}

# Aliases are collapsed to user-facing fields before matching. Product type is
# intentionally absent because query_class already supplies that association.
FIELD_ALIASES: tuple[tuple[str, frozenset[str]], ...] = (
    ("color", frozenset(("color", "basecolor", "topcolor", "upholsterycolor"))),
    (
        "material",
        frozenset(
            (
                "primarymaterial",
                "framematerial",
                "material",
                "upholsterymaterial",
                "basematerial",
                "topmaterial",
                "materialdetails",
            )
        ),
    ),
    ("style", frozenset(("style", "dsprimaryproductstyle", "dssecondaryproductstyle"))),
    ("shape", frozenset(("shape",))),
    ("finish", frozenset(("finish", "glossfinish"))),
    ("pattern", frozenset(("pattern",))),
    ("size", frozenset(("mattresssize",))),
)
FIELD_BY_ALIAS = {
    alias: field
    for field, aliases in FIELD_ALIASES
    for alias in aliases
}
EXCLUDED_VALUES = frozenset(
    (
        "yes",
        "no",
        "none",
        "other",
        "coffee",  # Common noun falsely exposed as a color in product metadata.
        "light",
        "dark",
    )
)
TOKEN_RE = re.compile(r"[a-z0-9]+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-products", type=int, default=100)
    args = parser.parse_args()
    if args.min_products <= 0:
        parser.error("min-products must be positive")
    return args


def read_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        yield from csv.DictReader(source, delimiter="\t")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_key(raw: str) -> str:
    return "".join(TOKEN_RE.findall(raw.lower()))


def normalize_value(raw: str) -> str:
    return " ".join(TOKEN_RE.findall(raw.lower()))


def parse_product_features(raw: str) -> set[tuple[str, str]]:
    associations: set[tuple[str, str]] = set()
    for item in raw.split("|"):
        if ":" not in item:
            continue
        raw_key, raw_value = item.split(":", 1)
        field = FIELD_BY_ALIAS.get(normalize_key(raw_key))
        value = normalize_value(raw_value)
        if (
            field is None
            or value in EXCLUDED_VALUES
            or not value
            or len(value.split()) > 4
        ):
            continue
        associations.add((field, value))
    return associations


def phrase_span(tokens: list[str], phrase: str) -> tuple[int, int] | None:
    expected = phrase.split()
    width = len(expected)
    for start in range(0, len(tokens) - width + 1):
        if tokens[start : start + width] == expected:
            return start, start + width
    return None


def query_associations(
    query: str,
    supported: dict[str, set[str]],
) -> set[tuple[str, str]]:
    tokens = TOKEN_RE.findall(query.lower())
    candidates: list[tuple[int, int, int, int, str, str]] = []
    field_order = {field: index for index, (field, _) in enumerate(FIELD_ALIASES)}
    for field, values in supported.items():
        for value in values:
            span = phrase_span(tokens, value)
            if span is None:
                continue
            start, end = span
            candidates.append(
                (-len(value.split()), -len(value), field_order[field], start, field, value)
            )
    candidates.sort()

    occupied: set[int] = set()
    selected_fields: set[str] = set()
    selected: set[tuple[str, str]] = set()
    for _, _, _, start, field, value in candidates:
        if field in selected_fields:
            continue
        end = start + len(value.split())
        if any(token_index in occupied for token_index in range(start, end)):
            continue
        selected.add((field, value))
        selected_fields.add(field)
        occupied.update(range(start, end))
    return selected


def encode_rows(
    rows: list[set[tuple[str, str]]],
    field_ids: dict[str, int],
    value_ids: dict[str, dict[str, int]],
) -> tuple[list[int], list[tuple[int, int]]]:
    offsets = [0]
    records: list[tuple[int, int]] = []
    for associations in rows:
        for field, value in sorted(
            associations,
            key=lambda item: (field_ids[item[0]], value_ids[item[0]][item[1]]),
        ):
            records.append((field_ids[field], value_ids[field][value]))
        offsets.append(len(records))
    return offsets, records


def write_u32s(destination, values: Iterable[int]) -> None:
    for value in values:
        destination.write(struct.pack("<I", value))


def main() -> int:
    args = parse_args()
    dataset_digests = {
        name: sha256_file(args.dataset_dir / name)
        for name in ("product.csv", "query.csv", "label.csv")
    }
    if dataset_digests != EXPECTED_DATASET_SHA256:
        raise ValueError(
            f"WANDS files do not match pinned revision {WANDS_REVISION}: {dataset_digests}"
        )

    product_rows = list(read_rows(args.dataset_dir / "product.csv"))
    query_rows = list(read_rows(args.dataset_dir / "query.csv"))
    product_features = [
        parse_product_features(row["product_features"])
        for row in product_rows
    ]

    support: Counter[tuple[str, str]] = Counter()
    for associations in product_features:
        support.update(associations)
    supported = {
        field: {
            value
            for candidate_field, value in support
            if candidate_field == field
            and support[(candidate_field, value)] >= args.min_products
        }
        for field, _ in FIELD_ALIASES
    }
    queries = [
        query_associations(row["query"], supported)
        for row in query_rows
    ]

    # Retain only coordinates actually stated by at least one published query.
    # This prevents unrelated catalog metadata from inflating document bundles.
    used = set().union(*queries)
    products = [associations & used for associations in product_features]
    fields = [
        field
        for field, _ in FIELD_ALIASES
        if any(candidate_field == field for candidate_field, _ in used)
    ]
    field_ids = {field: index for index, field in enumerate(fields)}
    value_ids = {
        field: {
            value: index
            for index, value in enumerate(
                sorted(value for candidate_field, value in used if candidate_field == field)
            )
        }
        for field in fields
    }
    product_offsets, product_records = encode_rows(products, field_ids, value_ids)
    query_offsets, query_records = encode_rows(queries, field_ids, value_ids)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as destination:
        destination.write(
            HEADER.pack(
                MAGIC,
                VERSION,
                len(products),
                len(queries),
                len(fields),
                len(product_records),
                len(query_records),
            )
        )
        write_u32s(destination, product_offsets)
        for field_id, value_id in product_records:
            destination.write(PAIR.pack(field_id, value_id))
        write_u32s(destination, query_offsets)
        for field_id, value_id in query_records:
            destination.write(PAIR.pack(field_id, value_id))

    manifest = {
        "format": MAGIC.decode("ascii"),
        "version": VERSION,
        "dataset_revision": WANDS_REVISION,
        "dataset_files": dataset_digests,
        "min_products": args.min_products,
        "products": len(products),
        "queries": len(queries),
        "fields": fields,
        "values_per_field": {
            field: len(values)
            for field, values in value_ids.items()
        },
        "product_associations": len(product_records),
        "query_associations": len(query_records),
        "queries_with_attributes": sum(bool(row) for row in queries),
        "queries_with_multiple_attributes": sum(len(row) > 1 for row in queries),
        "fixture_sha256": sha256_file(args.output),
    }
    manifest_path = args.output.with_suffix(args.output.suffix + ".json")
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
