#!/usr/bin/env python3
"""Prepare a pinned WANDS corpus for the Zig HDC quality benchmark.

The output is a compact little-endian binary fixture:

    8s magic ("AFHDCW01")
    u32 version
    u32 embedding dimensions
    u32 product count
    u32 query count
    u32 qrel count
    u32 max token length
    f32 product_embeddings[product_count][dimensions]
    f32 query_embeddings[query_count][dimensions]
    u32 product_ids[product_count]
    u32 product_class_ids[product_count]
    u32 product_category_ids[product_count]
    u32 query_ids[query_count]
    u32 query_class_ids[query_count]
    { u32 query_index, u32 product_index, u8 gain, u8[3] padding } qrels[qrel_count]

Run with the inference E2E environment, which owns numpy, transformers, and
onnxruntime:

    UV_CACHE_DIR=/tmp/uv-cache uv run --project e2e/inference \
      python tools/prepare_hdc_wands_fixture.py ...
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import struct
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import onnxruntime as ort
from transformers import AutoTokenizer

MAGIC = b"AFHDCW01"
VERSION = 1
HEADER = struct.Struct("<8s6I")
QREL = struct.Struct("<IIB3x")
GAIN = {"Irrelevant": 0, "Partial": 1, "Exact": 2}
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
WANDS_REVISION = "3b74dcf4ba29ab8ff3e6a50b5b09fc627cb882b5"
BGE_REVISION = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
EXPECTED_DATASET_SHA256 = {
    "product.csv": "d993926254572e6eba96c8fd87cc549a17fb91ad3748308036eee4cf92b10ac6",
    "query.csv": "63b61660560fecc33ec490804c7e2b81402ee3e7c31a9cbb5e03736639f68e95",
    "label.csv": "c11fe81ad62f17f56f316b0ec9630ebe8fbe1393578cb0ca4f05c17253a180ef",
}
EXPECTED_MODEL_SHA256 = {
    "onnx/model.onnx": "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
    "tokenizer.json": "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
}


@dataclass(frozen=True)
class Product:
    product_id: int
    text: str
    product_class: str
    category: str


@dataclass(frozen=True)
class Query:
    query_id: int
    text: str
    query_class: str


@dataclass(frozen=True)
class Qrel:
    query_index: int
    product_index: int
    gain: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--max-products", type=int)
    parser.add_argument("--max-queries", type=int)
    parser.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument(
        "--query-prefix",
        default=BGE_QUERY_PREFIX,
        help="Retrieval instruction prepended to query text; pass an empty string to disable.",
    )
    args = parser.parse_args()
    if args.batch_size <= 0 or args.max_tokens <= 0 or args.threads <= 0:
        parser.error("batch-size, max-tokens, and threads must be positive")
    return args


def read_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        yield from csv.DictReader(source, delimiter="\t")


def load_dataset(
    dataset_dir: Path,
    max_products: int | None,
    max_queries: int | None,
) -> tuple[list[Product], list[Query], list[Qrel]]:
    products: list[Product] = []
    product_index: dict[int, int] = {}
    for row in read_rows(dataset_dir / "product.csv"):
        if max_products is not None and len(products) >= max_products:
            break
        product_id = int(row["product_id"])
        product_index[product_id] = len(products)
        name = row["product_name"].strip()
        description = row["product_description"].strip()
        text = f"{name}. {description}" if description else name
        products.append(
            Product(
                product_id=product_id,
                text=text,
                product_class=row["product_class"].strip(),
                category=row["category hierarchy"].strip(),
            )
        )

    queries: list[Query] = []
    query_index: dict[int, int] = {}
    for row in read_rows(dataset_dir / "query.csv"):
        if max_queries is not None and len(queries) >= max_queries:
            break
        query_id = int(row["query_id"])
        query_index[query_id] = len(queries)
        queries.append(
            Query(
                query_id=query_id,
                text=row["query"].strip(),
                query_class=row["query_class"].strip(),
            )
        )

    qrels: list[Qrel] = []
    for row in read_rows(dataset_dir / "label.csv"):
        query_offset = query_index.get(int(row["query_id"]))
        product_offset = product_index.get(int(row["product_id"]))
        if query_offset is None or product_offset is None:
            continue
        qrels.append(
            Qrel(
                query_index=query_offset,
                product_index=product_offset,
                gain=GAIN[row["label"]],
            )
        )
    return products, queries, qrels


def intern(values: Iterable[str]) -> tuple[np.ndarray, list[str]]:
    ids: dict[str, int] = {}
    encoded: list[int] = []
    for value in values:
        encoded.append(ids.setdefault(value, len(ids)))
    labels = [""] * len(ids)
    for value, value_id in ids.items():
        labels[value_id] = value
    return np.asarray(encoded, dtype="<u4"), labels


def create_session(model_dir: Path, threads: int) -> ort.InferenceSession:
    options = ort.SessionOptions()
    options.intra_op_num_threads = threads
    options.inter_op_num_threads = 1
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    return ort.InferenceSession(
        str(model_dir / "onnx" / "model.onnx"),
        sess_options=options,
        providers=["CPUExecutionProvider"],
    )


def embed_texts(
    session: ort.InferenceSession,
    tokenizer: AutoTokenizer,
    texts: list[str],
    batch_size: int,
    max_tokens: int,
    label: str,
) -> np.ndarray:
    output_shape = session.get_outputs()[0].shape
    dimensions = int(output_shape[-1])
    embeddings = np.empty((len(texts), dimensions), dtype="<f4")
    input_names = {item.name for item in session.get_inputs()}
    started = time.monotonic()
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        tokens = tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=max_tokens,
            return_tensors="np",
        )
        inputs = {
            name: np.asarray(value, dtype=np.int64)
            for name, value in tokens.items()
            if name in input_names
        }
        hidden = session.run(None, inputs)[0]
        pooled = hidden[:, 0, :]
        norms = np.linalg.norm(pooled, axis=1, keepdims=True)
        pooled = pooled / np.maximum(norms, np.finfo(np.float32).tiny)
        embeddings[start : start + len(batch)] = pooled.astype("<f4", copy=False)
        completed = start + len(batch)
        if completed == len(texts) or completed % (batch_size * 10) == 0:
            elapsed = time.monotonic() - started
            rate = completed / elapsed if elapsed else 0
            print(
                f"{label}: {completed}/{len(texts)} ({rate:.1f} texts/s)",
                file=sys.stderr,
                flush=True,
            )
    return embeddings


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_fixture(
    path: Path,
    product_embeddings: np.ndarray,
    query_embeddings: np.ndarray,
    products: list[Product],
    queries: list[Query],
    qrels: list[Qrel],
    product_classes: np.ndarray,
    product_categories: np.ndarray,
    query_classes: np.ndarray,
    max_tokens: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as destination:
        destination.write(
            HEADER.pack(
                MAGIC,
                VERSION,
                product_embeddings.shape[1],
                len(products),
                len(queries),
                len(qrels),
                max_tokens,
            )
        )
        destination.write(product_embeddings.tobytes(order="C"))
        destination.write(query_embeddings.tobytes(order="C"))
        destination.write(np.asarray([item.product_id for item in products], dtype="<u4").tobytes())
        destination.write(product_classes.tobytes())
        destination.write(product_categories.tobytes())
        destination.write(np.asarray([item.query_id for item in queries], dtype="<u4").tobytes())
        destination.write(query_classes.tobytes())
        for qrel in qrels:
            destination.write(QREL.pack(qrel.query_index, qrel.product_index, qrel.gain))


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
    model_digests = {
        name: sha256_file(args.model_dir / name)
        for name in ("onnx/model.onnx", "tokenizer.json")
    }
    if model_digests != EXPECTED_MODEL_SHA256:
        raise ValueError(
            f"embedding model does not match pinned revision {BGE_REVISION}: {model_digests}"
        )

    products, queries, qrels = load_dataset(
        args.dataset_dir,
        args.max_products,
        args.max_queries,
    )
    if not products or not queries or not qrels:
        raise ValueError("selected WANDS fixture must contain products, queries, and qrels")

    all_classes = [item.product_class for item in products] + [item.query_class for item in queries]
    all_class_ids, class_labels = intern(all_classes)
    product_classes = all_class_ids[: len(products)]
    query_classes = all_class_ids[len(products) :]
    product_categories, category_labels = intern(item.category for item in products)

    tokenizer = AutoTokenizer.from_pretrained(args.model_dir, local_files_only=True)
    session = create_session(args.model_dir, args.threads)
    product_embeddings = embed_texts(
        session,
        tokenizer,
        [item.text for item in products],
        args.batch_size,
        args.max_tokens,
        "products",
    )
    query_embeddings = embed_texts(
        session,
        tokenizer,
        [f"{args.query_prefix}{item.text}" for item in queries],
        args.batch_size,
        args.max_tokens,
        "queries",
    )
    write_fixture(
        args.output,
        product_embeddings,
        query_embeddings,
        products,
        queries,
        qrels,
        product_classes,
        product_categories,
        query_classes,
        args.max_tokens,
    )

    manifest = {
        "format": MAGIC.decode("ascii"),
        "version": VERSION,
        "products": len(products),
        "queries": len(queries),
        "qrels": len(qrels),
        "dimensions": int(product_embeddings.shape[1]),
        "classes": len(class_labels),
        "categories": len(category_labels),
        "max_tokens": args.max_tokens,
        "query_prefix": args.query_prefix,
        "dataset_revision": WANDS_REVISION,
        "dataset_files": dataset_digests,
        "model_revision": BGE_REVISION,
        "model_files": model_digests,
        "fixture_sha256": sha256_file(args.output),
    }
    manifest_path = args.output.with_suffix(args.output.suffix + ".json")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
