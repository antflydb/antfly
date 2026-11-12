#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prepare raw squared-L2 query truth in bounded batches, without normalization.

Training ordinals are one-based physical row positions, independent of authored
IDs. Exact scores use float64 accumulation over original float32 coordinates.
"""

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def matrix(column, dims):
    if column.null_count:
        raise ValueError("null vector")
    offsets = column.offsets.to_numpy(zero_copy_only=False)
    if not np.all(np.diff(offsets) == dims):
        raise ValueError("inconsistent dimensions")
    values = column.values.slice(int(offsets[0]), int(offsets[-1] - offsets[0]))
    if values.null_count:
        raise ValueError("null coordinate")
    result = np.asarray(
        values.to_numpy(zero_copy_only=False), dtype=np.float32
    ).reshape(-1, dims)
    if result.ndim != 2 or result.shape[1] != dims or not np.isfinite(result).all():
        raise ValueError("invalid numeric vector list")
    return result


def parquet(path):
    with path.open("rb") as source:
        magic = source.read(8)
    if magic == b"AFOBJ001":
        mapped = pa.memory_map(str(path), "r")
        body = mapped.read_buffer().slice(84)
        return pq.ParquetFile(pa.BufferReader(body))
    return pq.ParquetFile(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("train", type=Path)
    parser.add_argument("queries", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--column", default="emb")
    parser.add_argument("--dims", type=int, default=768)
    parser.add_argument("--queries-count", type=int, default=100)
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument("--batch", type=int, default=4096)
    args = parser.parse_args()
    if min(args.dims, args.queries_count, args.k, args.batch) < 1:
        parser.error("dimensions, queries, k, and batch must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    query = parquet(args.queries)
    parts = []
    remaining = args.queries_count
    for batch in query.iter_batches(
        batch_size=args.queries_count, columns=[args.column]
    ):
        part = matrix(batch.column(0), args.dims)[:remaining]
        parts.append(part)
        remaining -= len(part)
        if remaining == 0:
            break
    if remaining:
        raise ValueError("too few queries")
    queries = np.concatenate(parts)
    queries.astype("<f4").tofile(args.output / "queries.f32")
    q = queries.astype(np.float64)
    qnorm = np.sum(q * q, axis=1)
    best = np.full((len(q), args.k), np.inf)
    ids = np.zeros((len(q), args.k), dtype=np.uint64)
    offset = 0
    start = time.perf_counter_ns()
    sources = (
        sorted(args.train.glob("buckets/antfly/objects/train-*.parquet"))
        if args.train.is_dir()
        else [args.train]
    )
    if not sources:
        raise ValueError("no training objects")
    source_rows = []
    for source_path in sources:
        before = offset
        for batch in parquet(source_path).iter_batches(
            batch_size=args.batch, columns=[args.column]
        ):
            x = matrix(batch.column(0), args.dims).astype(np.float64)
            scores = qnorm[:, None] + np.sum(x * x, axis=1)[None, :] - 2 * (q @ x.T)
            count = min(args.k, len(x))
            local = np.argpartition(scores, count - 1, axis=1)[:, :count]
            candidate = np.concatenate(
                (best, np.take_along_axis(scores, local, axis=1)), axis=1
            )
            candidate_ids = np.concatenate(
                (ids, local.astype(np.uint64) + offset + 1), axis=1
            )
            selected = np.argpartition(candidate, args.k - 1, axis=1)[:, : args.k]
            best = np.take_along_axis(candidate, selected, axis=1)
            ids = np.take_along_axis(candidate_ids, selected, axis=1)
            offset += len(x)
        source_rows.append(dict(name=source_path.name, rows=offset - before))
    scan_ns = time.perf_counter_ns() - start
    if offset < args.k or not np.isfinite(best).all():
        raise ValueError("incomplete ground truth")
    order = np.argsort(best, axis=1, kind="stable")
    ids = np.take_along_axis(ids, order, axis=1)
    ids.astype("<u8").tofile(args.output / "truth.u64")
    outputs = {}
    for name in ("queries.f32", "truth.u64"):
        outputs[name] = hashlib.sha256((args.output / name).read_bytes()).hexdigest()
    source_sha256 = None
    if args.train.is_file():
        with args.train.open("rb") as source:
            source_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    else:
        fixture = json.loads((args.train / "fixture.json").read_text())
        for source in source_rows:
            expected = next(
                item for item in fixture["files"] if item["name"] == source["name"]
            )
            path = next(path for path in sources if path.name == source["name"])
            with path.open("rb") as stream:
                header = stream.read(84)
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if (
                header[:8] != b"AFOBJ001"
                or header[20:84].decode("ascii") != actual
                or actual != expected["sha256"]
                or path.stat().st_size != expected["bytes"] + 84
            ):
                raise ValueError(f"framed source integrity mismatch: {path}")
            source["sha256"] = actual
    with args.queries.open("rb") as source:
        query_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    report = dict(
        source_sha256=source_sha256,
        query_parquet_sha256=query_sha256,
        sources=source_rows,
        vectors=offset,
        dims=args.dims,
        queries=len(q),
        k=args.k,
        metric="squared_l2",
        normalization="none",
        accumulation="float64",
        exact_scan_ns=scan_ns,
        preparation_ns=time.perf_counter_ns() - start,
        outputs=outputs,
        numpy=np.__version__,
    )
    (args.output / "prepare.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
