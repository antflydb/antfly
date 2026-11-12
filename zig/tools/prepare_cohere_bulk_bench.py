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

"""Prepare normalized Cohere VectorDBBench Parquet for the full-build probe."""

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


DIMS = 768
K = 100


def vectors(column):
    lengths = column.value_lengths().to_numpy(zero_copy_only=False)
    if not np.all(lengths == DIMS) or column.null_count:
        raise ValueError("expected non-null 768-dimensional embeddings")
    matrix = np.array(
        column.flatten().to_numpy(zero_copy_only=False), dtype="<f4", copy=True
    ).reshape(-1, DIMS)
    norms = np.linalg.norm(matrix, axis=1)
    if not np.all(np.isfinite(norms) & (norms > 0)):
        raise ValueError("invalid vector norm")
    matrix /= norms[:, None]
    return matrix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--limit", type=int, help="Use a prefix for a smoke run")
    args = parser.parse_args()
    train = pq.ParquetFile(args.source / "train.parquet")
    count = train.metadata.num_rows
    if args.limit is not None:
        if not K <= args.limit <= count:
            parser.error("--limit must be between 100 and the corpus size")
        count = args.limit
    if args.queries < 1:
        parser.error("--queries must be positive")
    args.output.mkdir(parents=True, exist_ok=False)

    started = time.perf_counter_ns()
    offset = 0
    with (args.output / "train.f32").open("wb") as out:
        for batch in train.iter_batches(batch_size=4096, columns=["id", "emb"]):
            batch = batch.slice(0, min(batch.num_rows, count - offset))
            ids = batch.column(0).to_numpy()
            if not np.array_equal(ids, np.arange(offset, offset + len(ids))):
                raise ValueError("expected ordered, contiguous training IDs")
            vectors(batch.column(1)).tofile(out)
            offset += len(ids)
            if offset == count:
                break
    scan_ns = time.perf_counter_ns() - started
    if offset != count:
        raise ValueError("training row count mismatch")

    test = pq.read_table(args.source / "test.parquet", columns=["id", "emb"])
    test = test.slice(0, args.queries).combine_chunks()
    queries = vectors(test["emb"].chunk(0))
    queries.tofile(args.output / "queries.f32")
    if count == train.metadata.num_rows:
        gt = pq.read_table(args.source / "neighbors.parquet")
        neighbors = dict(zip(gt["id"].to_pylist(), gt["neighbors_id"].to_pylist()))
        truth = np.asarray(
            [neighbors[key][:K] for key in test["id"].to_pylist()], dtype="<i8"
        )
        if truth.shape != (len(queries), K) or not np.all(
            (truth >= 0) & (truth < count)
        ):
            raise ValueError("invalid ground truth")
    else:
        matrix = np.memmap(
            args.output / "train.f32", dtype="<f4", mode="r", shape=(count, DIMS)
        )
        truth = np.empty((len(queries), K), dtype="<i8")
        for index, query in enumerate(queries):
            scores = matrix @ query
            truth[index] = np.argpartition(scores, -K)[-K:]
    # The Zig external-vector loader uses one-based row IDs.
    (truth + 1).astype("<u8").tofile(args.output / "truth.u64")
    report = dict(
        vectors=count,
        dims=DIMS,
        queries=len(queries),
        parquet_scan_normalize_write_ns=scan_ns,
    )
    (args.output / "prepare.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
