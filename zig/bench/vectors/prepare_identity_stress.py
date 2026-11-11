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

"""Generate bounded Parquet input for bulk identity-admission scale checks.

Requires PyArrow; coordinates are synthetic and are not a recall benchmark.
"""

import argparse
from array import array
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, default=3_000_000)
    parser.add_argument("--row-group-rows", type=int, default=100_000)
    args = parser.parse_args()
    if args.rows <= 0 or not 0 < args.row_group_rows <= 1_000_000:
        parser.error("rows must be positive and row-group-rows must be 1..1000000")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("id", pa.int64()), ("emb", pa.list_(pa.float32()))])
    with pq.ParquetWriter(
        args.output,
        schema,
        compression="zstd",
        use_dictionary=False,
        data_page_version="2.0",
        store_schema=False,
    ) as writer:
        for start in range(0, args.rows, args.row_group_rows):
            count = min(args.row_group_rows, args.rows - start)
            values = array("f")
            for i in range(start, start + count):
                values.extend(
                    (
                        i % 997 / 997,
                        i * 37 % 991 / 991,
                        i * 101 % 983 / 983,
                        i * 193 % 977 / 977,
                    )
                )
            flat = pa.Array.from_buffers(
                pa.float32(), len(values), [None, pa.py_buffer(values)]
            )
            vectors = pa.ListArray.from_arrays(
                pa.array(range(0, len(values) + 1, 4), type=pa.int32()), flat
            )
            writer.write_table(
                pa.Table.from_arrays(
                    [pa.array(range(start, start + count), type=pa.int64()), vectors],
                    schema=schema,
                )
            )
    digest = hashlib.sha256()
    with args.output.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    print(
        json.dumps(
            {
                "rows": args.rows,
                "dims": 4,
                "row_group_rows": args.row_group_rows,
                "bytes": args.output.stat().st_size,
                "sha256": digest.hexdigest(),
                "pyarrow": pa.__version__,
            }
        )
    )


if __name__ == "__main__":
    main()
