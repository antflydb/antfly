# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Independent compact-encoding fixtures shared by native and public E2Es.

Regenerate native fixtures with the lake-extra Python environment:
python zig/e2e/antfly/lake_encoded_fixtures.py \
  zig/pkg/antfly-embedded/src/serverless/query/testdata
"""

from decimal import Decimal
from pathlib import Path


def write_compact_parquet(path: Path, page_version: int) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    count = 300
    table = pa.table(
        {
            "amount": pa.array(
                [None if i % 17 == 0 else i for i in range(count)], type=pa.int64()
            ),
            "small": pa.array([i - 150 for i in range(count)], type=pa.int32()),
            "n": pa.array(
                [None if i % 11 == 0 else i + 0.25 for i in range(count)],
                type=pa.float64(),
            ),
            "real": pa.array([i + 0.5 for i in range(count)], type=pa.float32()),
            "label": pa.array(
                [None if i % 13 == 0 else f"prefix-{i:05d}" for i in range(count)]
            ),
            "short": pa.array([None if i % 19 == 0 else str(i) for i in range(count)]),
            "bsint": pa.array([i * 13 - 1000 for i in range(count)], type=pa.int64()),
            "exact": pa.array(
                [Decimal(i - 100) / 100 for i in range(count)],
                type=pa.decimal128(12, 2),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        use_dictionary=False,
        column_encoding={
            "amount": "DELTA_BINARY_PACKED",
            "small": "DELTA_BINARY_PACKED",
            "n": "BYTE_STREAM_SPLIT",
            "real": "BYTE_STREAM_SPLIT",
            "label": "DELTA_BYTE_ARRAY",
            "short": "DELTA_LENGTH_BYTE_ARRAY",
            "bsint": "BYTE_STREAM_SPLIT",
            "exact": "BYTE_STREAM_SPLIT",
        },
        compression="snappy",
        data_page_size=256,
        write_batch_size=32,
        data_page_version=f"{page_version}.0",
        write_page_index=True,
    )


if __name__ == "__main__":
    import sys

    directory = Path(sys.argv[1])
    directory.mkdir(parents=True, exist_ok=True)
    for version in (1, 2):
        write_compact_parquet(
            directory / f"pyarrow_encoded_v{version}.parquet", version
        )
