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

"""Normalize an HN BigQuery export into flat, DataPageV2 Parquet for Antfly.

Run with: uv run --with pyarrow python normalize.py INPUT OUTPUT
"""

import argparse
from html.parser import HTMLParser
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ("p", "br", "pre", "div", "li"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("p", "pre", "div", "li"):
            self.parts.append("\n")


def plain(value):
    parser = PlainText()
    parser.feed(value or "")
    return " ".join("".join(parser.parts).split())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    source = pq.read_table(args.input)
    records = source.to_pylist()
    for row in records:
        row["title"] = plain(row["title"])
        row["body"] = "\n".join(filter(None, (row["title"], plain(row["text_html"]))))
    normalized = pa.Table.from_pylist(records, schema=source.schema)
    pq.write_table(
        normalized,
        args.output,
        compression="snappy",
        use_dictionary=True,
        row_group_size=1024,
        data_page_size=16384,
        write_batch_size=256,
        data_page_version="2.0",
        write_page_index=True,
    )
    print(
        json.dumps(
            {
                "rows": len(records),
                "input_bytes": args.input.stat().st_size,
                "output_bytes": args.output.stat().st_size,
                "date_range_epoch": [
                    min(r["created_at"] for r in records),
                    max(r["created_at"] for r in records),
                ],
                "item_types": {
                    kind: sum(r["item_type"] == kind for r in records)
                    for kind in sorted({r["item_type"] for r in records})
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
