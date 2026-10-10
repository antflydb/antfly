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

"""Prepare pinned MASSIVE test slices for CUDA implementation parity.

Download the exact files in family/holdout/source.json into --source-dir as
<locale>.parquet. Requires pyarrow 23.0.1. These are public test examples, not a
claim that the checkpoints never saw them. Gold accuracy is not measured here.
"""

import argparse
import hashlib
import json
from pathlib import Path

from family_oracle import FIXTURES


def select_rows(rows, count):
    """Choose before inference, deterministically, with distinct batch texts."""
    if len({row["id"] for row in rows}) != len(rows):
        raise ValueError("duplicate source row IDs")
    selected, seen = [], set()
    for row in sorted(rows, key=lambda r: hashlib.sha256(r["id"].encode()).digest()):
        if row["partition"] != "test":
            raise ValueError("non-test source row")
        if row["utt"] not in seen:
            selected.append(row)
            seen.add(row["utt"])
        if len(selected) == count:
            return selected
    raise ValueError("insufficient distinct source rows")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import pyarrow.parquet as pq

    source = json.loads((FIXTURES / "holdout/source.json").read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    for filename, pin in source["files"].items():
        locale = filename.split("/")[0]
        path = args.source_dir / (locale + ".parquet")
        data = path.read_bytes()
        if (
            len(data) != pin["size_bytes"]
            or hashlib.sha256(data).hexdigest() != pin["sha256"]
        ):
            raise ValueError(f"source hash mismatch: {locale}")
        table = pq.read_table(path)
        labels = json.loads(table.schema.metadata[b"huggingface"])["info"]["features"][
            "intent"
        ]["names"]
        rows = select_rows(table.to_pylist(), 200)
        if any(row["locale"] != locale for row in rows):
            raise ValueError("source language mismatch")
        cases = [
            dict(
                id=f"{locale}_{index // 8:02d}",
                texts=[r["utt"] for r in rows[index : index + 8]],
                source_ids=[r["id"] for r in rows[index : index + 8]],
                tasks={"intent": labels},
            )
            for index in range(0, len(rows), 8)
        ]
        fixture = dict(
            format_version=1,
            dataset=source["dataset"],
            revision=source["revision"],
            split="test",
            locale=locale,
            source_file=filename,
            source_pin=pin,
            selection="first_200_distinct_texts_by_sha256_source_id",
            cases=cases,
        )
        (args.output / (locale + ".json")).write_text(
            json.dumps(fixture, ensure_ascii=False) + "\n"
        )


if __name__ == "__main__":
    main()
