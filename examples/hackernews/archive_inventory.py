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

"""Inventory immutable GCS Parquet footers without downloading archive bodies."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import subprocess

from google.cloud import storage
from google.oauth2.credentials import Credentials
import pyarrow.parquet as parquet


def storage_client(project):
    token = subprocess.run(
        ["gcloud", "auth", "print-access-token", "--project", project],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return storage.Client(project=project, credentials=Credentials(token))


def verify_inventory(expected):
    client = storage_client(expected["project"])
    actual = {
        b.name: (str(b.generation), b.size, b.crc32c)
        for b in client.list_blobs(
            expected["bucket"], prefix=expected["prefix"].rstrip("/") + "/"
        )
        if b.name.endswith(".parquet")
    }
    identities = {
        o["key"]: (o["generation"], o["bytes"], o["crc32c"])
        for o in expected["objects"]
    }
    if actual != identities:
        raise ValueError(
            "Live archive differs from the pinned inventory; capture and review a new inventory"
        )


def inventory(project, bucket_name, prefix, workers=4):
    client = storage_client(project)
    blobs = sorted(
        (
            b
            for b in client.list_blobs(bucket_name, prefix=prefix.rstrip("/") + "/")
            if b.name.endswith(".parquet")
        ),
        key=lambda b: b.name,
    )
    if not blobs:
        raise ValueError("No Parquet objects found")

    def inspect(blob):
        tail = blob.download_as_bytes(
            start=blob.size - 8,
            end=blob.size - 1,
            if_generation_match=int(blob.generation),
            timeout=30,
        )
        if tail[4:] != b"PAR1":
            raise ValueError(f"Invalid Parquet footer: {blob.name}")
        length = int.from_bytes(tail[:4], "little")
        if length > 16 * 1024 * 1024 or length + 8 > blob.size:
            raise ValueError(f"Unbounded Parquet footer: {blob.name}")
        footer = blob.download_as_bytes(
            start=blob.size - length - 8,
            end=blob.size - 1,
            if_generation_match=int(blob.generation),
            timeout=30,
        )
        metadata = parquet.read_metadata(io.BytesIO(b"PAR1" + footer))
        groups = []
        for i in range(metadata.num_row_groups):
            group = metadata.row_group(i)
            groups.append(
                {
                    "rows": group.num_rows,
                    "columns": {
                        group.column(j).path_in_schema: {
                            "compressed_bytes": group.column(j).total_compressed_size,
                            "decoded_bytes": group.column(j).total_uncompressed_size,
                        }
                        for j in range(group.num_columns)
                    },
                }
            )
        return {
            "key": blob.name,
            "generation": str(blob.generation),
            "bytes": blob.size,
            "crc32c": blob.crc32c,
            "rows": metadata.num_rows,
            "row_groups": groups,
        }

    with ThreadPoolExecutor(max_workers=workers) as pool:
        objects = list(pool.map(inspect, blobs))
    return {
        "project": project,
        "bucket": bucket_name,
        "prefix": prefix,
        "object_count": len(objects),
        "bytes": sum(o["bytes"] for o in objects),
        "rows": sum(o["rows"] for o in objects),
        "objects": objects,
        "note": "Generation-fenced footer inventory; source bodies were not downloaded.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = inventory(args.project, args.bucket, args.prefix)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps({k: v for k, v in result.items() if k != "objects"}))


if __name__ == "__main__":
    main()
