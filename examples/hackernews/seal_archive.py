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

"""Seal an existing generation-fenced archive inventory as immutable manifests.

Default output is local. --upload verifies the live inventory and creates only
content-addressed metadata objects under the existing archive prefix; no source
Parquet objects, IAM, buckets or BigQuery jobs are changed.
"""

import argparse
import hashlib
import json
from pathlib import Path


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def build(inventory, source_id, chunk_files=1024):
    if not source_id or not 1 <= chunk_files <= 4096:
        raise ValueError("A source ID and 1–4096 files per chunk are required")
    bucket = inventory["bucket"]
    prefix = inventory["prefix"].strip("/")
    objects = inventory["objects"]
    if (
        not objects
        or len(objects) > 1_000_000
        or inventory["object_count"] != len(objects)
    ):
        raise ValueError("Inventory must contain 1–1,000,000 objects")
    if not bucket or "/" in bucket or not prefix or ".." in prefix.split("/"):
        raise ValueError("Invalid archive bucket/prefix")
    revision = hashlib.sha256(encoded(inventory)).hexdigest()
    destination = f"{prefix}/_snapshots/{revision}"
    files = []
    seen = set()
    for obj in sorted(objects, key=lambda obj: obj["key"]):
        key = obj["key"]
        if (
            not key.startswith(prefix + "/")
            or not key.endswith(".parquet")
            or ".." in key.split("/")
            or key in seen
            or not str(obj["generation"]).isdigit()
            or int(obj["generation"]) <= 0
            or not isinstance(obj["bytes"], int)
            or obj["bytes"] <= 0
            or not isinstance(obj["rows"], int)
            or obj["rows"] < 0
        ):
            raise ValueError("Invalid, unversioned or duplicate Parquet object")
        seen.add(key)
        files.append(
            {
                "file_id": key,
                "object_uri": f"gs://{bucket}/{key}",
                "byte_len": obj["bytes"],
                "row_count": obj["rows"],
                "version_id": str(obj["generation"]),
            }
        )
    artifacts = {}
    chunks = []
    for start in range(0, len(files), chunk_files):
        group = files[start : start + chunk_files]
        payload = encoded(group)
        if len(payload) > 8 * 1024**2:
            raise ValueError("Manifest chunk exceeds 8 MiB; reduce --chunk-files")
        digest = hashlib.sha256(payload).hexdigest()
        name = digest + ".files.json"
        artifacts[name] = payload
        chunks.append(
            {
                "uri": f"gs://{bucket}/{destination}/{name}",
                "sha256": digest,
                "byte_len": len(payload),
                "file_count": len(group),
            }
        )
    if len(chunks) > 4096 or sum(c["byte_len"] for c in chunks) > 64 * 1024**2:
        raise ValueError(
            "Inventory exceeds the engine's bounded manifest metadata limit"
        )
    head = encoded(
        {
            "version": 1,
            "source_id": source_id,
            "chunks": chunks,
            "file_count": len(files),
        }
    )
    if len(head) > 1024**2:
        raise ValueError("Manifest head exceeds 1 MiB")
    name = hashlib.sha256(head).hexdigest() + ".parquet-manifest.json"
    artifacts[name] = head
    return f"gs://{bucket}/{destination}/{name}", destination, artifacts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chunk-files", type=int, default=1024)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    uri, destination, artifacts = build(inventory, args.source_id, args.chunk_files)
    args.output.mkdir(parents=True, exist_ok=True)
    for name, payload in artifacts.items():
        path = args.output / name
        if path.exists() and path.read_bytes() != payload:
            raise ValueError(f"Local immutable manifest differs: {name}")
        path.write_bytes(payload)
    if args.upload:
        from archive_inventory import storage_client, verify_inventory
        from google.api_core.exceptions import PreconditionFailed

        verify_inventory(inventory)
        bucket = storage_client(inventory["project"]).bucket(inventory["bucket"])
        # Publish the head last, after every referenced immutable chunk exists.
        for name, payload in artifacts.items():
            blob = bucket.blob(f"{destination}/{name}")
            try:
                blob.upload_from_string(
                    payload, content_type="application/json", if_generation_match=0
                )
            except PreconditionFailed:
                blob.reload()
                existing = blob.download_as_bytes(
                    if_generation_match=int(blob.generation)
                )
                if existing != payload:
                    raise ValueError(f"Remote immutable manifest differs: {name}")
    print(
        json.dumps(
            {
                "uri": uri,
                "files": inventory["object_count"],
                "metadata_objects": len(artifacts),
                "uploaded": args.upload,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
