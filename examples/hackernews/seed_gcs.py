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

"""Freeze a GCS seed inventory, or seed from an explicitly pinned generation."""

import argparse
import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
from urllib.parse import urlsplit

import google_crc32c
from google.cloud import storage


def inventory(client, bucket, prefix, max_bytes):
    objects = []
    size = 0
    for blob in client.list_blobs(bucket, prefix=prefix):
        if not blob.name.endswith(".parquet"):
            continue
        size += int(blob.size)
        if len(objects) >= 10000 or size > max_bytes:
            raise ValueError("Seed inventory exceeds its object/byte budget")
        objects.append(
            {
                "name": blob.name,
                "generation": str(blob.generation),
                "size": int(blob.size),
                "crc32c": blob.crc32c,
            }
        )
    if not objects:
        raise ValueError("Seed inventory is empty")
    return {
        "version": 1,
        "bucket": bucket,
        "objects": sorted(objects, key=lambda x: x["name"]),
    }


def validated_objects(manifest, bucket, max_bytes):
    if manifest.get("version") != 1 or manifest.get("bucket") != bucket:
        raise ValueError("Seed manifest bucket/version mismatch")
    objects = manifest["objects"]
    if not 1 <= len(objects) <= 10000:
        raise ValueError("Invalid seed object count")
    total, names = 0, set()
    for obj in objects:
        path = PurePosixPath(obj["name"])
        generation = str(obj["generation"])
        crc = base64.b64decode(obj["crc32c"], validate=True)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in obj["name"]
            or not obj["name"].startswith("archive/")
            or path.suffix != ".parquet"
            or obj["name"] in names
            or not generation.isdecimal()
            or int(generation) < 1
            or type(obj["size"]) is not int
            or obj["size"] < 1
            or len(crc) != 4
        ):
            raise ValueError("Invalid immutable seed object")
        total += obj["size"]
        names.add(obj["name"])
    if total > max_bytes:
        raise ValueError("Seed exceeds its admitted byte budget")
    return objects


def matches(path, obj):
    if not path.is_file() or path.stat().st_size != obj["size"]:
        return False
    crc = google_crc32c.Checksum()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            crc.update(chunk)
    return base64.b64encode(crc.digest()).decode() == obj["crc32c"]


def stage(client, manifest, bucket, destination, max_bytes):
    objects = validated_objects(manifest, bucket, max_bytes)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    paths = []
    for number, obj in enumerate(objects):
        # Local names never come from the object key; no traversal or symlinks.
        path = destination / f"part-{number:05d}.parquet"
        if path.is_symlink():
            raise ValueError("Seed destination contains a symlink")
        if not matches(path, obj):
            if shutil.disk_usage(destination).free < obj["size"] + 1024 * 1024:
                raise RuntimeError("Insufficient seed staging disk")
            partial = destination / f"part-{number:05d}.download"
            if partial.exists() or partial.is_symlink():
                partial.unlink()
            client.bucket(bucket).blob(
                obj["name"], generation=int(obj["generation"])
            ).download_to_filename(
                partial, if_generation_match=int(obj["generation"]), checksum="crc32c"
            )
            if not matches(partial, obj):
                raise ValueError("Seed object checksum/size mismatch")
            partial.replace(path)
        paths.append(path)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("inventory")
    freeze.add_argument("--bucket", required=True)
    freeze.add_argument("--prefix", default="archive/")
    run = commands.add_parser("seed")
    run.add_argument("--manifest", required=True)
    run.add_argument("--generation", type=int, required=True)
    run.add_argument("--recent-since", type=int, required=True)
    run.add_argument("--state", type=Path, default=Path("/state"))
    run.add_argument("--endpoint", required=True)
    run.add_argument("--warehouse", required=True)
    run.add_argument("--backup-root", required=True)
    args = parser.parse_args()
    if args.max_bytes < 1:
        parser.error("max-bytes must be positive")
    client = storage.Client(project=args.project)
    if args.command == "inventory":
        print(
            json.dumps(
                inventory(client, args.bucket, args.prefix, args.max_bytes),
                sort_keys=True,
            )
        )
        return
    from ingest import State, backup_state, publish, writer_lock
    from managed_api import authorization_headers, validate_backend
    from seed_current import seed_current

    validate_backend(args.endpoint, bool(authorization_headers()))
    uri = urlsplit(args.manifest)
    bucket = args.project + "-hn-archive"
    if (
        uri.scheme != "gs"
        or uri.netloc != bucket
        or uri.query
        or uri.fragment
        or not uri.path.startswith("/archive/")
        or args.generation < 1
    ):
        parser.error("Seed requires an immutable manifest in the HN archive bucket")
    raw = (
        client.bucket(bucket)
        .blob(uri.path.lstrip("/"), generation=args.generation)
        .download_as_bytes(
            if_generation_match=args.generation, start=0, end=8 * 1024 * 1024
        )
    )
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("Seed manifest exceeds 8 MiB")
    manifest = json.loads(raw)
    validated_objects(manifest, bucket, args.max_bytes)
    binding = json.dumps(
        {
            "manifest_sha256": hashlib.sha256(raw).hexdigest(),
            "uri": args.manifest,
            "generation": args.generation,
            "since": args.recent_since,
        },
        sort_keys=True,
    )
    with writer_lock(args.state):
        state = State(args.state / "ingestion.aflite")
        try:
            previous = state.get("seed_gcs_binding", "")
            if previous and previous != binding:
                raise RuntimeError("Seed manifest binding changed")
            if not previous and state.get("native_changes_checkpoint", ""):
                raise RuntimeError("GCS seed requires an unpublished writer")
            state.set("seed_gcs_binding", binding)
            if not state.get("initial_current_seed_complete", ""):
                paths = stage(
                    client, manifest, bucket, args.state / "archive", args.max_bytes
                )
                seed_current(
                    state,
                    paths,
                    args.recent_since,
                    lambda: publish(
                        state,
                        args.state,
                        args.warehouse,
                        args.project,
                        args.endpoint,
                        "hn_current",
                        True,
                        1000,
                    ),
                )
            backup_state(
                state,
                args.state,
                args.backup_root,
                args.warehouse,
                args.project,
                args.endpoint,
                "hn_current",
            )
            print(
                json.dumps(
                    {"seeded": True, "manifest_sha256": hashlib.sha256(raw).hexdigest()}
                )
            )
        finally:
            state.db.close()


if __name__ == "__main__":
    main()
