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

"""Durable, bounded HN ingestion and Iceberg publication.

Run with the dependencies in pyproject.toml. Mount --state on persistent storage.
One writer owns the state directory and warehouse. GCS uses application default
credentials (Workload Identity in GKE); no service-account keys are required.
"""

import argparse
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import time
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen

from lite_state import State
from lite_catalog import HackernewsCatalog


@contextmanager
def writer_lock(state):
    state.mkdir(parents=True, exist_ok=True)
    with (state / "writer.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def arrow_schema():
    import pyarrow as pa

    integers = {
        "hn_id",
        "points",
        "created_at",
        "parent_id",
        "comment_count",
        "root_story_id",
    }
    names = [
        "hn_id",
        "title",
        "url",
        "text_html",
        "body",
        "author",
        "points",
        "created_at",
        "item_type",
        "parent_id",
        "comment_count",
        "domain",
        "root_story_id",
        "created_month",
    ]
    return pa.schema(
        [
            pa.field(name, pa.int64() if name in integers else pa.string())
            for name in names
        ]
    )


class Publisher:
    """Publish immutable metadata then atomically move Antfly's commit pointer."""

    def __init__(self, root, project=None):
        self.root = root.rstrip("/")
        self.gcs = None
        if root.startswith("gs://"):
            from google.cloud import storage

            parsed = urlsplit(root)
            self.gcs = storage.Client(project=project).bucket(parsed.netloc)
            self.prefix = parsed.path.strip("/")
        else:
            self.local = Path(unquote(urlsplit(root).path)).resolve()

    def read(self, name):
        if self.gcs:
            from google.api_core.exceptions import NotFound

            blob = self.gcs.blob(self.prefix + "/" + name)
            try:
                blob.reload()
                return blob.download_as_bytes(if_generation_match=blob.generation), str(
                    blob.generation
                )
            except NotFound:
                return None, "0"
        path = self.local / name
        if not path.exists():
            return None, "0"
        import hashlib

        content = path.read_bytes()
        return content, hashlib.sha256(content).hexdigest()

    def put(self, name, body, expected):
        if self.gcs:
            self.gcs.blob(self.prefix + "/" + name).upload_from_string(
                body, if_generation_match=int(expected)
            )
        else:
            import os

            if self.read(name)[1] != expected:
                raise RuntimeError("publication pointer conflict")
            path = self.local / name
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(path.name + ".pending")
            with temp.open("wb") as output:
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp, path)
            directory = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def commit(self, metadata, expected):
        current, generation = self.read("metadata/version-hint.text")
        if generation != expected:
            # A lost successful CAS response is replayable after restart.
            if (
                current
                and self.read(f"metadata/v{int(current)}.metadata.json")[0] == metadata
            ):
                return int(current)
            raise RuntimeError("another publisher changed the Iceberg commit pointer")
        version = int(current or b"0") + 1
        name = f"metadata/v{version}.metadata.json"
        existing, _ = self.read(name)
        if existing is None:
            self.put(name, metadata, "0")
        elif existing != metadata:
            raise RuntimeError(
                "uncommitted metadata collision; preserve it for recovery"
            )
        self.put("metadata/version-hint.text", f"{version}\n".encode(), expected)
        return version


def publish(
    state,
    directory,
    warehouse,
    project=None,
    native_endpoint=None,
    native_table="hackernews",
    native_rows=False,
    batch_size=1000,
):
    from native_catalog import NativeCatalog
    import pyarrow as pa
    import pyarrow.parquet as pq
    import uuid
    from pyiceberg.expressions import In
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    if native_rows:
        from native_ingest import publish as publish_changes

        if not native_endpoint:
            raise ValueError("native row ingestion requires --native-endpoint")
        if state.get("publication", ""):
            raise RuntimeError(
                "finish the pending file publication before changing producer mode"
            )
        catalog = NativeCatalog(state, warehouse, native_endpoint, native_table)
        catalog.create_table_if_not_exists(
            "hackernews.items",
            schema=arrow_schema(),
            location=warehouse,
            properties={"format-version": "2"},
        )
        return publish_changes(state, native_endpoint, native_table, batch_size)

    catalog = (
        NativeCatalog(state, warehouse, native_endpoint, native_table)
        if native_endpoint
        else HackernewsCatalog(state, warehouse)
    )
    catalog.create_namespace_if_not_exists("hackernews")
    schema = arrow_schema()
    table = catalog.create_table_if_not_exists(
        "hackernews.items",
        schema=schema,
        location=warehouse,
        properties={"format-version": "2"},
    )
    if table.spec().is_unpartitioned():
        with table.update_spec() as update:
            update.add_identity("created_month")
    publisher = Publisher(warehouse, project)
    pending = state.get("publication", "")
    if pending:
        journal = json.loads(pending)
        if (
            journal["warehouse"] != warehouse
            or journal.get("native_endpoint") != native_endpoint
            or journal.get("native_table", "hackernews") != native_table
        ):
            raise RuntimeError("pending publication belongs to a different warehouse")
        months = journal["months"]
    else:
        months = state.dirty_months()
        if not months:
            return None
        _, generation = (
            (None, None)
            if native_endpoint
            else publisher.read("metadata/version-hint.text")
        )
        # Retry copy-on-write replacements; never append duplicate HN IDs.
        # PyIceberg 0.12 cannot stream RecordBatchReader into partitioned
        # tables. Write bounded month-homogeneous Parquet files, then register
        # their footer statistics in one delete/add-files transaction.
        files = []
        generation_id = uuid.uuid4().hex
        for month in months:
            for number, batch in enumerate(state.batches([month], schema)):
                uri = f"{warehouse.rstrip('/')}/data/{generation_id}/{month}/{number:08d}.parquet"
                with table.io.new_output(uri).create(overwrite=False) as output:
                    pq.write_table(
                        pa.Table.from_batches([batch]).cast(
                            schema_to_pyarrow(table.schema())
                        ),
                        output,
                        compression="snappy",
                        data_page_version="2.0",
                        write_page_index=True,
                    )
                files.append(uri)
        with table.transaction() as transaction:
            transaction.delete(In("created_month", months))
            if files:
                transaction.add_files(files)
        journal = {
            "warehouse": warehouse,
            "native_endpoint": native_endpoint,
            "native_table": native_table,
            "metadata_uri": table.metadata_location,
            "months": months,
            "expected": generation,
            "snapshot_id": table.current_snapshot().snapshot_id,
        }
        # Persist publication intent BEFORE creating the immutable alias. This
        # makes failed/lost pointer writes replayable without metadata collisions.
        with state.transaction():
            state.set("publication", json.dumps(journal))
    if native_endpoint:
        # The native catalog commit above is the sole lake authority.
        version = journal["snapshot_id"]
    else:
        with table.io.new_input(journal["metadata_uri"]).open() as source:
            metadata = source.read()
        version = publisher.commit(metadata, journal["expected"])
    with state.transaction():
        for month in months:
            state.db.delete("dirty:" + month)
        state.set("published_version", version)
        state.set("publication", "")
    return {
        "version": version,
        "months": months,
        "metadata_uri": journal["metadata_uri"],
        "source_uri": warehouse,
        "snapshot_id": journal["snapshot_id"],
    }


def archive_authority(
    warehouse, project=None, native_endpoint=None, native_table="hackernews"
):
    if native_endpoint:
        from native_catalog import NativeCatalog

        catalog = NativeCatalog(None, warehouse, native_endpoint, native_table)
        value = catalog._request("GET", "/lake/catalog")
        # Immutable metadata location also fences same-snapshot property commits.
        return value["metadata_location"].encode(), value["metadata"]["table-uuid"]
    return Publisher(warehouse, project).read("metadata/version-hint.text")


def backup_state(
    state,
    directory,
    backup_root,
    warehouse,
    project=None,
    native_endpoint=None,
    native_table="hackernews",
):
    """Upload one stable Lite snapshot, then CAS a manifest; no credentials."""
    import hashlib
    import shutil
    import tempfile
    import uuid

    store = Publisher(backup_root, project)
    source_pointer, source_generation = archive_authority(
        warehouse, project, native_endpoint, native_table
    )
    _, expected = store.read("latest.json")
    checkpoint = uuid.uuid4().hex
    manifest = {
        "checkpoint": checkpoint,
        "warehouse": warehouse,
        "native_endpoint": native_endpoint,
        "native_table": native_table,
        "source_pointer": (source_pointer or b"").decode(),
        "source_generation": source_generation,
        "files": {},
    }
    with tempfile.TemporaryDirectory(dir=directory, prefix="backup-") as temporary:
        for name in ("ingestion.aflite",):
            path = Path(temporary) / name
            state.db.native.copy_stable_snapshot(str(path))
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            key = f"checkpoints/{checkpoint}/{name}"
            if store.gcs:
                store.gcs.blob(store.prefix + "/" + key).upload_from_filename(
                    path, if_generation_match=0
                )
            else:
                output = store.local / key
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, output)
            manifest["files"][name] = {
                "key": key,
                "sha256": digest.hexdigest(),
                "bytes": path.stat().st_size,
            }
        store.put(
            "latest.json", json.dumps(manifest, sort_keys=True).encode(), expected
        )
    return manifest


def restore_state(
    directory,
    backup_root,
    warehouse,
    project=None,
    native_endpoint=None,
    native_table="hackernews",
):
    import hashlib
    import os
    import shutil
    import tempfile

    if any((directory / name).exists() for name in ("ingestion.aflite",)):
        raise RuntimeError("restore requires an empty state directory")
    store = Publisher(backup_root, project)
    body, _ = store.read("latest.json")
    if body is None:
        raise RuntimeError("no published checkpoint")
    manifest = json.loads(body)
    if (
        manifest["warehouse"] != warehouse
        or manifest.get("native_endpoint") != native_endpoint
        or manifest.get("native_table", "hackernews") != native_table
    ):
        raise RuntimeError("checkpoint warehouse mismatch")
    pointer, pointer_generation = archive_authority(
        warehouse, project, native_endpoint, native_table
    )
    if (pointer or b"").decode() != manifest[
        "source_pointer"
    ] or pointer_generation != manifest["source_generation"]:
        raise RuntimeError(
            "archive advanced after this checkpoint; reconcile before restoring"
        )
    with tempfile.TemporaryDirectory(dir=directory, prefix="restore-") as temporary:
        for name in ("ingestion.aflite",):
            entry = manifest["files"][name]
            if entry["key"] != f"checkpoints/{manifest['checkpoint']}/{name}":
                raise RuntimeError("invalid checkpoint object path")
            path = Path(temporary) / name
            if store.gcs:
                store.gcs.blob(store.prefix + "/" + entry["key"]).download_to_filename(
                    path
                )
            else:
                shutil.copyfile(store.local / entry["key"], path)
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            if (
                path.stat().st_size != entry["bytes"]
                or digest.hexdigest() != entry["sha256"]
            ):
                raise RuntimeError("checkpoint checksum mismatch")
            import antfly_embedded

            if not antfly_embedded.check_file(path)["valid"]:
                raise RuntimeError("invalid Lite checkpoint")
        for name in ("ingestion.aflite",):
            os.replace(Path(temporary) / name, directory / name)
    return manifest


def firebase(path):
    with urlopen(
        f"https://hacker-news.firebaseio.com/v0/{path}.json", timeout=30
    ) as response:
        # Individual item/updates responses have a bounded transport budget.
        body = response.read(8 * 1024 * 1024 + 1)
        if len(body) > 8 * 1024 * 1024:
            raise ValueError("HN response exceeds 8 MiB")
        return json.loads(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument(
        "--warehouse", required=True, help="Dedicated gs:// or file:// Iceberg root"
    )
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument(
        "--native-endpoint", help="Antfly API root, e.g. http://localhost:8080/db/v1"
    )
    parser.add_argument("--native-table", default="hackernews")
    parser.add_argument(
        "--native-rows",
        action="store_true",
        help="Send row CDC; Antfly owns WAL, Parquet and searchable publication",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--backup-root", help="Dedicated gs:// or file:// state checkpoint root"
    )
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument(
        "--publish-interval",
        type=int,
        default=3600,
        help="Batch archive edits; month replacement has write amplification",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("parquet", type=Path, nargs="+")
    sub.add_parser("poll")
    sub.add_parser("publish")
    sub.add_parser("backup")
    sub.add_parser("restore")
    sub.add_parser("run")
    args = parser.parse_args()
    if args.native_rows and not args.native_endpoint:
        parser.error("--native-rows requires --native-endpoint")
    if args.native_rows and args.batch_size > 16384:
        parser.error("native transaction batches are limited to 16384 changes")
    if args.batch_size < 1:
        parser.error("batch size must be positive")
    if args.interval < 1 or args.publish_interval < 1:
        parser.error("interval must be positive")
    if args.command in ("backup", "restore") and not args.backup_root:
        parser.error("backup/restore requires --backup-root")
    with writer_lock(args.state):
        if args.command == "restore":
            print(
                json.dumps(
                    restore_state(
                        args.state,
                        args.backup_root,
                        args.warehouse,
                        args.project,
                        args.native_endpoint,
                        args.native_table,
                    )
                )
            )
            return
        state = State(args.state / "ingestion.aflite")
        try:
            if state.get("publication", "") or state.get("native_changes_request", ""):
                publish(
                    state,
                    args.state,
                    args.warehouse,
                    args.project,
                    args.native_endpoint,
                    args.native_table,
                    args.native_rows,
                    args.batch_size,
                )
            if args.command == "backup":
                print(
                    json.dumps(
                        backup_state(
                            state,
                            args.state,
                            args.backup_root,
                            args.warehouse,
                            args.project,
                            args.native_endpoint,
                            args.native_table,
                        )
                    )
                )
                return
            if args.command == "run":
                next_publication = 0.0
                while True:
                    done = state.poll(firebase, args.batch_size)
                    result = None
                    if time.monotonic() >= next_publication:
                        result = publish(
                            state,
                            args.state,
                            args.warehouse,
                            args.project,
                            args.native_endpoint,
                            args.native_table,
                            args.native_rows,
                            args.batch_size,
                        )
                        next_publication = time.monotonic() + args.publish_interval
                    print(
                        json.dumps({"fetched": done, "publication": result}), flush=True
                    )
                    time.sleep(args.interval)
            if args.command == "backfill":
                state.backfill(args.parquet, args.batch_size)
            elif args.command == "poll":
                print(json.dumps({"fetched": state.poll(firebase, args.batch_size)}))
            result = publish(
                state,
                args.state,
                args.warehouse,
                args.project,
                args.native_endpoint,
                args.native_table,
                args.native_rows,
                args.batch_size,
            )
            print(json.dumps(result))
        finally:
            state.db.close()


if __name__ == "__main__":
    main()
