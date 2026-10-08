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
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen

from normalize import plain


@contextmanager
def writer_lock(state):
    state.mkdir(parents=True, exist_ok=True)
    with (state / "writer.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


class State:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY, parent INTEGER, root INTEGER,
                month TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS items_parent ON items(parent);
            CREATE INDEX IF NOT EXISTS items_month ON items(month);
            CREATE INDEX IF NOT EXISTS items_root ON items(root);
            CREATE TABLE IF NOT EXISTS dirty (month TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS checkpoints (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS pending (
                id INTEGER PRIMARY KEY, retry_at REAL NOT NULL DEFAULT 0
            );
        """)

    def get(self, key, default="0"):
        row = self.db.execute(
            "SELECT value FROM checkpoints WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else default

    def set(self, key, value):
        self.db.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?, ?)", (key, str(value))
        )

    def put(self, item):
        item = dict(item)
        if "type" in item or "item_type" in item:
            # Complete API/export records also clear moderation flags when a
            # previously dead/deleted item becomes live again.
            item.setdefault("dead", False)
            item.setdefault("deleted", False)
        item_id = int(item.get("id", item.get("hn_id")))
        old = self.db.execute(
            "SELECT payload, month, parent FROM items WHERE id=?", (item_id,)
        ).fetchone()
        # Firebase deletions may only contain id/deleted. Retain partition and
        # ancestry, but remove the row from the next published live snapshot.
        if old:
            prior = json.loads(old[0])
            prior.update(item)
            item = prior
        item["id"] = item_id
        created = int(item.get("time", item.get("created_at", 0)) or 0)
        month = datetime.fromtimestamp(created, timezone.utc).strftime("%Y-%m")
        parent = int(item.get("parent", item.get("parent_id", 0)) or 0)
        kind = item.get("type", item.get("item_type"))
        root = item_id if kind in ("story", "job", "poll") else None
        payload = json.dumps(item, sort_keys=True, separators=(",", ":"))
        if old and old[0] == payload:
            return
        if old and old[2] != parent:
            # Parent reassignment is unusual; invalidate descendants rather
            # than retaining an incorrect story root.
            self.db.execute("UPDATE items SET root=NULL WHERE parent<>0")
            self.db.execute(
                "INSERT OR IGNORE INTO dirty SELECT DISTINCT month FROM items"
            )
        self.db.execute(
            "INSERT OR REPLACE INTO items VALUES (?, ?, ?, ?, ?)",
            (item_id, parent, root, month, payload),
        )
        for value in {month, old[1] if old else month}:
            self.db.execute("INSERT OR IGNORE INTO dirty VALUES (?)", (value,))

    def resolve_roots(self):
        # Disk indexes and SQL joins avoid loading the ancestry graph into RAM.
        # Cycles/missing ancestors remain NULL rather than inventing a root.
        while True:
            self.db.execute("""INSERT OR IGNORE INTO dirty SELECT DISTINCT child.month
                FROM items child JOIN items parent ON child.parent=parent.id
                WHERE child.root IS NULL AND parent.root IS NOT NULL""")
            changed = self.db.execute("""UPDATE items SET root=(
                SELECT parent.root FROM items parent WHERE parent.id=items.parent
            ) WHERE root IS NULL AND EXISTS (
                SELECT 1 FROM items parent WHERE parent.id=items.parent AND parent.root IS NOT NULL
            )""").rowcount
            if not changed:
                break

    def missing_parents(self, limit):
        return [
            row[0]
            for row in self.db.execute(
                """SELECT DISTINCT child.parent
            FROM items child LEFT JOIN items parent ON parent.id=child.parent
            WHERE child.root IS NULL AND child.parent>0 AND parent.id IS NULL LIMIT ?""",
                (limit,),
            )
        ]

    def queue(self, ids):
        self.db.executemany(
            "INSERT OR IGNORE INTO pending(id) VALUES (?)", ((int(i),) for i in ids)
        )

    def poll(self, fetch, batch_size=1000, parent_limit=1000):
        if not self.get("maxitem", ""):
            raise RuntimeError("backfill first to establish the new-item watermark")
        newest = int(fetch("maxitem"))
        updates = fetch("updates").get("items", [])
        with self.db:
            # Persist work before advancing cursors. Null/error responses stay
            # in the retry queue even after the new-ID cursor passes them.
            cursor = int(self.get("maxitem"))
            stop = min(newest, cursor + batch_size)
            self.queue(range(cursor + 1, stop + 1))
            self.set("maxitem", stop)
            self.queue(updates)
            sweep = int(self.get("sweep"))
            ids = [
                r[0]
                for r in self.db.execute(
                    "SELECT id FROM items WHERE id>? ORDER BY id LIMIT ?",
                    (sweep, batch_size),
                )
            ]
            self.queue(ids)
            self.set("sweep", ids[-1] if ids else 0)
            self.queue(self.missing_parents(parent_limit))
        done = 0
        todo = self.db.execute(
            "SELECT id FROM pending WHERE retry_at<=? ORDER BY retry_at,id LIMIT ?",
            (time.time(), batch_size),
        ).fetchall()
        for (item_id,) in todo:
            try:
                item = fetch(f"item/{item_id}")
                if item is None or int(item.get("id", -1)) != item_id:
                    raise ValueError("item unavailable or mismatched ID")
            except (OSError, ValueError):
                with self.db:
                    self.db.execute(
                        "UPDATE pending SET retry_at=? WHERE id=?",
                        (time.time() + 60, item_id),
                    )
                continue
            with self.db:
                self.put(item)
                self.db.execute("DELETE FROM pending WHERE id=?", (item_id,))
            done += 1
        with self.db:
            self.resolve_roots()
        return done

    def backfill(self, filenames, batch_size=4096):
        import pyarrow.parquet as pq

        for filename in filenames:
            # Content-addressed source identity survives renames/replays; no
            # mutable offset into a rewritten input file can skip rows.
            import hashlib

            digest = hashlib.sha256()
            with Path(filename).open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            key = "backfill:" + digest.hexdigest()
            completed = int(self.get(key))
            offset = 0
            for batch in pq.ParquetFile(filename).iter_batches(batch_size=batch_size):
                rows = batch.to_pylist()
                end = offset + len(rows)
                if end > completed:
                    with self.db:
                        for item in rows[max(0, completed - offset) :]:
                            self.put(item)
                        self.set(key, end)
                offset = end
        with self.db:
            self.resolve_roots()
            maximum = self.db.execute(
                "SELECT COALESCE(MAX(id),0) FROM items"
            ).fetchone()[0]
            self.set("maxitem", max(int(self.get("maxitem")), maximum))

    def batches(self, months, schema, batch_size=4096):
        import pyarrow as pa

        placeholders = ",".join("?" for _ in months)
        cursor = self.db.execute(
            f"SELECT payload,root,month FROM items WHERE month IN ({placeholders}) ORDER BY month,id",
            months,
        )
        while rows := cursor.fetchmany(batch_size):
            records = []
            for payload, root, month in rows:
                row = json.loads(payload)
                kind = row.get("type", row.get("item_type"))
                if (
                    row.get("deleted")
                    or row.get("dead")
                    or kind not in ("story", "comment")
                ):
                    continue
                title = plain(row.get("title", ""))
                text = row.get("text", row.get("text_html", "")) or ""
                url = row.get("url", "") or ""
                records.append(
                    dict(
                        hn_id=row["id"],
                        title=title,
                        url=url,
                        text_html=text,
                        body="\n".join(filter(None, (title, plain(text)))),
                        author=row.get("by", row.get("author", "")) or "",
                        points=int(row.get("score", row.get("points", 0)) or 0),
                        created_at=int(row.get("time", row.get("created_at", 0)) or 0),
                        item_type=kind,
                        parent_id=int(row.get("parent", row.get("parent_id", 0)) or 0),
                        comment_count=int(
                            row.get("descendants", row.get("comment_count", 0)) or 0
                        ),
                        domain=urlsplit(url).hostname or "",
                        root_story_id=root,
                        created_month=month,
                    )
                )
            if records:
                yield pa.RecordBatch.from_pylist(records, schema=schema)


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


def publish(state, directory, warehouse, project=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import uuid
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.expressions import In
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    catalog = SqlCatalog(
        "hn", uri=f"sqlite:///{directory / 'catalog.sqlite'}", warehouse=warehouse
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
        if journal["warehouse"] != warehouse:
            raise RuntimeError("pending publication belongs to a different warehouse")
        months = journal["months"]
    else:
        months = [
            r[0] for r in state.db.execute("SELECT month FROM dirty ORDER BY month")
        ]
        if not months:
            return None
        _, generation = publisher.read("metadata/version-hint.text")
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
            "metadata_uri": table.metadata_location,
            "months": months,
            "expected": generation,
            "snapshot_id": table.current_snapshot().snapshot_id,
        }
        # Persist publication intent BEFORE creating the immutable alias. This
        # makes failed/lost pointer writes replayable without metadata collisions.
        with state.db:
            state.set("publication", json.dumps(journal))
    with table.io.new_input(journal["metadata_uri"]).open() as source:
        metadata = source.read()
    version = publisher.commit(metadata, journal["expected"])
    with state.db:
        state.db.executemany("DELETE FROM dirty WHERE month=?", ((m,) for m in months))
        state.set("published_version", version)
        state.set("publication", "")
    return {
        "version": version,
        "months": months,
        "metadata_uri": journal["metadata_uri"],
        "source_uri": warehouse,
        "snapshot_id": journal["snapshot_id"],
    }


def backup_state(state, directory, backup_root, warehouse, project=None):
    """Stream consistent SQLite backups, then CAS a manifest; no credentials."""
    import hashlib
    import shutil
    import tempfile
    import uuid

    store = Publisher(backup_root, project)
    source_pointer, source_generation = Publisher(warehouse, project).read(
        "metadata/version-hint.text"
    )
    _, expected = store.read("latest.json")
    checkpoint = uuid.uuid4().hex
    manifest = {
        "checkpoint": checkpoint,
        "warehouse": warehouse,
        "source_pointer": (source_pointer or b"").decode(),
        "source_generation": source_generation,
        "files": {},
    }
    with tempfile.TemporaryDirectory(dir=directory, prefix="backup-") as temporary:
        for name in ("items.sqlite", "catalog.sqlite"):
            path = Path(temporary) / name
            connection = (
                state.db
                if name == "items.sqlite"
                else sqlite3.connect(directory / name)
            )
            try:
                with sqlite3.connect(path) as target:
                    connection.backup(target)
            finally:
                if connection is not state.db:
                    connection.close()
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


def restore_state(directory, backup_root, warehouse, project=None):
    import hashlib
    import os
    import shutil
    import tempfile

    if any((directory / name).exists() for name in ("items.sqlite", "catalog.sqlite")):
        raise RuntimeError("restore requires an empty state directory")
    store = Publisher(backup_root, project)
    body, _ = store.read("latest.json")
    if body is None:
        raise RuntimeError("no published checkpoint")
    manifest = json.loads(body)
    if manifest["warehouse"] != warehouse:
        raise RuntimeError("checkpoint warehouse mismatch")
    pointer, _ = Publisher(warehouse, project).read("metadata/version-hint.text")
    if (pointer or b"").decode() != manifest["source_pointer"]:
        raise RuntimeError(
            "archive advanced after this checkpoint; reconcile before restoring"
        )
    with tempfile.TemporaryDirectory(dir=directory, prefix="restore-") as temporary:
        for name in ("items.sqlite", "catalog.sqlite"):
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
            with sqlite3.connect(path) as database:
                if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("invalid SQLite checkpoint")
        for name in ("items.sqlite", "catalog.sqlite"):
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
                        args.state, args.backup_root, args.warehouse, args.project
                    )
                )
            )
            return
        state = State(args.state / "items.sqlite")
        try:
            if state.get("publication", ""):
                publish(state, args.state, args.warehouse, args.project)
            if args.command == "backup":
                print(
                    json.dumps(
                        backup_state(
                            state,
                            args.state,
                            args.backup_root,
                            args.warehouse,
                            args.project,
                        )
                    )
                )
                return
            if args.command == "run":
                while True:
                    done = state.poll(firebase, args.batch_size)
                    result = publish(state, args.state, args.warehouse, args.project)
                    print(
                        json.dumps({"fetched": done, "publication": result}), flush=True
                    )
                    time.sleep(args.interval)
            if args.command == "backfill":
                state.backfill(args.parquet, args.batch_size)
            elif args.command == "poll":
                print(json.dumps({"fetched": state.poll(firebase, args.batch_size)}))
            result = publish(state, args.state, args.warehouse, args.project)
            print(json.dumps(result))
        finally:
            state.db.close()


if __name__ == "__main__":
    main()
