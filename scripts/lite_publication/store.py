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

"""One local filesystem and SQLite publication registry per managed instance."""

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path

from . import native
from .contract import ID, SHA, Error, require


@contextmanager
def locked(path, flags):
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle, flags)
        except BlockingIOError as exc:
            raise Error(
                "operation_busy", "Another worker holds the publication lock"
            ) from exc
        yield


@contextmanager
def write(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


class Destination:
    def __init__(self, root, destination):
        require(ID.fullmatch(destination), "Invalid destination ID")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root = root
        self.id = destination
        # Lock the directory itself so probing an unrelated directory never
        # creates a lock file there. New registries are published atomically;
        # readers can never observe an uninitialized registry.
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            registry = root / "publication.sqlite"
            staging = root / ".publication-bootstrap"
            if not registry.exists():
                require(
                    all(p == staging for p in root.iterdir()),
                    "Publication root must be empty on first use",
                    "destination_mismatch",
                )
                staging.unlink(missing_ok=True)
                self.initialize(staging, destination)
                os.replace(staging, registry)
                native.sync_directory(root)
            require(
                registry.is_file() and not registry.is_symlink(),
                "Invalid publication registry",
            )
            self.db = sqlite3.connect(registry, isolation_level=None, timeout=5)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA fullfsync=ON")
            require(
                self.db.execute("PRAGMA application_id").fetchone()[0] == 0x41465031,
                "Unknown publication registry",
                "destination_mismatch",
            )
            require(
                self.current()["id"] == destination,
                "Root already belongs to another destination",
                "destination_mismatch",
            )
        finally:
            os.close(fd)

    @staticmethod
    def initialize(path, destination):
        db = sqlite3.connect(path, isolation_level=None)
        try:
            # The private bootstrap is disposable until rename. Use an
            # in-memory rollback journal so process death leaves a single file.
            db.execute("PRAGMA journal_mode=MEMORY")
            with write(db):
                db.execute("PRAGMA application_id=0x41465031")
                db.execute("""CREATE TABLE destination
                    (singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL,
                     generation INTEGER NOT NULL, active TEXT)""")
                db.execute("""CREATE TABLE operations
                    (id TEXT PRIMARY KEY, digest TEXT NOT NULL, expected INTEGER NOT NULL,
                     phase TEXT NOT NULL, committed INTEGER, metadata TEXT)""")
                db.execute("""CREATE UNIQUE INDEX one_pending ON operations ((1))
                    WHERE phase IN ('preparing','prepared','validated')""")
                db.execute(
                    "INSERT INTO destination VALUES (1,?,0,NULL)", (destination,)
                )
        finally:
            db.close()
        with path.open("rb") as f:
            os.fsync(f.fileno())

    def current(self):
        return self.db.execute("SELECT * FROM destination WHERE singleton=1").fetchone()

    def operation(self, op):
        require(ID.fullmatch(op), "Invalid operation ID")
        row = self.db.execute("SELECT * FROM operations WHERE id=?", (op,)).fetchone()
        require(row is not None, "Unknown operation", "not_found")
        return row

    def path(self, op):
        require(ID.fullmatch(op), "Invalid operation ID")
        return self.root / ("op-" + hashlib.sha256(op.encode()).hexdigest() + ".aflite")

    def status(self, op=None):
        # A single read transaction snapshots destination and operation receipts
        # even when a different worker commits a subsequent update.
        self.db.execute("BEGIN")
        try:
            head = self.current()
            result = {"destination": self.id, "generation": head["generation"]}
            if head["active"]:
                result["publication"] = json.loads(
                    self.operation(head["active"])["metadata"]
                )
            if op is not None:
                row = self.operation(op)
                result.update(
                    operation=op,
                    state=row["phase"],
                    artifact_sha256=row["digest"],
                    expected_generation=row["expected"],
                    committed_generation=row["committed"],
                )
                if row["metadata"]:
                    result["candidate"] = json.loads(row["metadata"])
            return result
        finally:
            self.db.rollback()

    def prepare(self, op, expected, digest, artifact):
        require(
            SHA.fullmatch(digest) and 0 <= expected < 2**63 - 1,
            "Invalid digest or generation",
        )
        path = self.path(op)
        with write(self.db):
            old = self.db.execute(
                "SELECT * FROM operations WHERE id=?", (op,)
            ).fetchone()
            if old:
                require(
                    old["expected"] == expected and old["digest"] == digest,
                    "Operation already bound to different inputs",
                    "operation_conflict",
                )
            else:
                require(
                    expected == self.current()["generation"],
                    "Expected generation is stale",
                    "stale_generation",
                )
                pending = self.db.execute(
                    "SELECT id FROM operations WHERE phase IN ('preparing','prepared','validated')"
                ).fetchone()
                require(pending is None, "Another update is pending", "operation_busy")
                self.db.execute(
                    "INSERT INTO operations VALUES(?,?,?,'preparing',NULL,NULL)",
                    (op, digest, expected),
                )
        if self.operation(op)["phase"] != "preparing":
            return self.status(op)
        with (
            locked(self.root / "readers.lock", fcntl.LOCK_SH),
            locked(path.with_suffix(".work-lock"), fcntl.LOCK_EX | fcntl.LOCK_NB),
        ):
            if self.operation(op)["phase"] != "preparing":
                return self.status(op)
            tmp = path.with_suffix(".part")
            tmp.unlink(missing_ok=True)
            try:
                h = hashlib.sha256()
                size = 0
                with artifact.open("rb") as src, tmp.open("wb") as dst:
                    info = os.fstat(src.fileno())
                    require(
                        stat.S_ISREG(info.st_mode)
                        and 0 < info.st_size <= native.MAX_ARTIFACT,
                        "Expected a regular artifact within size limit",
                    )
                    for block in iter(lambda: src.read(1024 * 1024), b""):
                        size += len(block)
                        require(
                            size <= native.MAX_ARTIFACT, "Upload exceeded size limit"
                        )
                        dst.write(block)
                        h.update(block)
                    require(
                        h.hexdigest() == digest,
                        "Artifact checksum mismatch",
                        "digest_mismatch",
                    )
                    dst.flush()
                    os.fchmod(dst.fileno(), 0o400)
                    os.fsync(dst.fileno())
                # Publish staging bytes before recording them as prepared.
                # Cancellation during copying wins by changing the durable phase.
                with write(self.db):
                    if self.operation(op)["phase"] == "preparing":
                        os.replace(tmp, path)
                        native.sync_directory(self.root)
                        self.db.execute(
                            "UPDATE operations SET phase='prepared' WHERE id=?", (op,)
                        )
            finally:
                tmp.unlink(missing_ok=True)
        return self.status(op)

    def validate(self, op):
        row = self.operation(op)
        if row["phase"] in ("committed", "validated", "cancelled"):
            return self.status(op)
        require(
            row["phase"] == "prepared", "Run prepare before validation", "invalid_state"
        )
        with locked(self.root / "readers.lock", fcntl.LOCK_SH):
            row = self.operation(op)
            if row["phase"] == "prepared":
                metadata = native.inspect(self.path(op), row["digest"])
                with write(self.db):
                    self.db.execute(
                        "UPDATE operations SET phase='validated', metadata=? WHERE id=? AND phase='prepared'",
                        (json.dumps(metadata, separators=(",", ":")), op),
                    )
        return self.status(op)

    def activate(self, op):
        with write(self.db):
            row = self.operation(op)
            if row["phase"] != "committed":
                require(
                    row["phase"] == "validated",
                    "Activation requires a validated operation",
                    "invalid_state",
                )
                require(
                    row["expected"] == self.current()["generation"],
                    "Expected generation is stale",
                    "stale_generation",
                )
                require(
                    self.path(op).is_file(),
                    "Validated artifact missing",
                    "storage_error",
                )
                generation = row["expected"] + 1
                self.db.execute(
                    "UPDATE destination SET generation=?, active=? WHERE singleton=1",
                    (generation, op),
                )
                self.db.execute(
                    "UPDATE operations SET phase='committed', committed=? WHERE id=?",
                    (generation, op),
                )
        return self.status(op)

    def cancel(self, op):
        with write(self.db):
            self.operation(op)
            self.db.execute(
                "UPDATE operations SET phase='cancelled' WHERE id=? AND phase!='committed'",
                (op,),
            )
        return self.status(op)

    def cleanup(self):
        # Readers hold a shared lock across opening and using immutable files.
        # Activation needs no such lock and never waits for long-running reads.
        with (
            locked(self.root / "readers.lock", fcntl.LOCK_EX | fcntl.LOCK_NB),
            write(self.db),
        ):
            head = self.current()
            for row in self.db.execute("SELECT id, phase FROM operations"):
                if row["id"] == head["active"]:
                    continue
                path = self.path(row["id"])
                # A .part is unacknowledged and safe to retry even if the
                # preparing process died after fsync, before renaming.
                path.with_suffix(".part").unlink(missing_ok=True)
                if row["phase"] not in ("cancelled", "committed"):
                    continue
                path.unlink(missing_ok=True)
                Path(str(path) + ".lock").unlink(missing_ok=True)
                path.with_suffix(".work-lock").unlink(missing_ok=True)
            native.sync_directory(self.root)
        return self.status()

    @contextmanager
    def reader(self, generation):
        with locked(self.root / "readers.lock", fcntl.LOCK_SH):
            current = self.current()
            require(
                generation == current["generation"],
                "Publication generation changed",
                "stale_generation",
            )
            require(
                current["active"] is not None, "No published generation", "unpublished"
            )
            yield self.path(current["active"])
