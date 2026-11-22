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

"""Private, single-writer, append-only state for inference policy campaigns."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import uuid


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def checksum(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sample_key(row):
    return (row["case"], row["endpoint"], row["trial"], row["max_tokens"])


class StateError(ValueError):
    pass


class CampaignStore:
    """Journal is authoritative; JSON report is an atomic, final projection.

    Private reference vectors live in separate mode-0600 artifacts, never in
    public rows or journal records. Every recorded request is fsynced before proceeding.
    A crash can leave an unterminated last record; only that tail is discarded.
    """

    def __init__(self, report, identity, resume=False):
        self.report = Path(report)
        self.identity = identity
        self.resume = resume
        self.directory = self.report.with_name(self.report.name + ".state")
        self.lock = self.journal = None
        self.rows = []
        self.samples = {}
        self.references = {}
        self.epochs = set()
        self.epoch = None
        self.recovered_bytes = 0
        self.commit_failed = False

    def __enter__(self):
        try:
            self.directory.parent.mkdir(parents=True, exist_ok=True)
            self.directory.mkdir(mode=0o700, exist_ok=True)
            info = self.directory.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
            ):
                raise StateError("STATE_DIRECTORY_MUST_BE_PRIVATE")
            self.lock = self._private_file("lock")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise StateError("CAMPAIGN_ALREADY_RUNNING") from error
            self.journal = self._private_file("journal.jsonl")
            exists = os.fstat(self.journal.fileno()).st_size > 0
            if exists and not self.resume:
                raise StateError("CAMPAIGN_EXISTS_USE_RESUME")
            if not exists and self.report.exists():
                raise StateError("REPORT_HAS_NO_COMPATIBLE_JOURNAL")
            if exists:
                self._recover()
            else:
                self._append({"type": "identity", "identity": self.identity})
            self.epoch = uuid.uuid4().hex
            self._append(
                {
                    "type": "epoch",
                    "id": self.epoch,
                    "recovered_bytes": self.recovered_bytes,
                }
            )
            self.sync_directory(self.directory)
            self.sync_directory(self.directory.parent)
            return self
        except BaseException:
            self.close()
            raise

    def _private_file(self, name, create=True):
        flags = os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0)
        descriptor = os.open(self.directory / name, flags, 0o600)
        stream = os.fdopen(descriptor, "r+b", buffering=0)
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            stream.close()
            raise StateError("STATE_FILE_MUST_BE_PRIVATE")
        return stream

    def _recover(self):
        self.journal.seek(0)
        valid_end = 0
        first = True
        while True:
            line = self.journal.readline(4 * 1024 * 1024 + 1)
            if not line:
                break
            if len(line) > 4 * 1024 * 1024:
                raise StateError("JOURNAL_RECORD_TOO_LARGE")
            if not line.endswith(b"\n"):
                self.recovered_bytes = len(line)
                self.journal.truncate(valid_end)
                os.fsync(self.journal.fileno())
                break
            try:
                envelope = json.loads(line)
                event = envelope["record"]
                if checksum(event) != envelope["sha256"]:
                    raise StateError("JOURNAL_CHECKSUM_MISMATCH")
                if first:
                    if event != {"type": "identity", "identity": self.identity}:
                        raise StateError("RESUME_INPUTS_CHANGED")
                else:
                    self._apply(event)
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise StateError("JOURNAL_CORRUPT") from error
            first = False
            valid_end = self.journal.tell()
        if first:
            if self.report.exists():
                raise StateError("JOURNAL_MISSING_IDENTITY")
            self._append({"type": "identity", "identity": self.identity})
        self.journal.seek(0, os.SEEK_END)

    def _apply(self, event, validate_only=False):
        if event["type"] == "epoch":
            if event["id"] in self.epochs:
                raise StateError("DUPLICATE_EPOCH")
            if not validate_only:
                self.epochs.add(event["id"])
        elif event["type"] == "reference":
            if event["case"] in self.references:
                raise StateError("DUPLICATE_REFERENCE")
            sha = event["sha256"]
            if (
                not isinstance(sha, str)
                or len(sha) != 64
                or any(c not in "0123456789abcdef" for c in sha)
                or event["filename"] != "reference-" + sha + ".json"
            ):
                raise StateError("INVALID_REFERENCE_IDENTITY")
            if not validate_only:
                self.references[event["case"]] = event
        elif event["type"] == "attempt":
            row = event["row"]
            if row["epoch"] not in self.epochs or "embedding" in row or "text" in row:
                raise StateError("INVALID_PUBLIC_ATTEMPT")
            if not row["warmup"]:
                key = sample_key(row)
                if key in self.samples:
                    raise StateError("DUPLICATE_MEASUREMENT")
                if not validate_only:
                    self.samples[key] = row
            if not validate_only:
                self.rows.append(row)
        else:
            raise StateError("UNKNOWN_JOURNAL_EVENT")

    def _require_writable(self):
        if self.commit_failed:
            raise StateError("JOURNAL_COMMIT_UNCERTAIN_REOPEN_TO_RECOVER")

    def _append(self, event):
        self._require_writable()
        # Validate before committing an event that replay could not accept.
        raw = canonical({"record": event, "sha256": checksum(event)}) + b"\n"
        if len(raw) > 4 * 1024 * 1024:
            raise StateError("JOURNAL_RECORD_TOO_LARGE")
        if event["type"] != "identity":
            self._apply(event, validate_only=True)
        try:
            self.journal.seek(0, os.SEEK_END)
            remaining = memoryview(raw)
            while remaining:
                count = self.journal.write(remaining)
                if not count:
                    raise OSError("JOURNAL_WRITE_FAILED")
                remaining = remaining[count:]
            os.fsync(self.journal.fileno())
            if event["type"] != "identity":
                self._apply(event)
        except BaseException:
            # The tail may contain partial or fully written unacknowledged bytes.
            # Only recovery on a fresh open can decide what was committed.
            self.commit_failed = True
            raise

    def record(self, row):
        self._append({"type": "attempt", "row": dict(row, epoch=self.epoch)})

    def reference(self, case, endpoint, vector):
        self._require_writable()
        sha = checksum(vector)
        filename = "reference-" + sha + ".json"
        destination = self.directory / filename
        temporary = None
        try:
            if destination.exists() or destination.is_symlink():
                with self._private_file(filename, create=False) as stream:
                    if (
                        hashlib.sha256(stream.read(4 * 1024 * 1024 + 1)).hexdigest()
                        != sha
                    ):
                        raise StateError("REFERENCE_CHECKSUM_MISMATCH")
            else:
                with tempfile.NamedTemporaryFile(
                    dir=self.directory, prefix="reference-", delete=False
                ) as stream:
                    temporary = Path(stream.name)
                    stream.write(canonical(vector))
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(destination)
                self.sync_directory(self.directory)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        self._append(
            {
                "type": "reference",
                "case": case,
                "endpoint": endpoint,
                "filename": filename,
                "dimensions": len(vector),
                "sha256": sha,
            }
        )

    def reference_vector(self, case):
        reference = self.references[case]
        with self._private_file(reference["filename"], create=False) as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if (
            len(raw) > 4 * 1024 * 1024
            or hashlib.sha256(raw).hexdigest() != reference["sha256"]
        ):
            raise StateError("REFERENCE_CHECKSUM_MISMATCH")
        vector = json.loads(raw)
        if not isinstance(vector, list) or len(vector) != reference["dimensions"]:
            raise StateError("REFERENCE_DIMENSIONS_MISMATCH")
        return vector

    @staticmethod
    def sync_directory(path):
        directory = os.open(path, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def snapshot(self, report):
        """One linear projection per run, including interrupted runs."""
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                dir=self.report.parent,
                prefix=self.report.name + ".",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(report, stream, indent=2, allow_nan=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.report)
            self.sync_directory(self.report.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def close(self):
        if self.journal is not None:
            self.journal.close()
            self.journal = None
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def __exit__(self, *unused):
        self.close()
