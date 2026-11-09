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

"""Measure current-library SQL checkpoint counts using disposable synthetic data.

Counts include connection open/close and background service. They are neither
exact fsync counts nor a throughput qualification. No reference library runs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path

from lite_catalog_accounting import CatalogAccounting
from lite_contention import sdk


def checkpoint(database: Path) -> int:
    source = CatalogAccounting(database)
    try:
        return source.slot[0]
    finally:
        source.close()


def measure(library: Path, directory: Path, profile_name: str) -> dict:
    af = sdk(str(library))
    profile = af.Profile.NATIVE if profile_name == "native" else af.Profile.HOSTED
    with library.open("rb") as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sql-", dir=directory) as temporary:
        database = Path(temporary) / "database.aflite"
        with af.create(database, profile=profile) as db:
            db.sql("CREATE TABLE parent (id TEXT PRIMARY KEY)")
            db.sql(
                "CREATE TABLE child (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES parent(id))"
            )
            db.sql("CREATE TABLE ordinary (id TEXT PRIMARY KEY)")
            db.sql("CREATE TABLE plain (n BIGINT)")
            db.create_table(
                "documents",
                {
                    "version": 1,
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {"n": {"type": "integer"}},
                            }
                        }
                    },
                },
            )
            db.sql("INSERT INTO parent (id) VALUES ('p')")
        operations = [
            ("open-close", []),
            ("ordinary", ["INSERT INTO ordinary (id) VALUES ('a')"]),
            ("foreign-key", ["INSERT INTO child (id,parent_id) VALUES ('c','p')"]),
            (
                "session-unconstrained",
                ["BEGIN", "INSERT INTO plain (_id,n) VALUES ('p',4)", "COMMIT"],
            ),
            (
                "session-document",
                ["BEGIN", "INSERT INTO documents (_id,n) VALUES ('p',5)", "COMMIT"],
            ),
            (
                "session-one-namespace",
                ["BEGIN", "INSERT INTO ordinary (id) VALUES ('b')", "COMMIT"],
            ),
            (
                "session-two-namespace",
                [
                    "BEGIN",
                    "INSERT INTO ordinary (id) VALUES ('d')",
                    "INSERT INTO child (id,parent_id) VALUES ('e','p')",
                    "COMMIT",
                ],
            ),
        ]
        rows = []
        for label, statements in operations:
            before = checkpoint(database)
            started = time.monotonic()
            with af.open(database, profile=profile) as db:
                if label.startswith("session-"):
                    session = db.sql_session()
                    try:
                        for statement in statements:
                            session.execute(statement)
                    finally:
                        session.close()
                else:
                    for statement in statements:
                        db.sql(statement)
            rows.append(
                {
                    "operation": label,
                    "native_checkpoint_publications": checkpoint(database) - before,
                    "seconds": time.monotonic() - started,
                }
            )
        with af.open_readonly(database) as db:
            valid = bool(db.check()["valid"])
        if not valid:
            raise RuntimeError("Synthetic SQL fixture failed integrity")
    return {
        "scope": __doc__.strip(),
        "qualified": False,
        "profile": profile_name,
        "library_sha256": digest.hexdigest(),
        "rows": rows,
        "integrity_valid": valid,
        "database_retained": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".tools/lite-sql-publication",
    )
    parser.add_argument("--profile", choices=["native", "hosted"], default="native")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    result = measure(args.library.resolve(), args.directory, args.profile)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
