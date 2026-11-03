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

"""Measure SQL overlay invalidation using one prebuilt candidate library.

Compare staged inserts with and without unrelated document writes between
statements. Report operation counts, not a historical throughput comparison.
Each case creates its own native database; no filesystem database clones,
private inputs, models, or automatic mutation retries are used.
"""

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil

from lite_contention import sdk


COUNTERS = (
    "sql_constraint_overlay_hits",
    "sql_constraint_overlay_rebuilds",
    "sql_constraint_overlay_rebuild_entries",
    "sql_constraint_overlay_evictions",
    "sql_constraint_staged_statements",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--statements", type=int, default=16)
    args = parser.parse_args()
    if not 1 <= args.statements <= 512:
        parser.error("--statements must be between 1 and 512")
    args.directory.mkdir(parents=True, exist_ok=False)
    library = args.directory / args.library.name
    expected = digest(args.library)
    shutil.copyfile(args.library, library)
    if digest(library) != expected or digest(args.library) != expected:
        raise RuntimeError("library changed while freezing")
    af = sdk(str(library.resolve()))
    report = {
        "qualification": False,
        "scope": "Same-candidate SQL overlay operation counts; no throughput claim",
        "library_sha256": expected,
        "benchmark_sha256": digest(Path(__file__)),
        "statements": args.statements,
        "status": "incomplete",
        "cases": {},
    }
    output = args.directory / "result.json"

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n")

    save()
    for label, interleave in (("quiet", False), ("unrelated_primary_writes", True)):
        with af.create(
            args.directory / (label + ".aflite"),
            busy_timeout=5,
            local_runtime=False,
        ) as db:
            db.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT UNIQUE)")
            before = db.pending_work_stats()["background_publications"]
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                for number in range(args.statements):
                    session.execute(
                        "INSERT INTO items (id,name) VALUES ($1,$2)",
                        [number, "name-" + str(number)],
                    )
                    if interleave:
                        db.batch_json(
                            {"inserts": {"unrelated-document": {"n": number}}}
                        )
                after = db.pending_work_stats()["background_publications"]
                session.execute("COMMIT")
            rows = int(db.sql("SELECT COUNT(*) FROM items")["rows"][0][0])
            valid = db.check()["valid"]
            if rows != args.statements or not valid:
                raise RuntimeError("acknowledged row count or integrity failed")
            if (
                interleave
                and db.lookup("unrelated-document")["n"] != args.statements - 1
            ):
                raise RuntimeError("unrelated acknowledged document missing")
            report["cases"][label] = {
                "delta": {key: after[key] - before[key] for key in COUNTERS},
                "rows": rows,
                "integrity_valid": valid,
            }
            save()
    report["status"] = "diagnostic_completed"
    save()
    print(json.dumps(report))


if __name__ == "__main__":
    main()
