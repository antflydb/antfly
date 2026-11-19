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

"""Original multi-table/CASCADE TRUNCATE, errors and complete future postimages."""

import argparse
import json
import subprocess
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source = json.loads(
        subprocess.check_output(
            [
                "git",
                "show",
                inventory["source_commit"] + ":" + inventory["source_path"],
            ],
            text=True,
        )
    )["entries"]
    tables = ["docs", "docs_archive", "untouched"]
    entries = []
    for ordinal in [1102, 1104]:
        original = source[ordinal - 1]
        with postgres() as db:
            for table in tables:
                db.execute(f"CREATE TABLE {table}(_id TEXT PRIMARY KEY,title TEXT)")
                db.execute(f"INSERT INTO {table} VALUES ('doc:a','{table}:before')")

            def image():
                return {
                    table: output(
                        db.execute(f"SELECT _id,title FROM {table} ORDER BY _id")
                    )
                    for table in tables
                }

            before = image()
            failed_sql = "TRUNCATE docs,missing"
            try:
                db.execute(failed_sql)
                raise AssertionError("missing TRUNCATE target was accepted")
            except psycopg.Error as error:
                if error.sqlstate != "42P01":
                    raise
            failed = image()
            if failed != before:
                raise AssertionError("partial truncation on missing target")
            tag = db.execute(original["sql"]).statusmessage
            empty = image()
            selected = tables[:2] if ordinal == 1102 else tables[:1]
            for table in selected:
                db.execute(f"INSERT INTO {table} VALUES ('doc:a','{table}:after')")
            entries.append(
                {
                    "id": f"sql-{ordinal:04d}",
                    "sql": original["sql"],
                    "selected": selected,
                    "before": before,
                    "failure": {
                        "sql": failed_sql,
                        "sqlstate": "42P01",
                        "after": failed,
                    },
                    "tag": tag,
                    "empty": empty,
                    "final": image(),
                }
            )
    return {"format": 1, "reference": "PostgreSQL 18", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("document TRUNCATE PostgreSQL witness drift")
        print(
            "verified two original TRUNCATE lifecycles and atomic missing-target errors"
        )
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
