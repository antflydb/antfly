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

"""Qualify tablespace creation semantics in disposable PostgreSQL 18+.

By default execute the exact original, requiring its server-owned directory.
--isolated-location changes ONLY the LOCATION literal to an owned temporary
fixture directory and labels that adaptation in the reference. The native gate
always executes the unchanged original and verifies its exact location metadata.
Antfly applies physical placement through its existing placement policy; neither
mode implies native filesystem LOCATION overrides.
"""

import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from generate_sql_postgres_reference import FIXTURES, postgres


def original_case():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    original = next(case for case in inventory["entries"] if case["id"] == "sql-0110")
    match = re.search(
        r"\bLOCATION\s+'((?:[^']|'')*)'\s*;?\s*$", original["sql"], re.IGNORECASE
    )
    if match is None or original["params"]:
        raise AssertionError(
            "tablespace original no longer has its pinned literal LOCATION"
        )
    return original, match.group(1).replace("''", "'")


def require_empty_directory(location):
    directory = Path(location)
    if not directory.is_absolute() or directory.is_symlink():
        raise RuntimeError(
            "The original tablespace fixture requires a real absolute directory"
        )
    if not directory.is_dir():
        raise RuntimeError(
            f"Provision the original PostgreSQL fixture directory {location!r}, empty and owned "
            "by the local PostgreSQL server user. LOCATION will not be rewritten."
        )
    if directory.stat().st_uid != os.geteuid() or any(directory.iterdir()):
        raise RuntimeError(
            "The PostgreSQL fixture directory must be empty and server-owned"
        )


def reference(*, isolated_location=False):
    import psycopg
    from psycopg import sql

    original, declared_location = original_case()
    rows_sql = "SELECT id::text,status FROM usage_records ORDER BY id"
    directory = (
        TemporaryDirectory(prefix="antfly-pg-tablespace-", dir="/tmp")
        if isolated_location
        else nullcontext(declared_location)
    )
    with directory as fixture_location, postgres() as db:
        location = (
            str(Path(fixture_location).resolve())
            if isolated_location
            else declared_location
        )
        require_empty_directory(location)
        statement = original["sql"]
        if isolated_location:
            match = re.search(
                r"\bLOCATION\s+'((?:[^']|'')*)'\s*;?\s*$", statement, re.IGNORECASE
            )
            start, end = match.span(1)
            statement = (
                statement[: start - 1]
                + sql.Literal(location).as_string(db)
                + statement[end + 1 :]
            )
        created = False
        try:
            tag = db.execute(statement).statusmessage
            created = True
            before = db.execute(
                "SELECT oid,pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='fastspace'"
            ).fetchone()
            assert before is not None and before[1] == location
            db.execute(
                "CREATE TABLE usage_records(id UUID,status TEXT) TABLESPACE fastspace"
            )
            db.execute(
                "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000001','retained')"
            )
            bound = db.execute(
                "SELECT reltablespace FROM pg_class WHERE oid='usage_records'::regclass"
            ).fetchone()[0]
            assert bound == before[0]
            rows = db.execute(rows_sql).fetchall()
            codes = []
            for statement, expected in [
                (statement, "42710"),
                ("DROP TABLESPACE fastspace", "55000"),
            ]:
                try:
                    db.execute(statement)
                except psycopg.Error as error:
                    assert error.sqlstate == expected, error.sqlstate
                    codes.append(error.sqlstate)
                else:
                    raise AssertionError(
                        "PostgreSQL accepted a duplicate or in-use tablespace mutation"
                    )
                assert db.execute(rows_sql).fetchall() == rows
                assert (
                    db.execute(
                        "SELECT oid,pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='fastspace'"
                    ).fetchone()
                    == before
                )
            return json.loads(
                json.dumps(
                    {
                        "format": 1,
                        "reference": "PostgreSQL tablespace CREATE with isolated LOCATION fixture"
                        if isolated_location
                        else "PostgreSQL exact tablespace CREATE",
                        "oracle_location": "owned temporary directory; only LOCATION literal substituted"
                        if isolated_location
                        else "unchanged original literal",
                        "id": original["id"],
                        "source_entry_sha256": original["source_entry_sha256"],
                        "sql": original["sql"],
                        "location": declared_location,
                        "command_tag": tag,
                        "duplicate_sqlstate": codes[0],
                        "in_use_sqlstate": codes[1],
                        "bound_identity": True,
                        "preserved_on_refusal": True,
                        "rows": rows,
                    }
                )
            )
        finally:
            if created:
                # Only this owned, disposable server's objects are removed.
                db.execute("DROP TABLE IF EXISTS usage_records")
                db.execute("DROP TABLESPACE fastspace")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--isolated-location",
        action="store_true",
        help="Use an owned temporary PostgreSQL directory; record the LOCATION-only fixture adaptation explicitly",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output", type=Path)
    target.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference(isolated_location=args.isolated_location)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        assert json.loads(args.check.read_text()) == result, (
            "PostgreSQL tablespace CREATE reference drift"
        )
        print(
            "verified tablespace CREATE, bound identity and atomic refusals; LOCATION fixture: "
            + result["oracle_location"]
        )


if __name__ == "__main__":
    main()
