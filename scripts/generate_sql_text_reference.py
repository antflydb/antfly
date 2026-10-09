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

"""Verify bounded text/JSON scalar contracts against private PostgreSQL 18+."""

import json
from pathlib import Path

from generate_sql_postgres_reference import postgres


FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_text_reference.json"
)


def verify(db, cases):
    for case in cases:
        with db.transaction(force_rollback=True):
            db.execute("SET TRANSACTION READ ONLY")
            cursor = db.execute("SELECT " + case["sql"])
            if "error" in case:
                raise AssertionError(f"PostgreSQL unexpectedly accepted {case['sql']}")
            actual = cursor.fetchone()
            if "oid" in case and cursor.description[0].type_code != case["oid"]:
                raise AssertionError(f"PostgreSQL result type drift: {case['sql']}")
            if actual != (case["value"],):
                raise AssertionError(f"PostgreSQL drift: {case['sql']}: {actual!r}")


def main():
    import psycopg

    fixtures = [
        json.loads(path.read_text())
        for path in (
            FIXTURE,
            FIXTURE.with_name("sql_scalar_kernel_reference.json"),
        )
    ]
    for fixture in fixtures:
        if fixture["reference"] != "PostgreSQL exact SQL":
            raise ValueError("PostgreSQL reference required")
    with postgres() as db:
        locale = db.execute(
            "SELECT datctype FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0]
        if locale not in ("C", "POSIX"):
            raise RuntimeError("the explicit C-collation oracle changed")
        for case in [case for fixture in fixtures for case in fixture["entries"]]:
            try:
                verify(db, [case])
            except psycopg.Error as error:
                if case.get("error") != error.sqlstate:
                    raise
    count = sum(len(fixture["entries"]) for fixture in fixtures)
    print(f"Verified {count} PostgreSQL text/JSON scalar contracts")


if __name__ == "__main__":
    main()
