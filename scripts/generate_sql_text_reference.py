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

"""Verify bounded UTF-8 text-function contracts against private PostgreSQL 18+."""

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
            if actual != (case["value"],):
                raise AssertionError(f"PostgreSQL drift: {case['sql']}: {actual!r}")


def main():
    import psycopg

    fixture = json.loads(FIXTURE.read_text())
    if fixture["reference"] != "PostgreSQL exact SQL":
        raise ValueError("PostgreSQL reference required")
    with postgres() as db:
        for case in fixture["entries"]:
            try:
                verify(db, [case])
            except psycopg.Error as error:
                if case.get("error") != error.sqlstate:
                    raise
    print(f"Verified {len(fixture['entries'])} PostgreSQL text contracts")


if __name__ == "__main__":
    main()
