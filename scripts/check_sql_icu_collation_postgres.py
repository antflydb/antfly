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

"""Check the original ICU collation lifecycle against PostgreSQL."""

import json

from psycopg.errors import DependentObjectsStillExist

from generate_sql_postgres_reference import FIXTURES, postgres


def main():
    cases = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    originals = {case["id"]: case["sql"] for case in cases["entries"]}
    with postgres() as db:
        assert db.execute(originals["sql-0129"]).statusmessage == "CREATE COLLATION"
        original = db.execute(
            "SELECT oid, collprovider, colllocale, collisdeterministic "
            "FROM pg_collation WHERE collname = 'case_insensitive'"
        ).fetchone()
        assert original[1:] == ("i", "und-u-ks-level2", True), original
        db.execute(
            "CREATE TABLE collated_values (id integer PRIMARY KEY, "
            "v text COLLATE case_insensitive)"
        )
        db.execute("INSERT INTO collated_values VALUES (1,'z'),(2,'ä'),(3,'a'),(4,'A')")
        db.execute("CREATE INDEX collated_values_order ON collated_values (v)")
        assert db.execute(originals["sql-0130"]).statusmessage == "ALTER COLLATION"
        assert db.execute(
            "SELECT oid FROM pg_collation WHERE collname = 'ci_text'"
        ).fetchone() == (original[0],)
        assert db.execute("SELECT v FROM collated_values ORDER BY v").fetchall() == [
            ("A",),
            ("a",),
            ("ä",),
            ("z",),
        ]
        assert db.execute(
            "SELECT v FROM collated_values WHERE v < 'z' ORDER BY v"
        ).fetchall() == [("A",), ("a",), ("ä",)]
        try:
            with db.transaction():
                db.execute(originals["sql-0131"])
        except DependentObjectsStillExist:
            pass
        else:
            raise AssertionError("DROP COLLATION removed a collation still in use")
        db.execute("DROP TABLE collated_values")
        assert db.execute(originals["sql-0131"]).statusmessage == "DROP COLLATION"
        notices = []
        db.add_notice_handler(
            lambda notice: notices.append((notice.sqlstate, notice.message_primary))
        )
        assert db.execute(originals["sql-0132"]).statusmessage == "DROP COLLATION"
        assert notices == [("00000", 'collation "ci_text" does not exist, skipping')], notices
    print("Verified four original ICU collation cases on PostgreSQL")


if __name__ == "__main__":
    main()
