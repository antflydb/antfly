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

"""Independent PostgreSQL witnesses for FK-bearing LIKE definition lifetimes."""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres


def notes(db):
    return {
        "column": db.execute(
            "SELECT col_description('copied_child'::regclass,2)"
        ).fetchone()[0],
        "index": db.execute(
            "SELECT obj_description(indexrelid,'pg_class') FROM pg_index "
            "WHERE indrelid='copied_child'::regclass AND indisprimary"
        ).fetchone()[0],
        "check": db.execute(
            "SELECT obj_description(oid,'pg_constraint') FROM pg_constraint "
            "WHERE conrelid='copied_child'::regclass AND contype='c'"
        ).fetchone()[0],
    }


def generate():
    result = {"format": 1}
    with postgres() as owner:
        major = int(owner.execute("SHOW server_version_num").fetchone()[0]) // 10000
        if major < 18:
            raise ValueError("PostgreSQL 18 or newer is required")
        result["postgres_major"] = major
        owner.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        owner.execute("INSERT INTO parent VALUES(1),(2)")
        owner.execute(
            "CREATE TABLE template(id INTEGER PRIMARY KEY, "
            "quantity INTEGER NOT NULL DEFAULT 2, "
            "doubled INTEGER GENERATED ALWAYS AS(quantity*2) STORED, "
            "CONSTRAINT positive CHECK(quantity>0))"
        )
        owner.execute("COMMENT ON COLUMN template.quantity IS 'quantity at admission'")
        owner.execute("COMMENT ON INDEX template_pkey IS 'primary at admission'")
        owner.execute(
            "COMMENT ON CONSTRAINT positive ON template IS 'check at admission'"
        )
        with psycopg.connect(owner.info.dsn, autocommit=True) as observer:
            observer.execute("SET statement_timeout='2s'")
            with owner.transaction():
                owner.execute(
                    "CREATE TABLE copied_child(LIKE template INCLUDING ALL, "
                    "FOREIGN KEY(id) REFERENCES parent(id))"
                )
                result["observer_uncommitted"] = observer.execute(
                    "SELECT to_regclass('copied_child')::text"
                ).fetchone()[0]
                result["admitted_notes"] = notes(owner)
                owner.execute(
                    "COMMENT ON COLUMN template.quantity IS 'later source note'"
                )
                owner.execute("DROP TABLE template")
                result["first_row"] = list(
                    owner.execute(
                        "INSERT INTO copied_child(id) VALUES(1) RETURNING id,quantity,doubled"
                    ).fetchone()
                )
            result["source_after_commit"] = observer.execute(
                "SELECT to_regclass('template')::text"
            ).fetchone()[0]
            result["committed_notes"] = notes(observer)
            result["rows"] = [
                list(row)
                for row in observer.execute(
                    "SELECT id,quantity,doubled FROM copied_child ORDER BY id"
                )
            ]
            result["errors"] = []
            for statement in (
                "INSERT INTO copied_child(id) VALUES(99)",
                "INSERT INTO copied_child(id) VALUES(1)",
                "INSERT INTO copied_child(id,quantity) VALUES(2,-1)",
            ):
                try:
                    observer.execute(statement)
                except psycopg.Error as exc:
                    result["errors"].append(exc.sqlstate)
                else:
                    raise AssertionError("expected constraint violation")
            with owner.transaction(force_rollback=True):
                owner.execute(
                    "CREATE TABLE canceled_child(LIKE copied_child INCLUDING ALL, "
                    "FOREIGN KEY(id) REFERENCES parent(id))"
                )
                result["cancellation_hidden"] = observer.execute(
                    "SELECT to_regclass('canceled_child')::text"
                ).fetchone()[0]
            result["cancellation_complete"] = observer.execute(
                "SELECT to_regclass('canceled_child')::text"
            ).fetchone()[0]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = generate()
    if args.check:
        if json.loads(args.output.read_text()) != value:
            raise SystemExit("PostgreSQL initial FK/LIKE witness changed")
        print("PostgreSQL FK/LIKE visibility, source retirement and notes match")
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
