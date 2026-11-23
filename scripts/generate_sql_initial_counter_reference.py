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

"""Independent PostgreSQL witnesses for atomic FK/counter catalog visibility."""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres


def visible(db, names):
    return [
        [name, kind]
        for name, kind in db.execute(
            "SELECT relname, relkind::text FROM pg_class "
            "WHERE relnamespace='public'::regnamespace AND relname=ANY(%s) "
            "ORDER BY relname",
            (names,),
        )
    ]


def generate():
    result = {"format": 1}
    with postgres() as owner:
        major = int(owner.execute("SHOW server_version_num").fetchone()[0]) // 10000
        if major < 18:
            raise ValueError("PostgreSQL 18 or newer is required")
        result["postgres_major"] = major
        owner.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        owner.execute("INSERT INTO parent VALUES(1),(2)")
        names = ["atomic_child", "atomic_child_id_seq", "atomic_child_serial_id_seq"]
        with psycopg.connect(owner.info.dsn, autocommit=True) as observer:
            observer.execute("SET statement_timeout='2s'")
            with owner.transaction():
                owner.execute(
                    "CREATE TABLE atomic_child("
                    "id BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 42 INCREMENT BY 3),"
                    "serial_id BIGSERIAL, parent_id INT REFERENCES parent(id),"
                    "PRIMARY KEY(id), UNIQUE(parent_id) DEFERRABLE INITIALLY DEFERRED)"
                )
                result["owner_uncommitted"] = visible(owner, names)
                result["observer_uncommitted"] = visible(observer, names)
                result["first_row"] = list(
                    owner.execute(
                        "INSERT INTO atomic_child(parent_id) VALUES(1) "
                        "RETURNING id,serial_id,parent_id"
                    ).fetchone()
                )
            result["observer_committed"] = visible(observer, names)
            result["observer_rows"] = [
                list(row)
                for row in observer.execute(
                    "SELECT id,serial_id,parent_id FROM atomic_child ORDER BY id"
                )
            ]
            result["constraints"] = [
                list(row)
                for row in observer.execute(
                    "SELECT contype::text,condeferrable,condeferred FROM pg_constraint "
                    "WHERE conrelid='atomic_child'::regclass ORDER BY contype"
                )
            ]
            canceled = ["canceled_child", "canceled_child_id_seq"]
            with owner.transaction(force_rollback=True):
                owner.execute(
                    "CREATE TABLE canceled_child(id BIGSERIAL PRIMARY KEY,"
                    "parent_id INT REFERENCES parent(id))"
                )
                result["cancellation_hidden"] = visible(observer, canceled)
            result["cancellation_complete"] = visible(observer, canceled)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = generate()
    if args.check:
        if json.loads(args.output.read_text()) != value:
            raise SystemExit("PostgreSQL initial FK/counter visibility witness changed")
        print("PostgreSQL initial FK/counter atomic visibility and cancellation match")
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
