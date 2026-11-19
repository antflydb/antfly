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

"""PostgreSQL witnesses for copied counter visibility, rollback and owner retirement."""

import argparse
import json
from pathlib import Path

import psycopg
from psycopg import sql

from generate_sql_postgres_reference import postgres


def state(db, name):
    return list(
        db.execute(
            sql.SQL("SELECT last_value,is_called FROM {}").format(sql.Identifier(name))
        ).fetchone()
    )


def present(db, name):
    return db.execute("SELECT to_regclass(%s)::text", (name,)).fetchone()[0]


def rejected(db, statement):
    try:
        db.execute(statement)
    except psycopg.Error as exc:
        return exc.sqlstate
    raise AssertionError("expected PostgreSQL rejection")


def generate():
    result = {"format": 1}
    with postgres() as owner:
        major = int(owner.execute("SHOW server_version_num").fetchone()[0]) // 10000
        if major < 18:
            raise ValueError("PostgreSQL 18 or newer is required")
        result["postgres_major"] = major
        owner.execute(
            "CREATE TABLE effects_rows(id BIGSERIAL PRIMARY KEY,n INT NOT NULL CHECK(n>0),UNIQUE(n) DEFERRABLE INITIALLY DEFERRED)"
        )
        owner.execute("INSERT INTO effects_rows(n) VALUES(1)")
        result["before_prepare"] = state(owner, "effects_rows_id_seq")
        owner.execute("PREPARE null_insert AS INSERT INTO effects_rows(n) VALUES(NULL)")
        result["after_prepare"] = state(owner, "effects_rows_id_seq")
        result["rejected_insert_effects"] = []
        for statement in (
            "EXECUTE null_insert",
            "EXECUTE null_insert",
            "INSERT INTO effects_rows(n) VALUES(NULL)",
            "INSERT INTO effects_rows(n) VALUES(-1)",
            "INSERT INTO effects_rows(n) VALUES(1)",
        ):
            code = rejected(owner, statement)
            result["rejected_insert_effects"].append(
                [code, state(owner, "effects_rows_id_seq")]
            )
        owner.execute("DEALLOCATE null_insert")
        result["rows_after_rejections"] = [
            list(row)
            for row in owner.execute("SELECT id,n FROM effects_rows ORDER BY id")
        ]
        result["batch_defaults"] = [
            list(row)
            for row in owner.execute(
                "INSERT INTO effects_rows(n) VALUES(2),(3) RETURNING id,n"
            )
        ]
        result["batch_rejection"] = rejected(
            owner, "INSERT INTO effects_rows(n) VALUES(4),(NULL),(5)"
        )
        result["batch_rejection_state"] = state(owner, "effects_rows_id_seq")
        result["rows_after_batch_rejection"] = [
            list(row)
            for row in owner.execute("SELECT id,n FROM effects_rows ORDER BY id")
        ]
        owner.execute("CREATE TABLE parent(id INT PRIMARY KEY)")
        owner.execute("INSERT INTO parent VALUES(1),(2)")
        owner.execute(
            "CREATE TABLE counter_source("
            "id BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 42 INCREMENT BY 3),"
            "serial_id BIGSERIAL, quantity INT NOT NULL DEFAULT 2,"
            "doubled INT GENERATED ALWAYS AS(quantity*2) STORED,"
            "PRIMARY KEY(id), UNIQUE(quantity) DEFERRABLE INITIALLY DEFERRED,"
            "CHECK(quantity>0))"
        )
        result["source_first"] = list(
            owner.execute(
                "INSERT INTO counter_source DEFAULT VALUES RETURNING id,serial_id,quantity,doubled"
            ).fetchone()
        )
        with psycopg.connect(owner.info.dsn, autocommit=True) as observer:
            observer.execute("SET statement_timeout='2s'")
            with owner.transaction(force_rollback=True):
                owner.execute(
                    "CREATE TABLE canceled_copy(LIKE counter_source INCLUDING ALL,"
                    "FOREIGN KEY(quantity) REFERENCES parent(id))"
                )
                result["cancel_hidden"] = [
                    present(observer, name)
                    for name in ("canceled_copy", "canceled_copy_id_seq")
                ]
                result["allocation_while_hidden"] = observer.execute(
                    "SELECT nextval('counter_source_serial_id_seq')"
                ).fetchone()[0]
            result["cancel_complete"] = [
                present(observer, name)
                for name in ("canceled_copy", "canceled_copy_id_seq")
            ]
            result["source_after_cancel"] = state(
                observer, "counter_source_serial_id_seq"
            )
            with owner.transaction():
                owner.execute(
                    "CREATE TABLE counter_copy(LIKE counter_source INCLUDING ALL,"
                    "FOREIGN KEY(quantity) REFERENCES parent(id))"
                )
                result["copy_hidden"] = [
                    present(observer, name)
                    for name in ("counter_copy", "counter_copy_id_seq")
                ]
                result["second_hidden_allocation"] = observer.execute(
                    "SELECT nextval('counter_source_serial_id_seq')"
                ).fetchone()[0]
                result["copy_first"] = list(
                    owner.execute(
                        "INSERT INTO counter_copy DEFAULT VALUES RETURNING id,serial_id,quantity,doubled"
                    ).fetchone()
                )
            result["copy_visible"] = [
                present(observer, name)
                for name in ("counter_copy", "counter_copy_id_seq")
            ]
            result["source_identity"] = state(observer, "counter_source_id_seq")
            result["copy_identity"] = state(observer, "counter_copy_id_seq")
            result["shared_counter"] = state(observer, "counter_source_serial_id_seq")
            result["source_restrict"] = rejected(observer, "DROP TABLE counter_source")
            observer.execute("DROP TABLE counter_source CASCADE")
            result["source_retired"] = [
                present(observer, name)
                for name in (
                    "counter_source",
                    "counter_source_id_seq",
                    "counter_source_serial_id_seq",
                )
            ]
            result["copy_rows_after_cascade"] = [
                list(row)
                for row in observer.execute(
                    "SELECT id,serial_id,quantity,doubled FROM counter_copy ORDER BY id"
                )
            ]
            result["remaining_defaults"] = [
                row[0]
                for row in observer.execute(
                    "SELECT a.attname::text FROM pg_attribute a JOIN pg_attrdef d "
                    "ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                    "WHERE a.attrelid='counter_copy'::regclass AND a.attgenerated='' ORDER BY a.attnum"
                )
            ]
            result["remaining_identity"] = observer.execute(
                "SELECT attidentity::text FROM pg_attribute WHERE attrelid='counter_copy'::regclass AND attname='id'"
            ).fetchone()[0]
            result["copy_next"] = list(
                observer.execute(
                    "INSERT INTO counter_copy(serial_id,quantity) VALUES(99,1) RETURNING id,serial_id,quantity,doubled"
                ).fetchone()
            )
            result["missing_retired_default"] = rejected(
                observer, "INSERT INTO counter_copy(quantity) VALUES(1)"
            )
            result["surviving_fk"] = rejected(
                observer, "INSERT INTO counter_copy(serial_id,quantity) VALUES(101,999)"
            )
            result["copy_identity_after_failures"] = state(
                observer, "counter_copy_id_seq"
            )
            observer.execute("INSERT INTO parent VALUES(9)")
            result["parent_rows"] = [
                row[0] for row in observer.execute("SELECT id FROM parent ORDER BY id")
            ]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = generate()
    if args.check:
        if json.loads(args.output.read_text()) != value:
            raise SystemExit("PostgreSQL copied counter witness changed")
        print("PostgreSQL copied counters, rollback allocation and owner CASCADE match")
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
