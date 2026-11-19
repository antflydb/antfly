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

"""Original sequence DDL postimages; not evidence of native SQL activation."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

IDS = tuple(f"sql-{number:04d}" for number in range(311, 318))
NAME = "usage_records_id_seq"


def object_identity(db):
    row = db.execute(
        "SELECT c.oid,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relname=%s",
        (NAME,),
    ).fetchone()
    return row


def snapshot(db):
    identity = object_identity(db)
    if identity is None:
        return None
    oid, kind = identity
    if kind != "S":
        assert kind == "r"
        return {
            "kind": "table",
            "rows": [list(row) for row in db.execute(f"SELECT id FROM {NAME}")],
        }
    definition = db.execute(
        "SELECT format_type(seqtypid,NULL),seqincrement,seqmin,seqmax,seqstart,seqcache,seqcycle "
        "FROM pg_sequence WHERE seqrelid=%s",
        (oid,),
    ).fetchone()
    last_value, is_called = db.execute(
        f"SELECT last_value,is_called FROM {NAME}"
    ).fetchone()
    owner = db.execute(
        "SELECT n.nspname,c.relname,a.attname FROM pg_depend d "
        "JOIN pg_class c ON c.oid=d.refobjid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=d.refobjsubid "
        "WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass "
        "AND d.objid=%s AND d.deptype='a'",
        (oid,),
    ).fetchall()
    assert len(owner) <= 1
    return {
        "kind": "sequence",
        "integer": {"smallint": "int16", "integer": "int32", "bigint": "int64"}[
            definition[0]
        ],
        **dict(
            zip(
                ("increment", "minimum", "maximum", "start", "cache", "cycle"),
                definition[1:],
                strict=True,
            )
        ),
        "last_value": last_value,
        "is_called": is_called,
        "owned_by": dict(zip(("namespace", "table", "column"), owner[0], strict=True))
        if owner
        else None,
    }


def profiles(identity):
    if identity in {"sql-0311", "sql-0313"}:
        return ("absent", "existing_sequence", "existing_table")
    if identity == "sql-0312":
        return ("absent",)
    if identity in {"sql-0314", "sql-0315"}:
        return ("existing_sequence",)
    return ("existing_sequence", "absent", "existing_table")


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for identity in IDS:
            original = originals[identity]
            assert not original["params"]
            for profile in profiles(identity):
                setup = [
                    "CREATE TABLE usage_records(id bigint PRIMARY KEY)",
                    "INSERT INTO usage_records(id) VALUES (1)",
                ]
                if profile == "existing_sequence":
                    setup += [
                        f"CREATE SEQUENCE {NAME} START 100 CACHE 4 OWNED BY usage_records.id",
                        f"SELECT nextval('{NAME}')",
                    ]
                elif profile == "existing_table":
                    setup += [
                        f"CREATE TABLE {NAME}(id text PRIMARY KEY)",
                        f"INSERT INTO {NAME}(id) VALUES ('keep')",
                    ]
                with db.transaction(force_rollback=True):
                    for sql in setup:
                        db.execute(sql)
                    before = snapshot(db)
                    prior_identity = object_identity(db)
                    rows = db.execute(
                        "SELECT xmin::text,ctid::text,id FROM usage_records"
                    ).fetchall()
                    code, tag = None, "ERROR"
                    try:
                        with db.transaction():
                            result = db.execute(original["sql"])
                            tag = result.statusmessage
                    except psycopg.Error as error:
                        code = error.sqlstate
                    after = snapshot(db)
                    if code is not None:
                        assert before == after
                    assert (
                        rows
                        == db.execute(
                            "SELECT xmin::text,ctid::text,id FROM usage_records"
                        ).fetchall()
                    )
                    entries.append(
                        {
                            "id": identity,
                            "profile": profile,
                            "sql": original["sql"],
                            "setup": setup,
                            "sqlstate": code,
                            "command_tag": tag,
                            "before": before,
                            "after": after,
                            "object_identity_unchanged": prior_identity
                            == object_identity(db),
                        }
                    )
    return {"format": 1, "reference": "PostgreSQL exact SQL", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("sequence declaration PostgreSQL witness drift")
        print(f"verified {len(value['entries'])} original sequence DDL witnesses")
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
