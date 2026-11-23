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

"""PostgreSQL witnesses for original index and constraint COMMENT semantics."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def qualify_lifecycle(db):
    """Independent OID-preserving rename and fresh-identity LIKE witnesses."""
    db.execute(
        "CREATE TABLE comment_parent(id TEXT PRIMARY KEY,CONSTRAINT unique_after CHECK(id<>''),CONSTRAINT shared_ordinary CHECK(id<>''))"
    )
    db.execute(
        "CREATE TABLE comment_source(id TEXT,parent TEXT,CONSTRAINT check_before CHECK(id<>''),CONSTRAINT unique_before UNIQUE(id),CONSTRAINT fk_before FOREIGN KEY(parent) REFERENCES comment_parent(id))"
    )
    db.execute("INSERT INTO comment_parent VALUES ('p')")
    db.execute("INSERT INTO comment_source VALUES ('row','p')")
    db.execute("CREATE TABLE comment_ref(id TEXT REFERENCES comment_source(id))")
    db.execute("INSERT INTO comment_ref VALUES ('row')")
    db.execute("COMMENT ON CONSTRAINT check_before ON comment_source IS 'check note'")
    db.execute("COMMENT ON CONSTRAINT unique_before ON comment_source IS 'unique note'")
    db.execute("COMMENT ON INDEX unique_before IS 'index note'")
    db.execute("COMMENT ON CONSTRAINT fk_before ON comment_source IS 'fk note'")
    before_rows = db.execute(
        "SELECT xmin::text,ctid::text,* FROM comment_source"
    ).fetchall()
    for old, new, expected in (
        ("check_before", "check_after", "check note"),
        ("unique_before", "unique_after", "unique note"),
        ("fk_before", "fk_after", "fk note"),
    ):
        identity = db.execute(
            "SELECT oid FROM pg_constraint WHERE conrelid='comment_source'::regclass AND conname=%s",
            (old,),
        ).fetchone()[0]
        index = db.execute(
            "SELECT conindid FROM pg_constraint WHERE oid=%s", (identity,)
        ).fetchone()[0]
        db.execute(f"ALTER TABLE comment_source RENAME CONSTRAINT {old} TO {new}")
        actual, text = db.execute(
            "SELECT oid,obj_description(oid,'pg_constraint') FROM pg_constraint WHERE conrelid='comment_source'::regclass AND conname=%s",
            (new,),
        ).fetchone()
        assert actual == identity and text == expected
        if old == "unique_before":
            actual, text = db.execute(
                "SELECT oid,obj_description(oid,'pg_class') FROM pg_class WHERE oid='unique_after'::regclass"
            ).fetchone()
            assert actual == index and text == "index note"
        assert (
            before_rows
            == db.execute(
                "SELECT xmin::text,ctid::text,* FROM comment_source"
            ).fetchall()
        )
    db.execute("CREATE TABLE comment_copy(LIKE comment_source INCLUDING ALL)")
    db.execute(
        "CREATE TABLE comment_no_copy(LIKE comment_source INCLUDING ALL EXCLUDING COMMENTS)"
    )
    for table, expected in (
        ("comment_copy", {"c": "check note", "u": None}),
        ("comment_no_copy", {"c": None, "u": None}),
    ):
        rows = db.execute(
            "SELECT contype,obj_description(oid,'pg_constraint'),conindid,oid FROM pg_constraint WHERE conrelid=%s::regclass ORDER BY contype",
            (table,),
        ).fetchall()
        assert {row[0]: row[1] for row in rows} == expected, (
            table,
            rows,
        )  # LIKE never copies FK policies.
        unique = next(row for row in rows if row[0] == "u")
        assert db.execute(
            "SELECT obj_description(%s,'pg_class')", (unique[2],)
        ).fetchone()[0] == ("index note" if table == "comment_copy" else None)
        source = db.execute(
            "SELECT oid,conindid FROM pg_constraint WHERE conrelid='comment_source'::regclass AND contype='u'"
        ).fetchone()
        assert unique[3] != source[0] and unique[2] != source[1]
    # Explicit relation names may overlap another table's CHECK namespace.
    db.execute("CREATE INDEX shared_ordinary ON comment_source(id)")
    db.execute(
        "COMMENT ON CONSTRAINT unique_after ON comment_source IS 'changed source'"
    )
    assert (
        db.execute(
            "SELECT obj_description(oid,'pg_constraint') FROM pg_constraint WHERE conrelid='comment_copy'::regclass AND contype='u'"
        ).fetchone()[0]
        is None
    )


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if entry["id"] in {"sql-0144", "sql-0145"}
    }
    cases = []
    with postgres() as db:
        db.execute(
            "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT,"
            "CONSTRAINT usage_records_status_check CHECK(status IN ('before','untouched')))"
        )
        db.execute("CREATE INDEX usage_records_status_idx ON usage_records(status)")
        db.execute(
            "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')"
        )
        before = db.execute(
            "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
        ).fetchall()
        db.execute("CREATE ROLE comment_reader")
        db.execute("GRANT SELECT ON usage_records TO comment_reader")
        for id_, target, lookup in (
            (
                "sql-0144",
                "INDEX usage_records_status_idx",
                "SELECT oid,obj_description(oid,'pg_class') FROM pg_class WHERE oid='usage_records_status_idx'::regclass",
            ),
            (
                "sql-0145",
                "CONSTRAINT usage_records_status_check ON usage_records",
                "SELECT oid,obj_description(oid,'pg_constraint') FROM pg_constraint WHERE conrelid='usage_records'::regclass AND conname='usage_records_status_check'",
            ),
        ):
            original = originals[id_]
            assert not original["params"]
            identity = db.execute(lookup).fetchone()[0]
            steps = []
            for sql in (
                original["sql"],
                f"COMMENT ON {target} IS 'a ''quoted'' Ω'",
                f"COMMENT ON {target} IS NULL",
                f"COMMENT ON {target} IS ''",
                original["sql"],
            ):
                result = db.execute(sql)
                assert result.statusmessage == "COMMENT"
                actual_id, description = db.execute(lookup).fetchone()
                assert actual_id == identity
                assert (
                    before
                    == db.execute(
                        "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
                    ).fetchall()
                )
                steps.append({"sql": sql, "description": description})
            try:
                with db.transaction():
                    db.execute("SET LOCAL ROLE comment_reader")
                    db.execute(original["sql"])
            except psycopg.Error as error:
                assert error.sqlstate == "42501"
            else:
                raise AssertionError("SELECT must not grant object COMMENT")
            assert db.execute(lookup).fetchone()[1] == steps[-1]["description"]
            cases.append({"id": id_, "steps": steps, "denied_sqlstate": "42501"})
        for sql, expected in (
            ("COMMENT ON INDEX missing_index IS 'no'", "42P01"),
            (
                "COMMENT ON CONSTRAINT missing_constraint ON usage_records IS 'no'",
                "42704",
            ),
        ):
            try:
                with db.transaction():
                    db.execute(sql)
            except psycopg.Error as error:
                assert error.sqlstate == expected
            else:
                raise AssertionError("missing object COMMENT must fail")
        qualify_lifecycle(db)
    return {
        "format": 1,
        "cases": cases,
        "missing_index_sqlstate": "42P01",
        "missing_constraint_sqlstate": "42704",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.check:
        assert json.loads(args.check.read_text()) == result
    elif args.output:
        args.output.write_text(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
