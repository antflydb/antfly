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

"""PostgreSQL witnesses for original schema/database COMMENT statements."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    cases = []
    with postgres() as db:
        db.execute("CREATE DATABASE appdb")
        db.execute("CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)")
        db.execute(
            "INSERT INTO usage_records VALUES ('u1','before'),('u9','untouched')"
        )
        before = db.execute(
            "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
        ).fetchall()
        db.execute("CREATE ROLE comment_reader")
        db.execute("GRANT SELECT ON usage_records TO comment_reader")
        for case_id, kind, name, catalog, description in [
            ("sql-0146", "SCHEMA", "public", "pg_namespace", "obj_description"),
            ("sql-0147", "DATABASE", "appdb", "pg_database", "shobj_description"),
        ]:
            original = next(entry for entry in inventory if entry["id"] == case_id)
            assert not original["params"]
            column = "nspname" if kind == "SCHEMA" else "datname"

            def observe(
                target,
                query=f"SELECT oid,{description}(oid,'{catalog}') FROM {catalog} WHERE {column}=%s",
            ):
                return db.execute(query, (target,)).fetchone()

            identity = observe(name)[0]
            statements = [
                original["sql"],
                f"COMMENT ON {kind} {name} IS 'a ''quoted'' Ω'",
                f"COMMENT ON {kind} {name} IS NULL",
                f"COMMENT ON {kind} {name} IS ''",
                original["sql"],
            ]
            steps = []
            for statement in statements:
                assert db.execute(statement).statusmessage == "COMMENT"
                row = observe(name)
                assert row[0] == identity
                assert (
                    db.execute(
                        "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
                    ).fetchall()
                    == before
                )
                steps.append({"sql": statement, "description": row[1]})
            errors = {}
            for label, statement, role in [
                ("denied_sqlstate", original["sql"], "comment_reader"),
                (
                    "missing_sqlstate",
                    f"COMMENT ON {kind} missing_comment_object IS 'x'",
                    None,
                ),
            ]:
                try:
                    with db.transaction():
                        if role:
                            db.execute("SET LOCAL ROLE comment_reader")
                        db.execute(statement)
                except psycopg.Error as error:
                    errors[label] = error.sqlstate
                else:
                    # PostgreSQL warns, but succeeds, for an absent DATABASE.
                    assert label == "missing_sqlstate" and kind == "DATABASE"
                    errors[label] = None
            db.execute(f"CREATE {kind} comment_scope")
            db.execute(f"COMMENT ON {kind} comment_scope IS 'retained'")
            prior = observe("comment_scope")
            db.execute(f"ALTER {kind} comment_scope RENAME TO comment_scope_moved")
            assert observe("comment_scope_moved") == prior
            db.execute(f"DROP {kind} comment_scope_moved")
            db.execute(f"CREATE {kind} comment_scope_moved")
            recreated = observe("comment_scope_moved")
            assert recreated[0] != prior[0] and recreated[1] is None
            db.execute(f"DROP {kind} comment_scope_moved")
            assert observe(name) == (identity, steps[-1]["description"])
            cases.append(
                {
                    "id": case_id,
                    "kind": kind.lower(),
                    "name": name,
                    "steps": steps,
                    **errors,
                    "rename_preserves_identity": True,
                    "recreate_clears_description": True,
                }
            )
    return {"format": 1, "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("resource COMMENT PostgreSQL witness drift")
        print(
            "verified original schema/database COMMENT, removal, ownership, identity and unchanged rows"
        )
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
