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

"""Original FK VALIDATE lifecycle, checked against an owned PostgreSQL 18+ server."""

import argparse
import json
import subprocess
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source = json.loads(
        subprocess.check_output(
            [
                "git",
                "show",
                inventory["source_commit"] + ":" + inventory["source_path"],
            ],
            text=True,
        )
    )["entries"][735]
    first = "00000000-0000-0000-0000-000000000001"
    second = "00000000-0000-0000-0000-000000000002"
    select = "SELECT id::text,user_id FROM usage_records ORDER BY id"
    catalog = "SELECT convalidated FROM pg_constraint WHERE conname='usage_records_user_id_fkey'"
    steps = []
    with postgres() as db:

        def step(sql, state=None):
            try:
                cursor = db.execute(sql)
                if state is not None:
                    raise AssertionError(f"expected {state}: {sql}")
                value = {
                    "sql": sql,
                    "sqlstate": None,
                    "tag": cursor.statusmessage,
                    "output": output(cursor),
                }
            except psycopg.Error as error:
                if error.sqlstate != state:
                    raise
                value = {"sql": sql, "sqlstate": state, "tag": None, "output": None}
            steps.append(value)

        db.execute("CREATE TABLE users(id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO users(id) VALUES('known'),('spare')")
        cast_labels = output(
            db.execute(
                "SELECT id::text, q.id::text::text, CAST(q.id AS text), lower(q.id)::text, q.id::text AS explicit_name FROM users q ORDER BY q.id"
            )
        )
        assert cast_labels["columns"] == ["id", "id", "id", "lower", "explicit_name"], (
            cast_labels["columns"]
        )
        setup = source["apply_setup_sql"]
        assert len(setup) == 2
        db.execute(setup[0])
        db.execute(
            f"INSERT INTO usage_records(id,user_id) VALUES('{first}','missing'),('{second}','known')"
        )
        db.execute(setup[1])
        step(select)
        step(catalog)
        step(source["sql"], "23503")
        step(select)
        step(catalog)
        step(f"UPDATE usage_records SET user_id='known' WHERE id='{first}'")
        step(source["sql"])
        step(select)
        step(catalog)
        step(
            "INSERT INTO usage_records(id,user_id) VALUES('00000000-0000-0000-0000-000000000003','missing')",
            "23503",
        )
        step("DELETE FROM users WHERE id='known'", "23503")
        step(source["sql"])
        step(select)
        step(catalog)
    return {
        "id": "sql-0736",
        "source_setup": setup,
        "original_sql": source["sql"],
        "steps": steps,
        "cast_labels": cast_labels,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("FK validation PostgreSQL witness drift")
        print(
            f"verified original sql-0736 and {len(result['steps'])} lifecycle witnesses"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
