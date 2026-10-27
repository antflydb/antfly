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

"""Independent PostgreSQL witness for view temporal domains and immutable envelopes."""

import argparse
import json
import subprocess
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_document_alias_reference import output

SELECT = "SELECT id,DATE '2000-01-01' AS d,TIME '01:02:03' AS t,TIMETZ '01:02:03+02' AS z,TIMESTAMP '2000-01-01' AS s,TIMESTAMPTZ '2000-01-01' AS u,INTERVAL '1 day' AS i FROM usage_records"
READ = "SELECT * FROM outer_view ORDER BY id"
SEEDS = [
    {"key": "doc:a", "value": {"title": "Alpha", "unknown": {"retained": [1, 2]}}},
    {"key": "doc:b", "value": {"title": "Beta", "unknown": None}},
]


def reference():
    from psycopg.types.json import Jsonb

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    original = json.loads(
        subprocess.check_output(
            ["git", "show", inventory["source_commit"] + ":" + inventory["source_path"]]
        )
    )["entries"]
    case, source = next(
        (c, o)
        for c, o in zip(inventory["entries"], original, strict=True)
        if c["id"] == "sql-1073"
    )
    with postgres() as db:
        db.execute("CREATE TABLE usage_records(id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO usage_records(id) VALUES('a'),('b')")
        db.execute("CREATE VIEW inner_view AS " + SELECT)
        db.execute("CREATE VIEW outer_view AS SELECT * FROM inner_view")
        before = output(db.execute(READ))
        incompatible = "CREATE OR REPLACE VIEW inner_view AS " + SELECT.replace(
            "DATE '2000-01-01'", "TIMESTAMP '2000-01-01'", 1
        )
        try:
            db.execute(incompatible)
            raise AssertionError("temporal replacement accepted")
        except Exception as exc:
            assert getattr(exc, "sqlstate", None) == "42P16", exc
        assert output(db.execute(READ)) == before
        replacement = "CREATE OR REPLACE VIEW inner_view AS " + SELECT.replace(
            "DATE '2000-01-01'", "DATE '2001-02-03'", 1
        )
        db.execute(replacement)
        db.execute("ALTER VIEW inner_view RENAME TO renamed_view")
        db.execute("ALTER TABLE usage_records ADD COLUMN note TEXT")
        after = output(db.execute(READ))
        db.execute("UPDATE usage_records SET id='cold' WHERE id='a'")
        cold = output(db.execute(READ))
        try:
            db.execute("DROP VIEW renamed_view RESTRICT")
            raise AssertionError("dependent view dropped")
        except Exception as exc:
            assert getattr(exc, "sqlstate", None) == "2BP01", exc
        db.execute("DROP VIEW renamed_view CASCADE")
        # Antfly's engine-owned _id is modeled by a PostgreSQL generated
        # column. Ordinary PostgreSQL user columns named _id remain writable.
        db.execute(
            "CREATE TABLE docs(key TEXT PRIMARY KEY,_id TEXT GENERATED ALWAYS AS(key) STORED,_doc JSONB NOT NULL,title TEXT GENERATED ALWAYS AS(_doc->>'title') STORED)"
        )
        for seed in SEEDS:
            db.execute(
                "INSERT INTO docs(key,_doc) VALUES(%s,%s)",
                (seed["key"], Jsonb(seed["value"])),
            )
        docs = output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id"))
        try:
            db.execute(case["sql"])
            raise AssertionError("generated identity accepted")
        except Exception as exc:
            assert getattr(exc, "sqlstate", None) == "428C9", exc
        assert output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id")) == docs
        mutation = "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"Cold\"'::jsonb) WHERE _id='doc:a'"
        db.execute(mutation)
        changed = output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id"))
    return {
        "format": 1,
        "reference": "PostgreSQL 18 views and generated identity envelope",
        "view": {
            "select": SELECT,
            "read": READ,
            "before": before,
            "incompatible": incompatible,
            "replacement": replacement,
            "after": after,
            "cold": cold,
        },
        "document": {
            "id": case["id"],
            "sql": case["sql"],
            "schema": json.loads(source["source_schema_json"]),
            "seeds": SEEDS,
            "before": docs,
            "mutation": mutation,
            "after": changed,
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("view domain PostgreSQL witness drift")
        print(
            "verified temporal view lifecycle and sql-1073 generated-envelope rejection/postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
