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

"""Exact document-integrity originals and complete unchanged PostgreSQL images.

json_patch and document validation are explicitly declared Antfly extensions;
PostgreSQL itself supplies INSERT indirection analysis and statement atomicity.
The native campaign independently verifies real schema and owner enforcement.
"""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_document_patch_reference import document_schema
from generate_sql_postgres_reference import FIXTURES, postgres
from psycopg.types.json import Jsonb

IDS = ("sql-0909", "sql-0941")
SEEDS = [
    {
        "key": "doc:a",
        "value": {"title": "Before", "amount": 1, "metadata": {"profile": "pro"}},
    },
    {
        "key": "doc:b",
        "value": {"title": "Other", "amount": 2, "metadata": {"profile": "free"}},
    },
    {"key": "doc:c", "value": {"title": "Untouched", "amount": 3, "cold": [1, 2, 3]}},
]


def reference():
    cases = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    schemas = {
        e["id"]: e["schema"]
        for e in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        db.execute("CREATE SCHEMA antfly")
        db.execute("""CREATE FUNCTION antfly.json_patch(target jsonb, patch jsonb) RETURNS jsonb
            LANGUAGE plpgsql IMMUTABLE AS $$ DECLARE op jsonb; result jsonb := target;
            BEGIN FOR op IN SELECT value FROM jsonb_array_elements(patch->'ops') LOOP
              IF op->>'op' <> 'set' THEN RAISE EXCEPTION 'unsupported witness operation' USING ERRCODE='22023'; END IF;
              result := jsonb_set(result, string_to_array(op->>'path','.'), op->'value', true);
            END LOOP; RETURN result; END $$""")
        # JSON Schema is an Antfly API contract, not a PostgreSQL builtin.
        # Keep its diagnostic explicit rather than confusing it with CHECK.
        db.execute("""CREATE FUNCTION antfly.validate_document() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW._doc ? 'amount' AND jsonb_typeof(NEW._doc->'amount') <> 'number' THEN
              RAISE EXCEPTION 'document amount must be numeric' USING ERRCODE='22023';
            END IF; RETURN NEW; END $$""")
        for identity in IDS:
            with db.transaction(force_rollback=True):
                db.execute(
                    "CREATE TABLE docs(_id text PRIMARY KEY,_doc jsonb NOT NULL,metadata jsonb,title text)"
                )
                if identity == "sql-0909":
                    db.execute(
                        "CREATE TRIGGER document_schema BEFORE INSERT OR UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION antfly.validate_document()"
                    )
                for seed in SEEDS:
                    db.execute(
                        "INSERT INTO docs VALUES(%s,%s,%s,%s)",
                        (
                            seed["key"],
                            Jsonb(seed["value"]),
                            Jsonb(seed["value"].get("metadata")),
                            seed["value"]["title"],
                        ),
                    )
                versions = db.execute(
                    "SELECT _id,xmin::text FROM docs ORDER BY _id"
                ).fetchall()
                try:
                    with db.transaction(), psycopg.RawCursor(db) as cursor:
                        cursor.execute(
                            cases[identity]["sql"], cases[identity]["params"]
                        )
                except psycopg.Error as error:
                    code = error.sqlstate
                else:
                    raise AssertionError(f"{identity}: missing required rejection")
                assert code == ("22023" if identity == "sql-0909" else "42804"), code
                final = [
                    {"key": k, "value": v}
                    for k, v in db.execute("SELECT _id,_doc FROM docs ORDER BY _id")
                ]
                assert final == SEEDS
                assert (
                    db.execute(
                        "SELECT _id,xmin::text FROM docs ORDER BY _id"
                    ).fetchall()
                    == versions
                )
                entries.append(
                    {
                        "id": identity,
                        "schema": document_schema(schemas[identity]),
                        "command_tag": "UPDATE" if identity == "sql-0909" else "INSERT",
                        "affected": 0,
                        "sqlstate": code,
                        "seeds": SEEDS,
                        "untouched": [r["key"] for r in SEEDS],
                        "final": final,
                    }
                )
        # PostgreSQL must resolve the root target, not a same-named leaf or a
        # relation qualifier. Missing roots and quoted dots remain distinct.
        db.execute(
            'CREATE TABLE assignments(id text PRIMARY KEY, metadata jsonb, profile text, "metadata.profile" text)'
        )
        probes = [
            ("INSERT INTO assignments(id,metadata.profile) VALUES('x','bad')", "42804"),
            (
                "INSERT INTO assignments(id,assignments.profile) VALUES('x','bad')",
                "42703",
            ),
            (
                "INSERT INTO assignments(id,missing.profile) SELECT id,profile FROM assignments",
                "42703",
            ),
        ]
        probes += [
            ("UPDATE assignments SET metadata.profile='bad' WHERE false", "42804"),
            ("UPDATE assignments SET assignments.profile='bad' WHERE false", "42703"),
            (
                "UPDATE assignments AS metadata SET metadata.profile='bad' WHERE false",
                "42804",
            ),
            (
                "INSERT INTO assignments(id) VALUES('x') ON CONFLICT(id) DO UPDATE SET metadata.profile='bad'",
                "42804",
            ),
            (
                "MERGE INTO assignments AS a USING(VALUES('x')) AS v(id) ON a.id=v.id WHEN MATCHED THEN UPDATE SET a.profile='bad'",
                "42703",
            ),
            (
                "MERGE INTO assignments AS a USING(VALUES('x')) AS v(id) ON a.id=v.id WHEN MATCHED THEN UPDATE SET metadata.profile='bad'",
                "42804",
            ),
        ]
        probes += [
            ("UPDATE assignments SET metadata.a.b.c.d.e='bad' WHERE false", "42804"),
            (
                "UPDATE assignments SET (metadata.profile,profile)=('bad','shadowed') WHERE false",
                "42804",
            ),
            (
                "MERGE INTO assignments AS a USING(VALUES('x')) AS v(id) ON a.id=v.id WHEN NOT MATCHED THEN INSERT(metadata.profile) VALUES('bad')",
                "42804",
            ),
        ]
        for statement, expected in probes:
            try:
                with db.transaction():
                    db.execute(statement)
            except psycopg.Error as error:
                assert error.sqlstate == expected, (statement, error.sqlstate)
            else:
                raise AssertionError(statement)
        db.execute(
            "INSERT INTO assignments(id,\"metadata.profile\") VALUES('x','literal dot')"
        )
        assert db.execute(
            'SELECT id,profile,"metadata.profile" FROM assignments'
        ).fetchall() == [("x", None, "literal dot")]
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL with explicit Antfly patch and document-validation extensions",
        "seed_version": 42,
        "seeds": SEEDS,
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    if args.output and args.check:
        parser.error("--check cannot overwrite evidence")
    result = reference()
    if args.check:
        assert json.loads(args.check.read_text()) == result, (
            "document integrity reference drift"
        )
        print(
            "verified both original document-integrity failures, unchanged complete images and INSERT target domains"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
