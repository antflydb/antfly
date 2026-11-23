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

"""Exact document alias DML against PostgreSQL updatable JSONB views."""

import argparse
import json
import subprocess
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output as raw_output

IDS = ["sql-0917", "sql-1082", "sql-1088"]
SEEDS = [
    {
        "key": "doc:a",
        "value": {
            "title": "Alpha",
            "metadata": {"plan": "free"},
            "metadata_plan": "decoy",
            "unknown": {"retained": [1, 2]},
        },
    },
    {
        "key": "doc:b",
        "value": {"title": "Beta", "metadata": {"plan": "pro"}, "unknown": "keep"},
    },
    {"key": "doc:c", "value": {"title": "Gamma", "metadata": {}, "unknown": None}},
]


def output(cursor):
    result = raw_output(cursor)
    result["sql_nulls"] = [[value is None for value in row] for row in result["rows"]]
    for row in result["rows"]:
        for ordinal, oid in enumerate(result["column_oids"]):
            if oid == 3802 and row[ordinal] is not None:
                row[ordinal] = json.loads(row[ordinal])
    return result


def reference():
    from psycopg.types.json import Jsonb

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source = json.loads(
        subprocess.check_output(
            ["git", "show", inventory["source_commit"] + ":" + inventory["source_path"]]
        )
    )["entries"]
    cases = {
        case["id"]: (case, original)
        for case, original in zip(inventory["entries"], source, strict=True)
    }
    entries = []
    with postgres() as db:
        for identity in IDS:
            case, original = cases[identity]
            schema = json.loads(original["source_schema_json"])
            db.execute("DROP VIEW IF EXISTS docs CASCADE")
            db.execute("DROP TABLE IF EXISTS document_source")
            db.execute(
                "CREATE TABLE document_source(_id TEXT PRIMARY KEY,_doc JSONB NOT NULL)"
            )
            seeds = [] if identity == "sql-0917" else SEEDS
            for seed in seeds:
                db.execute(
                    "INSERT INTO document_source VALUES(%s,%s)",
                    (seed["key"], Jsonb(seed["value"])),
                )
            # The alias is an Antfly schema extension. PostgreSQL's standard
            # updatable-view mechanism supplies an independent JSONB boundary;
            # the original SQL statements below are never rewritten.
            db.execute(
                "CREATE VIEW docs AS SELECT _id,_doc,_doc->'metadata'->>'plan' AS metadata_plan,_doc->>'title' AS title FROM document_source"
            )
            db.execute("""CREATE OR REPLACE FUNCTION write_alias_document() RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE body jsonb;
            BEGIN
              IF TG_OP = 'INSERT' THEN body := '{}'::jsonb; ELSE body := OLD._doc; END IF;
              IF TG_OP = 'INSERT' OR NEW.metadata_plan IS DISTINCT FROM OLD.metadata_plan THEN
                IF NEW.metadata_plan IS NULL THEN body := body #- '{metadata,plan}';
                ELSE
                  body := jsonb_set(body, '{metadata}', coalesce(nullif(body->'metadata','null'::jsonb), '{}'::jsonb), true);
                  body := jsonb_set(body, '{metadata,plan}', to_jsonb(NEW.metadata_plan), true);
                END IF;
              END IF;
              IF TG_OP = 'INSERT' OR NEW.title IS DISTINCT FROM OLD.title THEN
                IF NEW.title IS NULL THEN body := body - 'title';
                ELSE body := jsonb_set(body, '{title}', to_jsonb(NEW.title), true); END IF;
              END IF;
              IF TG_OP = 'INSERT' THEN INSERT INTO document_source VALUES(NEW._id,body);
              ELSE UPDATE document_source SET _doc=body WHERE _id=OLD._id; END IF;
              NEW._doc := body;
              RETURN NEW;
            END $$""")
            db.execute(
                "CREATE TRIGGER alias_document_write INSTEAD OF INSERT OR UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION write_alias_document()"
            )
            cursor = db.execute(case["sql"])
            affected = cursor.rowcount
            rows = output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id"))
            aliases = output(
                db.execute("SELECT _id,metadata_plan FROM docs ORDER BY _id")
            )
            db.execute("UPDATE docs SET metadata_plan=NULL WHERE _id='doc:a'")
            null_rows = output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id"))
            db.execute("INSERT INTO docs(_id,metadata_plan) VALUES('doc:cold','cold')")
            cold_rows = output(db.execute("SELECT _id,_doc FROM docs ORDER BY _id"))
            entries.append(
                {
                    "id": identity,
                    "sql": case["sql"],
                    "schema": schema,
                    "seeds": seeds,
                    "affected": affected,
                    "rows": rows,
                    "aliases": aliases,
                    "null_rows": null_rows,
                    "cold_rows": cold_rows,
                }
            )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL over declared updatable JSONB aliases",
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document alias PostgreSQL witness drift")
        print(
            "verified three document alias DML originals and complete JSONB/cold postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
