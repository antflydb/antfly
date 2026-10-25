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

"""Qualify document producers using real PostgreSQL STORED generated columns.

Original SQL is unchanged. Historical inert property metadata is translated
explicitly into today's public generated_columns contract, never accepted as
producer authority. PostgreSQL triggers adapt only the document JSON boundary;
PostgreSQL itself computes values and enforces generated-column write errors.
"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess

from generate_sql_document_alias_reference import output
from generate_sql_postgres_reference import FIXTURES, postgres

IDS = ["sql-0910", "sql-0920", "sql-1072", "sql-1074", "sql-1079"]


def declaration(source):
    schema = deepcopy(source)
    schema.pop("version", None)
    schema["enforce_types"] = True
    declarations = []

    def expression(node):
        if "field" in node:
            return {"op": "column", "column": node["field"]}
        return {
            key: (
                [expression(arg) for arg in value]
                if key == "args"
                else "lower_ascii"
                if key == "op" and value == "lower"
                else value
            )
            for key, value in node.items()
        }

    properties = schema["document_schemas"]["doc"]["schema"]["properties"]
    for name, prop in properties.items():
        if "generated" in prop:
            original = prop.pop("generated")
            assert original["op"] == "expression"
            declarations.append(
                {"column": name, "expression": expression(original["expression"])}
            )
    schema["generated_columns"] = declarations
    return schema


def reference():
    import psycopg
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
        db.execute("CREATE SCHEMA antfly")
        db.execute("""CREATE FUNCTION antfly.json_patch(target jsonb, patch jsonb)
          RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
          DECLARE operation jsonb; result jsonb := target;
          BEGIN FOR operation IN SELECT value FROM jsonb_array_elements(patch->'ops') LOOP
            IF operation->>'op' <> 'set' THEN RAISE EXCEPTION 'unsupported oracle patch'; END IF;
            result := jsonb_set(result,string_to_array(operation->>'path','.'),operation->'value',true);
          END LOOP; RETURN result; END $$""")
        for identity in IDS:
            case, original = cases[identity]
            source_schema = json.loads(original["source_schema_json"])
            schema = declaration(source_schema)
            generated = schema["generated_columns"][0]["column"]
            base = schema["generated_columns"][0]["expression"]["args"][0]["column"]
            assert (base, generated) in {
                ("title", "title_lc"),
                ("status", "status_lower"),
            }
            with db.transaction(force_rollback=True):
                db.execute(
                    f"CREATE TABLE docs(_id text PRIMARY KEY,_doc jsonb NOT NULL DEFAULT '{{}}',"
                    f"{base} text,{generated} text GENERATED ALWAYS AS (lower({base})) STORED)"
                )
                db.execute(f"""CREATE OR REPLACE FUNCTION document_input() RETURNS trigger LANGUAGE plpgsql AS $$
                  BEGIN
                    IF TG_OP='INSERT' THEN
                      IF NEW._doc ? '{base}' THEN NEW.{base} := NEW._doc->>'{base}'; END IF;
                    ELSIF NEW._doc IS DISTINCT FROM OLD._doc THEN NEW.{base} := NEW._doc->>'{base}';
                    END IF;
                    IF NEW.{base} IS NULL THEN NEW._doc := NEW._doc - '{base}';
                    ELSE NEW._doc := jsonb_set(NEW._doc,'{{{base}}}',to_jsonb(NEW.{base}),true); END IF;
                    NEW._doc := NEW._doc - '{generated}';
                    RETURN NEW;
                  END $$""")
                db.execute(
                    "CREATE TRIGGER document_input BEFORE INSERT OR UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION document_input()"
                )
                seeds = [
                    {
                        "key": "doc:a",
                        "value": {base: "LAUNCH", "unknown": {"a": [1, None]}},
                    },
                    {"key": "doc:b", "value": {base: "OTHER", "keep": True}},
                    {"key": "doc:missing", "value": {"unknown": "absent"}},
                ]
                for seed in seeds:
                    db.execute(
                        "INSERT INTO docs(_id,_doc) VALUES(%s,%s)",
                        (seed["key"], Jsonb(seed["value"])),
                    )
                state_sql = f"SELECT _id,_doc || CASE WHEN {generated} IS NULL THEN '{{}}'::jsonb ELSE jsonb_build_object('{generated}',{generated}) END AS _doc FROM docs ORDER BY _id"
                before = output(db.execute(state_sql))
                sqlstate = None
                affected = 0
                try:
                    with db.transaction():
                        cursor = db.execute(case["sql"])
                        affected = cursor.rowcount
                except psycopg.Error as error:
                    sqlstate = error.sqlstate
                rows = output(db.execute(state_sql))
                db.execute(f"UPDATE docs SET {base}='COLD' WHERE _id='doc:a'")
                cold_rows = output(db.execute(state_sql))
                entries.append(
                    {
                        "id": identity,
                        "sql": case["sql"],
                        "source_schema": source_schema,
                        "schema": schema,
                        "base": base,
                        "generated": generated,
                        "seeds": seeds,
                        "affected": affected,
                        "sqlstate": sqlstate,
                        "before": before,
                        "rows": rows,
                        "cold_rows": cold_rows,
                    }
                )
    return {
        "format": 1,
        "reference": "PostgreSQL STORED generated columns with explicit JSON input adaptation",
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
            raise SystemExit("document producer PostgreSQL witness drift")
        print(
            "verified five original document producer mutations, write errors and complete postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
