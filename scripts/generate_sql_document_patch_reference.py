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

"""Exact original document patches over PostgreSQL JSONB extension primitives.

The Antfly-specific function is defined explicitly, not claimed as a PostgreSQL
builtin. Original SQL, physical version predicates and complete postimages are
retained. Native storage independently qualifies schema and commit enforcement.
"""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres, cursor_result

IDS = list(range(895, 907))
SEEDS = [
    {
        "key": "doc:a",
        "value": {
            "title": "Before",
            "obsolete": True,
            "cold": [1, 2, 3],
            "metadata": {"source": "api"},
        },
    },
    {"key": "doc:b", "value": {"title": "Other", "note": "preserved"}},
    {"key": "doc:c", "value": {"title": "Untouched", "note": "retained"}},
]


def reference():
    import psycopg
    from psycopg.types.json import Jsonb

    original = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    schemas = {
        case["id"]: case["schema"]
        for case in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        db.execute("CREATE SCHEMA antfly")
        db.execute("""CREATE FUNCTION antfly.document_merge(a jsonb,b jsonb) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
            SELECT CASE WHEN jsonb_typeof(a)='object' AND jsonb_typeof(b)='object' THEN
            (SELECT COALESCE(jsonb_object_agg(k,CASE WHEN a ? k AND b ? k THEN antfly.document_merge(a->k,b->k)
                WHEN b ? k THEN b->k ELSE a->k END),'{}'::jsonb)
             FROM (SELECT jsonb_object_keys(a) k UNION SELECT jsonb_object_keys(b)) keys)
            ELSE b END $$""")
        db.execute("""CREATE FUNCTION antfly.json_patch(target jsonb,patch jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
            DECLARE op jsonb; result jsonb := target; path text[];
            BEGIN FOR op IN SELECT value FROM jsonb_array_elements(patch->'ops') LOOP
                path := string_to_array(op->>'path','.');
                CASE op->>'op'
                WHEN 'set' THEN result := jsonb_set(result,path,op->'value',true);
                WHEN 'remove' THEN result := result #- path;
                WHEN 'merge' THEN result := antfly.document_merge(result,op->'value');
                ELSE RAISE EXCEPTION 'unknown patch operation' USING ERRCODE='22023';
                END CASE;
            END LOOP; RETURN result; END $$""")
        for number in IDS:
            identity = f"sql-{number:04}"
            with db.transaction(force_rollback=True):
                db.execute(
                    "CREATE TABLE docs(_id text PRIMARY KEY,_doc jsonb NOT NULL,_version bigint NOT NULL)"
                )
                for row in SEEDS:
                    db.execute(
                        "INSERT INTO docs VALUES(%s,%s,42)",
                        (row["key"], Jsonb(row["value"])),
                    )
                before_versions = dict(db.execute("SELECT _id,xmin::text FROM docs"))
                sqlstate = None
                try:
                    with db.transaction():
                        with psycopg.RawCursor(db) as cursor:
                            outcome = cursor_result(
                                cursor, original[identity], read=False
                            )
                except psycopg.Error as error:
                    sqlstate = error.sqlstate
                    outcome = {"affected": 0}
                expected_error = "22023" if number == 904 else None
                if sqlstate != expected_error:
                    raise ValueError(
                        f"{identity}: expected {expected_error}, got {sqlstate}"
                    )
                if sqlstate is None and outcome["affected"] == 0:
                    raise ValueError("non-exercising document patch witness")
                entries.append(
                    {
                        "id": identity,
                        # Historical compile-only rejections had no storage
                        # profile. Execute them against the same permissive
                        # raw-document table as the base JSON patch contract.
                        "schema": document_schema(
                            schemas.get(identity, schemas["sql-0895"])
                        ),
                        "affected": outcome["affected"],
                        "sqlstate": sqlstate,
                        "untouched": [
                            key
                            for key, version in db.execute(
                                "SELECT _id,xmin::text FROM docs ORDER BY _id"
                            )
                            if before_versions.get(key) == version
                        ],
                        "final": [
                            {"key": key, "value": value}
                            for key, value in db.execute(
                                "SELECT _id,_doc FROM docs ORDER BY _id"
                            )
                        ],
                    }
                )
    return {
        "format": 1,
        "reference": "PostgreSQL JSONB with explicit Antfly patch extension",
        "schema_mapping": "Arbitrary JSON document fields use unrestricted JSON Schema properties, not relational-only type=json declarations.",
        "seed_version": 42,
        "seeds": SEEDS,
        "entries": entries,
    }


def document_schema(source):
    """Keep the JSON value domain while using the native document contract.

    The extracted mega-branch profile used a relational-only raw type spelling.
    An unrestricted JSON Schema property is the document equivalent, and SQL
    derives the same JSONB column from it. Never enable relational physical
    encodings or bypass the document schema validator for this campaign.
    """
    from copy import deepcopy

    result = deepcopy(source)
    assert result["storage_mode"] == "document"
    props = result["document_schemas"][result["default_type"]]["schema"]["properties"]
    for prop in props.values():
        if prop.get("type") == "json":
            del prop["type"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.check and args.output:
        parser.error("--check cannot overwrite a frozen reference")
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("original document patch reference drift")
        print(f"verified {len(result['entries'])} exact original document patches")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
