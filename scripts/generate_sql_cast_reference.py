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

"""PostgreSQL witnesses for database-wide casts and prepared dependencies."""

import argparse
import json
from pathlib import Path

from generate_sql_document_alias_reference import output
from generate_sql_operator_reference import reference as campaign
from generate_sql_postgres_reference import postgres

BEFORE = [
    "CREATE TABLE cast_rows(id integer PRIMARY KEY,value jsonb,label text)",
    'INSERT INTO cast_rows VALUES(1,\'{"kind":"a","n":1}\',NULL),(2,\'null\',NULL),(3,NULL,NULL)',
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "CREATE VIEW builtin_cast_view AS SELECT id,value::text AS converted FROM cast_rows",
    "CREATE FUNCTION jsonb_to_text(jsonb) RETURNS text LANGUAGE sql AS $$SELECT CASE WHEN $1 IS NULL THEN 'sql-null' ELSE coalesce($1->>'kind','json-null') END$$",
    "sql-0139",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "SELECT id,converted FROM builtin_cast_view ORDER BY id",
    "UPDATE cast_rows SET label=value RETURNING id,label",
    "SELECT id,label FROM cast_rows ORDER BY id",
    "sql-0139",
    "CREATE SCHEMA cast_tools",
    "CREATE FUNCTION cast_tools.flags(jsonb,integer,boolean) RETURNS text LANGUAGE sql AS $$SELECT concat($2::text,':',$3::text)$$",
    "DROP CAST (jsonb AS text)",
    "CREATE CAST (jsonb AS text) WITH FUNCTION cast_tools.flags(jsonb,integer,boolean) AS ASSIGNMENT",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "UPDATE cast_rows SET label=value RETURNING id,label",
    "ALTER SCHEMA cast_tools RENAME TO cast_renamed",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "DROP FUNCTION cast_renamed.flags(jsonb,integer,boolean)",
    "CREATE CAST (integer AS numeric) WITH FUNCTION jsonb_to_text(jsonb)",
    "CREATE FUNCTION cast_typmod(uuid,integer,boolean) RETURNS numeric LANGUAGE sql AS $$SELECT $2::numeric$$",
    "CREATE CAST (uuid AS numeric) WITH FUNCTION cast_typmod(uuid,integer,boolean) AS ASSIGNMENT",
    "SELECT id,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid::numeric(8,2) AS converted FROM cast_rows ORDER BY id",
    "DROP CAST (uuid AS numeric)",
    "CREATE FUNCTION wrong_source(text) RETURNS text LANGUAGE sql AS $$SELECT $1$$",
    "CREATE CAST (text AS jsonb) WITH FUNCTION wrong_source(text)",
]
COLD = [
    "SELECT id,value::text AS converted,label FROM cast_rows ORDER BY id",
    "SELECT id,converted FROM builtin_cast_view ORDER BY id",
]
AFTER = [
    "sql-0140",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "sql-0141",
    "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb)",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "UPDATE cast_rows SET label=value RETURNING id,label",
    "SELECT id,label FROM cast_rows ORDER BY id",
    "DROP CAST (jsonb AS text)",
    "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT",
    "DROP FUNCTION jsonb_to_text(jsonb) CASCADE",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "CREATE FUNCTION jsonb_to_text(jsonb) RETURNS text LANGUAGE sql AS $$SELECT 'fresh'$$",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT",
    "SELECT id,value::text AS converted FROM cast_rows ORDER BY id",
    "DROP CAST (jsonb AS text)",
]


def prepared_reference():
    setup = [
        "CREATE TABLE prepared_cast_rows(id integer PRIMARY KEY,value jsonb)",
        "INSERT INTO prepared_cast_rows VALUES(1,'{}'),(2,'null'),(3,NULL)",
        "CREATE FUNCTION prepared_cast(jsonb) RETURNS text LANGUAGE sql AS $$SELECT 'prepared'$$",
    ]
    statement = "SELECT id,value::text AS converted FROM prepared_cast_rows ORDER BY id"
    create = (
        "CREATE CAST (jsonb AS text) WITH FUNCTION prepared_cast(jsonb) AS ASSIGNMENT"
    )
    replace = "CREATE OR REPLACE FUNCTION prepared_cast(jsonb) RETURNS text LANGUAGE sql AS $$SELECT 'replaced'$$"
    with postgres() as db:
        for sql in setup:
            db.execute(sql)
        before = output(db.execute(statement))
        db.execute(create)
        after_create = output(db.execute(statement))
        db.execute(replace)
        after_replace = output(db.execute(statement))
    return {
        "setup": setup,
        "statement": statement,
        "before": before,
        "create": create,
        "after_create": after_create,
        "replace": replace,
        "after_replace": after_replace,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = campaign(
        BEFORE,
        COLD,
        AFTER,
        ("sql-0139", "sql-0140", "sql-0141"),
        "PostgreSQL 18 database casts and retained support identities",
    )
    value["prepared"] = prepared_reference()
    encoded = json.dumps(value, indent=2) + "\n"
    if args.check:
        assert args.output.read_text() == encoded, "PostgreSQL cast lifecycle drift"
    else:
        args.output.write_text(encoded)


if __name__ == "__main__":
    main()
