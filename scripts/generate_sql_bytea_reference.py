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

"""Qualify strict native binary/text conversion against PostgreSQL 18+."""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres, read_reference


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    original = next(e for e in inventory["entries"] if e["id"] == "sql-0274")
    profile = {
        "schema": {
            "version": 1,
            "storage_mode": "relational",
            "default_type": "row",
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"id": {"type": "keyword"}},
                        "required": ["id"],
                    }
                }
            },
        },
        "rows": [{"key": str(i), "value": {"id": str(i)}} for i in range(7)],
    }
    expressions = [
        "convert_from(convert_to('café','UTF8'),'UTF8')",
        "to_jsonb(convert_to('café','UTF8'))",
        "jsonb_build_object('raw',convert_to('café','UTF8'))",
        "convert_from('\\x636166e9'::bytea,'LATIN1')",
        "convert_from('\\x80e9'::bytea,'LATIN1')",
        "convert_from('\\x80e9'::bytea,'WIN1252')",
        "convert_to('€é','WIN1252')",
        "convert_from('\\xefbbbf6869'::bytea,'UTF8')",
        "convert_from(NULL::bytea,'UTF8')",
        "convert_from('foo'::bytea,NULL)",
        "convert_to(NULL::text,'UTF8')",
        "convert_from('\\x'::bytea,'UTF-8')",
        "convert_from('\\xff'::bytea,'UTF8')",
        "convert_from('\\x00'::bytea,'UTF8')",
        "convert_from('\\xc080'::bytea,'UTF8')",
        "convert_from('\\xeda080'::bytea,'UTF8')",
        "convert_from('\\x81'::bytea,'WIN1252')",
        "convert_to('😀','LATIN1')",
        "convert_from('','no_such_encoding')",
        "convert_from('x'::text,'UTF8')",
        "'\\x0'::bytea",
        "'\\x0 0'::bytea",
        "'\\400'::bytea",
        "'\\x00 7f\t80ff'::bytea",
    ]
    for charset in ("LATIN1", "WIN1252"):
        for byte in range(256):
            literal = "'\\x" + bytes([byte]).hex() + "'::bytea"
            expressions.append(f"convert_from({literal},'{charset}')")
            expressions.append(
                f"convert_to(convert_from({literal},'{charset}'),'{charset}')"
            )
    for point in (1, 127, 128, 2047, 2048, 0xD7FF, 0xE000, 0xFFFF, 0x10000, 0x10FFFF):
        literal = "'\\x" + chr(point).encode().hex() + "'::bytea"
        expressions.append(f"convert_from({literal},'UTF8')")
    queries = [
        "SELECT convert_to('café','UTF8') AS b",
        "SELECT b,count(*) FROM (VALUES (convert_to('a','UTF8')),(convert_to('b','UTF8')),(convert_to('a','UTF8')),(NULL::bytea)) v(b) GROUP BY b ORDER BY b NULLS LAST",
        "SELECT l.b FROM (VALUES (convert_to('a','UTF8')),(convert_to('b','UTF8'))) l(b) JOIN (VALUES (convert_to('b','UTF8'))) r(b) ON l.b=r.b",
        "SELECT DISTINCT b FROM (VALUES ('\\x00'::bytea),('\\xff'::bytea),('\\x'::bytea),(NULL::bytea),('\\x00ff'::bytea),('\\x00'::bytea)) v(b) ORDER BY b NULLS FIRST",
        "SELECT convert_to('€é','WIN1252') AS b",
    ]
    query_results = []
    scalars = []
    with postgres() as db:
        read = read_reference(db, [original], profile)
        for sql in queries:
            cursor = db.execute(sql)
            oids = [column.type_code for column in cursor.description]
            rows = [
                [
                    "\\x" + value.hex()
                    if isinstance(value, bytes)
                    else str(value)
                    if oid == 20 and value is not None
                    else value
                    for value, oid in zip(row, oids, strict=True)
                ]
                for row in cursor.fetchall()
            ]
            query_results.append(dict(sql=sql, rows=rows, oids=oids))
        for sql in expressions:
            with db.transaction(force_rollback=True):
                try:
                    cursor = db.execute("SELECT " + sql)
                    value = cursor.fetchone()[0]
                    if isinstance(value, bytes):
                        value = "\\x" + value.hex()
                    scalars.append(
                        dict(
                            sql=sql,
                            value=value,
                            oid=cursor.description[0].type_code,
                            sqlstate=None,
                        )
                    )
                except psycopg.Error as error:
                    scalars.append(
                        dict(sql=sql, value=None, oid=None, sqlstate=error.sqlstate)
                    )
    return dict(read=read, scalars=scalars, queries=query_results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=FIXTURES / "sql_bytea_reference.json"
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = json.dumps(reference(), indent=2, ensure_ascii=False) + "\n"
    if args.check:
        if not args.output.exists() or args.output.read_text() != rendered:
            raise SystemExit(f"PostgreSQL bytea reference differs: {args.output}")
        print(
            "Verified unchanged sql-0274 and 1,058 strict conversion contracts and 5 buffered queries"
        )
    else:
        args.output.write_text(rendered)
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
