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

"""PostgreSQL ordinary ordered aggregates and original sql-1230 admission."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_postgres_reference import ARRAY_TYPES, array_reference
import datetime
import struct
from decimal import Decimal


def numeric_array(data):
    # PostgreSQL binary array framing; numeric cells use psycopg's independent
    # PostgreSQL decoder, never the native engine or floating conversion.
    from psycopg.types.numeric import NumericBinaryLoader

    if len(data) > 8 * 1024 * 1024 or len(data) < 12:
        raise ValueError("numeric array frame bounds")
    rank, flags, element = struct.unpack_from("!iii", data)
    if not 0 <= rank <= 6 or flags not in (0, 1) or element != 1700:
        raise ValueError("numeric array header")
    at = 12
    dimensions = []
    count = 1 if rank else 0
    for _ in range(rank):
        length, lower = struct.unpack_from("!ii", data, at)
        at += 8
        if length <= 0:
            raise ValueError("numeric array dimension")
        count *= length
        if count > 65536:
            raise ValueError("numeric array element bound")
        dimensions.append({"length": length, "lower_bound": lower})
    values = []
    nulls = []
    loader = NumericBinaryLoader(1700)
    for _ in range(count):
        length = struct.unpack_from("!i", data, at)[0]
        at += 4
        if length == -1:
            values.append(None)
            nulls.append(True)
        else:
            if length < 0 or length > len(data) - at:
                raise ValueError("numeric array payload bound")
            values.append(str(loader.load(data[at : at + length])))
            nulls.append(False)
            at += length
    if at != len(data) or (any(nulls) and not flags):
        raise ValueError("numeric array trailer")
    return {"dimensions": dimensions, "values": values, "sql_nulls": nulls}


def output(cursor):
    rows = cursor.fetchall()
    result = {
        "columns": [c.name for c in cursor.description],
        "column_oids": [c.type_code for c in cursor.description],
        "rows": [],
        "sql_nulls": [],
    }
    for index, row in enumerate(rows):
        values = []
        nulls = []
        for ordinal, (column, value) in enumerate(
            zip(cursor.description, row, strict=True)
        ):
            raw = cursor.pgresult.get_value(index, ordinal)
            nulls.append(raw is None)
            if raw is None:
                values.append(None)
            elif column.type_code in ARRAY_TYPES:
                values.append(array_reference(raw, column.type_code))
            elif column.type_code == 1231:
                values.append(numeric_array(raw))
            elif isinstance(value, Decimal):
                values.append(str(value))
            elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                values.append(value.isoformat())
            else:
                values.append(value)
        result["rows"].append(values)
        result["sql_nulls"].append(nulls)
    return result


CREATE = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,organization_id TEXT,status TEXT,metric_type TEXT,created_at TIMESTAMPTZ,amount NUMERIC(12,3),quantity INTEGER,tags SMALLINT[],metadata JSONB)"
SEED = """INSERT INTO usage_records(id,organization_id,status,metric_type,created_at,amount,quantity,tags,metadata) VALUES
('a','one','z','tokens','2025-01-03',10.125,1,ARRAY[2,3]::smallint[],'{"n":1}'::jsonb),
('b','one','a','tokens','2025-01-02',2.250,2,ARRAY[NULL,5]::smallint[],'null'::jsonb),
('c','one','z','tokens','2025-01-01',5.500,3,ARRAY[6,7]::smallint[],'{"n":1}'::jsonb),
('d','two',NULL,'tokens','2025-01-02',NULL,NULL,ARRAY[8,9]::smallint[],NULL::jsonb),
('e','two','','tokens',NULL,0.000,0,ARRAY[10,11]::smallint[],'{"n":2}'::jsonb),
('f',NULL,'a','tokens','2025-01-04',3.125,-1,NULL::smallint[],'{"n":3}'::jsonb),
('g','one','é','other','2025-01-05',8.000,5,ARRAY[]::smallint[],'{}'::jsonb)"""
READ = "SELECT * FROM usage_records ORDER BY id"


def statements():
    result = []
    for grouped in (False, True):
        prefix = "organization_id," if grouped else ""
        suffix = (
            " GROUP BY organization_id ORDER BY organization_id NULLS FIRST"
            if grouped
            else ""
        )
        for order in (
            "id ASC",
            "id DESC",
            "created_at DESC NULLS LAST,id",
            "quantity NULLS FIRST,id",
        ):
            for condition in ("TRUE", "FALSE", "quantity>0"):
                for expression in (
                    "string_agg(status,'|' ORDER BY {order})",
                    "array_agg(status ORDER BY {order})",
                    "array_agg(amount ORDER BY {order})",
                    "array_agg(metadata ORDER BY {order})",
                    "sum(quantity ORDER BY {order})",
                    "avg(amount ORDER BY {order})",
                ):
                    aggregate = expression.format(order=order)
                    result.append(
                        f"SELECT {prefix}{aggregate} FILTER(WHERE {condition}) AS value,count(*) AS n FROM usage_records{suffix}"
                    )
        for expression in (
            "string_agg(DISTINCT status,'|' ORDER BY status)",
            "string_agg(DISTINCT status,'|' ORDER BY status DESC NULLS FIRST)",
            "string_agg(status,CASE WHEN quantity%2=0 THEN ':' ELSE NULL END ORDER BY id)",
            "string_agg(DISTINCT status,CASE WHEN quantity%2=0 THEN ':' ELSE NULL END ORDER BY status,CASE WHEN quantity%2=0 THEN ':' ELSE NULL END)",
            "array_agg(DISTINCT status ORDER BY status NULLS FIRST)",
            "array_agg(DISTINCT amount ORDER BY amount DESC)",
            "array_agg(DISTINCT metadata ORDER BY metadata)",
            "sum(DISTINCT quantity ORDER BY quantity)",
            "count(DISTINCT quantity ORDER BY quantity)",
            "min(status ORDER BY id)",
            "max(amount ORDER BY created_at)",
            "bool_and(quantity>0 ORDER BY id)",
            "pg_catalog.string_agg(status,',' ORDER BY id)",
            "pg_catalog.array_agg(status ORDER BY id)",
            "string_agg((1/0)::text,',' ORDER BY 1/0) FILTER(WHERE FALSE)",
            "array_agg(1/0 ORDER BY 1/0) FILTER(WHERE FALSE)",
            "array_agg(tags ORDER BY id) FILTER(WHERE metric_type='tokens' AND tags IS NOT NULL)",
        ):
            result.append(
                f"SELECT {prefix}{expression} AS value,count(*) AS n FROM usage_records{suffix}"
            )
    result += [
        "SELECT string_agg(DISTINCT status,',' ORDER BY status::text),array_agg(DISTINCT quantity ORDER BY quantity::integer) FROM usage_records",
        "SELECT string_agg(DISTINCT lower(status),',' ORDER BY lower(status::text)) FROM usage_records",
        "SELECT string_agg(DISTINCT status,',' ORDER BY status::integer) FROM usage_records",
        "SELECT coalesce(ALL status,'fallback') FROM usage_records",
        "SELECT ROW(ALL quantity) FROM usage_records",
        "SELECT string_agg(ALL status,',' ORDER BY id),array_agg(ALL status ORDER BY id),sum(ALL quantity ORDER BY id) FROM usage_records",
        "SELECT coalesce(string_agg(status,','),'none') AS value FROM (SELECT status FROM usage_records ORDER BY id) AS sorted",
        # Mixed ordered aggregates may cause PostgreSQL to reorder all inputs.
        # Explicit aggregate keys make this result independent of plan choice.
        "SELECT concat_ws(';',string_agg(status,',' ORDER BY id),sum(quantity ORDER BY quantity)::text) AS value FROM (SELECT id,status,quantity FROM usage_records ORDER BY id) AS sorted",
        "SELECT string_agg(status,',') FROM (SELECT status FROM usage_records ORDER BY id DESC) AS sorted",
        "WITH grouped AS (SELECT organization_id,string_agg(status,',' ORDER BY id) AS value FROM usage_records GROUP BY organization_id) SELECT organization_id,length(value) FROM grouped ORDER BY organization_id NULLS FIRST",
        "SELECT string_agg(status,NULL ORDER BY id),string_agg('',',' ORDER BY id),array_agg(DISTINCT tags ORDER BY tags) FILTER(WHERE tags IS NOT NULL AND cardinality(tags)>0) FROM usage_records",
        "SELECT min(created_at::date ORDER BY id),max(created_at::date),percentile_disc(0.5) WITHIN GROUP(ORDER BY created_at::date) FROM usage_records",
        "SELECT created_at::date AS day,string_agg(status,',' ORDER BY id) FROM usage_records GROUP BY created_at::date ORDER BY day NULLS FIRST",
        "SELECT string_agg((1/(quantity-quantity))::text,',' ORDER BY id) FILTER(WHERE FALSE),array_agg(1/(quantity-quantity) ORDER BY id) FILTER(WHERE FALSE) FROM usage_records",
        "SELECT string_agg(status,',' ORDER BY id),array_agg(status ORDER BY id),sum(quantity),percentile_disc(0.5) WITHIN GROUP(ORDER BY quantity) FROM usage_records",
        "SELECT organization_id,string_agg(status,',' ORDER BY id) AS value FROM usage_records GROUP BY organization_id HAVING count(*)>1 ORDER BY value",
        "SELECT string_agg(status,',' ORDER BY id),array_agg(status ORDER BY id),count(*) FROM usage_records WHERE FALSE",
        "SELECT organization_id,array_agg(status ORDER BY id) FROM usage_records WHERE FALSE GROUP BY organization_id",
        "SELECT array_agg(tags ORDER BY id) FROM usage_records",
        "SELECT array_agg(tags ORDER BY id) FROM usage_records WHERE id='g'",
        "SELECT array_agg(a ORDER BY n) FROM (VALUES (1,ARRAY[1,2]),(2,ARRAY[3])) AS v(n,a)",
        "SELECT string_agg(status) FROM usage_records",
        "SELECT string_agg(quantity,',') FROM usage_records",
        "SELECT string_agg(status,quantity) FROM usage_records",
        "SELECT array_agg(NULL ORDER BY id) FROM usage_records",
        "SELECT array_agg(ARRAY[[[[[[quantity]]]]]]) FROM usage_records",
        "SELECT sum(DISTINCT quantity ORDER BY quantity+0) FROM usage_records",
        "SELECT array_agg(DISTINCT status ORDER BY created_at) FROM usage_records",
        "SELECT string_agg(DISTINCT status,'|' ORDER BY created_at) FROM usage_records",
        "SELECT string_agg(status,',' ORDER BY id) WITHIN GROUP(ORDER BY id) FROM usage_records",
        "SELECT string_agg(status,',' ORDER BY id) OVER() FROM usage_records",
        "SELECT array_agg(status ORDER BY id) OVER() FROM usage_records",
        "SELECT sum(quantity ORDER BY id) OVER() FROM usage_records",
    ]
    return result


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    original = next(c for c in inventory["entries"] if c["id"] == "sql-1230")
    cases = []
    with postgres() as db:
        db.execute(CREATE)
        db.execute(SEED)
        before = output(db.execute(READ, binary=True))
        for sql in statements() + [original["sql"]]:
            parameters = ["tokens"] if sql == original["sql"] else []
            actual = None
            code = None
            try:
                with psycopg.RawCursor(db) as cursor:
                    cursor.execute(sql, parameters, binary=True)
                    actual = output(cursor)
            except psycopg.Error as exc:
                code = exc.sqlstate
            cases.append(
                {
                    "sql": sql,
                    "parameters": parameters,
                    "output": actual,
                    "sqlstate": code,
                    "original": sql == original["sql"],
                }
            )
        assert output(db.execute(READ, binary=True)) == before
        assert cases[-1]["sqlstate"] == "42P10"
        view_create = "CREATE VIEW collection_view AS SELECT organization_id,string_agg(status,',' ORDER BY id) AS joined,array_agg(amount ORDER BY id) AS amounts FROM usage_records GROUP BY organization_id"
        view_read = "SELECT * FROM collection_view ORDER BY organization_id NULLS FIRST"
        db.execute(view_create)
        view_before = output(db.execute(view_read, binary=True))
        samples = [
            "SELECT string_agg(status,',' ORDER BY id),array_agg(amount ORDER BY id),count(*) FROM usage_records",
            "SELECT organization_id,array_agg(DISTINCT status ORDER BY status) AS values,sum(quantity ORDER BY id) AS total FROM usage_records GROUP BY organization_id ORDER BY organization_id NULLS FIRST",
        ]
        db.execute("CREATE ROLE collection_reader NOLOGIN")
        db.execute("GRANT SELECT ON usage_records TO collection_reader")
        db.execute("ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY")
        db.execute(
            "CREATE POLICY collection_visibility ON usage_records FOR SELECT TO collection_reader USING(status='a')"
        )
        db.execute("SET ROLE collection_reader")
        filtered = [
            {"sql": sql, "output": output(db.execute(sql, binary=True))}
            for sql in samples
        ]
        db.execute("RESET ROLE")
        mutation = "UPDATE usage_records SET status='cold',quantity=4,tags=ARRAY[12,13]::smallint[] WHERE id='a'"
        db.execute(mutation)
        cold = [
            {"sql": sql, "output": output(db.execute(sql, binary=True))}
            for sql in samples
        ]
        after = output(db.execute(READ, binary=True))
        view_after = output(db.execute(view_read, binary=True))
    return {
        "format": 1,
        "reference": "PostgreSQL 18 ordinary aggregate ordering and DISTINCT",
        "create": CREATE,
        "seed": SEED,
        "read": READ,
        "before": before,
        "entries": cases,
        "view_create": view_create,
        "view_read": view_read,
        "view_before": view_before,
        "view_after": view_after,
        "filtered": filtered,
        "mutation": mutation,
        "cold": cold,
        "after": after,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("ordered collection PostgreSQL witness drift")
        print(
            f"verified {len(result['entries'])} ordinary aggregate contracts and unchanged table postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
