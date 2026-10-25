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

"""Original PostgreSQL temporal-key cases with exact typed results and witnesses."""

import argparse
import json
import subprocess
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def output(cursor):
    result = cursor.pgresult
    return {
        "columns": [column.name for column in cursor.description or []],
        "column_oids": [column.type_code for column in cursor.description or []],
        "rows": [
            [
                None
                if result.get_value(row, col) is None
                else int(result.get_value(row, col))
                if cursor.description[col].type_code in (20, 21, 23)
                else result.get_value(row, col) == b"t"
                if cursor.description[col].type_code == 16
                else result.get_value(row, col).decode("utf-8")
                for col in range(result.nfields)
            ]
            for row in range(result.ntuples)
        ],
    }


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
    )["entries"]
    ids = [685, *range(1414, 1432)]
    originals = [(f"sql-{i:04}", source[i - 1]) for i in ids]
    creates = {}
    for _, entry in originals:
        for sql in entry.get("apply_setup_sql", []):
            creates.setdefault(sql.split()[2], sql)
    creates["products"] = originals[0][1]["sql"]
    tables, steps, rejected, operators = [], [], [], []
    with postgres() as db:
        # PostgreSQL's GiST equality classes live in this bundled extension.
        # Antfly uses its native typed tuple codec rather than that C module.
        db.execute("CREATE EXTENSION btree_gist")
        for name, sql in creates.items():
            db.execute(sql)
        for case_id, entry in originals[1:]:
            sqlstate = None
            try:
                cursor = db.execute(entry["sql"])
                actual = output(cursor)
            except psycopg.Error as error:
                if case_id != "sql-1429" or error.sqlstate != "42883":
                    raise
                sqlstate = error.sqlstate
                actual = {"columns": [], "column_oids": [], "rows": []}
            steps.append(
                {
                    "id": case_id,
                    "sql": entry["sql"],
                    "write": entry["sql"].startswith("INSERT"),
                    "output": actual,
                    "sqlstate": sqlstate,
                    "affected": max(0, cursor.rowcount)
                    if entry["sql"].startswith("INSERT")
                    else 0,
                }
            )
        for sql in [
            "INSERT INTO price_intervals(sku,valid_at,price) VALUES ('sku:a','[5,9)'::numrange,99)",
            "INSERT INTO price_intervals(sku,valid_at,price) VALUES ('sku:a','[9,11)'::numrange,99)",
            "INSERT INTO price_intervals(sku,valid_at,price) VALUES ('sku:a','empty'::numrange,99)",
            "INSERT INTO products(product_id,product_name,valid_at) VALUES (1,'overlap','[2025-02-01,2025-03-01)'::daterange)",
            "INSERT INTO published_prices(sku,visible_at) VALUES ('sku:a','[2025-01-01 00:00:00+00,2025-01-01 01:00:00+00)'::tstzrange)",
            "INSERT INTO local_prices(sku,local_at) VALUES ('sku:a','[2025-01-01 01:00:00,2025-01-01 02:00:00)'::tsrange)",
        ]:
            try:
                db.execute(sql)
            except psycopg.Error as error:
                if error.sqlstate not in ("23P01", "23514"):
                    raise
                rejected.append({"sql": sql, "sqlstate": error.sqlstate})
            else:
                raise ValueError("temporal constraint accepted a violating row")
        for expression in [
            "numrange(1,10) @> 7",
            "numrange(1,10) @> 7::numeric",
            "numrange(1,10) @> '7'",
            "numrange(1,10) @> '[2,3)'",
            "numrange(1,10) @> NULL",
            "daterange('2025-01-01','2025-02-01') @> DATE '2025-01-15'",
            "tsrange('2025-01-01','2025-02-01') @> TIMESTAMP '2025-01-15'",
            "tstzrange('2025-01-01','2025-02-01') @> TIMESTAMP '2025-01-15'",
            "tstzrange('2025-01-01','2025-02-01') @> TIMESTAMPTZ '2025-01-15 00:00:00+00'",
        ]:
            sql = f"SELECT ({expression})::text AS value"
            try:
                actual = output(db.execute(sql))
                code = None
            except psycopg.Error as error:
                actual = {"columns": [], "column_oids": [], "rows": []}
                code = error.sqlstate
            operators.append(
                {
                    "expression": f"({expression})::text",
                    "sql": sql,
                    "sqlstate": code,
                    "output": actual,
                }
            )
        # Touching half-open endpoints are adjacent, not overlapping. Remove
        # the witness before capturing immutable final postimages.
        adjacent = "INSERT INTO price_intervals(sku,valid_at,price) VALUES ('sku:a','[10,20)'::numrange,17)"
        removed = "DELETE FROM price_intervals WHERE sku='sku:a' AND valid_at='[10,20)'::numrange"
        db.execute(adjacent)
        db.execute(removed)
        for name, sql in creates.items():
            order = {
                "products": "product_id",
                "price_intervals": "sku",
                "local_prices": "sku",
                "published_prices": "sku",
            }[name]
            read = f"SELECT * FROM {name} ORDER BY {order}"
            tables.append(
                {
                    "name": name,
                    "create": sql,
                    "seed": "",
                    "read": read,
                    "final": output(db.execute(read)),
                }
            )
    return {
        "format": 1,
        "zone": "UTC",
        "ddl_id": "sql-0685",
        "tables": tables,
        "steps": steps,
        "rejected": rejected,
        "operators": operators,
        "adjacent": adjacent,
        "remove_adjacent": removed,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("temporal-key PostgreSQL witness drift")
        print(
            f"verified {len(result['steps']) + 1} original temporal-key cases and {len(result['rejected'])} rejection witnesses"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
