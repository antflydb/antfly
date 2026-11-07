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

"""PostgreSQL range text/binary, canonicalization and operator witnesses."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    import psycopg

    inputs = {
        "numrange": [
            "empty",
            "(,)",
            "[1,10)",
            "[1.00,10.000]",
            "(1,1)",
            "[1,1]",
            "(10,1)",
            "[NaN,NaN]",
            "[Infinity,)",
            "(,Infinity)",
            "[-Infinity,Infinity]",
            "[1e-20,1e20]",
            '["1.5","3.00")',
        ],
        "daterange": [
            "empty",
            "(,)",
            "[2025-01-01,2025-02-01)",
            "[2025-01-01,2025-02-01]",
            "(2025-01-01,2025-02-01]",
            "(2025-01-01,2025-01-02)",
            "[2024-02-28,2024-02-29]",
            "[infinity,)",
            "(infinity,)",
            "(-infinity,2025-01-01]",
            "[infinity,infinity]",
            "[5874897-12-31,5874897-12-31]",
        ],
        "tsrange": [
            "empty",
            "(,)",
            "[2025-01-01 00:00:00,2025-01-02 00:00:00)",
            "[2025-01-01 00:00:00,2025-01-01 00:00:00]",
            "[infinity,)",
            "(-infinity,infinity)",
            "[2000-01-01 00:00:00.000001,2000-01-01 00:00:00.000002)",
        ],
        "tstzrange": [
            "empty",
            "(,)",
            "[2025-01-01T01:30:00+01:30,2025-01-02T00:00:00Z)",
            "[2025-01-01 00:00:00+00,2025-01-02 00:00:00+00)",
            "[infinity,)",
            "(-infinity,infinity)",
            "[2000-01-01 00:00:00.000001+00,2000-01-01 00:00:00.000002+00)",
        ],
    }
    entries, relations = [], []
    with postgres() as db:
        for kind, literals in inputs.items():
            good = []
            for literal in literals:
                try:
                    row = db.execute(
                        f"SELECT r::text,range_send(r),lower(r)::text,upper(r)::text,isempty(r),lower_inc(r),upper_inc(r),lower_inf(r),upper_inf(r) FROM (SELECT %s::{kind} r) q",
                        (literal,),
                    ).fetchone()
                except psycopg.Error as error:
                    entries.append(
                        {"kind": kind, "input": literal, "sqlstate": error.sqlstate}
                    )
                    continue
                good.append(literal)
                entries.append(
                    {
                        "kind": kind,
                        "input": literal,
                        "sqlstate": None,
                        "text": row[0],
                        "binary": row[1].hex(),
                        "lower": row[2],
                        "upper": row[3],
                        "empty": row[4],
                        "lower_inc": row[5],
                        "upper_inc": row[6],
                        "lower_inf": row[7],
                        "upper_inf": row[8],
                    }
                )
            for left in good:
                for right in good:
                    row = db.execute(
                        f"SELECT a=b,a<b,a&&b,a@>b,a<@b,a-|-b FROM (SELECT %s::{kind} a,%s::{kind} b) q",
                        (left, right),
                    ).fetchone()
                    relations.append(
                        {
                            "kind": kind,
                            "left": left,
                            "right": right,
                            "equal": row[0],
                            "less": row[1],
                            "overlaps": row[2],
                            "contains": row[3],
                            "contained": row[4],
                            "adjacent": row[5],
                        }
                    )

        def output(cursor):
            result = cursor.pgresult
            return {
                "columns": [column.name for column in cursor.description],
                "column_oids": [column.type_code for column in cursor.description],
                "rows": [
                    [
                        None
                        if result.get_value(row, column) is None
                        else int(result.get_value(row, column))
                        if cursor.description[column].type_code in (20, 21, 23)
                        else result.get_value(row, column) == b"t"
                        if cursor.description[column].type_code == 16
                        else result.get_value(row, column).decode("utf-8")
                        for column in range(result.nfields)
                    ]
                    for row in range(result.ntuples)
                ],
            }

        tables, probes = [], []
        for kind in inputs:
            name = "range_" + kind
            create = f"CREATE TABLE {name}(id text PRIMARY KEY,value {kind} NOT NULL)"
            db.execute(create)
            values = [
                entry
                for entry in entries
                if entry["kind"] == kind and entry["sqlstate"] is None
            ]
            literals = [
                "('r{}','{}'::{})".format(
                    index, entry["input"].replace("'", "''"), kind
                )
                for index, entry in enumerate(values)
            ]
            seed = f"INSERT INTO {name}(id,value) VALUES " + ",".join(literals)
            db.execute(seed)
            read = f"SELECT id,value FROM {name} ORDER BY id"
            tables.append(
                {
                    "name": name,
                    "create": create,
                    "seed": seed,
                    "read": read,
                    "final": output(db.execute(read)),
                }
            )
            query = f"SELECT id,value,lower(value) AS lower_bound,upper(value) AS upper_bound,isempty(value) AS empty,lower_inc(value) AS lower_inc,upper_inc(value) AS upper_inc,lower_inf(value) AS lower_inf,upper_inf(value) AS upper_inf FROM {name} ORDER BY id"
            probes.append({"sql": query, "output": output(db.execute(query))})
            for query in [
                f"SELECT DISTINCT value FROM {name} ORDER BY value",
                f"SELECT value,COUNT(*) AS n FROM {name} GROUP BY value ORDER BY value",
                f"SELECT id FROM {name} WHERE value && '{values[2]['input'].replace(chr(39), chr(39) * 2)}'::{kind} ORDER BY id",
            ]:
                probes.append({"sql": query, "output": output(db.execute(query))})
        # sql-0683 is SQL-standard PERIOD syntax, which PostgreSQL does not
        # accept. Execute the entire original multi-action ALTER, then prove
        # the early ADD COLUMN actions did not leak through its syntax error.
        original = next(
            entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if entry["id"] == "sql-0683"
        )
        create = "CREATE TABLE usage_records(id text PRIMARY KEY)"
        seed = "INSERT INTO usage_records(id) VALUES ('unchanged')"
        db.execute(create)
        db.execute(seed)
        read = "SELECT id FROM usage_records ORDER BY id"
        before = output(db.execute(read))
        try:
            db.execute(original["sql"])
        except psycopg.Error as error:
            if error.sqlstate != "42601":
                raise
        else:
            raise ValueError("invalid original PERIOD grammar succeeded")
        if output(db.execute(read)) != before:
            raise ValueError("rejected ALTER changed source rows")
        columns = db.execute(
            "SELECT attname FROM pg_attribute WHERE attrelid='usage_records'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum"
        ).fetchall()
        if columns != [("id",)]:
            raise ValueError("rejected ALTER changed source schema")
        tables.append(
            {
                "name": "usage_records",
                "create": create,
                "seed": seed,
                "read": read,
                "final": before,
            }
        )
        rejected = [{"id": original["id"], "sql": original["sql"], "sqlstate": "42601"}]
    return {
        "format": 1,
        "zone": "UTC",
        "entries": entries,
        "relations": relations,
        "tables": tables,
        "probes": probes,
        "rejected": rejected,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path)
    p.add_argument("--check", type=Path)
    args = p.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("range PostgreSQL witness drift")
        print(
            f"verified {len(result['entries'])} range codecs and {len(result['relations'])} operator witnesses"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
