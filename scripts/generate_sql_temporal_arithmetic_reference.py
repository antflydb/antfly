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

"""Independent PostgreSQL UTC calendar/elapsed timestamp arithmetic witnesses."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    import psycopg

    dates = [
        "2024-01-31 23:59:59.123456+00",
        "2023-01-31 00:00:00+00",
        "2000-02-29 12:00:00+00",
        "1969-12-31 23:59:59.999999+00",
        "0001-01-31 00:00:00+00 BC",
        "infinity",
        "-infinity",
    ]
    intervals = [
        "1 month",
        "1 month 1 day",
        "-1 month 2 days -01:02:03.000004",
        "1 hour 30 minutes",
        "7 days",
        "-2147483648 months",
        "9223372036854775807 microseconds",
        "-9223372036854775808 microseconds",
        "-2147483648 days",
        "infinity",
        "-infinity",
    ]
    vectors = []
    with postgres() as db:
        for kind in ["timestamptz"]:
            for date in dates:
                for interval in intervals:
                    for op in ["+", "-"]:
                        expression = f"v {op} INTERVAL '{interval}'"
                        source = f"'{date}'::{kind}"
                        binary = (
                            db.execute(f"SELECT timestamptz_send({source})")
                            .fetchone()[0]
                            .hex()
                        )
                        try:
                            row = db.execute(
                                f"SELECT ({expression.replace('v', source, 1)})::text"
                            ).fetchone()[0]
                            vectors.append(
                                {
                                    "expression": expression,
                                    "timestamp": binary,
                                    "result": row,
                                    "sqlstate": None,
                                }
                            )
                        except psycopg.Error as error:
                            vectors.append(
                                {
                                    "expression": expression,
                                    "timestamp": binary,
                                    "result": None,
                                    "sqlstate": error.sqlstate,
                                }
                            )
        entries = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        db.execute(
            "CREATE TABLE usage_records(id text PRIMARY KEY,created_at timestamptz)"
        )
        db.execute(
            "INSERT INTO usage_records VALUES ('u1','2024-01-31 23:59:59.123456Z'),('u2','2023-01-31Z')"
        )
        cases = []
        for identity in ["sql-0593", "sql-0594"]:
            original = next(e for e in entries if e["id"] == identity)
            # Preserve original parameter identity and type using PREPARE.
            db.execute("PREPARE calendar_query(text) AS " + original["sql"])
            cursor = db.execute("EXECUTE calendar_query('u1')")
            values = cursor.fetchall()
            cases.append(
                {
                    "id": identity,
                    "sql": original["sql"],
                    "columns": [c.name for c in cursor.description],
                    "column_oids": [c.type_code for c in cursor.description],
                    "rows": [[v.isoformat() for v in r] for r in values],
                }
            )
            db.execute("DEALLOCATE calendar_query")
    return {"format": 1, "zone": "UTC", "vectors": vectors, "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("temporal arithmetic witness drift")
        print(
            f"verified {len(result['vectors'])} arithmetic vectors and {len(result['cases'])} original cases"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
