#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify NUMERIC type-modifier values and wire descriptors with PostgreSQL."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = Path(__file__).resolve().parents[1] / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_numeric_typmod_reference.json"


def cases():
    return (
        "CAST(12.345 AS numeric(4,2))",
        "CAST(-12.345 AS numeric(4,2))",
        "CAST(99.995 AS numeric(4,2))",
        "CAST(.00994 AS numeric(2,4))",
        "CAST(.00995 AS numeric(2,4))",
        "CAST(0 AS numeric(2,4))",
        "CAST(99499 AS numeric(2,-3))",
        "CAST(99500 AS numeric(2,-3))",
        "CAST(-99500 AS numeric(2,-3))",
        "CAST('NaN' AS numeric(1,0))",
        "CAST('Infinity' AS numeric(10,2))",
        "CAST('-Infinity' AS numeric(10,2))",
        "CAST(NULL AS numeric(4,2))",
        "CAST('bad' AS numeric(4,2))",
        "CAST(12.5 AS decimal(4))",
        "CAST(12.345 AS numeric(+4,+2))",
        "CAST(1234 AS numeric(4,-2))",
        "CAST(CAST(1.245 AS numeric(4,2)) AS numeric(3,1))",
        "CAST(1.245 AS numeric(3,1))",
        "CAST(CAST(1.245 AS numeric(4,2)) AS numeric)",
        "+(1.245::numeric(4,2))",
        "-(1.245::numeric(4,2))",
        "1.245::numeric(4,2)+2.345::numeric(4,2)",
        "CASE WHEN true THEN 1.245::numeric(4,2) ELSE 2.345::numeric(4,2) END",
        "CASE WHEN true THEN 1.245::numeric(4,2) ELSE 2.345::numeric(5,2) END",
        "COALESCE(NULL::numeric(4,2),1.245::numeric(4,2))",
        "COALESCE(NULL::numeric(4,2),1.245::numeric(5,2))",
        "CAST(CAST(1.2345 AS real) AS numeric(4,2))",
        "CAST(ARRAY[12.345,NULL,-12.345] AS numeric(4,2)[])",
        "CAST('[-2:0]={12.345,NULL,-12.345}' AS numeric(4,2)[])",
        "CAST(ARRAY[12.345,99.995] AS numeric(4,2)[])",
        "CAST(NULL AS numeric(4,2)[])",
        "CAST('{}' AS numeric(4,2)[])",
        "CAST(1 AS numeric(0))",
        "CAST(NULL AS numeric(1001,0))",
        "CAST(1 AS numeric(-1,0))",
        "CAST(1 AS numeric(1,-1001))",
        "CAST(1 AS numeric(1,1001))",
        "CAST(1 AS numeric(4,2,1))",
        "CAST(1 AS numeric(4.0,2))",
        "CAST(1 AS numeric(4,2.0))",
        "CAST(1 AS numeric())",
        "CAST(1 AS numeric(1e2,2))",
        "CAST(1 AS numeric(1_0,2))",
        "CASE WHEN false THEN 99.995::numeric(4,2) ELSE 0::numeric(4,2) END",
        "CASE WHEN true THEN ARRAY[1.245]::numeric(4,2)[] ELSE ARRAY[99.995]::numeric(4,2)[] END",
    )


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL 18 NUMERIC type modifiers", "entries": []}
    with postgres() as db:
        for sql in cases():
            entry = {"sql": sql}
            try:
                cursor = db.execute(f"SELECT ({sql})")
                entry["oid"] = cursor.pgresult.ftype(0)
                entry["typmod"] = cursor.pgresult.fmod(0)
                entry["expected"] = db.execute(f"SELECT ({sql})::text").fetchone()[0]
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["entries"].append(entry)
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC type-modifier oracle drift")
        print(f"Verified {len(output['entries'])} PostgreSQL NUMERIC type modifiers")


if __name__ == "__main__":
    main()
