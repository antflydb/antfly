#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify NUMERIC assignment/default/generated ordering against PostgreSQL."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = Path(__file__).resolve().parents[1] / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_numeric_assignment_reference.json"


def cases():
    return [
        {"use_default": True},
        *({"input": value} for value in (
            None, "1.244", "12.345", "-12.345", "0.001", "99.995",
            "99.994", "NaN", "Infinity", "-Infinity",
        )),
    ]


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL 18 NUMERIC assignments", "entries": []}
    with postgres() as db:
        # PostgreSQL does not allow generated-on-generated references. The
        # explicit nested cast is the independent observer of our native
        # dependency topology: base assignment, narrow target, dependent sum.
        db.execute("""
            CREATE TEMP TABLE numeric_assignment (
                base numeric(4,2) DEFAULT 1.245,
                narrow numeric(3,1) GENERATED ALWAYS AS (base) STORED,
                total numeric(5,2) GENERATED ALWAYS AS (base + base::numeric(3,1)) STORED
            )
        """)
        for case in cases():
            entry = dict(case)
            try:
                statement = "INSERT INTO numeric_assignment DEFAULT VALUES" if case.get("use_default") else "INSERT INTO numeric_assignment(base) VALUES (%s::numeric)"
                params = None if case.get("use_default") else (case["input"],)
                cursor = db.execute(statement + " RETURNING base::text, narrow::text, total::text", params)
                entry["expected"] = list(cursor.fetchone())
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["entries"].append(entry)
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC assignment oracle drift")
        print(f"Verified {len(output['entries'])} PostgreSQL NUMERIC assignments")


if __name__ == "__main__":
    main()
