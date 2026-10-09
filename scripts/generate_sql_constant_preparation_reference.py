#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify speculative constant preparation preserves PostgreSQL lazy demand."""

import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = Path(__file__).resolve().parents[1] / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_constant_preparation_reference.json"


def main():
    import psycopg

    fixture = json.loads(FIXTURE.read_text())
    with postgres() as db:
        for entry in fixture["entries"]:
            try:
                value = db.execute(f"SELECT ({entry['sql']})::text").fetchone()[0]
            except psycopg.Error as error:
                if entry.get("error") != error.sqlstate:
                    raise ValueError(f"SQLSTATE mismatch: {entry['sql']}") from error
            else:
                if "error" in entry or value != entry["expected"]:
                    raise ValueError(f"Value mismatch: {entry['sql']}")
    print(f"Verified {len(fixture['entries'])} PostgreSQL constant-preparation cases")


if __name__ == "__main__":
    main()
