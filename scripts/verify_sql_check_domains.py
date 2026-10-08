#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify wide logical CHECK domains against disposable PostgreSQL storage."""

import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_check_domain_reference.json"
)


def main():
    import psycopg

    fixture = json.loads(FIXTURE.read_text())
    blob = bytes(fixture["blob_bytes"])
    text = "x" * fixture["text_bytes"]
    with postgres() as db:
        db.execute(
            f"CREATE TEMP TABLE wide_checks (b bytea CHECK (b = decode(repeat('00', {len(blob)}), 'hex')), s text CHECK (s COLLATE \"C\" > ''))"
        )
        for entry in fixture["entries"]:
            b = {"same": blob, "other": b"\x01" + blob[1:], "null": None}[entry["blob"]]
            s = {"wide": text, "empty": "", "null": None}[entry["text"]]
            try:
                db.execute("INSERT INTO wide_checks VALUES (%s, %s)", (b, s))
                accepted = True
            except psycopg.Error as error:
                if error.sqlstate != "23514":
                    raise
                accepted = False
            if accepted != entry["accepted"]:
                raise ValueError(f"PostgreSQL wide CHECK oracle drift: {entry}")
    print(f"Verified {len(fixture['entries'])} PostgreSQL wide CHECK assignments")


if __name__ == "__main__":
    main()
