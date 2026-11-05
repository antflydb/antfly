#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent PostgreSQL witnesses for obsolete private document-view mappings.

Keep the original SELECTs and declared document domains. Public SQL views use
explicit path extraction/casts; storage-index lifecycle is not a view dependency.
This witness does not by itself qualify stale native source publication guards.
"""
import argparse
import json
import subprocess
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import postgres, cursor_result

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "79644dfa1605e8da0f486d021d1c1393577d6265"
IDS = ["sql-0853", "sql-0854", "sql-0855", "sql-0856", "sql-0859"]
PLAN_VIEW = "CREATE VIEW support_view AS SELECT d._id,r.metadata->>'plan' AS plan FROM docs d CROSS JOIN LATERAL jsonb_to_record(d._doc) AS r(metadata JSONB)"
NUMBER_VIEW = "CREATE VIEW support_view AS SELECT d._id,r.title::numeric AS title_as_number FROM docs d CROSS JOIN LATERAL jsonb_to_record(d._doc) AS r(title TEXT)"
SEEDS = [
    {"key": "doc:a", "value": {"title": "alpha", "metadata": {"plan": "pro"}}},
    {"key": "doc:b", "value": {"title": "17", "metadata": {"plan": "free"}}},
]


def reference():
    corpus = json.loads(
        subprocess.check_output(
            [
                "git", "show",
                f"{SOURCE}:zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json",
            ],
            cwd=ROOT,
        )
    )["entries"]
    by_id = {case_id: corpus[int(case_id[4:]) - 1] for case_id in IDS}
    entries = []
    with postgres() as db:
        db.execute("CREATE TABLE docs(_id TEXT PRIMARY KEY,_doc JSONB NOT NULL)")
        for seed in SEEDS:
            db.execute(
                "INSERT INTO docs VALUES(%s,%s::jsonb)",
                (seed["key"], json.dumps(seed["value"])),
            )
        for case_id in IDS:
            original = by_id[case_id]
            db.execute(NUMBER_VIEW if case_id == IDS[0] else PLAN_VIEW)
            entry = {
                "id": case_id,
                "sql": original["sql"],
                "native_request_json": original["native_request_json"],
                "catalog_tables": original["catalog_tables"],
            }
            try:
                with db.transaction(), db.cursor() as cursor:
                    entry["result"] = cursor_result(
                        cursor, {"id": case_id, "sql": original["sql"], "params": []}, True
                    )
            except psycopg.Error as error:
                entry["sqlstate"] = error.sqlstate
            db.execute("DROP VIEW support_view")
            entries.append(entry)
        db.execute(NUMBER_VIEW)
        db.execute(
            "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"23\"') WHERE _id='doc:a'"
        )
        with db.cursor() as cursor:
            positive = cursor_result(
                cursor, {"id": IDS[0], "sql": by_id[IDS[0]]["sql"], "params": []}, True
            )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL and source-declared document domains",
        "source_commit": SOURCE,
        "seeds": SEEDS,
        "number_view": NUMBER_VIEW,
        "plan_view": PLAN_VIEW,
        "entries": entries,
        "valid_numeric_result": positive,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document view guard PostgreSQL drift")
        print("verified five original SELECTs, source contracts, casts and projection guards")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
