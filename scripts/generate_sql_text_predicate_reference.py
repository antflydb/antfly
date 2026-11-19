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

"""Independent PostgreSQL witness for Antfly's implicit-document text predicate.

PostgreSQL has no implicit-row full_text_search extension. Preserve each original
SQL string and record its explicit field-scoped PostgreSQL lowering separately.
The native server executes the original SQL, never the reference lowering.
"""

import argparse
import copy
import json
import re
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import cursor_result, postgres
from generate_sql_view_reference import (
    FIXTURES,
    VIEW,
)
from generate_sql_view_reference import (
    SCHEMA as BASE_SCHEMA,
)
from generate_sql_view_reference import (
    SEEDS as BASE_SEEDS,
)

SCHEMA = copy.deepcopy(BASE_SCHEMA)
SCHEMA["document_schemas"]["doc"]["schema"]["properties"]["body"] = {"type": "text"}
SEEDS = copy.deepcopy(BASE_SEEDS)
for seed in SEEDS:
    seed["value"]["body"] = (
        "alpha public" if seed["key"] in {"doc:a", "doc:d", "doc:f"} else "beta other"
    )

IDS = [801, 814, 858, 860, 861, 862, 863, 864, 865, 867, 868, 869, 870, 871, 1579]
PATTERN = re.compile(r"full_text_search\('([a-z_.]+):([a-z]+)'\)")


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {case["id"]: case for case in inventory["entries"]}
    entries = []
    with postgres() as db:
        db.execute(
            "CREATE TABLE docs(_id TEXT PRIMARY KEY,_doc JSONB,status TEXT,body TEXT)"
        )
        for seed in SEEDS:
            db.execute(
                "INSERT INTO docs VALUES(%s,%s::jsonb,%s,%s)",
                (
                    seed["key"],
                    json.dumps(seed["value"]),
                    seed["value"]["status"],
                    seed["value"]["body"],
                ),
            )
        db.execute(VIEW)

        def observe():
            entries = []
            for number in IDS:
                case = cases[f"sql-{number:04d}"]
                witnesses = []

                def lower(match, witnesses=witnesses):
                    field, term = match.groups()
                    path = "{" + ",".join(field.split(".")) + "}"
                    predicate = (
                        "to_tsvector('simple',coalesce(candidate._doc #>> '"
                        + path
                        + "','')) @@ plainto_tsquery('simple','"
                        + term
                        + "')"
                    )
                    witness = (
                        "SELECT candidate._id FROM docs candidate WHERE " + predicate
                    )
                    witnesses.append(
                        {
                            "field": field,
                            "term": term,
                            "candidate_ids": [
                                r[0]
                                for r in db.execute(
                                    witness + " ORDER BY candidate._id"
                                ).fetchall()
                            ],
                        }
                    )
                    return "_id IN (" + witness + ")"

                reference_sql = PATTERN.sub(lower, case["sql"])
                if not witnesses or "full_text_search" in reference_sql:
                    raise ValueError(case["id"])
                with psycopg.RawCursor(db) as cursor:
                    entry = cursor_result(
                        cursor,
                        dict(case, sql=reference_sql),
                        read=True,
                        allow_empty=True,
                        exact_numeric=True,
                    )
                entry.update(
                    sql=case["sql"],
                    reference_sql=reference_sql,
                    text_witnesses=witnesses,
                )
                entries.append(entry)
            return entries

        entries = observe()
        db.execute("DELETE FROM docs WHERE _doc->>'title' IS DISTINCT FROM 'alpha'")
        filtered_entries = observe()
    return {
        "format": 1,
        "reference": "PostgreSQL field-scoped text witness and SQL residuals",
        "profile": {
            "description": "Exact unranked text candidates with independently evaluated PostgreSQL residuals",
            "schema": SCHEMA,
            "rows": SEEDS,
            "view_sql": VIEW,
        },
        "entries": entries,
        "filtered_entries": filtered_entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("text predicate witness differs from PostgreSQL")
        print(f"verified {len(result['entries'])} field-scoped text witnesses")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
