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

"""Exact remaining original read contracts, with nonempty native witnesses."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from uuid import UUID

from generate_sql_extended_read_reference import profile as extended_profile
from generate_sql_postgres_reference import (
    FIXTURES,
    create_table,
    cursor_result,
    execution_parameters,
    postgres,
    properties,
    read_reference,
)

IDS = {"sql-0292", "sql-0641", "sql-0642", "sql-1231", "sql-1237", "sql-1253"}
CLOCK_IDS = {"sql-0174", "sql-0175", "sql-0176"}


def profile():
    result = extended_profile()
    # Every DISTINCT ON tie-breaker is unique; enough groups exercise LIMIT
    # and OFFSET, including nondefault bounds and null/empty array sources.
    result["rows"][0]["value"]["tags"] = {
        "dimensions": [{"length": 1, "lower_bound": 1}],
        "values": ["hot"],
        "sql_nulls": [False],
    }
    for index, row in enumerate(result["rows"]):
        metadata = deepcopy(row["value"]["metadata"])
        metadata["flags"] = [f"group-{index // 3}"]
        metadata["source"] = f"source-{index % (index // 3 + 1)}"
        row["value"]["metadata"] = metadata
    return result


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    cases = [case for case in inventory if case["id"] in IDS]
    if len(cases) != len(IDS):
        raise ValueError("remaining read inventory drift")
    # sql-1231 has peers in its count ordering and unordered array_agg input.
    # Distinct flags are identical within each group so element order cannot
    # accidentally become a contract; compare every eligible order peer.
    observers = {
        "sql-1231": "SELECT w.*,w.source_count AS order_key FROM (SELECT organization_id, COUNT(DISTINCT metadata->>'source') AS source_count, array_agg(DISTINCT metadata->'flags') AS distinct_flags FROM usage_records WHERE metric_type = $1 GROUP BY organization_id) w ORDER BY order_key DESC"
    }
    with postgres() as db:
        result = read_reference(
            db,
            [case for case in cases if case["id"] != "sql-1237"],
            profile(),
            order_observers=observers,
        )
        import psycopg

        domain = profile()
        domain["rows"][0]["value"]["id"] = "u1"
        create_table(db, "usage_records", properties(domain["schema"]), domain["rows"])
        # HAVING false is an intentionally empty original, not permission for
        # other read cases to pass with nonexercising input. Observe nonempty
        # groups independently before executing the unchanged original.
        groups = db.execute(
            "SELECT organization_id,count(*) FROM usage_records GROUP BY organization_id"
        ).fetchall()
        if len(groups) != 6 or sum(row[1] for row in groups) != 18:
            raise ValueError("HAVING false lost its nonempty grouped witness")
        with psycopg.RawCursor(db) as cursor:
            empty = next(case for case in cases if case["id"] == "sql-1237")
            entry = cursor_result(
                cursor, empty, read=True, allow_empty=True, exact_numeric=True
            )
            if entry["rows"]:
                raise ValueError("HAVING false produced rows")
            entry["empty_contract"] = {
                "input_rows": 18,
                "input_groups": 6,
                "predicate": "false",
            }
            result["entries"].append(entry)
            result["entries"].sort(key=lambda item: item["id"])
        clock_contracts = []
        for case in inventory:
            if case["id"] not in CLOCK_IDS:
                continue
            with db.transaction(force_rollback=True):
                transaction_start = db.execute(
                    "SELECT transaction_timestamp()"
                ).fetchone()[0]
                with psycopg.RawCursor(db) as cursor:
                    cursor.execute(case["sql"], execution_parameters(db, case))
                    rows = cursor.fetchall()
                    if rows != [(transaction_start,)]:
                        raise ValueError("stable clock differs from transaction start")
                    clock_contracts.append(
                        {
                            "id": case["id"],
                            "columns": [column.name for column in cursor.description],
                            "column_oids": [
                                column.type_code for column in cursor.description
                            ],
                            "semantics": "transaction_start_microseconds",
                        }
                    )
        result["clock_contracts"] = clock_contracts
        original = next(case for case in inventory if case["id"] == "sql-0177")
        seen = set()
        with psycopg.RawCursor(db) as cursor:
            for _ in range(32):
                cursor.execute(original["sql"], execution_parameters(db, original))
                rows = cursor.fetchall()
                if len(rows) != 1 or len(rows[0]) != 1:
                    raise ValueError("UUID lost its matching native row")
                value = UUID(str(rows[0][0]))
                if (
                    value.version != 4
                    or value.variant != "specified in RFC 4122"
                    or value in seen
                ):
                    raise ValueError("UUID output is not fresh RFC 4122 version 4")
                seen.add(value)
            result["uuid_contract"] = {
                "id": original["id"],
                "columns": [column.name for column in cursor.description],
                "column_oids": [column.type_code for column in cursor.description],
                "version": 4,
                "variant": "RFC4122",
            }
    if result["excluded"]:
        raise ValueError(result["excluded"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("remaining read PostgreSQL drift")
        print(f"verified {len(result['entries'])} exact original read contracts")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
