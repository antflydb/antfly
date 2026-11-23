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

"""Verify exact PostgreSQL mutation rejections against real declared owners."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import (
    FIXTURES,
    mutation_profile,
    mutation_reference,
    postgres,
)

CONTRACTS = {
    **{f"sql-{n:04}": "42702" for n in (1396, 1397, 1401, 1404, 1405, 1441, 1466)},
    **{f"sql-{n:04}": "23505" for n in (1433, 1434, 1446, 1448, 1449, 1450, 1451)},
    "sql-1485": "21000",
    "sql-1486": "42P10",
    "sql-1435": "42883",
    "sql-1472": "42883",
}


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = [entry for entry in inventory["entries"] if entry["id"] in CONTRACTS]
    if len(cases) != len(CONTRACTS):
        raise ValueError("original mutation rejection inventory changed")
    with postgres() as db:
        result = mutation_reference(
            db, cases, mutation_profile("unique-email"), negative_sqlstates=CONTRACTS
        )
    if result["excluded"] or len(result["entries"]) != len(CONTRACTS):
        raise ValueError(f"mutation rejection contracts drifted: {result['excluded']}")
    pristine = result["entries"][0]["final_tables"]
    for entry in result["entries"]:
        if entry.pop("final_tables") != pristine:
            raise ValueError(
                "rejected mutation changed the independent PostgreSQL state"
            )
    result["rejection_final_tables"] = pristine
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("mutation rejection reference differs from PostgreSQL")
        print(f"verified {len(result['entries'])} exact mutation rejection contracts")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
