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

"""Original document MOD signatures use float8 columns, not SQL NUMERIC."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres, document_reference

IDS = [f"sql-{n:04}" for n in range(970, 974)]


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    schemas = {
        case["id"]: case["schema"]
        for case in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    cases = [case for case in inventory if case["id"] in IDS]
    if len(cases) != 4:
        raise ValueError("document numeric inventory drift")
    with postgres() as db:
        result = document_reference(db, cases, schemas, annotation_only=True)
    if result["excluded"]:
        raise ValueError(result["excluded"])
    for entry in result["entries"]:
        if entry["sqlstate"] != "42883" or entry["final"] != result["seeds"]:
            raise ValueError("invalid floating MOD signature changed its contract")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document numeric PostgreSQL drift")
        print(
            "verified four original floating MOD rejections and complete unchanged postimages"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
