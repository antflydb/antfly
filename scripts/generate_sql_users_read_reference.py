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

"""Original alias-qualified users read with a nonempty ordered LIMIT frontier."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import (
    FIXTURES,
    postgres,
    create_table,
    properties,
    execute,
)


def reference():
    original = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ][170]
    if original["id"] != "sql-0171":
        raise ValueError("users read inventory drift")
    profile = {
        "schema": {
            "version": 1,
            "storage_mode": "relational",
            "default_type": "row",
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            name: {"type": "keyword"}
                            for name in ["id", "email", "tenant_id", "status"]
                        },
                        "additionalProperties": False,
                    }
                }
            },
        },
        "rows": [
            {
                "key": str(n),
                "value": {
                    "id": f"user-{n}",
                    "email": email,
                    "tenant_id": f"tenant-{n}",
                    "status": "active",
                },
            }
            for n, email in enumerate(
                [
                    "ADA@EXAMPLE.TEST",
                    "ada@example.test",
                    "Ada@Example.Test",
                    "other@example.test",
                ]
            )
        ],
    }
    with postgres() as db:
        create_table(db, "users", properties(profile["schema"]), profile["rows"])
        entry = execute(db, original, read=True)
        if entry["rows"] != [["user-2"], ["user-1"]]:
            raise ValueError("users ordered frontier witness drift")
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": profile,
        "entries": [entry],
        "excluded": [],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("users read PostgreSQL drift")
        print(
            "verified exact alias-qualified users read with matching, excluded and LIMIT witnesses"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
