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

"""Independent missing-column and aggregate read witnesses for exact originals."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import (
    FIXTURES,
    postgres,
    read_reference,
    typed_array_read_profile,
    properties,
)

IDS = [203, 204, 1224, 1229, 1266, 1302, 1378, 1379, 1380, 1381]


def profile():
    result = typed_array_read_profile()
    columns = properties(result["schema"])
    for name in ("metric_type", "scope", "expires_at"):
        columns[name] = {"type": "keyword", "nullable": True}
    # More than two distinct groups exercise the original OFFSET 2, not an
    # empty-result waiver. One bounded domain owns all declared SQL arrays.
    template = result["rows"]
    result["rows"] = []
    for index in range(18):
        value = dict(template[index % len(template)]["value"])
        value.update(
            {
                "id": f"w{index:02}",
                "organization_id": f"org_{index // 3}",
                "tenant_id": "w02",
                "customer_id": "w01",
                "created_at": f"2026-02-{index + 1:02}T00:00:00Z",
                "metric_type": "tokens",
                "scope": "read write" if index % 2 else "write",
                "amount": index + 1,
                "quantity": index + 1,
                "status": ("open", "active", "ready", "op_ready")[index % 4],
                "expires_at": None
                if index % 4 == 0
                else f"2026-01-{index + 1:02}T00:00:00Z",
            }
        )
        result["rows"].append({"key": f"r{index:02}", "value": value})
    result["exact_numeric"] = True
    return result


def reference():
    originals = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    ids = {f"sql-{number:04}" for number in IDS}
    cases = [entry for entry in originals if entry["id"] in ids]
    if len(cases) != len(ids):
        raise ValueError("read inventory drift")
    observers = {}
    with postgres() as db:
        # Window output peers are genuinely unordered. Observe the complete
        # eligible frontier independently rather than pinning a PostgreSQL
        # physical LIMIT selection or adding an ORDER BY to the original.
        for case in cases:
            if case["id"] not in {"sql-1378", "sql-1379", "sql-1380", "sql-1381"}:
                continue
            body, suffix = case["sql"].rsplit(" ORDER BY ", 1)
            alias, direction, limit, count = suffix.split()
            if (direction, limit, count) != ("DESC", "LIMIT", "5"):
                raise ValueError("window witness shape drift")
            observers[case["id"]] = (
                f"SELECT w.*,w.{alias} AS order_key FROM ({body}) w ORDER BY order_key DESC"
            )
        result = read_reference(db, cases, profile(), order_observers=observers)
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
            raise SystemExit("extended read PostgreSQL drift")
        print(f"verified {len(result['entries'])} exact read contracts")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
