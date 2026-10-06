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

"""Generate independent SQLite expectations for explicitly selected SQL cases.

This is a limited reference, not a PostgreSQL compatibility claim. Statements
are never rewritten. Unsupported SQLite syntax/types are reported, not treated
as SQL implementation failures or given parity credit. Output goes to stdout.
"""

import argparse
from contextlib import closing
import json
import re
import sqlite3
import sys
from pathlib import Path
from sql_reference_functions import register

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/local/sql/fixtures"
FAMILIES = {"read", "query", "aggregate", "window", "join"}
EMPTY_CONTRACTS = {"sql-0180"}  # Original WHERE false read, not an inferred waiver.


def parameter(value):
    """Decode original internal parameter tags, never change statement SQL."""
    if not isinstance(value, dict):
        return value
    if len(value) != 1:
        raise ValueError("reference parameter needs one typed variant")
    kind, cell = next(iter(value.items()))
    if kind == "integer":
        return int(cell)
    if kind == "number":
        return float(cell)
    if kind == "boolean":
        return bool(cell)
    if kind in {"string", "json"}:
        return cell
    if kind == "null":
        return None
    raise ValueError(f"unsupported reference parameter: {kind}")


def reference(cases, rows, properties=None):
    output, excluded = [], []
    with closing(sqlite3.connect(":memory:")) as db:
        register(db)
        db.execute("PRAGMA case_sensitive_like = ON")
        if db.execute("SELECT 'A' LIKE 'a'").fetchone() != (0,):
            raise ValueError("reference requires case-sensitive LIKE")
        if properties is None:
            db.execute(
                "CREATE TABLE usage_records (id INTEGER, name TEXT, amount INTEGER, quantity INTEGER, status TEXT, enabled BOOLEAN, customer_id INTEGER, tenant_id INTEGER, created_at TEXT, metadata TEXT)"
            )
            columns = [
                entry[1] for entry in db.execute("PRAGMA table_info(usage_records)")
            ]
            json_columns = {"metadata"}
        else:
            if (
                not properties
                or len(properties) > 128
                or any(not name.replace("_", "").isalnum() for name in properties)
            ):
                raise ValueError("bounded profile requires ordinary column identifiers")
            types = {
                "integer": "INTEGER",
                "boolean": "BOOLEAN",
                "numeric": "REAL",
                "number": "REAL",
            }
            db.execute("ATTACH DATABASE ':memory:' AS public")
            db.execute(
                "CREATE TABLE public.usage_records ("
                + ",".join(
                    '"' + name + '" ' + types.get(prop.get("type"), "TEXT")
                    for name, prop in properties.items()
                )
                + ")"
            )
            columns = list(properties)
            json_columns = {
                name
                for name, prop in properties.items()
                if prop.get("type") in {"object", "array", "json"}
            }
        for row in rows:
            values = [row["value"].get(column) for column in columns]
            for index, column in enumerate(columns):
                if column in json_columns and values[index] is not None:
                    values[index] = json.dumps(values[index], separators=(",", ":"))
            db.execute(
                "INSERT INTO usage_records VALUES ("
                + ",".join("?" for _ in columns)
                + ")",
                values,
            )
        db.commit()
        allowed = {
            sqlite3.SQLITE_SELECT,
            sqlite3.SQLITE_READ,
            sqlite3.SQLITE_FUNCTION,
            sqlite3.SQLITE_RECURSIVE,
        }
        db.set_authorizer(
            lambda action, *_: (
                sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY
            )
        )
        steps = 0

        def progress():
            nonlocal steps
            steps += 1000
            return steps > 100_000

        db.set_progress_handler(progress, 1000)
        for case in cases:
            steps = 0
            if (
                case["family"] not in FAMILIES
                or case["source_expectation"] == "rejection"
            ):
                raise ValueError(f"{case['id']}: not a positive read contract")
            if re.search(
                r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|random|randomblob)\b",
                case["sql"],
                re.IGNORECASE,
            ):
                excluded.append(
                    {
                        "id": case["id"],
                        "reason": "nondeterministic/native clock profile required",
                    }
                )
                continue
            try:
                cursor = db.execute(
                    case["sql"],
                    {
                        str(i): parameter(value)
                        for i, value in enumerate(case["params"], 1)
                    },
                )
                if cursor.description is None:
                    raise ValueError(f"{case['id']}: not a read")
                result = [list(row) for row in cursor.fetchmany(4097)]
                if len(result) > 4096:
                    excluded.append(
                        {
                            "id": case["id"],
                            "reason": "reference exceeds 4096 row budget",
                        }
                    )
                    continue
                if not result and case["id"] not in EMPTY_CONTRACTS:
                    excluded.append(
                        {
                            "id": case["id"],
                            "reason": "empty reference result does not exercise this shape",
                        }
                    )
                    continue
                output.append(
                    {
                        "id": case["id"],
                        "columns": [column[0] for column in cursor.description],
                        "rows": result,
                    }
                )
            except (sqlite3.Error, TypeError) as error:
                excluded.append({"id": case["id"], "reason": str(error)})
    return {
        "reference": "SQLite exact SQL, case-sensitive LIKE, integer fixture",
        "entries": output,
        "excluded": excluded,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        type=Path,
        help="explicit native schema/row profile; never rewrites statement SQL",
    )
    parser.add_argument(
        "--cases",
        type=Path,
        help="existing reference manifest whose IDs should be regenerated",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify selected golden results without writing",
    )
    args = parser.parse_args()
    if args.check and not args.cases:
        parser.error("--check requires an explicit --cases manifest")
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    if args.cases:
        manifest = json.loads(args.cases.read_text())
        requested = [entry["id"] for entry in manifest["entries"]]
        ids = set(requested)
        if len(ids) != len(requested) or not ids:
            parser.error("case manifest needs unique, nonempty case IDs")
        known = {
            entry["id"]
            for entry in inventory
            if entry["family"] in FAMILIES
            and entry["source_expectation"] != "rejection"
        }
        if ids - known:
            parser.error(f"not positive read cases: {sorted(ids - known)}")
    else:
        # Discovery only: passing an oracle does not resolve a disposition.
        dispositions = json.loads(
            (FIXTURES / "sql_parity_dispositions.json").read_text()
        )["entries"]
        ids = {entry["id"] for entry in dispositions if entry["status"] == "unresolved"}
    cases = [
        case
        for case in inventory
        if case["id"] in ids
        and case["family"] in FAMILIES
        and case["source_expectation"] != "rejection"
    ]
    profile = json.loads(args.profile.read_text()) if args.profile else None
    rows = (
        profile["rows"]
        if profile
        else json.loads((FIXTURES / "sql_read_reference_rows.json").read_text())["rows"]
    )
    properties = (
        profile["schema"]["document_schemas"]["row"]["schema"]["properties"]
        if profile
        else None
    )
    result = reference(cases, rows, properties)
    if profile:
        result["profile"] = profile
    if args.check:
        if (
            result["excluded"]
            or result["entries"] != manifest["entries"]
            or result.get("profile") != manifest.get("profile")
        ):
            parser.error("golden results differ from the independent reference")
        print(f"verified {len(result['entries'])} exact read references")
        return
    json.dump(result, sys.stdout, indent=2, allow_nan=False)
    print()


if __name__ == "__main__":
    main()
