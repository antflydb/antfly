#!/usr/bin/env python3
"""Exact document SQL contracts under the current guarded SQL mutation model.

No schema annotation grants index readiness, uniqueness or write authority.
The source schemas are retained as provenance; generated/alias/metadata and
provider-specific profiles are excluded until they have native owner contracts.
SQLite only supplies bounded, independently reproducible scalar DML outcomes.
"""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from generate_sql_parity_read_reference import parameter
from sql_reference_functions import register

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/local/sql/fixtures"
SEEDS = [
    {
        "key": "doc:a",
        "value": {
            "title": "Launch",
            "status": "draft",
            "category": "release",
            "amount": 12,
            "enabled": True,
            "note": "keep-a",
            "metadata": {
                "source": "api",
                "billing": {"plan": "pro"},
                "flags": ["rated"],
            },
        },
    },
    {
        "key": "doc:b",
        "value": {
            "title": "Other",
            "status": "ready",
            "category": "other",
            "amount": 20,
            "enabled": False,
            "note": "keep-b",
            "metadata": {
                "source": "internal",
                "billing": {"plan": "free"},
                "flags": [],
            },
        },
    },
    {
        "key": "doc:c",
        "value": {
            "title": "Untouched",
            "status": "closed",
            "category": "other",
            "amount": 4,
            "enabled": False,
            "note": "keep-c",
            "metadata": {"source": "manual", "flags": []},
        },
    },
]


def reference(cases, schemas):
    entries, excluded = [], []
    for case in cases:
        schema = schemas[case["id"]]
        encoded = json.dumps(schema, separators=(",", ":"))
        if '"generated"' in encoded or "x-antfly-column-name" in encoded:
            excluded.append(
                {"id": case["id"], "reason": "native generated/alias profile required"}
            )
            continue
        properties = schema["document_schemas"][schema["default_type"]]["schema"][
            "properties"
        ]
        if any(name.startswith("_") for name in properties):
            raise ValueError("schema cannot replace reserved physical identity")
        columns = ["_id", *properties]
        with closing(sqlite3.connect(":memory:")) as db:
            register(db)
            db.execute("PRAGMA case_sensitive_like=ON")
            types = {
                "integer": "INTEGER",
                "numeric": "REAL",
                "number": "REAL",
                "boolean": "BOOLEAN",
            }
            db.execute(
                "CREATE TABLE docs (_id TEXT PRIMARY KEY"
                + "".join(
                    ',"' + name + '" ' + types.get(prop.get("type"), "TEXT")
                    for name, prop in properties.items()
                )
                + ")"
            )
            # Model SQL INSERT's create-only identity, not legacy native upsert.
            for seed in SEEDS:
                db.execute(
                    "INSERT INTO docs VALUES (" + ",".join("?" for _ in columns) + ")",
                    [
                        seed["key"],
                        *[
                            json.dumps(seed["value"].get(name), separators=(",", ":"))
                            if isinstance(seed["value"].get(name), (dict, list))
                            else seed["value"].get(name)
                            for name in properties
                        ],
                    ],
                )
            db.commit()
            db.isolation_level = None
            allowed = {
                sqlite3.SQLITE_SELECT,
                sqlite3.SQLITE_READ,
                sqlite3.SQLITE_FUNCTION,
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            }
            db.set_authorizer(
                lambda action, name, _column, database, _trigger: (
                    sqlite3.SQLITE_OK
                    if action in allowed
                    and (
                        action
                        not in {
                            sqlite3.SQLITE_INSERT,
                            sqlite3.SQLITE_UPDATE,
                            sqlite3.SQLITE_DELETE,
                        }
                        or (database == "main" and name == "docs")
                    )
                    else sqlite3.SQLITE_DENY
                )
            )
            steps = 0

            def progress():
                nonlocal steps
                steps += 1000
                return steps > 100_000

            db.set_progress_handler(progress, 1000)
            try:
                cursor = db.execute(
                    case["sql"],
                    {
                        str(i): parameter(value)
                        for i, value in enumerate(case["params"], 1)
                    },
                )
                rows = [list(row) for row in cursor.fetchmany(4097)]
                affected = db.execute("SELECT changes()").fetchone()[0]
                if not affected or len(rows) > 4096:
                    raise ValueError("non-exercising or oversized mutation")
                final = {
                    row[0]: row[1:]
                    for row in db.execute("SELECT * FROM docs ORDER BY _id").fetchmany(
                        4097
                    )
                }
                if len(final) > 4096:
                    raise ValueError("final storage exceeds row budget")
                # Native documents retain undeclared data during field updates.
                expected = []
                for seed in SEEDS:
                    if seed["key"] not in final:
                        continue
                    value = dict(seed["value"])
                    for name, cell in zip(properties, final[seed["key"]], strict=True):
                        original = seed["value"].get(name)
                        # SQLite REAL affinity must not change the physical
                        # representation of untouched native document fields.
                        if (
                            isinstance(original, int)
                            and not isinstance(original, bool)
                            and isinstance(cell, float)
                            and cell.is_integer()
                            and int(cell) == original
                        ):
                            cell = original
                        if isinstance(original, (dict, list)) and cell is not None:
                            cell = json.loads(cell)
                        if (
                            properties[name].get("type") == "boolean"
                            and cell is not None
                        ):
                            cell = bool(cell)
                        if name in seed["value"] or cell is not None:
                            value[name] = cell
                    expected.append({"key": seed["key"], "value": value})
                if set(final) - {seed["key"] for seed in SEEDS}:
                    raise ValueError("insert identity/default profile required")
                entries.append(
                    {
                        "id": case["id"],
                        "schema": schema,
                        "columns": [column[0] for column in cursor.description]
                        if cursor.description
                        else [],
                        "rows": rows,
                        "affected": affected,
                        "final": expected,
                    }
                )
            except (sqlite3.Error, ValueError, TypeError) as error:
                excluded.append({"id": case["id"], "reason": str(error)})
    return {
        "format": 1,
        "profile": "guarded-document-field-mutations",
        "seeds": SEEDS,
        "entries": entries,
        "excluded": excluded,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    campaign = json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
        "entries"
    ]
    schemas = {entry["id"]: entry["schema"] for entry in campaign}
    if len(schemas) != len(campaign) or len(campaign) != 211:
        parser.error("document campaign requires 211 unique source IDs")
    requested = set(schemas)
    if args.check:
        expected = json.loads(args.check.read_text())
        ids = [entry["id"] for entry in expected["entries"]]
        if not ids or len(ids) != len(set(ids)) or not set(ids) <= requested:
            parser.error("golden IDs must be unique campaign members")
        requested = set(ids)
    cases = [
        case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if case["id"] in requested
    ]
    result = reference(cases, schemas)
    if args.check:
        expected.pop("excluded", None)
        result.pop("excluded", None)
        if result != expected:
            parser.error("document reference drift")
        print(f"Verified {len(result['entries'])} exact document mutation contracts")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
