#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exact document-field mutations with native envelope version predicates.

PostgreSQL owns the logical scalar DML result. Antfly's native gate separately
checks complete JSON postimages, preserved opaque fields and durable versions.
"""

import argparse
from copy import deepcopy
import json
from pathlib import Path

from generate_sql_postgres_reference import (
    FIXTURES,
    SEEDS,
    create_table,
    encoded,
    postgres,
    properties,
)

IDS = [907, 908, 914, 931, 936, 937, 1078, 1080]
WITNESSES = deepcopy(SEEDS)
WITNESSES[0]["value"]["title"] = "Before"


def reference():
    import psycopg

    originals = {
        c["id"]: c
        for c in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    schemas = {
        c["id"]: c["schema"]
        for c in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    seeds = {row["key"]: row["value"] for row in WITNESSES}
    entries = []
    with postgres() as db:
        for number in IDS:
            identity = f"sql-{number:04d}"
            schema = schemas[identity]
            props = properties(schema)
            with db.transaction(force_rollback=True):
                create_table(db, "docs", props, WITNESSES, identity=True)
                db.execute(
                    "ALTER TABLE docs ALTER COLUMN _id SET DEFAULT replace(gen_random_uuid()::text,'-','')"
                )
                db.execute(
                    "ALTER TABLE docs ADD COLUMN _version bigint NOT NULL DEFAULT 42"
                )
                with psycopg.RawCursor(db) as cursor:
                    if originals[identity]["params"]:
                        raise ValueError("unexpected original parameters")
                    cursor.execute(originals[identity]["sql"])
                    affected = cursor.rowcount
                    tag = cursor.statusmessage.split()[0]
                if affected <= 0:
                    raise ValueError(f"{identity}: non-exercising witness")
                final, generated = [], []
                for row in db.execute("SELECT * FROM docs ORDER BY _id"):
                    key = row[0]
                    cells = dict(zip(props, row[1:-1], strict=True))
                    value = deepcopy(seeds.get(key, {}))
                    for name, cell in cells.items():
                        if name in value or cell is not None:
                            value[name] = encoded(cell)
                    if key in seeds:
                        final.append({"key": key, "value": value})
                    else:
                        if len(key) != 32 or any(
                            c not in "0123456789abcdef" for c in key
                        ):
                            raise ValueError("invalid opaque identity")
                        generated.append(value)
                entries.append(
                    {
                        "id": identity,
                        "schema": schema,
                        "command_tag": tag,
                        "affected": affected,
                        "final": final,
                        "generated_final": generated,
                    }
                )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL with native version predicate witness",
        "seed_version": 42,
        "seeds": WITNESSES,
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.check and args.output:
        parser.error("a check must not overwrite its reference")
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document envelope reference drift")
        print(f"verified {len(result['entries'])} original document envelope mutations")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
