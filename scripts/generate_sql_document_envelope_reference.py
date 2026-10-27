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

IDS = [
    907,
    908,
    914,
    919,
    931,
    936,
    937,
    942,
    944,
    945,
    1068,
    1071,
    1078,
    1080,
    1105,
    1106,
    1107,
    1108,
]
WITNESSES = deepcopy(SEEDS)
WITNESSES[0]["value"]["title"] = "Before"


def reference():
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb

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
                db.execute("ALTER TABLE docs ADD COLUMN _doc jsonb")
                for seed in WITNESSES:
                    db.execute(
                        "UPDATE docs SET _doc=%s WHERE _id=%s",
                        (Jsonb(seed["value"]), seed["key"]),
                    )
                before = dict(db.execute("SELECT _id,xmin::text FROM docs"))
                affected, tag, code = 0, originals[identity]["sql"].split()[0], None
                returned, columns, oids, nulls = [], [], [], []
                try:
                    with db.transaction(), psycopg.RawCursor(db) as cursor:
                        if originals[identity]["params"]:
                            raise ValueError("unexpected original parameters")
                        cursor.execute(originals[identity]["sql"], binary=True)
                        affected = cursor.rowcount
                        tag = cursor.statusmessage.split()[0]
                        if cursor.description:
                            columns = [c.name for c in cursor.description]
                            oids = [c.type_code for c in cursor.description]
                            returned = [
                                [encoded(cell) for cell in row]
                                for row in cursor.fetchall()
                            ]
                            nulls = [
                                [
                                    cursor.pgresult.get_value(i, j) is None
                                    for j in range(len(row))
                                ]
                                for i, row in enumerate(returned)
                            ]
                except psycopg.Error as error:
                    code = error.sqlstate
                # Self-MERGE's NOT MATCHED arm and conflict DO NOTHING are
                # necessarily zero-effect contracts, not discarded witnesses.
                if affected <= 0 and code is None and number not in {1106, 1107}:
                    raise ValueError(f"{identity}: non-exercising witness")
                after = dict(db.execute("SELECT _id,xmin::text FROM docs"))
                final, generated = [], []
                selection = sql.SQL("SELECT _id,{} FROM docs ORDER BY _id").format(
                    sql.SQL(",").join(sql.Identifier(name) for name in props)
                )
                for row in db.execute(selection):
                    key = row[0]
                    cells = dict(zip(props, row[1:], strict=True))
                    value = deepcopy(seeds.get(key, {}))
                    for name, cell in cells.items():
                        # PostgreSQL supplies the logical SQL value. At the
                        # document boundary, SQL NULL is absence when JSON
                        # null is forbidden by the declared scalar schema.
                        kind = props[name].get("type")
                        permits_json_null = kind is None or (
                            isinstance(kind, list) and "null" in kind
                        )
                        if cell is None and not permits_json_null:
                            value.pop(name, None)
                            continue
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
                        "requires_native_session": number in {1105, 1106, 1107, 1108},
                        "schema": schema,
                        "command_tag": tag,
                        "affected": affected,
                        "sqlstate": code,
                        "untouched": sorted(
                            key
                            for key, stamp in after.items()
                            if before.get(key) == stamp
                        ),
                        "columns": columns,
                        "column_oids": oids,
                        "rows": returned,
                        "sql_nulls": nulls,
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
