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

"""Original document assignments respect readonly metadata and JSONB typing.

The native envelope version is represented by a PostgreSQL stored generated
column solely to qualify its output-only assignment contract. Its literal
value is not a model of Antfly's commit clock. No statement is rewritten.
"""

import argparse
from copy import deepcopy
import json
from pathlib import Path

from generate_sql_postgres_reference import (
    FIXTURES,
    SEEDS,
    create_table,
    postgres,
    properties,
)

CONTRACTS = {
    "sql-0912": "428C9",
    "sql-0915": "428C9",
    "sql-0916": "42804",
    "sql-0921": "428C9",
    "sql-0943": "428C9",
    "sql-1083": "42804",
}


def reference():
    import psycopg

    originals = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    schemas = {
        case["id"]: case["schema"]
        for case in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for identity, code in CONTRACTS.items():
            schema = schemas[identity]
            with db.transaction(force_rollback=True):
                create_table(db, "docs", properties(schema), SEEDS, identity=True)
                db.execute(
                    "ALTER TABLE docs ADD COLUMN _version bigint GENERATED ALWAYS AS (42) STORED"
                )
                before = db.execute(
                    "SELECT to_jsonb(d) FROM docs d ORDER BY _id"
                ).fetchall()
                try:
                    with db.transaction(), psycopg.RawCursor(db) as cursor:
                        cursor.execute(originals[identity]["sql"])
                except psycopg.Error as error:
                    if error.sqlstate != code:
                        raise
                else:
                    raise ValueError(f"{identity}: invalid assignment succeeded")
                if (
                    before
                    != db.execute(
                        "SELECT to_jsonb(d) FROM docs d ORDER BY _id"
                    ).fetchall()
                ):
                    raise ValueError(
                        "rejected assignment changed the complete PostgreSQL postimage"
                    )
                entries.append(
                    {
                        "id": identity,
                        "schema": schema,
                        "sqlstate": code,
                        "final": deepcopy(SEEDS),
                    }
                )
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": "document-readonly-version-and-jsonb-assignment",
        "seeds": SEEDS,
        "entries": entries,
        "excluded": [],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.check and args.output:
        parser.error("--check cannot overwrite a reference")
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document assignment reference drift")
        print(f"verified {len(result['entries'])} exact original assignment boundaries")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
