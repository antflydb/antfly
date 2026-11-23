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

"""Exact document-root operations with independently owned JSONB postimages.

The opaque default identity is nondeterministic in both engines. Its actual
bytes are not frozen; shape, freshness, uniqueness and associated postimage are.
"""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, SEEDS, postgres

IDS = [884, 885, 889, 894]


def reference():
    import psycopg
    from psycopg.types.json import Jsonb

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
        for number in IDS:
            identity = f"sql-{number:04}"
            with db.transaction(force_rollback=True):
                db.execute(
                    "CREATE TABLE docs(_id text PRIMARY KEY DEFAULT replace(gen_random_uuid()::text,'-',''),_doc jsonb NOT NULL)"
                )
                for seed in SEEDS:
                    db.execute(
                        "INSERT INTO docs VALUES(%s,%s)",
                        (seed["key"], Jsonb(seed["value"])),
                    )
                with psycopg.RawCursor(db) as cursor:
                    if originals[identity]["params"]:
                        raise ValueError("root extension parameter contract drift")
                    cursor.execute(originals[identity]["sql"])
                    outcome = {
                        "affected": cursor.rowcount,
                        "command_tag": cursor.statusmessage.split()[0],
                    }
                expected = {884: 0, 885: 1, 889: 1, 894: 1}[number]
                if outcome["affected"] != expected:
                    raise ValueError(f"{identity}: original root outcome drift")
                known = {seed["key"] for seed in SEEDS}
                final, generated = [], []
                for key, value in db.execute("SELECT _id,_doc FROM docs ORDER BY _id"):
                    if key in known:
                        final.append({"key": key, "value": value})
                    else:
                        if len(key) != 32 or any(
                            c not in "0123456789abcdef" for c in key
                        ):
                            raise ValueError("invalid opaque default identity")
                        generated.append(value)
                if len(generated) != int(number == 885):
                    raise ValueError("unexpected generated identities")
                entries.append(
                    {
                        "id": identity,
                        "schema": schemas[identity],
                        "command_tag": outcome["command_tag"],
                        "affected": expected,
                        "final": final,
                        "generated_final": generated,
                    }
                )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL with opaque default identity",
        "seed_version": 42,
        "seeds": SEEDS,
        "entries": entries,
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
            raise SystemExit("document root extension reference drift")
        print(f"verified {len(result['entries'])} original document root operations")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
