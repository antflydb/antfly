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

"""PostgreSQL witnesses for native phonetic kernels and exact remaining scalar reads."""

import argparse
import itertools
import json
import subprocess

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {x["id"]: x for x in inventory["entries"]}
    source = json.loads(
        subprocess.check_output(
            ["git", "show", inventory["source_commit"] + ":" + inventory["source_path"]]
        )
    )["entries"]
    strings = list(
        dict.fromkeys(
            [
                "",
                "Robert",
                "Rupert",
                "Rubin",
                "Ashcraft",
                "Ashcroft",
                "Tymczak",
                "Pfister",
                "Ångström",
                "Émile",
                "東京",
                "123",
                "BHB",
                "BWB",
                "B-B",
                "ABC xyz",
                "A.B,C",
            ]
            + [chr(c) for c in range(1, 256)]
            + ["B" + chr(c) + "B" for c in range(1, 256)]
            + ["".join(p) for p in itertools.product("ABDFGHLR0123-", repeat=3)]
        )
    )
    with postgres() as db:
        db.execute("CREATE EXTENSION fuzzystrmatch")
        db.execute('CREATE EXTENSION "uuid-ossp"')
        values = [
            dict(zip(("input", "soundex", "difference"), row, strict=True))
            for row in db.execute(
                "SELECT x,soundex(x),difference(x,'Robert') FROM unnest(%s::text[]) WITH ORDINALITY AS v(x,n) ORDER BY n",
                (strings,),
            )
        ]
        profiles = []
        for identity in ["sql-0838", "sql-0178"]:
            case = cases[identity]
            if identity == "sql-0838":
                schema = json.loads(source[837]["source_schema_json"])
                rows = [
                    {
                        "key": f"doc:{i:03d}",
                        "value": {} if text is None else {"status": text},
                    }
                    for i, text in enumerate(
                        [
                            "A",
                            "aaahw",
                            "éA",
                            "Adams",
                            "B",
                            "a-e",
                            "123",
                            "",
                            None,
                            "ayow",
                            "AL",
                            "A100",
                        ]
                    )
                ]
                db.execute("CREATE TABLE docs(_id text PRIMARY KEY,status text)")
                for row in rows:
                    db.execute(
                        "INSERT INTO docs VALUES(%s,%s)",
                        (row["key"], row["value"].get("status")),
                    )
                cursor = db.execute(case["sql"])
                expected = cursor.fetchall()
                table = "docs"
            else:
                schema = {
                    "version": 1,
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {"id": {"type": "keyword"}},
                                "required": ["id"],
                                "additionalProperties": False,
                            }
                        }
                    },
                }
                rows = [
                    {"key": "u1", "value": {"id": "u1"}},
                    {"key": "u2", "value": {"id": "u2"}},
                ]
                db.execute("CREATE TABLE usage_records(id text PRIMARY KEY)")
                for row in rows:
                    db.execute(
                        "INSERT INTO usage_records VALUES(%s)", (row["value"]["id"],)
                    )
                assert case["params"] == [{"string": "u1"}]
                # Change only the driver's placeholder spelling, never source SQL.
                cursor = db.execute(case["sql"].replace("$1", "%s"), ("u1",))
                generated = cursor.fetchall()
                assert len(generated) == 1 and generated[0][0].version == 4
                expected = None
                table = "usage_records"
            profiles.append(
                {
                    "id": identity,
                    "sql": case["sql"],
                    "source_entry_sha256": case["source_entry_sha256"],
                    "params": case["params"],
                    "table": table,
                    "schema": schema,
                    "rows": rows,
                    "oids": [x.type_code for x in cursor.description],
                    "expected_rows": expected,
                    "row_count": 1 if expected is None else len(expected),
                }
            )
        probes = []
        from psycopg import Error

        for query in [
            "SELECT soundex(1)",
            "SELECT soundex()",
            "SELECT difference('a')",
            "SELECT difference('a',1)",
            "SELECT uuid_generate_v4(1)",
        ]:
            try:
                db.execute(query)
                raise AssertionError(query)
            except Error as exc:
                probes.append({"sql": query, "code": exc.sqlstate})
        nulls = db.execute(
            "SELECT soundex(NULL),difference(NULL,'a'),difference('a',NULL),difference('','')"
        ).fetchone()
    return {
        "format": 1,
        "reference": "PostgreSQL 18 fuzzystrmatch and uuid-ossp native-compatible functions",
        "values": values,
        "profiles": profiles,
        "probes": probes,
        "nulls": nulls,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    target = FIXTURES / "sql_phonetic_reference.json"
    value = json.loads(json.dumps(reference()))
    if args.check:
        assert json.loads(target.read_text()) == value, "phonetic reference drift"
    else:
        target.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
