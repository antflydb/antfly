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

"""Exact original schema lifecycle DDL, future-write enforcement and postimages."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres, cursor_result

FIRST = "00000000-0000-0000-0000-000000000001"
SECOND = "00000000-0000-0000-0000-000000000002"
PROBE = "00000000-0000-0000-0000-000000000003"
IDS = [
    677,
    678,
    680,
    681,
    727,
    728,
    729,
    730,
    731,
    733,
    738,
    740,
    779,
    1408,
    1412,
]


def declaration(number):
    if number not in IDS:
        raise ValueError("schema lifecycle case is outside the qualified cohort")
    identity = f"sql-{number:04}"
    stage = number in {677, 678, 680, 681}
    table = "usage_stage" if stage else "usage_records"
    columns = "tenant_id TEXT,id UUID,status TEXT"
    if number == 680:
        columns += ",CONSTRAINT usage_stage_pkey PRIMARY KEY(tenant_id,id)"
    if number == 681:
        columns += ",CONSTRAINT usage_stage_identity UNIQUE(tenant_id,id)"
    if not stage:
        columns = "id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT,amount NUMERIC,metadata JSONB,region TEXT"
        if number in {1408, 1412}:
            columns = columns.replace("id UUID", "id TEXT")
        if number == 1412:
            columns += ",CONSTRAINT positive_amount CHECK(amount >= 0)"
        columns += ",user_id TEXT"
    setup = []
    setup.append({"statement": f"CREATE TABLE {table}({columns})", "table": table})
    if number == 1408:
        setup.append(
            {
                "statement": "CREATE INDEX usage_records_email_idx ON usage_records(email)"
            }
        )
    if number == 733:
        setup.append(
            {
                "statement": "ALTER TABLE usage_records ADD CONSTRAINT usage_records_email_key UNIQUE(email)"
            }
        )
    if number == 740:
        setup.append(
            {
                "statement": "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_known_check CHECK(status != 'unknown')"
            }
        )
    seed = f"INSERT INTO {table}(tenant_id,id,status) VALUES ('tenant-a','{FIRST}','active'),('tenant-b','{SECOND}','ready')"
    if not stage:
        seed = f"INSERT INTO {table}(id,email,tenant_id,status,amount,metadata) VALUES ('{FIRST}','a@example.test','tenant-a','active',5,'{{\"source\":\"old\"}}'),('{SECOND}',NULL,'tenant-b','ready',NULL,NULL)"
    setup.append({"statement": seed})
    fields = (
        "tenant_id,id::text AS id,status"
        if stage
        else "id::text AS id,email,tenant_id,status,amount::text AS amount"
    )
    probes = []

    def probe(statement, code=None):
        # Rejected writes use an otherwise absent primary identity even after
        # reopening, so a PK collision cannot mask lost UNIQUE/CHECK enforcement.
        if code is not None:
            statement = statement.replace(PROBE, "00000000-0000-0000-0000-000000000004")
        probes.append({"statement": statement, "sqlstate": code})

    if number == 1408:
        probe(
            f"INSERT INTO usage_records(id,email) VALUES ('{PROBE}','a@example.test')"
        )
    elif number == 1412:
        probe(f"INSERT INTO usage_records(id,amount) VALUES ('{PROBE}',-2)", "23514")
        probe(f"INSERT INTO usage_records(id,amount) VALUES ('{PROBE}',42)")
    elif number in {677, 678}:
        probe(
            f"INSERT INTO usage_stage(tenant_id,id,status) VALUES ('tenant-a','{FIRST}','duplicate')",
            "23505",
        )
        probe(f"INSERT INTO usage_stage(tenant_id,id) VALUES (NULL,'{PROBE}')", "23502")
    elif number in {680, 681}:
        probe(
            f"INSERT INTO usage_stage(tenant_id,id,status) VALUES ('tenant-a','{FIRST}','duplicate')"
        )
    elif number == 727:
        fields += ",email_upper_key"
        probe(
            f"INSERT INTO usage_records(id,email,email_upper_key) VALUES ('{PROBE}','b@example.test','forged')",
            "428C9",
        )
        probe(
            f"INSERT INTO usage_records(id,email) VALUES ('{PROBE}','b@example.test')"
        )
    elif number == 728:
        fields += ",optional_status"
        probes.append(
            {
                "statement": "ALTER TABLE IF EXISTS missing_records ADD COLUMN optional_status text",
                "sqlstate": None,
                "repeat_on_reopen": True,
                "absent_table": "missing_records",
            }
        )
        probe(
            f"INSERT INTO usage_records(id,optional_status) VALUES ('{PROBE}','present')"
        )
    elif number == 729:
        fields += ",billing_status"
        probe(
            f"INSERT INTO usage_records(id,billing_status) VALUES ('{PROBE}',NULL)",
            "23502",
        )
        probe(f"INSERT INTO usage_records(id) VALUES ('{PROBE}')")
    elif number in {730, 731, 733}:
        probe(
            f"INSERT INTO usage_records(id,email) VALUES ('{PROBE}','a@example.test')",
            "23505",
        )
        probe(f"INSERT INTO usage_records(id,email) VALUES ('{PROBE}',NULL)")
    elif number == 738:
        probe(
            f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','unknown')",
            "23514",
        )
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','active')")
    elif number == 740:
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','unknown')")
    elif number == 779:
        probe("SELECT metadata FROM usage_records", "42703")
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','new')")
    return {
        "id": identity,
        "table": table,
        "setup": setup,
        "read_sql": f"SELECT {fields} FROM {table}",
        "ddl_sqlstate": {733: "42809", 1408: "42P10", 1412: "23514"}.get(number),
        "probes": probes,
    }


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    result = []
    with postgres() as db:
        for number in IDS:
            case = declaration(number)
            with db.transaction(force_rollback=True):
                for step in case["setup"]:
                    db.execute(step["statement"])
                try:
                    with db.transaction():
                        db.execute(originals[case["id"]]["sql"])
                except psycopg.Error as error:
                    if error.sqlstate != case["ddl_sqlstate"]:
                        raise
                else:
                    if case["ddl_sqlstate"] is not None:
                        raise ValueError(f"{case['id']}: DDL rejection succeeded")

                def snapshot():
                    with psycopg.RawCursor(db) as cursor:
                        return cursor_result(
                            cursor,
                            {"id": case["id"], "sql": case["read_sql"], "params": []},
                            read=True,
                        )

                case["after_ddl"] = snapshot()
                for probe in case["probes"]:
                    try:
                        with db.transaction():
                            db.execute(probe["statement"])
                    except psycopg.Error as error:
                        if error.sqlstate != probe["sqlstate"]:
                            raise
                    else:
                        if probe["sqlstate"] is not None:
                            raise ValueError(f"{case['id']}: rejection probe succeeded")
                    probe["after"] = snapshot()
                result.append(case)
    return {"format": 1, "reference": "PostgreSQL exact SQL", "cases": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("schema lifecycle PostgreSQL drift")
        print(
            f"verified {len(result['cases'])} original schema lifecycles and complete probe postimages"
        )
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
