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
import copy
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres, cursor_result

FIRST = "00000000-0000-0000-0000-000000000001"
SECOND = "00000000-0000-0000-0000-000000000002"
PROBE = "00000000-0000-0000-0000-000000000003"
IDS = [
    674,
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
    735,
    737,
    738,
    739,
    740,
    744,
    745,
    779,
    1408,
    1412,
]


def foreign_key_declaration(number):
    """Keep both sides real: parent key, historical children, and future writes."""
    if number == 674:
        table = "inline_constraints"
        setup = [
            {
                "statement": "CREATE TABLE tenants(id TEXT PRIMARY KEY)",
                "table": "tenants",
            },
            {"statement": "INSERT INTO tenants(id) VALUES ('tenant-a')"},
        ]
        columns = "id::text AS id,tenant_id,email,amount::text AS amount"
        statements = [
            (
                f"INSERT INTO {table}(id,tenant_id,email,amount) VALUES ('{PROBE}','tenant-a','one@example.test',1)",
                None,
            ),
            (
                f"INSERT INTO {table}(id,tenant_id,email,amount) VALUES ('00000000-0000-0000-0000-000000000004','missing','two@example.test',2)",
                "23503",
            ),
            (
                f"INSERT INTO {table}(id,tenant_id,email,amount) VALUES ('00000000-0000-0000-0000-000000000004','tenant-a','one@example.test',2)",
                "23505",
            ),
            (
                f"INSERT INTO {table}(id,tenant_id,email,amount) VALUES ('00000000-0000-0000-0000-000000000004','tenant-a','two@example.test',-1)",
                "23514",
            ),
        ]
    elif number == 735:
        table = "usage_records"
        setup = [
            {
                "statement": "CREATE TABLE users(id TEXT,region TEXT,PRIMARY KEY(id,region))",
                "table": "users",
            },
            {"statement": "INSERT INTO users(id,region) VALUES ('parent-a','us')"},
            {
                "statement": "CREATE TABLE usage_records(id UUID PRIMARY KEY,user_id TEXT,region TEXT)",
                "table": table,
            },
            {
                "statement": f"INSERT INTO usage_records(id,user_id,region) VALUES ('{FIRST}',NULL,NULL),('{SECOND}','parent-a','us')"
            },
        ]
        columns = "id::text AS id,user_id,region"
        statements = [
            (
                f"INSERT INTO {table}(id,user_id,region) VALUES ('00000000-0000-0000-0000-000000000004','parent-a',NULL)",
                "23503",
            ),
            (
                f"INSERT INTO {table}(id,user_id,region) VALUES ('00000000-0000-0000-0000-000000000004','missing','us')",
                "23503",
            ),
            (
                f"INSERT INTO {table}(id,user_id,region) VALUES ('{PROBE}','parent-a','us')",
                None,
            ),
        ]
    else:
        raise ValueError("unknown foreign-key lifecycle")
    return {
        "id": f"sql-{number:04}",
        "table": table,
        "setup": setup,
        "read_sql": f"SELECT {columns} FROM {table}",
        "ddl_sqlstate": None,
        "probes": [{"statement": sql, "sqlstate": code} for sql, code in statements],
    }


def declaration(number):
    if number not in IDS:
        raise ValueError("schema lifecycle case is outside the qualified cohort")
    if number in {674, 735}:
        return foreign_key_declaration(number)
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
    if number == 745:
        setup.append(
            {"statement": "ALTER TABLE usage_records ALTER COLUMN status SET NOT NULL"}
        )
    seed = f"INSERT INTO {table}(tenant_id,id,status) VALUES ('tenant-a','{FIRST}','active'),('tenant-b','{SECOND}','ready')"
    if not stage:
        seed = f"INSERT INTO {table}(id,email,tenant_id,status,amount,metadata) VALUES ('{FIRST}','a@example.test','tenant-a','active',5,'{{\"source\":\"old\"}}'),('{SECOND}',NULL,'tenant-b','ready',NULL,NULL)"
    if number == 737:
        seed = seed.replace("'active',5", "'unknown',5")
    if number == 1412:
        seed = seed.replace("'active',5", "'active',-5")
    setup.append({"statement": seed})
    if number in {739, 1412}:
        name = (
            "positive_amount" if number == 1412 else "usage_records_status_known_check"
        )
        expression = "amount >= 0" if number == 1412 else "status != 'unknown'"
        setup.append(
            {
                "statement": f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK({expression}) NOT VALID"
            }
        )
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
    elif number in {737, 738, 739}:
        probe(
            f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','unknown')",
            "23514",
        )
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','active')")
    elif number == 740:
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','unknown')")
    elif number == 744:
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}',NULL)", "23502")
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','active')")
    elif number == 745:
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}',NULL)")
    elif number == 779:
        probe("SELECT metadata FROM usage_records", "42703")
        probe(f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','new')")
    result = {
        "id": identity,
        "table": table,
        "setup": setup,
        "read_sql": f"SELECT {fields} FROM {table}",
        "ddl_sqlstate": {733: "42809", 1408: "42P10", 1412: "23514"}.get(number),
        "probes": probes,
    }
    if number in {737, 739, 1412}:
        result["check_name"] = (
            "positive_amount" if number == 1412 else "usage_records_status_known_check"
        )
    return result


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    result = []
    declarations = [declaration(number) for number in IDS]
    invalid_history = copy.deepcopy(declaration(738))
    invalid_history["scenario"] = (
        "invalid historical CHECK leaves predecessor published"
    )
    invalid_history["native_job_error"] = "ConstraintActivationFailed"
    invalid_history["ddl_sqlstate"] = "23514"
    invalid_history["setup"][-1]["statement"] = invalid_history["setup"][-1][
        "statement"
    ].replace("'active',5", "'unknown',5")
    invalid_history["probes"] = [
        {
            "statement": f"INSERT INTO usage_records(id,status) VALUES ('{PROBE}','unknown')",
            "sqlstate": None,
        }
    ]
    declarations.append(invalid_history)
    duplicate_history = copy.deepcopy(declaration(730))
    duplicate_history["scenario"] = (
        "duplicate historical UNIQUE leaves predecessor published"
    )
    duplicate_history["native_job_error"] = "ConstraintActivationFailed"
    duplicate_history["ddl_sqlstate"] = "23505"
    duplicate_history["setup"][-1]["statement"] = duplicate_history["setup"][-1][
        "statement"
    ].replace(f"'{SECOND}',NULL", f"'{SECOND}','a@example.test'")
    duplicate_history["probes"] = [
        {
            "statement": f"INSERT INTO usage_records(id,email) VALUES ('{PROBE}','a@example.test')",
            "sqlstate": None,
        }
    ]
    declarations.append(duplicate_history)
    invalid_validation = copy.deepcopy(declaration(739))
    invalid_validation["scenario"] = (
        "failed explicit validation preserves NOT VALID declaration"
    )
    invalid_validation["native_job_error"] = "ConstraintActivationFailed"
    invalid_validation["ddl_sqlstate"] = "23514"
    invalid_validation["setup"][-2]["statement"] = invalid_validation["setup"][-2][
        "statement"
    ].replace("'active',5", "'unknown',5")
    invalid_validation["probes"] = [
        {
            "statement": "INSERT INTO usage_records(id,status) VALUES ('00000000-0000-0000-0000-000000000004','unknown')",
            "sqlstate": "23514",
        },
        {
            "statement": "UPDATE usage_records SET status='active' WHERE status='unknown'",
            "sqlstate": None,
        },
    ]
    declarations.append(invalid_validation)
    mixed_validation = copy.deepcopy(declaration(739))
    mixed_validation["scenario"] = (
        "validating one CHECK preserves unrelated NOT VALID history"
    )
    mixed_validation["retained_not_valid_check"] = "positive_amount"
    mixed_validation["setup"][-2]["statement"] = mixed_validation["setup"][-2][
        "statement"
    ].replace("'active',5", "'active',-5")
    mixed_validation["setup"].append(
        {
            "statement": "ALTER TABLE usage_records ADD CONSTRAINT positive_amount CHECK(amount>=0) NOT VALID"
        }
    )
    mixed_validation["probes"].append(
        {
            "statement": "INSERT INTO usage_records(id,status,amount) VALUES ('00000000-0000-0000-0000-000000000005','active',-1)",
            "sqlstate": "23514",
        }
    )
    declarations.append(mixed_validation)
    with postgres() as db:
        for case in declarations:
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
                            allow_empty=case["id"] == "sql-0674",
                        )

                case["after_ddl"] = snapshot()
                if "retained_not_valid_check" in case:
                    assert not db.execute(
                        "SELECT convalidated FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s",
                        (case["table"], case["retained_not_valid_check"]),
                    ).fetchone()[0]

                def declaration_state():
                    return not db.execute(
                        "SELECT convalidated FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s",
                        (case["table"], case["check_name"]),
                    ).fetchone()[0]

                if "check_name" in case:
                    case["declared_not_valid"] = declaration_state()
                for probe in case["probes"]:
                    try:
                        with db.transaction():
                            db.execute(probe["statement"])
                            # The native campaign uses implicit one-statement
                            # transactions. The oracle's enclosing rollback
                            # transaction must exercise their commit-time FK
                            # check, not silently defer it until rollback.
                            if case["id"] == "sql-0735":
                                db.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    except psycopg.Error as error:
                        if error.sqlstate != probe["sqlstate"]:
                            raise
                    else:
                        if probe["sqlstate"] is not None:
                            raise ValueError(f"{case['id']}: rejection probe succeeded")
                    probe["after"] = snapshot()
                    if "check_name" in case:
                        probe["declared_not_valid"] = declaration_state()
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
