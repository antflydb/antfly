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

"""Qualify the original combined table contract and future UUID defaults."""

import argparse
import json
import sys
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, cursor_result, postgres

CASE_ID = "sql-0669"
READ = (
    "SELECT tenant_id,quantity::text AS quantity,metadata::text AS metadata,"
    "tags::text AS tags,tenant_key,uuid_extract_version(id)::text AS uuid_version,"
    "(uuid_extract_timestamp(id) IS NOT NULL)::text AS uuid_has_timestamp,"
    "(created_at IS NOT NULL)::text AS has_created_at "
    "FROM usage_records ORDER BY tenant_id"
)
PARENT_READ = "SELECT id FROM tenants ORDER BY id"
TYPED_READ = "SELECT id,tenant_id,quantity,metadata,tags,created_at,tenant_key FROM usage_records LIMIT 0"


def original():
    entries = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    entry = next(row for row in entries if row["id"] == CASE_ID)
    if entry["params"]:
        raise ValueError(
            "The original combined CREATE unexpectedly acquired parameters"
        )
    return entry["sql"]


def probe(statement, sqlstate=None, **extra):
    return {"statement": statement, "sqlstate": sqlstate, **extra}


def reference():
    import psycopg

    sql = original()
    parents = "('tenant-a'),('tenant-b'),('tenant-c'),('ABC'),('abc'),('tenant-v7'),('tenant-after'),('clock-a'),('clock-b')"
    case = {
        "id": CASE_ID,
        "table": "usage_records",
        "setup": [
            {
                "statement": "CREATE TABLE tenants(id TEXT PRIMARY KEY)",
                "table": "tenants",
            },
            {"statement": "INSERT INTO tenants(id) VALUES " + parents},
        ],
        "read_sql": READ,
        "ddl_sqlstate": None,
        "verify_primary_identity": True,
        "probes": [
            probe(
                "INSERT INTO usage_records(tenant_id,metadata,tags) VALUES('tenant-a','{\"n\":1.2300}'::jsonb,ARRAY['one',NULL,'two'])"
            ),
            probe("INSERT INTO usage_records(tenant_id) VALUES('missing')", "23503"),
            probe(
                "INSERT INTO usage_records(tenant_id,quantity) VALUES('tenant-b',-1)",
                "23514",
            ),
            probe(
                "INSERT INTO usage_records(tenant_id,quantity) VALUES('tenant-b',10000000000000000)",
                "22003",
            ),
            probe("INSERT INTO usage_records(tenant_id) VALUES(NULL)", "23502"),
            probe(
                "INSERT INTO usage_records(tenant_id,tenant_key) VALUES('tenant-b','forged')",
                "428C9",
            ),
            probe(
                "INSERT INTO usage_records(tenant_id,quantity) VALUES('tenant-b',1.235)"
            ),
            probe("UPDATE usage_records SET quantity=2.345 WHERE tenant_id='tenant-b'"),
            probe(
                "UPDATE tenants SET id='tenant-z' WHERE id='tenant-a'",
                "23503",
                reopen={"sqlstate": None},
            ),
            probe("DELETE FROM tenants WHERE id='tenant-a'"),
            probe(
                "UPDATE usage_records SET tenant_id='tenant-c' WHERE tenant_id='tenant-b'"
            ),
            probe("DELETE FROM tenants WHERE id='tenant-b'"),
            probe(
                "INSERT INTO usage_records(tenant_id,metadata,tags) VALUES('ABC','null'::jsonb,ARRAY[]::text[])"
            ),
            probe("INSERT INTO usage_records(tenant_id) VALUES('abc')"),
            probe("INSERT INTO usage_records(tenant_id) VALUES('ABC')", "23505"),
            probe(
                "INSERT INTO usage_records(id,tenant_id,quantity) SELECT id,'clock-a',0 FROM usage_records WHERE tenant_id='ABC'",
                "23505",
            ),
            probe(
                "INSERT INTO usage_records(id,tenant_id,quantity) VALUES(NULL,'clock-b',0)",
                "23502",
            ),
            probe("ALTER TABLE usage_records ALTER COLUMN id SET DEFAULT uuidv7()"),
            probe(
                "INSERT INTO usage_records(tenant_id,tags) VALUES('tenant-v7',ARRAY['v7'])"
            ),
            probe(sql, preserve_owner=True, repeat_on_reopen=True),
        ],
    }
    restarted = [
        probe(
            "INSERT INTO usage_records(tenant_id,quantity) VALUES('tenant-after',3.455)"
        ),
        probe(
            "UPDATE usage_records SET metadata='null'::jsonb,tags=ARRAY['cold',NULL] WHERE tenant_id='tenant-after'"
        ),
        probe("DELETE FROM tenants WHERE id='tenant-v7'"),
        probe(
            "UPDATE usage_records SET quantity=-1 WHERE tenant_id='tenant-after'",
            "23514",
        ),
        probe("INSERT INTO usage_records(tenant_id) VALUES('missing')", "23503"),
        probe("ALTER TABLE usage_records ALTER COLUMN quantity SET DEFAULT 1 / 0"),
        probe("INSERT INTO usage_records(tenant_id) VALUES('missing')", "22012"),
    ]
    clock_checks = [
        {
            "statement": "INSERT INTO usage_records(tenant_id,quantity) VALUES('clock-a',0) RETURNING (created_at = now())::text AS clock_matches"
        },
        {
            "statement": "INSERT INTO usage_records(tenant_id,quantity) VALUES('clock-b',0) RETURNING (created_at = transaction_timestamp())::text AS clock_matches"
        },
        {
            "statement": "SELECT (count(DISTINCT created_at) = 1 AND bool_and(created_at = now()))::text AS clock_matches FROM usage_records WHERE tenant_id IN ('clock-a','clock-b')"
        },
    ]
    with postgres() as db:
        print(f"PostgreSQL server version: {db.info.server_version}", file=sys.stderr)
        with db.transaction(force_rollback=True):
            if (
                db.execute("SELECT to_regclass('usage_records')").fetchone()[0]
                is not None
            ):
                raise ValueError("CREATE must exercise an absent relation")
            for step in case["setup"]:
                db.execute(step["statement"])
            if db.execute(sql).statusmessage != "CREATE TABLE":
                raise ValueError("Original CREATE was not admitted")
            typed_oids = [
                column.type_code for column in db.execute(TYPED_READ).description
            ]
            if typed_oids != [2950, 25, 1700, 3802, 1009, 1184, 25]:
                raise ValueError(f"Original table types differ: {typed_oids}")

            def snapshot(statement=READ, *, read=True):
                with psycopg.RawCursor(db) as cursor:
                    return cursor_result(
                        cursor,
                        {"id": CASE_ID, "sql": statement, "params": []},
                        read=read,
                        allow_empty=True,
                    )

            def relation():
                return db.execute(
                    "SELECT oid,relfilenode FROM pg_class WHERE oid='usage_records'::regclass"
                ).fetchone()

            def execute(step, replay=False):
                expected = (
                    step.get("reopen", {}).get("sqlstate")
                    if replay and "reopen" in step
                    else step["sqlstate"]
                )
                before = relation() if step.get("preserve_owner") else None
                try:
                    with db.transaction():
                        db.execute(step["statement"])
                except psycopg.Error as error:
                    if error.sqlstate != expected:
                        raise
                else:
                    if expected is not None:
                        raise ValueError(f"Expected {expected}: {step['statement']}")
                if before is not None and relation() != before:
                    raise ValueError("IF NOT EXISTS replaced its existing owner")

            case["after_ddl"] = snapshot()
            case["related_after_ddl"] = [
                {"statement": PARENT_READ, "output": snapshot(PARENT_READ)}
            ]
            for step in case["probes"]:
                execute(step)
                step["after"] = snapshot()
                step["related_after"] = [
                    {"statement": PARENT_READ, "output": snapshot(PARENT_READ)}
                ]
            final = snapshot()
            for step in case["probes"]:
                if (
                    step["sqlstate"] is not None
                    or step.get("repeat_on_reopen")
                    or "reopen" in step
                ):
                    execute(step, replay=True)
                    if snapshot() != final:
                        raise ValueError(
                            "Reopened rejection/conditional probe changed rows"
                        )
            for step in restarted:
                execute(step)
                step["after"] = snapshot()
                step["related_after"] = [
                    {"statement": PARENT_READ, "output": snapshot(PARENT_READ)}
                ]
            # A separate rollback scope proves transaction-time equality across
            # statements without changing the independently recorded postimages.
            with db.transaction(force_rollback=True):
                for step in clock_checks:
                    step["output"] = snapshot(
                        step["statement"],
                        read=not step["statement"].startswith("INSERT "),
                    )
                    if step["output"]["rows"] != [["true"]]:
                        raise ValueError("Transaction clock default lost its owner")
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL",
        "typed_oids": typed_oids,
        "clock_checks": clock_checks,
        "cases": [case],
        "restart_probes": restarted,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("combined table lifecycle PostgreSQL drift")
        print(
            "verified original combined CREATE, typed defaults, constraints, cascades, UUIDv7 evolution and postimages"
        )
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
