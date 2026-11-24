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

"""Original policy DDL with enabled-table edits, default denial and write gates."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

CREATE = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,tenant_id TEXT,status TEXT)"
SEED = "INSERT INTO usage_records(id,tenant_id,status) VALUES ('a','tenant-a','active'),('b','tenant-a','ready'),('c','tenant-b','active'),('d','tenant-b','blocked'),('e',NULL,'active'),('f','tenant-a',NULL),('g','tenant-b','ACTIVE')"
READ = "SELECT id,tenant_id,status FROM usage_records ORDER BY id"
ENABLE = "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY"
DISABLE = "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY"


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    originals = {entry["id"]: entry for entry in inventory["entries"]}
    cases = []
    with postgres() as db:
        db.execute("CREATE ROLE app_reader")
        db.execute('CREATE ROLE "pk-reader"')
        db.execute('CREATE ROLE "pk-rewriter"')
        db.execute('GRANT app_reader TO "pk-reader"')
        for number in range(321, 332):
            identity = f"sql-{number:04}"
            with db.transaction(force_rollback=True):
                db.execute(CREATE)
                db.execute(SEED)
                db.execute(
                    'GRANT SELECT,INSERT,UPDATE,DELETE ON usage_records TO "pk-reader"'
                )
                setup = []
                if number in {321, 322}:
                    setup.append(
                        "CREATE POLICY fixture_policy ON usage_records USING (tenant_id = 'tenant-a')"
                    )
                elif number in {329, 331}:
                    setup.append(
                        "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = 'tenant-b')"
                    )
                if number != 321:
                    setup.append(ENABLE)
                for statement in setup:
                    db.execute(statement)
                db.execute(
                    'CREATE POLICY fixture_audit_read ON usage_records TO "pk-rewriter" USING (true)'
                )
                command_tag = db.execute(originals[identity]["sql"]).statusmessage
                all_rows = [list(row) for row in db.execute(READ).fetchall()]
                db.execute('SET LOCAL ROLE "pk-reader"')
                db.execute("SET LOCAL app.tenant_id = 'tenant-a'")
                rows = [list(row) for row in db.execute(READ).fetchall()]
                filtered = [
                    list(row)
                    for row in db.execute(
                        READ.replace(" ORDER BY", " WHERE status = 'active' ORDER BY")
                    ).fetchall()
                ]
                probes = []
                for statement in [
                    "INSERT INTO usage_records(id,tenant_id,status) VALUES ('p','tenant-a','active')",
                    "INSERT INTO usage_records(id,tenant_id,status) VALUES ('q','tenant-b','blocked')",
                    "INSERT INTO usage_records(id,tenant_id,status) VALUES ('r','tenant-b','active')",
                    "UPDATE usage_records SET tenant_id = 'tenant-b',status = 'blocked' WHERE id = 'a'",
                    "UPDATE usage_records SET tenant_id = 'tenant-b',status = 'active' WHERE id = 'a'",
                    "INSERT INTO usage_records(id,tenant_id,status) VALUES ('p','tenant-a','active') RETURNING id,tenant_id,status",
                    "INSERT INTO usage_records(id,tenant_id,status) VALUES ('r','tenant-b','active') RETURNING id,tenant_id,status",
                ]:
                    code = None
                    returning = None
                    with db.transaction(force_rollback=True):
                        try:
                            cursor = db.execute(statement)
                            if cursor.description is not None:
                                returning = [list(row) for row in cursor.fetchall()]
                        except psycopg.Error as error:
                            code = error.sqlstate
                    probes.append(
                        {"statement": statement, "sqlstate": code, "rows": returning}
                    )
                cases.append(
                    {
                        "id": identity,
                        "command_tag": command_tag,
                        "setup": setup,
                        "rows": rows,
                        "all_rows": all_rows,
                        "filtered_rows": filtered,
                        "probes": probes,
                    }
                )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL",
        "create": CREATE,
        "seed": SEED,
        "read": READ,
        "cases": cases,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("policy lifecycle PostgreSQL drift")
        print(
            "verified 11 original policy DDL contracts, role-selected rows and rollback-only write probes"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
