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

"""Independent PostgreSQL database-default lifetime and precedence oracle.

Executes sql-0107 unchanged. PostgreSQL's placeholder GUC needs explicit base
values to witness Antfly's registered typed custom-setting base; those setup
values are not source-original SQL and earn no independent case credit.
"""

import json
from contextlib import ExitStack

from generate_sql_postgres_reference import FIXTURES, postgres


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    originals = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    original = next(row for row in originals if row["id"] == "sql-0107")
    with postgres() as db, ExitStack() as connections:
        db.execute("CREATE DATABASE tenant_ops")
        db.execute("CREATE DATABASE other_ops")
        db.execute("CREATE ROLE app_writer LOGIN")
        for database in ("tenant_ops", "other_ops"):
            db.execute(f"ALTER DATABASE {database} SET app.mode = 'base'")

        def login(database="tenant_ops"):
            return connections.enter_context(
                psycopg.connect(
                    db.info.dsn, user="app_writer", dbname=database, autocommit=True
                )
            )

        def value(connection, name="app.mode"):
            return connection.execute("SELECT current_setting(%s)", (name,)).fetchone()[
                0
            ]

        old = login()
        equal(value(old), "base")
        equal(db.execute(original["sql"]).statusmessage, "ALTER DATABASE")
        equal(
            db.execute(
                "SELECT unnest(setconfig) FROM pg_db_role_setting s "
                "JOIN pg_database d ON d.oid=s.setdatabase "
                "WHERE d.datname='tenant_ops' AND setrole=0 ORDER BY 1"
            ).fetchall(),
            [("TimeZone=UTC",), ("app.mode=base",)],
        )
        db.execute("ALTER DATABASE tenant_ops SET app.mode TO 'database'")
        fresh = login()
        equal(value(old), "base")
        equal(value(fresh), "database")
        equal(value(login("other_ops")), "base")
        fresh.execute("SET app.mode TO 'session'")
        equal(value(fresh), "session")
        fresh.execute("BEGIN")
        fresh.execute("SET LOCAL app.mode TO 'transaction'")
        equal(value(fresh), "transaction")
        fresh.execute("ROLLBACK")
        equal(value(fresh), "session")
        fresh.execute("RESET app.mode")
        equal(value(fresh), "database")
        db.execute("ALTER ROLE app_writer SET app.mode TO 'role'")
        equal(value(login()), "role")
        equal(value(login("other_ops")), "role")
        db.execute(
            "ALTER ROLE app_writer IN DATABASE tenant_ops SET app.mode TO 'role-database'"
        )
        equal(value(login()), "role-database")
        equal(value(login("other_ops")), "role")
        db.execute("ALTER DATABASE tenant_ops SET app.mode TO 'new-database'")
        equal(value(login()), "role-database")
        equal(value(fresh), "database")
        db.execute("ALTER ROLE app_writer IN DATABASE tenant_ops RESET app.mode")
        equal(value(login()), "role")
        db.execute("ALTER ROLE app_writer RESET app.mode")
        equal(value(login()), "new-database")
        db.execute("ALTER DATABASE tenant_ops RESET app.mode")
        # A registered setting would fall back to its definition base. In
        # PostgreSQL this fresh placeholder has no remaining source at all.
        reset = login()
        equal(
            reset.execute("SELECT current_setting('app.mode',true)").fetchone()[0], None
        )
        equal(value(fresh), "database")
        db.execute("ALTER DATABASE tenant_ops SET app.quota TO 17")
        db.execute("ALTER DATABASE tenant_ops SET app.enabled TO true")
        db.execute("ALTER DATABASE tenant_ops SET statement_timeout TO '1000ms'")
        typed = login()
        equal(value(typed, "app.quota"), "17")
        equal(value(typed, "app.enabled"), "true")
        equal(value(typed, "statement_timeout"), "1s")
        db.execute("ALTER DATABASE tenant_ops RESET ALL")
        db.execute("ALTER DATABASE tenant_ops RESET ALL")
        empty = login()
        equal(
            empty.execute("SELECT current_setting('app.quota',true)").fetchone()[0],
            None,
        )
        equal(
            empty.execute("SELECT current_setting('app.enabled',true)").fetchone()[0],
            None,
        )
        equal(value(typed, "app.quota"), "17")
        equal(value(login("other_ops")), "base")
        for sql, code, actor in (
            ("ALTER DATABASE absent SET app.mode TO 'missing'", "3D000", db),
            ("ALTER DATABASE tenant_ops SET statement_timeout TO '-1'", "22023", db),
            ("ALTER DATABASE tenant_ops SET app.mode TO 'forged'", "42501", old),
        ):
            try:
                actor.execute(sql)
            except psycopg.Error as error:
                equal(error.sqlstate, code)
            else:
                raise AssertionError(("missing rejection", sql))
        connections.close()
        db.execute("ALTER DATABASE tenant_ops SET app.mode TO 'renamed-default'")
        db.execute("ALTER DATABASE tenant_ops RENAME TO renamed_ops")
        equal(value(login("renamed_ops")), "renamed-default")
        connections.close()
        db.execute("DROP DATABASE renamed_ops")
        db.execute("CREATE DATABASE renamed_ops")
        equal(
            login("renamed_ops")
            .execute("SELECT current_setting('app.mode',true)")
            .fetchone()[0],
            None,
        )
    print(
        f"Verified {assertions} PostgreSQL database-default scope, login, precedence and RESET assertions"
    )


if __name__ == "__main__":
    check()
