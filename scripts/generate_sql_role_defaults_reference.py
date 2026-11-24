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

"""PostgreSQL witnesses for original ALTER ROLE defaults and login semantics."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

IDS = tuple(f"sql-{number:04d}" for number in range(343, 350))


def stored_defaults(db):
    return [
        list(row)
        for row in db.execute(
            "SELECT COALESCE(d.datname,''), unnest(s.setconfig) "
            "FROM pg_db_role_setting s "
            "JOIN pg_roles r ON r.oid=s.setrole "
            "LEFT JOIN pg_database d ON d.oid=s.setdatabase "
            "WHERE r.rolname='app_writer' ORDER BY 1,2"
        )
    ]


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        db.execute("CREATE ROLE app_writer LOGIN")
        db.execute("CREATE DATABASE appdb")
        # Match Antfly's explicitly registered string setting and its base
        # value. This is not an oracle for undeclared extension GUCs.
        db.execute("ALTER DATABASE appdb SET app.tenant_id=''")
        for identity in IDS:
            db.execute("ALTER ROLE app_writer RESET ALL")
            db.execute("ALTER ROLE app_writer IN DATABASE appdb RESET ALL")
            original = originals[identity]
            assert not original["params"]
            timeout = identity in ("sql-0346", "sql-0347", "sql-0348")
            name = "statement_timeout" if timeout else "app.tenant_id"
            if identity in ("sql-0345", "sql-0348"):
                initial = (
                    "ALTER ROLE app_writer SET statement_timeout='1ms'"
                    if timeout
                    else "ALTER ROLE app_writer SET app.tenant_id='acme'"
                )
                db.execute(initial)
            before = stored_defaults(db)
            with psycopg.connect(
                db.info.dsn, user="app_writer", dbname="appdb", autocommit=True
            ) as old_login:
                old_value = old_login.execute(
                    "SELECT current_setting(%s,true)", (name,)
                ).fetchone()[0]
                sqlstate = None
                tag = None
                try:
                    tag = db.execute(original["sql"]).statusmessage
                except psycopg.Error as error:
                    sqlstate = error.sqlstate
                after = stored_defaults(db)
                # ALTER ROLE changes defaults for future connections, not the
                # effective settings of connections which already exist.
                assert (
                    old_login.execute(
                        "SELECT current_setting(%s,true)", (name,)
                    ).fetchone()[0]
                    == old_value
                )
                with psycopg.connect(
                    db.info.dsn, user="app_writer", dbname="appdb", autocommit=True
                ) as new_login:
                    new_value = new_login.execute(
                        "SELECT current_setting(%s,true)", (name,)
                    ).fetchone()[0]
            if identity == "sql-0349":
                assert sqlstate == "42601" and before == after
            else:
                assert tag == "ALTER ROLE" and sqlstate is None
                expected = (
                    ("0" if timeout else "")
                    if identity in ("sql-0345", "sql-0348")
                    else ("1ms" if timeout else "acme")
                )
                assert new_value == expected, (identity, new_value, expected)
            entries.append(
                {
                    "id": identity,
                    "sql": original["sql"],
                    "command_tag": tag,
                    "sqlstate": sqlstate,
                    "before": before,
                    "after": after,
                    "old_login_value": old_value,
                    "new_login_value": new_value,
                }
            )
        # Check the full precedence chain, not merely persisted setconfig.
        db.execute("ALTER DATABASE appdb SET app.tenant_id='database'")
        db.execute("ALTER ROLE app_writer SET app.tenant_id='role'")
        db.execute("ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id='scoped'")
        for expected in ("scoped", "role", "database"):
            with psycopg.connect(
                db.info.dsn, user="app_writer", dbname="appdb", autocommit=True
            ) as login:
                assert login.execute("SHOW app.tenant_id").fetchone()[0] == expected
            if expected == "scoped":
                db.execute(
                    "ALTER ROLE app_writer IN DATABASE appdb RESET app.tenant_id"
                )
            elif expected == "role":
                db.execute("ALTER ROLE app_writer RESET app.tenant_id")
        # Scope follows the immutable database OID through rename and is
        # retired on DROP, not inherited by the next use of the same name.
        db.execute("ALTER ROLE app_writer SET app.tenant_id='acme'")
        db.execute("ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id='scoped'")
        original_oid = db.execute(
            "SELECT oid FROM pg_database WHERE datname='appdb'"
        ).fetchone()[0]
        db.execute("ALTER DATABASE appdb RENAME TO renameddb")
        assert ["renameddb", "app.tenant_id=scoped"] in stored_defaults(db)
        assert (
            db.execute(
                "SELECT oid FROM pg_database WHERE datname='renameddb'"
            ).fetchone()[0]
            == original_oid
        )
        with psycopg.connect(
            db.info.dsn, user="app_writer", dbname="renameddb", autocommit=True
        ) as login:
            assert login.execute("SHOW app.tenant_id").fetchone()[0] == "scoped"
        db.execute("DROP DATABASE renameddb")
        assert all(scope != "renameddb" for scope, _ in stored_defaults(db))
        db.execute("CREATE DATABASE renameddb")
        assert (
            db.execute(
                "SELECT oid FROM pg_database WHERE datname='renameddb'"
            ).fetchone()[0]
            != original_oid
        )
        with psycopg.connect(
            db.info.dsn, user="app_writer", dbname="renameddb", autocommit=True
        ) as login:
            assert login.execute("SHOW app.tenant_id").fetchone()[0] == "acme"
    return {"format": 1, "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    expected = reference()
    if args.check:
        if json.loads(args.check.read_text()) != expected:
            raise SystemExit("role-default PostgreSQL witness drift")
        print("verified 7 original role-default contracts and login precedence")
    else:
        print(json.dumps(expected, indent=2))


if __name__ == "__main__":
    main()
