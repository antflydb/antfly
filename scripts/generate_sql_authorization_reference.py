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

"""Qualify unchanged SQL authorization originals and their PostgreSQL postimages."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
IDS = ["sql-0342"] + [f"sql-{i:04}" for i in range(332, 342)] + ["sql-0350"]
SETUP = [
    "CREATE ROLE app_owner",
    "CREATE ROLE readonly",
    "CREATE ROLE reporting",
    "CREATE ROLE app_reader",
    "CREATE TABLE usage_records (id BIGINT PRIMARY KEY, amount BIGINT)",
]


def postimage(db):
    return {
        "memberships": db.execute("""
            SELECT r.rolname, m.rolname, a.admin_option, a.inherit_option, a.set_option
            FROM pg_auth_members a
            JOIN pg_roles r ON r.oid=a.roleid
            JOIN pg_roles m ON m.oid=a.member
            WHERE r.rolname IN ('readonly','reporting')
            ORDER BY 1,2,3,4,5
        """).fetchall(),
        "table_grants": db.execute("""
            SELECT p.rolname, x.privilege_type, x.is_grantable
            FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) x
            JOIN pg_roles p ON p.oid=x.grantee
            WHERE c.oid='usage_records'::regclass AND p.rolname='app_writer'
            ORDER BY 1,2,3
        """).fetchall(),
        "defaults": db.execute("""
            SELECT owner.rolname,n.nspname,p.rolname,x.privilege_type,x.is_grantable
            FROM pg_default_acl d
            JOIN pg_roles owner ON owner.oid=d.defaclrole
            JOIN pg_namespace n ON n.oid=d.defaclnamespace
            CROSS JOIN LATERAL aclexplode(d.defaclacl) x
            JOIN pg_roles p ON p.oid=x.grantee
            WHERE owner.rolname='app_owner' ORDER BY 1,2,3,4,5
        """).fetchall(),
        "writer_exists": db.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='app_writer')"
        ).fetchone()[0],
    }


def identity_defaults(db):
    """Supplemental grant/RLS lifecycle, not additional inventory cases."""
    from generate_sql_population_reference import restart_persistence_server

    def execute(connection, statement, reader=False):
        if reader:
            connection.execute('SET ROLE "pk-reader"')
        try:
            cursor = connection.execute(statement)
            return {"tag": cursor.statusmessage, "sqlstate": None}
        except Exception as error:
            code = getattr(error, "sqlstate", None)
            if code is None:
                raise
            return {"tag": None, "sqlstate": code}
        finally:
            if reader:
                connection.execute("RESET ROLE")

    def counter(connection):
        return connection.execute(
            "SELECT last_value FROM identity_acl_probe_id_seq"
        ).fetchone()[0]

    def ids(connection):
        return [
            row[0]
            for row in connection.execute(
                "SELECT id FROM identity_acl_probe ORDER BY id"
            )
        ]

    for statement in (
        'CREATE ROLE "pk-reader"',
        'GRANT USAGE ON SCHEMA public TO "pk-reader"',
        "CREATE ROLE identity_writer",
        "CREATE TABLE identity_acl_probe (id BIGINT GENERATED ALWAYS AS IDENTITY, amount BIGINT)",
        "GRANT INSERT, UPDATE ON identity_acl_probe TO identity_writer",
        'GRANT identity_writer TO "pk-reader"',
    ):
        db.execute(statement)
    result = {}
    result["insert"] = execute(
        db, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1", True
    )
    result["update"] = execute(
        db, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1", True
    )
    result["after_update_ids"] = ids(db)
    result["after_update_counter"] = counter(db)
    result["explicit_sequence"] = execute(
        db, "SELECT nextval('identity_acl_probe_id_seq')", True
    )
    result["read"] = execute(db, "SELECT id FROM identity_acl_probe", True)
    from psycopg import sql

    observer = db.execute("SELECT current_user").fetchone()[0]
    db.execute(sql.SQL("GRANT identity_writer TO {}").format(sql.Identifier(observer)))
    db.execute(
        "CREATE POLICY identity_acl_write ON identity_acl_probe TO identity_writer USING (amount=10) WITH CHECK (amount=10)"
    )
    db.execute("ALTER TABLE identity_acl_probe ENABLE ROW LEVEL SECURITY")
    result["rls_denied_insert"] = execute(
        db, "INSERT INTO identity_acl_probe(amount) VALUES(20) RETURNING 1", True
    )
    result["rls_insert"] = execute(
        db, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1", True
    )
    db.execute("REVOKE INSERT, UPDATE ON identity_acl_probe FROM identity_writer")
    result["revoked_insert"] = execute(
        db, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1", True
    )
    result["revoked_update"] = execute(
        db, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1", True
    )
    result["before_restart_ids"] = ids(db)
    result["before_restart_counter"] = counter(db)
    db.execute("GRANT INSERT, UPDATE ON identity_acl_probe TO identity_writer")
    directory = Path(db.execute("SHOW data_directory").fetchone()[0]).resolve()
    if directory.parent != Path(db.info.host).resolve():
        raise RuntimeError("refusing to restart a foreign PostgreSQL server")
    dsn = db.info.dsn
    db.close()
    with restart_persistence_server(dsn, directory, "fast") as resumed:
        result["recovered_counter"] = counter(resumed)
        result["cold_insert"] = execute(
            resumed,
            "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1",
            True,
        )
        result["after_restart_ids"] = ids(resumed)
        result["explicit_sequence_after_restart"] = execute(
            resumed, "SELECT nextval('identity_acl_probe_id_seq')", True
        )
        resumed.execute('REVOKE identity_writer FROM "pk-reader"')
        result["membership_revoked_insert"] = execute(
            resumed,
            "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1",
            True,
        )
        result["membership_revoked_update"] = execute(
            resumed, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1", True
        )
        result["after_restart_counter"] = counter(resumed)
        resumed.execute(
            sql.SQL("REVOKE identity_writer FROM {}").format(sql.Identifier(observer))
        )
        for statement in (
            "ALTER TABLE identity_acl_probe DISABLE ROW LEVEL SECURITY",
            "DROP POLICY identity_acl_write ON identity_acl_probe",
            "DROP TABLE identity_acl_probe",
            "DROP ROLE identity_writer",
        ):
            resumed.execute(statement)
    return result


def reference():
    originals = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    with postgres() as db:
        for sql in SETUP:
            db.execute(sql)
        steps = []
        for identity in IDS:
            sql = originals[identity]["sql"]
            if identity == "sql-0350":
                # A durable default grant is a dependency even after all
                # ordinary table privileges and memberships have retired.
                try:
                    db.execute(sql)
                except Exception as error:
                    if getattr(error, "sqlstate", None) != "2BP01":
                        raise
                    steps.append(
                        {
                            "id": identity,
                            "sql": sql,
                            "sqlstate": "2BP01",
                            "postimage": postimage(db),
                        }
                    )
                else:
                    raise AssertionError("DROP ROLE ignored its default ACL dependency")
                db.execute(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public REVOKE ALL PRIVILEGES ON TABLES FROM app_writer CASCADE"
                )
            cursor = db.execute(sql)
            steps.append(
                {
                    "id": identity,
                    "sql": sql,
                    "command_tag": cursor.statusmessage,
                    "postimage": postimage(db),
                }
            )
        return {"format": 1, "cases": steps, "identity_defaults": identity_defaults(db)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    target = FIXTURES / "sql_authorization_reference.json"
    rendered = json.dumps(reference(), indent=2) + "\n"
    if args.check:
        if target.read_text() != rendered:
            raise SystemExit("SQL authorization reference differs")
    else:
        target.write_text(rendered)


if __name__ == "__main__":
    main()
