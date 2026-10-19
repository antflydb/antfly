#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Qualify original tablespace name lifecycles, not physical location moves."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from generate_sql_postgres_reference import FIXTURES, postgres

IDS = ("sql-0111", "sql-0112", "sql-0113")


def reference():
    from psycopg import sql

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    cases = []
    with postgres() as db:
        for identity in IDS:
            # A real PostgreSQL tablespace needs an empty, server-owned directory
            # and CREATE TABLESPACE cannot run in a rollback transaction. Native
            # setup uses its existing catalog LOCATION; the original RENAME/DROP
            # statement remains byte-for-byte unchanged in both engines.
            with TemporaryDirectory(
                prefix="antfly-pg-tablespace-", dir="/tmp"
            ) as directory:
                before_name = (
                    "fastspace" if identity == "sql-0111" else "fastspace_archive"
                )
                setup = []
                prior = None
                if identity != "sql-0113":
                    db.execute(
                        sql.SQL("CREATE TABLESPACE {} LOCATION {}").format(
                            sql.Identifier(before_name), sql.Literal(directory)
                        )
                    )
                    prior = db.execute(
                        "SELECT oid,pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname=%s",
                        (before_name,),
                    ).fetchone()
                    setup.append(
                        f"CREATE TABLESPACE {before_name} LOCATION '/var/lib/antfly/fastspace'"
                    )
                original = originals[identity]
                assert not original["params"]
                command_tag = db.execute(original["sql"]).statusmessage
                after = db.execute(
                    "SELECT oid,spcname,pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname IN ('fastspace','fastspace_archive') ORDER BY spcname"
                ).fetchall()
                rename = identity == "sql-0111"
                if rename:
                    assert len(after) == 1 and after[0][0] == prior[0]
                    assert (
                        after[0][1] == "fastspace_archive" and after[0][2] == prior[1]
                    )
                else:
                    assert not after
                case = {
                    "id": identity,
                    "setup": setup,
                    "kind": "tablespace",
                    "name": "fastspace_archive",
                    "present": rename,
                    "command_tag": command_tag,
                }
                if rename:
                    case["prior_name"] = before_name
                if identity == "sql-0113":
                    case["unchanged"] = True
                    case["repeat_noop"] = True
                    assert db.execute(original["sql"]).statusmessage == command_tag
                cases.append(case)
                if rename:
                    db.execute("DROP TABLESPACE fastspace_archive")
    return {"format": 1, "entries": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    expected = reference()
    if args.check:
        actual = json.loads(args.check.read_text())
        selected = [entry for entry in actual["entries"] if entry["id"] in IDS]
        if actual["format"] != expected["format"] or selected != expected["entries"]:
            raise SystemExit("tablespace lifecycle PostgreSQL drift")
        print(
            "verified 3 original tablespace name lifecycles and preserved identity/location"
        )
    else:
        print(json.dumps(expected, indent=2))


if __name__ == "__main__":
    main()
