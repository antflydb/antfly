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

"""Owned PostgreSQL witnesses for the unchanged publication DDL originals."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    originals = {
        c["id"]: c
        for c in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres(logical_replication=True) as db:
        for name in ("usage_records", "usage_events"):
            db.execute(f"CREATE TABLE {name}(id integer PRIMARY KEY,payload text)")
        assert db.execute("SHOW wal_level").fetchone()[0] == "logical"

        def observe():
            rows = db.execute(
                "SELECT pubname,puballtables,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication ORDER BY pubname"
            ).fetchall()
            return [
                {
                    "name": row[0],
                    "all_tables": row[1],
                    "insert": row[2],
                    "update": row[3],
                    "delete": row[4],
                    "truncate": row[5],
                    "tables": [
                        r[0]
                        for r in db.execute(
                            "SELECT tablename FROM pg_publication_tables WHERE pubname=%s ORDER BY tablename",
                            (row[0],),
                        ).fetchall()
                    ],
                }
                for row in rows
            ]

        identity = None
        for i in range(114, 119):
            case = originals[f"sql-{i:04d}"]
            assert not case["params"]
            cursor = db.execute(case["sql"])
            state = observe()
            if i == 114:
                identity = db.execute(
                    "SELECT oid FROM pg_publication WHERE pubname='usage_pub'"
                ).fetchone()[0]
                assert state[0]["tables"] == ["usage_records"]
            if i == 116:
                assert (
                    db.execute(
                        "SELECT oid FROM pg_publication WHERE pubname='usage_pub'"
                    ).fetchone()[0]
                    == identity
                )
                assert next(p for p in state if p["name"] == "usage_pub")["tables"] == [
                    "usage_events",
                    "usage_records",
                ]
            if i >= 117:
                assert [p["name"] for p in state] == ["usage_pub_all"]
            entries.append(
                {
                    "id": case["id"],
                    "sql": case["sql"],
                    "command_tag": cursor.statusmessage,
                    "publications": state,
                }
            )
        all_identity = db.execute(
            "SELECT oid FROM pg_publication WHERE pubname='usage_pub_all'"
        ).fetchone()[0]
        future_create = (
            "CREATE TABLE usage_future (id integer PRIMARY KEY, payload text)"
        )
        future_insert = (
            "INSERT INTO usage_future (id, payload) VALUES (3, 'captured-future')"
        )
        db.execute(future_create)
        db.execute(future_insert)
        future_tables = [
            r[0]
            for r in db.execute(
                "SELECT tablename FROM pg_publication_tables WHERE pubname='usage_pub_all' ORDER BY tablename"
            ).fetchall()
        ]
        assert future_tables == ["usage_events", "usage_future", "usage_records"]
        assert (
            db.execute(
                "SELECT oid FROM pg_publication WHERE pubname='usage_pub_all'"
            ).fetchone()[0]
            == all_identity
        )
        db.execute("CREATE PUBLICATION usage_pub FOR TABLE usage_records")
        recreated = db.execute(
            "SELECT oid FROM pg_publication WHERE pubname='usage_pub'"
        ).fetchone()[0]
        assert recreated != identity
        return {
            "entries": entries,
            "future_create": future_create,
            "future_insert": future_insert,
            "future_tables": future_tables,
            "future_preserves_publication_identity": True,
            "alter_preserves_identity": True,
            "recreation_uses_new_identity": True,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    actual = reference()
    if args.check:
        expected = json.loads(args.check.read_text())
        if actual != expected:
            raise SystemExit("publication PostgreSQL witness changed")
        print(
            f"PostgreSQL publication witness verified: {len(actual['entries'])} originals"
        )
    else:
        print(json.dumps(actual, indent=2))


if __name__ == "__main__":
    main()
