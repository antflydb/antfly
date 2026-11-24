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

"""PostgreSQL 18+ witness for exact CLUSTER originals and physical heap order."""

import argparse
import json
from pathlib import Path

from generate_sql_document_alias_reference import output
from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    originals = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    setup = [
        "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT,body TEXT)",
        "CREATE INDEX usage_records_status_idx ON usage_records(status)",
        "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000001','zulu','first'),('00000000-0000-0000-0000-000000000004',NULL,'fourth'),('00000000-0000-0000-0000-000000000003','mike','third'),('00000000-0000-0000-0000-000000000002','alpha','second')",
    ]
    query = "SELECT id::text AS id,status,body FROM usage_records ORDER BY id"
    physical = "SELECT id::text AS id,status FROM usage_records ORDER BY ctid"
    after_writes = [
        "UPDATE usage_records SET status='bravo',body='updated' WHERE id='00000000-0000-0000-0000-000000000001'",
        "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000004'",
        "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000005','delta','fifth')",
    ]
    cases = []
    with postgres() as db:
        notices = []
        db.add_notice_handler(lambda notice: notices.append(notice.message_primary))
        for identity in ("sql-0078", "sql-0079"):
            for statement in setup:
                db.execute(statement)
            original = originals[identity]
            before = output(db.execute(query))
            heap_before = output(db.execute(physical))
            table = db.execute(
                "SELECT oid,relfilenode FROM pg_class WHERE oid='usage_records'::regclass"
            ).fetchone()
            index = db.execute(
                "SELECT oid FROM pg_class WHERE oid='usage_records_status_idx'::regclass"
            ).fetchone()[0]
            notices.clear()
            tag = db.execute(original["sql"]).statusmessage
            verbose = any("clustering" in text.lower() for text in notices)
            assert tag == "CLUSTER"
            assert verbose == (identity == "sql-0079")
            after = output(db.execute(query))
            heap_after = output(db.execute(physical))
            assert after == before
            assert heap_after != heap_before
            assert [row[0] for row in heap_after["rows"]] == [
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000003",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000004",
            ]
            replacement = db.execute(
                "SELECT oid,relfilenode FROM pg_class WHERE oid='usage_records'::regclass"
            ).fetchone()
            assert replacement[0] == table[0] and replacement[1] != table[1]
            assert (
                db.execute(
                    "SELECT oid FROM pg_class WHERE oid='usage_records_status_idx'::regclass"
                ).fetchone()[0]
                == index
            )
            assert db.execute(
                "SELECT indisclustered FROM pg_index WHERE indexrelid='usage_records_status_idx'::regclass"
            ).fetchone() == (True,)
            for statement in after_writes:
                db.execute(statement)
            changed = output(db.execute(query))
            preferred_tag = db.execute("CLUSTER usage_records").statusmessage
            assert preferred_tag == "CLUSTER"
            reclustered = output(db.execute(physical))
            assert [row[0] for row in reclustered["rows"]] == [
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000005",
                "00000000-0000-0000-0000-000000000003",
            ]
            assert output(db.execute(query)) == changed
            cases.append(
                {
                    **{k: original[k] for k in ("id", "source_entry_sha256", "sql")},
                    "setup": setup,
                    "query": query,
                    "output": before,
                    "command_tag": tag,
                    "verbose_notice": verbose,
                    "physical_order": heap_after,
                    "stable_table_and_index_identity": True,
                    "new_physical_generation": True,
                    "preferred_index": "usage_records_status_idx",
                    "after_writes": after_writes,
                    "changed_output": changed,
                    "preferred_recluster": "CLUSTER usage_records",
                    "reclustered_physical_order": reclustered,
                }
            )
            db.execute("DROP TABLE usage_records")
    return {
        "format": 1,
        "reference": "PostgreSQL exact CLUSTER and physical heap order",
        "cases": cases,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output", type=Path)
    target.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        assert json.loads(args.check.read_text()) == result, (
            "PostgreSQL CLUSTER reference drift"
        )
        print(
            "verified 2 exact CLUSTER originals, physical order, preferred-index reuse and future writes"
        )


if __name__ == "__main__":
    main()
