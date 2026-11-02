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

"""PostgreSQL witnesses for atomic data-modifying CTE statement graphs."""

import argparse
from decimal import Decimal
import json

from generate_sql_postgres_reference import FIXTURES, postgres


def value(item):
    if isinstance(item, Decimal):
        return str(item)
    if isinstance(item, (list, tuple)):
        return [value(cell) for cell in item]
    return item


def reference():
    from psycopg import Error

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    original = next(x for x in inventory["entries"] if x["id"] == "sql-0015")
    profiles = [
        ("original_merge_double_write", original["sql"]),
        (
            "disjoint_merge",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id,status) MERGE INTO usage_records t USING changed s ON t.id=s.id WHEN MATCHED THEN UPDATE SET status=s.status",
        ),
        (
            "unused_update",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT 7 AS answer",
        ),
        (
            "zero_demand",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT id FROM changed LIMIT 0",
        ),
        (
            "snapshot_and_returning",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id,status) SELECT old.id,old.status,changed.status FROM source_records old JOIN changed ON old.id=changed.id ORDER BY old.id",
        ),
        (
            "shared_volatile_producer",
            "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id,v) SELECT a.id,a.v,b.v FROM changed a JOIN changed b ON a.id=b.id ORDER BY a.id",
        ),
        (
            "dependent_mutations",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id,status), copied AS (INSERT INTO archived_records(id,status) SELECT id,status FROM changed RETURNING id,status) SELECT id,status FROM copied ORDER BY id",
        ),
        (
            "no_returning_unused",
            "WITH changed AS (UPDATE source_records SET status='ready') SELECT 7 AS answer",
        ),
        (
            "no_returning_reference",
            "WITH changed AS (UPDATE source_records SET status='ready') SELECT * FROM changed",
        ),
        (
            "nested_write",
            "SELECT * FROM (WITH changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT * FROM changed) q",
        ),
        (
            "recursive_write",
            "WITH RECURSIVE changed AS (UPDATE source_records SET status='ready' FROM changed c WHERE source_records.id=c.id RETURNING source_records.id) SELECT * FROM changed",
        ),
        (
            "rollback_all",
            "WITH changed AS (UPDATE source_records SET status='ready' RETURNING id) UPDATE usage_records SET v=1/(v-v) WHERE id IN(SELECT id FROM changed)",
        ),
        (
            "same_target_update",
            "WITH changed AS (UPDATE usage_records SET v=v+1 RETURNING id) UPDATE usage_records SET v=v+10 WHERE id IN(SELECT id FROM changed) RETURNING id,v",
        ),
        (
            "same_target_delete",
            "WITH changed AS (UPDATE usage_records SET v=v+1 RETURNING id) DELETE FROM usage_records WHERE id IN(SELECT id FROM changed) RETURNING id,v",
        ),
        (
            "own_insert_upsert",
            "WITH inserted AS (INSERT INTO archived_records(id,status) VALUES('x','first') RETURNING id) INSERT INTO archived_records(id,status) VALUES('x','second') ON CONFLICT(id) DO UPDATE SET status=excluded.status RETURNING id,status",
        ),
        (
            "own_insert_upsert_false",
            "WITH inserted AS (INSERT INTO archived_records(id,status) VALUES('x','first') RETURNING id) INSERT INTO archived_records(id,status) VALUES('x','second') ON CONFLICT(id) DO UPDATE SET status=excluded.status WHERE false RETURNING id,status",
        ),
        (
            "own_insert_do_nothing",
            "WITH inserted AS (INSERT INTO archived_records(id,status) VALUES('x','first') RETURNING id) INSERT INTO archived_records(id,status) VALUES('x','second') ON CONFLICT(id) DO NOTHING RETURNING id,status",
        ),
        (
            "own_insert_duplicate",
            "WITH inserted AS (INSERT INTO archived_records(id,status) VALUES('x','first') RETURNING id) INSERT INTO archived_records(id,status) VALUES('x','second') RETURNING id,status",
        ),
        (
            "delete_producer",
            "WITH deleted AS (DELETE FROM source_records WHERE id='a' RETURNING id,status) INSERT INTO archived_records(id,status) SELECT id,status FROM deleted RETURNING id,status",
        ),
        (
            "merge_producer",
            "WITH changed AS (MERGE INTO usage_records t USING source_records s ON t.id=s.id WHEN MATCHED THEN UPDATE SET status='merged' RETURNING t.id,t.status) SELECT id,status FROM changed ORDER BY id",
        ),
        (
            "typed_capture",
            "WITH changed AS (UPDATE source_records SET n=n+0.0001 RETURNING id,a,n) SELECT a.id,a.a,a.n,b.n FROM changed a JOIN changed b ON a.id=b.id ORDER BY a.id",
        ),
    ]
    entries = []
    with postgres() as db:
        for ordinal, (name, query) in enumerate(profiles):
            schema = f"effect_case_{ordinal}"
            db.execute(f"CREATE SCHEMA {schema}")
            db.execute(f"SET search_path TO {schema},public")
            db.execute("CREATE SEQUENCE effect_sequence")
            definition = "(id text PRIMARY KEY,status text,v integer DEFAULT 0,a integer[],n numeric(20,4))"
            for table in ("usage_records", "source_records", "archived_records"):
                db.execute("CREATE TABLE " + table + definition)
            for table in ("usage_records", "source_records"):
                db.execute(
                    "INSERT INTO "
                    + table
                    + " VALUES('a','queued',1,ARRAY[1,NULL,3],9007199254740993.0100),('b','closed',2,ARRAY[]::integer[],NULL)"
                )
            entry = {"name": name, "sql": query}
            if ordinal == 0:
                entry.update(
                    {
                        key: original[key]
                        for key in ("id", "source_entry_sha256", "params")
                    }
                )
            try:
                cursor = db.execute(query)
                entry["command_tag"] = cursor.statusmessage
                if cursor.description is not None:
                    entry["columns"] = [
                        {"name": col.name, "oid": col.type_code}
                        for col in cursor.description
                    ]
                    entry["rows"] = value(cursor.fetchall())
            except Error as error:
                entry["sqlstate"] = error.sqlstate
            entry["tables"] = {
                table: value(
                    db.execute(
                        "SELECT id,status,v,a,n FROM " + table + " ORDER BY id"
                    ).fetchall()
                )
                for table in ("usage_records", "source_records", "archived_records")
            }
            entry["sequence"] = value(
                db.execute(
                    "SELECT last_value,is_called FROM effect_sequence"
                ).fetchone()
            )
            entries.append(entry)
            db.execute("SET search_path TO public")
            db.execute(f"DROP SCHEMA {schema} CASCADE")
    return {
        "format": 1,
        "reference": "PostgreSQL 18 data-modifying WITH semantics",
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    target = FIXTURES / "sql_mutation_cte_reference.json"
    expected = reference()
    if args.check:
        assert json.loads(target.read_text()) == expected, (
            "mutation CTE reference drift"
        )
    else:
        target.write_text(json.dumps(expected, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
