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
import json
from decimal import Decimal

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
    profiles.extend(
        [
            (
                "fk_child_then_parent",
                "WITH added AS (INSERT INTO fk_children VALUES ('new-child','new-parent','new') RETURNING parent_id) INSERT INTO fk_parents SELECT parent_id FROM added RETURNING id",
            ),
            (
                "fk_delete_then_child_update",
                "WITH removed AS (DELETE FROM fk_parents WHERE id='a' RETURNING id) UPDATE fk_children SET status='seen' WHERE parent_id IN(SELECT id FROM removed) RETURNING id,status",
            ),
            (
                "fk_delete_then_parent_reinsert",
                "WITH removed AS (DELETE FROM fk_parents WHERE id='a' RETURNING id) INSERT INTO fk_parents SELECT id FROM removed RETURNING id",
            ),
            (
                "referenced_own_insert_upsert",
                "WITH added AS (INSERT INTO fk_parents VALUES ('new') RETURNING id) INSERT INTO fk_parents SELECT id FROM added ON CONFLICT(id) DO UPDATE SET id=excluded.id RETURNING id",
            ),
            (
                "referenced_own_insert_do_nothing",
                "WITH added AS (INSERT INTO fk_parents VALUES ('new') RETURNING id) INSERT INTO fk_parents SELECT id FROM added ON CONFLICT(id) DO NOTHING RETURNING id",
            ),
            (
                "shared_read_with_mutation",
                "WITH old AS MATERIALIZED (SELECT id,status FROM source_records), changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT a.id,a.status,b.status FROM old a JOIN old b USING(id) JOIN changed USING(id) ORDER BY a.id",
            ),
            (
                "inlined_read_with_mutation",
                "WITH old AS NOT MATERIALIZED (SELECT id,status FROM source_records), changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT old.id,old.status FROM old JOIN changed USING(id) ORDER BY old.id",
            ),
            (
                "volatile_read_not_materialized",
                "WITH sampled AS NOT MATERIALIZED (SELECT id,nextval('effect_sequence') v FROM source_records), changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT a.id,a.v,b.v FROM sampled a JOIN sampled b USING(id) JOIN changed USING(id) ORDER BY a.id",
            ),
            (
                "recursive_read_with_mutation",
                "WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<3), changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT n FROM numbers ORDER BY n",
            ),
            (
                "read_shadow_scope",
                "WITH outer_read AS (SELECT id FROM source_records), changed AS (UPDATE source_records SET status='ready' RETURNING id) SELECT old.id FROM (WITH outer_read AS (SELECT id FROM archived_records) SELECT id FROM outer_read) old JOIN changed USING(id)",
            ),
        ]
    )
    profiles.extend(
        [
            (
                "plan_insert_source_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) INSERT INTO archived_records(id,v) SELECT id,1/0 FROM changed",
            ),
            (
                "plan_insert_values_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) INSERT INTO archived_records(id,v) VALUES ('new',1/0)",
            ),
            (
                "plan_update_false_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) UPDATE usage_records SET v=1/0 WHERE false",
            ),
            (
                "plan_select_limit_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) SELECT 1/0 FROM changed LIMIT 0",
            ),
            (
                "plan_unused_read_error",
                "WITH unused AS (SELECT 1/0), changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) SELECT id FROM changed",
            ),
            (
                "plan_inactive_merge_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) MERGE INTO usage_records t USING changed s ON t.id=s.id WHEN MATCHED AND false THEN UPDATE SET v=1/0",
            ),
            (
                "plan_false_conflict_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id) INSERT INTO usage_records(id,v) SELECT id,2 FROM changed ON CONFLICT(id) DO UPDATE SET v=1/0 WHERE false RETURNING id",
            ),
            (
                "plan_false_returning_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id), invalid AS (UPDATE usage_records SET v=2 WHERE false RETURNING 1/0) SELECT 7",
            ),
            (
                "updated_owner_upsert",
                "WITH changed AS (UPDATE source_records SET v=v+1 RETURNING id) INSERT INTO source_records(id,v) SELECT id,2 FROM changed ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v",
            ),
            (
                "updated_owner_false_upsert",
                "WITH changed AS (UPDATE source_records SET v=v+1 RETURNING id) INSERT INTO source_records(id,v) SELECT id,2 FROM changed ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE false RETURNING id,v",
            ),
            (
                "updated_owner_do_nothing",
                "WITH changed AS (UPDATE source_records SET v=v+1 RETURNING id) INSERT INTO source_records(id,v) SELECT id,2 FROM changed ON CONFLICT(id) DO NOTHING RETURNING id,v",
            ),
            (
                "retired_owner_upsert",
                "WITH removed AS (DELETE FROM source_records RETURNING id) INSERT INTO source_records(id,v) SELECT id,2 FROM removed ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v",
            ),
            (
                "retired_owner_do_nothing",
                "WITH removed AS (DELETE FROM source_records RETURNING id) INSERT INTO source_records(id,v) SELECT id,2 FROM removed ON CONFLICT(id) DO NOTHING RETURNING id,v",
            ),
            (
                "mutation_error_interleaving",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING v) SELECT v/(v-v) FROM changed",
            ),
        ]
    )
    profiles.extend(
        [
            (
                "ordered_returning_error",
                "WITH changed AS (UPDATE source_records SET v=v RETURNING nextval('effect_sequence') AS n) SELECT n/(n-n) FROM changed",
            ),
            (
                "ordered_delete_returning_error",
                "WITH changed AS (DELETE FROM source_records RETURNING nextval('effect_sequence') AS n) SELECT n/(n-n) FROM changed",
            ),
            (
                "ordered_mutation_limit_one",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING v) SELECT v FROM changed LIMIT 1",
            ),
            (
                "ordered_mutation_limit_zero",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING v) SELECT v FROM changed LIMIT 0",
            ),
            (
                "ordered_unused_no_returning",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence')) SELECT 7",
            ),
            (
                "ordered_shared_returning",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id,v) SELECT a.id,a.v,b.v FROM changed a JOIN changed b USING(id) ORDER BY a.id",
            ),
            (
                "ordered_root_returning_error",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) UPDATE source_records SET v=nextval('effect_sequence') RETURNING v/(v-v)",
            ),
            (
                "ordered_root_returning",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) UPDATE source_records SET v=nextval('effect_sequence') RETURNING v",
            ),
        ]
    )
    profiles.extend(
        [
            (
                "ordered_insert_values_error",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('d',nextval('effect_sequence')) RETURNING v) SELECT v/(v-v) FROM added",
            ),
            (
                "ordered_insert_returning_error",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',1),('d',2) RETURNING nextval('effect_sequence') n) SELECT n/(n-n) FROM added",
            ),
            (
                "ordered_insert_source_error",
                "WITH added AS (INSERT INTO archived_records(id,v) SELECT id,nextval('effect_sequence') FROM source_records RETURNING v) SELECT v/(v-v) FROM added",
            ),
            (
                "ordered_insert_source_returning_error",
                "WITH added AS (INSERT INTO archived_records(id,v) SELECT id,v FROM source_records RETURNING nextval('effect_sequence') n) SELECT n/(n-n) FROM added",
            ),
            (
                "ordered_insert_limit_zero",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('d',nextval('effect_sequence')) RETURNING v) SELECT v FROM added LIMIT 0",
            ),
            (
                "ordered_insert_limit_one",
                "WITH added AS (INSERT INTO archived_records(id,v) SELECT id,nextval('effect_sequence') FROM source_records RETURNING v) SELECT v FROM added LIMIT 1",
            ),
            (
                "ordered_insert_unused",
                "WITH added AS (INSERT INTO archived_records(id,v) SELECT id,nextval('effect_sequence') FROM source_records) SELECT 7",
            ),
            (
                "ordered_insert_root_error",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('d',nextval('effect_sequence')) RETURNING v/(v-v)",
            ),
            (
                "ordered_insert_root",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) INSERT INTO archived_records(id,v) SELECT id,nextval('effect_sequence') FROM source_records RETURNING v",
            ),
            (
                "ordered_insert_duplicate_error",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('c',nextval('effect_sequence')),('d',nextval('effect_sequence')) RETURNING v) SELECT v FROM added",
            ),
            (
                "ordered_insert_source_duplicate_error",
                "WITH added AS (INSERT INTO archived_records(id,v) SELECT 'c',nextval('effect_sequence') FROM source_records RETURNING v) SELECT v FROM added",
            ),
        ]
    )
    profiles.extend(
        [
            (
                "ordered_merge_update_error",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v) SELECT v/(v-v) FROM changed",
            ),
            (
                "ordered_merge_returning_error",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=t.v RETURNING nextval('effect_sequence') n) SELECT n/(n-n) FROM changed",
            ),
            (
                "ordered_merge_delete_error",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN DELETE RETURNING nextval('effect_sequence') n) SELECT n/(n-n) FROM changed",
            ),
            (
                "ordered_merge_insert_error",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('c'),('d')) s(k) ON t.id=s.k WHEN NOT MATCHED THEN INSERT (id,v) VALUES(s.k,nextval('effect_sequence')) RETURNING t.v) SELECT v/(v-v) FROM changed",
            ),
            (
                "ordered_merge_limit_zero",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v) SELECT v FROM changed LIMIT 0",
            ),
            (
                "ordered_merge_limit_one",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v) SELECT v FROM changed LIMIT 1",
            ),
            (
                "ordered_merge_unused",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence')) SELECT 7",
            ),
            (
                "ordered_merge_root_error",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v/(t.v-t.v)",
            ),
            (
                "ordered_merge_root",
                "WITH unused AS (UPDATE archived_records SET v=v WHERE false) MERGE INTO source_records t USING (VALUES('a'),('b')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v",
            ),
            (
                "ordered_merge_duplicate_error",
                "WITH changed AS (MERGE INTO source_records t USING (VALUES('a'),('a')) s(k) ON t.id=s.k WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v) SELECT v FROM changed",
            ),
            (
                "ordered_same_command_merge_error",
                "WITH changed AS (UPDATE source_records SET v=nextval('effect_sequence') RETURNING id,v) MERGE INTO source_records t USING changed s ON t.id=s.id WHEN MATCHED THEN UPDATE SET v=nextval('effect_sequence') RETURNING t.v",
            ),
            (
                "ordered_insert_duplicate_nothing",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('c',nextval('effect_sequence')) ON CONFLICT(id) DO NOTHING RETURNING v) SELECT v FROM added",
            ),
            (
                "ordered_insert_duplicate_update",
                "WITH added AS (INSERT INTO archived_records(id,v) VALUES ('c',nextval('effect_sequence')),('c',nextval('effect_sequence')) ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING v) SELECT v FROM added",
            ),
        ]
    )
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
            db.execute("CREATE TABLE fk_parents(id text PRIMARY KEY)")
            db.execute(
                "CREATE TABLE fk_children(id text PRIMARY KEY,parent_id text REFERENCES fk_parents(id) ON DELETE CASCADE,status text)"
            )
            db.execute("INSERT INTO fk_parents VALUES ('a'),('b')")
            db.execute(
                "INSERT INTO fk_children VALUES ('ca','a','old'),('cb','b','old')"
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
            entry["fk_tables"] = {
                "fk_parents": value(
                    db.execute("SELECT id FROM fk_parents ORDER BY id").fetchall()
                ),
                "fk_children": value(
                    db.execute(
                        "SELECT id,parent_id,status FROM fk_children ORDER BY id"
                    ).fetchall()
                ),
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
