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

"""Qualify unchanged GIN originals and native campaign postimages on PostgreSQL."""

import json

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres


def check():
    cases = {
        e["id"]: e["sql"]
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    with postgres() as db:
        db.execute(
            "CREATE TABLE usage_records(id UUID PRIMARY KEY,tenant_id TEXT,metadata JSONB,tags TEXT[])"
        )
        db.execute(
            "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000001','a','{\"plan\":\"pro\"}'::jsonb,ARRAY['red','blue']),('00000000-0000-0000-0000-000000000002','b','{\"plan\":\"free\"}'::jsonb,ARRAY['green'])"
        )
        for identity in ("sql-0719", "sql-0721"):
            assert db.execute(cases[identity]).statusmessage == "CREATE INDEX"
        assert db.execute(
            "SELECT c.relname,a.amname,o.opcname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam JOIN pg_opclass o ON o.oid=i.indclass[0] WHERE c.relname IN ('usage_records_metadata_gin','usage_records_tags_gin') ORDER BY c.relname"
        ).fetchall() == [
            ("usage_records_metadata_gin", "gin", "jsonb_path_ops"),
            ("usage_records_tags_gin", "gin", "array_ops"),
        ]
        try:
            with db.transaction():
                db.execute(cases["sql-0720"])
        except psycopg.Error as error:
            assert error.sqlstate == "0A000", error.sqlstate
            assert "included columns" in str(error)
        else:
            raise AssertionError("GIN INCLUDE unexpectedly admitted")
        try:
            with db.transaction():
                db.execute(
                    "CREATE INDEX wrong_class ON usage_records USING gin(tenant_id array_ops)"
                )
        except psycopg.Error as error:
            assert error.sqlstate == "42804", error.sqlstate
        else:
            raise AssertionError("GIN operator class accepted an incompatible column")
        assert db.execute("SELECT to_regclass('wrong_class')").fetchone()[0] is None
        queries = (
            'SELECT id::text FROM usage_records WHERE metadata @> \'{"plan":"pro"}\'::jsonb ORDER BY id',
            "SELECT id::text FROM usage_records WHERE tags @> ARRAY['red'] ORDER BY id",
        )
        for sql in queries:
            assert db.execute(sql).fetchall() == [
                ("00000000-0000-0000-0000-000000000001",)
            ]
        first_row = "SELECT metadata->>'plan',tags[1] FROM usage_records WHERE id='00000000-0000-0000-0000-000000000001'"
        assert db.execute(first_row).fetchall() == [("pro", "red")]
        db.execute(
            "UPDATE usage_records SET metadata='{\"plan\":\"free\"}'::jsonb,tags=ARRAY['green'] WHERE tenant_id='a'"
        )
        db.execute(
            "UPDATE usage_records SET metadata='{\"plan\":\"pro\"}'::jsonb,tags=ARRAY['red'] WHERE tenant_id='b'"
        )
        for sql in queries:
            assert db.execute(sql).fetchall() == [
                ("00000000-0000-0000-0000-000000000002",)
            ]
        assert db.execute(first_row).fetchall() == [("free", "green")]
        db.execute("ALTER TABLE usage_records ADD COLUMN unrelated BIGINT")
        for sql in queries:
            assert db.execute(sql).fetchall() == [
                ("00000000-0000-0000-0000-000000000002",)
            ]
        db.execute("DROP INDEX usage_records_metadata_gin")
        db.execute("DROP INDEX usage_records_tags_gin")
        db.execute("CREATE TABLE usage_records_metadata_gin(id INTEGER)")
        db.execute("CREATE TABLE usage_records_tags_gin(id INTEGER)")
    print(
        "Verified 3 unchanged GIN originals, typed postimages and index name retirement"
    )


if __name__ == "__main__":
    check()
