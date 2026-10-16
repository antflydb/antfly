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

"""Original generated/default CREATE semantics, never a schema-only oracle."""

import json
import unittest

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres

GENERATED_READ = (
    "SELECT id::text AS id,status,amount::text AS amount,fee::text AS fee,"
    "metadata::text AS metadata,status_slug,amount_cents::text AS amount_cents,"
    "source_key FROM generated_expr_records ORDER BY id"
)
CASTED_READ = (
    "SELECT id::text AS id,amount::text AS amount,enabled::text AS enabled "
    "FROM casted_defaults ORDER BY id"
)
GENERATED_INITIAL = [
    (
        "00000000-0000-0000-0000-000000000001",
        " Ready Now ",
        "1.235",
        "0.005",
        '{"source": 123.00}',
        "-ready-now-",
        "124",
        "123.00",
    ),
    (
        "00000000-0000-0000-0000-000000000002",
        None,
        "-0.005",
        "0",
        '{"source": null}',
        None,
        "-1",
        None,
    ),
    (
        "00000000-0000-0000-0000-000000000003",
        "CLOSED",
        "9007199254740993.5",
        "0.005",
        '{"source": "exact"}',
        "closed",
        "900719925474099351",
        "exact",
    ),
]
CASTED_INITIAL = [
    ("00000000-0000-0000-0000-000000000001", "0", "true"),
    ("00000000-0000-0000-0000-000000000002", None, None),
    ("00000000-0000-0000-0000-000000000003", "0", "true"),
    ("00000000-0000-0000-0000-000000000004", "9007199254740993.5000", "false"),
]


class TableCreationTest(unittest.TestCase):
    def test_jsonb_text_display_preserves_scale_and_null_provenance(self):
        cases = (
            (
                'CAST(\'{"zz":1.2300e2,"a":[true,null]}\'::jsonb AS text)',
                '{"a": [true, null], "zz": 123.00}',
            ),
            (
                '\'{"a":{"zz":1.2300e2,"b":2}}\'::jsonb ->> \'a\'',
                '{"b": 2, "zz": 123.00}',
            ),
            ("CAST('\"text\"'::jsonb AS text)", '"text"'),
            ("CAST('null'::jsonb AS text)", "null"),
            ("CAST(NULL::jsonb AS text)", None),
        )
        with postgres() as db:
            print(f"PostgreSQL server version: {db.info.server_version}")
            for expression, expected in cases:
                with self.subTest(expression=expression):
                    cursor = db.execute("SELECT " + expression)
                    self.assertEqual(25, cursor.description[0].type_code)
                    self.assertEqual([(expected,)], cursor.fetchall())

    def test_original_creation_enforcement_generation_and_future_mutations(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        inputs = json.loads(
            (FIXTURES / "sql_generated_original_reference.json").read_text()
        )
        with postgres() as db:
            print(f"PostgreSQL server version: {db.info.server_version}")
            for case_id, table in (
                ("sql-0671", "generated_expr_records"),
                ("sql-0675", "casted_defaults"),
            ):
                self.assertEqual([], originals[case_id]["params"])
                self.assertEqual(
                    "CREATE TABLE", db.execute(originals[case_id]["sql"]).statusmessage
                )
                self.assertEqual(
                    [(table + "_pkey", "p")],
                    db.execute(
                        "SELECT conname,contype FROM pg_constraint WHERE conrelid=%s::regclass AND contype='p'",
                        (table,),
                    ).fetchall(),
                )
                self.assertEqual(
                    [(True,)],
                    db.execute(
                        "SELECT attnotnull FROM pg_attribute "
                        "WHERE attrelid=%s::regclass AND attname='id'",
                        (table,),
                    ).fetchall(),
                )
            for query, oids in (
                (
                    "SELECT id,status,amount,fee,metadata,status_slug,amount_cents,source_key "
                    "FROM generated_expr_records LIMIT 0",
                    [2950, 25, 1700, 1700, 3802, 25, 1700, 25],
                ),
                (
                    "SELECT id,amount,enabled FROM casted_defaults LIMIT 0",
                    [2950, 1700, 16],
                ),
            ):
                self.assertEqual(
                    oids, [column.type_code for column in db.execute(query).description]
                )
            for index, row in enumerate(inputs["inputs"]):
                inserted = db.execute(
                    "INSERT INTO generated_expr_records(id,status,amount,fee,metadata) "
                    "VALUES(%s::uuid,%s,%s::numeric,%s::numeric,%s::jsonb) "
                    "RETURNING id::text AS id,status,amount::text AS amount,fee::text AS fee,"
                    "metadata::text AS metadata,status_slug,amount_cents::text AS amount_cents,source_key",
                    (
                        row["id"],
                        row["status"],
                        row["amount"],
                        row["fee"],
                        row["metadata_json"],
                    ),
                )
                self.assertEqual([GENERATED_INITIAL[index]], inserted.fetchall())
            db.execute(
                "INSERT INTO casted_defaults(id) VALUES('00000000-0000-0000-0000-000000000001')"
            )
            db.execute(
                "INSERT INTO casted_defaults(id,amount,enabled) VALUES"
                "('00000000-0000-0000-0000-000000000002',NULL,NULL),"
                "('00000000-0000-0000-0000-000000000003',DEFAULT,DEFAULT),"
                "('00000000-0000-0000-0000-000000000004','9007199254740993.5000'::numeric,false)"
            )
            self.assertEqual(GENERATED_INITIAL, db.execute(GENERATED_READ).fetchall())
            self.assertEqual(CASTED_INITIAL, db.execute(CASTED_READ).fetchall())
            for statement, state in (
                (
                    "INSERT INTO generated_expr_records(id,status) VALUES"
                    "('00000000-0000-0000-0000-000000000001','duplicate')",
                    "23505",
                ),
                ("INSERT INTO generated_expr_records(id) VALUES(NULL)", "23502"),
                (
                    "INSERT INTO generated_expr_records(id,status_slug) VALUES"
                    "('00000000-0000-0000-0000-000000000004','bad')",
                    "428C9",
                ),
                (
                    "UPDATE generated_expr_records SET status_slug='bad' WHERE false",
                    "428C9",
                ),
                (
                    "INSERT INTO casted_defaults(id) VALUES('00000000-0000-0000-0000-000000000001')",
                    "23505",
                ),
                ("INSERT INTO casted_defaults(id) VALUES(NULL)", "23502"),
            ):
                with self.assertRaises(psycopg.Error) as rejected:
                    db.execute(statement)
                self.assertEqual(state, rejected.exception.sqlstate)
                self.assertEqual(
                    GENERATED_INITIAL, db.execute(GENERATED_READ).fetchall()
                )
                self.assertEqual(CASTED_INITIAL, db.execute(CASTED_READ).fetchall())
            db.execute(
                "UPDATE generated_expr_records SET status='Ready Again',fee=0.015,"
                'metadata=\'{"source":"updated"}\'::jsonb,status_slug=DEFAULT,'
                "amount_cents=DEFAULT,source_key=DEFAULT "
                "WHERE id='00000000-0000-0000-0000-000000000001'"
            )
            updated = list(GENERATED_INITIAL)
            updated[0] = (
                "00000000-0000-0000-0000-000000000001",
                "Ready Again",
                "1.235",
                "0.015",
                '{"source": "updated"}',
                "ready-again",
                "125",
                "updated",
            )
            self.assertEqual(updated, db.execute(GENERATED_READ).fetchall())
            db.execute(
                "UPDATE generated_expr_records SET status=DEFAULT,amount=DEFAULT,"
                "fee=DEFAULT,metadata=DEFAULT,status_slug=DEFAULT,amount_cents=DEFAULT,"
                "source_key=DEFAULT WHERE id='00000000-0000-0000-0000-000000000002'"
            )
            updated[1] = ("00000000-0000-0000-0000-000000000002",) + (None,) * 7
            self.assertEqual(updated, db.execute(GENERATED_READ).fetchall())
            db.execute(
                "UPDATE casted_defaults SET amount=DEFAULT,enabled=DEFAULT "
                "WHERE id='00000000-0000-0000-0000-000000000002'"
            )
            casted = list(CASTED_INITIAL)
            casted[1] = ("00000000-0000-0000-0000-000000000002", "0", "true")
            self.assertEqual(casted, db.execute(CASTED_READ).fetchall())
