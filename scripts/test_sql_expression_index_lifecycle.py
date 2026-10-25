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

"""Original expression-index DDL and full mutation postimages against PostgreSQL."""

import json
import unittest
from decimal import Decimal
import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres

CASES = (
    "sql-0017",
    "sql-0712",
    "sql-0713",
    "sql-0714",
    "sql-0715",
    "sql-0716",
    "sql-0717",
    "sql-0718",
    "sql-0722",
    "sql-0723",
)
PARTIAL = {"sql-0722", "sql-0723"}
UNIQUE = PARTIAL | {"sql-0017"}
MUTABLE = {"sql-0712", "sql-0713"}


def row(number, email, status):
    return (f"00000000-0000-0000-0000-{number:012}", email, "tenant", status)


class ExpressionIndexTest(unittest.TestCase):
    def test_original_concurrent_partial_build_keeps_writers_admitted(self):
        # sql-0707: hold an old physical snapshot at PostgreSQL's final
        # concurrent-build barrier, then change membership from another writer.
        from concurrent.futures import ThreadPoolExecutor
        import time

        original = next(
            item
            for item in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if item["id"] == "sql-0707"
        )
        self.assertEqual([], original["params"])
        with postgres() as db:
            db.execute("CREATE TABLE usage_records(id BIGINT PRIMARY KEY,status TEXT)")
            db.execute(
                "INSERT INTO usage_records VALUES(1,'pending'),(2,'done'),(3,'pending')"
            )
            with (
                psycopg.connect(db.info.dsn, autocommit=True) as builder,
                psycopg.connect(db.info.dsn, autocommit=True) as writer,
            ):
                writer.execute("SET statement_timeout='5s'")
                builder.execute("SET statement_timeout='10s'")
                db.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                db.execute("SELECT * FROM usage_records").fetchall()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(builder.execute, original["sql"])
                    try:
                        deadline = time.monotonic() + 5
                        while True:
                            phase = writer.execute(
                                "SELECT phase FROM pg_stat_progress_create_index WHERE relid='usage_records'::regclass"
                            ).fetchone()
                            if phase and phase[0] == "waiting for old snapshots":
                                break
                            if time.monotonic() >= deadline:
                                self.fail(
                                    "concurrent build never reached its retained-snapshot barrier"
                                )
                            time.sleep(0.01)
                        writer.execute(
                            "UPDATE usage_records SET status='done' WHERE id=1"
                        )
                        writer.execute(
                            "UPDATE usage_records SET status='pending' WHERE id=2"
                        )
                        writer.execute("DELETE FROM usage_records WHERE id=3")
                        writer.execute("INSERT INTO usage_records VALUES(4,'pending')")
                        self.assertFalse(future.done())
                        self.assertEqual(
                            [(1, "pending"), (2, "done"), (3, "pending")],
                            db.execute(
                                "SELECT * FROM usage_records ORDER BY id"
                            ).fetchall(),
                        )
                    finally:
                        db.execute("ROLLBACK")
                    self.assertEqual(
                        "CREATE INDEX", future.result(timeout=10).statusmessage
                    )
                self.assertEqual(
                    [(1, "done"), (2, "pending"), (4, "pending")],
                    db.execute("SELECT * FROM usage_records ORDER BY id").fetchall(),
                )
                db.execute("SET enable_seqscan=off")
                self.assertEqual(
                    [(2,), (4,)],
                    db.execute(
                        "SELECT id FROM usage_records WHERE status='pending' ORDER BY id"
                    ).fetchall(),
                )
                self.assertEqual(
                    [(True, True)],
                    db.execute(
                        "SELECT indisready,indisvalid FROM pg_index WHERE indexrelid='usage_records_status_idx'::regclass"
                    ).fetchall(),
                )

    def test_original_concurrent_index_drop_retires_and_reuses_name(self):
        # sql-0725: execute unchanged, retain rows and permit another build.
        original = next(
            item
            for item in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if item["id"] == "sql-0725"
        )
        self.assertEqual([], original["params"])
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT)"
            )
            seeds = [
                ("00000000-0000-0000-0000-000000000001", None, "tenant", "pending"),
                (
                    "00000000-0000-0000-0000-000000000002",
                    "owner@example.com",
                    "tenant",
                    "active",
                ),
            ]
            for item in seeds:
                db.execute("INSERT INTO usage_records VALUES(%s::uuid,%s,%s,%s)", item)
            create = (
                "CREATE UNIQUE INDEX usage_records_status_idx ON usage_records(status)"
            )
            old = None
            for _ in range(2):
                db.execute(create)
                identity = db.execute(
                    "SELECT 'usage_records_status_idx'::regclass::oid"
                ).fetchone()[0]
                self.assertNotEqual(old, identity)
                old = identity
                with self.assertRaises(psycopg.Error) as caught:
                    with db.transaction():
                        db.execute(original["sql"])
                self.assertEqual("25001", caught.exception.sqlstate)
                self.assertEqual(
                    "DROP INDEX", db.execute(original["sql"]).statusmessage
                )
                self.assertEqual(
                    [(None,)],
                    db.execute(
                        "SELECT to_regclass('usage_records_status_idx')"
                    ).fetchall(),
                )
                self.assertEqual(
                    seeds,
                    db.execute(
                        "SELECT id::text,email,tenant_id,status FROM usage_records ORDER BY id"
                    ).fetchall(),
                )
                db.execute(
                    "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000003',NULL,'other','pending')"
                )
                self.assertEqual(
                    seeds
                    + [
                        (
                            "00000000-0000-0000-0000-000000000003",
                            None,
                            "other",
                            "pending",
                        )
                    ],
                    db.execute(
                        "SELECT id::text,email,tenant_id,status FROM usage_records ORDER BY id"
                    ).fetchall(),
                )
                db.execute(
                    "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000003'"
                )

    def test_original_partial_column_indexes_cover_nulls_and_conditional_creation(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            for case in ("sql-0695", "sql-0704", "sql-0707", "sql-0710", "sql-0711"):
                with self.subTest(case=case):
                    namespace = "col_" + case.replace("-", "_")
                    db.execute(f'CREATE SCHEMA "{namespace}"')
                    db.execute(f'SET search_path TO "{namespace}"')
                    db.execute(
                        "CREATE TABLE usage_records "
                        "(id UUID PRIMARY KEY,status TEXT,tenant_id TEXT,amount BIGINT)"
                    )

                    def value(number, status, amount):
                        return (
                            f"00000000-0000-0000-0000-{number:012}",
                            status,
                            "tenant",
                            amount,
                        )

                    expected = [
                        value(1, "pending", -1),
                        value(2, "done", -1),
                        value(3, "pending", 5),
                        value(4, None, None),
                        value(6, None, -1),
                    ]
                    for item in expected:
                        db.execute(
                            "INSERT INTO usage_records(id,status,tenant_id,amount) VALUES(%s::uuid,%s,%s,%s)",
                            item,
                        )
                    predicate = (
                        "amount = -1" if case == "sql-0711" else "status = 'pending'"
                    )

                    def verify():
                        self.assertEqual(
                            expected,
                            db.execute(
                                "SELECT id::text,status,tenant_id,amount FROM usage_records ORDER BY id"
                            ).fetchall(),
                        )
                        selected = [
                            item
                            for item in expected
                            if (
                                item[3] == -1
                                if case == "sql-0711"
                                else item[1] == "pending"
                            )
                        ]
                        self.assertEqual(
                            selected,
                            db.execute(
                                "SELECT id::text,status,tenant_id,amount FROM usage_records WHERE "
                                + predicate
                                + " ORDER BY id"
                            ).fetchall(),
                        )

                    original = originals[case]
                    self.assertEqual([], original["params"])
                    self.assertEqual(
                        "CREATE INDEX", db.execute(original["sql"]).statusmessage
                    )
                    name = (
                        "usage_records_status_cover_idx"
                        if case == "sql-0704"
                        else "usage_records_negative_amount_idx"
                        if case == "sql-0711"
                        else "usage_records_status_idx"
                    )

                    def definition():
                        return db.execute(
                            "SELECT c.oid,pg_get_indexdef(c.oid),i.indnkeyatts,i.indnatts,i.indisunique,i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=%s AND i.indrelid='usage_records'::regclass",
                            (name,),
                        ).fetchall()

                    before = definition()
                    self.assertEqual(1, len(before))
                    self.assertEqual(
                        (1, 3 if case == "sql-0704" else 1, False, True), before[0][2:]
                    )
                    if case == "sql-0710":
                        self.assertEqual(
                            "CREATE INDEX", db.execute(original["sql"]).statusmessage
                        )
                        self.assertEqual(before, definition())
                    verify()
                    for number, status, amount in (
                        (1, "done", 2),
                        (2, "pending", 5),
                        (3, "done", -1),
                    ):
                        db.execute(
                            "UPDATE usage_records SET status=%s,amount=%s WHERE id=%s::uuid",
                            (status, amount, value(number, status, amount)[0]),
                        )
                    db.execute(
                        "INSERT INTO usage_records(id,status,tenant_id,amount) VALUES(%s::uuid,%s,%s,%s)",
                        value(5, "pending", -1),
                    )
                    db.execute(
                        "DELETE FROM usage_records WHERE id=%s::uuid",
                        (value(2, None, None)[0],),
                    )
                    expected = [
                        value(1, "done", 2),
                        value(3, "done", -1),
                        value(4, None, None),
                        value(5, "pending", -1),
                        value(6, None, -1),
                    ]
                    verify()
                    if case == "sql-0710":
                        self.assertEqual(
                            "CREATE INDEX", db.execute(original["sql"]).statusmessage
                        )
                        self.assertEqual(before, definition())
                    db.execute(
                        "UPDATE usage_records SET status='pending',amount=-1 WHERE id=%s::uuid",
                        (value(4, None, None)[0],),
                    )
                    expected[2] = value(4, "pending", -1)
                    verify()

    def test_join_access_placement_and_shared_producer_semantics(self):
        with postgres() as db:
            print(
                "PostgreSQL join access oracle:",
                db.execute("SHOW server_version_num").fetchone()[0],
            )
            db.execute(
                "CREATE TABLE invoices (_id TEXT PRIMARY KEY, amount NUMERIC(20,4), label TEXT, tenant BIGINT)"
            )
            db.execute(
                "INSERT INTO invoices VALUES ('one',9007199254740993.1250,'READY',7)"
            )
            db.execute(
                "CREATE INDEX mixed_idx ON invoices(tenant DESC NULLS FIRST,lower(label)) INCLUDE(amount) WHERE tenant>0"
            )
            db.execute(
                "CREATE INDEX default_label_idx ON invoices(coalesce(label,'empty'))"
            )
            expected = Decimal("9007199254740993.1250")
            for statement, rows in (
                (
                    "SELECT p.amount FROM invoices p CROSS JOIN invoices q WHERE p.tenant=7 AND p.tenant>0 AND lower(p.label)='ready'",
                    [(expected,)],
                ),
                (
                    "SELECT p.amount FROM invoices p LEFT JOIN invoices q ON true WHERE lower(q.label)='ready'",
                    [(expected,)],
                ),
                (
                    "SELECT p.amount FROM invoices p LEFT JOIN invoices q ON true WHERE coalesce(q.label,'empty')='empty'",
                    [],
                ),
                (
                    "SELECT p.amount FROM invoices p FULL JOIN invoices q ON true WHERE coalesce(q.label,'empty')='empty'",
                    [],
                ),
                (
                    "SELECT p.amount FROM invoices p LEFT JOIN invoices q ON coalesce(q.label,'empty')='empty'",
                    [(expected,)],
                ),
            ):
                with self.subTest(statement=statement):
                    self.assertEqual(rows, db.execute(statement).fetchall())
            db.execute("INSERT INTO invoices VALUES ('two',2.0000,'OTHER',8)")
            for statement in (
                "SELECT p.amount FROM invoices p WHERE p.amount=%s",
                "SELECT p.amount FROM (SELECT amount FROM invoices) p WHERE p.amount=%s",
            ):
                with self.subTest(statement=statement):
                    self.assertEqual(
                        [(expected,)], db.execute(statement, (expected,)).fetchall()
                    )
                    self.assertEqual(
                        [],
                        db.execute(
                            statement, (Decimal("9007199254740993.1251"),)
                        ).fetchall(),
                    )
            self.assertEqual(
                [(expected,)],
                db.execute(
                    "UPDATE invoices p SET amount=p.amount FROM invoices q WHERE p._id='one' AND q._id='two' AND q.tenant=8 RETURNING p.amount"
                ).fetchall(),
            )
            self.assertEqual(
                [(expected,)],
                db.execute(
                    "SELECT p.amount FROM invoices p WHERE p._id='one'"
                ).fetchall(),
            )
            self.assertEqual(
                [(expected,), (expected,)],
                db.execute(
                    "WITH c AS MATERIALIZED (SELECT amount,label FROM invoices) SELECT p.amount FROM c p CROSS JOIN c q WHERE lower(p.label)='ready'"
                ).fetchall(),
            )
            self.assertEqual(
                [(expected,)],
                db.execute(
                    "WITH c AS MATERIALIZED (SELECT amount,label FROM invoices WHERE lower(label)='ready') SELECT p.amount FROM c p CROSS JOIN c q"
                ).fetchall(),
            )
            self.assertEqual(
                [],
                db.execute(
                    "SELECT p.amount FROM (SELECT amount,label FROM invoices ORDER BY _id LIMIT 1) p WHERE coalesce(p.label,'empty')='OTHER'"
                ).fetchall(),
            )

    def test_exact_numeric_literals_with_composite_expression_indexes(self):
        with postgres() as db:
            print(
                "PostgreSQL NUMERIC oracle:",
                db.execute("SHOW server_version_num").fetchone()[0],
            )
            db.execute(
                "CREATE TABLE invoices (_id TEXT PRIMARY KEY, amount NUMERIC(20,4), label TEXT, tenant BIGINT)"
            )
            db.execute(
                "INSERT INTO invoices (_id,amount,label,tenant) VALUES ('one',9007199254740993.1250,'READY',7)"
            )

            def verify(expected):
                self.assertEqual(
                    [(expected,)],
                    db.execute("SELECT amount::text FROM invoices").fetchall(),
                )

            verify("9007199254740993.1250")
            self.assertEqual(
                (None,),
                db.execute(
                    "SELECT 1.5 < ALL (SELECT 2 UNION ALL SELECT NULL)"
                ).fetchone(),
            )
            db.execute("CREATE INDEX tenant_idx ON invoices(tenant)")
            db.execute(
                "CREATE INDEX mixed_idx ON invoices(tenant DESC NULLS FIRST,lower(label)) INCLUDE(amount) WHERE tenant>0"
            )
            for expected in ("9007199254740993.1251", "9007199254740993.1252"):
                self.assertEqual(
                    [(expected,)],
                    db.execute(
                        "UPDATE invoices SET amount=amount+0.0001 RETURNING amount::text"
                    ).fetchall(),
                )
                verify(expected)
            self.assertEqual(
                [("9007199254740993.1253",)],
                db.execute(
                    "UPDATE invoices SET amount=%s RETURNING amount::text",
                    (Decimal("9007199254740993.1253"),),
                ).fetchall(),
            )
            verify("9007199254740993.1253")
            for literal, expected in (
                ("'1.2300'", "1.2300"),
                ("9007199254740993", "9007199254740993.0000"),
                ("NULL", None),
            ):
                self.assertEqual(
                    [(expected,)],
                    db.execute(
                        f"UPDATE invoices SET amount={literal} RETURNING amount::text"
                    ).fetchall(),
                )
                verify(expected)
            for statement, code in (
                ("UPDATE invoices SET amount='1.2300'::text", "42804"),
                ("UPDATE invoices SET amount=99999999999999999", "22003"),
                (
                    "UPDATE invoices SET amount=abs('-9223372036854775808'::bigint)",
                    "22003",
                ),
            ):
                with self.assertRaises(psycopg.Error) as failure:
                    db.execute(statement)
                self.assertEqual(code, failure.exception.sqlstate)
                verify(None)

    def test_original_qualified_index_names_are_syntax_errors(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            db.execute("CREATE TABLE usage_records(status TEXT)")
            for case_id in ("sql-0708", "sql-0709"):
                with self.subTest(case=case_id):
                    with self.assertRaises(psycopg.Error) as caught:
                        with db.transaction():
                            db.execute(originals[case_id]["sql"])
                    self.assertEqual("42601", caught.exception.sqlstate)
            db.execute("CREATE INDEX status_idx ON public.usage_records(status)")
            db.execute("DROP INDEX public.status_idx")

    def test_original_expression_index_lifecycles(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            print(
                "PostgreSQL server version:",
                db.execute("SHOW server_version_num").fetchone()[0],
            )
            for case in CASES:
                with self.subTest(case=case):
                    namespace = "expr_" + case.replace("-", "_")
                    db.execute(f'CREATE SCHEMA "{namespace}"')
                    db.execute(f'SET search_path TO "{namespace}"')
                    db.execute(
                        "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT)"
                    )
                    expected = [
                        row(
                            1,
                            "Owner@Example.com",
                            "active" if case in PARTIAL else "old-start",
                        ),
                        row(
                            2,
                            "other@example.com",
                            "active" if case in PARTIAL else "old-finish",
                        ),
                    ]

                    def verify():
                        self.assertEqual(
                            expected,
                            db.execute(
                                "SELECT id::text,email,tenant_id,status FROM usage_records ORDER BY id"
                            ).fetchall(),
                        )

                    def insert(value):
                        db.execute(
                            "INSERT INTO usage_records(id,email,tenant_id,status) VALUES(%s::uuid,%s,%s,%s)",
                            value,
                        )

                    def reject(statement, parameters, code):
                        with self.assertRaises(psycopg.Error) as failure:
                            db.execute(statement, parameters)
                        self.assertEqual(code, failure.exception.sqlstate)
                        verify()

                    for value in expected:
                        insert(value)
                    verify()
                    original = originals[case]
                    self.assertEqual([], original["params"])
                    if case in MUTABLE:
                        reject(original["sql"], (), "42P17")
                        self.assertEqual(
                            [],
                            db.execute(
                                "SELECT indexname FROM pg_indexes WHERE schemaname=%s AND indexname != 'usage_records_pkey'",
                                (namespace,),
                            ).fetchall(),
                        )
                    else:
                        self.assertEqual(
                            "CREATE INDEX", db.execute(original["sql"]).statusmessage
                        )
                        verify()
                        status = "active" if case in PARTIAL else "new-start"
                        collision = row(3, "owner@example.com", status)
                        update = "UPDATE usage_records SET email='OWNER@EXAMPLE.COM',status=%s WHERE id='00000000-0000-0000-0000-000000000002'"
                        if case in UNIQUE:
                            reject(
                                "INSERT INTO usage_records(id,email,tenant_id,status) VALUES(%s::uuid,%s,%s,%s)",
                                collision,
                                "23505",
                            )
                            reject(update, (status,), "23505")
                        else:
                            insert(collision)
                            expected.append(collision)
                            db.execute(update, (status,))
                            expected[1] = row(2, "OWNER@EXAMPLE.COM", status)
                        verify()
                        nullable = row(
                            4,
                            "owner@example.com" if case in PARTIAL else None,
                            "inactive" if case in PARTIAL else None,
                        )
                        insert(nullable)
                        expected.append(nullable)
                        if case in PARTIAL:
                            reject(
                                "UPDATE usage_records SET status='active' WHERE id='00000000-0000-0000-0000-000000000004'",
                                (),
                                "23505",
                            )
                        verify()
                    # Same mutation sequence as the native cold-restart phase.
                    # PostgreSQL remains an independent semantics oracle, not a
                    # substitute for Antfly owner-recovery qualification.
                    db.execute(
                        "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000001'"
                    )
                    status = "active" if case in PARTIAL else "new-start"
                    replacement = row(6, "owner@example.com", status)
                    insert(replacement)
                    expected = expected[1:] + [replacement]
                    verify()
                    if case in UNIQUE:
                        reject(
                            "INSERT INTO usage_records(id,email,tenant_id,status) VALUES(%s::uuid,%s,%s,%s)",
                            row(7, "OWNER@EXAMPLE.COM", status),
                            "23505",
                        )
                    db.execute("RESET search_path")
