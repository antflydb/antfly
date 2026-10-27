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

"""PostgreSQL oracle for widened SUM transitions and window frame removal."""

import unittest
from decimal import Decimal

from generate_sql_postgres_reference import postgres


class IntegerSumTest(unittest.TestCase):
    def test_window_rank_and_ntile_signatures_and_validation(self):
        import psycopg

        with postgres() as db:
            cursor = db.execute(
                "SELECT row_number() OVER (), rank() OVER (), dense_rank() OVER (), ntile(2::smallint) OVER (), ntile(NULL) OVER () FROM (VALUES (1),(2),(3)) v(n)"
            )
            self.assertEqual(
                [20, 20, 20, 23, 23],
                [column.type_code for column in cursor.description],
            )
            self.assertTrue(all(row[4] is None for row in cursor.fetchmany(4)))
            for query, state in [
                ("SELECT ntile(2147483648) OVER ()", "42883"),
                ("SELECT ntile(2::bigint) OVER ()", "42883"),
                ("SELECT ntile(0) OVER ()", "22014"),
                ("SELECT ntile(-1) OVER ()", "22014"),
            ]:
                with self.subTest(sql=query):
                    with (
                        self.assertRaises(psycopg.Error) as failure,
                        db.transaction(force_rollback=True),
                    ):
                        db.execute(query)
                    self.assertEqual(state, failure.exception.sqlstate)

    def test_integer_average_preserves_numeric_scale_and_exact_window_results(self):
        cases = [
            (
                "SELECT avg(n) FROM (VALUES (1::bigint),(1),(2)) v(n)",
                ["1.3333333333333333"],
            ),
            (
                "SELECT avg(n) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807)) v(n)",
                ["9223372036854775807"],
            ),
            (
                "SELECT avg(n) FROM (VALUES (9007199254740993::bigint),(9007199254740994)) v(n)",
                ["9007199254740993.5000"],
            ),
            (
                "SELECT avg(n) FROM (VALUES (0::integer),(0)) v(n)",
                ["0.00000000000000000000"],
            ),
            (
                "SELECT avg(n) FROM (VALUES (-1::smallint),(2::smallint)) v(n)",
                ["0.50000000000000000000"],
            ),
            (
                "SELECT avg(DISTINCT n) FILTER (WHERE n>0) FROM (VALUES (1),(2),(2),(NULL),(-1)) v(n)",
                ["1.5000000000000000"],
            ),
            (
                "SELECT avg(n) FILTER (WHERE false) FROM (VALUES (1::bigint)) v(n)",
                [None],
            ),
            (
                "SELECT avg(n) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (0,9007199254740993::bigint),(1,9007199254740994::bigint),(2,NULL),(3,0::bigint)) v(i,n) ORDER BY i",
                [
                    "9007199254740993.0000",
                    "9007199254740993.5000",
                    "9007199254740994.0000",
                    "0.00000000000000000000",
                ],
            ),
            (
                "SELECT avg(n) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM (VALUES (0,1),(1,2),(2,3)) v(i,n) ORDER BY i",
                ["2.5000000000000000", "2.0000000000000000", "1.5000000000000000"],
            ),
            (
                "SELECT CASE WHEN true THEN avg(n) ELSE 0 END AS x FROM (VALUES (1),(2)) v(n) UNION SELECT 1.5::numeric",
                ["1.5000000000000000"],
            ),
        ]
        with postgres() as db:
            for query, expected in cases:
                with self.subTest(sql=query):
                    cursor = db.execute(query)
                    self.assertEqual(1700, cursor.description[0].type_code)
                    actual = [row[0] for row in cursor.fetchmany(len(expected) + 1)]
                    self.assertEqual(len(expected), len(actual))
                    for number, text in zip(actual, expected, strict=True):
                        if text is None:
                            self.assertIsNone(number)
                        else:
                            self.assertEqual(
                                Decimal(text).as_tuple(), number.as_tuple()
                            )

    def test_joined_assignment_coerces_before_atomic_publication(self):
        import psycopg

        with postgres() as db:
            db.execute(
                "CREATE TEMP TABLE target(_id text PRIMARY KEY,n bigint,cold text)"
            )
            db.execute("CREATE TEMP TABLE source(id text PRIMARY KEY)")
            db.execute("INSERT INTO target VALUES ('a',0,'old'),('b',0,'old')")
            db.execute("INSERT INTO source VALUES ('a'),('b')")
            for expression, expected in [
                ("1.5", 2),
                ("'2.5'::real", 2),
                ("9007199254740992.5::numeric", 9007199254740993),
            ]:
                with (
                    self.subTest(expression=expression),
                    db.transaction(force_rollback=True),
                ):
                    cursor = db.execute(
                        f"UPDATE target t SET n={expression},cold='new' FROM source s WHERE t._id=s.id RETURNING t.n"
                    )
                    self.assertEqual([(expected,), (expected,)], cursor.fetchmany(3))
            with (
                self.assertRaises(psycopg.Error) as failure,
                db.transaction(force_rollback=True),
            ):
                db.execute(
                    "UPDATE target t SET n=CASE WHEN s.id='a' THEN 9007199254740993::numeric ELSE 18446744073709551614::numeric END,cold='new' FROM source s WHERE t._id=s.id"
                )
            self.assertEqual("22003", failure.exception.sqlstate)
            self.assertEqual(
                [(0, "old"), (0, "old")],
                db.execute("SELECT n,cold FROM target ORDER BY _id").fetchmany(3),
            )

    def test_assignment_rounds_exactly_and_overflow_aborts_the_statement(self):
        import psycopg

        with postgres() as db:
            db.execute(
                "CREATE TEMP TABLE items (_id text PRIMARY KEY,n bigint NOT NULL)"
            )
            for query, expected in [
                (
                    "INSERT INTO items SELECT 'a',9007199254740992.5::numeric",
                    9007199254740993,
                ),
                (
                    "INSERT INTO items VALUES ('a',9007199254740992.5::numeric)",
                    9007199254740993,
                ),
                (
                    "INSERT INTO items SELECT 'a',sum(9007199254740993::bigint)",
                    9007199254740993,
                ),
                ("INSERT INTO items SELECT 'a',1.5", 2),
                ("INSERT INTO items SELECT 'a',-1.5", -2),
                ("INSERT INTO items SELECT 'a',2.5::double precision", 2),
                ("INSERT INTO items SELECT 'a',2.5::real", 2),
            ]:
                with self.subTest(sql=query), db.transaction(force_rollback=True):
                    db.execute(query)
                    self.assertEqual(
                        [(expected,)],
                        db.execute("SELECT n FROM items").fetchmany(2),
                    )
            for query, state in [
                (
                    "INSERT INTO items SELECT 'a',9007199254740993::numeric UNION ALL SELECT 'b',18446744073709551614::numeric",
                    "22003",
                ),
                (
                    "INSERT INTO items SELECT 'a',sum(n) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807::bigint)) v(n)",
                    "22003",
                ),
                ("INSERT INTO items SELECT 'a','NaN'::numeric", "0A000"),
            ]:
                with self.subTest(sql=query):
                    with (
                        self.assertRaises(psycopg.Error) as failure,
                        db.transaction(force_rollback=True),
                    ):
                        db.execute(query)
                    self.assertEqual(state, failure.exception.sqlstate)
                    self.assertEqual(
                        [(0,)], db.execute("SELECT count(*) FROM items").fetchmany(1)
                    )
            db.execute("CREATE TEMP TABLE exact_numbers(n numeric)")
            db.execute("INSERT INTO exact_numbers SELECT '1.23456789'::real")
            self.assertEqual(
                [(Decimal("1.23457"),)],
                db.execute("SELECT n FROM exact_numbers").fetchmany(2),
            )

    def test_bigint_results_are_numeric_without_int8_intermediate_overflow(self):
        source = "(VALUES (0,9223372036854775807::bigint),(1,9223372036854775807::bigint),(2,-9223372036854775807::bigint)) v(i,n)"
        with postgres() as db:
            for query, expected in [
                (
                    f"SELECT sum(n) FILTER (WHERE n>0) FROM {source}",
                    ["18446744073709551614"],
                ),
                (
                    f"SELECT sum(DISTINCT n) FILTER (WHERE n>0) FROM {source}",
                    ["9223372036854775807"],
                ),
                (f"SELECT sum(n) FILTER (WHERE false) FROM {source}", [None]),
                (
                    "SELECT CASE WHEN TRUE THEN SUM(1::bigint) ELSE 0 END AS x UNION SELECT 1",
                    ["1"],
                ),
                (
                    f"SELECT sum(n) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {source} ORDER BY i",
                    ["9223372036854775807", "18446744073709551614", "0"],
                ),
                (
                    f"SELECT sum(n) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM {source} ORDER BY i",
                    ["0", "0", "18446744073709551614"],
                ),
            ]:
                with self.subTest(sql=query), db.transaction(force_rollback=True):
                    db.execute("SET TRANSACTION READ ONLY")
                    cursor = db.execute(query)
                    self.assertEqual(1700, cursor.description[0].type_code)
                    self.assertEqual(
                        [
                            (Decimal(value) if value is not None else None,)
                            for value in expected
                        ],
                        cursor.fetchmany(4),
                    )
            for identity in ("smallint", "integer"):
                with self.subTest(identity=identity):
                    cursor = db.execute(
                        f"SELECT sum(n) FROM (VALUES (1::{identity}),(2::{identity})) v(n)"
                    )
                    self.assertEqual(20, cursor.description[0].type_code)
                    self.assertEqual([(3,)], cursor.fetchmany(2))
