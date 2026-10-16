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

"""Supplemental replay contracts, not original-case inventory qualification."""

import unittest

import psycopg
from generate_sql_postgres_reference import postgres


class ReplayPrefixTest(unittest.TestCase):
    def test_owned_materialized_and_recursive_prefix_contracts(self):
        with postgres() as db:
            db.execute("CREATE TEMP TABLE things (id bigint)")
            db.execute("INSERT INTO things SELECT generate_series(0,4095)")
            cases = [
                (
                    "SELECT n FROM (VALUES (1),(2),(NULL)) v(n)",
                    [(1,), (2,), (None,)],
                    ["n"],
                    [23],
                ),
                (
                    "SELECT n FROM (VALUES (1),(2147483648)) v(n)",
                    [(1,), (2147483648,)],
                    ["n"],
                    [20],
                ),
                (
                    "SELECT n FROM (VALUES (1),(9223372036854775807)) v(n)",
                    [(1,), (9223372036854775807,)],
                    ["n"],
                    [20],
                ),
                ("SELECT 1 AS n UNION ALL SELECT 2", [(1,), (2,)], ["n"], [23]),
                (
                    "SELECT 1::smallint AS n UNION ALL SELECT 2::smallint",
                    [(1,), (2,)],
                    ["n"],
                    [21],
                ),
                (
                    "SELECT 1::smallint AS n UNION ALL SELECT 2",
                    [(1,), (2,)],
                    ["n"],
                    [23],
                ),
                (
                    "SELECT 1 AS n UNION ALL SELECT 2147483648",
                    [(1,), (2147483648,)],
                    ["n"],
                    [20],
                ),
                (
                    "SELECT NULL AS n UNION ALL SELECT 1",
                    [(None,), (1,)],
                    ["n"],
                    [23],
                ),
                (
                    "WITH q AS MATERIALIZED (SELECT 10/(1-id) AS v FROM things) SELECT v FROM q LIMIT 1",
                    [(10,)],
                    ["v"],
                    [20],
                ),
                (
                    "WITH q AS MATERIALIZED (SELECT 10/(1-id) AS v FROM things), nested AS MATERIALIZED (SELECT v FROM q) SELECT v FROM nested LIMIT 1",
                    [(10,)],
                    ["v"],
                    [20],
                ),
                (
                    "SELECT p.n, q.v FROM (VALUES (1),(2)) p(n) CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT id+p.n AS v FROM things), nested AS MATERIALIZED (SELECT v FROM c) SELECT v FROM nested LIMIT 1) q",
                    [(1, 1), (2, 2)],
                    ["n", "v"],
                    [23, 20],
                ),
                (
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5",
                    [(1,), (2,), (3,), (4,), (5,)],
                    ["n"],
                    [23],
                ),
                (
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT n FROM r ORDER BY n DESC LIMIT 1",
                    [(5,)],
                    ["n"],
                    [23],
                ),
                (
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 10/(n-1) FROM r) SELECT n FROM r LIMIT 1",
                    [(1,)],
                    ["n"],
                    [23],
                ),
                (
                    "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT n FROM r LIMIT 1) q",
                    [(1, 1), (10, 10)],
                    ["n", "n"],
                    [23, 23],
                ),
                (
                    "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT n FROM r LIMIT 2) q",
                    [(1, 1), (1, 2), (10, 10), (10, 11)],
                    ["n", "n"],
                    [23, 23],
                ),
                (
                    "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+2), c AS MATERIALIZED (SELECT n FROM r), nested AS MATERIALIZED (SELECT n FROM c) SELECT n FROM nested LIMIT 2) q",
                    [(1, 1), (1, 2), (10, 10), (10, 11)],
                    ["n", "n"],
                    [23, 23],
                ),
                (
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT CASE WHEN n=1 THEN 2 ELSE 1 END FROM r) SELECT n FROM r",
                    [(1,), (2,)],
                    ["n"],
                    [23],
                ),
            ]
            for query, expected, labels, oids in cases:
                with self.subTest(sql=query), db.transaction(force_rollback=True):
                    db.execute("SET TRANSACTION READ ONLY")
                    cursor = db.execute(query)
                    self.assertEqual(labels, [c.name for c in cursor.description])
                    self.assertEqual(oids, [c.type_code for c in cursor.description])
                    # These controlled heap/VALUES witnesses establish producer
                    # prefixes, not a portable ordering guarantee without ORDER BY.
                    self.assertEqual(expected, cursor.fetchmany(6))
            for query in [
                "WITH q AS MATERIALIZED (SELECT 10/(1-id) AS v FROM things) SELECT v FROM q LIMIT 2",
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 10/(n-1) FROM r) SELECT n FROM r LIMIT 2",
            ]:
                with self.subTest(sql=query):
                    with (
                        self.assertRaises(psycopg.errors.DivisionByZero) as failure,
                        db.transaction(force_rollback=True),
                    ):
                        db.execute("SET TRANSACTION READ ONLY")
                        db.execute(query).fetchmany(6)
                    self.assertEqual("22012", failure.exception.sqlstate)


if __name__ == "__main__":
    unittest.main()
