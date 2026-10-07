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

import re
import unittest

from generate_sql_postgres_reference import execute, FIXTURES, postgres


class ArraySearchReferenceTest(unittest.TestCase):
    def test_array_search_result_oids_and_complete_physical_shape(self):
        with postgres() as db:
            for sql, oid, dimensions, values, nulls in (
                (
                    "SELECT array_positions('[0:3]={1,NULL,1,NULL}'::int4[],NULL)",
                    1007,
                    [{"length": 2, "lower_bound": 1}],
                    ["1", "3"],
                    [False, False],
                ),
                (
                    "SELECT array_remove('[0:3]={1,NULL,1,2}'::int4[],1)",
                    1007,
                    [{"length": 2, "lower_bound": 0}],
                    [None, "2"],
                    [True, False],
                ),
                (
                    "SELECT array_replace('[0:1][3:4]={{1,NULL},{1,2}}'::int4[],1,9)",
                    1007,
                    [
                        {"length": 2, "lower_bound": 0},
                        {"length": 2, "lower_bound": 3},
                    ],
                    ["9", None, "9", "2"],
                    [False, True, False, False],
                ),
                (
                    "SELECT array_replace(ARRAY[1]::int2[],1::int8,9007199254740993::int8)",
                    1016,
                    [{"length": 1, "lower_bound": 1}],
                    ["9007199254740993"],
                    [False],
                ),
            ):
                with self.subTest(sql=sql):
                    result = execute(
                        db,
                        {"id": "array-search-output", "sql": sql, "params": []},
                        read=True,
                    )
                    self.assertEqual([oid], result["column_oids"])
                    self.assertEqual([[False]], result["sql_nulls"])
                    self.assertEqual(
                        [
                            [
                                {
                                    "dimensions": dimensions,
                                    "values": values,
                                    "sql_nulls": nulls,
                                }
                            ]
                        ],
                        result["rows"],
                    )

    def test_native_search_bounds_nulls_and_promotions_match_postgres(self):
        source = (FIXTURES.parent / "scalar.zig").read_text()
        section = source.split(
            'test "SQL PostgreSQL array search and transform preserve typed NULLs bounds and promotion" {',
            1,
        )[1].split('\ntest "', 1)[0]
        cases = re.findall(r'\.sql = "([^"]+)"', section)
        self.assertEqual(23, len(cases))
        with postgres() as db:
            for index, expression in enumerate(cases):
                with self.subTest(expression=expression):
                    result = execute(
                        db,
                        {
                            "id": f"array-search-{index}",
                            "sql": "SELECT " + expression,
                            "params": [],
                        },
                        read=True,
                    )
                    self.assertEqual([[True]], result["rows"])
                    self.assertEqual([[False]], result["sql_nulls"])
                    self.assertEqual([16], result["column_oids"])

    def test_native_search_diagnostics_match_postgres(self):
        import psycopg

        source = (FIXTURES.parent / "scalar.zig").read_text()
        section = source.split(
            'test "SQL PostgreSQL array search diagnoses dimensions initial position and signatures" {',
            1,
        )[1].split('\ntest "', 1)[0]
        cases = re.findall(r'\.sql = "([^"]+)", \.err = error\.(\w+)', section)
        self.assertEqual(9, len(cases))
        codes = {
            "UnsupportedSqlShape": "0A000",
            "SqlNullValueNotAllowed": "22004",
            "SqlUndefinedFunction": "42883",
        }
        with postgres() as db:
            for expression, error in cases:
                with self.subTest(expression=expression):
                    with db.transaction(force_rollback=True):
                        with self.assertRaises(psycopg.Error) as caught:
                            db.execute("SELECT " + expression)
                        self.assertEqual(codes[error], caught.exception.sqlstate)


if __name__ == "__main__":
    unittest.main()
