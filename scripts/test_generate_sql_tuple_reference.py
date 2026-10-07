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

import json
from pathlib import Path
import re
import unittest

from generate_sql_postgres_reference import execute, FIXTURES, postgres
from generate_sql_tuple_reference import reference


class TupleReferenceTest(unittest.TestCase):
    def test_native_row_membership_query_boundaries_match_postgres(self):
        source = (FIXTURES.parent / "relation_runtime.zig").read_text()
        section = source.split(
            'test "SQL row membership executes captured typed relations with NULL-aware truth and query boundaries" {',
            1,
        )[1].split('\ntest "', 1)[0]
        cases = re.findall(r'\.sql = "([^"]+)", \.rows = "([^"]+)"', section)
        self.assertEqual(14, len(cases))
        with postgres() as db:
            for index, (sql, rows) in enumerate(cases):
                with self.subTest(sql=sql):
                    result = execute(
                        db,
                        {"id": f"tuple-expression-{index}", "sql": sql, "params": []},
                        read=True,
                    )
                    expected = json.loads(rows)
                    self.assertEqual(expected, result["rows"])
                    self.assertEqual(
                        [[cell is None for cell in row] for row in expected],
                        result["sql_nulls"],
                    )
                    self.assertEqual([16] * len(expected[0]), result["column_oids"])

    def test_row_membership_arity_and_array_operator_errors_match_postgres(self):
        import psycopg

        with postgres() as db:
            for sql, code in (
                ("SELECT (1,2) IN (SELECT 1)", "42601"),
                ("SELECT (1,2) IN (SELECT 1,2,3)", "42601"),
                ("SELECT (ARRAY[1]::int2[],1) IN (SELECT ARRAY[1]::int8[],1)", "42883"),
                ("SELECT (1,2) IN (SELECT true,2)", "42883"),
            ):
                with self.subTest(sql=sql):
                    with db.transaction(force_rollback=True):
                        with self.assertRaises(psycopg.Error) as error:
                            db.execute(sql)
                        self.assertEqual(code, error.exception.sqlstate)

    def test_exact_postgres_truth_tables_and_three_valued_negation(self):
        fixture = Path(__file__).resolve().parents[1] / (
            "zig/pkg/antfly-embedded/src/sql/fixtures/sql_tuple_reference.json"
        )
        with postgres() as db:
            result = reference(db)
        self.assertEqual(json.loads(fixture.read_text()), result)
        self.assertEqual(110, len(result["cases"]))
        self.assertEqual(1740, sum(len(case["truth"]) for case in result["cases"]))


if __name__ == "__main__":
    unittest.main()
