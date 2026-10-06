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

import unittest

from generate_sql_parity_read_reference import reference


class ReadReferenceTest(unittest.TestCase):
    def test_profile_binds_full_scope_and_text_identity_without_rewriting_sql(self):
        rows = [{"key": "a", "value": {"id": "u1", "metadata": {"source": "api"}}}]
        result = reference(
            [
                self.case(
                    "SELECT id,metadata->>'source' FROM public.usage_records WHERE id=$1",
                    [{"string": "u1"}],
                )
            ],
            rows,
            {"id": {"type": "keyword"}, "metadata": {"type": "json"}},
        )
        self.assertEqual([["u1", "api"]], result["entries"][0]["rows"])

    def test_nondeterministic_clock_has_no_sqlite_parity_credit(self):
        result = reference(
            [self.case("SELECT CURRENT_TIMESTAMP FROM usage_records")], self.rows
        )
        self.assertEqual([], result["entries"])
        self.assertIn("clock", result["excluded"][0]["reason"])

    rows = [
        {
            "key": "a",
            "value": {
                "id": 9007199254740993,
                "name": "exact",
                "status": "OPEN",
                "metadata": {"source": "api"},
            },
        },
        {"key": "b", "value": {"id": 2, "status": "open", "metadata": {}}},
    ]

    def case(self, sql, params=()):
        return {
            "id": "sql-0001",
            "family": "read",
            "source_expectation": "requires_behavior_review",
            "sql": sql,
            "params": params,
        }

    def test_named_parameters_preserve_exact_bigint_and_order(self):
        result = reference(
            [
                self.case(
                    "SELECT id,name FROM usage_records WHERE id=$1",
                    [{"integer": "9007199254740993"}],
                )
            ],
            self.rows,
        )
        self.assertEqual([[9007199254740993, "exact"]], result["entries"][0]["rows"])
        self.assertEqual(["id", "name"], result["entries"][0]["columns"])

    def test_like_is_case_sensitive_and_json_missing_is_null(self):
        result = reference(
            [
                self.case(
                    "SELECT id, metadata->>'source' AS source FROM usage_records WHERE status LIKE 'op%' ORDER BY id"
                )
            ],
            self.rows,
        )
        self.assertEqual([[2, None]], result["entries"][0]["rows"])

    def test_reference_cannot_execute_disguised_mutations(self):
        result = reference(
            [self.case("DELETE FROM usage_records RETURNING id")], self.rows
        )
        self.assertEqual([], result["entries"])
        self.assertTrue(result["excluded"])

    def test_unavailable_shapes_and_vacuous_fixtures_receive_no_credit(self):
        result = reference(
            [
                self.case("SELECT id FROM missing"),
                self.case("SELECT id FROM usage_records WHERE status='absent'"),
            ],
            self.rows,
        )
        self.assertEqual([], result["entries"])
        self.assertEqual(2, len(result["excluded"]))

    def test_original_rejections_are_not_positive_reference_cases(self):
        case = self.case("SELECT 1")
        case["source_expectation"] = "rejection"
        with self.assertRaises(ValueError):
            reference([case], self.rows)

    def test_empty_credit_is_explicit_not_a_sql_substring_heuristic(self):
        result = reference(
            [self.case("SELECT 'WHERE false' FROM usage_records WHERE id=0")], self.rows
        )
        self.assertEqual([], result["entries"])
        case = self.case("SELECT id FROM usage_records WHERE false")
        case["id"] = "sql-0180"
        result = reference([case], self.rows)
        self.assertEqual([], result["entries"][0]["rows"])

    def test_recursive_reference_work_and_result_sets_are_bounded(self):
        result = reference(
            [
                self.case(
                    "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT x FROM r"
                )
            ],
            self.rows,
        )
        self.assertEqual([], result["entries"])
        self.assertEqual(1, len(result["excluded"]))
        result = reference(
            [
                self.case(
                    "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT sum(x) FROM r"
                )
            ],
            self.rows,
        )
        self.assertEqual([], result["entries"])
        self.assertEqual(1, len(result["excluded"]))


if __name__ == "__main__":
    unittest.main()
