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
import unittest

from generate_sql_mutation_reference import COLUMNS, reference, seeds


class MutationReferenceTest(unittest.TestCase):
    def case(self, sql, *, name="mutation", family="update", params=()):
        return {
            "id": "sql-0571",
            "name": name,
            "family": family,
            "sql": sql,
            "params": params,
        }

    def test_update_records_returning_and_complete_untouched_state(self):
        result = reference(
            [
                self.case(
                    "UPDATE usage_records SET status=$1 WHERE id='u1' RETURNING id,status",
                    params=[{"string": "updated"}],
                )
            ]
        )
        entry = result["entries"][0]
        self.assertEqual(1, entry["affected"])
        self.assertEqual([["u1", "updated"]], entry["rows"])
        self.assertEqual(2, len(entry["final"]))
        self.assertEqual("closed", entry["final"][1][list(COLUMNS).index("status")])

    def test_each_case_is_independently_seeded(self):
        result = reference(
            [
                self.case("DELETE FROM usage_records", family="delete"),
                self.case("UPDATE usage_records SET status='new' RETURNING id,status"),
            ]
        )
        self.assertEqual([], result["entries"][0]["final"])
        self.assertEqual(2, result["entries"][1]["affected"])
        self.assertEqual(2, len(seeds()))

    def test_json_wrapper_is_not_an_unquoted_string(self):
        result = reference(
            [
                self.case(
                    "INSERT INTO usage_records(id,metadata) VALUES('new',to_jsonb($1)) RETURNING metadata",
                    family="insert",
                    params=[{"string": "wrapped"}],
                )
            ]
        )
        self.assertEqual([['"wrapped"']], result["entries"][0]["rows"])

    def test_locking_syntax_is_never_rewritten(self):
        result = reference(
            [
                self.case(
                    "UPDATE usage_records SET status='new' FOR UPDATE SKIP LOCKED RETURNING id"
                )
            ]
        )
        self.assertEqual([], result["entries"])
        self.assertIn("FOR", result["excluded"][0]["reason"])

    def test_non_exercising_results_and_unproven_index_profiles_are_excluded(self):
        for case in [
            self.case("UPDATE usage_records SET status='new' WHERE false"),
            self.case(
                "UPDATE usage_records SET status='new' WHERE email='a@example.test'",
                name="point update unique selector",
            ),
            self.case(
                "INSERT INTO usage_records(id) VALUES('u1') ON CONFLICT(id) DO NOTHING",
                family="insert",
            ),
        ]:
            with self.subTest(case=case):
                self.assertEqual([], reference([case])["entries"])

    def test_reference_rejects_schema_changes_and_multiple_statements(self):
        for sql in [
            "DROP TABLE usage_records",
            "UPDATE usage_records SET status='x'; DELETE FROM usage_records",
        ]:
            self.assertEqual([], reference([self.case(sql)])["entries"])

    def test_campaign_is_a_fixed_unique_source_owned_cohort(self):
        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        campaign = json.loads((fixtures / "sql_mutation_campaign.json").read_text())
        corpus = {
            case["id"]: case
            for case in json.loads(
                (fixtures / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        self.assertEqual(235, len(campaign["entries"]))
        self.assertEqual(235, len({case["id"] for case in campaign["entries"]}))
        for case in campaign["entries"]:
            self.assertEqual(corpus[case["id"]]["family"], case["family"])
            self.assertNotIn("sql", case)
            self.assertNotIn("params", case)


if __name__ == "__main__":
    unittest.main()
