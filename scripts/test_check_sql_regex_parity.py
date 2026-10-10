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

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from check_sql_regex_parity import (
    Case,
    cases,
    patterns,
    profile,
    reference,
    regressions,
    run_probe,
    subjects,
)


class RegexParityCampaignTest(unittest.TestCase):
    def test_binary_subject_enumeration_is_complete_and_not_sampled(self):
        self.assertEqual(["", "a", "b", "aa", "ab", "ba", "bb"], subjects(2))
        self.assertEqual(31, len(subjects(4)))

    def test_selection_matrix_covers_every_subject_and_start(self):
        generated = set(cases(2))
        for pattern in patterns():
            for text in subjects(2):
                for start in range(len(text) + 1):
                    self.assertIn(
                        Case(pattern.text, text, pattern.captures, start=start),
                        generated,
                    )

    def test_fixed_and_equal_endpoint_bounds_are_distinct(self):
        text = {pattern.text for pattern in patterns()}
        for expected in (
            "a{1}",
            "a{1}?",
            "a{1,1}",
            "a{1,1}?",
            "(a*?){1}",
            "(a*?){1,1}",
        ):
            self.assertIn(expected, text)

    def test_seeded_cases_are_deterministic_and_ids_bind_all_inputs(self):
        first = cases(0, 19, 100)
        self.assertEqual(first, cases(0, 19, 100))
        self.assertNotEqual(first, cases(0, 20, 100))
        self.assertEqual(len(first), len({case.record()["id"] for case in first}))
        self.assertNotEqual(
            Case("a", "a", 0).record()["id"], Case("a", "a", 0, start=1).record()["id"]
        )

    def test_regression_scope_is_independent_of_enumeration_bounds(self):
        self.assertEqual(41, len(regressions()))
        self.assertEqual(41, len(set(regressions())))

    def test_postgres_18_cannot_certify_the_19_lane(self):
        db = Mock()
        db.execute.return_value.fetchone.return_value = (
            180000,
            "UTF8",
            "C",
            "PostgreSQL 18",
        )
        with self.assertRaisesRegex(RuntimeError, "profile mismatch"):
            profile(db, 19)

    def test_oracle_timeout_is_inconclusive_not_a_rejection(self):
        import psycopg

        db = Mock()
        db.execute.side_effect = psycopg.errors.QueryCanceled("oracle timeout")
        with self.assertRaisesRegex(RuntimeError, "inconclusive oracle"):
            reference(db, Case("a", "a", 0))

    def test_native_reports_must_bind_the_entire_witness_and_outcome_count(self):
        artifact = {
            "profile": {"postgres_major": 18, "encoding": "UTF8", "collation": "C"},
            "entries": [{"id": "one"}],
        }
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            probe = directory / "probe"
            probe.write_bytes(b"test probe identity")

            def invoke(args, **kwargs):
                self.assertTrue(kwargs["check"])
                self.assertEqual(120, kwargs["timeout"])
                report = {
                    "format": 1,
                    "profile": artifact["profile"],
                    "checked": 1,
                    "mismatch_count": 0,
                    "failures": [],
                    "witness_sha256": hashlib.sha256(args[1].read_bytes()).hexdigest(),
                }
                report.update(self.mutation)
                args[2].write_text(json.dumps(report))

            with patch("check_sql_regex_parity.subprocess.run", side_effect=invoke):
                self.mutation = {}
                self.assertEqual(
                    0, run_probe(probe, artifact, directory)["mismatch_count"]
                )
                for mutation in (
                    {"checked": 0},
                    {"witness_sha256": "wrong"},
                    {"mismatch_count": 1},
                    {"mismatch_count": -1},
                    {"profile": {"postgres_major": 19}},
                    {"mismatch_count": 1, "failures": [{"id": "foreign"}]},
                ):
                    self.mutation = mutation
                    with self.assertRaisesRegex(
                        RuntimeError, "incomplete or incompatible"
                    ):
                        run_probe(probe, artifact, directory)


if __name__ == "__main__":
    unittest.main()
