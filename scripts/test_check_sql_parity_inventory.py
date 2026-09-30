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

import copy
import json
import unittest
from unittest.mock import patch

from check_sql_parity_inventory import (
    FIXTURES,
    evidence_runs,
    release_blockers,
    run_evidence,
    validate,
)


class ParityInventoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inventory = (FIXTURES / "sql_parity_inventory.json").read_bytes()
        cls.ledger = json.loads(
            (FIXTURES / "sql_parity_dispositions.json").read_bytes()
        )

    def test_original_inventory_is_exhaustive(self):
        inventory, entries, _ = validate(self.inventory, self.ledger)
        self.assertEqual(1586, len(inventory["entries"]))
        self.assertEqual(1586, len(entries))

    def test_missing_and_duplicate_dispositions_are_rejected(self):
        for mutate in (
            lambda entries: entries.pop(),
            lambda entries: entries.append(entries[0]),
        ):
            ledger = copy.deepcopy(self.ledger)
            mutate(ledger["entries"])
            with self.assertRaises(ValueError):
                validate(self.inventory, ledger)

    def test_fixture_edit_cannot_silently_remove_original_scope(self):
        with self.assertRaisesRegex(ValueError, "checksum"):
            validate(self.inventory + b" ", self.ledger)

    def test_completed_status_needs_executable_evidence(self):
        for status in ("implemented", "rejected", "superseded"):
            ledger = copy.deepcopy(self.ledger)
            unresolved = next(
                entry
                for entry in ledger["entries"]
                if entry["status"] == "unresolved" and not entry.get("evidence")
            )
            unresolved["status"] = status
            with self.assertRaisesRegex(ValueError, "executable evidence"):
                validate(self.inventory, ledger)

    def test_required_original_behavior_cannot_be_relabelled_as_rejected(self):
        ledger = copy.deepcopy(self.ledger)
        implemented = next(
            entry for entry in ledger["entries"] if entry["status"] == "implemented"
        )
        implemented["status"] = "rejected"
        with self.assertRaisesRegex(ValueError, "non-rejection source contract"):
            validate(self.inventory, ledger)

    def test_original_rejection_needs_supersession_for_new_behavior(self):
        ledger = copy.deepcopy(self.ledger)
        rejected = next(
            entry for entry in ledger["entries"] if entry["status"] == "rejected"
        )
        rejected["status"] = "implemented"
        with self.assertRaisesRegex(ValueError, "original rejection"):
            validate(self.inventory, ledger)

    def test_case_id_must_be_in_a_cited_test_not_elsewhere_in_file(self):
        ledger = copy.deepcopy(self.ledger)
        entry = next(row for row in ledger["entries"] if row["id"] == "sql-0160")
        entry["evidence"][0]["test"] = entry["evidence"][1]["test"]
        entry["evidence"][2]["test"] = (
            "staged restore worker publishes a dependency complete mixed native cohort"
        )
        with self.assertRaisesRegex(ValueError, "at least one cited evidence test"):
            validate(self.inventory, ledger)

    def test_deferral_does_not_count_as_completion(self):
        self.assertEqual(1, len(release_blockers([{"status": "deferred"}])))

    def test_unresolved_partial_evidence_is_checked_and_runs_without_release_credit(
        self,
    ):
        _, entries, gate_ids = validate(self.inventory, self.ledger)
        partial = next(entry for entry in entries if entry["id"] == "sql-0008")
        self.assertEqual("unresolved", partial["status"])
        self.assertIn("sql-original-prepared-cte-runtime", gate_ids)
        self.assertIn("pgwire-original-prepared-cte", gate_ids)
        ledger = copy.deepcopy(self.ledger)
        partial = next(
            entry for entry in ledger["entries"] if entry["id"] == "sql-0008"
        )
        partial["evidence"][0]["test"] = (
            "SQL joined mutation equality work scales with inputs not Cartesian candidates"
        )
        partial["evidence"][1]["test"] = (
            "pgwire original prepared CTE INSERT defers mutation until execute"
        )
        partial["evidence"][2]["test"] = (
            "SQL document reads reject the relational stateless fallback before transport"
        )
        with self.assertRaisesRegex(ValueError, "at least one cited evidence test"):
            validate(self.inventory, ledger)

    def test_changed_original_source_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "source checksum"):
            validate(self.inventory, self.ledger, source_bytes=b"{}")

    def test_referenced_evidence_can_run_before_release_is_ready(self):
        _, entries, gate_ids = validate(self.inventory, self.ledger)
        self.assertGreater(len(release_blockers(entries)), 0)
        self.assertTrue(gate_ids)
        runs = evidence_runs(gate_ids, self.ledger["gates"])
        self.assertLess(len(runs), len(gate_ids))
        self.assertEqual(
            gate_ids, sorted(gate for run in runs for gate in run["gate_ids"])
        )
        with patch("check_sql_parity_inventory.subprocess.run") as run:
            run_evidence(gate_ids, self.ledger["gates"])
        self.assertEqual(len(runs), run.call_count)
        for planned, call in zip(runs, run.call_args_list, strict=True):
            self.assertEqual(planned["command"], call.args[0])
            self.assertEqual(planned["timeout_seconds"], call.kwargs["timeout"])
            self.assertTrue(call.kwargs["check"])

    def test_evidence_grouping_preserves_distinct_filters_and_isolation(self):
        gates = {
            "a": {
                "command": ["zig", "build", "sql-test", "--", "--test-filter", "alpha"],
                "cwd": "zig",
                "timeout_seconds": 5,
            },
            "b": {
                "command": [
                    "zig",
                    "build",
                    "sql-test",
                    "--",
                    "--test-filter",
                    "beta",
                    "--test-filter",
                    "alpha",
                ],
                "cwd": "zig",
                "timeout_seconds": 7,
            },
            "c": {
                "command": ["zig", "build", "sql-test", "--", "--test-filter", "gamma"],
                "cwd": "other",
                "timeout_seconds": 9,
            },
            "d": {
                "command": [
                    "zig",
                    "build",
                    "pgwire-test",
                    "--",
                    "--test-filter",
                    "delta",
                ],
                "cwd": "zig",
                "timeout_seconds": 11,
            },
            "e": {
                "command": ["zig", "build", "sql-test", "--", "CTE materialization"],
                "cwd": "zig",
                "timeout_seconds": 13,
            },
        }
        runs = evidence_runs(list(gates), gates)
        self.assertEqual(4, len(runs))
        self.assertEqual(["a", "b"], runs[0]["gate_ids"])
        self.assertEqual(
            [
                "zig",
                "build",
                "sql-test",
                "--",
                "--test-filter",
                "alpha",
                "--test-filter",
                "beta",
            ],
            runs[0]["command"],
        )
        self.assertEqual(12, runs[0]["timeout_seconds"])
        self.assertEqual(["c"], runs[1]["gate_ids"])
        self.assertEqual(["d"], runs[2]["gate_ids"])
        self.assertEqual(["e"], runs[3]["gate_ids"])


if __name__ == "__main__":
    unittest.main()
