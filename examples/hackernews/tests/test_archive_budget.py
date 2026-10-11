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
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import archive_budget as budget  # noqa: E402
import archive_qualification as archive  # noqa: E402


class BudgetReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.ledger = Path(self.directory.name) / "ledger.json"
        self.evidence_path = Path(self.directory.name) / "billing.json"
        self.config = json.loads((ROOT / "archive-qualification.json").read_text())
        self.config["budget"]["max_runs"] = 2
        archive.reserve(self.ledger, self.config, "failed")
        archive.reserve(self.ledger, self.config, "interrupted")
        self.original = json.loads(self.ledger.read_text())
        self.evidence = {
            "project": self.config["project"],
            "kind": "completed_pod_compute",
            "source_table": "billing.detailed_export",
            "export_time_utc": "2020-01-03T00:00:00Z",
            "runs": [
                {
                    "run_id": "failed",
                    "pod": "hn-failed",
                    "pod_deleted": True,
                    "closed_at_utc": "2020-01-01T00:00:00Z",
                    "coverage_through_utc": "2020-01-02T00:00:00Z",
                    "gross_compute_usd": 0.2,
                    "pending_noncompute_usd": 10,
                }
            ],
        }

    def closed_evidence(self):
        return {
            "project": self.config["project"],
            "kind": "reviewed_closed_pod_compute_bound",
            "runs": [
                {
                    "run_id": "failed",
                    "admitted_pod": {
                        "kind": "Pod",
                        "metadata": {
                            "name": "hn-failed",
                            "uid": "owned-uid",
                            "creationTimestamp": "2020-01-01T00:00:00Z",
                        },
                        "spec": {
                            "activeDeadlineSeconds": 3400,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "resources": {
                                        "requests": {
                                            "cpu": "8",
                                            "memory": "48Gi",
                                            "ephemeral-storage": "10Gi",
                                        }
                                    }
                                }
                            ],
                        },
                    },
                    "cleanup": {
                        "pod": "hn-failed",
                        "pod_uid": "owned-uid",
                        "created": True,
                        "deleted": True,
                        "observed_absent_at_utc": "2020-01-01T01:00:00Z",
                    },
                }
            ],
        }

    def apply_bound(self, evidence):
        self.evidence_path.write_text(json.dumps(evidence))
        return budget.bound_closed_pods(self.ledger, self.evidence_path)

    def test_closed_bound_keeps_noncompute_and_actual_billing_supersedes_it(self):
        result = self.apply_bound(self.closed_evidence())
        self.assertFalse(result["posted_billing"])
        self.assertAlmostEqual(result["exposure_usd"], 56.187338)
        self.assertTrue(self.apply_bound(self.closed_evidence())["already_applied"])
        original = json.loads(self.ledger.read_text())
        self.assertEqual(original["runs"], self.original["runs"])
        self.assertEqual(original["budget"], self.original["budget"])
        # Even an unexpected actual overrun must replace the estimate.
        self.evidence["runs"][0]["gross_compute_usd"] = 30
        self.assertAlmostEqual(self.apply()["exposure_usd"], 85)

    def test_closed_bound_rejects_wrong_owner_active_pod_and_unaccounted_storage(self):
        evidence = self.closed_evidence()
        cases = []
        wrong = deepcopy(evidence)
        wrong["runs"][0]["cleanup"]["pod_uid"] = "replacement-uid"
        cases.append(wrong)
        active = deepcopy(evidence)
        active["runs"][0]["cleanup"]["deleted"] = False
        cases.append(active)
        long = deepcopy(evidence)
        long["runs"][0]["admitted_pod"]["spec"]["activeDeadlineSeconds"] = 100000
        cases.append(long)
        persistent = deepcopy(evidence)
        persistent["runs"][0]["admitted_pod"]["spec"]["volumes"] = [
            {"persistentVolumeClaim": {"claimName": "state"}}
        ]
        cases.append(persistent)
        sidecar = deepcopy(evidence)
        sidecar["runs"][0]["admitted_pod"]["spec"]["containers"].append({})
        cases.append(sidecar)
        before = self.ledger.read_bytes()
        for invalid in cases:
            with self.subTest(evidence=invalid), self.assertRaises(ValueError):
                self.apply_bound(invalid)
            self.assertEqual(before, self.ledger.read_bytes())

    def apply(self):
        self.evidence_path.write_text(json.dumps(self.evidence))
        return budget.reconcile(self.ledger, self.evidence_path)

    def test_settlement_preserves_failed_history_pending_holds_and_is_idempotent(self):
        result = self.apply()
        self.assertAlmostEqual(result["exposure_usd"], 55.2)
        self.assertAlmostEqual(result["released_usd"], 9.8)
        recorded = json.loads(self.ledger.read_text())
        self.assertEqual(recorded["runs"], self.original["runs"])
        self.assertEqual(recorded["budget"], self.original["budget"])
        self.assertTrue(self.apply()["already_applied"])
        self.assertEqual(json.loads(self.ledger.read_text()), recorded)
        self.config["budget"]["max_runs"] = 4
        archive.reserve(self.ledger, self.config, "retry", extend_run_limit=True)
        self.assertAlmostEqual(
            budget.exposure(json.loads(self.ledger.read_text())), 75.2
        )

    def test_planning_uses_reconciled_history_without_expanding_ceiling(self):
        second = deepcopy(self.evidence["runs"][0])
        second.update(run_id="interrupted", pod="hn-interrupted", gross_compute_usd=0.3)
        self.evidence["runs"].append(second)
        self.apply()
        self.config["budget"]["max_runs"] = 7
        inventory = {
            "project": self.config["project"],
            "bucket": self.config["bucket"],
            "prefix": self.config["source_prefix"] + "/archive",
            "rows": self.config["expected_rows"],
            "object_count": self.config["source_objects"],
            "bytes": self.config["source_bytes"],
            "objects": [{"generation": "123", "crc32c": "abc"}]
            * self.config["source_objects"],
        }
        with self.assertRaisesRegex(ValueError, "150 ceiling"):
            archive.plan(self.config, inventory)
        estimate = archive.plan(
            self.config, inventory, json.loads(self.ledger.read_text())
        )
        self.assertEqual(estimate["planned_total_reserved_usd"], 145.5)
        self.config["budget"]["ceiling_usd"] = 151
        with self.assertRaisesRegex(ValueError, "150 ceiling"):
            archive.reserve(self.ledger, self.config, "expanded", extend_run_limit=True)

    def test_invalid_evidence_never_changes_ledger(self):
        cases = [
            ("project", "another-project"),
            ("kind", "net_cost_refund"),
            ("pod_deleted", False),
            ("run_id", "unknown"),
            ("pending_noncompute_usd", 0),
            ("gross_compute_usd", -1),
            ("gross_compute_usd", float("nan")),
            ("gross_compute_usd", float("inf")),
            ("gross_compute_usd", True),
            ("coverage_through_utc", "2019-01-01T00:00:00Z"),
            ("coverage_through_utc", "2021-01-01T00:00:00Z"),
        ]
        original = deepcopy(self.evidence)
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.evidence = deepcopy(original)
                target = (
                    self.evidence
                    if field in ("project", "kind")
                    else self.evidence["runs"][0]
                )
                target[field] = value
                with self.assertRaises(ValueError):
                    self.apply()
                self.assertEqual(json.loads(self.ledger.read_text()), self.original)

    def test_posted_cost_cannot_decrease_and_overruns_block_admission(self):
        self.apply()
        self.evidence["runs"][0]["gross_compute_usd"] = 0.1
        with self.assertRaisesRegex(ValueError, "cannot decrease"):
            self.apply()
        self.evidence["runs"][0]["gross_compute_usd"] = 140
        self.apply()
        self.config["budget"]["max_runs"] = 4
        with self.assertRaisesRegex(ValueError, "Budget exhausted"):
            archive.reserve(self.ledger, self.config, "overrun", extend_run_limit=True)
        self.assertEqual(len(json.loads(self.ledger.read_text())["runs"]), 2)

    def test_unsettled_local_diagnostics_still_consume_budget(self):
        self.apply()
        recorded = json.loads(self.ledger.read_text())
        recorded["local_diagnostics"] = [{"reserved_usd": 100}]
        self.ledger.write_text(json.dumps(recorded))
        self.config["budget"]["max_runs"] = 4
        with self.assertRaisesRegex(ValueError, "Budget exhausted"):
            archive.reserve(self.ledger, self.config, "overrun", extend_run_limit=True)

    def test_duplicate_runs_and_double_settlements_are_rejected(self):
        self.evidence["runs"].append(deepcopy(self.evidence["runs"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.apply()
        archive.reserve(
            self.ledger,
            {**self.config, "budget": {**self.config["budget"], "max_runs": 3}},
            "third",
            extend_run_limit=True,
        )
        with self.assertRaisesRegex(ValueError, "Duplicate run"):
            archive.reserve(
                self.ledger,
                {**self.config, "budget": {**self.config["budget"], "max_runs": 4}},
                "third",
                extend_run_limit=True,
            )


if __name__ == "__main__":
    unittest.main()
