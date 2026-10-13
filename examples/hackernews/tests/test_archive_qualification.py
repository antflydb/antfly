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

"""Release gates must reject incomplete evidence and never refund failed runs."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import sys
import unittest

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location(
    "hn_archive", ROOT / "archive_qualification.py"
)
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


class ArchiveQualificationTest(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "archive-qualification.json").read_text())
        self.inventory = {
            "project": self.config["project"],
            "bucket": self.config["bucket"],
            "prefix": self.config["source_prefix"] + "/archive",
            "rows": self.config["expected_rows"],
            "object_count": self.config["source_objects"],
            "bytes": self.config["source_bytes"],
            "objects": [{"generation": "123", "crc32c": "abc"}]
            * self.config["source_objects"],
        }
        self.report = {
            "row_count": self.config["expected_rows"],
            "verified": list(archive.REQUIRED_CHECKS),
            "warm_samples_ms": [100] * 20,
            "concurrent_search_ms": [200] * 8,
            "after_restart_ms": 300,
            "first_search_ms": 1000,
            "ready_seconds": 100,
            "metadata_filters": {
                name: {"samples_ms": [100] * 20}
                for name in ("author", "created_at", "item_type", "points", "combined")
            },
            "workloads": {
                term: {"samples_ms": [100] * 20}
                for term in self.config["workload_terms"]
            },
            "build_peak_rss_bytes": 1024**3,
            "cursor_depth": {"hits": 5125, "samples_ms": [100] * 40, "restart_ms": 100},
        }
        self.regional = {
            "runs": {"indexed": [deepcopy(self.report), deepcopy(self.report)]}
        }

    def test_reference_evidence_passes(self):
        self.assertTrue(archive.assess(self.regional, self.config)["passed"])
        self.assertLess(
            archive.plan(self.config, self.inventory)["planned_total_reserved_usd"], 150
        )

    def test_slow_tail_fails_even_with_fast_median(self):
        self.regional["runs"]["indexed"][0]["warm_samples_ms"][-2:] = [10000, 10000]
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])

    def test_missing_correctness_workload_and_memory_evidence_fail(self):
        for field, value in (
            ("verified", []),
            ("workloads", {}),
            ("build_peak_rss_bytes", None),
        ):
            with self.subTest(field=field):
                regional = deepcopy(self.regional)
                regional["runs"]["indexed"][0][field] = value
                self.assertFalse(archive.assess(regional, self.config)["passed"])

    def test_sample_and_concurrency_coverage_fail(self):
        self.regional["runs"]["indexed"][0]["concurrent_search_ms"] = [100]
        self.regional["runs"]["indexed"][0]["warm_samples_ms"] = [100]
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])

    def test_stored_source_policy_must_match_the_configured_profile(self):
        self.config["store_source"] = True
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])
        for report in self.regional["runs"]["indexed"]:
            report["store_source"] = True
        self.assertTrue(archive.assess(self.regional, self.config)["passed"])

    def test_deep_cursor_workload_cannot_silently_change(self):
        self.config["cursor_term"] = "AI"
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])
        for report in self.regional["runs"]["indexed"]:
            report["cursor_depth"]["term"] = "AI"
        self.assertTrue(archive.assess(self.regional, self.config)["passed"])

    def test_composed_coverage_is_required_when_enabled(self):
        self.config["qualify_composed_sources"] = True
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])
        for report in self.regional["runs"]["indexed"]:
            report["composed_sources"] = {
                "verified": [
                    "union_history",
                    "overlay_nonmatching_suppression",
                    "saved_current_visibility",
                    "mutable_composed_cursor_restart",
                ]
            }
        self.assertTrue(archive.assess(self.regional, self.config)["passed"])

    def test_composed_benchmark_requires_both_sources_and_bounded_current_fixture(self):
        self.config.update(benchmark_composed=True, composed_current_rows=1000)
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])
        for report in self.regional["runs"]["indexed"]:
            report["composed_benchmarks"] = {
                name: {
                    "first_search_ms": 1000,
                    "warm_samples_ms": [100] * 20,
                    "result_count": 5,
                    "current_fixture_rows": 1000,
                }
                for name in ("union", "saved_overlay")
            }
        self.assertTrue(archive.assess(self.regional, self.config)["passed"])
        for field, value in (
            ("first_search_ms", 20000),
            ("first_search_ms", float("nan")),
            ("warm_samples_ms", [100]),
            ("warm_samples_ms", [5000] * 20),
            ("current_fixture_rows", 999),
            ("result_count", 0),
        ):
            with self.subTest(field=field, value=value):
                evidence = deepcopy(self.regional)
                evidence["runs"]["indexed"][0]["composed_benchmarks"]["saved_overlay"][
                    field
                ] = value
                self.assertFalse(archive.assess(evidence, self.config)["passed"])

    def test_composed_plan_rejects_missing_correctness_or_unbounded_fixture(self):
        self.config["benchmark_composed"] = True
        with self.assertRaisesRegex(ValueError, "Composed benchmarks require"):
            archive.plan(self.config, self.inventory)
        self.config["qualify_composed_sources"] = True
        self.config["composed_current_rows"] = 4097
        with self.assertRaisesRegex(ValueError, "Composed benchmarks require"):
            archive.plan(self.config, self.inventory)

    def test_registration_is_part_of_the_initial_publication_gate(self):
        self.regional["runs"]["indexed"][0]["registration_seconds"] = 3000
        self.assertFalse(archive.assess(self.regional, self.config)["passed"])

    def test_source_mismatch_fails_before_cloud_work(self):
        self.inventory["rows"] -= 1
        with self.assertRaisesRegex(ValueError, "rows differs"):
            archive.plan(self.config, self.inventory)

    def test_budget_cannot_expand_above_authorization(self):
        self.config["budget"]["ceiling_usd"] = 151
        with self.assertRaisesRegex(ValueError, "150 ceiling"):
            archive.plan(self.config, self.inventory)

    def test_failed_or_interrupted_reservations_exhaust_retry_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            self.config["budget"]["max_runs"] = 2
            archive.reserve(ledger, self.config, "failed")
            archive.reserve(ledger, self.config, "interrupted")
            with self.assertRaisesRegex(ValueError, "Run limit exhausted"):
                archive.reserve(ledger, self.config, "unapproved-retry")
            self.assertEqual(len(json.loads(ledger.read_text())["runs"]), 2)

    def test_dry_run_reports_exhausted_admission_without_resetting_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            self.config["budget"]["max_runs"] = 1
            archive.reserve(ledger, self.config, "failed")
            before = ledger.read_bytes()
            result = archive.plan(self.config, self.inventory, json.loads(before))
            self.assertEqual(result["remaining_run_slots"], 0)
            self.assertFalse(result["can_reserve_next_run"])
            self.assertEqual(ledger.read_bytes(), before)

    def test_existing_ledger_cannot_silently_reset_prior_spend(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            archive.reserve(ledger, self.config, "first")
            self.config["budget"]["prior_work_reserved_usd"] = 0
            with self.assertRaisesRegex(ValueError, "ledger budget differs"):
                archive.reserve(ledger, self.config, "second")

    def test_local_diagnostics_reserve_the_same_cloud_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            archive.reserve(ledger, self.config, "first")
            recorded = json.loads(ledger.read_text())
            recorded["local_diagnostics"] = [{"id": "local", "reserved_usd": 100}]
            ledger.write_text(json.dumps(recorded))
            with self.assertRaisesRegex(ValueError, "Budget exhausted"):
                archive.reserve(ledger, self.config, "second")
            self.assertEqual(json.loads(ledger.read_text()), recorded)

    def test_reviewed_retry_extension_preserves_spend_and_failed_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            self.config["budget"]["max_runs"] = 2
            archive.reserve(ledger, self.config, "failed")
            archive.reserve(ledger, self.config, "interrupted")
            self.config["budget"]["max_runs"] = 4
            with self.assertRaisesRegex(ValueError, "budget differs"):
                archive.reserve(ledger, self.config, "implicit-extension")
            archive.reserve(
                ledger, self.config, "reviewed-retry", extend_run_limit=True
            )
            recorded = json.loads(ledger.read_text())
            self.assertEqual(len(recorded["runs"]), 3)
            self.assertEqual(recorded["run_limit_extensions"][0]["prior_runs"], 2)
            self.config["budget"]["ceiling_usd"] = 149
            with self.assertRaisesRegex(ValueError, "preserve the approved budget"):
                archive.reserve(
                    ledger, self.config, "changed-budget", extend_run_limit=True
                )


if __name__ == "__main__":
    unittest.main()
