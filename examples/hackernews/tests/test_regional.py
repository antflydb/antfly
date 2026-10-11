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

"""Prevent comparative qualification from accepting inconsistent exact results."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "hn_regional", Path(__file__).parents[1] / "regional.py"
)
regional = importlib.util.module_from_spec(spec)
spec.loader.exec_module(regional)


class ComparativeResultsTest(unittest.TestCase):
    def setUp(self):
        self.report = {
            "source": "gs://bucket/pinned/archive",
            "row_count": 47717307,
            "query": {"full_text_search": {"query": "database"}},
            "concurrency": 4,
            "first_response": {
                "hits": {"hits": [{"_source": {"hn_id": 42}, "_score": 2.5}]}
            },
            "metadata_filters": {"author": {"total": 100, "warm_ms": 15}},
        }

    def test_latency_differences_are_allowed(self):
        other = deepcopy(self.report)
        other["metadata_filters"]["author"]["warm_ms"] = 1000
        regional.compare_runs([self.report, other])

    def test_matching_top_hits_do_not_hide_wrong_filter_totals(self):
        other = deepcopy(self.report)
        other["metadata_filters"]["author"]["total"] = 99
        with self.assertRaisesRegex(ValueError, "Exact filter total"):
            regional.compare_runs([self.report, other])

    def test_workload_changes_cannot_hide_behind_matching_primary_query(self):
        self.report["workloads"] = {
            "sqlite": {"total": 100, "top_hits": [[42, 2.5]], "samples_ms": [100]}
        }
        other = deepcopy(self.report)
        other["workloads"]["sqlite"]["total"] = 99
        with self.assertRaisesRegex(ValueError, "Workload results differ"):
            regional.compare_runs([self.report, other])
        other = deepcopy(self.report)
        other["workloads"] = {}
        with self.assertRaisesRegex(ValueError, "Workload coverage differs"):
            regional.compare_runs([self.report, other])

    def test_distinct_sources_cannot_be_compared(self):
        other = deepcopy(self.report)
        other["source"] = "gs://bucket/other/archive"
        with self.assertRaisesRegex(ValueError, "source differs"):
            regional.compare_runs([self.report, other])

    def test_score_and_filter_coverage_must_agree(self):
        other = deepcopy(self.report)
        other["first_response"]["hits"]["hits"][0]["_score"] += 1
        with self.assertRaisesRegex(ValueError, "IDs/scores"):
            regional.compare_runs([self.report, other])
        other = deepcopy(self.report)
        other["metadata_filters"] = {}
        with self.assertRaisesRegex(ValueError, "Filter coverage"):
            regional.compare_runs([self.report, other])


class CredentialLifetimeTest(unittest.TestCase):
    def test_refreshes_and_admits_full_remaining_worker_lifetime(self):
        calls = []

        def invoke(argv, **kwargs):
            calls.append((argv, kwargs))
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "credential": {
                            "access_token": "test-token",
                            "token_expiry": "2026-10-10T13:00:00Z",
                        }
                    }
                )
            )

        token, evidence = regional.fresh_bearer_for_lifetime(
            invoke,
            "test-project",
            3400,
            now=datetime(2026, 10, 10, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(token, "test-token")
        self.assertIn("--force-auth-refresh", calls[0][0])
        self.assertEqual(evidence["remaining_seconds"], 3600)
        self.assertNotIn(token, json.dumps(evidence))

    def test_rejects_nearly_expired_cached_token(self):
        def invoke(*args, **kwargs):
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "credential": {
                            "access_token": "test-token",
                            "token_expiry": "2026-10-10T12:10:00Z",
                        }
                    }
                )
            )

        with self.assertRaisesRegex(RuntimeError, "worker hard deadline"):
            regional.fresh_bearer_for_lifetime(
                invoke,
                "test-project",
                3400,
                now=datetime(2026, 10, 10, 12, tzinfo=timezone.utc),
            )

    def test_invalid_expiry_does_not_leak_credential(self):
        def invoke(*args, **kwargs):
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "credential": {
                            "access_token": "test-token",
                            "token_expiry": "bad-test-token",
                        }
                    }
                )
            )

        with self.assertRaises(RuntimeError) as caught:
            regional.fresh_bearer_for_lifetime(invoke, "test-project", 3400)
        self.assertNotIn("test-token", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
