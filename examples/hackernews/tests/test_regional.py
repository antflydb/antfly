# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prevent comparative qualification from accepting inconsistent exact results."""

from copy import deepcopy
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


if __name__ == "__main__":
    unittest.main()
