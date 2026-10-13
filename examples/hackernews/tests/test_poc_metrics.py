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

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "hackernews_poc_metrics", Path(__file__).resolve().parents[1] / "poc.py"
)
poc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(poc)


class LakeMetricEvidenceTests(unittest.TestCase):
    def test_aggregate_phases_survive_parsing_and_delta_capture(self):
        before = poc.parse_lake_metrics(
            'antfly_lake_query_phase_nanoseconds_total{phase="total"} 1000000000\n'
            'antfly_lake_query_phase_nanoseconds_total{phase="publication"} 800000000\n'
            'antfly_lake_query_phase_nanoseconds_total{phase="search"} 100000000\n'
            'antfly_lake_query_phase_nanoseconds_total{phase="delivery"} 100000000\n'
            'antfly_lake_query_phase_nanoseconds_total{phase="hydration"} 10000000\n'
            "antfly_lake_cache_provider_reads_total 4\n"
            "antfly_lake_cache_disk_ready 1\n"
        )
        increments = {
            "total": 4380000000,
            "publication": 4250000000,
            "search": 87000000,
            "delivery": 43000000,
            "hydration": 7000000,
        }
        after = dict(before)
        for phase, value in increments.items():
            after[f'antfly_lake_query_phase_nanoseconds_total{{phase="{phase}"}}'] += (
                value
            )
        after["antfly_lake_cache_provider_reads_total"] += 1
        delta = poc.lake_metric_deltas(before, after)
        self.assertNotIn("antfly_lake_cache_disk_ready", delta)
        self.assertEqual(delta["antfly_lake_cache_provider_reads_total"], 1)
        profile = poc.lake_phase_profile(delta, 4385.25)
        self.assertEqual(profile["pre_search_ms"], 4250)
        self.assertEqual(profile["search_ms"], 87)
        self.assertEqual(profile["post_search_ms"], 43)
        self.assertEqual(profile["hydration_ms"], 7)
        self.assertEqual(profile["outside_lake_ms"], 5.25)
        self.assertEqual(
            profile["total_ms"],
            profile["pre_search_ms"] + profile["search_ms"] + profile["post_search_ms"],
        )

    def test_unknown_labels_and_non_lake_metrics_are_excluded(self):
        metrics = poc.parse_lake_metrics(
            "# HELP antfly_lake_query_phase_nanoseconds_total phase timings\n"
            'antfly_lake_query_phase_nanoseconds_total{phase="total",table="private"} 1\n'
            'antfly_lake_query_phase_nanoseconds_total{phase="unknown"} 2\n'
            'antfly_lake_cache_disk_unavailable{reason="private"} 1\n'
            "other_counter_total 4\n"
            "antfly_lake_query_total_nanoseconds_total 5\n"
        )
        self.assertEqual(metrics, {"antfly_lake_query_total_nanoseconds_total": 5})

    def test_legacy_or_partial_metrics_do_not_invent_phase_evidence(self):
        self.assertIsNone(poc.lake_phase_profile({}, 500))
        self.assertIsNone(
            poc.lake_phase_profile(
                {'antfly_lake_query_phase_nanoseconds_total{phase="total"}': 500000000},
                500,
            )
        )
