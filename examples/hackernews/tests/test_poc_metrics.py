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
    def test_slowest_profile_survives_a_multi_request_burst_as_absolute_gauges(self):
        timings = dict(
            total=3325,
            definition=1,
            leaf=3310,
            native_leaf=3100,
            cursor=10,
            composition=4,
        )
        text = "antfly_lake_query_composed_total_calls_total 20\n" + "".join(
            f"antfly_lake_query_slowest_composed_{phase}_nanoseconds {value * 1000000}\n"
            for phase, value in timings.items()
        )
        metrics = poc.parse_lake_metrics(text)
        self.assertIsNone(poc.composed_phase_profile(metrics, 3325))
        profile = poc.slowest_composed_profile(metrics)
        self.assertEqual(profile["total_ms"], 3325)
        self.assertEqual(profile["native_leaf_ms"], 3100)
        self.assertEqual(profile["composition_ms"], 4)
        self.assertEqual(profile["diagnostic_drops"], 0)
        metrics["antfly_lake_query_slow_profile_drops_total"] = 2
        self.assertEqual(poc.slowest_composed_profile(metrics)["diagnostic_drops"], 2)
        del metrics["antfly_lake_query_slowest_composed_leaf_nanoseconds"]
        self.assertIsNone(poc.slowest_composed_profile(metrics))
        self.assertIsNone(poc.slowest_composed_profile({}))

    def test_composed_profile_preserves_nested_native_work_without_double_counting(
        self,
    ):
        timings = dict(
            total=2360, definition=10, leaf=2250, native_leaf=1800, cursor=80
        )
        text = "antfly_lake_query_composed_total_calls_total 1\n" + "".join(
            f"antfly_lake_query_composed_{phase}_nanoseconds_total {value * 1000000}\n"
            for phase, value in timings.items()
        )
        delta = poc.lake_metric_deltas({}, poc.parse_lake_metrics(text))
        profile = poc.composed_phase_profile(delta, 2366)
        self.assertEqual(profile["native_leaves_ms"], 1800)
        self.assertEqual(profile["composition_ms"], 20)
        self.assertEqual(profile["outside_composed_ms"], 6)
        self.assertEqual(
            profile["total_ms"],
            profile["definition_ms"]
            + profile["leaves_ms"]
            + profile["cursor_storage_ms"]
            + profile["composition_ms"],
        )
        for calls in (0, 2):
            delta["antfly_lake_query_composed_total_calls_total"] = calls
            self.assertIsNone(poc.composed_phase_profile(delta, 2366))
        self.assertIsNone(poc.composed_phase_profile({}, 2366))

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


class NativeTextPhaseTests(unittest.TestCase):
    def test_native_text_phases_preserve_overlapping_scopes_and_failed_work(self):
        timings = {"total": 300, "ranking": 270, "text_directory": 12,
                   "text_inventory": 15, "text_opening": 40, "text_term_plan": 8,
                   "text_navigation": 5, "text_postings": 210, "text_cursor": 2}
        delta = poc.parse_lake_metrics("".join(
            f"antfly_lake_query_{key}_nanoseconds_total {value * 1000000}\n"
            for key, value in timings.items()))
        profile = poc.lake_phase_profile(delta, 310)
        self.assertEqual(profile["text_opening_ms"], 40)
        self.assertEqual(profile["text_postings_ms"], 210)
        self.assertEqual(profile["outside_lake_ms"], 10)
        self.assertIn("Do not sum", profile["scope"])
        self.assertIsNone(poc.lake_phase_profile({"antfly_lake_query_total_nanoseconds_total": 1}, 1))

    def test_native_text_phases_survive_coexisting_coarse_metrics(self):
        delta = {
            f'antfly_lake_query_phase_nanoseconds_total{{phase="{phase}"}}': value * 1_000_000
            for phase, value in dict(total=300, publication=20, search=270, hydration=0, delivery=10).items()
        }
        delta.update({f"antfly_lake_query_{phase}_nanoseconds_total": value * 1_000_000
                      for phase, value in dict(text_opening=40, text_postings=210, cursor_retention=8, result_delivery=2).items()})
        profile = poc.lake_phase_profile(delta, 310)
        self.assertEqual(profile["search_ms"], 270)
        self.assertEqual(profile["text_opening_ms"], 40)
        self.assertEqual(profile["text_postings_ms"], 210)
        self.assertEqual(profile["cursor_retention_ms"], 8)
        self.assertEqual(profile["result_delivery_ms"], 2)
        self.assertEqual(profile["outside_lake_ms"], 10)
        self.assertIn("Do not sum", profile["scope"])
