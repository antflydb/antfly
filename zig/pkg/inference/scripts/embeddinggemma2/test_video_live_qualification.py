#!/usr/bin/env python3
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
import unittest
import video_live_qualification as live


class LiveQualificationTests(unittest.TestCase):
    def reports(self):
        vectors = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
        queries = [
            {"id": "dragon", "vector": [0.8, 0.6, 0.0]},
            {"id": "landscape", "vector": [0.0, 0.6, 0.8]},
        ]
        rows = [
            {
                "case": "sintel-6s",
                "ordinal": i,
                "epoch": 0,
                "start_ns": 200_000_000 + i * 2_000_000_000,
                "end_ns": 200_000_000 + (i + 1) * 2_000_000_000,
                "input_tokens": 246,
                "max_abs": 0.0,
                "vector": v,
                "baseline_vector": v[:],
            }
            for i, v in enumerate(vectors)
        ]
        report = {"status": "pass", "cases": rows, "queries": queries}
        oracle = {
            "status": "pass",
            "cases": {
                **{
                    "sintel-6s/" + str(i): {"vector": v, "encoder_tokens": 246}
                    for i, v in enumerate(vectors)
                },
                **{q["id"]: {"vector": q["vector"]} for q in queries},
            },
        }
        return report, oracle

    def test_exact_windows_queries_scores_and_rankings(self):
        report, oracle = self.reports()
        result = live.validate(report, oracle)
        self.assertTrue(result["stream_baseline_exact"])
        self.assertTrue(result["retrieval_agreement"]["pass"])

    def test_incomplete_reports_and_baseline_or_interval_drift_fail(self):
        report, oracle = self.reports()
        for mutate in (
            lambda r: r.update(status="running"),
            lambda r: r["cases"].pop(),
            lambda r: r["cases"][1].update(start_ns=0),
            lambda r: r["cases"][1].update(input_tokens=247),
            lambda r: r["cases"][1].update(baseline_vector=[0.1, 0.9, 0.0]),
            lambda r: r["queries"][0].update(vector=[0.6, 0.8, 0.0]),
        ):
            bad = copy.deepcopy(report)
            mutate(bad)
            with self.assertRaises(ValueError):
                live.validate(bad, oracle)


if __name__ == "__main__":
    unittest.main()
