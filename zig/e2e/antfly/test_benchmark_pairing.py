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

"""Statistical contracts for the benchmark drivers; no server required."""

import math
import pytest
from benchmark_pairing import summarize, validate_rounds


def test_position_effect_cancels_without_counting_queries_as_trials():
    samples = []
    for index in range(8):
        for variant in ("baseline", "candidate"):
            first = "baseline" if index % 2 == 0 else "candidate"
            cost = (
                100
                * (2 if variant != first else 1)
                * (0.5 if variant == "candidate" else 1)
            )
            for _ in range(100):
                samples.append({"round": index, "variant": variant, "elapsed_ns": cost})
    result = summarize(samples)["comparisons"][0]
    metric = result["metrics"]["elapsed_ns"]
    assert result["balanced_blocks"] == 4
    assert metric["independent_runs_per_variant"] == 8
    assert metric["candidate_over_baseline"] == pytest.approx(0.5)
    assert metric["bootstrap_95_interval"] == pytest.approx([0.5, 0.5])
    assert metric["position_medians"] == {
        "baseline": [100, 200],
        "candidate": [50, 100],
    }
    assert {sample["position"] for sample in samples} == {0, 1}


def test_independent_blocks_retain_uncertainty_and_grouping():
    samples = [
        {
            "round": index,
            "variant": variant,
            "phase": phase,
            "elapsed_ns": 100 * (1 + index // 2 if variant == "candidate" else 1),
        }
        for phase in ["full", "conditional"]
        for index in range(8)
        for variant in ["baseline", "candidate"]
    ]
    for comparison in summarize(samples, ("phase",))["comparisons"]:
        metric = comparison["metrics"]["elapsed_ns"]
        assert metric["candidate_over_baseline"] == pytest.approx(math.pow(24, 0.25))
        low, high = metric["bootstrap_95_interval"]
        assert low < metric["candidate_over_baseline"] < high


def test_unbalanced_timing_rejected_but_attribution_is_explicit():
    for count in [0, 1, 3, 5, 7]:
        with pytest.raises(ValueError):
            validate_rounds(count)
    validate_rounds(1, attribution=True)
    assert summarize([], attribution=True)["attribution_only"]
    with pytest.raises(ValueError):
        summarize(
            [
                {"round": 0, "variant": "baseline", "elapsed_ns": 1},
                {"round": 1, "variant": "baseline", "elapsed_ns": 1},
            ]
        )
