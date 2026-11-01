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

"""Balanced AB/BA comparisons; fresh server runs are the sampling units."""

import hashlib
import math
from pathlib import Path
import random
import statistics


def validate_rounds(rounds, *, attribution=False):
    if rounds < 1 or (not attribution and (rounds < 2 or rounds % 2)):
        raise ValueError(
            "paired timing requires an even number of rounds (at least two)"
        )


def summarize(samples, group_fields=(), *, attribution=False):
    for row in samples:
        first = "baseline" if row["round"] % 2 == 0 else "candidate"
        row["position"] = int(row["variant"] != first)
    result = {
        "sampling_unit": "fresh-server run; adjacent AB/BA rounds form one balanced block",
        "helper_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "attribution_only": attribution,
        "comparisons": [],
    }
    if attribution:
        return result
    groups = {}
    for row in samples:
        key = tuple(row.get(field) for field in group_fields)
        groups.setdefault(key, []).append(row)
    metrics = ("elapsed_ns", "server_cpu_ns", "peak_rss_bytes", "peak_footprint_bytes")
    for key, rows in groups.items():
        runs = {}
        for row in rows:
            runs.setdefault((row["round"], row["variant"]), []).append(row)
        rounds = sorted({index for index, _ in runs})
        validate_rounds(len(rounds))
        if rounds != list(range(len(rounds))):
            raise ValueError("comparison rounds must be contiguous from zero")
        comparison = dict(zip(group_fields, key))
        comparison["balanced_blocks"] = len(rounds) // 2
        comparison["metrics"] = {}
        for metric in metrics:
            values = {}
            for run, queries in runs.items():
                found = [query.get(metric) for query in queries]
                if any(value is None for value in found):
                    break
                values[run] = statistics.median(found)
            else:
                if any(
                    (index, variant) not in values
                    for index in rounds
                    for variant in ("baseline", "candidate")
                ):
                    raise ValueError("both variants are required in every round")
                if any(value <= 0 for value in values.values()):
                    continue
                logs = [
                    statistics.mean(
                        math.log(
                            values[(index, "candidate")] / values[(index, "baseline")]
                        )
                        for index in (start, start + 1)
                    )
                    for start in range(0, len(rounds), 2)
                ]
                rng = random.Random(0)
                estimates = sorted(
                    math.exp(statistics.mean(rng.choices(logs, k=len(logs))))
                    for _ in range(2000)
                )
                positions = {}
                for variant in ("baseline", "candidate"):
                    positions[variant] = [
                        statistics.median(
                            value
                            for (index, who), value in values.items()
                            if who == variant
                            and int(
                                who != ("baseline" if index % 2 == 0 else "candidate")
                            )
                            == position
                        )
                        for position in (0, 1)
                    ]
                comparison["metrics"][metric] = {
                    "candidate_over_baseline": math.exp(statistics.mean(logs)),
                    "block_ratios": [math.exp(value) for value in logs],
                    "bootstrap_95_interval": [estimates[49], estimates[1949]]
                    if len(logs) >= 2
                    else None,
                    "position_medians": positions,
                    "independent_runs_per_variant": len(rounds),
                }
        result["comparisons"].append(comparison)
    return result
