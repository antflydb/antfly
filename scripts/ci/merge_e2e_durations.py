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

"""Merge disjoint lane observations into next run's common duration baseline."""

import argparse
import json
from pathlib import Path

from zig_e2e_shard import load_history_entries


def merge(baseline, observations):
    result = json.loads(Path(baseline).read_text())
    original = load_history_entries(baseline)
    result["tests"] = dict(original)
    seen = set()
    for path in sorted(observations):
        for node, entry in load_history_entries(path).items():
            # Each lane starts from the same baseline; unchanged entries aren't
            # observations and must not overwrite another lane's new sample.
            if entry.get("samples", 0) <= original.get(node, {}).get("samples", 0):
                continue
            if node in seen:
                raise ValueError(f"E2E test observed in multiple lanes: {node}")
            seen.add(node)
            result["tests"][node] = entry
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--observations", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = merge(args.baseline, Path(args.observations).rglob("durations.json"))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
