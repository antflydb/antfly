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

"""Summarize diagnose-zig-build-memory.sh samples; never rewrite reservations."""

import argparse
import json
import math
from pathlib import Path
import re

GIB = 1024**3


def summarize(text):
    units = {}
    samples = {}
    precise_samples = True
    for line in text.splitlines()[1:]:
        fields = line.split("\t")
        if len(fields) not in (6, 7):
            raise ValueError("expected six legacy or seven current TSV fields")
        _, elapsed, pid, rss_kib, _, command = fields[:6]
        size = int(rss_kib) * 1024
        if size < 0:
            raise ValueError("negative RSS")
        precise_samples &= len(fields) == 7
        sample = fields[6] if len(fields) == 7 else elapsed
        # Legacy traces can include several polls within one second. Their
        # per-PID maxima form an upper envelope, not a simultaneous RSS peak.
        processes = samples.setdefault(sample, {})
        processes[pid] = max(processes.get(pid, 0), size)
        match = re.search(
            r"(?:^|\s)--name\s+(antfly-storage-kernel|antfly-runtime-[\w-]+)(?:\s|$)",
            command,
        )
        if not match:
            continue
        unit = (
            "storage_kernel"
            if match[1] == "antfly-storage-kernel"
            else match[1].removeprefix("antfly-runtime-")
        )
        row = units.setdefault(unit, {"peak_rss_bytes": 0, "command": command})
        if size > row["peak_rss_bytes"]:
            row.update(peak_rss_bytes=size, command=command)
    for row in units.values():
        row["minimum_reservation_gib"] = math.ceil(
            row["peak_rss_bytes"] * 5 / (4 * GIB)
        )
    return {
        "format_version": 1,
        "units": units,
        "sampled_compiler_aggregate_peak_bytes": max(
            (sum(p.values()) for p in samples.values()), default=0
        ),
        "aggregate_is_same_poll": precise_samples,
        "reservation_headroom_percent": 25,
        "qualification": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = summarize(args.trace.read_text())
    report["trace"] = str(args.trace.resolve())
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
