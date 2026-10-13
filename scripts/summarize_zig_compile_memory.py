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
import shlex
import shutil

GIB = 1024**3


def expand_response_files(command, response_dir=None, cache=None, capture_dir=None):
    """Retain response files with the trace; missing arguments must fail closed."""
    if " @" not in command:
        return command
    cache = {} if cache is None else cache

    def expand(arguments, active=()):
        result = []
        for argument in arguments:
            if not argument.startswith("@"):
                result.append(argument)
                continue
            path = Path(argument[1:])
            if not path.is_file() and response_dir is not None:
                path = Path(response_dir) / path.name
            path = path.resolve()
            if path in active or len(active) >= 8:
                raise ValueError(f"recursive compiler response file: {path}")
            if path not in cache:
                try:
                    content = path.read_text()
                    cache[path] = shlex.split(content)
                    if capture_dir is not None:
                        destination = Path(capture_dir) / path.name
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if destination.exists() and destination.read_text() != content:
                            raise ValueError(
                                f"response file basename collision: {path.name}"
                            )
                        if destination.resolve() != path:
                            shutil.copyfile(path, destination)
                except OSError as error:
                    raise ValueError(
                        f"compiler response file unavailable: {path}; retain it or use --response-dir"
                    ) from error
            result.extend(expand(cache[path], (*active, path)))
        return result

    return shlex.join(expand(shlex.split(command)))


def summarize(text, response_dir=None, capture_dir=None):
    units = {}
    samples = {}
    precise_samples = True
    response_cache = {}
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
        expanded = expand_response_files(
            command, response_dir, response_cache, capture_dir
        )
        match = re.search(
            r"(?:^|\s)--name\s+(antfly(?:-[\w-]+)?|libantfly)(?:\s|$)",
            expanded,
        )
        if not match:
            continue
        unit = (
            "storage_kernel"
            if match[1] == "antfly-storage-kernel"
            else (
                match[1].removeprefix("antfly-runtime-")
                if match[1].startswith("antfly-runtime-")
                else match[1].removeprefix("antfly-").replace("-", "_")
            )
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
    parser.add_argument(
        "--response-dir", type=Path, help="Directory of retained Zig response files"
    )
    parser.add_argument(
        "--capture-response-dir",
        type=Path,
        help="Copy compiler response files for portable trace attribution",
    )
    args = parser.parse_args()
    report = summarize(
        args.trace.read_text(), args.response_dir, args.capture_response_dir
    )
    report["trace"] = str(args.trace.resolve())
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
