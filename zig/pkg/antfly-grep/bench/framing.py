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

"""Isolate JSONL framing cost with deterministic fragmented reads.

Compare a pinned repository reader with the working reader. Both use stdlib
JSON, identical byte chunks and mocked transport; these are CPU controls, not
end-to-end pipe/producer measurements. Every run verifies the decoded payload.
"""

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
import time
from pathlib import Path
from unittest.mock import patch

import session_compare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="Pinned Git revision")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=9)
    args = parser.parse_args()
    if args.rounds < 3:
        parser.error("at least three rounds required")
    source_path = "zig/pkg/antfly-grep/bench/session_compare.py"
    source = subprocess.check_output(["git", "show", f"{args.baseline}:{source_path}"])
    namespace = dict(vars(session_compare))
    namespace["__name__"] = "baseline_session_reader"
    # Run the trusted pinned module's definitions (never its CLI main) so its
    # decoder and framing implementation both remain genuine baseline code.
    exec(compile(source, "baseline-reader", "exec"), namespace)
    readers = {"baseline": namespace["Service"], "current": session_compare.Service}
    report = {
        "measurement": __doc__,
        "host": platform.platform(),
        "baseline": args.baseline,
        "baseline_reader_sha256": hashlib.sha256(source).hexdigest(),
        "current_reader_sha256": hashlib.sha256(
            Path(session_compare.__file__).read_bytes()
        ).hexdigest(),
        "chunk_bytes": 65536,
        "trials": {},
    }
    for size in ("dense", 1, 4, 16, 64):
        count = 20000 if size == "dense" else 1
        expected = {"text": "x" * (128 if size == "dense" else size * 1024 * 1024)}
        raw = (json.dumps(expected, separators=(",", ":")).encode() + b"\n") * count
        chunks = [raw[i : i + 65536] for i in range(0, len(raw), 65536)]
        trials = report["trials"][str(size)] = []
        for n in range(-1, args.rounds):
            trial = {}
            for name in ("baseline", "current") if n % 2 else ("current", "baseline"):
                reader = readers[name].__new__(readers[name])
                reader.pending = bytearray()
                reader.cursor = reader.wire_bytes = 0
                reader.process = type(
                    "Process",
                    (),
                    {"stdout": type("Out", (), {"fileno": lambda _: 17})()},
                )()
                stream = iter(chunks)
                with (
                    patch("session_compare.select.select", return_value=([17], [], [])),
                    patch(
                        "session_compare.os.read", side_effect=lambda *_: next(stream)
                    ),
                ):
                    start, cpu = time.monotonic(), time.process_time()
                    for _ in range(count):
                        result = reader.read()
                    trial[name] = {
                        "seconds": time.monotonic() - start,
                        "cpu_seconds": time.process_time() - cpu,
                    }
                assert result == expected
                assert reader.wire_bytes == len(raw)
            if n >= 0:
                trials.append(trial)
    report["median"] = {
        size: {
            mode: {
                key: statistics.median(t[mode][key] for t in trials)
                for key in ("seconds", "cpu_seconds")
            }
            for mode in readers
        }
        for size, trials in report["trials"].items()
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median"], indent=2))


if __name__ == "__main__":
    main()
