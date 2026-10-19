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

"""Isolate native v2/v3 encoding with a buffered raw-byte consumer.

Validate complete decoded results before timing. Timed requests drain every
frame and validate summaries/termination, but decode only control records.
Producer CPU includes workers; raw-drain elapsed is not editor/client latency.
Use enough requests to amortize 10 ms process CPU counter quantization.
"""

import argparse
import hashlib
import io
import json
import os
import platform
import signal
import statistics
import tempfile
import time
from pathlib import Path

from lexical_reuse import signature
from registered import Client
from session_compare import cpu_seconds
from transport_compare import delivery


def timeout(*_):
    raise TimeoutError("native encoding control exceeded 120 seconds")


def measure(client, handle, requests, expected):
    assert client.cursor == len(client.pending)
    request = (
        json.dumps(
            {
                "version": 1,
                "id": 1,
                "op": "search",
                "revision": client.ready["revision"],
                "query_handle": handle,
                "if_result_token": "0" * 64,
                "delta": True,
            }
        ).encode()
        + b"\n"
    )
    reader = io.BufferedReader(client.process.stdout, 65536)
    previous = signal.signal(signal.SIGALRM, timeout)
    wire = 0
    try:
        signal.setitimer(signal.ITIMER_REAL, 120)
        before = cpu_seconds(client.process.pid)
        started = time.monotonic()
        for _ in range(requests):
            client.process.stdin.write(request)
            client.process.stdin.flush()
            summary = None
            while True:
                row = reader.readline()
                if not row:
                    raise RuntimeError("producer closed before encoding control ended")
                wire += len(row)
                if b'"type":"summary"' in row[:48]:
                    summary = json.loads(row)
                if b'"type":"workspace_error"' in row[:48]:
                    raise RuntimeError(row)
                if b'"type":"workspace_end"' in row[:48]:
                    terminal = json.loads(row)
                    if (
                        terminal["exit_code"] != 0
                        or summary is None
                        or not summary["stats"]["complete"]
                    ):
                        raise RuntimeError("incomplete encoding control")
                    for key, value in expected.items():
                        if summary["stats"][key] != value:
                            raise RuntimeError(f"encoding control changed {key}")
                    break
        elapsed = time.monotonic() - started
        after = cpu_seconds(client.process.pid)
        return {
            "seconds_per_request": elapsed / requests,
            "producer_cpu_per_request": (after - before) / requests,
            "wire_bytes_per_request": wire / requests,
        }
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        reader.detach()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--requests", type=int, default=128)
    parser.add_argument("--lines", type=int, default=20000)
    args = parser.parse_args()
    if (
        platform.system() not in ("Darwin", "Linux")
        or args.rounds < 3
        or args.requests < 32
        or args.lines < 1
    ):
        parser.error(
            "Darwin/Linux, at least three rounds, 32 requests and one line required"
        )
    binaries = {
        "baseline": args.baseline.resolve(),
        "candidate": args.candidate.resolve(),
    }
    modes = ("baseline_v2", "candidate_v2", "baseline_v3", "candidate_v3")
    report = {
        "measurement": __doc__,
        "host": platform.platform(),
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "binaries": {
            name: hashlib.sha256(p.read_bytes()).hexdigest()
            for name, p in binaries.items()
        },
        "requests_per_control": args.requests,
        "lines": args.lines,
        "trials": [],
    }
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    cache = Path(__file__).resolve().parents[4] / ".zig-cache"
    with tempfile.TemporaryDirectory(prefix="grep-encoding-", dir=cache) as directory:
        root = Path(directory)
        (root / "src").mkdir()
        (root / "src/dense.txt").write_text("STOP é original\n" * args.lines)
        env.update(HOME=directory, XDG_CONFIG_HOME=directory)
        reference = None
        for n in range(args.rounds):
            trial = {}
            offset = n % len(modes)
            for mode in modes[offset:] + modes[:offset]:
                client = Client(binaries[mode.split("_")[0]], root, "src", [], env)
                try:
                    _, rows = client.exchange(
                        "register",
                        args=["STOP", "--threads=1"],
                        batches=mode.endswith("v3"),
                    )
                    handle = rows[0]["query_handle"]
                    result, view = delivery(client, handle)
                    actual = signature(view.materialize())
                    if reference is None:
                        reference = actual
                    assert (
                        actual == reference
                        and result["logical_stats"]["matches"] == args.lines
                    )
                    trial[mode] = measure(
                        client, handle, args.requests, result["logical_stats"]
                    )
                finally:
                    client.close()
            report["trials"].append(trial)
    report["median"] = {
        mode: {
            key: statistics.median(t[mode][key] for t in report["trials"])
            for key in report["trials"][0][mode]
        }
        for mode in modes
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median"], indent=2))


if __name__ == "__main__":
    main()
