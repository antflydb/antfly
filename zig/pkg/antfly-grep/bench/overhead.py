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

"""Repeated dense delivery, full/minimal acknowledgements and client omission.

Warm processes and filesystem data. Capture/refresh and CPU-counter sampling are
outside request timing. CPU is cumulative server process CPU divided by repeated
requests; Darwin counters have 10ms precision. Results include client decoding.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path

from lexical_reuse import signature
from registered import Client
from retained import RetainedClient
from session_compare import cpu_seconds


def batch(client, handle, rounds, fields):
    before_cpu = cpu_seconds(client.process.pid)
    before_bytes = client.wire_bytes
    started = time.monotonic()
    first = []
    for _ in range(rounds):
        result, rows = client.exchange(
            "search", revision=client.ready["revision"], query_handle=handle, **fields
        )
        if result["first_event_seconds"] is not None:
            first.append(result["first_event_seconds"])
    elapsed = time.monotonic() - started
    after_cpu = cpu_seconds(client.process.pid)
    return {
        "seconds_per_request": elapsed / rounds,
        "server_cpu_seconds_per_request": (after_cpu - before_cpu) / rounds
        if before_cpu is not None
        else None,
        "bytes_per_request": (client.wire_bytes - before_bytes) / rounds,
        "median_first_event_seconds": statistics.median(first) if first else None,
    }, rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=128)
    parser.add_argument("--trials", type=int, default=9)
    args = parser.parse_args()
    if min(args.files, args.rounds, args.trials) < 1:
        parser.error("positive sizes required")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "files": args.files,
        "rounds": args.rounds,
        "trials": [],
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "binaries": {
            k: {"sha256": hashlib.sha256(v.read_bytes()).hexdigest()}
            for k, v in binaries.items()
        },
    }
    with tempfile.TemporaryDirectory(prefix="grep-overhead-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        for i in range(args.files):
            (root / "src" / f"f{i:05}.txt").write_text(
                "STOP original\n" + "x" * 48 + "\n"
            )
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
        clients, handles, tokens, expected = {}, {}, {}, None
        try:
            for name, binary in binaries.items():
                client = clients[name] = Client(binary, root, "src", [], env)
                _, registered = client.exchange(
                    "register", args=["STOP", "--threads=2"]
                )
                handle = handles[name] = registered[0]["query_handle"]
                _, full = client.exchange(
                    "search", query_handle=handle, revision=client.ready["revision"]
                )
                actual = signature(full)
                if expected is not None and actual != expected:
                    raise RuntimeError("full output differs")
                expected = actual
                tokens[name] = next(
                    r["result_token"] for r in full if r["type"] == "result_token"
                )
            for trial in range(args.trials):
                result = {"load_average": os.getloadavg()}
                order = list(binaries) if trial % 2 else list(reversed(binaries))
                for name in order:
                    client, handle = clients[name], handles[name]
                    full, rows = batch(client, handle, args.rounds, {})
                    if signature(rows) != expected:
                        raise RuntimeError("repeated full output differs")
                    conditional, rows = batch(
                        client,
                        handle,
                        args.rounds * 32,
                        {"if_result_token": tokens[name]},
                    )
                    if not any(r["type"] == "workspace_unchanged" for r in rows):
                        raise RuntimeError("full acknowledgement missing")
                    result[name] = {"full": full, "conditional": conditional}
                    if name == "current":
                        minimal, rows = batch(
                            client,
                            handle,
                            args.rounds * 32,
                            {
                                "if_result_token": tokens[name],
                                "acknowledgement": "minimal",
                            },
                        )
                        if len(rows) != 1 or rows[0]["result_token"] != tokens[name]:
                            raise RuntimeError("minimal acknowledgement differs")
                        result[name]["minimal"] = minimal
                report["trials"].append(result)
        finally:
            for client in clients.values():
                client.close()
        retained = RetainedClient(binaries["current"], root, "src", [], env)
        try:
            _, registered = retained.exchange("register", args=["STOP"])
            handle = registered[0]["query_handle"]
            view, _ = retained.search_view(handle)
            if signature(view.materialize()) != expected:
                raise RuntimeError("retained view differs")
            before = retained.wire_bytes
            started = time.monotonic()
            for _ in range(args.rounds * 32):
                _, result = retained.search_view(handle)
                if result["requests"] or result["response_bytes"]:
                    raise RuntimeError("unchanged retained view performed I/O")
            report["client_omission"] = {
                "seconds_per_read": (time.monotonic() - started) / (args.rounds * 32),
                "requests": 0,
                "response_bytes": retained.wire_bytes - before,
            }
        finally:
            retained.close()
    report["median"] = {
        name: {
            case: {
                metric: statistics.median(
                    t[name][case][metric] for t in report["trials"]
                )
                for metric, value in report["trials"][0][name][case].items()
                if value is not None
            }
            for case in report["trials"][0][name]
        }
        for name in binaries
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median"], indent=2))
    print(json.dumps(report["client_omission"]))


if __name__ == "__main__":
    main()
