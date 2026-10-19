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

"""Paired workspace transport and refresh-plus-delivery qualification.

Warm shared host, alternating modes; exact match/context/provenance/statistics
verification follows each timed pair. Refresh includes source observation and
query delivery, rather than reporting query-only incremental speedups.
"""

import argparse
import gc
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from lexical_reuse import signature
from retained import Payloads, View
from registered import Client


def delivery(client, handle):
    payloads = Payloads()
    result, rows = client.exchange(
        "search",
        revision=client.ready["revision"],
        query_handle=handle,
        if_result_token="0" * 64,
        delta=True,
        consume=payloads.consume,
    )
    stats = result["summary"]["stats"]
    result["logical_stats"] = {
        key: stats[key]
        for key in (
            "files",
            "bytes",
            "matches",
            "occurrences",
            "occurrences_exact",
            "files_with_matches",
            "errors",
            "skipped_paths",
            "invalid_utf8_files",
            "complete",
        )
    }
    view = View(handle)
    view.apply(
        rows, client.ready["revision"], client.ready["observed_at"], payloads=payloads
    )
    return result, view


def resident_kib(client):
    """Untimed resident-set sample; includes allocator/kernel mapping effects."""
    status = Path(f"/proc/{client.process.pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
        return None
    if platform.system() == "Darwin":
        return int(
            subprocess.check_output(["ps", "-o", "rss=", "-p", str(client.process.pid)])
        )
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rounds", type=int, default=9)
    parser.add_argument("--files", type=int, default=6000)
    parser.add_argument("--dense-lines", type=int, default=20000)
    args = parser.parse_args()
    if args.rounds < 3 or min(args.files, args.dense_lines) < 1:
        parser.error("at least three rounds and positive sizes required")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "parameters": vars(args)
        | {
            "current": str(args.current),
            "baseline": str(args.baseline),
            "output": str(args.output),
        },
        "binaries": {
            k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in binaries.items()
        },
        "trials": {},
        "capture": {},
        "limits": "Warm shared macOS host or ARM64 Docker VM; captures sample startup, not cold storage. Consumer CPU excludes producer CPU. RSS samples are untimed and exclude kernel descriptor storage. No independent native Linux, Windows runtime or live-provider qualification.",
    }
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    cache = Path(__file__).resolve().parents[4] / ".zig-cache"
    cache.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="grep-transport-", dir=cache) as directory:
        root = Path(directory)
        (root / "src").mkdir()
        env.update(HOME=directory, XDG_CONFIG_HOME=directory)
        for name, count in (("tiny", 1), ("dense", args.dense_lines)):
            (root / "src/dense.txt").write_text("STOP é original\n" * count)
            clients, handles = {}, {}
            modes = ("baseline_v2", "current_v2", "current_v3")
            trials = report["trials"][name] = []
            try:
                for mode in modes:
                    key = mode.split("_")[0]
                    client = clients[mode] = Client(binaries[key], root, "src", [], env)
                    _, rows = client.exchange(
                        "register",
                        args=["STOP", "--threads=1"],
                        **({"batches": True} if mode.endswith("v3") else {}),
                    )
                    handles[mode] = rows[0]["query_handle"]
                    delivery(client, handles[mode])
                for n in range(-1, args.rounds):
                    gc.collect()
                    views, trial = {}, {}
                    for mode in modes[n % 3 :] + modes[: n % 3]:
                        cpu, start = time.process_time(), time.monotonic()
                        result, view = delivery(clients[mode], handles[mode])
                        trial[mode] = {
                            "seconds": time.monotonic() - start,
                            "consumer_cpu_seconds": time.process_time() - cpu,
                            "first_event_seconds": result["first_event_seconds"],
                            "producer_seconds": result["producer_seconds"],
                            "wire_bytes": result["response_bytes"],
                            "logical_stats": result["logical_stats"],
                            "retained_payload_bytes": sum(
                                map(len, view.files.values())
                            ),
                        }
                        views[mode] = view
                    assert all(
                        trial[mode]["logical_stats"] == trial[modes[0]]["logical_stats"]
                        for mode in modes
                    )
                    reference = signature(views[modes[0]].materialize())
                    assert all(
                        signature(v.materialize()) == reference for v in views.values()
                    )
                    if n >= 0:
                        trials.append(trial)
            finally:
                for client in clients.values():
                    client.close()
        (root / "src/dense.txt").unlink()
        for n in range(args.files):
            (root / "src" / f"f{n:05}.txt").write_text("ordinary noise\n" * 8)
        (root / "src/f00000.txt").write_text("STOP stable\n")
        clients, handles = {}, {}
        trials = report["trials"]["refresh_and_search"] = []
        try:
            for mode, binary in binaries.items():
                client = clients[mode] = Client(
                    binary, root, "src", [], env, ("--workspace-watch",)
                )
                report["capture"][mode] = {
                    "seconds": client.capture_seconds,
                    "resident_kib": resident_kib(client),
                    "ready": client.ready,
                }
                _, rows = client.exchange("register", args=["STOP", "--threads=1"])
                handles[mode] = rows[0]["query_handle"]
                delivery(client, handles[mode])
            for n in range(-1, args.rounds):
                if n % 3 == 1:
                    (root / "src/added.txt").write_text(f"STOP added {n}\n")
                elif n % 3 == 2:
                    (root / "src/added.txt").unlink(missing_ok=True)
                (root / "src" / f"f{args.files - 1:05}.txt").write_text(
                    f"STOP edited {n}\n"
                )
                views, trial = {}, {}
                for mode in (
                    ("baseline", "current") if n % 2 else ("current", "baseline")
                ):
                    client = clients[mode]
                    start, cpu = time.monotonic(), time.process_time()
                    refresh, _ = client.exchange("refresh")
                    result, view = delivery(client, handles[mode])
                    trial[mode] = {
                        "seconds": time.monotonic() - start,
                        "consumer_cpu_seconds": time.process_time() - cpu,
                        "refresh_seconds": refresh["seconds"],
                        "query_seconds": result["seconds"],
                        "ready": client.ready,
                        "logical_stats": result["logical_stats"],
                    }
                    views[mode] = view
                assert (
                    trial["baseline"]["logical_stats"]
                    == trial["current"]["logical_stats"]
                )
                assert (
                    clients["baseline"].ready["revision"]
                    == clients["current"].ready["revision"]
                )
                assert signature(views["baseline"].materialize()) == signature(
                    views["current"].materialize()
                )
                # Forced reads are an untimed oracle for witness correctness.
                _, rows = clients["current"].exchange("refresh", force_rescan=True)
                assert rows[0]["revision"] == trial["current"]["ready"]["revision"]
                if n >= 0:
                    trials.append(trial)
        finally:
            for client in clients.values():
                client.close()
    report["median"] = {
        workload: {
            mode: {
                key: statistics.median(t[mode][key] for t in trials)
                for key in trials[0][mode]
                if isinstance(trials[0][mode][key], (int, float))
            }
            for mode in trials[0]
        }
        for workload, trials in report["trials"].items()
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median"], indent=2))


if __name__ == "__main__":
    main()
