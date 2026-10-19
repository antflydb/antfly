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
import pstats
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from lexical_reuse import signature
from retained import Payloads, View
from registered import Client
from session_compare import cpu_seconds


def new_profile():
    # This directory also has the existing regex driver profile.py. Do not let
    # it shadow the stdlib profile module imported internally by cProfile.
    paths = sys.path[:]
    try:
        directory = Path(__file__).resolve().parent
        sys.path[:] = [p for p in paths if Path(p).resolve() != directory]
        import cProfile
    finally:
        sys.path[:] = paths
    return cProfile.Profile()


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
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Untimed consumer profiles and amortized producer CPU controls",
    )
    parser.add_argument("--cpu-rounds", type=int, default=32)
    parser.add_argument(
        "--capture-rounds",
        type=int,
        default=0,
        help="Additional paired cold-process captures (filesystem cache stays warm)",
    )
    args = parser.parse_args()
    if (
        args.rounds < 3
        or min(args.files, args.dense_lines, args.cpu_rounds) < 1
        or args.capture_rounds < 0
    ):
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
            modes = ("baseline_v2", "current_v2", "baseline_v3", "current_v3")
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
                    for mode in modes[n % len(modes) :] + modes[: n % len(modes)]:
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
                if args.profile and name == "dense":
                    controls = report["dense_cpu_trials"] = []
                    for batch in range(3):
                        trial = {}
                        for mode in modes[batch:] + modes[:batch]:
                            client = clients[mode]
                            before = cpu_seconds(client.process.pid)
                            cpu, start = time.process_time(), time.monotonic()
                            for _ in range(args.cpu_rounds):
                                delivery(client, handles[mode])
                            elapsed, consumer = (
                                time.monotonic() - start,
                                time.process_time() - cpu,
                            )
                            after = cpu_seconds(client.process.pid)
                            trial[mode] = {
                                "seconds_per_request": elapsed / args.cpu_rounds,
                                "consumer_cpu_per_request": consumer / args.cpu_rounds,
                                "producer_cpu_per_request": None
                                if before is None
                                else (after - before) / args.cpu_rounds,
                            }
                        controls.append(trial)
                    report["dense_cpu_controls"] = {
                        mode: {
                            key: statistics.median(t[mode][key] for t in controls)
                            if controls[0][mode][key] is not None
                            else None
                            for key in controls[0][mode]
                        }
                        for mode in modes
                    }
                    report["consumer_profiles"] = {}
                    for mode in modes:
                        client = clients[mode]
                        profile = new_profile()
                        profile.runcall(delivery, client, handles[mode])
                        stats = pstats.Stats(profile)
                        report["consumer_profiles"][mode] = [
                            {
                                "function": f"{Path(key[0]).name}:{key[1]}:{key[2]}",
                                "calls": value[1],
                                "self_seconds": value[2],
                                "cumulative_seconds": value[3],
                            }
                            for key, value in sorted(
                                stats.stats.items(),
                                key=lambda entry: entry[1][2],
                                reverse=True,
                            )[:20]
                        ]
            finally:
                for client in clients.values():
                    client.close()
        (root / "src/dense.txt").unlink()
        for n in range(args.files):
            (root / "src" / f"f{n:05}.txt").write_text("ordinary noise\n" * 8)
        (root / "src/f00000.txt").write_text("STOP stable\n")
        if args.capture_rounds:
            report["capture_trials"] = []
            for n in range(args.capture_rounds):
                trial = {}
                for mode in (
                    ("baseline", "current") if n % 2 else ("current", "baseline")
                ):
                    client = Client(
                        binaries[mode], root, "src", [], env, ("--workspace-watch",)
                    )
                    try:
                        trial[mode] = {
                            "seconds": client.capture_seconds,
                            "producer_cpu_seconds": cpu_seconds(client.process.pid),
                            "resident_kib": resident_kib(client),
                            "revision": client.ready["revision"],
                        }
                    finally:
                        client.close()
                assert trial["baseline"]["revision"] == trial["current"]["revision"]
                report["capture_trials"].append(trial)
        clients, handles = {}, {}
        oracle = Client(binaries["current"], root, "src", [], env)
        report["freshness_oracle"] = (
            "Independent conservative process; no forced capture mutates either timed client's generation or caches."
        )
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
                _, rows = oracle.exchange("refresh", force_rescan=True)
                assert rows[0]["revision"] == trial["current"]["ready"]["revision"]
                if n >= 0:
                    trials.append(trial)
        finally:
            for client in clients.values():
                client.close()
            oracle.close()
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
