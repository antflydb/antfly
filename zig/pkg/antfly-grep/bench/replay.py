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

"""Measure full replay, conditional transport, presentation reuse and refresh.

Uses a native constant-score fixture and pinned snapshots. Producer elapsed time
includes output backpressure, while client elapsed includes protocol/JSON work.
Neither is CPU time. Every full response must retain identical matched evidence.
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

from session_compare import Service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=51)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 9:
        parser.error("at least nine rounds required")
    binary, library = args.binary.resolve(), args.library.resolve()
    cache = Path(__file__).resolve().parents[3] / ".zig-cache"
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    with tempfile.TemporaryDirectory(prefix="replay-", dir=cache) as directory:
        root = Path(directory)
        source = root / "source"
        source.mkdir()
        for i in range(128):
            (source / f"f{i:05}.py").write_text(
                f"def retry_{i}():\n" + "    STOP()\n" * 32
            )
        env.update(HOME=str(root), XDG_CONFIG_HOME=str(root / "config"))
        query = [
            "--ask=retry",
            "--model=fixture",
            "--backend=local",
            f"--library={library}",
            f"--cache-dir={root / 'scores'}",
            "--json",
            "--no-refine",
            "--threads=1",
            "--max-result-files=15",
            "--max-requests=unlimited",
        ]
        service = Service(binary, root, "source", query, env, ("--workspace-watch",))
        runs = {name: [] for name in ("full", "conditional", "presentation")}
        refreshes = []
        try:
            _, expected = service.search(require_warm=False)
            if not expected:
                raise RuntimeError("fixture must produce selected output")
            token = next(
                r["result_token"] for r in service.rows if r["type"] == "result_token"
            )
            for i in range(args.rounds):
                order = (
                    ("full", "conditional") if i % 2 == 0 else ("conditional", "full")
                )
                for mode in order:
                    result, evidence = service.search(
                        token=token if mode == "conditional" else None
                    )
                    if result["summary"]["stats"]["query_cache_hits"] != 1:
                        raise RuntimeError(result)
                    if mode == "full" and evidence != expected:
                        raise RuntimeError("replay changed matched evidence")
                    if mode == "conditional" and (
                        evidence
                        or not any(
                            r["type"] == "workspace_unchanged" for r in service.rows
                        )
                    ):
                        raise RuntimeError(
                            "conditional replay did not acknowledge unchanged"
                        )
                    runs[mode].append({**result, "load_average": os.getloadavg()})
                # A unique presentation identity exercises per-file reuse every time.
                result, _ = service.search(query=[*query, f"--before-context={i + 1}"])
                stats = result["summary"]["stats"]
                if (
                    stats["query_cache_hits"]
                    or stats["file_evaluation_cache_hits"] != 128
                ):
                    raise RuntimeError(result)
                runs["presentation"].append(result)
            for i in range(10):
                force = i % 2 == 1
                started = time.monotonic()
                service.process.stdin.write(
                    json.dumps(
                        {
                            "version": 1,
                            "id": i,
                            "op": "refresh",
                            "force_rescan": force,
                        }
                    ).encode()
                    + b"\n"
                )
                service.process.stdin.flush()
                ready = service.read()
                if ready["type"] != "workspace_ready":
                    raise RuntimeError(ready)
                service.ready = ready
                refreshes.append(
                    {
                        "force_rescan": force,
                        "seconds": time.monotonic() - started,
                        "ready": ready,
                    }
                )
        finally:
            service.close()
        report = {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "library_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
            "fixture_only": True,
            "files": 128,
            "rounds": args.rounds,
            "median": {
                mode: {
                    field: statistics.median(row[field] for row in rows)
                    for field in ("seconds", "producer_seconds", "response_bytes")
                }
                for mode, rows in runs.items()
            },
            "refresh_median_seconds": {
                str(force): statistics.median(
                    r["seconds"] for r in refreshes if r["force_rescan"] == force
                )
                for force in (False, True)
            },
            "runs": runs,
            "refreshes": refreshes,
            "limitations": "Native macOS shared host, warm caches, synthetic selected results and constant-score fixture. Producer elapsed includes pipe backpressure, excludes option parsing/identity setup and summary serialization; client elapsed also includes JSON decoding. No live-provider, native Linux hardware or cold-cache performance claims. Watching is opt-in and refresh is explicit; force_rescan is conservative.",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: report[k] for k in ("median", "refresh_median_seconds")}))


if __name__ == "__main__":
    main()
