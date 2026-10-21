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

"""Paired exact edit deltas against a prior binary, including capture work."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path
from registered import Client, update_view


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("binary", type=Path)
    p.add_argument("baseline", type=Path)
    p.add_argument("--library", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--rounds", type=int, default=19)
    args = p.parse_args()
    paths = {"current": args.binary.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "fixture_only": True,
        "measurement": "Alternating paired exact edit deltas; capture and search timed separately and together; result reconstruction checked outside timing. One provider evaluation per edit. Warm filesystem, synthetic 128-file native fixture.",
        "binaries": {
            n: {
                "sha256": hashlib.sha256(b.read_bytes()).hexdigest(),
                "bytes": b.stat().st_size,
            }
            for n, b in paths.items()
        },
        "cases": [],
    }
    for maximum in (15, 128):
        with tempfile.TemporaryDirectory(prefix="grep-paired-delta-") as directory:
            root = Path(directory)
            (root / "src").mkdir()
            for i in range(128):
                (root / "src" / f"f{i:05}.py").write_text(
                    f"def retry_{i}():\n" + "    STOP()\n" * 32
                )
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
            }
            env.update(HOME=str(root), XDG_CONFIG_HOME=str(root / "config"))
            for n in paths:
                (root / n).mkdir()
            services = {}
            handles = {}
            tokens = {}
            views = {}
            try:
                for name, binary in paths.items():
                    query = [
                        "--ask=retry",
                        "--model=fixture",
                        "--backend=local",
                        f"--library={args.library.resolve()}",
                        f"--cache-dir={root / name}",
                        "--json-compact",
                        "--no-refine",
                        "--threads=2",
                        f"--max-result-files={maximum}",
                        "--max-requests=unlimited",
                    ]
                    service = Client(
                        binary, root, "src", query, env, ("--workspace-watch",)
                    )
                    services[name] = service
                    _, rows = service.exchange("register", args=query)
                    handles[name] = rows[0]["query_handle"]
                    _, rows = service.exchange(
                        "search",
                        revision=service.ready["revision"],
                        query_handle=handles[name],
                    )
                    tokens[name] = next(
                        r["result_token"] for r in rows if r["type"] == "result_token"
                    )
                    views[name] = update_view({}, rows)
                assert views["current"] == views["baseline"]
                trials = []
                for i in range(args.rounds):
                    (root / "src/f00000.py").write_text(
                        f"def retry_0(): # revision {i}\n" + "    STOP()\n" * 32
                    )
                    pair = {"load_average": os.getloadavg()}
                    for name in (
                        ["current", "baseline"] if i % 2 else ["baseline", "current"]
                    ):
                        service = services[name]
                        start = time.monotonic()
                        capture, _ = service.exchange("refresh")
                        result, rows = service.exchange(
                            "search",
                            revision=service.ready["revision"],
                            query_handle=handles[name],
                            if_result_token=tokens[name],
                            delta=True,
                        )
                        total = time.monotonic() - start
                        assert (
                            result["summary"]["stats"]["incremental_files_evaluated"]
                            == 1
                        )
                        assert result["summary"]["requests"] == 1
                        assert any(r["type"] == "workspace_delta" for r in rows)
                        views[name] = update_view(views[name], rows)
                        tokens[name] = next(
                            r["result_token"]
                            for r in rows
                            if r["type"] == "result_token"
                        )
                        pair[name] = {
                            "refresh_seconds": capture["seconds"],
                            "search_seconds": result["seconds"],
                            "total_seconds": total,
                            "response_bytes": result["response_bytes"],
                            "ready": capture["ready"],
                        }
                    assert views["current"] == views["baseline"]
                    trials.append(pair)
                report["cases"].append(
                    {
                        "max_result_files": maximum,
                        "trials": trials,
                        "median": {
                            name: {
                                key: statistics.median(t[name][key] for t in trials)
                                for key in (
                                    "refresh_seconds",
                                    "search_seconds",
                                    "total_seconds",
                                    "response_bytes",
                                )
                            }
                            for name in services
                        },
                    }
                )
            finally:
                for service in services.values():
                    service.close()
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
