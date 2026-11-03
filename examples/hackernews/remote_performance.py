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

"""Run a resumable, matched local/GCS and stored-source qualification matrix.

Use --run explicitly; otherwise emit the plan. Execute this with the same Python
runtime/dependencies as poc.py. Each cell gets an independent publication/state.
Credentials remain managed by poc.py; this runner never reads or records tokens.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time


def fingerprint(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def distribution(samples):
    if not samples:
        return None
    ordered = sorted(samples)
    return {
        "samples_ms": samples,
        "median_ms": statistics.median(samples),
        "p95_ms": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "max_ms": ordered[-1],
    }


def summarize(report):
    return {
        "rows": report["row_count"],
        "publication_seconds": report["ready_seconds"],
        "cold_ms": report["first_search_ms"],
        "warm": distribution(report["warm_samples_ms"]),
        "restart_ms": report["after_restart_ms"],
        "store_source": report["store_source"],
        "verified": report["verified"],
        "io_profiles": report["io_profiles"],
        "top_hits": [
            (hit["_source"]["hn_id"], hit["_score"])
            for hit in report["first_response"]["hits"]["hits"]
        ],
        "composed": {
            name: {
                "cold_ms": value["first_search_ms"],
                "warm": distribution(value["warm_samples_ms"]),
            }
            for name, value in report["composed_benchmarks"].items()
        },
    }


def plan(args):
    harness = {
        name: fingerprint(Path(__file__).with_name(name))
        for name in ("poc.py", "remote_performance.py")
    }
    fixture = [
        {
            "path": str(p.relative_to(args.filesystem_source)),
            "bytes": p.stat().st_size,
            "sha256": fingerprint(p),
        }
        for p in sorted((args.filesystem_source / "archive").glob("*.parquet"))
    ]
    if not fixture:
        raise ValueError("The local fixture must contain archive/*.parquet")
    cells = []
    for binding in args.binary:
        label, separator, filename = binding.partition("=")
        if (
            not separator
            or not label
            or any(
                c
                not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
                for c in label
            )
        ):
            raise ValueError("--binary must be a safe LABEL=/absolute/binary binding")
        binary = Path(filename).resolve(strict=True)
        digest = fingerprint(binary)
        for provider in args.providers:
            for stored in (True, False):
                for cycle in range(args.cycles):
                    name = f"{label}-{provider}-stored-{int(stored)}-{cycle + 1}"
                    state = args.state.resolve() / name
                    command = [
                        sys.executable,
                        str(Path(__file__).with_name("poc.py")),
                        "--binary",
                        str(binary),
                        "--state",
                        str(state),
                        "--port",
                        str(args.port),
                        "--expected-rows",
                        str(args.expected_rows),
                        "--repeats",
                        str(args.repeats),
                        "--process-memory-budget-mb",
                        str(args.memory_mb),
                        "--require-filters",
                        "--cold-cache",
                        "--disable-navigation-warming",
                        "--store-source" if stored else "--no-store-source",
                        "--cursor-retention-ms",
                        "600000",
                        "--cursor-pages",
                        "8",
                        "--cursor-term",
                        "the",
                        "--qualify-composed-sources",
                        "--benchmark-composed",
                        "--composed-current-rows",
                        str(args.current_rows),
                        "--build-timeout",
                        "1200",
                    ]
                    if args.diagnose_warm:
                        command.append("--diagnose-warm")
                    if provider == "local":
                        command.extend(
                            [
                                "--filesystem-source",
                                str(args.filesystem_source.resolve()),
                            ]
                        )
                    else:
                        if (
                            not args.gcs_source_prefix
                            or not args.gcs_artifact_prefix
                            or not args.bucket
                        ):
                            raise ValueError(
                                "GCS requires bucket, source prefix and a fresh artifact prefix"
                            )
                        source = args.gcs_source_prefix.strip("/")
                        artifacts = args.gcs_artifact_prefix.strip("/")
                        if (
                            not artifacts
                            or not source
                            or artifacts == source
                            or source.startswith(artifacts + "/")
                            or artifacts.startswith(source + "/")
                        ):
                            raise ValueError(
                                "Source and artifact prefixes must be disjoint"
                            )
                        command.extend(
                            [
                                "--project",
                                args.project,
                                "--bucket",
                                args.bucket,
                                "--prefix",
                                source,
                                "--artifact-prefix",
                                artifacts + "/" + name,
                            ]
                        )
                    cells.append(
                        {
                            "name": name,
                            "binary_sha256": digest,
                            "harness_sha256": harness,
                            "fixture": fixture,
                            "fixture_root": str(args.filesystem_source.resolve()),
                            "provider": provider,
                            "store_source": stored,
                            "command": command,
                            "state": str(state),
                        }
                    )
    if len({c["name"] for c in cells}) != len(cells):
        raise ValueError("Duplicate binary labels")
    # Pair identical scenarios and alternate binary order on successive cycles.
    # A resumed cell retains its exact recipe regardless of schedule position.
    labels = [binding.partition("=")[0] for binding in args.binary]

    def schedule(cell):
        label, provider, _, stored, cycle = cell["name"].rsplit("-", 4)
        ordinal = labels.index(label)
        return (
            int(cycle),
            args.providers.index(provider),
            -int(stored),
            ordinal if int(cycle) % 2 else -ordinal,
        )

    cells.sort(key=schedule)
    return cells


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def execute(cells, root):
    root.mkdir(parents=True, exist_ok=True)
    # One runner owns this matrix. A stale lock requires operator inspection;
    # never silently run two engines against the same state or reservation.
    lock = root / "matrix.lock"
    descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    try:
        for cell in cells:
            state = Path(cell["state"])
            state.mkdir(parents=True, exist_ok=True)
            recipe = state / "matrix-cell.json"
            if recipe.exists() and json.loads(recipe.read_text()) != cell:
                raise ValueError("Refusing changed matrix cell: " + cell["name"])
            write_json(recipe, cell)
            for name, digest in cell["harness_sha256"].items():
                if fingerprint(Path(__file__).with_name(name)) != digest:
                    raise ValueError("Harness changed after planning: " + name)
            for item in cell["fixture"]:
                source = Path(cell["fixture_root"]) / item["path"]
                if fingerprint(source) != item["sha256"]:
                    raise ValueError("Fixture changed after planning: " + cell["name"])
            if (
                fingerprint(
                    Path(cell["command"][cell["command"].index("--binary") + 1])
                )
                != cell["binary_sha256"]
            ):
                raise ValueError("Binary changed after planning: " + cell["name"])
            status_path = state / "matrix-result.json"
            if status_path.exists():
                if json.loads(status_path.read_text()).get("status") == "passed":
                    continue
                raise ValueError(
                    "Incomplete matrix cell requires fresh state and artifact prefix: "
                    + cell["name"]
                    + "; preserve the failed attempt as evidence"
                )
            if any(path.name != "matrix-cell.json" for path in state.iterdir()):
                raise ValueError("Unmeasured existing engine state: " + cell["name"])
            status = {
                "status": "running",
                "started_at": time.time(),
                "load_before": os.getloadavg(),
            }
            write_json(status_path, status)
            with (state / "matrix.log").open("a") as log:
                child = subprocess.Popen(
                    cell["command"],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    code = child.wait(timeout=3600)
                except BaseException:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
                    status.update(status="interrupted", finished_at=time.time())
                    write_json(status_path, status)
                    raise
            status.update(
                status="passed" if code == 0 else "failed",
                returncode=code,
                finished_at=time.time(),
                load_after=os.getloadavg(),
            )
            if code == 0:
                status["measurements"] = summarize(
                    json.loads((state / "report.json").read_text())
                )
            write_json(status_path, status)
            print(
                json.dumps({"cell": cell["name"], "status": status["status"]}),
                flush=True,
            )
            if code:
                raise RuntimeError(
                    "Qualification failed; retained log/state: " + str(state)
                )
        results = [
            json.loads((Path(cell["state"]) / "matrix-result.json").read_text())
            for cell in cells
        ]
        expected = results[0]["measurements"]["top_hits"]
        if any(result["measurements"]["top_hits"] != expected for result in results):
            raise RuntimeError(
                "Matched cells disagree on top HN IDs/scores; results retained"
            )
        write_json(
            root / "matrix-summary.json",
            {
                "complete": True,
                "cells": [
                    dict(cell=cell["name"], **result)
                    for cell, result in zip(cells, results)
                ],
                "scope": "Serial local/GCS diagnostic matrix; OS caches are not flushed. Logical object counters include background work and nested calls.",
            },
        )
    finally:
        lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        action="append",
        required=True,
        help="LABEL=/absolute/binary; repeat for before/after",
    )
    parser.add_argument("--filesystem-source", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument(
        "--providers", nargs="+", choices=["local", "gcs"], default=["local", "gcs"]
    )
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--bucket")
    parser.add_argument("--gcs-source-prefix")
    parser.add_argument("--gcs-artifact-prefix")
    parser.add_argument("--port", type=int, default=18977)
    parser.add_argument("--expected-rows", type=int, default=10000)
    parser.add_argument("--current-rows", type=int, default=128)
    parser.add_argument("--memory-mb", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--diagnose-warm", action="store_true")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if (
        min(
            args.repeats,
            args.cycles,
            args.expected_rows,
            args.memory_mb,
            args.current_rows,
        )
        <= 0
    ):
        parser.error("counts and budgets must be positive")
    cells = plan(args)
    if args.run:
        execute(cells, args.state.resolve())
    else:
        print(json.dumps(cells, indent=2))


if __name__ == "__main__":
    main()
