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

"""Compare experimental L2 e-means against Hilbert and Lloyd HBC builders.

Run from zig/ after building hbc_read_bench. Saves raw JSONL and macOS process
peak RSS; RSS includes dataset, index, queries and storage, not only clustering.
"""

import argparse
import json
import platform
import re
import statistics
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="zig-out/bin/hbc_read_bench")
    parser.add_argument(
        "--output", type=Path, default=Path(".zig-cache/emeans-results")
    )
    parser.add_argument(
        "--build", choices=("bulk_build", "online_coalesced"), default="bulk_build"
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--vectors", type=int, default=10000)
    parser.add_argument("--dims", type=int, default=128)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--search-width", type=int, default=16)
    args = parser.parse_args()
    if (
        min(
            args.vectors,
            args.dims,
            args.samples,
            args.queries,
            args.search_width,
            args.batch_size,
        )
        <= 0
    ):
        parser.error(
            "counts, dimensions, search width, and batch size must be positive"
        )
    if not Path(args.binary).is_file():
        parser.error(f"benchmark binary not found: {args.binary}")
    args.output.mkdir(parents=True, exist_ok=True)
    summary = []
    for algorithm in ("hilbert_seeded", "kmeans", "emeans"):
        command = [
            str(Path(args.binary).resolve()),
            "hbc-read",
            "--metric",
            "l2_squared",
            "--storage",
            "memory",
            "--build",
            args.build,
            "--batch-size",
            str(args.batch_size),
            "--kmeans-backend",
            "cpu",
            "--vectors",
            str(args.vectors),
            "--dims",
            str(args.dims),
            "--samples",
            str(args.samples),
            "--queries",
            str(args.queries),
            "--recall-queries",
            str(args.queries),
            "--seed",
            str(args.seed),
            "--search-width",
            str(args.search_width),
        ]
        if algorithm != "hilbert_seeded":
            command += ["--bulk-build-" + algorithm]
        timed = platform.system() == "Darwin"
        if timed:
            command = ["/usr/bin/time", "-l"] + command
        result = subprocess.run(command, text=True, capture_output=True)
        (args.output / (algorithm + ".jsonl")).write_text(result.stdout)
        (args.output / (algorithm + ".stderr")).write_text(result.stderr)
        if result.returncode:
            raise SystemExit(
                f"{algorithm} failed; see {args.output}/{algorithm}.stderr"
            )
        records = [
            json.loads(line)
            for line in result.stdout.splitlines()
            if line.startswith("{")
        ]
        builds = [r for r in records if r.get("workload") == "build"]
        reads = [r for r in records if r.get("workload") == "warm_query_no_metadata"]
        shapes = [r for r in records if r.get("workload") == "build_shape"]
        if not (len(builds) == len(reads) == len(shapes) == args.samples):
            raise RuntimeError(f"{algorithm}: incomplete benchmark output")
        if any(
            r["members"] != args.vectors or r["max_leaf_members"] > 128 for r in shapes
        ):
            raise RuntimeError(f"{algorithm}: invalid persisted leaf shape")
        rss = re.search(r"(\d+)\s+maximum resident set size", result.stderr)
        row = dict(
            algorithm=algorithm,
            build=args.build,
            batch_size=args.batch_size,
            command=command,
            vectors=args.vectors,
            dims=args.dims,
            samples=args.samples,
            seed=args.seed,
            search_width=args.search_width,
            build_ms=statistics.median(r["ns"] for r in builds) / 1e6,
            query_ns=statistics.median(r["ns"] / r["queries"] for r in reads),
            recall=statistics.median(r["recall_at_k"] for r in reads),
            query_p95_ns=statistics.median(r["latency_p95_ns"] for r in reads),
            shapes=shapes,
            peak_process_rss_bytes=int(rss[1]) if rss else None,
        )
        summary.append(row)
        print(json.dumps(row), flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
