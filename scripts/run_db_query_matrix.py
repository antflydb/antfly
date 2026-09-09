#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Compare DB query, analytics, and public query workloads, building once."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def cases(profile, suite, public_docs=None, analytics_docs=None):
    """Preserve the bounded/smoke storage comparisons and 100k public workload."""
    result = []
    if suite in ("all", "storage"):
        sizes = (
            [(128, 3, 2, 16, 16, 8), (256, 4, 2, 16, 64, 8), (384, 4, 2, 192, 32, 8)]
            if profile == "smoke"
            else [
                (1024, 8, 4, 128, 64, 16),
                (2048, 8, 3, 64, 256, 16),
                (2048, 8, 3, 1024, 64, 16),
            ]
        )
        for name, values in zip(
            ("tiny_baseline", "selective_small_filter", "broad_large_filter"), sizes
        ):
            args = []
            for flag, value in zip(
                ("docs", "queries", "repeats", "filter-size", "sparse-dims", "limit"),
                values,
            ):
                args.extend((f"--{flag}", str(value)))
            args += [
                "--batch-size",
                "256",
                "--body-repeat",
                "1",
                "--with-sparse",
                "--max-ordinal-ratio",
                "1.25",
                "--require-public-resolution-delta",
            ]
            result.append(
                (name, "storage_bench", ["query", *args], "docid_query_bench_summary")
            )
    if suite in ("all", "public"):
        docs = (
            public_docs
            if public_docs is not None
            else (200 if profile == "smoke" else 100000)
        )
        common = [
            "--mode",
            "handler",
            "--docs",
            str(docs),
            "--dims",
            "64",
            "--queries",
            "2",
            "--repeats",
            "1",
            "--k",
            "20",
            "--batch-size",
            "1000",
            "--sync-level",
            "full_index",
            "--load-progress-interval",
            "25000",
        ]
        for shape, extra in (
            ("full-text", []),
            ("dense-filter", []),
            ("sparse-filter", ["--with-sparse"]),
            ("graph-expand", ["--with-graph"]),
            ("algebraic-filter", ["--with-algebraic"]),
            ("hybrid-composed", ["--with-sparse", "--with-algebraic"]),
        ):
            result.append(
                (
                    shape.replace("-", "_"),
                    "api_bench",
                    common + ["--query-shape", shape] + extra,
                    "public_query_guardrail_summary",
                )
            )
    if suite in ("all", "analytics"):
        smoke = profile == "smoke"
        docs = (
            analytics_docs if analytics_docs is not None else (100 if smoke else 50000)
        )
        common = [
            "--algebraic-profile",
            "production_hardening",
            "--docs",
            str(docs),
            "--repeats",
            "1" if smoke else "5",
            "--batch-size",
            "25" if smoke else "1000",
            "--churn-ops",
            "4" if smoke else "5000",
            "--customers",
            "16" if smoke else "4096",
            "--products",
            "8" if smoke else "128",
        ]
        for name, mode, backend in (
            ("analytics", "lsm-analytics", "lsm"),
            ("adaptive_coverage", "adaptive-coverage", "mem" if smoke else "lsm"),
            ("cold_warm_reads", "cold", "lsm"),
        ):
            result.append(
                (
                    name,
                    "storage_bench",
                    ["analytics", *common]
                    + [
                        "--mode",
                        mode,
                        "--algebraic-backend",
                        backend,
                        "--fanout",
                        "2" if smoke else "4",
                    ],
                    "dataset",
                )
            )
        result.append(
            (
                "graph_traversal",
                "storage_bench",
                ["analytics", *common]
                + [
                    "--mode",
                    "graph-traversal",
                    "--algebraic-backend",
                    "mem" if smoke else "lsm",
                    "--docs",
                    str(min(docs, 10000)),
                    "--fanout",
                    "2" if smoke else "4",
                ],
                "graph_algebraic_traversal",
            )
        )
        public = [
            "--mode",
            "handler",
            "--query-shape",
            "hybrid-filter-exclude-project",
            "--docs",
            str(public_docs if public_docs is not None else (200 if smoke else 10000)),
            "--dims",
            "32" if smoke else "128",
            "--queries",
            "1" if smoke else "10",
            "--repeats",
            "1" if smoke else "3",
            "--k",
            "5" if smoke else "25",
            "--search-threads",
            "2" if smoke else "8",
        ]
        for name, flags in (
            ("no_schema", []),
            ("schema_only", ["--with-schema"]),
            ("schema_algebraic", ["--with-schema", "--with-algebraic"]),
        ):
            result.append(
                (
                    f"compare_{name}",
                    "api_bench",
                    public + flags,
                    "public_query_guardrail_summary",
                )
            )
    return result


def summary_args():
    # Preserve the production sweep's coverage and correctness gates. Measured
    # performance limits and baseline ratios remain explicit caller arguments.
    result = ["--require-performance-evidence"]
    for metric in (
        "dataset-cases",
        "lsm-dataset-cases",
        "algebraic-query-records",
        "doc-scan-query-records",
        "full-text-query-records",
        "lsm-query-records",
        "cold-query-records",
        "warm-query-records",
        "constrained-query-records",
        "wide-query-records",
        "stats-query-records",
        "cardinality-query-records",
        "range-query-records",
        "histogram-query-records",
        "fanout-dataset-cases",
        "churn-records",
    ):
        result.extend((f"--min-{metric}", "1"))
    return result + [
        "--min-public-query-comparison-pairs",
        "2",
        "--max-correctness-failures",
        "0",
        "--max-unclassified-algebraic-comparisons",
        "0",
    ]


def run_matrix(args):
    selected = cases(args.profile, args.suite, args.public_docs, args.analytics_docs)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    binaries = args.bin_dir.resolve()
    targets = []
    if args.suite in ("all", "storage", "analytics"):
        targets.append("antfly-storage-bench")
    if args.suite in ("all", "public", "analytics"):
        targets.append("antfly-api-bench")
    metadata = {
        "root": str(ROOT),
        "profile": args.profile,
        "suite": args.suite,
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "platform": platform.platform(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "git_status": subprocess.check_output(
            ["git", "status", "--short"], cwd=ROOT, text=True
        ),
    }
    (out / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (
        (out / "commands.txt").open("w") as commands,
        (out / "status.tsv").open("w") as status,
        (out / "combined.jsonl").open("w") as combined,
        (out / "summary.jsonl").open("w") as summaries,
    ):
        if not args.skip_build:
            command = ["zig", "build", *targets]
            commands.write("build\t" + shlex.join(command) + "\n")
            commands.flush()
            subprocess.run(command, cwd=ROOT / "zig", check=True)

        def run_case(name, binary, flags, summary_event, extra):
            command = [str(binaries / binary), *flags, *extra]
            commands.write(name + "\t" + shlex.join(command) + "\n")
            commands.flush()
            print(f"running {name}", flush=True)
            with (
                (out / f"{name}.stdout").open("w") as stdout,
                (out / f"{name}.stderr").open("w") as stderr,
            ):
                proc = subprocess.run(command, cwd=ROOT, stdout=stdout, stderr=stderr)
            found_summary = False
            for stream in ("stdout", "stderr"):
                with (out / f"{name}.{stream}").open() as log:
                    for line in log:
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(record, dict):
                            continue
                        record["matrix_case"] = name
                        encoded = json.dumps(record) + "\n"
                        combined.write(encoded)
                        if record.get("event") == summary_event:
                            summaries.write(encoded)
                            found_summary = True
            code = proc.returncode or (0 if found_summary else 1)
            status.write(f"{name}\t{code}\n")
            status.flush()
            if code:
                raise SystemExit(
                    f"{name} failed (exit={proc.returncode}, summary={found_summary}); see {out}"
                )

        for name, binary, flags, summary_event in selected:
            extra = (
                args.public_arg
                if binary == "api_bench"
                else args.storage_arg
                if flags[0] == "query"
                else args.analytics_arg
            )
            run_case(name, binary, flags, summary_event, extra)
        if args.suite in ("all", "analytics"):
            combined.flush()
            flags = ["summary", "--input", str(out / "combined.jsonl"), *summary_args()]
            if args.baseline:
                flags += ["--baseline", str(args.baseline.resolve())]
            run_case(
                "comparison",
                "storage_bench",
                flags,
                "performance_evidence_summary",
                args.summary_arg,
            )
    print(f"wrote {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "bounded"), default="bounded")
    parser.add_argument(
        "--suite", choices=("all", "storage", "public", "analytics"), default="all"
    )
    parser.add_argument(
        "--public-docs",
        type=int,
        help="override the public workload's 100k default (200 in smoke)",
    )
    parser.add_argument(
        "--analytics-docs",
        type=int,
        help="analytics scale (50k bounded, 100 smoke; graph capped at 10k)",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="prior DB comparison JSONL for analytics baseline ratios",
    )
    parser.add_argument(
        "--analytics-arg",
        action="append",
        default=[],
        help="append a DB analytics driver argument, including LSM bulk/tuning options",
    )
    parser.add_argument(
        "--summary-arg",
        action="append",
        default=[],
        help="append a comparison threshold; use --summary-arg=--flag",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT
        / "bench/results/db-query-matrix"
        / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
    )
    parser.add_argument(
        "--skip-build", action="store_true", help="use previously built binaries"
    )
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "zig/zig-out/bin")
    parser.add_argument(
        "--storage-arg",
        action="append",
        default=[],
        help="append a storage driver argument; use --storage-arg=--flag",
    )
    parser.add_argument(
        "--public-arg",
        action="append",
        default=[],
        help="append a public driver argument; use --public-arg=--flag",
    )
    args = parser.parse_args()
    if args.public_docs is not None and args.public_docs <= 0:
        parser.error("--public-docs must be positive")
    if args.analytics_docs is not None and args.analytics_docs <= 0:
        parser.error("--analytics-docs must be positive")
    if args.bin_dir != ROOT / "zig/zig-out/bin" and not args.skip_build:
        parser.error("--bin-dir requires --skip-build")
    run_matrix(args)


if __name__ == "__main__":
    main()
