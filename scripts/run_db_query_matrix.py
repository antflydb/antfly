#!/usr/bin/env python3
"""Compare storage query paths and public query shapes, building each tool once."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def cases(profile, suite, public_docs=None):
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
            result.append((name, "db_query_bench", args, "docid_query_bench_summary"))
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
                    "public_query_guardrail",
                    common + ["--query-shape", shape] + extra,
                    "public_query_guardrail_summary",
                )
            )
    return result


def run_matrix(args):
    selected = cases(args.profile, args.suite, args.public_docs)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    binaries = args.bin_dir.resolve()
    targets = []
    if args.suite in ("all", "storage"):
        targets.append("antfly-storage-db-bench")
    if args.suite in ("all", "public"):
        targets.append("public-query-guardrail")
    metadata = {
        "root": str(ROOT),
        "profile": args.profile,
        "suite": args.suite,
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
        for name, binary, flags, summary_event in selected:
            extra = args.storage_arg if binary == "db_query_bench" else args.public_arg
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
                        record["case"] = name
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
    print(f"wrote {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "bounded"), default="bounded")
    parser.add_argument("--suite", choices=("all", "storage", "public"), default="all")
    parser.add_argument(
        "--public-docs",
        type=int,
        help="override the public workload's 100k default (200 in smoke)",
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
    if args.bin_dir != ROOT / "zig/zig-out/bin" and not args.skip_build:
        parser.error("--bin-dir requires --skip-build")
    run_matrix(args)


if __name__ == "__main__":
    main()
