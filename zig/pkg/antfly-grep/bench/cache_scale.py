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

"""Fixture-only cache scaling; paired binaries, bounded scratch, no provider network."""

import argparse
import hashlib
import json
import os
import platform
import time
import statistics
import subprocess
import tempfile
from pathlib import Path

from compare import measure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--files", type=int, default=2048)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--top-k", type=int, default=0)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--excerpt-lines",
        type=int,
        default=0,
        help="Use one plain-text file and jevgrep line evidence to measure excerpt scaling",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Use compact JSON for the candidate; reconstruct match payloads before comparison",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.files < 1 or args.rounds < 1 or args.threads < 1 or args.top_k < 0:
        parser.error(
            "files, rounds and threads must be positive; top-k must be nonnegative"
        )
    if args.excerpt_lines < 0 or (args.excerpt_lines and args.files != 1):
        parser.error("excerpt-lines requires --files=1 and a nonnegative line count")
    binaries = {
        "antfly-grep": str(args.binary.resolve()),
        "baseline": str(args.baseline.resolve()),
    }
    report = {
        "files": args.files,
        "rounds": args.rounds,
        "top_k": args.top_k,
        "threads": args.threads,
        "excerpt_lines": args.excerpt_lines,
        "fixture_only": True,
        "fixture_library_sha256": hashlib.sha256(args.library.read_bytes()).hexdigest(),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
        },
        "recorded_at_unix": time.time(),
        "limitations": "Synthetic Python or line-evidence sources and native constant-score fixture; warm filesystem on one host; no model quality or billed tokens measured",
        "binaries": {
            n: {
                "sha256": hashlib.sha256(Path(b).read_bytes()).hexdigest(),
                "version": subprocess.check_output([b, "--version"], text=True).strip(),
            }
            for n, b in binaries.items()
        },
        "cases": {},
    }
    cache_root = Path(__file__).resolve().parents[3] / ".zig-cache"
    cache_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="grep-scale-", dir=cache_root) as directory:
        root = Path(directory)
        source = root / "src"
        source.mkdir()
        if args.excerpt_lines:
            (source / "overview.txt").write_text(
                "".join(f"STOP row_{i}\n" for i in range(args.excerpt_lines))
            )
        else:
            for i in range(args.files):
                (source / f"module_{i:05d}.py").write_text(
                    "".join(
                        f"def retry_{i}_{j}(value):\n    return STOP(value)\n\n"
                        for j in range(16)
                    )
                )
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
        }
        env.update(HOME=str(root), TERM="dumb")
        commands = {}
        for name, binary in binaries.items():
            commands[name] = [
                binary,
                "--ask=retry",
                "--model=fixture",
                "--backend=local",
                "--library",
                str(args.library.resolve()),
                "--chunk-lines=1000",
                "--context-lines=0",
                "--no-refine",
                f"--threads={args.threads}",
                "--max-files=0",
                "--max-requests=unlimited",
                "--cache-dir",
                str(root / name),
                "--json-compact"
                if args.compact and name == "antfly-grep"
                else "--json",
                "src",
            ]
            if args.excerpt_lines:
                commands[name][1:2] = [
                    "ask",
                    "retry",
                    f"--max-count={args.excerpt_lines}",
                ]
                commands[name][commands[name].index("--chunk-lines=1000")] = (
                    "--unit=line"
                )
                commands[name][-1] = "src/overview.txt"
            if args.top_k:
                commands[name].insert(1, f"--max-result-files={args.top_k}")
            cold_start = time.perf_counter()
            result = subprocess.run(
                commands[name], cwd=root, env=env, capture_output=True, check=True
            )
            cold_seconds = time.perf_counter() - cold_start
            report["cases"][name] = {
                "cold_summary": json.loads(result.stdout.splitlines()[-1]),
                "cold_seconds": cold_seconds,
                "warm_runs": [],
            }
        for trial in range(args.rounds):
            for name in list(commands) if trial % 2 == 0 else list(reversed(commands)):
                report["cases"][name]["warm_runs"].append(
                    measure(commands[name], root, env)
                )
        signatures = []
        for name, command in commands.items():
            result = subprocess.run(
                command, cwd=root, env=env, capture_output=True, check=True
            )
            rows = [json.loads(line) for line in result.stdout.splitlines()]
            report["cases"][name]["warm_summary"] = rows[-1]
            if (
                args.excerpt_lines
                and rows[-1]["stats"]["matches"] != args.excerpt_lines
            ):
                raise RuntimeError(
                    "excerpt fixture did not evaluate/select every source line"
                )
            report["cases"][name]["median_seconds"] = statistics.median(
                r["seconds"] for r in report["cases"][name]["warm_runs"]
            )
            report["cases"][name]["cache_files"] = len(list((root / name).iterdir()))
            report["cases"][name]["cache_bytes"] = sum(
                p.stat().st_size for p in (root / name).iterdir()
            )
            report["cases"][name]["cache_allocated_bytes"] = sum(
                getattr(p.stat(), "st_blocks", 0) * 512 for p in (root / name).iterdir()
            )
            report["cases"][name]["output_bytes"] = len(result.stdout)
            files = {
                row["file_id"]: row["data"]["path"]
                for row in rows
                if row["type"] == "begin" and "file_id" in row
            }
            for row in rows:
                if row["type"] == "match" and "file_id" in row:
                    row["data"]["path"] = files[row["file_id"]]
            canonical = []
            for row in rows:
                if row["type"] == "match":
                    canonical.append(
                        {
                            "type": row["type"],
                            "data": row.get("data"),
                            "probability": row.get("probability"),
                            "scope": row.get("scope"),
                        }
                    )
                elif row["type"] == "file":
                    canonical.append(
                        {
                            key: row.get(key)
                            for key in (
                                "type",
                                "path",
                                "relevance",
                                "file_preview_score",
                                "selection",
                                "match",
                                "reading_leads",
                                "regions",
                                "lines",
                                "calls",
                                "omitted",
                                "parse_complete",
                                "complete",
                            )
                        }
                    )
            signatures.append(
                sorted(json.dumps(row, sort_keys=True) for row in canonical)
            )
        if signatures[0] != signatures[1]:
            raise RuntimeError("result payloads differ")
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
