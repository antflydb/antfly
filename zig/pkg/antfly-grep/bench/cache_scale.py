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
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Use compact JSON for the candidate; reconstruct match payloads before comparison",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.files < 1 or args.rounds < 1:
        parser.error("files and rounds must be positive")
    binaries = {
        "antfly-grep": str(args.binary.resolve()),
        "baseline": str(args.baseline.resolve()),
    }
    report = {
        "files": args.files,
        "rounds": args.rounds,
        "fixture_only": True,
        "limitations": "Synthetic Python sources and native constant-score fixture; warm filesystem on one host; no model quality or billed tokens measured",
        "binaries": {
            n: {"sha256": hashlib.sha256(Path(b).read_bytes()).hexdigest()}
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
                "--threads=1",
                "--max-files=0",
                "--max-requests=unlimited",
                "--cache-dir",
                str(root / name),
                "--json-compact"
                if args.compact and name == "antfly-grep"
                else "--json",
                "src",
            ]
            result = subprocess.run(
                commands[name], cwd=root, env=env, capture_output=True, check=True
            )
            report["cases"][name] = {
                "cold_summary": json.loads(result.stdout.splitlines()[-1]),
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
            signatures.append(
                sorted(
                    json.dumps(
                        {
                            "type": row["type"],
                            "data": row.get("data"),
                            "probability": row.get("probability"),
                            "scope": row.get("scope"),
                        },
                        sort_keys=True,
                    )
                    for row in rows
                    if row["type"] == "match"
                )
            )
        if signatures[0] != signatures[1]:
            raise RuntimeError("result payloads differ")
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
