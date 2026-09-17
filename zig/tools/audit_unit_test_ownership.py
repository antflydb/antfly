#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Audit the actual runtime selections of the aggregate Antfly unit gate."""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path


def inventory(text: str) -> list[str]:
    # DB-core's two processes prefix their diagnostics with the partition name.
    return [
        line.partition("TEST\t")[2]
        for line in text.splitlines()
        if "TEST\t" in line and not line.endswith(".test_0")
    ]


def audit(suites: dict[str, list[str]]) -> dict:
    owners = defaultdict(list)
    for suite, names in suites.items():
        for name in names:
            owners[name].append(suite)
    return {
        "executions": sum(map(len, suites.values())),
        "unique_tests": len(owners),
        "duplicates": {
            name: runs for name, runs in sorted(owners.items()) if len(runs) > 1
        },
        "suites": suites,
    }


def protocol_inventory(data: bytes) -> list[str]:
    offset = 0
    while offset < len(data):
        if offset + 8 > len(data):
            raise ValueError("truncated Zig protocol header")
        tag, size = struct.unpack_from("=II", data, offset)
        offset += 8
        payload = data[offset : offset + size]
        if len(payload) != size:
            raise ValueError("truncated Zig protocol payload")
        offset += size
        if tag != 3:  # test_metadata
            continue
        strings_length, count = struct.unpack_from("=II", payload)
        string_start = 8 + count * 8  # names + expected-panic indexes
        if len(payload) != string_start + strings_length:
            raise ValueError("invalid Zig test metadata size")
        indexes = struct.unpack_from("=" + "I" * count, payload, 8)
        strings = payload[string_start:]
        names = []
        for index in indexes:
            if index >= len(strings) or b"\0" not in strings[index:]:
                raise ValueError("invalid Zig test name offset")
            names.append(strings[index:].split(b"\0", 1)[0].decode())
        return names
    raise ValueError("Zig runner returned no test metadata")


def query_runner(
    executable: Path, inference: bool, runtime_args: list[str]
) -> list[str]:
    if not inference:
        result = subprocess.run(
            [str(executable), "--listen=-"],
            input=struct.pack("=IIII", 4, 0, 0, 0),
            capture_output=True,
            timeout=60,
            check=True,
        )
        return protocol_inventory(result.stdout)
    with tempfile.TemporaryDirectory(prefix="antfly-test-inventory-") as directory:
        output = Path(directory) / "names"
        env = dict(os.environ, ANTFLY_INFERENCE_TEST_LIST_FILE=str(output))
        subprocess.run(
            [str(executable), *runtime_args],
            env=env,
            capture_output=True,
            timeout=60,
            check=True,
        )
        return output.read_text().splitlines()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol-executable", type=Path)
    parser.add_argument("--inference-executable", type=Path)
    parser.add_argument(
        "--inventory", nargs=2, action="append", default=[], metavar=("OWNER", "PATH")
    )
    parser.add_argument(
        "--baseline-inventory",
        nargs=2,
        action="append",
        default=[],
        metavar=("OWNER", "PATH"),
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--allow-overlap", action="store_true")
    args, runtime_args = parser.parse_known_args()
    if args.protocol_executable or args.inference_executable:
        if runtime_args[:1] == ["--"]:
            runtime_args = runtime_args[1:]
        for name in query_runner(
            args.protocol_executable or args.inference_executable,
            bool(args.inference_executable),
            runtime_args,
        ):
            print(f"TEST\t{name}", file=sys.stderr)
        return 0
    if runtime_args or args.report is None:
        parser.error("audit requires --report and accepts no runtime arguments")
    suites = {}
    for owner, path in args.inventory:
        if owner in suites:
            raise ValueError(f"duplicate suite identifier: {owner}")
        suites[owner] = inventory(Path(path).read_text())
    result = audit(suites)
    errors = []
    if args.baseline or args.baseline_inventory:
        before = (
            json.loads(args.baseline.read_text())
            if args.baseline
            else {
                "suites": {
                    owner: inventory(Path(path).read_text())
                    for owner, path in args.baseline_inventory
                }
            }
        )
        before_names = {name for names in before["suites"].values() for name in names}
        after_names = {name for names in suites.values() for name in names}
        result["baseline_executions"] = sum(map(len, before["suites"].values()))
        result["lost_tests"] = sorted(before_names - after_names)
        result["added_tests"] = sorted(after_names - before_names)
        errors.extend(f"lost coverage: {name}" for name in result["lost_tests"])
        errors.extend(f"unexpected coverage: {name}" for name in result["added_tests"])
    repeats = result["executions"] - result["unique_tests"]
    print(
        f"Unit ownership: {result['unique_tests']} named tests, {result['executions']} executions, {repeats} repeats",
        file=sys.stderr,
    )
    if not args.allow_overlap:
        errors.extend(
            f"multiple owners: {name}: {', '.join(owners)}"
            for name, owners in result["duplicates"].items()
        )
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    for error in errors:
        print(error, file=sys.stderr)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
