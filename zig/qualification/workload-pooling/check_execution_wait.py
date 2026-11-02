#!/usr/bin/env python3
"""Qualify checked wait callbacks using a separately compiled native owner."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--target")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--global-cache-dir", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = [
        "pkg/antfly-embedded/src/common/execution_wait.zig",
        "pkg/antfly-embedded/src/execution_wait_archive_fixture.zig",
        "pkg/antfly-embedded/src/execution_wait_test_root.zig",
        "pkg/antfly-embedded/src/runtime_callback_abi.zig",
        "pkg/antfly-embedded/src/source_catalog.zig",
        "pkg/antfly-embedded/src/source_catalog_control.zig",
        "lib/runtime/src/runtime_error_abi.zig",
        "lib/runtime/src/runtime_native_abi.zig",
        "qualification/workload-pooling/check_execution_wait.py",
    ]

    def source_hashes():
        return {
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in sources
        }

    hashes = source_hashes()
    common = [
        "-Osafe",
        "--cache-dir",
        args.cache_dir,
        "--global-cache-dir",
        args.global_cache_dir,
    ]
    if args.target:
        common.extend(["-target", args.target])
    owner = output / "wait-owner.o"
    modules = ["--dep", "antfly_runtime_abi"]
    runtime_module = "-Mantfly_runtime_abi=lib/runtime/src/root.zig"
    stages = [
        (
            "owner",
            [
                args.zig,
                "build-obj",
                *common,
                *modules,
                "-Mroot=pkg/antfly-embedded/src/execution_wait_archive_fixture.zig",
                runtime_module,
                f"-femit-bin={owner}",
            ],
            None,
        ),
        (
            "callbacks",
            [
                args.zig,
                "test",
                str(owner),
                *common,
                "--test-filter",
                "execution wait",
                *modules,
                "-Mroot=pkg/antfly-embedded/src/execution_wait_test_root.zig",
                runtime_module,
            ],
            "All 3 tests passed.",
        ),
        (
            "error-abi",
            [args.zig, "test", "lib/runtime/src/runtime_error_abi.zig", *common],
            "All 25 tests passed.",
        ),
    ]
    receipt = {
        "passed": False,
        "target": args.target or "native",
        "source_sha256": hashes,
        "stages": [],
    }
    version = subprocess.run(
        [args.zig, "version"], capture_output=True, text=True, check=True
    )
    receipt["zig_version"] = version.stdout.strip()
    for name, command, expected in stages:
        result = subprocess.run(command, cwd=root, capture_output=True)
        log = result.stdout + result.stderr
        (output / f"{name}.log").write_bytes(log)
        passed = result.returncode == 0 and (
            expected is None or expected.encode() in log
        )
        receipt["stages"].append(
            {
                "name": name,
                "command": command,
                "exit_code": result.returncode,
                "passed": passed,
            }
        )
        if not passed:
            break
    else:
        receipt["passed"] = hashes == source_hashes()
        receipt["owner_sha256"] = hashlib.sha256(owner.read_bytes()).hexdigest()
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "output": str(output)}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
