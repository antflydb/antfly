#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reproduce the Bloom filter experiment with the repository's Zig 0.17."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def run(args, **kwargs):
    print("+", " ".join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default=os.environ.get("ZIG", "zig"))
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--storage",
        action="store_true",
        help="Also build and compare actual LSM backends",
    )
    parser.add_argument(
        "--blocked-storage",
        action="store_true",
        help="Include the experimental split storage module",
    )
    parser.add_argument(
        "--skip-micro", action="store_true", help="Only run the storage comparison"
    )
    args = parser.parse_args()
    zig = args.zig
    version = subprocess.check_output([zig, "version"], text=True).strip()
    if version != "0.17.0":
        parser.error(f"Expected pinned Zig 0.17.0, got {version}")
    run([zig, "test", ROOT / "zig/lib/bloom/src/mod.zig", "-O", "ReleaseSafe"])
    run(
        [
            zig,
            "test",
            "-O",
            "ReleaseSafe",
            "--dep",
            "bloom",
            f"-Mroot={HERE / 'review_test.zig'}",
            f"-Mbloom={ROOT / 'zig/lib/bloom/src/mod.zig'}",
        ]
    )
    modules = [
        "--dep",
        "bloom",
        f"-Mroot={HERE / 'bench.zig'}",
        f"-Mbloom={ROOT / 'zig/lib/bloom/src/mod.zig'}",
    ]
    run([zig, "test", "-O", "ReleaseSafe", "-lc", *modules])
    bench = HERE / "bench-0.17"
    run([zig, "build-exe", "-O", "ReleaseFast", "-lc", *modules, f"-femit-bin={bench}"])
    storage = args.storage or args.blocked_storage
    variants = (
        ["baseline", "optimized", "split"]
        if args.blocked_storage
        else ["baseline", "optimized"]
    )
    if args.blocked_storage:
        run([zig, "test", HERE / "split_module.zig", "-O", "ReleaseSafe"])
    if storage:
        module = ROOT / "zig/lib/bloom/src/mod.zig"
        optimized = module.read_bytes()
        try:
            for name in variants:
                data = (
                    optimized
                    if name == "optimized"
                    else (
                        HERE
                        / ("baseline.zig" if name == "baseline" else "split_module.zig")
                    ).read_bytes()
                )
                module.write_bytes(data)
                run(
                    [
                        zig,
                        "build",
                        "lsm-backend-bench",
                        "-Doptimize=ReleaseFast",
                        "-j2",
                    ],
                    cwd=ROOT / "zig",
                )
                shutil.copy2(
                    ROOT / "zig/zig-out/bin/lsm_backend_bench", HERE / f"lsm-{name}"
                )
        finally:
            module.write_bytes(optimized)
    # No compilation overlaps timed work; reverse storage order each round.
    for index in range(1, args.rounds + 1):
        if not args.skip_micro:
            with (HERE / f"results-{index}.csv").open("w") as output:
                run([bench], stderr=output)
        if storage:
            names = variants if index % 2 else list(reversed(variants))
            for name in names:
                prefix = "storage-threeway" if args.blocked_storage else "storage"
                with (HERE / f"{prefix}-{index}-{name}.txt").open("w") as output:
                    run(
                        [
                            HERE / f"lsm-{name}",
                            "--in-range-misses",
                            "--samples",
                            "3",
                            "--keys",
                            "20000",
                            "--storage",
                            "memory",
                            "--cache",
                            "both",
                            "--bloom-bits-per-key",
                            "14",
                            "--hit-repeats",
                            "3",
                            "--miss-repeats",
                            "10",
                            "--short-scan-repeats",
                            "1",
                            "--full-scan-repeats",
                            "1",
                            "--reopen-repeats",
                            "1",
                            "--mixed-repeats",
                            "0",
                            "--concurrent-read-threads",
                            "0",
                        ],
                        stdout=output,
                        stderr=subprocess.STDOUT,
                    )


if __name__ == "__main__":
    main()
