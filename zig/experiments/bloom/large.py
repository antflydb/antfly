#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare prebuilt storage variants with a million keys and rotated order."""

import argparse
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--one-hash", action="store_true")
args = parser.parse_args()
variants = (
    ["baseline", "split", "onehash"]
    if args.one_hash
    else ["baseline", "optimized", "split"]
)
prefix = "large-onehash" if args.one_hash else "large"
for index in range(3):
    names = variants[index:] + variants[:index]
    for name in names:
        command = [
            str(HERE / f"lsm-{name}"),
            "--in-range-misses",
            "--samples",
            "1",
            "--keys",
            "1000000",
            "--storage",
            "memory",
            "--cache",
            "both",
            "--bloom-bits-per-key",
            "14",
            "--hit-repeats",
            "1",
            "--miss-repeats",
            "3",
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
        ]
        print("+", " ".join(command), flush=True)
        with (HERE / f"{prefix}-{index + 1}-{name}.txt").open("w") as output:
            subprocess.run(command, check=True, stdout=output, stderr=subprocess.STDOUT)
