#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reproduce real-content differential qualification; never a runtime dependency.

Use the pinned CC BY 3.0 Sintel trailer and the built video-qualification binary.
Artifacts stay in --output; receipts include exact encoding commands and hashes.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from qualify_h264 import qualify

SOURCE_HASH = "b670602fa00934ca27c4351bb0efe7ea7a07fae57284e44226025eeed7c51254"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(args.source.read_bytes()).hexdigest() == SOURCE_HASH
    args.output.mkdir(parents=True, exist_ok=True)
    policies = [
        ("baseline", "yuv420p", "baseline", "cabac=0:bframes=0:ref=4"),
        ("main", "yuv420p", "main", "cabac=1:bframes=3:ref=4:weightp=2:weightb=1"),
        ("high10", "yuv420p10le", "high10", "cabac=1:bframes=3:ref=4"),
        ("high444", "yuv444p", "high444", "cabac=1:bframes=3:ref=4"),
    ]
    receipts = []
    for name, pixel_format, profile, parameters in policies:
        path = args.output / (name + ".mp4")
        command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            "12",
            "-i",
            str(args.source),
            "-frames:v",
            "24",
            "-vf",
            "scale=320:192",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "slow",
            "-crf",
            "22",
            "-pix_fmt",
            pixel_format,
            "-profile:v",
            profile,
            "-x264-params",
            parameters,
            str(path),
        ]
        subprocess.run(command, check=True)
        native = path.with_suffix(".jsonl")
        with native.open("wb") as output:
            subprocess.run(
                [str(args.native.resolve()), "hash", str(path.resolve())],
                stdout=output,
                check=True,
            )
        receipt = qualify(path, native)
        receipt.update(case=name, command=command)
        receipts.append(receipt)
    result = dict(
        source_sha256=SOURCE_HASH,
        attribution="Sintel, Blender Foundation / durian.blender.org, CC BY 3.0",
        cases=receipts,
    )
    (args.output / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
