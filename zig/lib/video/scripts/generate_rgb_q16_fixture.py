#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate independent RGB24 integer-rounding vectors using pinned FFmpeg."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    width, height = 26, 16
    y = bytes((i * 37 + 11) % 256 for i in range(width * height))
    uv = bytes((i * 61 + 7) % 256 for i in range(width * height // 2))
    planar = y + uv[::2] + uv[1::2]
    result = bytearray()
    for matrix in ("smpte170m", "bt709", "bt2020nc"):
        for full in (False, True):
            command = [
                args.ffmpeg,
                "-v",
                "error",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "yuv420p",
                "-s",
                f"{width}x{height}",
                "-colorspace",
                matrix,
                "-color_range",
                "pc" if full else "tv",
                "-i",
                "-",
                "-frames:v",
                "1",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "-",
            ]
            rgb = subprocess.run(
                command, input=planar, stdout=subprocess.PIPE, check=True
            ).stdout
            if len(rgb) != width * height * 3:
                raise ValueError("unexpected RGB size")
            result.extend(rgb)
    args.output.write_bytes(result)
    receipt = dict(
        generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        width=width,
        height=height,
        input_sha256=hashlib.sha256(planar).hexdigest(),
        output_sha256=hashlib.sha256(result).hexdigest(),
        ffmpeg=subprocess.check_output(
            [args.ffmpeg, "-version"], text=True
        ).splitlines()[0],
        policy="default RGB24 from non-vector-aligned planar 8-bit rows; matrix/range explicit",
    )
    args.output.with_suffix(".json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
