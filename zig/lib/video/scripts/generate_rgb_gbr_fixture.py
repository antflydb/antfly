#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate independent identity-matrix RGB24 oracles using offline FFmpeg."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ffmpeg", default="ffmpeg")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    output, cases = bytearray(), []
    for width, height in [(9, 7), (26, 16), (127, 95), (129, 129)]:
        for depth in [8, 10, 12, 14]:
            count, maximum = width * height, 1 << depth
            raw = b"".join(
                ((i * multiplier + offset) % maximum).to_bytes(
                    1 if depth == 8 else 2, "little"
                )
                for multiplier, offset in [(1, 0), (37, 91), (73, 11)]
                for i in range(count)
            )
            command = [
                args.ffmpeg,
                "-v",
                "error",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "gbrp" + (f"{depth}le" if depth > 8 else ""),
                "-s",
                f"{width}x{height}",
                "-i",
                "-",
                "-frames:v",
                "1",
                "-pix_fmt",
                "rgb24",
                "-f",
                "rawvideo",
                "-",
            ]
            rgb = subprocess.check_output(command, input=raw, timeout=120)
            if len(rgb) != count * 3:
                raise ValueError("RGB oracle geometry changed")
            cases.append(
                dict(
                    width=width,
                    height=height,
                    depth=depth,
                    offset=len(output),
                    length=len(rgb),
                    input_sha256=hashlib.sha256(raw).hexdigest(),
                    rgb_sha256=hashlib.sha256(rgb).hexdigest(),
                    command=command,
                )
            )
            output.extend(rgb)
    args.output.write_bytes(output)
    args.output.with_suffix(".json").write_text(
        json.dumps(
            dict(
                ffmpeg=subprocess.check_output([args.ffmpeg, "-version"], text=True),
                cases=cases,
                sha256=hashlib.sha256(output).hexdigest(),
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
