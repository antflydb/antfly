#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate independent native-depth subsampled RGB vectors using offline FFmpeg."""

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
    result = bytearray()
    cases = []
    for width, height in ((9, 7), (26, 16), (127, 95)):
        for chroma in (1, 2):
            for depth in (8, 10, 12):
                count = width * height
                cw, ch = (width + 1) // 2, (height + 1) // 2 if chroma == 1 else height
                sample_bytes = 1 if depth == 8 else 2
                planes = b"".join(
                    ((i * multiplier + offset) % (1 << depth)).to_bytes(
                        sample_bytes, "little"
                    )
                    for multiplier, offset, samples in (
                        (37, 11, count),
                        (61, 7, cw * ch),
                        (73, 29, cw * ch),
                    )
                    for i in range(samples)
                )
                for matrix in ("smpte170m", "bt709", "bt2020nc"):
                    for full in (False, True):
                        command = [
                            args.ffmpeg,
                            "-v",
                            "error",
                            "-f",
                            "rawvideo",
                            "-pix_fmt",
                            "yuv"
                            + ("420" if chroma == 1 else "422")
                            + "p"
                            + (f"{depth}le" if depth > 8 else ""),
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
                            command, input=planes, stdout=subprocess.PIPE, check=True
                        ).stdout
                        if len(rgb) != count * 3:
                            raise ValueError("unexpected RGB geometry")
                        cases.append(
                            {
                                "width": width,
                                "height": height,
                                "depth": depth,
                                "chroma": chroma,
                                "matrix": matrix,
                                "full": full,
                                "offset": len(result),
                                "length": len(rgb),
                                "command": command,
                                "input_sha256": hashlib.sha256(planes).hexdigest(),
                                "rgb_sha256": hashlib.sha256(rgb).hexdigest(),
                            }
                        )
                        result.extend(rgb)
    args.output.write_bytes(result)
    args.output.with_suffix(".json").write_text(
        json.dumps(
            {
                "ffmpeg": subprocess.check_output([args.ffmpeg, "-version"], text=True),
                "cases": cases,
                "sha256": hashlib.sha256(result).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
