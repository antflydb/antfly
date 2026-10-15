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

"""Generate independent native-depth 4:4:4 RGB vectors using offline FFmpeg."""

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
        for depth in (8, 10, 12):
            count = width * height
            sample_bytes = 1 if depth == 8 else 2
            planes = b"".join(
                ((i * multiplier + offset) % (1 << depth)).to_bytes(
                    sample_bytes, "little"
                )
                for multiplier, offset in ((37, 11), (61, 7), (73, 29))
                for i in range(count)
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
                        "yuv444p" + (f"{depth}le" if depth > 8 else ""),
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
