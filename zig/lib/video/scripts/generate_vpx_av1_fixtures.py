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

"""Offline fixture regeneration; FFmpeg is never a runtime/build dependency."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def generate(ffmpeg, output):
    output.mkdir(parents=True, exist_ok=True)
    cases = [
        ("av1", "av1", 128, 96, "yuv420p", 0),
        ("av1", "av1-10", 128, 96, "yuv420p10le", 0),
        ("av1", "av1-grain", 128, 96, "yuv420p", 0),
        ("av1", "av1-odd", 127, 95, "yuv420p", 0),
        ("vp9", "vp9", 128, 96, "yuv420p", 0),
    ]
    for profile, pixel in [
        (0, "yuv420p"),
        (1, "yuv444p"),
        (1, "yuv422p"),
        (2, "yuv420p10le"),
        (2, "yuv420p12le"),
        (3, "yuv444p10le"),
        (3, "yuv422p12le"),
    ]:
        cases.append(("vp9", "vp9-" + pixel, 127, 95, pixel, profile))
    goldens = []
    recipes = []
    with tempfile.TemporaryDirectory(prefix="antfly-codec-reference-") as temporary:
        for codec, name, width, height, pixel, profile in cases:
            base = name == codec
            source = "testsrc2" if base else "testsrc"
            packet = output / (name + ".ivf")
            command = [
                ffmpeg,
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"{source}=size={width}x{height}:rate=6",
                "-frames:v",
                "12",
                "-an",
                "-pix_fmt",
                pixel,
            ]
            if codec == "av1":
                command += [
                    "-c:v",
                    "libsvtav1",
                    "-preset",
                    "12" if base else "10",
                    "-threads",
                    "2",
                    "-crf",
                    "30",
                    "-g",
                    "6",
                    "-svtav1-params",
                    "lp=2"
                    + (
                        ":film-grain=10:film-grain-denoise=0" if "grain" in name else ""
                    ),
                ]
            else:
                command += [
                    "-c:v",
                    "libvpx-vp9",
                    "-profile:v",
                    str(profile),
                    "-deadline",
                    "good",
                    "-cpu-used",
                    "4" if base else "3",
                    "-threads",
                    "2",
                    "-crf",
                    "30",
                    "-b:v",
                    "0",
                    "-g",
                    "6",
                ]
            subprocess.run(command + ["-f", "ivf", "-y", str(packet)], check=True)
            raw = Path(temporary) / (name + ".yuv")
            decode = [
                ffmpeg,
                "-v",
                "error",
                "-i",
                str(packet),
                "-pix_fmt",
                pixel,
                "-f",
                "rawvideo",
                "-y",
                str(raw),
            ]
            subprocess.run(decode, check=True)
            depth = 12 if "12le" in pixel else 10 if "10le" in pixel else 8
            chroma = 3 if "444" in pixel else 2 if "422" in pixel else 1
            cw = width if chroma == 3 else (width + 1) // 2
            ch = height if chroma >= 2 else (height + 1) // 2
            size = (width * height + 2 * cw * ch) * (1 if depth == 8 else 2)
            samples = raw.read_bytes()
            if len(samples) != 12 * size:
                raise RuntimeError("reference picture count/geometry mismatch: " + name)
            goldens.append(
                dict(
                    name=name,
                    codec=codec,
                    width=width,
                    height=height,
                    depth=depth,
                    chroma=chroma,
                    packet_sha256=hashlib.sha256(packet.read_bytes()).hexdigest(),
                    frame_sha256=[
                        hashlib.sha256(samples[i : i + size]).hexdigest()
                        for i in range(0, len(samples), size)
                    ],
                )
            )
            recipes.append(
                dict(
                    name=name,
                    encode=command + ["-f", "ivf", "-y", packet.name],
                    reference_decode=decode,
                )
            )
            if base:
                subprocess.run(
                    [
                        ffmpeg,
                        "-v",
                        "error",
                        "-i",
                        str(packet),
                        "-c:v",
                        "copy",
                        "-an",
                        "-y",
                        str(output / (name + ".webm")),
                    ],
                    check=True,
                )
    (output / "golden.json").write_text(json.dumps(goldens, indent=2) + "\n")
    (output / "provenance.json").write_text(
        json.dumps(
            dict(
                ffmpeg=subprocess.check_output([ffmpeg, "-version"], text=True),
                recipes=recipes,
                webm_sha256={
                    name: hashlib.sha256((output / name).read_bytes()).hexdigest()
                    for name in ["vp9.webm", "av1.webm"]
                },
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.ffmpeg, args.output)
