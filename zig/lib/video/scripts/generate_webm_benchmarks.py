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

"""Reproduce larger SDR WebM streams and independent picture hashes from Sintel.

Sintel copyright Blender Foundation, CC BY 3.0. Only qualification tooling uses
FFmpeg; native decoder builds never invoke encoders or download sources.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

SOURCE_SHA256 = "b670602fa00934ca27c4351bb0efe7ea7a07fae57284e44226025eeed7c51254"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate(source, output, ffmpeg):
    if sha(source) != SOURCE_SHA256:
        raise ValueError("Sintel source differs from pinned original")
    output.mkdir(parents=True, exist_ok=True)
    version = subprocess.check_output([ffmpeg, "-version"], text=True).splitlines()[0]
    manifest = {
        "version": 1,
        "source": {
            "url": "https://download.blender.org/durian/trailer/sintel_trailer-480p.mp4",
            "sha256": SOURCE_SHA256,
            "copyright": "Blender Foundation / durian.blender.org",
            "license": "CC BY 3.0",
            "license_url": "https://creativecommons.org/licenses/by/3.0/",
        },
        "encoder": version,
        "streams": [],
    }
    for width, height, duration in [(854, 480, None), (1920, 1080, 12)]:
        for codec, encoder in [
            ("vp8", "libvpx"),
            ("vp9", "libvpx-vp9"),
            ("av1", "libsvtav1"),
        ]:
            name = f"sintel-{codec}-{height}p.webm"
            path = output / name
            command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-y"]
            if duration:
                command += ["-ss", "8"]
            command += ["-i", str(source), "-map", "0:v:0", "-an"]
            if duration:
                command += ["-t", str(duration)]
            command += [
                "-vf",
                f"scale={width}:{height},setsar=1",
                "-r",
                "24",
                "-c:v",
                encoder,
                "-threads",
                "2",
                "-g",
                "48",
                "-pix_fmt",
                "yuv420p",
            ]
            if codec == "vp8":
                command += [
                    "-lag-in-frames",
                    "0",
                    "-deadline",
                    "good",
                    "-cpu-used",
                    "2",
                    "-auto-alt-ref",
                    "0",
                    "-b:v",
                    "1200k",
                    "-crf",
                    "24",
                ]
            elif codec == "vp9":
                command += [
                    "-lag-in-frames",
                    "0",
                    "-deadline",
                    "good",
                    "-cpu-used",
                    "3",
                    "-row-mt",
                    "0",
                    "-auto-alt-ref",
                    "0",
                    "-b:v",
                    "0",
                    "-crf",
                    "32",
                ]
            else:
                command += [
                    "-preset",
                    "8",
                    "-svtav1-params",
                    "lp=2",
                    "-b:v",
                    "0",
                    "-crf",
                    "36",
                ]
            command += [
                "-fflags",
                "+bitexact",
                "-flags",
                "+bitexact",
                "-map_metadata",
                "-1",
                str(path),
            ]
            print("Encoding", name, flush=True)
            subprocess.run(command, check=True, timeout=1800)
            # NV12 independently packs visible 8-bit 4:2:0 planes, matching the
            # native Frame boundary without any native code in the oracle.
            decoder = [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(path),
                "-map",
                "0:v:0",
                "-fps_mode",
                "passthrough",
                "-pix_fmt",
                "nv12",
                "-f",
                "rawvideo",
                "-",
            ]
            hashes = []
            size = width * height + ((width + 1) // 2) * ((height + 1) // 2) * 2
            with subprocess.Popen(decoder, stdout=subprocess.PIPE) as process:
                while True:
                    frame = process.stdout.read(size)
                    if not frame:
                        break
                    if len(frame) != size:
                        raise ValueError("truncated independent picture")
                    hashes.append(hashlib.sha256(frame).hexdigest())
                if process.wait(timeout=120) != 0:
                    raise ValueError("independent decoder failed")
            row = {
                "path": name,
                "sha256": sha(path),
                "width": width,
                "height": height,
                "frames": len(hashes),
                "picture_sha256": hashes,
                "command": [
                    "SOURCE" if v == str(source) else "OUTPUT" if v == str(path) else v
                    for v in command
                ],
                "oracle": decoder[:-1] + ["STDOUT"],
                "modifications": "8-bit SDR transcode; 1080p upscales an excerpt of the 480p source, not native 1080p detail",
            }
            manifest["streams"].append(row)
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    args = parser.parse_args()
    generate(args.source, args.output, args.ffmpeg)


if __name__ == "__main__":
    main()
