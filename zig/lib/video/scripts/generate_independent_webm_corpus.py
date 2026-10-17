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

"""Pinned Big Buck Bunny excerpts and independent NV12 decoder oracles.

Qualification uses FFmpeg/libvpx/SVT-AV1 offline. Native runtime/build paths do
not invoke these tools. Derived pictures retain the movie's CC BY 3.0 license.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


SOURCE_SHA256 = "f78f39603e6774907f2faafabf26a667f4a6fc31769ec304a8a8f7c62d280508"
ARCHIVE_SHA256 = "109e3ede8790bd633f374ca311d9cc61dce8d7f98f5b0797ca98199c9fbceedf"
WIDTH, HEIGHT, FRAMES = 320, 180, 36


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def picture_hashes(ffmpeg, path):
    command = [
        ffmpeg,
        "-v",
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
    size = WIDTH * HEIGHT * 3 // 2
    hashes = []
    with subprocess.Popen(command, stdout=subprocess.PIPE) as process:
        try:
            while True:
                frame = process.stdout.read(size)
                if not frame:
                    break
                if len(frame) != size:
                    raise ValueError("truncated independent picture")
                hashes.append(hashlib.sha256(frame).hexdigest())
                if len(hashes) > FRAMES:
                    raise ValueError("unexpected visible picture count")
            if process.wait(timeout=60) != 0:
                raise ValueError("independent decoder failed")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    if len(hashes) != FRAMES:
        raise ValueError("independent visible picture count differs")
    return hashes, command


def generate(source, output, ffmpeg):
    if sha(source) != SOURCE_SHA256:
        raise ValueError("Big Buck Bunny source differs from pinned original")
    output.mkdir(parents=True, exist_ok=False)
    version = subprocess.check_output([ffmpeg, "-version"], text=True).splitlines()[0]
    manifest = {
        "version": 1,
        "status": "running",
        "tool_sha256": sha(Path(__file__)),
        "source": {
            "title": "Big Buck Bunny",
            "url": "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4.zip",
            "archive_sha256": ARCHIVE_SHA256,
            "member": "BigBuckBunny_320x180.mp4",
            "sha256": SOURCE_SHA256,
            "copyright": "(c) copyright 2008, Blender Foundation / www.bigbuckbunny.org",
            "license": "CC BY 3.0",
            "license_url": "https://creativecommons.org/licenses/by/3.0/",
            "attribution_source": "https://peach.blender.org/about/",
        },
        "encoder": version,
        "streams": [],
    }
    try:
        # Independent scenes, not repeated encodes of the existing Sintel input.
        for start in (48, 160, 300):
            for codec, encoder in (
                ("vp8", "libvpx"),
                ("vp9", "libvpx-vp9"),
                ("av1", "libsvtav1"),
            ):
                path = output / f"bbb-{codec}-{start}s.webm"
                command = [
                    ffmpeg,
                    "-v",
                    "error",
                    "-y",
                    "-ss",
                    str(start),
                    "-i",
                    str(source),
                    "-map",
                    "0:v:0",
                    "-an",
                    "-frames:v",
                    str(FRAMES),
                    "-vf",
                    "setsar=1",
                    "-r",
                    "24",
                    "-c:v",
                    encoder,
                    "-threads",
                    "2",
                    "-g",
                    "24",
                    "-pix_fmt",
                    "yuv420p",
                ]
                if codec in ("vp8", "vp9"):
                    command += [
                        "-deadline",
                        "good",
                        "-cpu-used",
                        "2",
                        "-lag-in-frames",
                        "12",
                        "-auto-alt-ref",
                        "1",
                        "-b:v",
                        "240k" if codec == "vp8" else "0",
                        "-crf",
                        "24" if codec == "vp8" else "32",
                    ]
                    if codec == "vp9":
                        command += ["-row-mt", "0"]
                else:
                    command += [
                        "-preset",
                        "8",
                        "-svtav1-params",
                        "lp=2",
                        "-b:v",
                        "0",
                        "-crf",
                        "32",
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
                print("Encoding", path.name, flush=True)
                subprocess.run(command, check=True, timeout=180)
                hashes, oracle = picture_hashes(ffmpeg, path)
                manifest["streams"].append(
                    {
                        "path": path.name,
                        "sha256": sha(path),
                        "width": WIDTH,
                        "height": HEIGHT,
                        "frames": FRAMES,
                        "depth": 8,
                        "chroma": 1,
                        "start_seconds": start,
                        "picture_sha256": hashes,
                        "command": [
                            "SOURCE"
                            if v == str(source)
                            else "OUTPUT"
                            if v == str(path)
                            else v
                            for v in command
                        ],
                        "oracle": [
                            "CODED_INPUT"
                            if v == str(path)
                            else "STDOUT"
                            if v == "-"
                            else v
                            for v in oracle
                        ],
                        "modifications": "36-picture 24 FPS excerpt, original 320x180 geometry, SAR1, 8-bit 4:2:0 transcode without audio",
                    }
                )
        manifest["status"] = "pass"
    except Exception as error:
        manifest["status"] = "failed"
        manifest["failure"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
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
