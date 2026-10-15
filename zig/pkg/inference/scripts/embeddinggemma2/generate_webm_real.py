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

"""Generate attributed VP9/AV1 real-stream fixtures using offline reference tools.

Normal Antfly builds never run this script or require these encoders/decoders.
The pinned source and checked-in fixture hashes remain qualification evidence.
"""

import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[5]
SOURCE = (
    ROOT
    / "zig/pkg/inference/scripts/embeddinggemma2/testdata/retrieval/scene-1-h264.mp4"
)
OUTPUT = ROOT / "zig/lib/video/testdata/vpx-av1"


def main():
    if (
        hashlib.sha256(SOURCE.read_bytes()).hexdigest()
        != "aa4bad5af54bd8c793b0206d3856b0c7a4cd53b8a80ea487a5f5c2555dafc570"
    ):
        raise ValueError("pinned Sintel source changed")
    records = []
    for codec, encoder in (
        (
            "vp9",
            [
                "-c:v",
                "libvpx-vp9",
                "-cpu-used",
                "4",
                "-crf",
                "30",
                "-b:v",
                "0",
                "-g",
                "48",
            ],
        ),
        (
            "av1",
            [
                "-c:v",
                "libsvtav1",
                "-preset",
                "10",
                "-crf",
                "30",
                "-g",
                "48",
                "-svtav1-params",
                "lp=2",
            ],
        ),
    ):
        target = OUTPUT / (codec + "-sintel.webm")
        command = [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-i",
            str(SOURCE),
            "-an",
            "-vf",
            "scale=192:128",
            "-pix_fmt",
            "yuv420p",
            *encoder,
            str(target),
        ]
        subprocess.run(command, check=True, timeout=120)
        raw = subprocess.check_output(
            [
                "ffmpeg",
                "-v",
                "error",
                "-i",
                str(target),
                "-fps_mode",
                "passthrough",
                "-pix_fmt",
                "yuv420p",
                "-f",
                "rawvideo",
                "-",
            ],
            timeout=120,
        )
        size = 192 * 128 * 3 // 2
        if len(raw) % size:
            raise ValueError("reference frame size mismatch")
        records.append(
            dict(
                name=codec + "-sintel",
                codec=codec,
                sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                command=command,
                width=192,
                height=128,
                frame_sha256=[
                    hashlib.sha256(raw[i : i + size]).hexdigest()
                    for i in range(0, len(raw), size)
                ],
            )
        )
    corpus = json.loads(
        (
            ROOT
            / "zig/pkg/inference/scripts/embeddinggemma2/testdata/retrieval/corpus.json"
        ).read_text()
    )
    provenance = next(
        row["provenance"] for row in corpus["documents"] if row["id"] == "scene-1-h264"
    )
    (OUTPUT / "real-streams.json").write_text(
        json.dumps(
            dict(
                provenance=provenance,
                source_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
                ffmpeg=subprocess.check_output(
                    ["ffmpeg", "-version"], text=True
                ).splitlines()[0],
                streams=records,
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
