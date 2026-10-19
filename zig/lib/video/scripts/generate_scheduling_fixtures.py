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

"""Original closed/open-GOP H.264 clips and FFprobe decode-order receipts."""

import hashlib
import json
import subprocess
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "testdata"


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


files = []
for name, opened in [("schedule-closed.mp4", 0), ("schedule-open.mp4", 1)]:
    path = DATA / name
    run(
        [
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x96:rate=10:duration=6",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-pix_fmt",
            "yuv420p",
            "-bf",
            "2",
            "-g",
            "10",
            "-keyint_min",
            "10",
            "-sc_threshold",
            "0",
            "-x264-params",
            f"open-gop={opened}:b-adapt=0",
            str(path),
        ]
    )
    packets = json.loads(
        run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_packets",
                "-show_entries",
                "packet=pts,dts,size,flags,pos",
                "-of",
                "json",
                str(path),
            ]
        )
    )["packets"]
    files.append(
        dict(
            file=name,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            bytes=path.stat().st_size,
            packets=packets,
        )
    )
receipt = dict(
    provenance="Original Apache-2.0 testsrc2 clips: 60 pictures, fixed GOP 10, two B pictures. Open GOP sync hints include non-IDR recovery pictures; only the initial IDR authorizes a start.",
    ffmpeg=run(["ffmpeg", "-version"]).decode().splitlines()[0],
    files=files,
)
(DATA / "scheduling-oracle.json").write_text(json.dumps(receipt, indent=2) + "\n")
print("Generated two scheduling fixtures and FFprobe packet receipts")
