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

"""FFmpeg-qualified native-depth monochrome PAFF PCM vectors."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from generate_h264_advanced_vectors import DATA, interlaced_pcm, mux_pcm


def main():
    cases = []
    with tempfile.TemporaryDirectory() as name:
        for depth in (8, 10, 14):
            for cabac in (False, True):
                encoded, known = interlaced_pcm(True, depth, 0, cabac=cabac)
                stem = f"h264-mono-{depth}-{'cabac' if cabac else 'cavlc'}"
                path = DATA / (stem + ".mp4")
                payload = mux_pcm(encoded, 64, 64)
                path.write_bytes(payload)
                raw = Path(name) / "oracle.raw"
                subprocess.run(
                    [
                        "ffmpeg",
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-y",
                        "-i",
                        str(path),
                        "-pix_fmt",
                        "yuv420p" if depth == 8 else f"yuv420p{depth}le",
                        "-f",
                        "rawvideo",
                        str(raw),
                    ],
                    check=True,
                )
                assert raw.read_bytes()[: len(known)] == known, stem
                cases.append(
                    dict(
                        name=stem,
                        bit_depth=depth,
                        mp4_sha256=hashlib.sha256(payload).hexdigest(),
                        sha256=hashlib.sha256(known).hexdigest(),
                    )
                )
    (DATA / "h264-mono-oracle.json").write_text(
        json.dumps(dict(cases=cases), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
