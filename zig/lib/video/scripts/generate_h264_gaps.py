#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""PCM pictures with declared frame-number gaps and wraparound, FFmpeg oracle."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from generate_h264_advanced_vectors import DATA, pcm_vector, mux_pcm


def main():
    cases = []
    with tempfile.TemporaryDirectory() as name:
        for poc in (1, 2):
            for cabac in (False, True):
                encoded = pcm_vector(
                    cabac, poc, frames=4, frame_numbers=[0, 3, 6, 9], gaps=True
                )
                nals = encoded.split(b"\x00\x00\x00\x01")[1:]
                parts = [b"".join(b"\x00\x00\x00\x01" + n for n in nals[:3])] + [
                    b"\x00\x00\x00\x01" + n for n in nals[3:]
                ]
                stem = f"h264-gaps-poc{poc}-{'cabac' if cabac else 'cavlc'}"
                payload = mux_pcm(parts)
                path = DATA / (stem + ".mp4")
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
                        "-fps_mode",
                        "passthrough",
                        "-pix_fmt",
                        "nv12",
                        "-f",
                        "rawvideo",
                        str(raw),
                    ],
                    check=True,
                )
                frames = raw.read_bytes()
                size = 64 * 48 * 3 // 2
                assert len(frames) == 4 * size
                cases.append(
                    dict(
                        name=stem,
                        mp4_sha256=hashlib.sha256(payload).hexdigest(),
                        frames=[
                            hashlib.sha256(frames[i : i + size]).hexdigest()
                            for i in range(0, len(frames), size)
                        ],
                    )
                )
    (DATA / "h264-gaps-oracle.json").write_text(
        json.dumps(dict(cases=cases), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
