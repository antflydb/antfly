#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Dynamic avc3 transport; native samples qualify each sequence with FFmpeg."""

import hashlib
import json
import tempfile
import subprocess
from pathlib import Path
from generate_h264_advanced_vectors import (
    DATA,
    Bits,
    pcm_vector,
    interlaced_pcm,
    mux_pcm,
    validate_known,
)

START = b"\x00\x00\x00\x01"


def main():
    cases = []
    parts, frames = [], []
    with tempfile.TemporaryDirectory() as name:
        for width, height, depth, chroma, cabac in [
            (64, 64, 8, 1, False),
            (96, 96, 10, 2, True),
            (80, 96, 14, 3, False),
        ]:
            encoded, known = interlaced_pcm(
                True, depth, chroma, width=width, height=height, cabac=cabac
            )
            canonical = Path(name) / "canonical.mp4"
            canonical.write_bytes(mux_pcm(encoded, width, height))
            validate_known(Path(name), canonical, known, depth, chroma, width, height)
            parts.append(encoded)
            frames.append(
                dict(
                    width=width,
                    height=height,
                    bit_depth=depth,
                    chroma_format=chroma,
                    sha256=hashlib.sha256(known).hexdigest(),
                )
            )
        geometry = mux_pcm(parts, 64, 64, sample_entry="avc3")
        (DATA / "h264-dynamic-geometry.mp4").write_bytes(geometry)
        cases.append(
            dict(
                name="h264-dynamic-geometry",
                mp4_sha256=hashlib.sha256(geometry).hexdigest(),
                frames=frames,
            )
        )
        encoded = pcm_vector(False, 2, frames=3)
        nals = encoded.split(START)[1:]
        canonical.write_bytes(
            mux_pcm(
                [b"".join(START + n for n in nals[:3])] + [START + n for n in nals[3:]]
            )
        )
        raw = Path(name) / "pps.nv12"
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(canonical),
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
        known = raw.read_bytes()
        nals = encoded.split(START)[1:]
        pps = Bits()
        for value in (0, 0):
            pps.ue(value)
        pps.fixed(0, 2)
        for value in (0, 0, 0):
            pps.ue(value)
        pps.fixed(0, 3)
        for value in (2, 0, 0):
            pps.se(value)
        pps.fixed(1, 1)
        pps.fixed(0, 2)
        packets = [
            b"".join(START + n for n in nals[:3]),
            pps.nal(0x68) + START + nals[3],
            START + nals[1] + START + nals[4],
        ]
        clip = mux_pcm(packets, sample_entry="avc3")
        path = DATA / "h264-dynamic-pps.mp4"
        path.write_bytes(clip)
        validate_known(Path(name), path, known, 8, 1, 64, 48)
        size = len(known) // 3
        frames = [
            dict(
                width=64,
                height=48,
                bit_depth=8,
                chroma_format=1,
                sha256=hashlib.sha256(known[i * size : (i + 1) * size]).hexdigest(),
            )
            for i in range(3)
        ]
        cases.append(
            dict(
                name="h264-dynamic-pps",
                mp4_sha256=hashlib.sha256(clip).hexdigest(),
                frames=frames,
            )
        )
    (DATA / "h264-dynamic-oracle.json").write_text(
        json.dumps(dict(cases=cases), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
