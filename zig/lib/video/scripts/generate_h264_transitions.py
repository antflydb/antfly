#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Non-IDR SPS transitions; canonical per-epoch FFmpeg and known-sample oracles."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from generate_h264_advanced_vectors import (
    DATA,
    Bits,
    configuration,
    pcm_vector,
    mux_pcm,
)

START = b"\x00\x00\x00\x01"


def raw(path, output):
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
            str(output),
        ],
        check=True,
    )
    return output.read_bytes()


def frame(width, height, pixels):
    return dict(
        width=width,
        height=height,
        bit_depth=8,
        chroma_format=1,
        sha256=hashlib.sha256(pixels).hexdigest(),
    )


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        cases = []
        first = pcm_vector(False, 2, frames=1)
        canonical = root / "canonical.mp4"
        canonical.write_bytes(mux_pcm(first))
        known = raw(canonical, root / "first.raw")
        sps, pps = configuration(crop_right=1, references=2)

        def skip(number):
            bits = Bits()
            for value in (0, 0, 0):
                bits.ue(value)
            bits.fixed(number, 4)
            bits.fixed(0, 3)  # reference override, list reordering, adaptive marking
            bits.se(0)
            bits.ue(1)
            bits.ue(64 * 48 // 256)
            return bits.nal(0x61)

        packets = [
            first,
            sps.nal(0x67) + pps.nal(0x68) + skip(1),
            START + first.split(START)[1] + skip(2),
        ]
        crop = b"".join(known[y * 64 : y * 64 + 62] for y in range(48)) + b"".join(
            known[64 * 48 + y * 64 : 64 * 48 + y * 64 + 62] for y in range(24)
        )
        payload = mux_pcm(packets, sample_entry="avc3")
        (DATA / "h264-sps-predicted.mp4").write_bytes(payload)
        cases.append(
            dict(
                name="sps-predicted",
                mp4_sha256=hashlib.sha256(payload).hexdigest(),
                frames=[
                    frame(64, 48, known),
                    frame(62, 48, crop),
                    frame(64, 48, known),
                ],
            )
        )
        packets = [first]
        frames = [frame(64, 48, known)]
        for width, height in [(80, 64), (96, 48)]:
            canonical.write_bytes(
                mux_pcm(pcm_vector(False, 2, width, height, frames=1), width, height)
            )
            pixels = raw(canonical, root / "epoch.raw")
            packets.append(
                pcm_vector(
                    False,
                    2,
                    width,
                    height,
                    frames=1,
                    frame_numbers=[1],
                    idr_first=False,
                )
            )
            frames.append(frame(width, height, pixels))
        payload = mux_pcm(packets, sample_entry="avc3")
        (DATA / "h264-sps-intra-geometry.mp4").write_bytes(payload)
        cases.append(
            dict(
                name="sps-intra-geometry",
                mp4_sha256=hashlib.sha256(payload).hexdigest(),
                frames=frames,
            )
        )
        (DATA / "h264-transitions-oracle.json").write_text(
            json.dumps(dict(cases=cases), indent=2) + "\n"
        )


if __name__ == "__main__":
    main()
