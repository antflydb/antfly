#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Original portable MJPEG/MOV and independent FFmpeg RGBA oracle."""

import hashlib
import json
import subprocess
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "testdata"


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


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
        "testsrc2=size=64x48:rate=4:duration=2",
        "-c:v",
        "mjpeg",
        "-threads",
        "1",
        "-pix_fmt",
        "yuvj444p",
        str(DATA / "mjpeg.mov"),
    ]
)
run(
    [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(DATA / "mjpeg.mov"),
        "-sws_flags",
        "neighbor",
        "-pix_fmt",
        "rgba",
        "-f",
        "rawvideo",
        str(DATA / "mjpeg.rgba"),
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
            "packet=pts,dts,size,pos",
            "-of",
            "json",
            str(DATA / "mjpeg.mov"),
        ]
    )
)["packets"]
receipt = dict(
    provenance="Original Apache-2.0 testsrc2: 8 complete baseline 4:4:4 JPEG samples, 64x48, 4 FPS. FFmpeg RGBA oracle uses neighbor chroma upsampling; no runtime FFmpeg dependency.",
    ffmpeg=run(["ffmpeg", "-version"]).decode().splitlines()[0],
    packets=packets,
    files=[
        dict(
            file=name,
            bytes=(DATA / name).stat().st_size,
            sha256=hashlib.sha256((DATA / name).read_bytes()).hexdigest(),
        )
        for name in ["mjpeg.mov", "mjpeg.rgba"]
    ],
)
(DATA / "mjpeg-oracle.json").write_text(json.dumps(receipt, indent=2) + "\n")
