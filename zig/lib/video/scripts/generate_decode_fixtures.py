#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Regenerate original native decode/preparation fixtures and FFmpeg receipts."""

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "testdata"


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


shutil.copyfile(
    ROOT.parent / "media/testdata/bframes-tail.mp4", DATA / "decode-bframes.mp4"
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
        str(DATA / "decode-bframes.mp4"),
        "-pix_fmt",
        "nv12",
        "-f",
        "rawvideo",
        str(DATA / "decode-bframes.nv12"),
    ]
)
for name, source, extra in [
    ("prepare-sdr.mp4", "testsrc2=size=160x96:rate=2:duration=1", []),
    ("decode-hardware.mp4", "testsrc2=size=640x360:rate=4:duration=1", ["-bf", "2"]),
]:
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
            source,
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-pix_fmt",
            "yuv420p",
            *extra,
            str(DATA / name),
        ]
    )
files = [
    "decode-bframes.mp4",
    "decode-bframes.nv12",
    "prepare-sdr.mp4",
    "decode-hardware.mp4",
]
manifest = dict(
    provenance="Original synthetic testsrc2 clips under Apache-2.0. decode-bframes is copied from the shared media fixture. NV12 is independent FFmpeg decode in presentation order, tightly packed 32x24 luma then interleaved 16x12 chroma, 20 pictures.",
    ffmpeg=run(["ffmpeg", "-version"]).decode().splitlines()[0],
    files=[
        dict(
            file=name,
            sha256=hashlib.sha256((DATA / name).read_bytes()).hexdigest(),
            bytes=(DATA / name).stat().st_size,
        )
        for name in files
    ],
)
(DATA / "decode-oracle.json").write_text(json.dumps(manifest, indent=2) + "\n")
print("Generated four decode/preparation artifacts and provenance receipts")
