#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Offline x264/FFmpeg oracles. Neither library is a runtime dependency."""

import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "testdata"


def run(*args):
    subprocess.run(args, check=True)


with tempfile.TemporaryDirectory(prefix="antfly-h264-") as tmp:
    work = Path(tmp)
    flags = shlex.split(
        subprocess.check_output(["pkg-config", "--cflags", "--libs", "x264"], text=True)
    )
    run(
        "cc",
        str(ROOT / "scripts/generate_h264_intra.c"),
        *flags,
        "-o",
        str(work / "encoder"),
    )
    cases = [
        ("h264-intra", 64, 48, 8, 23, 0, 0),
        ("h264-intra-qp3", 64, 48, 2, 3, 0, 0),
        ("h264-intra-crop", 62, 46, 2, 45, 0, 0),
        ("h264-intra-full", 64, 48, 2, 12, 1, 0),
        ("h264-intra-plane", 64, 48, 1, 23, 0, 0),
        ("h264-intra-filtered", 64, 48, 1, 23, 0, 1),
    ]
    receipts = []
    for name, width, height, frames, qp, full, filtered in cases:
        raw, annex = work / "input.yuv", work / "output.264"
        run(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            (
                f"nullsrc=size={width}x{height}:rate=4,geq=lum=40+X*2+Y:cb=80+X+Y:cr=170-X+Y"
                if name.endswith("plane")
                else f"testsrc2=size={width}x{height}:rate=4"
            ),
            "-frames:v",
            str(frames),
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rawvideo",
            str(raw),
        )
        run(
            str(work / "encoder"),
            str(raw),
            str(annex),
            str(width),
            str(height),
            str(frames),
            str(qp),
            str(full),
            str(filtered),
        )
        mp4, nv12 = DATA / (name + ".mp4"), DATA / (name + ".nv12")
        run(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-framerate",
            "4",
            "-i",
            str(annex),
            "-c",
            "copy",
            "-bsf:v",
            "filter_units=remove_types=7|8",
            str(mp4),
        )
        run(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(mp4),
            "-pix_fmt",
            "nv12",
            "-f",
            "rawvideo",
            str(nv12),
        )
        receipt = json.loads(
            subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "stream=width,height,time_base,color_range:packet=pts,dts,duration,pos,size,flags",
                    "-of",
                    "json",
                    str(mp4),
                ],
                text=True,
            )
        )
        receipt.update(
            name=name,
            qp=qp,
            full_range=bool(full),
            filtered=bool(filtered),
            mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
            nv12_sha256=hashlib.sha256(nv12.read_bytes()).hexdigest(),
        )
        receipts.append(receipt)
    ffmpeg = subprocess.check_output(["ffmpeg", "-version"], text=True).splitlines()[0]
    (DATA / "h264-intra-oracle.json").write_text(
        json.dumps(dict(ffmpeg=ffmpeg, cases=receipts), indent=2) + "\n"
    )


# A normative I_PCM access unit qualifies byte-aligned uncompressed samples,
# including emulation-prevention bytes, independently of encoder heuristics.
class Bits:
    def __init__(self):
        self.bits = ""

    def fixed(self, value, size):
        self.bits += format(value, f"0{size}b")

    def ue(self, value):
        code = format(value + 1, "b")
        self.bits += "0" * (len(code) - 1) + code

    def nal(self, kind):
        bits = self.bits + "1"
        bits += "0" * (-len(bits) % 8)
        raw = bytes(int(bits[i : i + 8], 2) for i in range(0, len(bits), 8))
        output, zeros = bytearray([kind]), 0
        for value in raw:
            if zeros == 2 and value <= 3:
                output.append(3)
                zeros = 0
            output.append(value)
            zeros = zeros + 1 if value == 0 else 0
        return b"\x00\x00\x00\x01" + output


sps = Bits()
for value in (66, 0xC0, 10):
    sps.fixed(value, 8)
for value in (0, 0, 2, 0):
    sps.ue(value)
sps.fixed(0, 1)
for value in (0, 0):
    sps.ue(value)
for value in (1, 1, 0, 0):
    sps.fixed(value, 1)
pps = Bits()
pps.ue(0)
pps.ue(0)
pps.fixed(0, 2)
for value in (0, 0, 0):
    pps.ue(value)
pps.fixed(0, 3)
for value in (0, 0, 0):
    pps.ue(value)  # signed zero has ue(0) codeword
pps.fixed(1, 1)
pps.fixed(0, 2)
slice_bits = Bits()
for value in (0, 7, 0):
    slice_bits.ue(value)
slice_bits.fixed(0, 4)
slice_bits.ue(0)
slice_bits.fixed(0, 2)
slice_bits.ue(0)
slice_bits.ue(1)
slice_bits.ue(25)
slice_bits.bits += "0" * (-len(slice_bits.bits) % 8)
raw_pixels = (
    bytes(96)
    + bytes((i * 17) % 256 for i in range(96, 256))
    + bytes(range(64))
    + bytes(255 - i for i in range(64))
)
for value in raw_pixels:
    slice_bits.fixed(value, 8)
with tempfile.TemporaryDirectory(prefix="antfly-h264-pcm-") as tmp:
    annex = Path(tmp) / "pcm.264"
    annex.write_bytes(sps.nal(0x67) + pps.nal(0x68) + slice_bits.nal(0x65))
    mp4, nv12 = DATA / "h264-pcm.mp4", DATA / "h264-pcm.nv12"
    run(
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-framerate",
        "4",
        "-i",
        str(annex),
        "-c",
        "copy",
        "-bsf:v",
        "filter_units=remove_types=7|8",
        str(mp4),
    )
    run(
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(mp4),
        "-pix_fmt",
        "nv12",
        "-f",
        "rawvideo",
        str(nv12),
    )

    receipts.append(
        dict(
            name="h264-pcm",
            mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
            nv12_sha256=hashlib.sha256(nv12.read_bytes()).hexdigest(),
            full_range=False,
            filtered=False,
        )
    )
    (DATA / "h264-intra-oracle.json").write_text(
        json.dumps(dict(ffmpeg=ffmpeg, cases=receipts), indent=2) + "\n"
    )
