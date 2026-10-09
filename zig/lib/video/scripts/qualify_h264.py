#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare a native hash JSONL against FFmpeg's native-depth decoded samples.

This does not encode a new fixture or convert component depth/range. It strips
movie edits from the oracle clock and compares media PTS. Any mismatch fails.
"""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path


def qualify(path, native):
    info = json.loads(
        subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-ignore_editlist",
                "1",
                "-select_streams",
                "v:0",
                "-show_streams",
                "-show_frames",
                "-of",
                "json",
                str(path),
            ]
        )
    )
    stream = info["streams"][0]
    width, height, fmt = stream["width"], stream["height"], stream["pix_fmt"]
    # Exact integer-plane storage; no RGB or limited/full range conversion.
    if fmt.startswith("yuvj"):
        fmt = "yuv" + fmt[4:]
    chroma = next((c for c in ("420", "422", "444") if c in fmt), None)
    if chroma is None:
        raise ValueError("use a per-plane native-depth oracle for " + fmt)
    suffix = fmt.split("p", 1)[1]
    depth = int(suffix[:-2]) if suffix else 8
    word = 1 if depth == 8 else 2
    sx, sy = (1 if chroma == "444" else 2), (2 if chroma == "420" else 1)
    y_bytes, c_bytes = width * height * word, width // sx * (height // sy) * word
    size = y_bytes + 2 * c_bytes
    records = [json.loads(line) for line in Path(native).read_text().splitlines()]
    by_pts = {}
    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / "native.raw"
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ignore_editlist",
                "1",
                "-i",
                str(path),
                "-map",
                "0:v:0",
                "-fps_mode",
                "passthrough",
                "-pix_fmt",
                fmt,
                "-f",
                "rawvideo",
                str(raw),
            ],
            check=True,
        )
        pixels = raw.read_bytes()
        frames = info["frames"]
        assert len(pixels) == size * len(frames), (len(pixels), size, len(frames))
        for i, frame in enumerate(frames):
            block = pixels[i * size : (i + 1) * size]
            uv = bytearray(2 * c_bytes)
            for n in range(c_bytes // word):
                uv[n * 2 * word : (n * 2 + 1) * word] = block[
                    y_bytes + n * word : y_bytes + (n + 1) * word
                ]
                uv[(n * 2 + 1) * word : (n * 2 + 2) * word] = block[
                    y_bytes + c_bytes + n * word : y_bytes + c_bytes + (n + 1) * word
                ]
            by_pts.setdefault(frame["pts"], []).append(
                hashlib.sha256(block[:y_bytes] + uv).hexdigest()
            )
    for record in records:
        expected = by_pts[record["media_pts"]]
        assert record["sha256"] in expected, record
        expected.remove(record["sha256"])
        assert (record["width"], record["height"], record["bit_depth"]) == (
            width,
            height,
            depth,
        ), record
    assert len(records) == len(info["frames"]), (len(records), len(info["frames"]))
    return dict(
        input_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest(),
        frames=len(records),
        width=width,
        height=height,
        native_pixel_format=fmt,
        ffmpeg=subprocess.check_output(["ffmpeg", "-version"], text=True).splitlines()[
            0
        ],
        result="all native sample hashes match",
        clock="media PTS, movie edit disabled",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("native_jsonl", type=Path)
    args = parser.parse_args()
    print(json.dumps(qualify(args.input, args.native_jsonl), sort_keys=True))


if __name__ == "__main__":
    main()
