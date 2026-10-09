#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent, offline H.264 coding-tool vectors (x264 encode/FFmpeg decode)."""

import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
data = root / "testdata"


def run(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def oracle(name):
    mp4, nv12 = data / (name + ".mp4"), data / (name + ".nv12")
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
    probe = json.loads(
        subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_streams",
                "-show_frames",
                "-of",
                "json",
                str(mp4),
            ],
            text=True,
        )
    )
    stream = probe["streams"][0]
    return dict(
        name=name,
        width=stream["width"],
        height=stream["height"],
        profile=stream["profile"],
        frames=len(probe["frames"]),
        picture_types=[f["pict_type"] for f in probe["frames"]],
        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
        nv12_sha256=hashlib.sha256(nv12.read_bytes()).hexdigest(),
    )


# Stable single-threaded encoder settings; parameter strings are part of receipts.
cli_cases = [
    ("h264-main-intra", 64, 48, 4, "main", "keyint=1:cabac=1:8x8dct=0", None),
    ("h264-high-intra", 128, 96, 4, "high", "keyint=1:cabac=1:8x8dct=1", None),
    ("h264-high-cavlc8", 128, 96, 4, "high", "keyint=1:cabac=0:8x8dct=1", None),
    ("h264-baseline-p", 128, 96, 8, "baseline", "bframes=0:ref=1:weightp=0", None),
    ("h264-high-p", 128, 96, 8, "high", "bframes=0:ref=1:weightp=0", None),
    (
        "h264-main-b",
        128,
        96,
        12,
        "main",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=2:weightp=0:weightb=0:cabac=1:direct=spatial",
        None,
    ),
    (
        "h264-high-b",
        128,
        96,
        12,
        "high",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=2:weightp=0:weightb=0:cabac=1:direct=temporal",
        None,
    ),
    (
        "h264-cavlc-b",
        128,
        96,
        12,
        "main",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=2:weightp=0:weightb=0:cabac=0:direct=spatial",
        None,
    ),
    (
        "h264-high-weighted",
        128,
        96,
        16,
        "high",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1:direct=spatial",
        None,
    ),
    (
        "h264-high-fade",
        128,
        96,
        16,
        "high",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1:direct=spatial",
        "fade=t=in:st=0:d=3",
    ),
    (
        "h264-high-pyramid",
        128,
        96,
        16,
        "high",
        "bframes=3:b-adapt=0:b-pyramid=normal:ref=4:weightp=2:weightb=1:direct=auto",
        None,
    ),
    (
        "h264-high-wrap",
        64,
        48,
        40,
        "high",
        "bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1:direct=spatial",
        None,
    ),
]
cli_cases += [
    (
        "h264-high-jvt",
        128,
        96,
        12,
        "high",
        "cqm=jvt:bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1",
        None,
    ),
    (
        "h264-high-custom",
        128,
        96,
        12,
        "high",
        "cqm4iy="
        + ",".join(str(7 + (i * 11) % 31) for i in range(16))
        + ":cqm4py="
        + ",".join(str(11 + (i * 7) % 41) for i in range(16))
        + ":cqm4ic="
        + ",".join(str(9 + (i * 13) % 29) for i in range(16))
        + ":cqm4pc="
        + ",".join(str(13 + (i * 5) % 37) for i in range(16))
        + ":cqm8iy="
        + ",".join(str(8 + (i * 7) % 43) for i in range(64))
        + ":cqm8py="
        + ",".join(str(9 + (i * 11) % 47) for i in range(64))
        + ":bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1",
        None,
    ),
]

cli_cases += [
    (
        "h264-baseline-slices",
        128,
        96,
        12,
        "baseline",
        "slice-max-mbs=5:bframes=0:ref=2:weightp=0",
        None,
    ),
    (
        "h264-high-slices",
        128,
        96,
        12,
        "high",
        "slice-max-mbs=5:bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1:cqm=jvt",
        None,
    ),
    (
        "h264-high-constrained",
        128,
        96,
        12,
        "high",
        "slice-max-mbs=5:constrained-intra=1:bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1",
        None,
    ),
    (
        "h264-high-slice-threads",
        128,
        96,
        12,
        "high",
        "threads=3:sliced-threads=1:slices=3:bframes=2:b-adapt=0:b-pyramid=none:ref=3:weightp=2:weightb=1",
        None,
    ),
]

with tempfile.TemporaryDirectory(prefix="antfly-h264-tools-") as tmp:
    work = Path(tmp)
    flags = shlex.split(
        subprocess.check_output(["pkg-config", "--cflags", "--libs", "x264"], text=True)
    )
    run(
        "cc",
        str(root / "scripts/generate_h264_intra.c"),
        *flags,
        "-o",
        str(work / "encoder"),
    )
    receipts = []
    for name, width, height, qp, filtered in [
        ("h264-intra4", 64, 48, 23, 0),
        ("h264-intra4-filtered", 64, 48, 23, 1),
        ("h264-intra4-crop", 62, 46, 39, 1),
        ("h264-intra4-low", 64, 48, 3, 1),
    ]:
        run(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"testsrc2=size={width}x{height}:rate=4",
            "-frames:v",
            "4",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rawvideo",
            str(work / "input.yuv"),
        )
        run(
            str(work / "encoder"),
            str(work / "input.yuv"),
            str(work / "out.264"),
            str(width),
            str(height),
            "4",
            str(qp),
            "0",
            str(filtered),
            "1",
        )
        run(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-framerate",
            "4",
            "-i",
            str(work / "out.264"),
            "-c",
            "copy",
            "-bsf:v",
            "filter_units=remove_types=7|8",
            str(data / (name + ".mp4")),
        )
        receipts.append(
            oracle(name)
            | dict(
                qp=qp,
                deblocking=bool(filtered),
                encoder="generate_h264_intra.c with Intra4 enabled",
            )
        )
    for name, width, height, frames, profile, params, effect in cli_cases:
        params = f"threads=1:keyint={frames}:min-keyint={frames}:scenecut=0:" + params
        args = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"testsrc2=size={width}x{height}:rate=4",
        ]
        if effect:
            args += ["-vf", effect]
        run(
            *args,
            "-frames:v",
            str(frames),
            "-c:v",
            "libx264",
            "-profile:v",
            profile,
            "-qp",
            "23",
            "-x264-params",
            params,
            "-bsf:v",
            "filter_units=remove_types=7|8",
            str(data / (name + ".mp4")),
        )
        receipts.append(oracle(name) | dict(qp=23, x264_params=params, effect=effect))
    (data / "h264-tools-oracle.json").write_text(
        json.dumps(
            dict(
                ffmpeg=subprocess.check_output(
                    ["ffmpeg", "-version"], text=True
                ).splitlines()[0],
                x264=subprocess.check_output(
                    ["pkg-config", "--modversion", "x264"], text=True
                ).strip(),
                cases=receipts,
            ),
            indent=2,
        )
        + "\n"
    )
