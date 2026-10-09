#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Offline FFmpeg/FFprobe indexes; no runtime media tool dependency."""

import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
VIDEO = ROOT.parent / "video/testdata"
DATA = ROOT / "testdata"
for name, source, flags, codec in [
    (
        "fragmented",
        "schedule-closed.mp4",
        "frag_keyframe+empty_moov+default_base_moof",
        "copy",
    ),
    (
        "fragmented-bframes",
        "decode-bframes.mp4",
        "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets",
        "copy",
    ),
    ("video", "schedule-closed.mp4", None, "libvpx-vp9"),
]:
    path = DATA / (name + (".webm" if name == "video" else ".mp4"))
    args = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(VIDEO / source),
    ]
    args += (
        ["-c", codec]
        if codec == "copy"
        else [
            "-c:v",
            codec,
            "-deadline",
            "realtime",
            "-cpu-used",
            "8",
            "-g",
            "12",
            "-an",
        ]
    )
    if flags:
        args += ["-movflags", flags]
    subprocess.run([*args, str(path)], check=True)
    oracle = json.loads(
        subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=id,width,height,time_base:packet=pts,dts,duration,pos,size,flags",
                "-of",
                "json",
                str(path),
            ],
            text=True,
        )
    )
    oracle["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    if name == "fragmented-bframes":
        # The generated first IDR has source PTS zero; FFprobe translates signed
        # composition timestamps by one frame. Preserve this explicit origin.
        oracle["ffprobe_shift_ticks"] = oracle["packets"][0]["pts"]
    receipt = "webm-oracle.json" if name == "video" else name + "-oracle.json"
    (DATA / receipt).write_text(json.dumps(oracle, indent=2) + "\n")
