#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate original synthetic video fixtures and independent ffprobe receipts.
FFmpeg is an offline oracle; no runtime code invokes it. Re-run explicitly when
updating the oracle, and review hashes/version changes with the resulting diff.
"""

import hashlib
import json
from pathlib import Path
import subprocess
import struct

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "testdata"
DATA.mkdir(exist_ok=True)


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


base = [
    "ffmpeg",
    "-nostdin",
    "-hide_banner",
    "-loglevel",
    "error",
    "-y",
    "-f",
    "lavfi",
    "-i",
    "testsrc2=size=32x24:rate=10:duration=2",
]
codec = [
    "-c:v",
    "libx264",
    "-threads",
    "1",
    "-pix_fmt",
    "yuv420p",
    "-g",
    "10",
    "-bf",
    "2",
]
run(base + codec + [str(DATA / "bframes-tail.mp4")])
run(
    base
    + ["-vf", "select=not(mod(n\\,3))", "-fps_mode", "vfr"]
    + codec
    + ["-movflags", "+faststart", str(DATA / "vfr-front.mp4")]
)
run(
    [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-display_rotation",
        "90",
        "-i",
        str(DATA / "bframes-tail.mp4"),
        "-c",
        "copy",
        str(DATA / "rotated.mp4"),
    ]
)
run(
    base
    + ["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=2"]
    + codec
    + ["-c:a", "aac", "-shortest", str(DATA / "video-audio.mp4")]
)
run(base + codec + ["-movflags", "+negative_cts_offsets", str(DATA / "signed-cts.mp4")])


# Rewrite only the tail metadata, keeping every packet offset unchanged. This
# independently exercises co64 chunk offsets and 16-bit compact stz2 sizes.
def rewrite_boxes(data):
    out = bytearray()
    cursor = 0
    while cursor < len(data):
        size, kind = struct.unpack_from(">I4s", data, cursor)
        payload = data[cursor + 8 : cursor + size]
        if kind in [b"moov", b"trak", b"mdia", b"minf", b"stbl", b"edts", b"dinf"]:
            payload = rewrite_boxes(payload)
        elif kind == b"stco":
            count = struct.unpack_from(">I", payload, 4)[0]
            payload = payload[:8] + b"".join(
                struct.pack(">Q", struct.unpack_from(">I", payload, 8 + i * 4)[0])
                for i in range(count)
            )
            kind = b"co64"
        elif kind == b"stsz":
            fixed, count = struct.unpack_from(">II", payload, 4)
            sizes = [
                fixed or struct.unpack_from(">I", payload, 12 + i * 4)[0]
                for i in range(count)
            ]
            payload = (
                b"\0" * 7
                + b"\x10"
                + struct.pack(">I", count)
                + b"".join(struct.pack(">H", v) for v in sizes)
            )
            kind = b"stz2"
        out += struct.pack(">I4s", len(payload) + 8, kind) + payload
        cursor += size
    return bytes(out)


(DATA / "compact-co64.mp4").write_bytes(
    rewrite_boxes((DATA / "bframes-tail.mp4").read_bytes())
)
receipts = []
for name in [
    "bframes-tail.mp4",
    "vfr-front.mp4",
    "rotated.mp4",
    "video-audio.mp4",
    "signed-cts.mp4",
    "compact-co64.mp4",
]:
    path = DATA / name
    info = json.loads(
        run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_streams",
                "-show_packets",
                "-of",
                "json",
                str(path),
            ]
        )
    )
    stream = info["streams"][0]
    receipts.append(
        dict(
            file=name,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            timescale=int(stream["time_base"].split("/")[1]),
            track_id=int(stream["id"], 16),
            width=stream["width"],
            height=stream["height"],
            packets=[
                dict(
                    dts=int(p["dts"]),
                    pts=int(p["pts"]),
                    duration=int(p["duration"]),
                    offset=int(p["pos"]),
                    size=int(p["size"]),
                    sync="K" in p["flags"],
                )
                for p in info["packets"]
            ],
            side_data=stream.get("side_data_list", []),
        )
    )
manifest = dict(
    provenance="Original synthetic testsrc2 and sine fixtures; generated locally for Antfly under Apache-2.0. No downloaded media.",
    ffmpeg=run(["ffmpeg", "-version"]).decode().splitlines()[0],
    ffprobe=run(["ffprobe", "-version"]).decode().splitlines()[0],
    fixtures=receipts,
)
(DATA / "mp4-oracle.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"Generated {len(receipts)} MP4 fixtures and ffprobe packet receipts")
