#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Independent FFmpeg/FFprobe transport packet and decoded-picture oracles."""

import hashlib
import json
from pathlib import Path
import subprocess
import shutil

ROOT = Path(__file__).resolve().parents[4]
OUTPUT = ROOT / "zig/lib/media/testdata/transport"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def raw_data(text):
    result = bytearray()
    for line in text.splitlines():
        if not line.strip():
            continue
        value = line.split(":", 1)[1].split("  ", 1)[0]
        result.extend(bytes.fromhex(value))
    return bytes(result)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generator_sha256": sha(Path(__file__).read_bytes()),
        "ffmpeg": subprocess.check_output(
            ["ffmpeg", "-version"], text=True
        ).splitlines()[0],
        "cases": {},
    }
    for name, filename in (
        ("closed", "schedule-closed.mp4"),
        ("bframes", "decode-bframes.mp4"),
    ):
        source = ROOT / "zig/lib/video/testdata" / filename
        target = OUTPUT / (name + ".ts")
        command = [
            "ffmpeg",
            "-y",
            "-v",
            "error",
            "-i",
            str(source),
            "-map",
            "0:v:0",
            "-c:v",
            "copy",
            "-bsf:v",
            "h264_mp4toannexb",
            "-avoid_negative_ts",
            "make_zero",
            "-mpegts_flags",
            "resend_headers",
            "-muxdelay",
            "0",
            "-muxpreload",
            "0",
            "-f",
            "mpegts",
            str(target),
        ]
        subprocess.run(command, check=True)
        probe = json.loads(
            subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_packets",
                    "-show_data",
                    "-show_streams",
                    "-of",
                    "json",
                    str(target),
                ]
            )
        )
        payload = bytearray()
        packets = []
        for row in probe["packets"]:
            data = raw_data(row["data"])
            if len(data) != int(row["size"]):
                raise ValueError("FFprobe packet size mismatch")
            packets.append(
                {
                    "offset": len(payload),
                    "size": len(data),
                    "pts": int(row["pts"]),
                    "dts": int(row["dts"]),
                    "duration": int(row["duration"]),
                    "sync": "K" in row["flags"],
                    "sha256": sha(data),
                }
            )
            payload.extend(data)
        (OUTPUT / (name + "-pes.bin")).write_bytes(payload)
        stream = probe["streams"][0]
        width, height = int(stream["width"]), int(stream["height"])
        decoded = subprocess.check_output(
            [
                "ffmpeg",
                "-v",
                "error",
                "-i",
                str(target),
                "-pix_fmt",
                "nv12",
                "-fps_mode",
                "passthrough",
                "-f",
                "rawvideo",
                "-",
            ]
        )
        frame_bytes = width * height * 3 // 2
        if len(decoded) % frame_bytes:
            raise ValueError("invalid independent decoded geometry")
        oracle = {
            "packets": packets,
            "width": width,
            "height": height,
            "time_base": stream["time_base"],
            "decoded_sha256": [
                sha(decoded[i : i + frame_bytes])
                for i in range(0, len(decoded), frame_bytes)
            ],
        }
        (OUTPUT / (name + ".json")).write_text(json.dumps(oracle, indent=2) + "\n")
        manifest["cases"][name] = {
            "source": str(source.relative_to(ROOT)),
            "source_sha256": sha(source.read_bytes()),
            "transport_sha256": sha(target.read_bytes()),
            "pes_sha256": sha(payload),
            "oracle_sha256": sha((OUTPUT / (name + ".json")).read_bytes()),
            "command": command,
        }
    video_output = ROOT / "zig/lib/video/testdata/transport"
    video_output.mkdir(exist_ok=True)
    for name in manifest["cases"]:
        for suffix in (".ts", ".json"):
            shutil.copyfile(OUTPUT / (name + suffix), video_output / (name + suffix))
    (OUTPUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
