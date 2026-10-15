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

"""Repacketize pinned AVC into untimed continuation PES; verify with FFprobe."""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[4]
MEDIA = ROOT / "zig/lib/media/testdata/transport"
VIDEO = ROOT / "zig/lib/video/testdata/transport"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def stamp(value, prefix):
    value &= (1 << 33) - 1
    return bytes(
        (
            (prefix << 4) | ((value >> 29) & 14) | 1,
            (value >> 22) & 255,
            ((value >> 14) & 254) | 1,
            (value >> 7) & 255,
            ((value << 1) & 254) | 1,
        )
    )


def packetize(payload, pid, counter):
    result = bytearray()
    first = True
    while payload:
        chunk, payload = payload[:184], payload[184:]
        header = bytes(
            (
                0x47,
                (64 if first else 0) | (pid >> 8),
                pid & 255,
                (16 if len(chunk) == 184 else 48) | counter,
            )
        )
        if len(chunk) < 184:
            length = 183 - len(chunk)
            adaptation = bytes((length,)) + (
                bytes((0,)) + b"\xff" * (length - 1) if length else b""
            )
        else:
            adaptation = b""
        result.extend(header + adaptation + chunk)
        counter = (counter + 1) & 15
        first = False
    return result, counter


def main():
    manifest = dict(generator_sha256=sha(Path(__file__).read_bytes()), cases={})
    for name in ("closed", "bframes"):
        source = (MEDIA / f"{name}.ts").read_bytes()
        oracle = json.loads((MEDIA / f"{name}.json").read_text())
        raw = (MEDIA / f"{name}-pes.bin").read_bytes()
        # Preserve initialization PSI, ending immediately before first video PES.
        prefix = bytearray()
        for offset in range(0, len(source), 188):
            packet = source[offset : offset + 188]
            cursor = 4 + (1 + packet[4] if packet[3] & 32 else 0)
            if packet[1] & 64 and packet[cursor : cursor + 4] == b"\x00\x00\x01\xe0":
                pid = ((packet[1] & 31) << 8) | packet[2]
                break
            prefix.extend(packet)
        else:
            raise ValueError("source video PES not found")
        output, counter = prefix, 0
        pes_count = 0
        for row in oracle["packets"]:
            access_unit = raw[row["offset"] : row["offset"] + row["size"]]
            # Splits inside 3/4-byte prefixes, AUD/SPS/PPS and coded slice bytes.
            boundaries = sorted(
                {0, len(access_unit)}
                | {
                    x
                    for x in (1, 2, 3, 5, 10, 17, len(access_unit) // 2)
                    if 0 < x < len(access_unit)
                }
            )
            for index, (start, end) in enumerate(zip(boundaries, boundaries[1:])):
                clocks = (
                    stamp(row["pts"], 3) + stamp(row["dts"], 1) if index == 0 else b""
                )
                body = (
                    bytes((128, 192 if clocks else 0, len(clocks)))
                    + clocks
                    + access_unit[start:end]
                )
                declared = len(body) if len(body) <= 65535 else 0
                pes = b"\x00\x00\x01\xe0" + declared.to_bytes(2, "big") + body
                packets, counter = packetize(pes, pid, counter)
                output.extend(packets)
                pes_count += 1
        path = MEDIA / f"{name}-split.ts"
        path.write_bytes(output)
        probe = json.loads(
            subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_packets",
                    "-show_data_hash",
                    "sha256",
                    "-of",
                    "json",
                    str(path),
                ]
            )
        )
        if len(probe["packets"]) != len(oracle["packets"]):
            raise ValueError("independent access-unit count differs")
        for actual, expected in zip(probe["packets"], oracle["packets"]):
            if (
                ("pts" in actual and int(actual["pts"]) != expected["pts"])
                or ("dts" in actual and int(actual["dts"]) != expected["dts"])
                or actual["data_hash"] != "SHA256:" + expected["sha256"]
            ):
                raise ValueError("independent access-unit bytes/clocks differ")
        decoded = subprocess.check_output(
            [
                "ffmpeg",
                "-v",
                "error",
                "-i",
                str(path),
                "-pix_fmt",
                "nv12",
                "-fps_mode",
                "passthrough",
                "-f",
                "rawvideo",
                "-",
            ]
        )
        size = oracle["width"] * oracle["height"] * 3 // 2
        if [sha(decoded[i : i + size]) for i in range(0, len(decoded), size)] != oracle[
            "decoded_sha256"
        ]:
            raise ValueError("independent decoded pictures differ")
        for directory in (MEDIA, VIDEO):
            shutil.copyfile(MEDIA / f"{name}.json", directory / f"{name}-split.json")
        shutil.copyfile(path, VIDEO / path.name)
        manifest["cases"][name] = dict(
            source_sha256=sha(source),
            transport_sha256=sha(output),
            access_units=len(oracle["packets"]),
            pes_packets=pes_count,
            oracle_sha256=sha((MEDIA / f"{name}-split.json").read_bytes()),
            independent="FFprobe complete AU bytes and FFmpeg every NV12 picture; clocks preserved from original independent oracle",
            ffprobe_missing_timestamp_packets=sum(
                "pts" not in p or "dts" not in p for p in probe["packets"]
            ),
        )
    (MEDIA / "split-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
