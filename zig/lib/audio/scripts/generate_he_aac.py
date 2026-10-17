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

"""Generate owned, diverse HE-AAC/PS fixtures and independent FFmpeg oracles.

Apple's encoder is an offline fixture tool; the runtime remains maintained Zig.
"""

import argparse
from array import array
import hashlib
import json
import math
from pathlib import Path
import shutil
import struct
import subprocess
import wave


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate_signal(path, channels, rate=48000, seconds=2):
    state = 0x51F15EED
    samples = array("h")
    for i in range(rate * seconds):
        t = i / rate
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        noise = ((state >> 8) / 8388608.0 - 1) * 0.025
        envelope = min(1.0, max(0.0, (t - 0.03) * 100))
        envelope *= 0.25 + 0.75 * (0.5 + 0.5 * math.sin(2 * math.pi * 3.7 * t))
        if i % 7019 < 240:
            envelope *= 0.08
        left = envelope * (
            0.19 * math.sin(2 * math.pi * (230 * t + 900 * t * t))
            + 0.11 * math.sin(2 * math.pi * 7100 * t)
            + 0.075 * math.sin(2 * math.pi * 12100 * t)
            + noise
        )
        right = envelope * (
            0.17 * math.sin(2 * math.pi * (550 * t + 1200 * t * t))
            + 0.095 * math.sin(2 * math.pi * 5300 * t + 0.7)
            + 0.08 * math.sin(2 * math.pi * 14300 * t + 1.1)
            - noise
        )
        values = (left, right) if channels == 2 else ((left + right) * 0.5,)
        for value in values:
            samples.append(max(-32768, min(32767, round(value * 32767))))
    with wave.open(str(path), "wb") as out:
        out.setnchannels(channels)
        out.setsampwidth(2)
        out.setframerate(rate)
        out.writeframes(samples.tobytes())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out / "reference").mkdir(exist_ok=True)
    ffmpeg = Path(shutil.which("ffmpeg") or "ffmpeg")
    manifest = {
        "ffmpeg_sha256": digest(ffmpeg),
        "version": subprocess.check_output(
            [str(ffmpeg), "-version"], text=True
        ).splitlines()[0],
        "cases": [],
    }
    for profile, bitrate, channels, label in [
        (4, 48000, 2, "he-aac-48000-mixed"),
        (28, 32000, 2, "he-aac-v2-48000-mixed"),
        (4, 32000, 1, "he-aac-48000-mono"),
    ]:
        source = out / ("source-" + str(channels) + "ch.wav")
        generate_signal(source, channels)
        encoded = out / ("owned-" + label + ".m4a")
        reference = out / "reference" / (encoded.stem + "-reference.s16le")
        command = [
            str(ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(source),
            "-c:a",
            "aac_at",
            "-profile:a",
            str(profile),
            "-b:a",
            str(bitrate),
            "-ar",
            "48000",
            "-ac",
            str(channels),
            str(encoded),
        ]
        subprocess.run(command, check=True)
        # FFmpeg exposes mono HE-AAC as duplicate stereo; select the actual
        # coded channel explicitly instead of invoking a gain-changing downmix.
        pcm_command = [
            str(ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(encoded),
        ]
        if channels == 1:
            pcm_command += ["-af", "pan=mono|c0=c0"]
        pcm_command += ["-f", "s16le", "-acodec", "pcm_s16le", str(reference)]
        subprocess.run(pcm_command, check=True)
        raw = reference.read_bytes()
        values = struct.unpack("<" + "h" * (len(raw) // 2), raw)
        mono = [
            int(sum(values[i : i + channels]) / channels)
            for i in range(0, min(len(values), 12000 * channels), channels)
        ]
        (out / "reference" / (encoded.stem + "-mono-excerpt.s16le")).write_bytes(
            struct.pack("<" + "h" * len(mono), *mono)
        )
        manifest["cases"].append(
            {
                "name": encoded.name,
                "rate": 48000,
                "channels": channels,
                "profile": profile,
                "bitrate": bitrate,
                "source_sha256": digest(source),
                "encoded_sha256": digest(encoded),
                "reference_sha256": digest(reference),
                "command": command,
            }
        )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
