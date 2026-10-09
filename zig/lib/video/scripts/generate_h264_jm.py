#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify rare tools with official JM 19.0 encoder AND decoder, offline only.

Requires the public Sintel trailer and a locally built official JM tree. No JM
source or binary is vendored; production decoding remains pure Zig.
"""

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from generate_h264_advanced_vectors import DATA, mux_pcm

SOURCE_HASH = "b670602fa00934ca27c4351bb0efe7ea7a07fae57284e44226025eeed7c51254"


def invoke(jm, executable, parameters, log):
    command = [
        str(jm / "bin" / executable),
        "-d",
        str(
            jm
            / "bin"
            / ("encoder_main.cfg" if executable == "lencod.exe" else "decoder.cfg")
        ),
    ]
    for key, value in parameters.items():
        command += ["-p", f"{key}={value}"]
    with log.open("wb") as output:
        subprocess.run(
            command, cwd=jm, stdout=output, stderr=subprocess.STDOUT, check=True
        )


def mux(encoded, separate):
    nals = [n for n in re.split(b"\x00\x00\x00?\x01", encoded) if n]
    sps = next(n for n in nals if n[0] & 31 == 7)
    pps = next(n for n in nals if n[0] & 31 == 8)
    if separate:
        vcl = [n for n in nals if n[0] & 31 in (1, 5)]
        assert len(vcl) == 12
        parts = [vcl[i : i + 3] for i in range(0, len(vcl), 3)]
    else:
        parts = []
        for n in nals:
            if n[0] & 31 in (1, 2, 5):
                parts.append([])
            if n[0] & 31 in (1, 2, 3, 4, 5):
                parts[-1].append(n)
    parts[0] = [sps, pps] + parts[0]
    return mux_pcm([b"".join(b"\x00\x00\x00\x01" + n for n in part) for part in parts])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jm-root", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    args = parser.parse_args()
    jm, source = args.jm_root.resolve(), args.source.resolve()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == SOURCE_HASH
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for chroma in (420, 444):
            subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-ss",
                    "12",
                    "-i",
                    str(source),
                    "-frames:v",
                    "4",
                    "-vf",
                    "scale=64:48",
                    "-an",
                    "-pix_fmt",
                    f"yuv{chroma}p",
                    "-f",
                    "rawvideo",
                    str(root / f"source-{chroma}.yuv"),
                ],
                check=True,
            )
        source420 = (root / "source-420.yuv").read_bytes()
        (root / "source-0.yuv").write_bytes(
            b"".join(
                source420[i : i + 64 * 48]
                for i in range(0, len(source420), 64 * 48 * 3 // 2)
            )
        )
        common = dict(
            SourceWidth=64,
            SourceHeight=48,
            FramesToBeEncoded=4,
            NumberBFrames=0,
            NumberReferenceFrames=2,
            SymbolMode=0,
            ProfileIDC=88,
        )
        cases = [
            ("partitions", dict(PartitionMode=1), 8, False),
            ("mono-8", dict(ProfileIDC=100, YUVFormat=0, SymbolMode=1), 8, False),
            (
                "mono-10",
                dict(
                    ProfileIDC=110,
                    YUVFormat=0,
                    SourceBitDepthRescale=1,
                    OutputBitDepthLuma=10,
                    OutputBitDepthChroma=10,
                ),
                10,
                False,
            ),
            (
                "separate",
                dict(ProfileIDC=244, YUVFormat=3, SeparateColourPlane=1, SymbolMode=1),
                8,
                True,
            ),
            (
                "separate-10",
                dict(
                    ProfileIDC=244,
                    YUVFormat=3,
                    SeparateColourPlane=1,
                    SourceBitDepthRescale=1,
                    OutputBitDepthLuma=10,
                    OutputBitDepthChroma=10,
                ),
                10,
                True,
            ),
            (
                "separate-14",
                dict(
                    ProfileIDC=244,
                    YUVFormat=3,
                    SeparateColourPlane=1,
                    SourceBitDepthRescale=1,
                    OutputBitDepthLuma=14,
                    OutputBitDepthChroma=14,
                ),
                14,
                True,
            ),
            (
                "primary-sp",
                dict(
                    SPPicturePeriodicity=1,
                    SP_output=1,
                    SP_output_name=root / "primary.dat",
                ),
                8,
                False,
            ),
            (
                "secondary-sp",
                dict(
                    SPPicturePeriodicity=1,
                    SP2_FRAMES=1,
                    SP2_input_name1=root / "primary.dat",
                    SP2_input_name2=root / "primary.dat",
                ),
                8,
                False,
            ),
            ("si", dict(SPPicturePeriodicity=1, SI_FRAMES=1), 8, False),
        ]
        for name, policy, depth, separate in cases:
            stream, reconstructed, decoded = (
                root / (name + ".264"),
                root / (name + "-encoder.yuv"),
                root / (name + "-decoder.yuv"),
            )
            parameters = dict(
                common,
                InputFile=root
                / f"source-{0 if policy.get('YUVFormat') == 0 else 444 if separate else 420}.yuv",
                OutputFile=stream,
                ReconFile=reconstructed,
                StatsFile=root / (name + ".stats"),
            )
            parameters.update(policy)
            invoke(jm, "lencod.exe", parameters, root / (name + "-encode.log"))
            invoke(
                jm,
                "ldecod.exe",
                dict(InputFile=stream, OutputFile=decoded, Silent=1),
                root / (name + "-decode.log"),
            )
            payload = mux(stream.read_bytes(), separate)
            stem = "h264-jm-" + name
            (DATA / (stem + ".mp4")).write_bytes(payload)
            word = 1 if depth == 8 else 2
            y, c = (
                64 * 48 * word,
                (
                    0
                    if policy.get("YUVFormat") == 0
                    else 64 * 48
                    if separate
                    else 64 * 48 // 4
                )
                * word,
            )
            raw = decoded.read_bytes()
            size = y * 3 // 2 if policy.get("YUVFormat") == 0 else y + 2 * c
            assert len(raw) == 4 * size, (name, len(raw), size)
            frames = []
            for i in range(4):
                block = raw[i * size : (i + 1) * size]
                uv = bytearray(2 * c)
                for n in range(c // word):
                    uv[n * 2 * word : (n * 2 + 1) * word] = block[
                        y + n * word : y + (n + 1) * word
                    ]
                    uv[(n * 2 + 1) * word : (n * 2 + 2) * word] = block[
                        y + c + n * word : y + c + (n + 1) * word
                    ]
                frames.append(hashlib.sha256(block[:y] + uv).hexdigest())
            receipt = dict(
                oracle="official JM 19.0 decoder; encoder output is not the oracle",
                source_sha256=SOURCE_HASH,
                source_clock_seconds=12,
                mp4_sha256=hashlib.sha256(payload).hexdigest(),
                elementary_sha256=hashlib.sha256(stream.read_bytes()).hexdigest(),
                bit_depth=depth,
                chroma_format=0
                if policy.get("YUVFormat") == 0
                else 3
                if separate
                else 1,
                frames=frames,
            )
            (DATA / (stem + "-oracle.json")).write_text(
                json.dumps(receipt, indent=2) + "\n"
            )


if __name__ == "__main__":
    main()
