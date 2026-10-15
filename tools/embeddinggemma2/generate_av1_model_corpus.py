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

"""Remux pinned AV1 format fixtures for pretrained model qualification.

Reference tools are offline only. Every coded packet must survive the remux
unchanged; profiles/depth/chroma and the displayed frame count are probed.
"""

import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ivf_packets(encoded):
    if len(encoded) < 32 or encoded[:4] != b"DKIF":
        raise ValueError("invalid IVF header")
    header = struct.unpack_from("<H", encoded, 6)[0]
    if header != 32 or encoded[8:12] != b"AV01":
        raise ValueError("expected AV1 IVF")
    at = header
    result = []
    while at < len(encoded):
        if len(encoded) - at < 12:
            raise ValueError("truncated IVF packet header")
        length = struct.unpack_from("<I", encoded, at)[0]
        at += 12
        if not length or length > len(encoded) - at:
            raise ValueError("invalid IVF packet length")
        result.append(
            (struct.unpack_from("<Q", encoded, at - 8)[0], encoded[at : at + length])
        )
        at += length
    if not result:
        raise ValueError("empty IVF")
    return result


def element(ident, payload):
    width = max(1, (len(payload).bit_length() + 6) // 7)
    if len(payload) == (1 << (7 * width)) - 1:
        width += 1
    return (
        ident.to_bytes((ident.bit_length() + 7) // 8, "big")
        + ((1 << (7 * width)) | len(payload)).to_bytes(width, "big")
        + payload
    )


def uint(ident, value):
    return element(ident, value.to_bytes(max(1, (value.bit_length() + 7) // 8), "big"))


def webm(encoded):
    """Minimal deterministic WebM mux; retain every original temporal unit."""
    width, height, rate, scale = struct.unpack_from("<HHII", encoded, 12)
    packets = ivf_packets(encoded)
    header = element(
        0x1A45DFA3,
        uint(0x4286, 1)
        + uint(0x42F7, 1)
        + uint(0x42F2, 4)
        + uint(0x42F3, 8)
        + element(0x4282, b"webm")
        + uint(0x4287, 4)
        + uint(0x4285, 2),
    )
    info = element(
        0x1549A966,
        uint(0x2AD7B1, 1000000)
        + element(0x4489, struct.pack(">d", len(packets) * scale * 1000 / rate))
        + element(0x4D80, b"Antfly")
        + element(0x5741, b"Antfly"),
    )
    track = element(
        0x1654AE6B,
        element(
            0xAE,
            uint(0xD7, 1)
            + uint(0x73C5, 1)
            + uint(0x83, 1)
            + element(0x86, b"V_AV1")
            + uint(0x23E383, scale * 1000000000 // rate)
            + element(0xE0, uint(0xB0, width) + uint(0xBA, height)),
        ),
    )
    clusters = b"".join(
        element(
            0x1F43B675,
            uint(0xE7, timestamp * scale * 1000 // rate)
            + element(0xA3, b"\x81\x00\x00" + bytes([0x80 if i == 0 else 0]) + packet),
        )
        for i, (timestamp, packet) in enumerate(packets)
    )
    return header + element(0x18538067, info + track + clusters)


def generate(source, output, ffprobe, ffmpeg, include_scalable=False):
    output.mkdir(parents=True, exist_ok=True)
    golden = json.loads((source / "golden.json").read_text())
    if include_scalable:
        layers = json.loads((source / "layers.json").read_text())
        top = next(
            row
            for row in layers["cases"]
            if row["operating_point"] == 0 and not row["all_layers"]
        )
        golden.append(
            dict(
                name="av1-svc",
                framing="low_overhead",
                packet_sha256=layers["packet_sha256"],
                width=128,
                height=96,
                depth=8,
                chroma=1,
                frame_sha256=[frame["sha256"] for frame in top["frames"]],
            )
        )
    documents = []
    evidence = []
    for row in golden:
        if row["framing"] != "low_overhead":
            continue  # Annex B is an elementary API, not WebM model input.
        original = source / (row["name"] + ".ivf")
        if sha(original) != row["packet_sha256"]:
            raise ValueError("pinned source changed: " + row["name"])
        target = output / (row["name"] + ".webm")
        target.write_bytes(webm(original.read_bytes()))
        packets = json.loads(
            subprocess.check_output(
                [
                    ffprobe,
                    "-v",
                    "error",
                    "-show_packets",
                    "-show_entries",
                    "packet=data_hash",
                    "-show_data_hash",
                    "sha256",
                    "-of",
                    "json",
                    str(target),
                ],
                timeout=120,
            )
        )["packets"]
        original_packets = [
            hashlib.sha256(packet).hexdigest()
            for _, packet in ivf_packets(original.read_bytes())
        ]
        if [
            p["data_hash"].removeprefix("SHA256:") for p in packets
        ] != original_packets:
            raise ValueError("remux changed coded packets: " + row["name"])
        probe = json.loads(
            subprocess.check_output(
                [
                    ffprobe,
                    "-v",
                    "error",
                    "-count_frames",
                    "-show_streams",
                    "-show_format",
                    "-of",
                    "json",
                    str(target),
                ],
                timeout=120,
            )
        )
        stream = probe["streams"][0]
        expected_pixel = (
            ("gray" + (str(row["depth"]) + "le" if row["depth"] > 8 else ""))
            if row["chroma"] == 0
            else (
                "yuv"
                + {1: "420", 2: "422", 3: "444"}[row["chroma"]]
                + "p"
                + (str(row["depth"]) + "le" if row["depth"] > 8 else "")
            )
        )
        if (
            stream["codec_name"],
            stream["width"],
            stream["height"],
            stream["pix_fmt"],
            int(stream["nb_read_frames"]),
        ) != (
            "av1",
            row["width"],
            row["height"],
            expected_pixel,
            len(row["frame_sha256"]),
        ):
            raise ValueError("format/displayed count changed: " + row["name"])
        raw = subprocess.check_output(
            [
                ffmpeg,
                "-v",
                "error",
                "-i",
                str(target),
                "-f",
                "rawvideo",
                "-pix_fmt",
                expected_pixel,
                "-",
            ],
            timeout=120,
        )
        sx, sy = {0: (0, 0), 1: (1, 1), 2: (1, 0), 3: (0, 0)}[row["chroma"]]
        samples = row["width"] * row["height"]
        if row["chroma"]:
            samples += (
                2
                * ((row["width"] + (1 << sx) - 1) >> sx)
                * ((row["height"] + (1 << sy) - 1) >> sy)
            )
        frame_bytes = samples * (2 if row["depth"] > 8 else 1)
        hashes = [
            hashlib.sha256(raw[i : i + frame_bytes]).hexdigest()
            for i in range(0, len(raw), frame_bytes)
        ]
        if hashes != row["frame_sha256"]:
            raise ValueError("independent displayed pixels changed: " + row["name"])
        documents.append(
            {
                "id": row["name"],
                "path": target.name,
                "sha256": sha(target),
                "provenance": {
                    "title": "Antfly generated AV1 expanded-format pattern",
                    "license": "Apache-2.0",
                    "source_sha256": sha(original),
                    "source": str(original),
                    "generation": "zig/lib/video/scripts/generate_av1_extended_fixtures.py",
                    "muxer": "tools/embeddinggemma2/generate_av1_model_corpus.py (packet-preserving WebM)",
                },
            }
        )
        evidence.append(
            {
                "id": row["name"],
                "depth": row["depth"],
                "chroma": row["chroma"],
                "stream": stream,
                "duration": probe["format"]["duration"],
                "coded_packet_sha256": original_packets,
                "decoded_frame_sha256": hashes,
            }
        )
    corpus = {
        "version": 1,
        "purpose": "Expanded AV1 pretrained reference preservation; no relevance labels",
        "documents": documents,
        "queries": [
            {"id": "colour", "text": "A colourful pattern with moving shapes."},
            {"id": "gray", "text": "A grayscale pattern."},
        ],
    }
    (output / "corpus.json").write_text(json.dumps(corpus, indent=2) + "\n")
    (output / "formats.json").write_text(json.dumps(evidence, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=Path("zig/lib/video/testdata/av1-extended")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ffprobe", default="ffprobe")
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument(
        "--include-scalable",
        action="store_true",
        help="Include pinned two-spatial/three-temporal-layer AV1 at operating point zero",
    )
    args = parser.parse_args()
    generate(args.source, args.output, args.ffprobe, args.ffmpeg, args.include_scalable)


if __name__ == "__main__":
    main()
