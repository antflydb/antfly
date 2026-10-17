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

"""Offline AV1 profile fixtures using pinned AOM encoder/independent decoder."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import struct
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate(ffmpeg, aomenc, aomdec, svc_encoder, output):
    output.mkdir(parents=True, exist_ok=True)
    cases = [
        (0, 8),
        (0, 12),
        (1, 12),
        (2, 8),
        (2, 10),
        (2, 12),
        (3, 8),
        (3, 10),
        (3, 12),
    ]
    goldens = []
    recipes = []
    width, height, count = 127, 95, 12
    with tempfile.TemporaryDirectory(prefix="antfly-av1-extended-") as temporary:
        for chroma, depth, annex_b in [(c, d, False) for c, d in cases] + [
            (1, 8, True)
        ]:
            name = "av1-annex-b" if annex_b else f"av1-c{chroma}-{depth}"
            profile = 2 if depth == 12 or chroma == 2 else 1 if chroma == 3 else 0
            pixel = (
                "yuv"
                + {0: "420", 1: "420", 2: "422", 3: "444"}[chroma]
                + "p"
                + (f"{depth}le" if depth > 8 else "")
            )
            raw = Path(temporary) / (name + ".yuv")
            source = [
                ffmpeg,
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"testsrc=size={width}x{height}:rate=6",
                "-frames:v",
                str(count),
                "-pix_fmt",
                pixel,
                "-f",
                "rawvideo",
                "-y",
                str(raw),
            ]
            subprocess.run(source, check=True)
            packet = output / (name + ".ivf")
            # AOM 3.13.1 tries to configure subsampling before initializing
            # its encoder for explicit 12-bit non-420 profiles. Automatic
            # profile promotion uses the actual input layout and avoids this
            # CLI initialization-order bug without patching reference code.
            encode = [
                aomenc,
                "--ivf",
                "--passes=1",
                "--cpu-used=6",
                "--threads=1",
                "--lag-in-frames=4",
                f"--limit={count}",
                f"--width={width}",
                f"--height={height}",
                "--fps=6/1",
                f"--profile={0 if depth == 12 and chroma >= 2 else profile}",
                f"--input-chroma-subsampling-x={0 if chroma == 3 else 1}",
                f"--input-chroma-subsampling-y={1 if chroma < 2 else 0}",
                f"--bit-depth={depth}",
                f"--input-bit-depth={depth}",
                "--i" + {0: "420", 1: "420", 2: "422", 3: "444"}[chroma],
                "--end-usage=q",
                "--cq-level=30",
                "--kf-max-dist=6",
            ]
            if annex_b:
                encode += ["--annexb=1"]
            if chroma == 0:
                encode += ["--monochrome"]
            encode += ["-o", str(packet), str(raw)]
            result = subprocess.run(
                encode, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
            if result.returncode != 0:
                raise RuntimeError(name + ": " + result.stderr.decode())
            reference = Path(temporary) / (name + "-decoded.yuv")
            decode = [
                aomdec,
                "--rawvideo",
                f"--output-bit-depth={depth}",
                "-o",
                str(reference),
                str(packet),
            ]
            if annex_b:
                decode += ["--annexb"]
            subprocess.run(decode, check=True)
            cw = width if chroma == 3 else (width + 1) // 2
            ch = height if chroma >= 2 else (height + 1) // 2
            size = (width * height + (0 if chroma == 0 else 2 * cw * ch)) * (
                1 if depth == 8 else 2
            )
            data = reference.read_bytes()
            if len(data) != count * size:
                raise RuntimeError(
                    f"unexpected reference geometry/count for {name}: {len(data)} != {count * size}"
                )
            goldens.append(
                dict(
                    name=name,
                    codec="av1",
                    framing="annex_b" if annex_b else "low_overhead",
                    width=width,
                    height=height,
                    depth=depth,
                    chroma=chroma,
                    packet_sha256=sha(packet),
                    frame_sha256=[
                        hashlib.sha256(data[i : i + size]).hexdigest()
                        for i in range(0, len(data), size)
                    ],
                )
            )
            recipes.append(dict(name=name, source=source, encode=encode, decode=decode))
        raw = Path(temporary) / "svc.yuv"
        source = [
            ffmpeg,
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=6",
            "-frames:v",
            "12",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rawvideo",
            "-y",
            str(raw),
        ]
        subprocess.run(source, check=True)
        packet = output / "av1-svc.ivf"
        encode = [
            svc_encoder,
            "--width=128",
            "--height=96",
            "--timebase=1/6",
            "--spatial-layers=2",
            "--temporal-layers=3",
            "--scale-factors=1/2,1/1",
            "--bitrates=30,50,80,100,150,240",
            "--layering-mode=7",
            "--threads=1",
            "--speed=8",
            "--kf-dist=6",
            "--target-bitrate=320",
            "--test-decode=1",
            "--output=" + str(packet),
            str(raw),
        ]
        subprocess.run(encode, check=True, stdout=subprocess.DEVNULL)
        # Group the encoder's per-spatial-frame IVF packets into complete
        # temporal units. Layer-output policy applies to a whole temporal unit.
        original = packet.read_bytes()
        units = []
        at = 32
        while at < len(original):
            size, timestamp = struct.unpack_from("<IQ", original, at)
            at += 12
            data = original[at : at + size]
            at += size
            if units and units[-1][0] == timestamp:
                units[-1][1].extend(data)
            else:
                units.append((timestamp, bytearray(data)))
        header = bytearray(original[:32])
        struct.pack_into("<I", header, 24, len(units))
        packet.write_bytes(
            header
            + b"".join(
                struct.pack("<IQ", len(data), timestamp) + data
                for timestamp, data in units
            )
        )
        # AOM's example additionally writes independently extracted layer files.
        for extra in output.glob("av1-svc.ivf_*.av1"):
            extra.unlink()
        layer_goldens = []
        for point, all_layers in [(p, False) for p in range(6)] + [
            (0, True),
            (3, True),
        ]:
            decoded = Path(temporary) / f"svc-{point}-{all_layers}-%4-%w-%h.yuv"
            decode = [
                aomdec,
                "--i420",
                f"--oppoint={point}",
                "-o",
                str(decoded),
                str(packet),
            ]
            if all_layers:
                decode += ["--all-layers"]
            subprocess.run(decode, check=True)
            pictures = []
            ordered_bytes = bytearray()
            frame_files = Path(temporary).glob(f"svc-{point}-{all_layers}-*.yuv")

            def picture_key(path):
                parts = path.stem.rsplit("-", 3)
                return int(parts[-3]), int(parts[-2]), int(parts[-1])

            for picture in sorted(frame_files, key=picture_key):
                dimensions = picture.stem.rsplit("-", 2)
                w, h = int(dimensions[-2]), int(dimensions[-1])
                data = picture.read_bytes()
                if len(data) != w * h * 3 // 2:
                    raise RuntimeError("SVC reference picture size mismatch")
                ordered_bytes.extend(data)
                pictures.append(
                    dict(width=w, height=h, sha256=hashlib.sha256(data).hexdigest())
                )
            raw_output = Path(temporary) / f"ordered-{point}-{all_layers}.yuv"
            raw_decode = [
                aomdec,
                "--rawvideo",
                f"--oppoint={point}",
                "-o",
                str(raw_output),
                str(packet),
            ]
            if all_layers:
                raw_decode += ["--all-layers"]
            subprocess.run(raw_decode, check=True)
            if raw_output.read_bytes() != ordered_bytes:
                raise RuntimeError(
                    "SVC picture metadata disagrees with raw output ordering"
                )
            recipes.append(
                dict(name=f"svc-ordered-{point}-{all_layers}", decode=raw_decode)
            )
            if not pictures:
                raise RuntimeError("SVC reference produced no pictures")
            layer_goldens.append(
                dict(operating_point=point, all_layers=all_layers, frames=pictures)
            )
            recipes.append(dict(name=f"svc-{point}-{all_layers}", decode=decode))
        recipes.append(dict(name="av1-svc", source=source, encode=encode))
        (output / "layers.json").write_text(
            json.dumps(dict(packet_sha256=sha(packet), cases=layer_goldens), indent=2)
            + "\n"
        )
    (output / "golden.json").write_text(json.dumps(goldens, indent=2) + "\n")
    (output / "provenance.json").write_text(
        json.dumps(
            dict(
                aom_version="3.13.1",
                aom_commit="d772e334cc724105040382a977ebb10dfd393293",
                source_url="https://aomedia.googlesource.com/aom/+/refs/tags/v3.13.1",
                tool_sha256={
                    "aomenc": sha(Path(aomenc)),
                    "aomdec": sha(Path(aomdec)),
                    "ffmpeg": sha(Path(ffmpeg)),
                    "svc_encoder": sha(Path(svc_encoder)),
                },
                recipes=recipes,
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ffmpeg", required=True)
    parser.add_argument("--aomenc", required=True)
    parser.add_argument("--aomdec", required=True)
    parser.add_argument("--svc-encoder", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.ffmpeg, args.aomenc, args.aomdec, args.svc_encoder, args.output)
