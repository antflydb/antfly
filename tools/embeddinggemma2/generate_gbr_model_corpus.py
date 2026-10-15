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

"""Generate identity-matrix AV1/VP9 clips and independent decoded-plane receipts."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ffmpeg", default="ffmpeg")
    p.add_argument("--ffprobe", default="ffprobe")
    p.add_argument("--aomenc", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    documents, cases = [], []
    with tempfile.TemporaryDirectory(prefix="antfly-gbr-") as temporary:
        work = Path(temporary)
        for width, height in [(127, 95), (128, 96)]:
            for depth in [8, 10, 12]:
                pixel = "gbrp" + (f"{depth}le" if depth > 8 else "")
                raw = work / "source.raw"
                source = [
                    args.ffmpeg,
                    "-v",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    f"testsrc=size={width}x{height}:rate=6",
                    "-frames:v",
                    "12",
                    "-pix_fmt",
                    pixel,
                    "-f",
                    "rawvideo",
                    "-y",
                    str(raw),
                ]
                subprocess.run(source, check=True, timeout=120)
                for codec in ["av1", "vp9"]:
                    name = f"{codec}-gbr-{depth}-{width}"
                    target = args.output / (name + ".webm")
                    commands = [source]
                    if codec == "av1":
                        encoded = work / "encoded.ivf"
                        encode = [
                            args.aomenc,
                            "--ivf",
                            "--passes=1",
                            "--i444",
                            f"--width={width}",
                            f"--height={height}",
                            "--fps=6/1",
                            "--limit=12",
                            "--threads=1",
                            "--cpu-used=8",
                            "--end-usage=q",
                            "--cq-level=30",
                            f"--profile={0 if depth == 12 else 1}",
                            "--input-chroma-subsampling-x=0",
                            "--input-chroma-subsampling-y=0",
                            f"--bit-depth={depth}",
                            f"--input-bit-depth={depth}",
                            "--color-primaries=bt709",
                            "--transfer-characteristics=srgb",
                            "--matrix-coefficients=identity",
                            "-o",
                            str(encoded),
                            str(raw),
                        ]
                        result = subprocess.run(
                            encode,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE,
                            timeout=120,
                        )
                        if result.returncode:
                            raise RuntimeError(name + ": " + result.stderr.decode())
                        mux = [
                            args.ffmpeg,
                            "-v",
                            "error",
                            "-i",
                            str(encoded),
                            "-c:v",
                            "copy",
                            "-fflags",
                            "+bitexact",
                            "-flags:v",
                            "+bitexact",
                            "-y",
                            str(target),
                        ]
                        subprocess.run(mux, check=True, timeout=120)
                        commands += [encode, mux]
                    else:
                        encode = [
                            args.ffmpeg,
                            "-v",
                            "error",
                            "-f",
                            "rawvideo",
                            "-pix_fmt",
                            pixel,
                            "-s",
                            f"{width}x{height}",
                            "-r",
                            "6",
                            "-color_primaries",
                            "bt709",
                            "-color_trc",
                            "iec61966-2-1",
                            "-colorspace",
                            "rgb",
                            "-color_range",
                            "pc",
                            "-i",
                            str(raw),
                            "-c:v",
                            "libvpx-vp9",
                            "-deadline",
                            "realtime",
                            "-cpu-used",
                            "8",
                            "-threads",
                            "1",
                            "-crf",
                            "30",
                            "-b:v",
                            "0",
                            "-fflags",
                            "+bitexact",
                            "-flags:v",
                            "+bitexact",
                            "-y",
                            str(target),
                        ]
                        subprocess.run(encode, check=True, timeout=120)
                        commands += [encode]
                    stream = json.loads(
                        subprocess.check_output(
                            [
                                args.ffprobe,
                                "-v",
                                "error",
                                "-count_frames",
                                "-show_streams",
                                "-of",
                                "json",
                                str(target),
                            ],
                            timeout=120,
                        )
                    )["streams"][0]
                    if (
                        stream["pix_fmt"],
                        stream["color_space"],
                        stream["color_range"],
                        int(stream["nb_read_frames"]),
                    ) != (pixel, "gbr", "pc", 12):
                        raise ValueError("encoded identity profile changed")
                    decoded = subprocess.check_output(
                        [
                            args.ffmpeg,
                            "-v",
                            "error",
                            "-i",
                            str(target),
                            "-f",
                            "rawvideo",
                            "-pix_fmt",
                            pixel,
                            "-",
                        ],
                        timeout=120,
                    )
                    frame_bytes = width * height * 3 * (1 if depth == 8 else 2)
                    if len(decoded) != frame_bytes * 12:
                        raise ValueError("decoded plane geometry changed")
                    hashes = [
                        hashlib.sha256(decoded[i : i + frame_bytes]).hexdigest()
                        for i in range(0, len(decoded), frame_bytes)
                    ]
                    documents.append(
                        dict(
                            id=name,
                            path=target.name,
                            sha256=sha(target),
                            provenance=dict(
                                title="Antfly generated identity-matrix RGB pattern",
                                license="Apache-2.0",
                                source_sha256=sha(raw),
                                generation="tools/embeddinggemma2/generate_gbr_model_corpus.py",
                            ),
                        )
                    )
                    cases.append(
                        dict(
                            id=name,
                            width=width,
                            height=height,
                            depth=depth,
                            chroma=3,
                            stream=stream,
                            source_sha256=sha(raw),
                            decoded_frame_sha256=hashes,
                            commands=[
                                [
                                    word.replace(str(work), "$WORK").replace(
                                        str(args.output), "$OUTPUT"
                                    )
                                    for word in command
                                ]
                                for command in commands
                            ],
                        )
                    )
    (args.output / "corpus.json").write_text(
        json.dumps(
            dict(
                version=1,
                purpose="Identity-matrix reference preservation; no relevance labels",
                documents=documents,
                queries=[
                    dict(id="colour", text="A colourful pattern with moving shapes."),
                    dict(id="gray", text="A grayscale pattern."),
                ],
            ),
            indent=2,
        )
        + "\n"
    )
    (args.output / "formats.json").write_text(
        json.dumps(
            dict(
                ffmpeg=subprocess.check_output([args.ffmpeg, "-version"], text=True),
                aomenc_sha256=sha(Path(args.aomenc)),
                cases=cases,
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
