#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reencode the pinned full Sintel excerpt as AV1 4:4:4 for qualification.

Offline FFmpeg is a reference tool, never an inference runtime dependency.
The source is Blender Foundation's CC BY 3.0 trailer, previously transcoded to
4:2:0. This derivative does not add source chroma detail.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import shutil

SOURCE_SHA256 = "db6917f3a9d42ed3fce12384711d31f84ffd1aef682a2ea8f04d7815497ec187"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate(source, output, ffmpeg, aomenc, ffprobe):
    if sha(source) != SOURCE_SHA256:
        raise ValueError("pinned source changed")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "sintel-av1-444-480p.webm"
    raw_command = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(source),
        "-map",
        "0:v:0",
        "-an",
        "-pix_fmt",
        "yuv444p",
        "-f",
        "yuv4mpegpipe",
        "-",
    ]
    command = [
        aomenc,
        "--quiet",
        "--debug",
        "--webm",
        "--passes=1",
        "--profile=1",
        "--cpu-used=8",
        "--threads=2",
        "--row-mt=1",
        "--end-usage=q",
        "--cq-level=38",
        "--kf-max-dist=48",
        "--bit-depth=8",
        "--matrix-coefficients=6",
        "--color-primaries=6",
        "--transfer-characteristics=6",
        "-o",
        str(target),
        "-",
    ]
    with subprocess.Popen(raw_command, stdout=subprocess.PIPE) as raw_process:
        try:
            with subprocess.Popen(command, stdin=raw_process.stdout) as encoder:
                raw_process.stdout.close()
                try:
                    if encoder.wait(timeout=1800) != 0:
                        raise ValueError("reference encode failed")
                except BaseException:
                    encoder.kill()
                    encoder.wait()
                    raise
            if raw_process.wait(timeout=120) != 0:
                raise ValueError("source decode failed")
        finally:
            if raw_process.poll() is None:
                raw_process.kill()
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
    if (
        stream["codec_name"],
        stream["profile"],
        stream["width"],
        stream["height"],
        stream["pix_fmt"],
        int(stream["nb_read_frames"]),
    ) != ("av1", "High", 854, 480, "yuv444p", 1253):
        raise ValueError("coded profile/geometry/displayed count changed")
    width, height = 854, 480
    count = width * height
    decoder = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(target),
        "-map",
        "0:v:0",
        "-fps_mode",
        "passthrough",
        "-pix_fmt",
        "yuv444p",
        "-f",
        "rawvideo",
        "-",
    ]
    hashes = []
    with subprocess.Popen(decoder, stdout=subprocess.PIPE) as process:
        while True:
            frame = process.stdout.read(count * 3)
            if not frame:
                break
            if len(frame) != count * 3:
                raise ValueError("truncated reference picture")
            uv = bytearray(count * 2)
            uv[::2] = frame[count : count * 2]
            uv[1::2] = frame[count * 2 :]
            hashes.append(hashlib.sha256(frame[:count] + uv).hexdigest())
        if process.wait(timeout=120) != 0:
            raise ValueError("reference decoder failed")
    if len(hashes) != 1253:
        raise ValueError("full excerpt displayed count changed")
    manifest = {
        "version": 1,
        "source": {
            "path": str(source),
            "sha256": SOURCE_SHA256,
            "copyright": "Blender Foundation / durian.blender.org",
            "license": "CC BY 3.0",
            "url": "https://download.blender.org/durian/trailer/sintel_trailer-480p.mp4",
            "license_url": "https://creativecommons.org/licenses/by/3.0/",
        },
        "encoder": subprocess.check_output([ffmpeg, "-version"], text=True),
        "aomenc_sha256": sha(Path(shutil.which(aomenc) or aomenc)),
        "probe": probe,
        "streams": [
            {
                "path": target.name,
                "sha256": sha(target),
                "width": width,
                "height": height,
                "depth": 8,
                "chroma": 3,
                "frames": len(hashes),
                "picture_sha256": hashes,
                "command": command,
                "raw_command": raw_command,
                "oracle": decoder,
                "modifications": "52-second 4:4:4 AV1 derivative of a pinned 4:2:0 SDR source; no added chroma detail",
            }
        ],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(
            "zig/lib/video/testdata/benchmarks/webm/corpus/sintel-av1-480p.webm"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--aomenc", required=True)
    parser.add_argument("--ffprobe", default="ffprobe")
    args = parser.parse_args()
    generate(args.source, args.output, args.ffmpeg, args.aomenc, args.ffprobe)


if __name__ == "__main__":
    main()
