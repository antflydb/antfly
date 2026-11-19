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

"""Compare an opt-in CUDA video capture with the upstream PyAV decode boundary."""

import argparse
import json
from pathlib import Path

import av
import numpy as np
from benchmark_gemma4 import digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    metadata = sorted(
        (p for p in args.capture.glob("*.json") if p.stem.isdecimal()),
        key=lambda p: int(p.stem),
    )
    if not metadata:
        raise ValueError("Empty CUDA capture")
    wanted = [json.loads(path.read_text()) for path in metadata]
    rows = []
    with av.open(str(args.input)) as container:
        frames = iter(container.decode(video=0))
        frame = next(frames)
        for path, info in zip(metadata, wanted, strict=True):
            timestamp = info["timestamp_us"]
            while float(frame.time) * 1e6 < timestamp - 1:
                frame = next(frames)
            if abs(float(frame.time) * 1e6 - timestamp) > 1:
                raise ValueError("CUDA/PyAV frame timestamp differs")
            if info["high_depth"] or info["tone"]:
                raise ValueError("This diagnostic compares SDR eight-bit NV12")
            width, height, pitch = info["width"], info["height"], info["pitch"]
            if (frame.width, frame.height) != (width, height):
                raise ValueError("CUDA/PyAV geometry differs")
            nv12_path, rgb_path = path.with_suffix(".nv12"), path.with_suffix(".rgb")
            nv12 = np.fromfile(nv12_path, dtype=np.uint8).reshape(
                height * 3 // 2, pitch
            )[:, :width]
            expected_nv12 = frame.to_ndarray(format="nv12")
            rgb = np.fromfile(rgb_path, dtype=np.uint8).reshape(height, width, 3)
            expected_rgb = frame.to_ndarray(format="rgb24")
            delta = rgb.astype(np.int16) - expected_rgb.astype(np.int16)
            rows.append(
                {
                    "frame": int(path.stem),
                    "metadata": info,
                    "capture_sha256": {
                        p.name: digest(p) for p in (path, nv12_path, rgb_path)
                    },
                    "nv12_equal": bool(np.array_equal(nv12, expected_nv12)),
                    "rgb_equal": bool(np.array_equal(rgb, expected_rgb)),
                    "rgb_mae": float(np.abs(delta).mean()),
                    "rgb_max_abs": int(np.abs(delta).max()),
                    "rgb_mean_signed": float(delta.mean()),
                    "rgb_mean_signed_channels": delta.mean(axis=(0, 1)).tolist(),
                }
            )
    args.out.write_text(
        json.dumps(
            {
                "scope": "raw decoder surfaces and RGB8 conversion before resize; no numerical tolerance or generation qualification inferred",
                "input_sha256": digest(args.input),
                "pyav_version": av.__version__,
                "libraries": av.library_versions,
                "rows": rows,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
