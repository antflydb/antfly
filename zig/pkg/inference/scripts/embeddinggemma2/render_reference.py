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

"""Independent float64 rendering oracle for the version-3 inference policy.

BT.2100 PQ or HLG (1000 nit, gamma 1.2), D65 BT.2020 to BT.709,
fixed 100-nit exposure, luminance Reinhard L/(1+L), sRGB, RGB8.
This policy intentionally ignores dynamic/mastering display metadata.
"""

import math
from pathlib import Path
import struct


def render(rgb, transfer, wide=True):
    rgb = [min(1.0, max(0.0, c)) for c in rgb]
    if transfer == "pq":
        linear = []
        for n in rgb:
            v = n ** (32 / 2523)
            linear.append(
                100
                * (max(v - 3424 / 4096, 0) / (2413 / 128 - 2392 / 128 * v))
                ** (16384 / 2610)
            )
    elif transfer == "hlg":
        linear = [
            n * n / 3
            if n <= 0.5
            else (math.exp((n - 0.55991073) / 0.17883277) + 0.28466892) / 12
            for n in rgb
        ]
        weights = (0.2627, 0.6780, 0.0593) if wide else (0.2126, 0.7152, 0.0722)
        luminance = sum(c * w for c, w in zip(linear, weights))
        linear = [c * 10 * max(luminance, 0) ** 0.2 for c in linear]
    else:
        raise ValueError("unknown transfer")
    if wide:
        matrix = (
            (1.660491, -0.587641, -0.072850),
            (-0.124550, 1.132900, -0.008350),
            (-0.018151, -0.100579, 1.118730),
        )
        linear = [sum(a * b for a, b in zip(row, linear)) for row in matrix]
    luminance = max(sum(a * b for a, b in zip((0.2126, 0.7152, 0.0722), linear)), 0)
    linear = [min(1.0, max(0.0, c / (1 + luminance))) for c in linear]
    return bytes(
        math.floor(
            255 * (12.92 * c if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055)
            + 0.5
        )
        for c in linear
    )


def yuv_rgb(y, u, v, depth=10, full=False, wide=True):
    scale = 2 ** (depth - 8)
    y, u, v = y / scale, u / scale - 128, v / scale - 128
    if not full:
        y, u, v = (y - 16) * 255 / 219, u * 255 / 224, v * 255 / 224
    else:
        factor = 255 * scale / (2**depth - 1)
        y, u, v = y * factor, u * factor, v * factor
    coefficients = (
        (1.4746, 0.164553, 0.571353, 1.8814)
        if wide
        else (1.5748, 0.187324, 0.468124, 1.8556)
    )
    return [
        (y + coefficients[0] * v) / 255,
        (y - coefficients[1] * u - coefficients[2] * v) / 255,
        (y + coefficients[3] * u) / 255,
    ]


def fixtures(directory):
    directory.mkdir(parents=True, exist_ok=True)
    width = height = 48
    y = [((x * 71 + row * 37) % 1024) for row in range(height) for x in range(width)]
    uv = [
        (256 + (x * 29 + row * 17) % 512, 256 + (x * 13 + row * 47) % 512)
        for row in range(height // 2)
        for x in range(width // 2)
    ]
    samples = y + [c for pair in uv for c in pair]
    (directory / "hdr-10bit-nv12.bin").write_bytes(
        struct.pack("<" + "H" * len(samples), *samples)
    )
    for transfer in ("pq", "hlg"):
        for wide in (False, True):
            for full in (False, True):
                rgba = bytearray()
                for row in range(height):
                    for x in range(width):
                        u, v = uv[(row // 2) * (width // 2) + x // 2]
                        rgba.extend(
                            render(
                                yuv_rgb(y[row * width + x], u, v, full=full, wide=wide),
                                transfer,
                                wide,
                            )
                        )
                        rgba.append(255)
                name = f"hdr-{transfer}-{'2020' if wide else '709'}-{'full' if full else 'limited'}.rgba"
                (directory / name).write_bytes(rgba)


if __name__ == "__main__":
    fixtures(Path(__file__).resolve().parents[5] / "zig/lib/video/testdata")
