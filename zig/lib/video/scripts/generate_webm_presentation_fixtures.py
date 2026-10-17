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

"""Add presentation declarations without re-encoding synthetic WebM fixtures.

SeekHead/Cues are removed because their segment-relative byte positions change.
Compressed Cluster payloads are preserved byte for byte. No codec dependency.
"""

import hashlib
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[4]
OUTPUT = ROOT / "zig/pkg/inference/src/testdata/video/presentation"
CASES = {
    "vp9-crop-roll": ("vp9", {"crop": [1, 3, 5, 7], "display": [16, 9], "roll": 90}),
    "av1-crop-roll": ("av1", {"crop": [1, 3, 5, 7], "display": [16, 9], "roll": -90}),
    "vp8-colour": (
        "vp8",
        {"colour": {"matrix": 1, "transfer": 1, "primaries": 1, "range": 2}},
    ),
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def elements(data):
    pos = 0
    while pos < len(data):
        first = data[pos]
        if first == 0:
            raise ValueError("zero element ID")
        n = 9 - first.bit_length()
        if n > 4:
            raise ValueError("oversized ID")
        ident = int.from_bytes(data[pos : pos + n], "big")
        pos += n
        first = data[pos]
        if first == 0:
            raise ValueError("zero size prefix")
        n = 9 - first.bit_length()
        value = int.from_bytes(data[pos : pos + n], "big") & ((1 << (7 * n)) - 1)
        pos += n
        end = len(data) if value == (1 << (7 * n)) - 1 else pos + value
        if end > len(data):
            raise ValueError("element outside parent")
        yield ident, data[pos:end]
        pos = end


def element(ident, data):
    n = max(1, (len(data).bit_length() + 6) // 7)
    if len(data) == (1 << (7 * n)) - 1:
        n += 1
    return (
        ident.to_bytes((ident.bit_length() + 7) // 8, "big")
        + ((1 << (7 * n)) | len(data)).to_bytes(n, "big")
        + data
    )


def uint(ident, value):
    return element(ident, value.to_bytes(max(1, (value.bit_length() + 7) // 8), "big"))


def rewrite_video(data, policy):
    replacement = {}
    if "crop" in policy:
        for ident, value in zip((0x54CC, 0x54BB, 0x54DD, 0x54AA), policy["crop"]):
            replacement[ident] = uint(ident, value)
        replacement[0x54B0] = uint(0x54B0, policy["display"][0])
        replacement[0x54BA] = uint(0x54BA, policy["display"][1])
        replacement[0x54B2] = uint(0x54B2, 3)
        replacement[0x7670] = element(
            0x7670, element(0x7675, struct.pack(">d", policy["roll"]))
        )
    if "colour" in policy:
        replacement[0x55B0] = element(
            0x55B0,
            b"".join(
                uint(ident, policy["colour"][key])
                for ident, key in (
                    (0x55B1, "matrix"),
                    (0x55BA, "transfer"),
                    (0x55BB, "primaries"),
                    (0x55B9, "range"),
                )
            ),
        )
    return b"".join(
        element(i, v) for i, v in elements(data) if i not in replacement
    ) + b"".join(replacement.values())


def rewrite(data, policy):
    def descend(payload, level):
        result = []
        for ident, value in elements(payload):
            if level == 0 and ident == 0x18538067:
                value = descend(value, 1)
            elif level == 1 and ident in (0x114D9B74, 0x1C53BB6B):
                continue
            elif (level, ident) in ((1, 0x1654AE6B), (2, 0xAE)):
                value = descend(value, level + 1)
            elif level == 3 and ident == 0xE0:
                value = rewrite_video(value, policy)
            result.append(element(ident, value))
        return b"".join(result)

    return descend(data, 0)


def clusters(data):
    segment = next(v for i, v in elements(data) if i == 0x18538067)
    return [digest(v) for i, v in elements(segment) if i == 0x1F43B675]


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    cases = {}
    for name, (codec, policy) in CASES.items():
        source = OUTPUT.parent / (codec + ".webm")
        data = source.read_bytes()
        encoded = rewrite(data, policy)
        if not clusters(data) or clusters(data) != clusters(encoded):
            raise ValueError("compressed payload changed")
        target = OUTPUT / (name + ".webm")
        target.write_bytes(encoded)
        cases[name] = {
            "source": str(source.relative_to(ROOT)),
            "source_sha256": digest(data),
            "path": target.name,
            "sha256": digest(encoded),
            "policy": policy,
            "cluster_sha256": clusters(encoded),
        }
    (OUTPUT / "manifest.json").write_text(
        json.dumps(
            {
                "generator_sha256": digest(Path(__file__).read_bytes()),
                "scope": __doc__,
                "cases": cases,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
