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

"""Derive hev1 regression clips from the maintained HEVC fixtures using FFmpeg.

Move all parameter sets into sample zero, then leave hvcC empty or VPS-only.
Rebuild ancestor box lengths and chunk offsets rather than padding invalid hvcC.
"""

import struct
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "src/testdata/video/codecs"
U32 = struct.Struct(">I")
CONTAINERS = {
    b"moov": 0,
    b"trak": 0,
    b"mdia": 0,
    b"minf": 0,
    b"stbl": 0,
    b"stsd": 8,
    b"hev1": 78,
    b"hvc1": 78,
}


def boxes(data, start=0, end=None, parents=()):
    end = len(data) if end is None else end
    at = start
    while at < end:
        size = U32.unpack_from(data, at)[0]
        assert 8 <= size <= end - at, (at, size, end)
        kind = bytes(data[at + 4 : at + 8])
        yield at, size, kind, parents
        if kind in CONTAINERS:
            yield from boxes(data, at + 8 + CONTAINERS[kind], at + size, (*parents, at))
        at += size
    assert at == end


def convert(source, target, keep_vps):
    with tempfile.TemporaryDirectory(dir=ROOT) as directory:
        encoded = Path(directory) / "inband.mp4"
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-y",
                "-i",
                str(ROOT / source),
                "-map",
                "0:v:0",
                "-c:v",
                "copy",
                "-tag:v",
                "hev1",
                "-map_metadata",
                "-1",
                "-fflags",
                "+bitexact",
                "-movflags",
                "+faststart",
                str(encoded),
            ],
            check=True,
        )
        data = bytearray(encoded.read_bytes())
    configs = [box for box in boxes(data) if box[2] == b"hvcC"]
    assert len(configs) == 1
    at, size, _, parents = configs[0]
    config = data[at + 8 : at + size]
    assert config[21] & 3 == 3, "fixtures use four-byte NAL lengths"
    arrays = []
    headers = bytearray()
    cursor = 23
    for _ in range(config[22]):
        first = cursor
        kind = config[cursor] & 63
        count = int.from_bytes(config[cursor + 1 : cursor + 3], "big")
        cursor += 3
        for _ in range(count):
            length = int.from_bytes(config[cursor : cursor + 2], "big")
            headers.extend(U32.pack(length))
            headers.extend(config[cursor + 2 : cursor + 2 + length])
            cursor += 2 + length
        if keep_vps and kind == 32:
            arrays.append(config[first:cursor])
    # Add length-prefixed parameter sets to sample zero, retaining later samples.
    layout = list(boxes(data))
    chunks = [box[0] for box in layout if box[2] == b"stco"]
    assert len(chunks) == 1
    first_sample = U32.unpack_from(data, chunks[0] + 16)[0]
    for offset, length, kind, _ in layout:
        if kind == b"stsz":
            assert U32.unpack_from(data, offset + 12)[0] == 0
            U32.pack_into(
                data, offset + 20, U32.unpack_from(data, offset + 20)[0] + len(headers)
            )
        elif kind == b"mdat":
            assert offset + 8 <= first_sample < offset + length
            U32.pack_into(data, offset, length + len(headers))
        elif kind == b"stco":
            for entry in range(U32.unpack_from(data, offset + 12)[0]):
                pos = offset + 16 + 4 * entry
                value = U32.unpack_from(data, pos)[0]
                if value > first_sample:
                    U32.pack_into(data, pos, value + len(headers))
    data[first_sample:first_sample] = headers
    replacement = config[:22] + bytes([len(arrays)]) + b"".join(arrays)
    replacement = U32.pack(len(replacement) + 8) + b"hvcC" + replacement
    delta = len(replacement) - size
    for parent in parents:
        U32.pack_into(data, parent, U32.unpack_from(data, parent)[0] + delta)
    data[at : at + size] = replacement
    for offset, _, kind, _ in boxes(data):
        if kind == b"pasp":
            data[offset + 4 : offset + 8] = b"free"
        elif kind == b"stco":
            count = U32.unpack_from(data, offset + 12)[0]
            for entry in range(count):
                pos = offset + 16 + 4 * entry
                value = U32.unpack_from(data, pos)[0]
                if value >= at + size:
                    U32.pack_into(data, pos, value + delta)
        assert kind != b"co64", "small fixtures should use stco"
    (ROOT / target).write_bytes(data)


if __name__ == "__main__":
    convert("hevc-sar-nopasp.mp4", "hevc-sar-inband.mp4", False)
    convert("hevc-hlg-nocolr.mp4", "hevc-hlg-partial-inband.mp4", True)
