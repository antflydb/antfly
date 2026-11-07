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

import re
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


def unescape(unit):
    raw = bytearray()
    zeros = 0
    for byte in unit[2:]:
        if zeros == 2 and byte == 3:
            zeros = 0
            continue
        raw.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return "".join(f"{byte:08b}" for byte in raw).rstrip("0")


def escape(header, bits):
    bits += "0" * (-len(bits) % 8)
    raw = bytes(int(bits[index : index + 8], 2) for index in range(0, len(bits), 8))
    out = bytearray(header)
    zeros = 0
    for byte in raw:
        if zeros == 2 and byte <= 3:
            out.append(3)
            zeros = 0
        out.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return out


def multiple_sps(
    source="hevc-sar-nopasp.mp4",
    inband="hevc-sar-inband.mp4",
    target="hevc-multi-sps.mp4",
    library_copy=True,
):
    original = (ROOT / source).read_bytes()
    trace = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "verbose",
            "-i",
            str(ROOT / source),
            "-c:v",
            "copy",
            "-bsf:v",
            "trace_headers",
            "-f",
            "null",
            "-",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stderr

    def offset(name):
        match = re.search(r"\s(\d+)\s+" + name + r"\s+[01]+ = ", trace)
        assert match, name
        return int(match[1]) - 16  # NAL header is not part of the RBSP.

    id_bit = offset("sps_seq_parameter_set_id")
    sar_bit = offset("aspect_ratio_idc")
    at, size, _, _ = next(box for box in boxes(original) if box[2] == b"hvcC")
    config = original[at + 8 : at + size]
    sets = []
    cursor = 23
    for _ in range(config[22]):
        kind = config[cursor] & 63
        count = int.from_bytes(config[cursor + 1 : cursor + 3], "big")
        cursor += 3
        units = []
        for _ in range(count):
            length = int.from_bytes(config[cursor : cursor + 2], "big")
            cursor += 2
            units.append(config[cursor : cursor + length])
            cursor += length
        if kind in (32, 33, 34):
            sets.append((kind, units))
    payload = bytearray(config[:22])
    payload.append(len(sets))
    for kind, units in sets:
        if kind == 33:
            unit = units[0]
            bits = unescape(unit)
            assert bits[id_bit] == "1" and bits[sar_bit : sar_bit + 8] == "00010000"
            bits = bits[:sar_bit] + "00000001" + bits[sar_bit + 8 :]
            bits = bits[:id_bit] + "010" + bits[id_bit + 1 :]
            units = [escape(unit[:2], bits), *units]
        elif kind == 34:
            unit = units[0]
            bits = unescape(unit)
            assert bits[:2] == "11"  # PPS 0 references SPS 0.
            # PPS 1 references the unused 1:1 SPS 1. PPS 2 references SPS 0,
            # allowing the same-policy switch test to change PPS identities.
            units = [
                unit,
                escape(unit[:2], "010010" + bits[2:]),
                escape(unit[:2], "0111" + bits[2:]),
            ]
        payload += bytes([kind]) + len(units).to_bytes(2, "big")
        for unit in units:
            payload += len(unit).to_bytes(2, "big") + unit
    data = bytearray((ROOT / inband).read_bytes())
    at, size, _, parents = next(box for box in boxes(data) if box[2] == b"hvcC")
    replacement = U32.pack(len(payload) + 8) + b"hvcC" + payload
    delta = len(replacement) - size
    for parent in parents:
        U32.pack_into(data, parent, U32.unpack_from(data, parent)[0] + delta)
    data[at : at + size] = replacement
    for offset, _, kind, _ in boxes(data):
        if kind != b"stco":
            continue
        for index in range(U32.unpack_from(data, offset + 12)[0]):
            pos = offset + 16 + 4 * index
            value = U32.unpack_from(data, pos)[0]
            if value >= at + size:
                U32.pack_into(data, pos, value + delta)
    (ROOT / target).write_bytes(data)
    if library_copy:
        project = Path(__file__).resolve().parents[5]
        (project / "zig/lib/video/testdata/hevc-multi-sps.mp4").write_bytes(data)


def hardware_sized_sar():
    # GB10's NVDEC HEVC minimum is 144x144. Keep the small library fixtures,
    # but qualify the real hardware decoder with the same case at 192x192.
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-i",
            str(ROOT / "hevc-sar-nopasp.mp4"),
            "-vf",
            "scale=192:192,setsar=2/1",
            "-c:v",
            "libx265",
            "-pix_fmt",
            "yuv420p",
            "-preset",
            "ultrafast",
            "-x265-params",
            "pools=1:frame-threads=1:log-level=error:info=0",
            "-tag:v",
            "hvc1",
            "-map_metadata",
            "-1",
            "-fflags",
            "+bitexact",
            str(ROOT / "hevc-sar192.mp4"),
        ],
        check=True,
    )
    convert("hevc-sar192.mp4", "hevc-sar192-inband.mp4", False)
    multiple_sps(
        "hevc-sar192.mp4", "hevc-sar192-inband.mp4", "hevc-multi-sps192.mp4", False
    )


def unsupported_chroma():
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=red:s=64x64:r=1",
            "-frames:v",
            "2",
            "-pix_fmt",
            "yuv422p10le",
            "-c:v",
            "libx265",
            "-preset",
            "ultrafast",
            "-x265-params",
            "pools=1:frame-threads=1:log-level=error:info=0",
            "-tag:v",
            "hvc1",
            "-map_metadata",
            "-1",
            "-fflags",
            "+bitexact",
            str(ROOT / "hevc-422.mp4"),
        ],
        check=True,
    )


def cropped_reference_fixtures():
    # A tiny presentation can still need a 4K coded reference pool. Use a
    # conformance window, not a resize, to exercise that distinction.
    project = Path(__file__).resolve().parents[5]
    for depth, pix_fmt in ((8, "yuv420p"), (10, "yuv420p10le")):
        name = f"hevc-cropped4k-{depth}.mp4"
        target = ROOT / name
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "color=gray:s=3840x2160:r=1",
                "-frames:v",
                "2",
                "-c:v",
                "libx265",
                "-pix_fmt",
                pix_fmt,
                "-preset",
                "ultrafast",
                "-x265-params",
                "pools=1:frame-threads=1:log-level=error:info=0:keyint=1",
                "-tag:v",
                "hvc1",
                "-bsf:v",
                "hevc_metadata=crop_right=3712:crop_bottom=2032",
                "-map_metadata",
                "-1",
                "-fflags",
                "+bitexact",
                str(target),
            ],
            check=True,
        )
        data = bytearray(target.read_bytes())
        for start, size, typ, _ in boxes(data):
            if typ == b"hvc1":
                data[start + 32 : start + 36] = struct.pack(">HH", 128, 128)
            elif typ == b"tkhd":
                data[start + size - 8 : start + size] = struct.pack(
                    ">II", 128 << 16, 128 << 16
                )
        target.write_bytes(data)
        (project / "zig/lib/video/testdata" / name).write_bytes(data)
    convert("hevc-cropped4k-8.mp4", "hevc-cropped4k-inband.mp4", False)
    (project / "zig/lib/video/testdata/hevc-cropped4k-inband.mp4").write_bytes(
        (ROOT / "hevc-cropped4k-inband.mp4").read_bytes()
    )


if __name__ == "__main__":
    convert("hevc-sar-nopasp.mp4", "hevc-sar-inband.mp4", False)
    convert("hevc-hlg-nocolr.mp4", "hevc-hlg-partial-inband.mp4", True)
    multiple_sps()
    unsupported_chroma()
    hardware_sized_sar()
    cropped_reference_fixtures()
