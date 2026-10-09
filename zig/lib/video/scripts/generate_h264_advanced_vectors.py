#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Normative vectors for syntax tools ordinary encoders do not readily emit.

CABAC fixture encoding follows ITU-T H.264 9.3.4's informative reference algorithm.
FFmpeg independently checks supported native pixel depths. FMO and depths
11/13 use normative known-sample vectors. No encoder is used at runtime.
"""

import ast
import hashlib
import json
from pathlib import Path
import random
import struct
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "testdata"


class Bits:
    def __init__(self):
        self.bits = ""

    def fixed(self, value, size):
        self.bits += format(value, f"0{size}b")

    def ue(self, value):
        code = format(value + 1, "b")
        self.bits += "0" * (len(code) - 1) + code

    def se(self, value):
        self.ue(2 * abs(value) - (value > 0))

    def align(self, value=0):
        self.bits += str(value) * (-len(self.bits) % 8)

    def nal(self, kind, stop=True):
        bits = self.bits + ("1" if stop else "")
        bits += "0" * (-len(bits) % 8)
        raw = bytes(int(bits[i : i + 8], 2) for i in range(0, len(bits), 8))
        output, zeros = bytearray([kind]), 0
        for value in raw:
            if zeros == 2 and value <= 3:
                output.append(3)
                zeros = 0
            output.append(value)
            zeros = zeros + 1 if value == 0 else 0
        return b"\x00\x00\x00\x01" + output


def table(name):
    source = (ROOT / "src/h264_cabac_tables.zig").read_text()
    start = source.index("=", source.index("pub const " + name)) + 1
    body = source[start : source.index(";", start)]
    return ast.literal_eval(body.replace(".{", "[").replace("}", "]").strip())


class Cabac:
    lps = table("lps")
    transition = table("transition")

    def __init__(self, bits, qp=26):
        self.bits = bits
        self.state = []
        for row in table("initial"):
            m, n = row[0]
            pre = min(126, max(1, (m * qp >> 4) + n))
            self.state.append(((63 - pre) * 2) if pre <= 63 else ((pre - 64) * 2 + 1))
        self.restart()

    def restart(self):
        self.low, self.range, self.outstanding, self.first = 0, 510, 0, True

    def put(self, value):
        if self.first:
            self.first = False
        else:
            self.bits.fixed(value, 1)
        for _ in range(self.outstanding):
            self.bits.fixed(1 - value, 1)
        self.outstanding = 0

    def normalize(self):
        while self.range < 256:
            if self.low < 256:
                self.put(0)
            elif self.low >= 512:
                self.put(1)
                self.low -= 512
            else:
                self.outstanding += 1
                self.low -= 256
            self.low <<= 1
            self.range <<= 1

    def bin(self, context, value):
        encoded = self.state[context]
        state, mps = encoded >> 1, encoded & 1
        lps = self.lps[state][(self.range >> 6) & 3]
        self.range -= lps
        if value != mps:
            self.low += self.range
            self.range = lps
            self.state[context] = self.transition[state][0] * 2 + (mps ^ (state == 0))
        else:
            self.state[context] = self.transition[state][1] * 2 + mps
        self.normalize()

    def terminate(self, value):
        self.range -= 2
        if value:
            self.low += self.range
            self.range = 2
            self.normalize()
            self.put((self.low >> 9) & 1)
            self.bits.fixed(((self.low >> 7) & 3) | 1, 2)
        else:
            self.normalize()


def configuration(
    cabac=False, poc=2, width=64, height=48, gaps=False, crop_right=0, references=1
):
    sps = Bits()
    for value in (77 if cabac else 66, 0, 10):
        sps.fixed(value, 8)
    sps.ue(0)
    sps.ue(0)
    sps.ue(poc)
    if poc == 1:
        sps.fixed(0, 1)
        sps.se(-1)
        sps.se(1)
        sps.ue(2)
        sps.se(2)
        sps.se(2)
    sps.ue(references)
    sps.fixed(gaps, 1)
    sps.ue(width // 16 - 1)
    sps.ue(height // 16 - 1)
    sps.fixed(1, 1)
    sps.fixed(1, 1)
    sps.fixed(bool(crop_right), 1)
    if crop_right:
        for crop in (0, crop_right, 0, 0):
            sps.ue(crop)
    sps.fixed(0, 1)
    pps = Bits()
    pps.ue(0)
    pps.ue(0)
    pps.fixed(cabac, 1)
    pps.fixed(poc == 1, 1)
    for value in (0, 0, 0):
        pps.ue(value)
    pps.fixed(0, 3)
    for value in (0, 0, 0):
        pps.se(value)
    pps.fixed(1, 1)
    pps.fixed(0, 2)
    return sps, pps


def pcm_vector(
    cabac,
    poc,
    width=64,
    height=48,
    frames=4,
    frame_numbers=None,
    gaps=False,
    idr_first=True,
):
    sps, pps = configuration(cabac, poc, width, height, gaps)
    output = sps.nal(0x67) + pps.nal(0x68)
    rng = random.Random(2026)
    for frame in range(frames):
        bits = Bits()
        for value in (0, 2, 0):
            bits.ue(value)
        bits.fixed(frame if frame_numbers is None else frame_numbers[frame], 4)
        if frame == 0 and idr_first:
            bits.ue(0)
        if poc == 1:
            bits.se(0)
            bits.se(-1)
        if frame == 0 and idr_first:
            bits.fixed(0, 2)
        else:
            bits.fixed(0, 1)
        bits.se(0)
        bits.ue(1)
        bits.align(1 if cabac else 0) if cabac else None
        coder = Cabac(bits) if cabac else None
        for mb in range(width * height // 256):
            if coder:
                coder.bin(3 + (mb % (width // 16) != 0) + (mb >= width // 16), 1)
                coder.terminate(1)
            else:
                bits.ue(25)
            bits.align()
            for _ in range(384):
                bits.fixed(rng.randrange(256), 8)
            if coder:
                coder.restart()
                coder.terminate(mb + 1 == width * height // 256)
        output += bits.nal(0x65 if frame == 0 and idr_first else 0x61, stop=not cabac)
    return output


def grouped_vector(
    kind, direction=False, width=64, height=48, redundant=False, missing=False
):
    """Each PCM sample is known independently of slice order and entropy decoding."""
    w, h = width // 16, height // 16
    size = w * h
    mapping = [0] * size
    sps, _ = configuration(False, 2, width, height)
    pps = Bits()
    pps.ue(0)
    pps.ue(0)
    pps.fixed(0, 2)
    pps.ue(1)
    pps.ue(kind)
    if kind == 0:
        pps.ue(0)
        pps.ue(1)
        mapping = [0 if i % 3 == 0 else 1 for i in range(size)]
    elif kind == 1:
        mapping = [(i % w + i // w) % 2 for i in range(size)]
    elif kind == 2:
        pps.ue(1)
        pps.ue(6)
        mapping = [
            0 if 0 <= i // w <= 1 and 1 <= i % w <= 2 else 1 for i in range(size)
        ]
    elif kind in (3, 4, 5):
        pps.fixed(direction, 1)
        pps.ue(1)
        count = 6
        if kind == 3:
            mapping = [1] * size
            x, y = (w - direction) // 2, (h - direction) // 2
            left = right = x
            top = bottom = y
            dx, dy = direction - 1, int(direction)
            while count:
                if mapping[y * w + x]:
                    mapping[y * w + x] = 0
                    count -= 1
                if dx == -1 and x == left:
                    left = max(0, left - 1)
                    x = left
                    dx, dy = 0, 2 * direction - 1
                elif dx == 1 and x == right:
                    right = min(w - 1, right + 1)
                    x = right
                    dx, dy = 0, 1 - 2 * direction
                elif dy == -1 and y == top:
                    top = max(0, top - 1)
                    y = top
                    dx, dy = 1 - 2 * direction, 0
                elif dy == 1 and y == bottom:
                    bottom = min(h - 1, bottom + 1)
                    y = bottom
                    dx, dy = 2 * direction - 1, 0
                else:
                    x += dx
                    y += dy
        else:
            upper = size - count if direction else count
            mapping = [
                int(direction)
                if (i if kind == 4 else i % w * h + i // w) < upper
                else 1 - int(direction)
                for i in range(size)
            ]
    elif kind == 6:
        pps.ue(size - 1)
        mapping = [(i * 7 + i // w) % 2 for i in range(size)]
        for group in mapping:
            pps.fixed(group, 1)
    for _ in range(2):
        pps.ue(0)
    pps.fixed(0, 3)
    for _ in range(3):
        pps.se(0)
    pps.fixed(1, 1)
    pps.fixed(0, 1)
    pps.fixed(redundant, 1)
    output = sps.nal(0x67) + pps.nal(0x68)
    y_plane = bytearray(width * height)
    uv = bytearray(width * height // 2)
    slices = []
    for group, redundant_count in [
        (group, count)
        for group in range(2)
        for count in ((1,) if missing else (0, 1) if redundant else (0,))
    ]:
        members = [i for i in range(size) if mapping[i] == group]
        bits = Bits()
        bits.ue(members[0])
        bits.ue(2)
        bits.ue(0)
        bits.fixed(0, 4)
        bits.ue(0)
        if redundant:
            bits.ue(redundant_count)
        bits.fixed(0, 2)
        bits.se(0)
        bits.ue(1)
        if kind in (3, 4, 5):
            bits.fixed(3, 3)
        for mb in members:
            bits.ue(25)
            bits.align()
            for plane in range(3):
                edge = 16 if plane == 0 else 8
                for row in range(edge):
                    for col in range(edge):
                        value = (mb * 17 + plane * 67 + row * 11 + col * 3) % 256
                        bits.fixed(value, 8)
                        if plane == 0:
                            y_plane[
                                (mb // w * 16 + row) * width + mb % w * 16 + col
                            ] = value
                        else:
                            uv[
                                (mb // w * 8 + row) * width
                                + mb % w * 16
                                + col * 2
                                + plane
                                - 1
                            ] = value
        slices.append((members[0], redundant_count, bits.nal(0x65)))
    # ASO: later first_mb precedes earlier first_mb.
    return output + b"".join(
        nal for _, _, nal in sorted(slices, key=lambda item: (-item[0], item[1]))
    ), bytes(y_plane + uv)


def interlaced_pcm(
    paff=False,
    depth=8,
    chroma=1,
    all_fields=False,
    width=64,
    height=64,
    references=1,
    sample_offset=0,
    frame_num=0,
    idr_first=True,
    marking=None,
    idr_long=False,
    bottom_first=False,
    cabac=False,
    slice_mbs=None,
):
    sx, sy = (1 if chroma == 3 else 2), (2 if chroma == 1 else 1)
    w, h = width // 16, height // 32
    sps, pps = Bits(), Bits()
    profile = (
        244
        if chroma == 3 or depth > 10
        else 122
        if chroma == 2
        else 110
        if depth > 8
        else 100
    )
    for value in (profile, 0, 21):
        sps.fixed(value, 8)
    sps.ue(0)
    sps.ue(chroma)
    if chroma == 3:
        sps.fixed(0, 1)
    sps.ue(depth - 8)
    sps.ue(depth - 8)
    sps.fixed(0, 2)
    sps.ue(0)
    sps.ue(2)
    sps.ue(references)
    sps.fixed(0, 1)
    sps.ue(w - 1)
    sps.ue(h - 1)
    sps.fixed(0, 1)
    sps.fixed(not paff, 1)
    sps.fixed(1, 1)
    sps.fixed(0, 2)
    pps.ue(0)
    pps.ue(0)
    pps.fixed(cabac, 1)
    pps.fixed(0, 1)
    for _ in range(3):
        pps.ue(0)
    pps.fixed(0, 3)
    for _ in range(3):
        pps.se(0)
    pps.fixed(1, 1)
    pps.fixed(0, 2)
    output = sps.nal(0x67) + pps.nal(0x68)
    planes = [
        [0] * (width * height // (1 if p == 0 else sx * sy))
        for p in range(1 if chroma == 0 else 3)
    ]
    first_side = 1 if bottom_first else 0
    for parity in ((1, 0) if bottom_first else (0, 1)) if paff else (0,):
        size = w * h * (1 if paff else 2)
        step = size if slice_mbs is None else slice_mbs
        assert step > 0 and (paff or slice_mbs is None)
        for first_address in range(0, size, step):
            last_address = min(size, first_address + step)
            bits = Bits()
            bits.ue(first_address)
            bits.ue(2)
            bits.ue(0)
            bits.fixed(frame_num, 4)
            bits.fixed(paff, 1)
            if paff:
                bits.fixed(parity, 1)
            if idr_first and (not paff or parity == first_side):
                bits.ue(0)
            if idr_first and (not paff or parity == first_side):
                bits.fixed(0, 1)
                bits.fixed(idr_long, 1)
            else:
                commands = marking[parity] if marking else []
                bits.fixed(bool(commands), 1)
                for operation, first, second in commands:
                    bits.ue(operation)
                    if operation in (1, 2, 3, 4, 6):
                        bits.ue(first)
                    if operation == 3:
                        bits.ue(second)
                if commands:
                    bits.ue(0)
            bits.se(0)
            bits.ue(1)
            if cabac:
                assert paff
                bits.align(1)
            coder = Cabac(bits) if cabac else None
            for address in range(first_address, last_address):
                pair = address if paff else address // 2
                bottom = parity if paff else address % 2
                field = paff or all_fields or (pair % 3 == 1)
                if not paff and address % 2 == 0:
                    bits.fixed(field, 1)
                if coder:
                    coder.bin(
                        3
                        + (address % w != 0 and address - 1 >= first_address)
                        + (address >= w and address - w >= first_address),
                        1,
                    )
                    coder.terminate(1)
                else:
                    bits.ue(25)
                bits.align()
                for p in range(1 if chroma == 0 else 3):
                    px, py = (1, 1) if p == 0 else (sx, sy)
                    edge_x, edge_y = 16 // px, 16 // py
                    stride = width // px
                    for row in range(edge_y):
                        physical_y = pair // w * 2 * edge_y + (
                            row * 2 + bottom if field else row + bottom * edge_y
                        )
                        for col in range(edge_x):
                            x = pair % w * edge_x + col
                            value = (
                                physical_y * 41 + x * 19 + p * 53 + sample_offset
                            ) % (1 << depth)
                            bits.fixed(value, depth)
                            planes[p][physical_y * stride + x] = value
                if coder:
                    coder.restart()
                    coder.terminate(address + 1 == last_address)
            output += bits.nal(
                0x65 if idr_first and (not paff or parity == first_side) else 0x41,
                stop=not cabac,
            )
    known = (
        planes[0]
        if chroma == 0
        else planes[0] + [v for pair in zip(planes[1], planes[2]) for v in pair]
    )
    word = "H" if depth > 8 else "B"
    return output, struct.pack("<" + word * len(known), *known)


def mux_pcm(annex, width=64, height=48, composition_offsets=None, sample_entry="avc1"):
    assert sample_entry in ("avc1", "avc3")
    """Minimal ISO BMFF muxer; supports tools FFmpeg intentionally rejects."""

    def box(kind, payload):
        return struct.pack(">I4s", len(payload) + 8, kind.encode()) + payload

    def integers(*values):
        return struct.pack(">" + "I" * len(values), *values)

    def full(kind, *values):
        return box(kind, integers(0, *values))

    annexes = annex if isinstance(annex, list) else [annex]
    nals = annexes[0].split(b"\x00\x00\x00\x01")[1:]
    sps, pps = nals[:2]
    packets = [
        b"".join(
            integers(len(nal)) + nal
            for nal in (nals[2:] if sample_entry == "avc1" else nals)
        )
    ]
    packets += [
        b"".join(
            integers(len(nal)) + nal for nal in part.split(b"\x00\x00\x00\x01")[1:]
        )
        for part in annexes[1:]
    ]
    packet = b"".join(packets)
    count = len(packets)
    avcc = (
        bytes([1, *sps[1:4], 255, 225])
        + struct.pack(">H", len(sps))
        + sps
        + bytes([1])
        + struct.pack(">H", len(pps))
        + pps
    )
    visual = bytearray(78)
    visual[6:8] = struct.pack(">H", 1)
    visual[24:28] = struct.pack(">HH", width, height)
    visual[28:36] = integers(72 << 16, 72 << 16)
    visual[40:42] = struct.pack(">H", 1)
    visual[74:78] = struct.pack(">HH", 24, 65535)
    tkhd = bytearray(84)
    tkhd[3] = 3
    tkhd[12:16] = integers(1)
    tkhd[40:76] = integers(1 << 16, 0, 0, 0, 1 << 16, 0, 0, 0, 1 << 30)
    tkhd[76:84] = integers(width << 16, height << 16)
    ftyp = box("ftyp", b"isom" + integers(512) + b"isomavc1mp41")

    def movie(offset):
        stbl = box(
            "stbl",
            box(
                "stsd",
                integers(0, 1) + box(sample_entry, bytes(visual) + box("avcC", avcc)),
            )
            + full("stts", 1, count, 1)
            + full("stsc", 1, 1, count, 1)
            + full("stsz", 0, count, *(len(part) for part in packets))
            + full("stco", 1, offset)
            + full("stss", 1, 1),
        )
        if composition_offsets is not None:
            assert len(composition_offsets) == count
            stbl = box(
                "stbl",
                stbl[8:]
                + box(
                    "ctts",
                    integers(1 << 24, count)
                    + b"".join(
                        integers(1, offset & 0xFFFFFFFF)
                        for offset in composition_offsets
                    ),
                ),
            )
        dinf = box("dinf", box("dref", integers(0, 1) + box("url ", integers(1))))
        mdia = box(
            "mdia",
            full("mdhd", 0, 0, 4, count, 0)
            + box("hdlr", integers(0, 0) + b"vide" + bytes(12) + b"Video\x00")
            + box("minf", full("vmhd", 0, 0) + dinf + stbl),
        )
        mvhd = bytearray(100)
        mvhd[12:20] = integers(4, count)
        mvhd[20:24] = integers(1 << 16)
        mvhd[24:26] = struct.pack(">H", 256)
        mvhd[36:72] = integers(1 << 16, 0, 0, 0, 1 << 16, 0, 0, 0, 1 << 30)
        mvhd[96:100] = integers(2)
        return box(
            "moov",
            box("mvhd", bytes(mvhd)) + box("trak", box("tkhd", bytes(tkhd)) + mdia),
        )

    moov = movie(0)
    moov = movie(len(ftyp) + len(moov) + 8)
    return ftyp + moov + box("mdat", packet)


def paff_prediction(depth, chroma):
    initial, known = interlaced_pcm(True, depth, chroma, True, references=2)
    packets = [initial]
    for frame in (1, 2):
        packet = b""
        for parity in (0, 1):
            bits = Bits()
            bits.ue(0)
            bits.ue(0)
            bits.ue(0)
            bits.fixed(frame, 4)
            bits.fixed(1, 1)
            bits.fixed(parity, 1)
            explicit = frame == 2 and parity == 1
            bits.fixed(explicit, 1)
            if explicit:
                bits.ue(1)
            reordered = frame == 1 and parity == 0 and chroma >= 2
            bits.fixed(reordered, 1)
            if reordered:
                bits.ue(0)
                bits.ue(2)
                bits.ue(3)
            bits.fixed(0, 1)
            bits.se(0)
            bits.ue(1)
            if explicit:
                for _ in range(8):
                    bits.ue(0)
                    bits.ue(0)
                    bits.fixed(0, 1)
                    bits.se(0)
                    bits.se(0)
                    bits.ue(0)
            else:
                bits.ue(8)
            packet += bits.nal(0x41)
        packets.append(packet)
    word = 2 if depth > 8 else 1
    size = 64 * 64
    sub_x, sub_y = (1 if chroma == 3 else 2), (2 if chroma == 1 else 1)
    middle = bytearray(known)
    if chroma >= 2:
        for plane in range(2):
            stride = (64 if plane == 0 else 64 // sub_x * 2) * word
            height = 64
            start = 0 if plane == 0 else size * word
            for y in range(0, height, 2):
                middle[start + y * stride : start + (y + 1) * stride] = known[
                    start + (y + 1) * stride : start + (y + 2) * stride
                ]
    last = bytearray(middle)
    for plane in range(2):
        stride = (64 if plane == 0 else 64 // sub_x * 2) * word
        height = 64 if plane == 0 else 64 // sub_y
        start = 0 if plane == 0 else size * word
        for y in range(1, height, 2):
            if plane == 1 and chroma == 1:
                for x in range(stride // word):
                    offset = start + (y - 1) * stride + x * word
                    following = start + min(y + 1, height - 2) * stride + x * word
                    a = int.from_bytes(middle[offset : offset + word], "little")
                    b = int.from_bytes(middle[following : following + word], "little")
                    value = (3 * a + b + 2) // 4
                    at = start + y * stride + x * word
                    last[at : at + word] = value.to_bytes(word, "little")
            else:
                last[start + y * stride : start + (y + 1) * stride] = middle[
                    start + (y - 1) * stride : start + y * stride
                ]
    return packets, known + middle + last


def paff_b(depth, chroma, spatial):
    first, old = interlaced_pcm(True, depth, chroma, True, references=2)
    encoded, future = interlaced_pcm(
        True,
        depth,
        chroma,
        True,
        references=2,
        sample_offset=32,
        frame_num=1,
        idr_first=False,
    )
    new = b"".join(
        b"\x00\x00\x00\x01" + nal for nal in encoded.split(b"\x00\x00\x00\x01")[3:]
    )
    b_packet = b""
    for parity in (0, 1):
        bits = Bits()
        bits.ue(0)
        bits.ue(1)
        bits.ue(0)
        bits.fixed(1, 4)
        bits.fixed(1, 1)
        bits.fixed(parity, 1)
        bits.fixed(spatial, 1)
        bits.fixed(0, 3)
        bits.se(0)
        bits.ue(1)
        bits.ue(8)
        b_packet += bits.nal(0x01)
    word = 2 if depth > 8 else 1
    blend = bytearray()
    for i in range(0, len(old), word):
        value = (
            int.from_bytes(old[i : i + word], "little")
            + int.from_bytes(future[i : i + word], "little")
            + 1
        ) // 2
        blend.extend(value.to_bytes(word, "little"))
    return [first, new, b_packet], old + blend + future


def intra_dc(depth, chroma, second_offset=0):
    """One Intra16 macroblock: DC level +1 at QPprime 48 gives +10."""
    sps, pps, bits = Bits(), Bits(), Bits()
    for v in (244, 0, 21):
        sps.fixed(v, 8)
    sps.ue(0)
    sps.ue(chroma)
    if chroma == 3:
        sps.fixed(0, 1)
    sps.ue(depth - 8)
    sps.ue(depth - 8)
    sps.fixed(0, 2)
    sps.ue(0)
    sps.ue(2)
    sps.ue(1)
    sps.fixed(0, 1)
    sps.ue(0)
    sps.ue(0)
    sps.fixed(3, 2)
    sps.fixed(0, 2)
    pps.ue(0)
    pps.ue(0)
    pps.fixed(0, 2)
    for _ in range(3):
        pps.ue(0)
    pps.fixed(0, 3)
    pps.se(22 - 6 * (depth - 8))
    pps.se(0)
    pps.se(0)
    pps.fixed(1, 1)
    pps.fixed(0, 2)
    if second_offset:
        pps.fixed(0, 2)
        pps.se(second_offset)
    bits.ue(0)
    bits.ue(2)
    bits.ue(0)
    bits.fixed(0, 4)
    bits.ue(0)
    bits.fixed(0, 2)
    bits.se(0)
    bits.ue(1)
    bits.ue(3)
    if chroma != 3:
        bits.ue(0)
    bits.se(0)
    for _ in range(3 if chroma == 3 else 1):
        bits.fixed(1, 2)
        bits.fixed(0, 1)
        bits.fixed(1, 1)
    midpoint = 1 << (depth - 1)
    known = [midpoint + 10] * 256
    sub = (1 if chroma == 3 else 2) * (2 if chroma == 1 else 1)

    def chroma_increment(offset):
        qpi = 48 - 6 * (depth - 8) + offset
        mapping = (
            29,
            30,
            31,
            32,
            32,
            33,
            34,
            34,
            35,
            35,
            36,
            36,
            37,
            37,
            37,
            38,
            38,
            38,
            39,
            39,
            39,
            39,
        )
        qp = (qpi if qpi < 30 else mapping[qpi - 30]) + 6 * (depth - 8)
        scaled = (10, 11, 13, 14, 16, 18)[qp % 6] * 16 * (1 << (qp // 6 - 6))
        return (scaled + 32) // 64

    known += (
        [midpoint + chroma_increment(0), midpoint + chroma_increment(second_offset)]
        * 256
        if chroma == 3
        else [midpoint] * (512 // sub)
    )
    return sps.nal(0x67) + pps.nal(0x68) + bits.nal(0x65), struct.pack(
        "<" + "H" * len(known), *known
    )


def run(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def validate_known(work, mp4, known, depth, chroma, width, height):
    """Independent native-planar oracle; do not quantize high-depth samples."""
    if depth in (11, 13):
        return "known normative samples (FFmpeg has no 11/13-bit pixel format)"
    fmt = f"yuv{420 if chroma == 1 else 422 if chroma == 2 else 444}p" + (
        f"{depth}le" if depth > 8 else ""
    )
    oracle = work / (mp4.stem + ".planar")
    run(
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(mp4),
        "-fps_mode",
        "passthrough",
        "-pix_fmt",
        fmt,
        "-f",
        "rawvideo",
        str(oracle),
    )
    raw = oracle.read_bytes()
    word = 2 if depth > 8 else 1
    size = width * height
    chroma_size = size // ((1 if chroma == 3 else 2) * (2 if chroma == 1 else 1))
    frame_size = (size + 2 * chroma_size) * word
    assert len(raw) == len(known)
    semiplanar = bytearray()
    for start in range(0, len(raw), frame_size):
        semiplanar.extend(raw[start : start + size * word])
        for i in range(chroma_size):
            for plane in range(2):
                at = start + (size + plane * chroma_size + i) * word
                semiplanar.extend(raw[at : at + word])
    assert semiplanar == known, mp4.stem
    return "FFmpeg native planar samples match independently known values"


if __name__ == "__main__":
    receipts = []
    with tempfile.TemporaryDirectory(prefix="antfly-h264-advanced-") as tmp:
        work = Path(tmp)
        for name, cabac, poc in [
            ("h264-cabac-pcm", True, 2),
            ("h264-poc1-pcm", False, 1),
        ]:
            annex = work / (name + ".264")
            annex.write_bytes(pcm_vector(cabac, poc))
            mp4, nv12 = DATA / (name + ".mp4"), DATA / (name + ".nv12")
            run(
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-framerate",
                "4",
                "-i",
                str(annex),
                "-c",
                "copy",
                "-bsf:v",
                "filter_units=remove_types=7|8",
                str(mp4),
            )
            run(
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(mp4),
                "-pix_fmt",
                "nv12",
                "-f",
                "rawvideo",
                str(nv12),
            )
            assert nv12.stat().st_size == 64 * 48 * 3 // 2 * 4
            receipts.append(
                dict(
                    name=name,
                    cabac=cabac,
                    poc_type=poc,
                    mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                    nv12_sha256=hashlib.sha256(nv12.read_bytes()).hexdigest(),
                )
            )
        for kind in range(7):
            for direction in range(2 if kind in (3, 4, 5) else 1):
                name = f"h264-groups-{kind}-{direction}"
                annex = work / (name + ".264")
                encoded, known = grouped_vector(kind, bool(direction))
                annex.write_bytes(encoded)
                mp4, nv12 = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                mp4.write_bytes(mux_pcm(encoded))
                nv12.write_bytes(known)
                receipts.append(
                    dict(
                        name=name,
                        map_type=kind,
                        direction=direction,
                        oracle="known PCM samples, H.264 8.2.2 mapping; standalone ISO BMFF mux",
                        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                        nv12_sha256=hashlib.sha256(known).hexdigest(),
                    )
                )
        for missing in (False, True):
            name = "h264-redundant" + ("-missing-primary" if missing else "")
            encoded, known = grouped_vector(1, redundant=True, missing=missing)
            mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
            mp4.write_bytes(mux_pcm(encoded))
            native.write_bytes(known)
            receipts.append(
                dict(
                    name=name,
                    oracle="known primary PCM samples, redundant copies do not replace them",
                    mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                    nv12_sha256=hashlib.sha256(known).hexdigest(),
                )
            )
        for paff, depth, chroma, all_fields in (
            (False, 8, 1, False),
            (False, 10, 2, False),
            (False, 10, 3, True),
            (True, 8, 1, True),
            (True, 12, 2, True),
            (True, 14, 3, True),
        ):
            name = f"h264-{'paff' if paff else 'mbaff'}-pcm-{depth}-{chroma}"
            encoded, known = interlaced_pcm(paff, depth, chroma, all_fields)
            mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
            mp4.write_bytes(mux_pcm(encoded, 64, 64))
            native.write_bytes(known)
            validation = validate_known(work, mp4, known, depth, chroma, 64, 64)
            receipts.append(
                dict(
                    name=name,
                    bit_depth=depth,
                    chroma_format=chroma,
                    oracle=validation,
                    mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                    nv12_sha256=hashlib.sha256(known).hexdigest(),
                )
            )
        for depth, chroma in ((8, 1), (10, 2), (14, 3)):
            name = f"h264-paff-prediction-{depth}-{chroma}"
            packets, known = paff_prediction(depth, chroma)
            mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
            mp4.write_bytes(mux_pcm(packets, 64, 64))
            native.write_bytes(known)
            validation = validate_known(work, mp4, known, depth, chroma, 64, 64)
            receipts.append(
                dict(
                    name=name,
                    bit_depth=depth,
                    chroma_format=chroma,
                    oracle=validation
                    + ", PAFF previous/current-first-field references and list reordering",
                    mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                    nv12_sha256=hashlib.sha256(known).hexdigest(),
                )
            )
        for depth, chroma in ((8, 1), (10, 2), (14, 3)):
            for spatial in (False, True):
                name = f"h264-paff-b-{depth}-{chroma}-{'spatial' if spatial else 'temporal'}"
                packets, known = paff_b(depth, chroma, spatial)
                mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                mp4.write_bytes(mux_pcm(packets, 64, 64, [0, 1, -1]))
                native.write_bytes(known)
                validation = validate_known(work, mp4, known, depth, chroma, 64, 64)
                receipts.append(
                    dict(
                        name=name,
                        bit_depth=depth,
                        chroma_format=chroma,
                        oracle=validation
                        + ", spatial/temporal B skip and field list order",
                        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                        nv12_sha256=hashlib.sha256(known).hexdigest(),
                    )
                )
        for depth in (9, 11, 12, 13, 14):
            for chroma in (1, 2, 3):
                name = f"h264-intra-dc-{depth}-{chroma}"
                encoded, known = intra_dc(depth, chroma)
                mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                mp4.write_bytes(mux_pcm(encoded, 16, 16))
                native.write_bytes(known)
                validation = validate_known(work, mp4, known, depth, chroma, 16, 16)
                receipts.append(
                    dict(
                        name=name,
                        bit_depth=depth,
                        chroma_format=chroma,
                        oracle=validation
                        + ", Intra16 DC level +1, luma QPprime 48: midpoint+10",
                        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                        nv12_sha256=hashlib.sha256(known).hexdigest(),
                    )
                )
        name = "h264-intra-dc-offsets-14-3"
        encoded, known = intra_dc(14, 3, 2)
        mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
        mp4.write_bytes(mux_pcm(encoded, 16, 16))
        native.write_bytes(known)
        validation = validate_known(work, mp4, known, 14, 3, 16, 16)
        receipts.append(
            dict(
                name=name,
                bit_depth=14,
                chroma_format=3,
                oracle=validation + ", Cb/Cr QPprime 48/50 gives midpoint+10/+13",
                mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                nv12_sha256=hashlib.sha256(known).hexdigest(),
            )
        )
        for chroma, depth, interlaced, filtered, lossless in tuple(
            (*case, False)
            for case in (
                (1, 10, False, True),
                (2, 8, False, True),
                (2, 10, False, True),
                (3, 8, False, True),
                (3, 10, False, True),
                (1, 8, True, True),
                (1, 10, True, True),
                (1, 8, True, False),
                (1, 10, True, False),
            )
        ) + (
            (1, 8, False, True, True),
            (2, 10, False, True, True),
            (3, 8, False, True, True),
            (3, 10, False, True, True),
        ):
            for cabac in (False, True):
                name = f"h264-high{depth}{'-lossless' if lossless else ''}{'-mbaff' if interlaced else ''}{'-unfiltered' if not filtered else ''}{f'-{420 if chroma == 1 else 422 if chroma == 2 else 444}' if chroma != 1 else ''}-{'cabac' if cabac else 'cavlc'}"
                width, height, frames = 128, 96, 12
                raw = work / (name + ".yuv")
                samples = []
                for frame in range(frames):
                    for plane in range(3):
                        pw, ph = (
                            (width, height)
                            if plane == 0
                            else (
                                width // (1 if chroma == 3 else 2),
                                height // (2 if chroma == 1 else 1),
                            )
                        )
                        for y in range(ph):
                            for x in range(pw):
                                if interlaced and x < pw // 2:
                                    value = (
                                        (x + frame * (3 if y % 2 == 0 else -2)) * 7
                                        + (y // 2) * 11
                                        + plane * 217
                                        + (y % 2) * (1 << (depth - 1))
                                    )
                                else:
                                    value = (
                                        (x + frame * 3) * 7
                                        + y * 11
                                        + plane * 217
                                        + ((x // 9 + y // 13) % 2) * 371
                                    )
                                samples.append(value % (1 << depth))
                raw.write_bytes(
                    struct.pack(
                        "<" + ("H" if depth > 8 else "B") * len(samples), *samples
                    )
                )
                mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                run(
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "rawvideo",
                    "-pix_fmt",
                    (
                        f"yuv{420 if chroma == 1 else 422 if chroma == 2 else 444}p{depth}le"
                        if depth > 8
                        else f"yuv{420 if chroma == 1 else 422 if chroma == 2 else 444}p"
                    ),
                    "-s",
                    f"{width}x{height}",
                    "-r",
                    "4",
                    "-i",
                    str(raw),
                    "-frames:v",
                    str(frames),
                    "-c:v",
                    "libx264",
                    "-profile:v",
                    "high444"
                    if lossless
                    else (
                        ("high10" if depth > 8 else "high")
                        if chroma == 1
                        else "high422"
                        if chroma == 2
                        else "high444"
                    ),
                    "-x264-params",
                    f"no-deblock={int(not filtered)}:interlaced={int(interlaced)}:tff={int(interlaced)}:threads=1:qp={0 if lossless else 23}:keyint=12:bframes=2:ref=3:weightp=2:weightb=1:cabac={int(cabac)}:slice-max-mbs=5:cqm=jvt",
                    str(mp4),
                )
                oracle = work / (name + ".decoded")
                run(
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(mp4),
                    "-pix_fmt",
                    (
                        f"yuv{420 if chroma == 1 else 422 if chroma == 2 else 444}p{depth}le"
                        if depth > 8
                        else f"yuv{420 if chroma == 1 else 422 if chroma == 2 else 444}p"
                    ),
                    "-f",
                    "rawvideo",
                    str(oracle),
                )
                word = "H" if depth > 8 else "B"
                decoded = struct.unpack(
                    "<" + word * (oracle.stat().st_size // (2 if depth > 8 else 1)),
                    oracle.read_bytes(),
                )
                output = []
                size = width * height
                for frame in range(frames):
                    sub_y = 2 if chroma == 1 else 1
                    sub_x = 1 if chroma == 3 else 2
                    start = frame * (size + 2 * size // (sub_x * sub_y))
                    output.extend(decoded[start : start + size])
                    for i in range(size // (sub_x * sub_y)):
                        output.extend(
                            (
                                decoded[start + size + i],
                                decoded[start + size + size // (sub_x * sub_y) + i],
                            )
                        )
                native.write_bytes(struct.pack("<" + word * len(output), *output))
                receipts.append(
                    dict(
                        name=name,
                        bit_depth=depth,
                        chroma_format=chroma,
                        cabac=cabac,
                        oracle="FFmpeg native planar samples, interleaved without quantization",
                        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                        nv12_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),
                    )
                )
        (DATA / "h264-advanced-oracle.json").write_text(
            json.dumps(
                dict(
                    ffmpeg=subprocess.check_output(
                        ["ffmpeg", "-version"], text=True
                    ).splitlines()[0],
                    cases=receipts,
                ),
                indent=2,
            )
            + "\n"
        )
