#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Transport assembly vectors; canonical field pairs qualify samples with FFmpeg.

Unrelated coded pictures between paired fields are a declared decoder extension,
not H.264 conformance. FFmpeg qualifies the original contiguous picture streams.
"""

import hashlib
import json
from pathlib import Path
import tempfile

from generate_h264_advanced_vectors import (
    DATA,
    Bits,
    interlaced_pcm,
    mux_pcm,
    validate_known,
)

START = b"\x00\x00\x00\x01"
AUX = START + b"\x09\xf0"  # AUD-only packet, not a selected picture


def parts(encoded):
    nals = encoded.split(START)[1:]
    return [START + nal for nal in nals[:2]], [START + nal for nal in nals[2:]]


def pcm(
    depth, chroma, frame=0, cabac=False, bottom=False, fragments=False, offset=0, refs=4
):
    return interlaced_pcm(
        True,
        depth,
        chroma,
        references=refs,
        frame_num=frame,
        idr_first=frame == 0,
        sample_offset=offset,
        cabac=cabac,
        bottom_first=bottom,
        slice_mbs=2 if fragments else None,
    )


def fragmented(depth, chroma, cabac, bottom):
    encoded, known = pcm(depth, chroma, cabac=cabac, bottom=bottom, fragments=True)
    sets, nals = parts(encoded)
    packets, members = [], []
    for i, nal in enumerate(nals):
        members.append(len(packets))
        packets.append((b"".join(sets) if i == 0 else b"") + nal)
        packets.append(AUX)
    return packets, known, [members], [encoded]


def interleaved(depth, chroma, fragments=False):
    pairs = [
        pcm(depth, chroma, f, fragments=fragments, offset=f * 29) for f in range(3)
    ]
    sets, nals = parts(pairs[0][0])
    lists = [nals] + [parts(pair[0])[1] for pair in pairs[1:]]
    half = len(nals) // 2
    schedule = [(0, 0), (1, 0), (2, 0), (1, 1), (0, 1), (2, 1)]
    packets, members = [], [[], [], []]
    for frame, parity in schedule:
        for nal in lists[frame][parity * half : (parity + 1) * half]:
            members[frame].append(len(packets))
            packets.append((b"".join(sets) if not packets else b"") + nal)
    return (
        packets,
        b"".join(pair[1] for pair in pairs),
        members,
        [pairs[0][0]] + [b"".join(parts(pair[0])[1]) for pair in pairs[1:]],
    )


def frozen_prediction(depth, chroma):
    initial, known = pcm(depth, chroma, refs=1)
    sets, first = parts(initial)
    middle, _ = pcm(depth, chroma, 1, refs=1)
    _, middle_fields = parts(middle)
    future, future_known = pcm(depth, chroma, 2, offset=51, refs=1)
    _, future_fields = parts(future)
    slices = []
    for first_mb in range(0, 8, 2):
        bits = Bits()
        for value in (first_mb, 0, 0):
            bits.ue(value)
        bits.fixed(1, 4)
        bits.fixed(1, 1)
        bits.fixed(0, 1)
        bits.fixed(0, 3)  # active count, reordering and adaptive marking
        bits.se(0)
        bits.ue(1)
        bits.ue(2)  # two P skip macroblocks
        slices.append(bits.nal(0x41))
    packets = [
        b"".join(sets + first),
        slices[0],
        b"".join(future_fields),
        *slices[1:],
        middle_fields[1],
    ]
    members = [[0], [1, 3, 4, 5, 6], [2]]
    canonical = [initial, b"".join(slices) + middle_fields[1], b"".join(future_fields)]
    return packets, known + known + future_known, members, canonical


def main():
    cases = []
    with tempfile.TemporaryDirectory(prefix="antfly-paff-assembly-") as tmp:
        for depth, chroma in ((8, 1), (10, 2), (14, 3)):
            variants = [
                ("fragments-cavlc", fragmented(depth, chroma, False, False)),
                ("fragments-cabac-bottom", fragmented(depth, chroma, True, True)),
                ("interleaved", interleaved(depth, chroma)),
                ("interleaved-fragments", interleaved(depth, chroma, True)),
                ("frozen-prediction", frozen_prediction(depth, chroma)),
            ]
            for kind, (packets, known, members, canonical) in variants:
                name = f"h264-paff-{kind}-{depth}-{chroma}"
                owner = {
                    packet: frame
                    for frame, indices in enumerate(members)
                    for packet in indices
                }
                pts = [owner.get(i, 10000) for i in range(len(packets))]
                mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                mp4.write_bytes(
                    mux_pcm(packets, 64, 64, [value - i for i, value in enumerate(pts)])
                )
                native.write_bytes(known)
                oracle_file = Path(tmp) / (name + "-canonical.mp4")
                oracle_file.write_bytes(mux_pcm(canonical, 64, 64))
                oracle = validate_known(
                    Path(tmp), oracle_file, known, depth, chroma, 64, 64
                )
                cases.append(
                    dict(
                        name=name,
                        bit_depth=depth,
                        chroma_format=chroma,
                        members=members,
                        oracle=oracle
                        + " after canonical field-pair assembly; transport/inter-picture extension tested independently",
                        mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                        nv12_sha256=hashlib.sha256(known).hexdigest(),
                    )
                )
    (DATA / "h264-paff-assembly-oracle.json").write_text(
        json.dumps(dict(cases=cases), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
