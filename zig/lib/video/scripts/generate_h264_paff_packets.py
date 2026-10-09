#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Known PAFF samples in standalone packets and field MMCO, checked by FFmpeg."""

from functools import partial
import hashlib
import json
from pathlib import Path
import tempfile

from generate_h264_advanced_vectors import (
    DATA,
    Bits,
    interlaced_pcm,
    mux_pcm,
    paff_prediction,
    paff_b,
    validate_known,
)


def split_fields(packets):
    result = []
    for index, packet in enumerate(packets):
        nals = packet.split(b"\x00\x00\x00\x01")[1:]
        sets = nals[:2] if index == 0 else []
        fields = nals[2:] if index == 0 else nals
        assert len(fields) == 2
        result += [
            b"".join(b"\x00\x00\x00\x01" + nal for nal in sets + fields[:1]),
            b"\x00\x00\x00\x01" + fields[1],
        ]
    return result


def long_prediction(depth, chroma):
    first, known = interlaced_pcm(True, depth, chroma, references=4)
    second, middle = interlaced_pcm(
        True,
        depth,
        chroma,
        references=4,
        frame_num=1,
        idr_first=False,
        sample_offset=32,
        marking=[[(4, 2, 0), (3, 1, 0), (3, 2, 0)], []],
    )
    second = b"".join(
        b"\x00\x00\x00\x01" + nal for nal in second.split(b"\x00\x00\x00\x01")[3:]
    )
    packet = b""
    for parity in (0, 1):
        bits = Bits()
        bits.ue(0)
        bits.ue(0)
        bits.ue(0)
        bits.fixed(2, 4)
        bits.fixed(1, 1)
        bits.fixed(parity, 1)
        bits.fixed(0, 1)  # default active count
        bits.fixed(1, 1)
        bits.ue(2)  # long-term reference list modification
        bits.ue(1)  # index zero, same parity
        bits.ue(3)
        bits.fixed(1, 1)
        bits.ue(4)
        bits.ue(2)
        bits.ue(6)
        bits.ue(1)
        bits.ue(0)
        bits.se(0)
        bits.ue(1)
        bits.ue(8)  # all macroblocks skipped
        packet += bits.nal(0x41)
    return [first, second, packet], known + middle + known


def adaptive(depth, chroma):
    packets, output = [], b""
    commands = [
        [[], []],
        [[(4, 3, 0), (3, 1, 2), (6, 1, 0)], [(1, 1, 0), (6, 1, 0)]],
        [[(2, 5, 0), (4, 2, 0), (6, 0, 0)], [(2, 2, 0), (6, 0, 0)]],
        [[(4, 0, 0)], [(5, 0, 0)]],
    ]
    for frame, marking in enumerate(commands):
        packet, known = interlaced_pcm(
            True,
            depth,
            chroma,
            references=4,
            frame_num=frame,
            idr_first=frame == 0,
            sample_offset=frame * 31,
            marking=marking,
        )
        if frame:
            nals = packet.split(b"\x00\x00\x00\x01")[3:]
            packet = b"".join(b"\x00\x00\x00\x01" + nal for nal in nals)
        packets.append(packet)
        output += known
    return packets, output


def pcm_picture(depth, chroma, **tools):
    packet, known = interlaced_pcm(True, depth, chroma, **tools)
    return [packet], known


def main():
    cases = []
    with tempfile.TemporaryDirectory(prefix="antfly-paff-packets-") as tmp:
        for depth, chroma in ((8, 1), (10, 2), (14, 3)):
            for kind, generate in (
                ("split", paff_prediction),
                ("long", long_prediction),
                ("adaptive", adaptive),
                ("cabac", partial(pcm_picture, cabac=True)),
                ("bottom", partial(pcm_picture, bottom_first=True)),
                (
                    "idr-long",
                    partial(pcm_picture, idr_long=True, marking=[[], [(6, 0, 0)]]),
                ),
                ("b-spatial", lambda d, c: paff_b(d, c, True)),
                ("b-temporal", lambda d, c: paff_b(d, c, False)),
            ):
                packets, known = generate(depth, chroma)
                for standalone in (
                    (False, True) if kind in ("long", "adaptive") else (True,)
                ):
                    name = f"h264-paff-{kind}-{'packets' if standalone else 'paired'}-{depth}-{chroma}"
                    encoded = split_fields(packets) if standalone else packets
                    mp4, native = DATA / (name + ".mp4"), DATA / (name + ".nv12")
                    mp4.write_bytes(
                        mux_pcm(
                            encoded,
                            64,
                            64,
                            [0, 0, 2, 2, -2, -2] if kind.startswith("b-") else None,
                        )
                    )
                    native.write_bytes(known)
                    oracle = validate_known(
                        Path(tmp), mp4, known, depth, chroma, 64, 64
                    )
                    cases.append(
                        dict(
                            name=name,
                            bit_depth=depth,
                            chroma_format=chroma,
                            standalone=standalone,
                            oracle=oracle,
                            mp4_sha256=hashlib.sha256(mp4.read_bytes()).hexdigest(),
                            nv12_sha256=hashlib.sha256(known).hexdigest(),
                        )
                    )
    (DATA / "h264-paff-packets-oracle.json").write_text(
        json.dumps(dict(cases=cases), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
