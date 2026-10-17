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

"""Qualify VFR sampling and display rendering with pinned independent oracles.

Retiming preserves coded frames and the average-FPS sample indexes. Rotation and
SAR use separate positive pretrained references for the version-3 rendering
policy. PQ-tagged MJPEG remains rejected because its RGB decoder is SDR-only.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import struct
import urllib.error

import compare
import reference
import video_compare

CONTAINERS = {b"moov", b"trak", b"mdia", b"minf", b"stbl"}


def boxes(data):
    offset = 0
    while offset < len(data):
        if len(data) - offset < 8:
            raise ValueError("truncated fixture box")
        size, kind = struct.unpack_from(">I4s", data, offset)
        if size < 8 or size > len(data) - offset:
            raise ValueError("unsupported fixture box size")
        yield kind, data[offset + 8 : offset + size]
        offset += size


def box(kind, payload):
    return struct.pack(">I4s", len(payload) + 8, kind) + payload


def rewrite(data, target, transform):
    matches = 0

    def visit(encoded):
        nonlocal matches
        output = bytearray()
        for kind, payload in boxes(encoded):
            if kind == target:
                matches += 1
                payload = transform(payload)
            elif kind in CONTAINERS:
                payload = visit(payload)
            output.extend(box(kind, payload))
        return bytes(output)

    result = visit(data)
    if matches != 1:
        raise ValueError("qualification requires one video track/target box")
    # These pinned files have metadata after media: rebuilding moov cannot move
    # compressed samples or change stco/co64 addresses.
    original = list(boxes(data))
    modified = list(boxes(result))
    if [k for k, _ in original] != [k for k, _ in modified]:
        raise ValueError("fixture top-level boxes changed")
    if next(i for i, (k, _) in enumerate(original) if k == b"moov") < next(
        i for i, (k, _) in enumerate(original) if k == b"mdat"
    ):
        raise ValueError("fixture metadata must follow compressed media")
    if [p for k, p in original if k == b"mdat"] != [
        p for k, p in modified if k == b"mdat"
    ]:
        raise ValueError("compressed frame bytes changed")
    return result


def variants(encoded):
    def retime(payload):
        if payload[:4] != bytes(4) or len(payload) != 16:
            raise ValueError("expected version-0 uniform stts")
        entries, count, duration = struct.unpack_from(">III", payload, 4)
        if (entries, count, duration) != (1, 8, 4096):
            raise ValueError("pinned fixture clock changed")
        durations = [1024, 1024, 1024, 1024, 8192, 8192, 6144, 6144]
        assert sum(durations) == count * duration
        return (
            bytes(4)
            + struct.pack(">I", count)
            + b"".join(struct.pack(">II", 1, value) for value in durations)
        )

    def rotate(payload):
        if payload[0] != 0 or len(payload) != 84:
            raise ValueError("expected pinned version-0 tkhd")
        output = bytearray(payload)
        struct.pack_into(">9i", output, 40, 0, 65536, 0, -65536, 0, 0, 0, 0, 1 << 30)
        return bytes(output)

    def sample_entry(payload, *, aspect=False, hdr=False):
        if payload[:8] != bytes(4) + struct.pack(">I", 1):
            raise ValueError("expected one sample description")
        entries = list(boxes(payload[8:]))
        kind, sample = entries[0]
        if kind != b"jpeg" or len(sample) < 78:
            raise ValueError("expected pinned MJPEG visual description")
        children = list(boxes(sample[78:]))
        found = False
        output = bytearray(sample[:78])
        for child, value in children:
            if child == b"pasp" and aspect:
                found = True
                value = struct.pack(">II", 2, 1)
            output.extend(box(child, value))
        if aspect and not found:
            raise ValueError("expected pinned square pixel aspect")
        if hdr:
            output.extend(box(b"colr", b"nclx" + struct.pack(">HHHB", 1, 16, 6, 128)))
        return payload[:8] + box(kind, output)

    return {
        "vfr_average_fps": (rewrite(encoded, b"stts", retime), None),
        "rotation_90": (rewrite(encoded, b"tkhd", rotate), None),
        "non_square_pixels": (
            rewrite(encoded, b"stsd", lambda p: sample_entry(p, aspect=True)),
            None,
        ),
        "pq_hdr": (
            rewrite(encoded, b"stsd", lambda p: sample_entry(p, hdr=True)),
            "UnsupportedVideoColor",
        ),
    }


def run(args):
    reference.verify_checkpoint(args.model_dir)
    oracle = json.loads(args.oracle.read_text())
    if oracle["revision"] != reference.REVISION:
        raise ValueError("unqualified reference")
    asset = next(a for a in oracle["assets"] if a["path"] == "video-mjpeg.mov")
    path = args.oracle.parent / asset["path"]
    if reference.sha256(path) != asset["sha256"]:
        raise ValueError("pinned pixel source changed")
    expected = next(c for c in oracle["cases"] if c["name"] == "mjpeg_video")
    if expected["selected_frames"] != [[0, 4]]:
        raise ValueError("pinned sample indexes changed")
    rendering = json.loads(args.render_oracle.read_text())
    if (
        rendering.get("status") != "pass"
        or rendering.get("recipe") != "embeddinggemma2-f32-mean-v3-display-hdr"
        or rendering.get("revision") != reference.REVISION
    ):
        raise ValueError("unqualified display rendering oracle")
    report = dict(
        status="running",
        backend=args.backend,
        binary_sha256=reference.sha256(args.binary),
        oracle_sha256=reference.sha256(args.oracle),
        render_oracle_sha256=reference.sha256(args.render_oracle),
        source_sha256=asset["sha256"],
        platform=video_compare.conditions(),
        cases={},
        scope=__doc__,
    )
    args.url = None
    try:
        with video_compare.server(args) as url:
            for name, (encoded, rejection) in variants(path.read_bytes()).items():
                body = dict(
                    model=args.model_dir.name,
                    input=[
                        dict(
                            content=[
                                dict(
                                    type="media",
                                    mime_type="video/quicktime",
                                    data=base64.b64encode(encoded).decode(),
                                )
                            ]
                        )
                    ],
                )
                row = dict(sha256=hashlib.sha256(encoded).hexdigest())
                try:
                    response = compare.post(url + "/ai/v1", "/embed", body)
                except urllib.error.HTTPError as error:
                    if rejection is None:
                        raise
                    failure = video_compare.failure_details(error)
                    if error.code != 400 or rejection not in failure.get(
                        "response_body", ""
                    ):
                        raise AssertionError(
                            f"wrong display rejection: {failure}"
                        ) from error
                    row["rejection"] = failure
                else:
                    if rejection is not None:
                        raise AssertionError(
                            f"unqualified display input accepted: {name}"
                        )
                    display_case = {
                        "rotation_90": "rotation_90",
                        "non_square_pixels": "sar_2_1",
                    }.get(name)
                    positive = (
                        rendering["cases"][display_case] if display_case else expected
                    )
                    if display_case and positive["sha256"] != row["sha256"]:
                        raise ValueError("display rendering fixture changed")
                    row["agreement"] = compare.assert_agreement(
                        response["data"][0]["embedding"], positive["embedding"]
                    )
                    prompt_tokens = (
                        positive["prompt_tokens"]
                        if display_case
                        else len(expected["token_ids"])
                    )
                    if response["usage"]["prompt_tokens"] != prompt_tokens:
                        raise AssertionError("VFR sampling changed model token count")
                    row["sampled_frame_indexes"] = [0, 4]
                report["cases"][name] = row
                compare.publish(args.output, report)
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = video_compare.failure_details(error)
        raise
    finally:
        if hasattr(args, "server_memory"):
            report["server_memory"] = args.server_memory
        compare.publish(args.output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--backend", choices=["native", "metal"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--oracle",
        type=Path,
        default=Path(__file__).parent / "testdata/video/video-oracle.json",
    )
    parser.add_argument(
        "--render-oracle",
        type=Path,
        default=Path(__file__).parent / "testdata/render/pytorch-cpu.json",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
