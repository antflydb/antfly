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

"""Reject damaged WebM codec packets and qualify clean pretrained request retries."""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.error

import compare
import reference
import video_compare


def damage(path):
    packet = json.loads(
        subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_packets",
                "-show_data",
                "-of",
                "json",
                str(path),
            ],
            timeout=30,
        )
    )["packets"][0]
    coded = b"".join(
        bytes.fromhex(line.partition(":")[2][:40])
        for line in packet["data"].splitlines()
        if ":" in line
    )
    if len(coded) != int(packet["size"]):
        raise ValueError("reference packet dump size mismatch")
    raw = path.read_bytes()
    if not coded or raw.count(coded) != 1:
        raise ValueError("first coded packet must occur exactly once")
    offset = raw.index(coded)
    damaged = bytearray(raw)
    damaged[offset : offset + len(coded)] = bytes(len(coded))
    return bytes(damaged), dict(
        offset=offset, bytes=len(coded), packet_sha256=hashlib.sha256(coded).hexdigest()
    )


def run(args):
    oracle = json.loads(args.reference.read_text())
    if oracle["status"] != "pass":
        raise ValueError("complete pretrained oracle required")
    data = video_compare.corpus(args.corpus)
    if oracle["corpus_sha256"] != reference.sha256(args.corpus):
        raise ValueError("oracle corpus mismatch")
    report = dict(
        status="running",
        backend=args.backend,
        binary_sha256=reference.sha256(args.binary),
        tool_sha256=reference.sha256(Path(__file__)),
        oracle_sha256=reference.sha256(args.reference),
        cases={},
    )
    args.url = None
    try:
        with video_compare.server(args) as url:
            for row in data["documents"][:3]:
                damaged, mutation = damage(Path(row["path"]))
                body = dict(
                    model=args.model_dir.name,
                    input=[
                        dict(
                            content=[
                                dict(
                                    type="media",
                                    mime_type="video/webm",
                                    data=base64.b64encode(damaged).decode(),
                                )
                            ]
                        )
                    ],
                )
                try:
                    compare.post(url + "/ai/v1", "/embed", body)
                except urllib.error.HTTPError as error:
                    response = json.loads(error.read(65536))
                    if error.code != 400 or response["error"] not in (
                        "INVALID_VIDEO",
                        "INVALID_REQUEST",
                    ):
                        raise AssertionError(
                            f"corrupt coded packet response: {error.code} {response}"
                        )
                else:
                    raise AssertionError("damaged coded packet was accepted")
                body["input"][0]["content"][0]["data"] = base64.b64encode(
                    Path(row["path"]).read_bytes()
                ).decode()
                clean = compare.post(url + "/ai/v1", "/embed", body)
                metrics = compare.assert_agreement(
                    clean["data"][0]["embedding"], oracle["cases"][row["id"]]["vector"]
                )
                report["cases"][row["id"]] = dict(
                    mutation=mutation,
                    rejected=dict(status=400, code=response["error"]),
                    clean_retry=metrics,
                )
        report["status"] = "pass"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = video_compare.failure_details(error)
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["native", "metal"], required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
