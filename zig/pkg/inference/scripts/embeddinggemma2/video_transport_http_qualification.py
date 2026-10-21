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

"""Check native TS HTTP failure classification without publishing partial output."""

import argparse
import base64
import json
from pathlib import Path
import urllib.error

import compare
import reference
import video_compare


def corrupt_pat(source):
    data = bytearray(source)
    if len(data) % 188:
        raise ValueError("unaligned fixture")
    for offset in range(0, len(data), 188):
        if data[offset] != 0x47:
            raise ValueError("invalid fixture sync")
        pid = ((data[offset + 1] & 31) << 8) | data[offset + 2]
        if pid != 0 or not data[offset + 1] & 64:
            continue
        cursor = offset + 4
        if data[offset + 3] & 32:
            cursor += 1 + data[offset + 4]
        section = cursor + 1 + data[cursor]
        if section + 6 >= offset + 188:
            raise ValueError("fixture PAT crosses packet")
        data[section + 5] ^= 1
        return bytes(data)
    raise ValueError("fixture PAT absent")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("native", "metal"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.url = None
    reference.verify_checkpoint(args.model_dir)
    data = corrupt_pat(args.fixture.read_bytes())
    report = dict(
        status="running",
        backend=args.backend,
        binary_sha256=reference.sha256(args.binary),
        fixture_sha256=reference.sha256(args.fixture),
        tool_sha256=reference.sha256(Path(__file__)),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with video_compare.server(args) as url:
        body = {
            "model": args.model_dir.name,
            "input": [
                {
                    "content": [
                        {
                            "type": "media",
                            "mime_type": "video/mp2t",
                            "data": base64.b64encode(data).decode(),
                        }
                    ]
                }
            ],
        }
        try:
            compare.post(url + "/ai/v1", "/embed", body)
        except urllib.error.HTTPError as error:
            response = json.loads(error.read())
            if (
                error.code != 400
                or response.get("error") != "INVALID_REQUEST"
                or response.get("message") != "InvalidTransportCrc"
            ):
                raise AssertionError((error.code, response)) from error
            if "data" in response:
                raise AssertionError("corrupt input published embedding data")
            report.update(status="pass", http_status=error.code, response=response)
        else:
            raise AssertionError("corrupt PAT unexpectedly accepted")
    compare.publish(args.output, report)


if __name__ == "__main__":
    main()
