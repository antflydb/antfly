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

"""Run pinned pretrained video cases through a production native/Metal server."""

import argparse
import base64
import json
from pathlib import Path
import time
import urllib.error

import compare
import reference
import video_compare


def run(args):
    reference.verify_checkpoint(args.model_dir)
    oracle = json.loads(args.oracle.read_text())
    if oracle["revision"] != reference.REVISION or oracle["precision"] != "float32":
        raise ValueError("unqualified oracle")
    for asset in oracle["assets"]:
        if reference.sha256(args.oracle.parent / asset["path"]) != asset["sha256"]:
            raise ValueError("oracle asset changed")
    report = dict(
        status="running",
        backend=args.backend,
        platform=video_compare.conditions(),
        binary_sha256=reference.sha256(args.binary),
        oracle_sha256=reference.sha256(args.oracle),
        cases={},
    )
    identity = None
    args.url = None
    try:
        with video_compare.server(args) as url:
            for case in oracle["cases"]:
                if args.case and case["name"] != args.case:
                    continue
                content = []
                for path in case["videos"]:
                    content.append(
                        dict(
                            type="media",
                            mime_type="video/quicktime"
                            if path.endswith(".mov")
                            else "video/mp4",
                            data=base64.b64encode(
                                (args.oracle.parent / path).read_bytes()
                            ).decode(),
                        )
                    )
                if case["name"] == "two_videos":
                    content.insert(1, dict(type="text", text="Compare these clips."))
                if case["name"] == "text_video":
                    content.insert(0, dict(type="text", text="Describe the action."))
                elif case["name"] == "video_text":
                    content.append(dict(type="text", text="Describe the action."))
                body = dict(model=args.model_dir.name, input=[dict(content=content)])
                if identity:
                    body["model_identity"] = identity
                start = time.perf_counter()
                if args.verify_capacity_retry and not report["cases"]:
                    try:
                        compare.post(url + "/ai/v1", "/embed", body)
                    except urllib.error.HTTPError as error:
                        if (
                            error.code != 503
                            or error.headers.get("Retry-After") is None
                        ):
                            raise AssertionError(
                                "admission denial must return 503 with Retry-After"
                            ) from error
                        report["capacity_denial"] = dict(
                            status=error.code,
                            retry_after=error.headers["Retry-After"],
                            body=error.read(65536).decode("utf-8", errors="replace"),
                        )
                    else:
                        raise AssertionError(
                            "injected admission denial was not observed"
                        )
                response = compare.post(url + "/ai/v1", "/embed", body)
                elapsed = time.perf_counter() - start
                if identity and identity != response["model_identity"]:
                    raise ValueError("model identity changed")
                identity = response["model_identity"]
                if response["usage"]["prompt_tokens"] != len(case["token_ids"]):
                    raise ValueError("oracle token count differs")
                vector = response["data"][0]["embedding"]
                metrics = compare.assert_agreement(vector, case["embedding"])
                report["cases"][case["name"]] = dict(agreement=metrics, seconds=elapsed)
                if case["name"] == "text_video":
                    body["dimensions"] = 128
                    truncated = compare.post(url + "/ai/v1", "/embed", body)["data"][0][
                        "embedding"
                    ]
                    denominator = sum(x * x for x in vector[:128]) ** 0.5
                    compare.assert_agreement(
                        truncated, [x / denominator for x in vector[:128]]
                    )
                    report["http_128_dimensions"] = "pass"
                if args.verify_decode_retry and case["name"] == "mjpeg_video":
                    # This verified fixture contains one JPEG SOI per container
                    # sample. Break the second selected picture after a valid
                    # first preparation can have entered the projection queue.
                    broken = bytearray(
                        (args.oracle.parent / case["videos"][0]).read_bytes()
                    )
                    starts = [
                        i
                        for i in range(len(broken) - 1)
                        if broken[i : i + 2] == b"\xff\xd8"
                    ]
                    asset = next(
                        item
                        for item in oracle["assets"]
                        if item["path"] == case["videos"][0]
                    )
                    selected = case["selected_frames"][0]
                    if (
                        len(starts) != asset["frames"]
                        or len(selected) < 2
                        or selected[1] <= selected[0]
                    ):
                        raise ValueError(
                            "decode retry requires the pinned progressive MJPEG fixture"
                        )
                    position = starts[selected[1]]
                    broken[position : position + 2] = b"\x00\x00"
                    broken_content = [
                        dict(content[0], data=base64.b64encode(broken).decode())
                    ]
                    malformed = dict(body, input=[dict(content=broken_content)])
                    try:
                        compare.post(url + "/ai/v1", "/embed", malformed)
                    except urllib.error.HTTPError as error:
                        if error.code != 400:
                            raise AssertionError(
                                "a malformed decoded frame must return HTTP 400"
                            ) from error
                        report["decode_failure"] = video_compare.failure_details(error)
                    else:
                        raise AssertionError("corrupted selected JPEG was accepted")
                    retry = compare.post(url + "/ai/v1", "/embed", body)
                    report["decode_failure_retry"] = compare.assert_agreement(
                        retry["data"][0]["embedding"], case["embedding"]
                    )
                    report["decode_failure_frame"] = selected[1]
                compare.publish(args.output, report)
                print(case["name"], elapsed, flush=True)
        report["scope"] = (
            "single selected case"
            if args.case
            else "all six pinned cases plus HTTP 128 dimensions"
        )
        report["model_identity"] = identity
        report["status"] = "pass"
    except Exception as error:
        report["failure"] = video_compare.failure_details(error)
        raise
    finally:
        if hasattr(args, "server_memory"):
            report["server_memory"] = args.server_memory
        if report["status"] != "pass":
            report["status"] = "failed"
        compare.publish(args.output, report)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--backend", choices=["native", "metal"], default="native")
    p.add_argument("--model-dir", type=Path, required=True)
    p.add_argument(
        "--oracle",
        type=Path,
        default=Path(__file__).parent / "testdata/video/video-oracle.json",
    )
    p.add_argument(
        "--case",
        choices=[
            "mjpeg_video",
            "h264_video",
            "text_video",
            "video_text",
            "repeated_video",
            "two_videos",
        ],
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument(
        "--verify-capacity-retry",
        action="store_true",
        help="Inject one execution admission denial and require HTTP 503/Retry-After before retrying the pinned case",
    )
    p.add_argument(
        "--verify-decode-retry",
        action="store_true",
        help="Corrupt the second selected MJPEG frame, require HTTP 400 and verify a clean retry",
    )
    args = p.parse_args()
    if args.verify_decode_retry and args.case not in (None, "mjpeg_video"):
        p.error("--verify-decode-retry requires the MJPEG case")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
