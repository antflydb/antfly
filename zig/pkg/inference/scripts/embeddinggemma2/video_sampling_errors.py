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

"""Qualify timestamp-policy HTTP rejection, retry and separate window outputs."""

import argparse
import base64
import copy
import json
from pathlib import Path
import urllib.error

import compare
import reference
import video_compare


def group(row):
    part = {
        "type": "media",
        "mime_type": "video/quicktime" if row["path"].endswith(".mov") else "video/mp4",
        "data": base64.b64encode(Path(row["path"]).read_bytes()).decode(),
    }
    if "sampling" in row:
        part["sampling"] = row["sampling"]
    return {"content": [part]}


def run(args):
    data = video_compare.corpus(args.corpus)
    oracle = json.loads(args.reference.read_text())
    if oracle["status"] != "pass" or oracle["corpus_sha256"] != reference.sha256(
        args.corpus
    ):
        raise ValueError("matching completed pretrained oracle required")
    report = {
        "status": "running",
        "backend": args.backend,
        "binary_sha256": reference.sha256(args.binary),
        "tool_sha256": reference.sha256(Path(__file__)),
        "oracle_sha256": reference.sha256(args.reference),
        "cases": [],
    }
    args.url = None
    clean_row = next(row for row in data["documents"] if row["id"] == "vfr-timestamps")
    body = {"model": args.model_dir.name, "input": [group(clean_row)]}
    invalid = [
        {},
        {"policy": "reference", "step_ns": 1},
        {"policy": "timestamps", "step_ns": 0},
        {"policy": "timestamps", "start_ns": -1},
        {"policy": "timestamps", "start_ns": 2, "end_ns": 1},
        {"policy": "timestamps", "max_frames": 33},
        {"policy": "timestamps", "max_frames": True},
        {"policy": "timestamps", "overflow": "ignore"},
        {"policy": "timestamps", "end_ns": 9223372036854775808},
        {"policy": "timestamps", "unknown": 1},
        {"policy": "timestamps", "start_ns": 100_000_000_000},
    ]
    try:
        with video_compare.server(args) as url:
            for sampling in invalid:
                damaged = copy.deepcopy(body)
                damaged["input"][0]["content"][0]["sampling"] = sampling
                try:
                    compare.post(url + "/ai/v1", "/embed", damaged)
                except urllib.error.HTTPError as error:
                    detail = video_compare.failure_details(error)
                    if detail["http_status"] != 400:
                        raise AssertionError(
                            f"policy failure was not HTTP 400: {detail}"
                        )
                else:
                    raise AssertionError(f"invalid policy was accepted: {sampling}")
                response = compare.post(url + "/ai/v1", "/embed", body)
                metrics = compare.assert_agreement(
                    response["data"][0]["embedding"],
                    oracle["cases"][clean_row["id"]]["vector"],
                )
                report["cases"].append(
                    {"sampling": sampling, "rejected": detail, "clean_retry": metrics}
                )
            invalid_text = {
                "model": args.model_dir.name,
                "input": [
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": "A caption.",
                                "sampling": {"policy": "timestamps"},
                            }
                        ]
                    }
                ],
            }
            try:
                compare.post(url + "/ai/v1", "/embed", invalid_text)
            except urllib.error.HTTPError as error:
                detail = video_compare.failure_details(error)
                if detail["http_status"] != 400:
                    raise AssertionError("sampling on text did not produce HTTP 400")
            else:
                raise AssertionError("sampling on text was accepted")
            report["non_video_rejected"] = detail
            windows = [
                row
                for row in data["documents"]
                if row["id"].startswith("sintel-window-")
            ]
            response = compare.post(
                url + "/ai/v1",
                "/embed",
                {
                    "model": args.model_dir.name,
                    "input": [group(row) for row in windows],
                },
            )
            if len(response["data"]) != len(windows):
                raise AssertionError(
                    "separate windows did not return separate embeddings"
                )
            report["windows"] = {
                row["id"]: compare.assert_agreement(
                    item["embedding"], oracle["cases"][row["id"]]["vector"]
                )
                for row, item in zip(windows, response["data"], strict=True)
            }
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
