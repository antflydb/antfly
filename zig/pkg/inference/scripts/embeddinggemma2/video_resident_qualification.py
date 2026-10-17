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

"""Pretrained reduced-dimension parity for resident video and flat text inputs."""

import argparse
import base64
import json
import math
from pathlib import Path

import compare
import reference
import video_compare


def body(model, row, dimensions):
    if "text" in row:
        return {
            "model": model,
            "input": [row["text"]],
            "task_type": "RETRIEVAL_QUERY",
            "dimensions": dimensions,
        }
    part = {
        "type": "media",
        "mime_type": "video/quicktime"
        if row["path"].endswith(".mov")
        else "video/webm"
        if row["path"].endswith(".webm")
        else "video/mp4",
        "data": base64.b64encode(Path(row["path"]).read_bytes()).decode(),
    }
    if "sampling" in row:
        part["sampling"] = row["sampling"]
    return {"model": model, "input": [{"content": [part]}], "dimensions": dimensions}


def run(args):
    reference.verify_checkpoint(args.model_dir)
    data = video_compare.corpus(args.corpus)
    required = {"vfr-timestamps", "paff-timestamps", "sintel-window-0"}
    if not required.issubset({row["id"] for row in data["documents"]}):
        raise ValueError(
            "resident qualification requires VFR, PAFF and long-window inputs"
        )
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
        "checkpoint_receipt_sha256": reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        ),
        "cases": {},
    }
    args.url = None
    try:
        with video_compare.server(args) as url:
            rows = data["queries"] + [
                row
                for row in data["documents"]
                if row["id"] in ("vfr-timestamps", "paff-timestamps", "sintel-window-0")
            ]
            for row in rows:
                full = oracle["cases"][row["id"]]["vector"]
                for dimensions in (128, 256, 512, 768):
                    denominator = math.sqrt(sum(x * x for x in full[:dimensions]))
                    expected = [x / denominator for x in full[:dimensions]]
                    response = compare.post(
                        url + "/ai/v1",
                        "/embed",
                        body(args.model_dir.name, row, dimensions),
                    )
                    if report.get("model_identity") not in (
                        None,
                        response["model_identity"],
                    ):
                        raise ValueError(
                            "model identity changed between dimension requests"
                        )
                    actual = response["data"][0]["embedding"]
                    if len(actual) != dimensions:
                        raise AssertionError(
                            "requested dimension did not reach final output"
                        )
                    report["cases"][f"{row['id']}/{dimensions}"] = (
                        compare.assert_agreement(actual, expected)
                    )
                    report["model_identity"] = response["model_identity"]
            # Restore full dimensions after several reduced requests on the same
            # resident model; per-request dimensions must not leak across calls.
            row = data["queries"][0]
            request = body(args.model_dir.name, row, 768)
            del request["dimensions"]
            actual = compare.post(url + "/ai/v1", "/embed", request)["data"][0][
                "embedding"
            ]
            report["default_dimension_retry"] = compare.assert_agreement(
                actual, oracle["cases"][row["id"]]["vector"]
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
