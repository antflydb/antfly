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

"""Pinned PyTorch reference for every Sintel rolling/sparse live interval.

Sampling is relative to the independently decoded first presentation picture.
The native live report adds its 200 ms source-clock origin to these intervals.
FFmpeg and PyTorch remain offline qualification dependencies only.
"""

import argparse
from pathlib import Path
import types

import compare
import reference
import video_compare
import video_live_qualification


def inputs(fixture):
    suite, original = video_live_qualification.inputs(fixture)
    template = original["documents"][0]
    documents = []
    for name, count, advance in (
        ("sintel-6s", 6, 1_000_000_000),
        ("sintel-6s-sparse", 2, 3_000_000_000),
    ):
        for ordinal in range(count):
            start = ordinal * advance
            documents.append(
                {
                    **template,
                    "id": name + "/" + str(ordinal),
                    "sampling": {
                        "policy": "timestamps",
                        "start_ns": start,
                        "end_ns": min(start + 2_000_000_000, 6_000_000_000),
                    },
                }
            )
    return {"documents": documents, "queries": suite["queries"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).parent / "testdata/live-pretrained/fixture.json",
    )
    parser.add_argument("--backend", choices=("cpu", "mps"), required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.threads <= 16:
        raise ValueError("threads must be within 1..16")
    data = inputs(args.fixture)
    reference.verify_reference(args.model_dir)
    parameters = types.SimpleNamespace(
        model_dir=args.model_dir,
        backend=args.backend,
        threads=args.threads,
        warmups=0,
        iterations=1,
        output=args.output,
    )
    report = {"status": "running", "backend": args.backend, "cases": {}}
    try:
        video_compare.pytorch(parameters, data, report)
        expected = {row["id"] for row in data["documents"] + data["queries"]}
        if set(report["cases"]) != expected:
            raise ValueError("missing rolling reference case")
        for row in data["documents"]:
            tokens = 124 if row["id"] == "sintel-6s/5" else 246
            if report["cases"][row["id"]]["encoder_tokens"] != tokens:
                raise ValueError("rolling reference token count changed")
        report["retrieval"] = video_compare.retrieval(
            data, {name: row["vector"] for name, row in report["cases"].items()}
        )
        report["documents"] = data["documents"]
        report["fixture_metadata_sha256"] = reference.sha256(args.fixture)
        report["checkpoint_receipt_sha256"] = reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        )
        report["scope"] = (
            "All six rolling and two sparse Sintel intervals, including the clipped tail."
        )
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        compare.publish(args.output, report)


if __name__ == "__main__":
    main()
