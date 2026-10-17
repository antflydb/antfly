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

"""Pretrained preservation for repeated/overlapping video groups in one request."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import types
import compare
import reference
import video_compare


def inputs(fixture):
    metadata = json.loads(fixture.read_text())
    clip = fixture.parent / metadata["path"]
    if reference.sha256(clip) != metadata["sha256"]:
        raise ValueError("fixture changed")
    documents = [
        {
            "id": "sintel-6s/" + str(i),
            "path": str(clip.resolve()),
            "sha256": metadata["sha256"],
            "provenance": metadata,
            "sampling": {
                "policy": "timestamps",
                "start_ns": i * 2_000_000_000,
                "end_ns": (i + 1) * 2_000_000_000,
            },
        }
        for i in range(3)
    ]
    queries = [
        {"id": "dragon", "text": "A dragon and a young woman."},
        {"id": "landscape", "text": "A wide landscape with mountains."},
    ]
    suite = {
        "cases": [
            {
                "id": "sintel-6s",
                "path": str(clip.resolve()),
                "init_end": metadata["init_end"],
                "fragment_ends": metadata["fragment_ends"],
            }
        ],
        "queries": queries,
    }
    return suite, {"documents": documents, "queries": queries}


def validate(report, oracle):
    if report.get("status") != "pass" or oracle.get("status") != "pass":
        raise ValueError("completed native and oracle reports required")
    if len(report["cases"]) != 3 or len(report["queries"]) != 2:
        raise ValueError("all windows and queries required")
    vectors = {row["id"]: row["vector"] for row in report["queries"]}
    maximum = 0.0
    for i, row in enumerate(report["cases"]):
        name = "sintel-6s/" + str(i)
        if row["case"] != "sintel-6s" or row["ordinal"] != i or row["epoch"] != 0:
            raise ValueError("window identity changed")
        if (
            row["start_ns"] != 200_000_000 + i * 2_000_000_000
            or row["end_ns"] != 200_000_000 + (i + 1) * 2_000_000_000
        ):
            raise ValueError("source interval changed")
        if row["input_tokens"] != oracle["cases"][name]["encoder_tokens"]:
            raise ValueError("pretrained encoder token count changed")
        metric = compare.assert_agreement(
            row["vector"], oracle["cases"][name]["vector"]
        )
        maximum = max(maximum, metric["max_abs"])
        if row["vector"] != row["baseline_vector"] or row["max_abs"] != 0:
            raise ValueError("stream/complete-clip vector drift")
        vectors[name] = row["vector"]
    for row in report["queries"]:
        compare.assert_agreement(row["vector"], oracle["cases"][row["id"]]["vector"])
    data = {
        "documents": [{"id": "sintel-6s/" + str(i)} for i in range(3)],
        "queries": [{"id": "dragon"}, {"id": "landscape"}],
    }
    actual = video_compare.retrieval(data, vectors)
    expected = video_compare.retrieval(
        data, {k: v["vector"] for k, v in oracle["cases"].items()}
    )
    # Exact ranking and score preservation; no application-relevance labels.
    agreement = video_compare.rank_agreement(actual, expected, (1, 3), 1e-5)
    if not agreement["pass"]:
        raise ValueError("reference ranking/score drift")
    return {
        "max_reference_abs": maximum,
        "retrieval": actual,
        "reference_retrieval": expected,
        "stream_baseline_exact": True,
        "retrieval_agreement": agreement,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).parent / "testdata/live-pretrained/fixture.json",
    )
    parser.add_argument(
        "--backend", choices=("native", "metal", "cpu", "mps"), required=True
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference.verify_checkpoint(args.model_dir)
    suite, data = inputs(args.fixture)
    if args.backend in ("cpu", "mps"):
        parameters = types.SimpleNamespace(
            model_dir=args.model_dir,
            backend=args.backend,
            threads=4,
            warmups=0,
            iterations=1,
            output=args.output,
        )
        report = {"status": "running", "backend": args.backend, "cases": {}}
        video_compare.pytorch(parameters, data, report)
        report["status"] = "pass"
        report["checkpoint_receipt_sha256"] = reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        )
        compare.publish(args.output, report)
        return
    if args.binary is None or args.reference is None:
        raise ValueError("native/Metal require binary and reference")
    suite_file = args.output.with_suffix(".suite.json")
    suite_file.write_text(json.dumps(suite, indent=2) + "\n")
    raw_file = args.output.with_suffix(".native.json")
    command = [
        str(args.binary.resolve()),
        "--model",
        str(args.model_dir.resolve()),
        "--suite",
        str(suite_file.resolve()),
        "--backend",
        args.backend,
        "--output",
        str(raw_file.resolve()),
    ]
    process = subprocess.Popen(command, start_new_session=True)
    try:
        exit_code = process.wait(timeout=900)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        raise
    if exit_code:
        raise RuntimeError(f"qualification binary exited {exit_code}")
    raw = json.loads(raw_file.read_text())
    oracle = json.loads(args.reference.read_text())
    evidence = validate(raw, oracle)
    report = {
        "status": "pass",
        "backend": args.backend,
        "qualification": evidence,
        "binary_sha256": reference.sha256(args.binary),
        "fixture_metadata_sha256": reference.sha256(args.fixture),
        "checkpoint_receipt_sha256": reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        ),
        "oracle_sha256": reference.sha256(args.reference),
        "raw": raw,
    }
    compare.publish(args.output, report)


if __name__ == "__main__":
    main()
