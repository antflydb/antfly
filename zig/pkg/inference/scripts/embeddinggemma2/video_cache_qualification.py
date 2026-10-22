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
import copy
import base64
import json
import time
from pathlib import Path
import compare
import reference
import video_compare


def group(row):
    path = Path(row["path"])
    mime = (
        "video/quicktime"
        if path.suffix == ".mov"
        else "video/webm"
        if path.suffix == ".webm"
        else "video/mp4"
    )
    part = {
        "type": "media",
        "mime_type": mime,
        "data": base64.b64encode(path.read_bytes()).decode(),
    }
    if "sampling" in row:
        part["sampling"] = row["sampling"]
    return {"content": [part]}


def run(args):
    reference.verify_checkpoint(args.model_dir)
    data = video_compare.corpus(args.corpus)
    oracle = json.loads(args.reference.read_text())
    if oracle["status"] != "pass" or oracle["corpus_sha256"] != reference.sha256(
        args.corpus
    ):
        raise ValueError("completed oracle for the exact corpus required")
    rows = data["documents"]
    required = {
        "vfr-reference",
        "vfr-timestamps",
        "paff-reference",
        "paff-timestamps",
        "vp9-timestamps",
        "av1-timestamps",
        "sintel-window-0",
        "sintel-window-20",
    }
    if {r["id"] for r in rows} != required:
        raise ValueError("complete sampling corpus required")
    report = {
        "status": "running",
        "backend": args.backend,
        "binary_sha256": reference.sha256(args.binary),
        "tool_sha256": reference.sha256(Path(__file__)),
        "corpus_sha256": reference.sha256(args.corpus),
        "oracle_sha256": reference.sha256(args.reference),
        "checkpoint_receipt_sha256": reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        ),
        "cases": {},
        "limits": "Request-scoped projection reuse; timings are diagnostic, without a performance gate. Cache ownership/counters are qualified by native unit tests.",
    }
    bodies = {r["id"]: group(r) for r in rows}
    for name, source in (
        ("vfr-first", "vfr-timestamps"),
        ("paff-first", "paff-timestamps"),
        ("vp9-first", "vp9-timestamps"),
        ("av1-first", "av1-timestamps"),
    ):
        bodies[name] = copy.deepcopy(bodies[source])
        bodies[name]["content"][0]["sampling"]["max_frames"] = 1
    bodies["sintel-overlap"] = copy.deepcopy(bodies["sintel-window-0"])
    bodies["sintel-overlap"]["content"][0]["sampling"]["max_frames"] = 3
    names = [
        "vfr-first",
        "vfr-reference",
        "vfr-timestamps",
        "vfr-reference",
        "paff-first",
        "paff-reference",
        "paff-timestamps",
        "paff-reference",
        "vp9-first",
        "vp9-timestamps",
        "vp9-timestamps",
        "av1-first",
        "av1-timestamps",
        "av1-timestamps",
        "sintel-window-0",
        "sintel-overlap",
        "sintel-window-20",
        "sintel-window-0",
    ]
    multipart = [
        {"content": bodies["vfr-reference"]["content"] * 2},
        {
            "content": bodies["vp9-timestamps"]["content"]
            + bodies["vp9-first"]["content"]
        },
    ]
    baseline_args = copy.copy(args)
    baseline_args.binary = args.baseline_binary
    baseline_args.output = args.output.with_name(args.output.stem + "-uncached.json")
    baseline_args.url = None
    report["baseline_binary_sha256"] = reference.sha256(args.baseline_binary)
    baseline_vectors = {}
    with video_compare.server(baseline_args) as url:
        start = time.perf_counter()
        response = compare.post(
            url + "/ai/v1",
            "/embed",
            {"model": args.model_dir.name, "input": [bodies[n] for n in names]},
        )
        report["uncached_batch_completed_seconds"] = time.perf_counter() - start
        if len(response["data"]) != len(names):
            raise ValueError("uncached group count changed")
        report["baseline_model_identity"] = response["model_identity"]
        for index, (name, output) in enumerate(zip(names, response["data"])):
            if output["index"] != index:
                raise ValueError("uncached group order changed")
            baseline_vectors[name] = output["embedding"]
        multi_reference = compare.post(
            url + "/ai/v1", "/embed", {"model": args.model_dir.name, "input": multipart}
        )["data"]
    report["baseline_server_memory"] = baseline_args.server_memory
    args.url = None
    with video_compare.server(args) as url:
        identity = None
        singles = {}
        for row in rows:
            response = compare.post(
                url + "/ai/v1",
                "/embed",
                {"model": args.model_dir.name, "input": [bodies[row["id"]]]},
            )
            if identity not in (None, response["model_identity"]):
                raise ValueError("model identity changed")
            identity = response["model_identity"]
            vector = response["data"][0]["embedding"]
            singles[row["id"]] = vector
            report["cases"][row["id"]] = {
                "standalone_pytorch": compare.assert_agreement(
                    vector, oracle["cases"][row["id"]]["vector"]
                )
            }
        # Later groups select both cached and missing logical pictures. Repeated
        # groups follow those mixed hits so wrapper teardown is also exercised.
        start = time.perf_counter()
        response = compare.post(
            url + "/ai/v1",
            "/embed",
            {
                "model": args.model_dir.name,
                "model_identity": identity,
                "input": [bodies[n] for n in names],
            },
        )
        if response["model_identity"] != identity or len(response["data"]) != len(
            names
        ):
            raise ValueError("model identity or group count changed")
        report["batch_completed_seconds"] = time.perf_counter() - start
        report["ordered_batch"] = []
        for index, (name, output) in enumerate(zip(names, response["data"])):
            if output["index"] != index:
                raise ValueError("group order changed")
            vector = output["embedding"]
            evidence = {
                "index": index,
                "case": name,
                "uncached": compare.assert_agreement(vector, baseline_vectors[name]),
            }
            if name in singles:
                evidence["standalone"] = compare.assert_agreement(vector, singles[name])
                evidence["pytorch"] = compare.assert_agreement(
                    vector, oracle["cases"][name]["vector"]
                )
            report["ordered_batch"].append(evidence)
        response_multi = compare.post(
            url + "/ai/v1",
            "/embed",
            {
                "model": args.model_dir.name,
                "model_identity": identity,
                "input": multipart,
            },
        )["data"]
        if len(response_multi) != len(multi_reference):
            raise ValueError("multipart group count changed")
        report["multipart"] = [
            compare.assert_agreement(actual["embedding"], expected["embedding"])
            for actual, expected in zip(response_multi, multi_reference)
        ]
        retrieval_vectors = dict(baseline_vectors)
        for row in data["queries"]:
            response_query = compare.post(
                url + "/ai/v1",
                "/embed",
                {
                    "model": args.model_dir.name,
                    "model_identity": identity,
                    "task_type": "RETRIEVAL_QUERY",
                    "input": [row["text"]],
                },
            )
            retrieval_vectors[row["id"]] = response_query["data"][0]["embedding"]
            compare.assert_agreement(
                retrieval_vectors[row["id"]], oracle["cases"][row["id"]]["vector"]
            )
        for name, output in zip(names, response["data"]):
            retrieval_vectors[name] = output["embedding"]
        actual_retrieval = video_compare.retrieval(data, retrieval_vectors)
        expected_retrieval = video_compare.retrieval(
            data, {name: value["vector"] for name, value in oracle["cases"].items()}
        )
        score_error = max(
            abs(actual_retrieval["scores"][q][d] - expected_retrieval["scores"][q][d])
            for q in actual_retrieval["scores"]
            for d in actual_retrieval["scores"][q]
        )
        if score_error > 2e-4:
            raise ValueError("retrieval cosine score drift")
        report["retrieval"] = {
            "max_score_error": score_error,
            "rankings": video_compare.rank_agreement(
                actual_retrieval, expected_retrieval, (1, 3, 5), 2e-4
            ),
        }
        # Independent HTTP requests must own independent caches and vectors.
        retry = compare.post(
            url + "/ai/v1",
            "/embed",
            {
                "model": args.model_dir.name,
                "model_identity": identity,
                "input": [bodies["vfr-timestamps"]],
            },
        )
        report["independent_request"] = compare.assert_agreement(
            retry["data"][0]["embedding"], singles["vfr-timestamps"]
        )
        report["model_identity"] = identity
    report["server_memory"] = args.server_memory
    report["status"] = "pass"
    compare.publish(args.output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("native", "metal"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
