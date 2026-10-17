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

"""Exercise declared capacity retry on an owned pretrained video server.

The existing test-only admission hook forces one run denial. Real admission
limits remain enabled. This probe complements hard-disconnect qualification.
"""

import argparse
import base64
import json
from pathlib import Path

if __package__:
    from . import compare, reference, video_compare as video
else:
    import compare
    import reference
    import video_compare as video


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--backend", choices=["native", "metal"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--process-memory-budget-mb", type=int, default=4096)
    args = parser.parse_args()
    if not 1 <= args.process_memory_budget_mb <= 65536:
        parser.error("process envelope must be between 1 and 65536 MiB")
    args.url = None
    args.verify_capacity_retry = True
    args.output.parent.mkdir(parents=True, exist_ok=True)
    data = video.corpus(args.corpus)
    expected = json.loads(args.reference.read_text())
    reference.verify_checkpoint(args.model_dir)
    if (
        expected["status"] != "pass"
        or expected["corpus_sha256"] != reference.sha256(args.corpus)
        or expected["checkpoint_receipt_sha256"]
        != reference.sha256(args.model_dir / "embeddinggemma2_receipt.json")
    ):
        raise ValueError("reference/checkpoint/corpus mismatch")
    row = data["documents"][0]
    body = {
        "model": args.model_dir.name,
        "task_type": "RETRIEVAL_DOCUMENT",
        "input": [
            {
                "content": [
                    {
                        "type": "media",
                        "mime_type": "video/webm",
                        "data": base64.b64encode(
                            Path(row["path"]).read_bytes()
                        ).decode(),
                    }
                ]
            }
        ],
    }
    result = dict(
        status="running",
        backend=args.backend,
        binary_sha256=reference.sha256(args.binary),
        tool_sha256=reference.sha256(Path(__file__)),
        retry_tool_sha256=reference.sha256(Path(video.__file__)),
        corpus_sha256=reference.sha256(args.corpus),
        reference_sha256=reference.sha256(args.reference),
        case=row["id"],
        configured_forced_run_denials=1,
        process_memory_budget_mb=args.process_memory_budget_mb,
    )
    try:
        with video.server(args) as url:
            response, attempts = video.post_capacity_retry(url, body)
            result["denials"] = attempts
            if not attempts:
                raise ValueError("test-only run admission denial was not observed")
            vector = response["data"][0]["embedding"]
            result["vector"] = vector
            result["agreement"] = compare.agreement(
                vector, expected["cases"][row["id"]]["vector"]
            )
            compare.assert_agreement(vector, expected["cases"][row["id"]]["vector"])
            result["model_identity"] = response["model_identity"]
            result["metrics_after"] = video.embedding_metrics(url)
            if result["metrics_after"]["active"] or result["metrics_after"]["admitted"]:
                raise ValueError(
                    "video request retained admission after successful retry"
                )
        result["server_memory"] = getattr(args, "_server_memory", None)
        result["status"] = "pass"
    except Exception as error:
        result["status"] = "failed"
        result["failure"] = video.failure_details(error)
        compare.publish(args.output, result)
        raise
    compare.publish(args.output, result)


if __name__ == "__main__":
    main()
