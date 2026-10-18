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

"""Bind published Decide benchmark inputs to the decoded-GGUF reference logits.

Use benchmark-cases.json and validation.json from the same immutable Hugging
Face bundle revision. This keeps the large oracle/evidence files outside Git.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path


def prepare(cases, validation, reference):
    outputs = validation["outputs"][reference]
    by_id = {row["id"]: row for row in outputs}
    if len(by_id) != len(outputs) or len({row["id"] for row in cases}) != len(cases):
        raise ValueError("duplicate reference or benchmark case")
    if set(by_id) != {row["id"] for row in cases}:
        raise ValueError("reference and benchmark cases differ")
    result = []
    for case in cases:
        source = by_id[case["id"]]
        if len(case["input_ids"]) != source["tokens"]:
            raise ValueError(f"{case['id']}: token count mismatch")
        tasks = json.loads(case["schema_json"])["classifications"]
        if set(source["logits"]) != {task["name"] for task in tasks}:
            raise ValueError(f"{case['id']}: classification tasks differ")
        rows = []
        for task in tasks:
            logits = source["logits"][task["name"]]
            if set(logits) != set(task["labels"]):
                raise ValueError(f"{case['id']}: classification labels differ")
            values = [logits[label] for label in task["labels"]]
            if not all(math.isfinite(value) for value in values):
                raise ValueError(f"{case['id']}: non-finite reference logit")
            rows.append(values)
        result.append({**case, "logits": rows})
    return result


WINDOW_REFERENCE_SHA256 = (
    "a5cff9fd74c25a93c8367490218170c94c3c47f6edcc035a2db356202f8dad81"
)


def prepare_windows(data):
    """Single-window upstream logits; multi-window averages are not raw logits."""
    if hashlib.sha256(data).hexdigest() != WINDOW_REFERENCE_SHA256:
        raise ValueError("window reference differs from the pinned upstream capture")
    capture = json.loads(data)
    result = []
    for case in capture["cases"]:
        if len(case["windows"]) != 1:
            continue
        window = case["windows"][0]
        tasks = json.loads(case["native_schema_json"])["classifications"]
        logits = case["classification_logits"]
        if len(tasks) != 1 or len(logits) != len(tasks[0]["labels"]):
            raise ValueError("unexpected window classification geometry")
        result.append(
            {
                "id": case["id"],
                "text": window["text"],
                "schema_json": case["native_schema_json"],
                "input_ids": window["input_ids"],
                "logits": [logits],
            }
        )
    if not any(len(case["input_ids"]) == 1024 for case in result):
        raise ValueError("missing upstream 1024-token boundary")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--validation", type=Path, required=True)
    parser.add_argument("--reference", choices=("fp32", "q8_0"), default="q8_0")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--window-reference",
        type=Path,
        help="Append the pinned single-window upstream context probes (FP32 reference only)",
    )
    args = parser.parse_args()
    result = prepare(
        json.loads(args.cases.read_text()),
        json.loads(args.validation.read_text()),
        args.reference,
    )
    if args.window_reference:
        if args.reference != "fp32":
            parser.error("window context probes require --reference fp32")
        result += prepare_windows(args.window_reference.read_bytes())
    args.output.write_text(json.dumps(result, ensure_ascii=False) + "\n")
    print(f"Wrote {len(result)} {args.reference} reference cases to {args.output}")


if __name__ == "__main__":
    main()
