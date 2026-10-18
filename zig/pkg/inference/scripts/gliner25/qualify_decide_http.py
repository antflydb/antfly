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

"""Replay independent classification references through a resident HTTP server."""

import argparse
import copy
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


def post(url, body):
    started = time.monotonic()
    request = urllib.request.Request(
        url + "/ai/v1/extract",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urllib.request.urlopen(request, timeout=1200)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response), time.monotonic() - started


def request(case, model):
    schema = json.loads(case["schema_json"])
    for task in schema["classifications"]:
        task["top_k"] = len(task["labels"])
    return {
        "schema_version": 2,
        "model": model,
        "schema": schema,
        "inputs": [{"content": case["text"]}],
        "options": {"include_confidence": True},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--url", default="http://127.0.0.1:18783")
    parser.add_argument("--model", default="antflydb/GLiNER2.5-Decide-1B")
    parser.add_argument("--backend", choices=["native", "metal"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference_bytes = args.reference.read_bytes()
    reference_sha256 = hashlib.sha256(reference_bytes).hexdigest()
    cases = json.loads(reference_bytes)["cases"]
    reports = []
    for case in cases:
        body = request(case, args.model)
        status, result, seconds = post(args.url, body)
        if len(case["input_ids"]) > 1024:
            assert status == 413 and result["error"] == "EXTRACTION_LIMIT_EXCEEDED", (
                result
            )
            reports.append(
                {
                    "id": case["id"],
                    "status": status,
                    "rejected": True,
                    "seconds": seconds,
                }
            )
            continue
        assert status == 200, result
        assert result["usage"]["prompt_tokens"] == len(case["input_ids"]), result
        values = {
            (entry["name"], entry["label"]): entry["score"]
            for entry in result["data"][0]["classifications"]
        }
        errors, answers, gold = [], {}, {}
        for task, expected in zip(
            body["schema"]["classifications"], case["probabilities"]
        ):
            actual = [values[task["name"], label] for label in task["labels"]]
            errors += [abs(x - y) for x, y in zip(actual, expected)]
            winner = task["labels"][max(range(len(actual)), key=actual.__getitem__)]
            oracle = task["labels"][max(range(len(expected)), key=expected.__getitem__)]
            assert winner == oracle, (case["id"], winner, oracle)
            answers[task["name"]] = winner
            if task["name"] in case["gold"]:
                gold[task["name"]] = winner == case["gold"][task["name"]]
        assert len(values) == len(errors) and max(errors) <= 0.01, (
            case["id"],
            values,
            errors,
        )
        reports.append(
            {
                "id": case["id"],
                "tokens": len(case["input_ids"]),
                "probability_error": max(errors),
                "answers": answers,
                "gold_correct": gold,
                "seconds": seconds,
            }
        )
        print(case["id"], len(case["input_ids"]), max(errors), flush=True)
        args.output.write_text(
            json.dumps(
                {
                    "reference_sha256": reference_sha256,
                    "backend": args.backend,
                    "complete": False,
                    "reports": reports,
                },
                indent=2,
            )
            + "\n"
        )
    small = request(cases[0], args.model)
    multiple = copy.deepcopy(small)
    multiple["inputs"] *= 2
    status, result, _ = post(args.url, multiple)
    assert status in (400, 413), result
    # Exact 64-window admission with 1009 repeated, single-token words plus the processor terminal per
    # nonoverlapping window. Each model sequence includes the same schema.
    capacity = {
        "schema_version": 2,
        "model": args.model,
        "schema": {
            "classifications": [
                {
                    "name": "intent",
                    "labels": ["refund", "support", "cancel"],
                    "top_k": 3,
                }
            ]
        },
        "inputs": [{"content": " ".join(["word"] * (64 * 1009))}],
        "options": {
            "include_confidence": True,
            "long_document": {
                "mode": "window",
                "window_words": 1010,
                "overlap_words": 0,
                "max_windows": 64,
            },
        },
    }
    status, result, seconds = post(args.url, capacity)
    assert status == 200, result
    tokens = result["usage"]["prompt_tokens"]
    assert tokens == 64 * 1024, result
    capacity_report = {
        "windows": 64,
        "tokens": tokens,
        "seconds": seconds,
        "response": result,
    }
    capacity["inputs"][0]["content"] += " " + " ".join(["word"] * 1009)
    capacity["options"]["long_document"]["max_windows"] = 65
    status, result, _ = post(args.url, capacity)
    assert status == 413 and result["error"] == "EXTRACTION_LIMIT_EXCEEDED", result
    status, result, _ = post(args.url, small)
    assert status == 200, result
    args.output.write_text(
        json.dumps(
            {
                "reference_sha256": reference_sha256,
                "backend": args.backend,
                "complete": True,
                "reports": reports,
                "capacity": capacity_report,
                "two_items_rejected": True,
                "sixty_five_windows_rejected": True,
                "subsequent_request_succeeded": True,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
