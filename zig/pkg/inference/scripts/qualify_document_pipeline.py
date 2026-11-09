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

"""Compare singleton and concurrent native OCR with the write-pool fixtures.

This measures inference HTTP throughput, not source fetching or artifact
publication. Native PDF orchestration is checked by pdf-ocr-integration-test.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import statistics
import struct
import time
import urllib.request
import urllib.error

import qualify_read_pool_40g as shared

CORPUS = Path(__file__).parent / "lightonocr3/corpus/cases.json"


def envelope(model, image, max_tokens):
    metadata = json.dumps(
        {"model": model, "images": [{"url": "attachment:0"}], "max_tokens": max_tokens}
    ).encode()
    mime = b"image/png"
    return (
        b"AFATT001"
        + struct.pack("<QII", len(metadata), 1, 0)
        + struct.pack("<IIQ", len(mime), 0, len(image))
        + metadata
        + mime
        + image
    )


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def validate(response, required):
    require(not response.get("error"), response)
    require(len(response.get("data", [])) == 1, response)
    item = response["data"][0]
    require(not item.get("error"), item)
    require(item.get("finish_reason") not in ("length", "max_tokens"), item)
    require(item.get("status", "complete") == "complete", item)
    text = re.sub(r"\s+", " ", item["text"]).strip().casefold()
    require(bool(text), item)
    for value in required:
        require(value.casefold() in text, {"missing": value, "text": text})
    return text


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    cases_path = args.cases.resolve()
    cases = json.loads(cases_path.read_text())["cases"]
    prepared = []
    for case in cases:
        image = (cases_path.parent / case["image"]).resolve().read_bytes()
        prepared.append(
            (
                case,
                envelope(args.model, image, args.max_tokens),
                hashlib.sha256(image).hexdigest(),
            )
        )
    references = {}
    rows = []

    def request(index, phase):
        case, body, digest = prepared[index]
        req = urllib.request.Request(
            args.url.rstrip("/") + "/ai/v1/read",
            data=body,
            headers={
                "Content-Type": "application/vnd.antfly.attachments.v1",
                "X-Antfly-Inference-Timeout-Ms": "180000",
            },
        )
        started = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=190) as result:
                require(result.status == 200, result.status)
                response = json.loads(result.read())
        except urllib.error.HTTPError as error:
            detail = error.read().decode(errors="replace")
            (args.output / "failure.json").write_text(
                json.dumps(
                    {
                        "case": case["name"],
                        "phase": phase,
                        "status": error.code,
                        "body": detail,
                    },
                    indent=2,
                )
                + "\n"
            )
            raise RuntimeError(f"OCR HTTP {error.code}: {detail}") from error
        seconds = time.monotonic() - started
        text = validate(response, case["required"])
        if index in references:
            require(
                text == references[index],
                {
                    "case": case["name"],
                    "phase": phase,
                    "expected": references[index],
                    "actual": text,
                },
            )
        return {
            "case": case["name"],
            "image_sha256": digest,
            "phase": phase,
            "seconds": seconds,
            "normalized_text": text,
            "usage": response.get("usage"),
            "execution": response.get("execution"),
        }

    for index in range(len(prepared)):
        row = request(index, "warmup")
        references[index] = row["normalized_text"]
        rows.append(row)
    _, before, _ = shared.http(args.url, "/metrics", text=True)
    (args.output / "metrics-before.prom").write_text(before)
    rounds = []
    for round_index in range(args.rounds):
        # Alternate order to reduce warm-cache and drift bias.
        for concurrency in (
            [1, args.concurrency] if round_index % 2 == 0 else [args.concurrency, 1]
        ):
            phase = f"round-{round_index}-concurrency-{concurrency}"
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                completed = list(
                    pool.map(
                        lambda index: request(index % len(prepared), phase),
                        range(args.requests),
                    )
                )
            seconds = time.monotonic() - started
            rows.extend(completed)
            row = {
                "round": round_index,
                "concurrency": concurrency,
                "completed": len(completed),
                "seconds": seconds,
                "pages_per_second": len(completed) / seconds,
                "median_request_seconds": statistics.median(
                    r["seconds"] for r in completed
                ),
            }
            rounds.append(row)
            print(json.dumps(row), flush=True)
            (args.output / "requests.json").write_text(
                json.dumps(rows, indent=2) + "\n"
            )
    _, after, _ = shared.http(args.url, "/metrics", text=True)
    (args.output / "metrics-after.prom").write_text(after)
    singleton = statistics.median(
        r["pages_per_second"] for r in rounds if r["concurrency"] == 1
    )
    concurrent = statistics.median(
        r["pages_per_second"] for r in rounds if r["concurrency"] == args.concurrency
    )
    report = {
        "scope": "inference HTTP only; synthetic smoke corpus; not an A100 qualification",
        "model": args.model,
        "transport": "AFATT001",
        "rounds": rounds,
        "singleton_pages_per_second": singleton,
        "concurrent_pages_per_second": concurrent,
        "throughput_ratio": concurrent / singleton,
        "normalized_output_parity": True,
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://127.0.0.1:18879")
    p.add_argument("--model", required=True)
    p.add_argument("--cases", type=Path, default=CORPUS)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--requests", type=int, default=8)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--max-tokens", type=int, default=2048)
    a = p.parse_args()
    if not (
        1 <= a.requests <= 128
        and 1 <= a.rounds <= 10
        and 2 <= a.concurrency <= 4
        and 1 <= a.max_tokens <= 2048
    ):
        p.error("requests 1..128, rounds 1..10, concurrency 2..4, max-tokens 1..2048")
    run(a)
