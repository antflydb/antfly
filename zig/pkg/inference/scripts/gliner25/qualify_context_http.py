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

"""Exercise the pinned base model's encoded-token capacity through HTTP.

This is an explicit, potentially slow qualification command, not an accuracy
benchmark. The service should already be running with the pinned base artifact.
No retries or output normalization are performed.
"""

import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from benchmark_linux_cpu_http import strict_json


def execute(endpoint, text, timeout):
    payload = {
        "schema_version": 2,
        "model": "fastino/gliner2.5-base-v1",
        "schema": {"entities": ["concept"]},
        "options": {
            "include_spans": True,
            "offset_unit": "utf8_bytes",
            "long_document": {
                "mode": "window",
                "window_words": 4096,
                "overlap_words": 128,
            },
        },
        "inputs": [{"content": text}],
    }
    body = json.dumps(payload, allow_nan=False).encode()
    request = Request(endpoint, data=body, headers={"Content-Type": "application/json"})
    start = time.perf_counter_ns()
    with urlopen(request, timeout=timeout) as response:
        raw = response.read(16 * 1024 * 1024 + 1)
        if response.status != 200 or len(raw) > 16 * 1024 * 1024:
            raise ValueError("unsuccessful or oversized response")
    duration = time.perf_counter_ns() - start
    output = strict_json(raw)
    usage = output["usage"]
    if (
        usage["completion_tokens"] != 0
        or usage["total_tokens"] != usage["prompt_tokens"]
    ):
        raise ValueError("inconsistent extraction token usage")
    if len(output["data"]) != 1:
        raise ValueError("expected one result")
    row = output["data"][0]
    if row["long_document"]["window_count"] != 1:
        raise ValueError("capacity request was split into windows")
    encoded = text.encode()
    for entity in row.get("entities", []):
        start, end = entity["start"], entity["end"]
        if (
            not 0 <= start < end <= len(encoded)
            or encoded[start:end] != entity["text"].encode()
        ):
            raise ValueError("invalid source span")
    return {
        "duration_ns": duration,
        "request_sha256": hashlib.sha256(body).hexdigest(),
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "output": output,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args()
    url = urlsplit(args.endpoint)
    if url.scheme != "http" or url.hostname not in ("localhost", "127.0.0.1", "::1"):
        parser.error("use the isolated loopback qualification service")
    if not 0 < args.timeout <= 1800:
        parser.error("timeout must be in (0, 1800]")
    # Refuse to overwrite qualification evidence, including a failed run.
    with args.output.open("x") as destination:
        report = {
            "status": "running",
            "scope": "execution_capacity_only",
            "endpoint": args.endpoint,
        }
        try:
            word = "configuration_visibility_maintenance_acknowledgement "
            probes = [execute(args.endpoint, word * n, args.timeout) for n in (1, 2)]
            tokens = [sample["output"]["usage"]["prompt_tokens"] for sample in probes]
            word_tokens = tokens[1] - tokens[0]
            overhead = tokens[0] - word_tokens
            if word_tokens <= 0 or not 0 <= overhead < 16384:
                raise ValueError("invalid measured token geometry")
            words = (16384 - overhead) // word_tokens
            if not 1 <= words <= 4095:
                raise ValueError("encoded capacity cannot fit one body-word window")
            report.update(
                word=word,
                word_tokens=word_tokens,
                overhead=overhead,
                source_words=words,
                probes=probes,
            )
            report["capacity"] = execute(args.endpoint, word * words, args.timeout)
            actual = report["capacity"]["output"]["usage"]["prompt_tokens"]
            if (
                actual != overhead + word_tokens * words
                or not 16384 - word_tokens < actual <= 16384
            ):
                raise ValueError(
                    "full request did not reach the measured encoded-token boundary"
                )
            report["status"] = "complete"
        except BaseException as error:
            report.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            destination.write(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
