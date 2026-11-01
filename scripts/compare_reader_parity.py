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

"""Compare independently captured, hash-only OCR correctness and warm throughput.

Capture each engine in isolation with qualify_reader_page_latency.py. Use the
same pinned checkpoint, verified fixtures, warm-up, token allowance, client and
concurrency. This compares measured serving shapes; it never rewrites output or
turns a reference engine's own serial/batch drift into a correctness pass.
The caller must verify the declared model revision against installed artifacts.
"""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


def checked_hash(value, length=64):
    if (
        not isinstance(value, str)
        or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None
    ):
        raise ValueError("missing or invalid qualification hash")
    return value


def positive_number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("missing or invalid measurement")
    return value


def checked_receipt(report, *, native, concurrency):
    if report.get("passed") is not True:
        raise ValueError("capture did not pass complete-output checks")
    if report.get("api_shape") != ("antfly-reader" if native else "openai-chat"):
        raise ValueError("capture uses the wrong API shape")
    if (
        type(report.get("concurrency")) is not int
        or report["concurrency"] != concurrency
    ):
        raise ValueError("capture concurrency differs from the requested serving shape")
    revision = report.get("model_revision")
    if (
        not isinstance(revision, str)
        or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", revision) is None
    ):
        raise ValueError("capture must declare its verified pinned model revision")
    checked_hash(report.get("benchmark_script_sha256"))
    if type(report.get("max_tokens")) is not int or report["max_tokens"] <= 0:
        raise ValueError("capture must declare its output allowance")
    warmup = report.get("warmup_observations", [])
    if (
        type(report.get("warmup_rounds")) is not int
        or not 1 <= report["warmup_rounds"] <= 2
        or len(warmup) != report["warmup_rounds"]
    ):
        raise ValueError("capture needs explicit, excluded warm-up observations")
    rounds = report.get("observations", [])
    if not 2 <= len(rounds) <= 4:
        raise ValueError("capture needs 2–4 warm measured rounds")
    samples = defaultdict(set)
    prompt_tokens = defaultdict(set)
    cohorts = []
    shape = None
    for measured, observation in [(False, o) for o in warmup] + [
        (True, o) for o in rounds
    ]:
        rows = observation["requests"]
        if not rows or len(rows) < concurrency:
            raise ValueError("capture cannot fill the selected serving concurrency")
        current_shape = []
        for row in rows:
            digest = checked_hash(row.get("image_sha256"))
            text = checked_hash(row.get("text_sha256"))
            if (
                row.get("http_status") != 200
                or row.get("items") != 1
                or row.get("tail_pass") is not True
                or row.get("statuses") != ["completed"]
            ):
                raise ValueError("capture contains an incomplete or invalid response")
            current_shape.append(digest)
            samples[digest].add(text)
            tokens = (row.get("usage") or {}).get("prompt_tokens")
            if type(tokens) is not int or tokens <= 0:
                raise ValueError(
                    "capture must report a positive prompt-token count per image"
                )
            prompt_tokens[digest].add(tokens)
            if measured:
                if native and concurrency > 1:
                    execution = row.get("execution") or {}
                    width = execution.get("max_native_step_items")
                    if (
                        type(width) is not int
                        or width < 2
                        or type(execution.get("native_items")) is not int
                        or execution["native_items"] != 1
                    ):
                        raise ValueError(
                            "capture lacks measured multirow decoder execution"
                        )
        if shape is None:
            shape = current_shape
        elif current_shape != shape:
            raise ValueError("capture changed page ordering or counts between rounds")
    for observation in rounds:
        cohorts.append(positive_number(observation["seconds"]))
    elapsed = statistics.median(cohorts)
    return {
        "revision": revision,
        "script": report["benchmark_script_sha256"],
        "max_tokens": report["max_tokens"],
        "shape": shape,
        "samples": dict(samples),
        "prompt_tokens": dict(prompt_tokens),
        "seconds": elapsed,
        "pages_per_second": len(shape) / elapsed,
        "rounds": len(rounds),
        "warmup_rounds": len(warmup),
    }


def exact_outputs(left, right):
    return left.keys() == right.keys() and all(
        len(left[k]) == len(right[k]) == 1 and left[k] == right[k] for k in left
    )


def compare(native_serial, native_batch, reference_serial, reference_batch):
    concurrency = native_batch.get("concurrency")
    if type(concurrency) is not int or not 2 <= concurrency <= 16:
        raise ValueError("batch capture must select 2–16 concurrent requests")
    ns = checked_receipt(native_serial, native=True, concurrency=1)
    nb = checked_receipt(native_batch, native=True, concurrency=concurrency)
    rs = checked_receipt(reference_serial, native=False, concurrency=1)
    rb = checked_receipt(reference_batch, native=False, concurrency=concurrency)
    captures = (ns, nb, rs, rb)
    for key in ("revision", "script", "max_tokens", "rounds", "warmup_rounds"):
        if len({c[key] for c in captures}) != 1:
            raise ValueError(f"captures disagree on {key}")
    if ns["shape"] != rs["shape"] or nb["shape"] != rb["shape"]:
        raise ValueError("engines did not measure identical ordered page cohorts")
    serial_counts, batch_counts = Counter(ns["shape"]), Counter(nb["shape"])
    if (
        serial_counts.keys() != batch_counts.keys()
        or len({batch_counts[k] / serial_counts[k] for k in serial_counts}) != 1
    ):
        raise ValueError(
            "serial and concurrent cohorts have different page distributions"
        )
    if any(c["prompt_tokens"] != ns["prompt_tokens"] for c in captures) or any(
        len(tokens) != 1 for tokens in ns["prompt_tokens"].values()
    ):
        raise ValueError("captures disagree on per-image prompt-token counts")
    correctness = {
        "serial_exact": exact_outputs(ns["samples"], rs["samples"]),
        "concurrent_exact": exact_outputs(nb["samples"], rb["samples"]),
        "native_batch_stable": exact_outputs(ns["samples"], nb["samples"]),
        "reference_batch_stable": exact_outputs(rs["samples"], rb["samples"]),
    }
    timings = {}
    for mode, native, reference in (("serial", ns, rs), ("concurrent", nb, rb)):
        timings[mode] = {
            "native_median_seconds": native["seconds"],
            "reference_median_seconds": reference["seconds"],
            "native_pages_per_second": native["pages_per_second"],
            "reference_pages_per_second": reference["pages_per_second"],
            "native_over_reference_latency": native["seconds"] / reference["seconds"],
            "parity": native["seconds"] <= reference["seconds"],
        }
    return {
        "schema": "antfly.reader-parity/v1",
        "model_revision": ns["revision"],
        "concurrency": concurrency,
        "max_tokens": ns["max_tokens"],
        "correctness": correctness,
        "timings": timings,
        "passed": all(correctness.values())
        and all(t["parity"] for t in timings.values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "native-serial",
        "native-batch",
        "reference-serial",
        "reference-batch",
    ):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    names = ("native_serial", "native_batch", "reference_serial", "reference_batch")
    paths = [getattr(args, name) for name in names]
    digests = {}
    try:
        captures = []
        for name, path in zip(names, paths):
            raw = path.read_bytes()
            digests[name] = hashlib.sha256(raw).hexdigest()
            captures.append(json.loads(raw))
        result = compare(*captures)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        result = {
            "schema": "antfly.reader-parity/v1",
            "passed": False,
            "error": str(error),
        }
    result["capture_sha256"] = digests
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
