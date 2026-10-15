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

"""Completed-request load, continuation, disconnect/retry and memory qualification.

Uses a real checkpoint-backed server. Reports belong under .benchmark-results.
A disconnect is followed by a successful retry and metric sampling; EOF is never
counted as completed coverage. No external benchmarking dependencies are used.
"""

from __future__ import annotations

import argparse
import concurrent.futures
from collections import Counter
import hashlib
import json
import mimetypes
import platform
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from qualify_media_chunks import chunk_response, fixtures, media, owned_server, post


def metrics(url):
    with urllib.request.urlopen(
        url.rstrip("/") + "/ml/v1/metrics", timeout=30
    ) as response:
        text = response.read(4 * 1024 * 1024).decode()
    result = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, value, *_ = line.split()
        if any(
            word in key
            for word in (
                "bytes",
                "memory",
                "active",
                "admission",
                "transfer",
                "decode",
                "models_loaded",
            )
        ):
            result[key] = float(value)
    return result


def drained_metrics(url):
    deadline = time.monotonic() + 30
    while True:
        current = metrics(url)
        active = current.get("antfly_inference_requests_active", 0)
        admitted = current.get("antfly_admission_inference_in_flight_requests", 0)
        if active == 0 and admitted == 0:
            return current
        if time.monotonic() >= deadline:
            raise AssertionError("request/cancellation admission did not drain")
        time.sleep(0.25)


def request_for(args, name):
    value, policy = fixtures(args.video)[name]
    if name == "audio" and getattr(args, "audio", None):
        mime = mimetypes.guess_type(args.audio.name)[0]
        value = media(mime or "audio/mpeg", args.audio.read_bytes())
        policy = {"window_ns": 30_000_000_000, "overlap_ns": 2_000_000_000}
    embedding = {"model": args.model}
    if getattr(args, "model_identity", None):
        embedding["model_identity"] = args.model_identity
    return {"input": value, "embedding": embedding, "policy": policy}


def complete_request(url, body, on_retry):
    # Retry only explicit admission failures. A terminal coverage record
    # tells us exactly which acknowledged ordinals to preserve on resume.
    retryable = {
        "ResourceTemporarilyUnavailable",
        "ConcurrencyUnavailable",
        "InferenceAdmissionQueueFull",
        "SharedAdmissionExceeded",
        "MemoryBudgetExceeded",
    }
    request = dict(body)
    acknowledged = []
    context = None
    first_ordinal = None
    deadline = time.monotonic() + 180
    for attempt in range(65):
        result = None
        try:
            result = chunk_response(url, request)
        except RuntimeError as error:
            if not any(status in str(error) for status in ("HTTP 503:", "HTTP 429:")):
                raise
            code = "HTTP_ADMISSION"
        else:
            identity = tuple(
                result[key]
                for key in (
                    "source_fingerprint",
                    "plan_id",
                    "model_identity",
                    "model_revision",
                )
            )
            if context is None:
                context = identity
                first_ordinal = result["start_ordinal"]
            assert identity == context, "retry changed immutable execution context"
            assert result["start_ordinal"] == first_ordinal + len(acknowledged)
            for index, item in enumerate(
                result["data"], first_ordinal + len(acknowledged)
            ):
                assert item["descriptor"]["ordinal"] == index
                assert not item.get("error")
            acknowledged.extend(result["data"])
            assert result["covered_through_ordinal"] == first_ordinal + len(
                acknowledged
            )
            error = result.get("error")
            if not error:
                result["data"] = acknowledged
                result["start_ordinal"] = first_ordinal
                return result
            code = error["code"]
            if code not in retryable or not result.get("next_continuation"):
                raise AssertionError(result)
            request["continuation"] = result["next_continuation"]
        if attempt == 64 or time.monotonic() >= deadline:
            raise AssertionError(f"admission retry budget exhausted: {code}")
        on_retry(code)
        time.sleep(min(1.0, 0.02 * 2 ** min(attempt, 6)))
    raise AssertionError("unreachable retry loop")


def qualify(args):
    cases = args.cases.split(",")
    timings = []
    completed = 0
    failures = []
    records = []
    stopped = threading.Event()
    before = metrics(args.url)
    peaks = dict(before)
    samples = 0
    retry_codes = Counter()
    retry_lock = threading.Lock()

    def note_retry(code):
        with retry_lock:
            retry_codes[code] += 1

    def complete(body):
        return complete_request(args.url, body, note_retry)

    def sample():
        nonlocal samples
        while not stopped.wait(0.25):
            try:
                for key, value in metrics(args.url).items():
                    peaks[key] = max(peaks.get(key, value), value)
                samples += 1
            except Exception as error:
                failures.append(f"metric sample: {error}")

    observer = threading.Thread(target=sample, daemon=True)
    observer.start()
    try:
        # Cold planning must leave the loaded backend-session count unchanged.
        planning_start = time.monotonic()
        body = request_for(args, cases[0])
        body["embedding"] = {"model": args.model, "execute": False}
        planned = complete(body)
        assert planned["status"] == "complete"
        planning_after = metrics(args.url)
        for key, value in before.items():
            if "models_loaded" in key:
                assert planning_after[key] == value, (key, value, planning_after[key])
        planning_seconds = time.monotonic() - planning_start
        args.model_identity = planned["model_identity"]
        discovery_url = (
            args.url.rstrip("/")
            + "/ai/v1/models?"
            + urllib.parse.urlencode({"model": args.model, "task": "embed"})
        )
        with urllib.request.urlopen(discovery_url, timeout=60) as response:
            info = json.load(response)["embedders"][args.model]
        assert info["model_revision"] == planned["model_revision"]
        limits = info["chunking_limits"]
        assert all(
            limits[key] > 0
            for key in (
                "max_working_bytes",
                "max_event_bytes",
                "remote_requests",
                "remote_fetched_bytes",
            )
        )
        stale = dict(body, continuation="0" * 64 + ":0")
        try:
            chunk_response(args.url, stale)
        except RuntimeError as error:
            assert "HTTP 409:" in str(error), str(error)
        else:
            raise AssertionError("stale continuation was accepted")
        # Exact IDs and vectors must survive bounded delivery and continuation.
        for name in cases:
            body = request_for(args, name)
            full = complete(body)
            assert full["status"] == "complete" and not full.get("gaps")
            resumed = []
            cursor = None
            while True:
                page = dict(body, config={"max_chunks": 1}, stream=True)
                if cursor:
                    page["continuation"] = cursor
                response = complete(page)
                resumed.extend(response["data"])
                cursor = response.get("next_continuation")
                if not cursor:
                    assert response["status"] == "complete"
                    break
                assert response["status"] == "partial"
            assert [item["descriptor"] for item in resumed] == [
                item["descriptor"] for item in full["data"]
            ]
            for actual, expected in zip(resumed, full["data"]):
                assert len(actual["embedding"]) == len(expected["embedding"])
                assert (
                    max(
                        abs(x - y)
                        for x, y in zip(actual["embedding"], expected["embedding"])
                    )
                    < 1e-5
                )
            # Closing after start/chunk leaves completion unconfirmed. A retry
            # must work and retain source/plan identity after producer cleanup.
            with post(args.url, "chunk", dict(body, stream=True)) as connection:
                first = json.loads(connection.readline(16 * 1024 * 1024))
                assert first["object"] == "chunk.embedding.start"
            retry = complete(body)
            assert retry["status"] == "complete"
            assert retry["plan_id"] == full["plan_id"]
            records.append(
                {
                    "case": name,
                    "chunks": len(full["data"]),
                    "model_identity": full["model_identity"],
                    "model_revision": full["model_revision"],
                    "plan_id": full["plan_id"],
                    "source_fingerprint": full["source_fingerprint"],
                    "continuation": "pass",
                    "disconnect_retry": "pass",
                }
            )

        warm_baseline = drained_metrics(args.url)
        begin = time.monotonic()
        deadline = begin + args.seconds

        def worker(index):
            durations = []
            iteration = 0
            while time.monotonic() < deadline:
                name = cases[(index + iteration) % len(cases)]
                started = time.monotonic()
                result = complete(dict(request_for(args, name), stream=True))
                assert result["status"] == "complete" and not result.get("gaps"), result
                durations.append(time.monotonic() - started)
                iteration += 1
            return durations

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.concurrency
        ) as executor:
            futures = [
                executor.submit(worker, index) for index in range(args.concurrency)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    timings.extend(future.result())
                except Exception as error:
                    failures.append(str(error))
        completed = len(timings)
        elapsed = time.monotonic() - begin
        assert completed > 0
    finally:
        stopped.set()
        observer.join(timeout=35)
    after = drained_metrics(args.url)
    if args.binary:
        assert (
            after["antfly_inference_models_loaded"]
            > before["antfly_inference_models_loaded"]
        ), "loaded-model gauge must observe actual execution"
    sorted_times = sorted(timings)
    report = {
        "backend": args.backend,
        "binary_sha256": getattr(args, "binary_sha256", None),
        "host": platform.platform(),
        "effective_limits": limits,
        "stale_continuation": "rejected",
        "cold_planning_seconds": planning_seconds,
        "planning_without_backend_session": True,
        "concurrency": args.concurrency,
        "requested_seconds": args.seconds,
        "elapsed_seconds": elapsed,
        "completed_requests": completed,
        "completed_requests_per_second": completed / elapsed,
        "median_seconds": statistics.median(timings),
        "p95_seconds": sorted_times[
            min(len(sorted_times) - 1, int(len(sorted_times) * 0.95))
        ],
        "cases": records,
        "admission_retries": dict(retry_codes),
        "failures": failures,
        "metric_samples": samples,
        "metrics_before": before,
        "metrics_warm_baseline": warm_baseline,
        "cancellation_and_load_drained": True,
        "metrics_peak_sampled": peaks,
        "metrics_after": after,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "backend",
                    "concurrency",
                    "completed_requests",
                    "elapsed_seconds",
                    "median_seconds",
                    "p95_seconds",
                    "admission_retries",
                    "failures",
                )
            }
        )
    )
    assert not failures, failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--model", default="model")
    parser.add_argument("--backend", default="native")
    parser.add_argument("--video", type=Path)
    parser.add_argument("--audio", type=Path)
    parser.add_argument("--cases", default="text,audio,video")
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    assert args.seconds > 0 and args.concurrency > 0
    if args.binary:
        with args.binary.open("rb") as binary:
            digest = hashlib.sha256()
            for block in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(block)
            args.binary_sha256 = digest.hexdigest()
    with owned_server(args) as url:
        args.url = url
        qualify(args)


if __name__ == "__main__":
    main()
