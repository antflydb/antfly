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

"""Bounded native GPU qualification; does not provision, scale, or deploy pools.

Requires canonical candidate policies: output=512 tokens/item, input=8192
 tokens/item, decoded pixels=4,000,000/batch, max dimension=2048, request
 concurrency=1. Rejection fixtures stay below endpoint/model hard limits.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import struct
import threading
import time
import urllib.error
import urllib.request
import zlib

READERS = [
    "lightonai/LightOnOCR-3-0.8B:safetensors@36d8636925327c671bd8e479c517a004f7566bd9",
    "lightonai/LightOnOCR-3-4B:safetensors@b71010f095dc3735de7ee6b9969bff8f8f50b39a",
]
GENERATOR = "ggml-org/gemma-4-E4B-it-GGUF:gguf:Q4_0"


def blank_png(width, height):
    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    pixels = (b"\x00" + b"\xff" * (width * 3)) * height
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(pixels))
        + chunk(b"IEND", b"")
    )


def image_url(data):
    return "data:image/png;base64," + base64.b64encode(data).decode()


class Probe:
    def __init__(self, timeout):
        self.timeout = timeout
        self.results = []

    def request(self, url, path, body=None):
        headers = {"Content-Type": "application/json"}
        token = os.environ.get("ANTFLY_INFERENCE_TOKEN")
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(
            url.rstrip("/") + "/ai/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            # Record synthetic inference results only; never record headers or token.
            return error.code, json.loads(error.read(65536))

    def instances(self, url, model):
        status, body = self.request(url, "/models/instances")
        assert status == 200, "instance inspection failed"
        instances = body["data"]
        assert len(instances) == 1, "one resident model policy was not enforced"
        assert instances[0]["model"] == model and instances[0]["backend"] == "cuda", (
            "expected CUDA model is not resident"
        )
        return instances

    @staticmethod
    def validate_success(path, result, expected=None):
        if path == "/read":
            items = result.get("data", [])
            assert items, "OCR returned no items"
            assert all(not item.get("error") for item in items), (
                "OCR returned item errors"
            )
            text = " ".join(item.get("text", "") for item in items)
            assert all(value in text.lower() for value in expected or []), (
                "OCR fixture fields missing"
            )
        else:
            choice = result["choices"][0]
            text = choice["message"]["content"]
            assert "READY" in text.upper(), "generation did not answer the fixture"
            assert choice.get("finish_reason") != "length", "generation was truncated"
        assert text.strip(), "empty inference result"
        return text

    def accepted(self, url, path, body, expected=None):
        status, result = self.request(url, path, body)
        assert status == 200, f"inference failed: {status} {result}"
        text = self.validate_success(path, result, expected)
        return {
            "status": status,
            "text": text,
            "instances": self.instances(url, body["model"]),
        }

    def rejected(self, url, path, body, expected_code):
        status, result = self.request(url, path, body)
        assert status == 413 and result.get("error") == expected_code, (
            f"expected deployment {expected_code}, got {status} {result}"
        )
        assert result.get("retryable") is False, "policy failure must not be retryable"
        return {"status": status, "error": result}

    def overload(self, url, path, body, expected=None):
        barrier = threading.Barrier(2)

        def call(_):
            barrier.wait(timeout=10)
            return self.request(url, path, body)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(call, range(2)))
        statuses = sorted(status for status, _ in results)
        assert statuses == [200, 503], (
            f"expected one success and one bounded rejection: {results}"
        )
        success = next(result for status, result in results if status == 200)
        self.validate_success(path, success, expected)
        failure = next(result for status, result in results if status == 503)
        assert (
            failure.get("error") == "SERVICE_UNAVAILABLE"
            and failure.get("reason") == "inference_admission"
        ), f"expected request-concurrency rejection, got {failure}"
        assert failure.get("retryable") is True, (
            "concurrency rejection must be retryable"
        )
        status, _ = self.request(url, "/models")
        assert status == 200, "discovery unhealthy after overload"
        return {"statuses": statuses, "rejection": failure}

    def record(self, name, action):
        start = time.monotonic()
        report = {"check": name}
        try:
            report.update(action())
            report["success"] = True
        except Exception as error:
            report.update(success=False, error=str(error))
        report["seconds"] = round(time.monotonic() - start, 2)
        self.results.append(report)
        print(json.dumps(report), flush=True)
        return report["success"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader-url")
    parser.add_argument("--generator-url")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--invoice",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "pkg/inference/src/models/testdata/lightonocr3/invoice.png",
    )
    args = parser.parse_args()
    if not (args.reader_url or args.generator_url) or not 1 <= args.timeout <= 300:
        parser.error(
            "provide at least one pool URL and a timeout between 1 and 300 seconds"
        )
    probe = Probe(args.timeout)
    if args.reader_url:
        body = {
            "images": [{"url": image_url(args.invoice.read_bytes())}],
            "max_tokens": 512,
        }
        # Exercise cold/request-time loads, repeated work, and both switch directions.
        for model in [READERS[0], READERS[1], READERS[1], READERS[0]]:
            request = dict(body, model=model)
            if not probe.record(
                "reader " + model,
                lambda r=request: probe.accepted(
                    args.reader_url, "/read", r, ["invoice", "12345", "42.00"]
                ),
            ):
                break
        request = dict(body, model=READERS[0])
        probe.record(
            "reader oversized output",
            lambda: probe.rejected(
                args.reader_url,
                "/read",
                dict(request, max_tokens=513),
                "OUTPUT_TOKEN_LIMIT_EXCEEDED",
            ),
        )
        probe.record(
            "reader oversized image",
            lambda: probe.rejected(
                args.reader_url,
                "/read",
                dict(request, images=[{"url": image_url(blank_png(2048, 2000))}]),
                "DECODED_PIXELS_EXCEEDED",
            ),
        )
        probe.record(
            "reader concurrent overload",
            lambda: probe.overload(
                args.reader_url, "/read", request, ["invoice", "12345", "42.00"]
            ),
        )
    if args.generator_url:
        request = {
            "model": GENERATOR,
            "messages": [{"role": "user", "content": "Reply only READY."}],
            "max_tokens": 32,
            "enable_thinking": False,
            "temperature": 0,
            "mode": "eager",
        }
        for _ in range(2):
            if not probe.record(
                "generator",
                lambda: probe.accepted(args.generator_url, "/generate", request),
            ):
                break
        near_limit = dict(
            request,
            messages=[
                {
                    "role": "user",
                    "content": "Filler follows:\n"
                    + " x" * 8000
                    + "\nReply only READY.",
                }
            ],
        )
        probe.record(
            "generator near input limit",
            lambda: probe.accepted(args.generator_url, "/generate", near_limit),
        )
        probe.record(
            "generator oversized output",
            lambda: probe.rejected(
                args.generator_url,
                "/generate",
                dict(request, max_tokens=513),
                "OUTPUT_TOKEN_LIMIT_EXCEEDED",
            ),
        )
        probe.record(
            "generator oversized input",
            lambda: probe.rejected(
                args.generator_url,
                "/generate",
                dict(request, messages=[{"role": "user", "content": " x" * 9000}]),
                "INPUT_TOKEN_LIMIT_EXCEEDED",
            ),
        )
        probe.record(
            "generator concurrent overload",
            lambda: probe.overload(args.generator_url, "/generate", request),
        )
    args.report.write_text(json.dumps(probe.results, indent=2) + "\n")
    return 0 if all(item["success"] for item in probe.results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
