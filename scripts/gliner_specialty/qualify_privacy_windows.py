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

"""Qualify short, UTF-8 and long PII extraction on a required backend.

Uses synthetic text only. This checks execution and coordinates, not recall
on a representative application corpus. Model loading verifies pinned bytes.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

MODEL = "fastino/gliner2-privacy-filter-PII-multi"
REVISION = "1cb4166094dc58fa8d836429f060d6c95f62b495"


def request_body() -> dict:
    return {
        "schema_version": 2,
        "model": MODEL,
        "schema": {"entities": ["email", "person"]},
        "inputs": [
            {
                "id": "short",
                "content": "Contact Maria Jensen at maria.jensen@example.dk.",
            },
            {"id": "unicode", "content": "Éva: maria.jensen@example.dk."},
            {
                "id": "long",
                "content": "The weather today is pleasant. " * 180
                + "Contact Maria Jensen at maria.jensen@example.dk.",
            },
        ],
        "options": {
            "include_spans": True,
            "include_confidence": True,
            "offset_unit": "utf8_bytes",
            "overlap": "allow",
            "long_document": {
                "mode": "window",
                "window_words": 384,
                "overlap_words": 32,
                "max_windows": 64,
            },
        },
    }


def validate(request: dict, result: dict, reference: dict | None) -> dict:
    rows = result["data"]
    assert len(rows) == len(request["inputs"]), "result cardinality differs"
    maximum_delta = 0.0
    for ordinal, (source, actual) in enumerate(zip(request["inputs"], rows)):
        assert source["id"] == actual["id"], "input order differs"
        assert actual["offset_unit"] == "utf8_bytes"
        assert actual["long_document"]["window_count"] >= 1
        for entity in actual["entities"]:
            raw = source["content"].encode()
            assert 0 <= entity["start"] < entity["end"] <= len(raw)
            assert raw[entity["start"] : entity["end"]].decode() == entity["text"]
        assert any(entity["label"] == "email" for entity in actual["entities"])
        if reference is not None:
            expected = reference["data"][ordinal]
            assert actual["id"] == expected["id"]
            assert (
                actual["long_document"]["window_count"]
                == expected["long_document"]["window_count"]
            )
            assert len(actual["entities"]) == len(expected["entities"])
            for left, right in zip(actual["entities"], expected["entities"]):
                for key in ("label", "text", "start", "end"):
                    assert left[key] == right[key], (key, left, right)
                maximum_delta = max(maximum_delta, abs(left["score"] - right["score"]))
    windows = rows[-1]["long_document"]["window_count"]
    assert windows > 1, "long input did not exercise windowing"
    assert any(
        entity["label"] == "email" and entity["start"] > 5000
        for entity in rows[-1]["entities"]
    )
    assert maximum_delta <= 0.005, maximum_delta
    return {
        "documents": len(rows),
        "long_windows": windows,
        "tail_email_detected": True,
        "discrete_spans_equal": True if reference is not None else None,
        "max_score_delta_against_reference": maximum_delta
        if reference is not None
        else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--backend", choices=["native", "metal", "cuda"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reference", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    request = request_body()
    request_bytes = json.dumps(request).encode()
    (args.output / "request.json").write_bytes(request_bytes)
    env = {
        **os.environ,
        "ANTFLY_INFERENCE_REQUIRED_BACKEND": args.backend,
        "ANTFLY_INFERENCE_PREFERRED_BACKEND": args.backend,
    }
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    binary = args.server.resolve()
    command = [
        str(binary),
        *(["inference"] if binary.name == "antfly" else []),
        "run",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--models-dir",
        str(args.models_dir.resolve()),
        "--ml-dir",
        str(args.output.resolve() / "ml"),
        "--max-loaded-models",
        "1",
    ]
    log_path = args.output / f"{args.backend}.log"
    with log_path.open("w") as log:
        proc = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 30
            while True:
                assert proc.poll() is None, f"server exited; see {log_path}"
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline, "server readiness timed out"
                    time.sleep(0.1)
            http_request = urllib.request.Request(
                f"http://127.0.0.1:{port}/ai/v1/extract",
                data=request_bytes,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(http_request, timeout=240) as response:
                result = json.load(response)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
    (args.output / f"{args.backend}.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    reference = json.loads(args.reference.read_text()) if args.reference else None
    report = validate(request, result, reference)
    assert f"selected backend {args.backend}" in log_path.read_text(), (
        "required backend was not observed"
    )
    report.update(backend=args.backend, model=MODEL, revision=REVISION)
    (args.output / f"{args.backend}-comparison.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    main()
