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

"""Bounded local inference timing; never treats failed/truncated work as throughput.

Runs an owned loopback server, uses existing model registrations, and records GPU
contention. This is a stage sample, not an end-to-end corpus or A100 qualification.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request


def hardware():
    result = {}
    for name, query in (
        ("gpu", "--query-gpu=name,utilization.gpu,temperature.gpu,power.draw"),
        ("gpu_processes", "--query-compute-apps=pid,process_name,used_gpu_memory"),
    ):
        run = subprocess.run(
            ["nvidia-smi", query, "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        result[name] = run.stdout.strip().splitlines()
    return result


def request(base, route, body, timeout):
    wire = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + "/ai/v1" + route,
        data=wire,
        headers={
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(int(timeout * 1000)),
        },
    )
    with urllib.request.urlopen(req, timeout=timeout + 5) as response:
        return json.loads(response.read())


def validate(kind, response, count, required=()):
    if response.get("error") or len(response.get("data", [])) != count:
        raise ValueError(response)
    for item in response["data"]:
        if item.get("error"):
            raise ValueError(item)
        if kind == "ocr":
            if (
                not item.get("text", "").strip()
                or item.get("status", "complete") != "complete"
                or item.get("finish_reason") in ("length", "max_tokens")
            ):
                raise ValueError("incomplete OCR response")
            for value in required:
                if value.casefold() not in item["text"].casefold():
                    raise ValueError("OCR missed required text: " + value)
        elif kind == "extract":
            if not item.get("entities"):
                raise ValueError("extraction returned no expected entity groups")
        elif kind == "embed":
            vector = item.get("embedding", [])
            if len(vector) != 768 or not all(math.isfinite(x) for x in vector):
                raise ValueError("invalid embedding dimension or values")


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[2]
    ocr = json.loads(
        (Path(__file__).parent / "lightonocr-reader.example.json").read_text()
    )["model"]
    embed = json.loads(
        (Path(__file__).parent / "antfly-embedder.example.json").read_text()
    )["model"]
    settings = (Path(__file__).parent / "indexes.go").read_text()

    def setting(name):
        return re.search(name + r'\s*=\s*"([^"]+)"', settings).group(1)

    extractor = setting("DefaultRecognizerModel")
    if args.extractor_model:
        extractor = args.extractor_model
    report = {
        "scope": "short inference-stage sample; no corpus/index/graph-materialization timing",
        "contention": "other GPU users are neither stopped nor excluded",
        "runtime": subprocess.check_output(
            [str(args.antfly), "version"], text=True, stderr=subprocess.STDOUT
        ).strip(),
        "models": {"ocr": ocr, "embed": embed, "extract": extractor},
        "hardware_before": hardware(),
        "requests": [],
    }
    config = {
        "resources": {
            "memory": {"max_bytes": 48 << 30},
            "inference": {
                "memory": {
                    "max_host_bytes": 32 << 30,
                    "max_device_bytes": 32 << 30,
                    "max_kv_bytes": 4 << 30,
                    "max_scratch_bytes": 8 << 30,
                },
                "admission": {"max_concurrent_requests": 2, "max_work_units": 64},
                "request_limits": {
                    "max_items_per_request": 8,
                    "max_input_tokens_per_item": 8192,
                    "max_output_tokens_per_item": 8192,
                    "max_decoded_pixels_per_batch": 16000000,
                },
            },
        },
        "inference": {
            "models_dir": str(args.models_dir),
            "max_loaded_models": 1,
            "keep_alive": "0",
            "prompt_cache": {"enabled": False},
            "generation_batching": {"mode": "on", "max_step_items": 2},
        },
    }
    config_path = args.output / "config.json"
    config_path.write_text(json.dumps(config))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["ANTFLY_INFERENCE_REQUIRED_BACKEND"] = "cuda"
    log = (args.output / "server.log").open("w")
    server = subprocess.Popen(
        [
            str(args.antfly),
            "inference",
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--config",
            str(config_path),
        ],
        env=env,
        stdout=log,
        stderr=log,
    )
    started = time.monotonic()
    report_lock = threading.Lock()

    def timed(kind, label, route, body, count=1, required=()):
        remaining = args.budget_seconds - (time.monotonic() - started)
        if remaining < 5:
            raise TimeoutError("benchmark wall-clock budget exhausted")
        before = time.monotonic()
        row = {
            "kind": kind,
            "label": label,
            "items": count,
            "request_sha256": hashlib.sha256(
                json.dumps(body, sort_keys=True).encode()
            ).hexdigest(),
        }
        try:
            response = request(base, route, body, min(args.request_seconds, remaining))
            validate(kind, response, count, required)
            row.update(
                complete=True,
                usage=response.get("usage"),
                execution=response.get("execution"),
            )
            if kind == "ocr":
                row["text_characters"] = len(response["data"][0]["text"])
            return response
        except Exception as error:
            row.update(complete=False, error=str(error))
            if isinstance(error, urllib.error.HTTPError):
                row["response"] = error.read(4096).decode(errors="replace")
            raise
        finally:
            row["seconds"] = time.monotonic() - before
            if row.get("complete"):
                row["items_per_second"] = count / row["seconds"]
            with report_lock:
                report["requests"].append(row)
                print(json.dumps(row), flush=True)
                (args.output / "report.json").write_text(
                    json.dumps(report, indent=2) + "\n"
                )

    try:
        while time.monotonic() - started < 30:
            if server.poll() is not None:
                raise RuntimeError("owned inference server exited; inspect server.log")
            try:
                request(base, "/models", None, 2)
                break
            except (OSError, ValueError):
                time.sleep(0.2)
        else:
            raise TimeoutError("inference startup exceeded 30 seconds")
        text = (
            "John works at Apple. Alice works at Google. "
            "The reviewed records describe meetings in New York and travel to Florida. "
        ) * 12
        if "ocr" in args.stages:
            corpus = root / "zig/pkg/inference/scripts/lightonocr3/corpus"
            case = json.loads((corpus / "cases.json").read_text())["cases"][1]
            image = (corpus / case["image"]).resolve().read_bytes()
            body = {
                "model": ocr,
                "images": [
                    {"url": "data:image/png;base64," + base64.b64encode(image).decode()}
                ],
                "max_tokens": case["max_new_tokens"],
            }
            for label in ("synthetic-receipt-cold", "synthetic-receipt-warm"):
                timed("ocr", label, "/read", body, required=case["required"])
            if args.ocr_concurrency > 1:
                pair_started = time.monotonic()
                with ThreadPoolExecutor(max_workers=args.ocr_concurrency) as pool:
                    futures = [
                        pool.submit(
                            timed,
                            "ocr",
                            f"synthetic-receipt-concurrent-{i}",
                            "/read",
                            body,
                            required=case["required"],
                        )
                        for i in range(args.ocr_concurrency)
                    ]
                    for future in futures:
                        future.result()
                seconds = time.monotonic() - pair_started
                report["ocr_concurrent"] = {
                    "completed": len(futures),
                    "seconds": seconds,
                    "pages_per_second": len(futures) / seconds,
                }
            if args.dense_image:
                image = args.dense_image.read_bytes()
                timed(
                    "ocr",
                    "real-dense-page-warm-model",
                    "/read",
                    {
                        "model": ocr,
                        "images": [
                            {
                                "url": "data:image/png;base64,"
                                + base64.b64encode(image).decode()
                            }
                        ],
                        "max_tokens": 8192,
                    },
                )
        if "embed" in args.stages:
            for label in ("text-chunks-cold", "text-chunks-warm"):
                timed(
                    "embed",
                    label,
                    "/embed",
                    {
                        "model": embed,
                        "input": [
                            (f"Document {i} {label}. " + text)[:512] for i in range(4)
                        ],
                        "task_type": "RETRIEVAL_DOCUMENT",
                        "dimensions": 768,
                    },
                    4,
                )
        if "extract" in args.stages:
            schema = {
                "entities": setting("DefaultEntityLabels").split(","),
                "relations": [
                    {"type": name}
                    for name in setting("DefaultRelationLabels").split(",")
                ],
            }
            for label in ("relation-window-cold", "relation-window-warm"):
                timed(
                    "extract",
                    label,
                    "/extract",
                    {
                        "schema_version": 2,
                        "model": extractor,
                        "schema": schema,
                        "inputs": [{"id": "window-0", "content": text + " " + label}],
                        "options": {
                            "include_confidence": True,
                            "include_spans": True,
                            "offset_unit": "unicode_codepoints",
                            "long_document": {"mode": "window"},
                        },
                    },
                )
    except Exception as error:
        report["error"] = str(error)
    finally:
        server.terminate()
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        log.close()
        report.update(seconds=time.monotonic() - started, hardware_after=hardware())
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 1 if report.get("error") else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dense-image", type=Path)
    parser.add_argument("--ocr-concurrency", type=int, choices=(1, 2), default=1)
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=("ocr", "embed", "extract"),
        default=["ocr", "embed", "extract"],
    )
    parser.add_argument("--extractor-model")
    parser.add_argument("--budget-seconds", type=float, default=180)
    parser.add_argument("--request-seconds", type=float, default=60)
    arguments = parser.parse_args()
    if not (0 < arguments.request_seconds <= arguments.budget_seconds <= 600):
        parser.error("require 0 < request-seconds <= budget-seconds <= 600")
    raise SystemExit(run(arguments))
