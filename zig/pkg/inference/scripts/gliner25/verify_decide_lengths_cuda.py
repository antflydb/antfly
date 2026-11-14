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

"""Compare original Decide CUDA API and HTTP outputs at attention route boundaries.

Runs upstream, native core and HTTP in separate processes. Exact prepared token
IDs are checked, not estimated word lengths. Outputs remain available on failure.
This is a correctness gate, not a latency benchmark.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import time

from benchmark_decide_hotel_cuda import TASKS, cases, confidences, normalized

LENGTHS = (127, 128, 129, 255, 256, 257, 511, 512)
BATCHES = (1, 3, 4, 8, 32)


def python_reference(args):
    import torch
    from gliner2 import AutoExtractor, Schema
    import importlib.metadata

    torch.set_num_threads(2)
    model = AutoExtractor.from_pretrained(str(args.model)).to("cuda").eval()
    (args.out / "python-versions.json").write_text(
        json.dumps(
            dict(
                torch=torch.__version__,
                gliner2=importlib.metadata.version("gliner2"),
                transformers=importlib.metadata.version("transformers"),
                gpu=torch.cuda.get_device_name(),
                dtype=str(next(model.parameters()).dtype),
                allow_tf32=torch.backends.cuda.matmul.allow_tf32,
            )
        )
    )
    schema = Schema.from_dict(
        {
            "classifications": [
                {"task": name, **config} for name, config in TASKS.items()
            ]
        }
    )
    fixture, expected = [], []
    seeds = [case["texts"][0] for case in cases()[:8]]
    for length in LENGTHS:
        texts, ids = [], []
        for seed in seeds:
            padding = 0
            for _ in range(length + 1):
                text = seed + " please" * padding
                prepared = model.processor.collate_fn_inference(
                    [(text, schema)], error_policy="raise"
                )
                tokens = prepared.input_ids[0].tolist()
                if len(tokens) == length:
                    break
                padding += length - len(tokens)
                if padding < 0:
                    raise AssertionError("seed exceeds target length")
            else:
                raise AssertionError(f"cannot construct {length} tokens")
            texts.append(text)
            ids.append(tokens)
        for batch in BATCHES:
            selected = [texts[i % len(texts)] for i in range(batch)]
            fixture.append(dict(id=f"s{length}_b{batch}", texts=selected, tasks=TASKS))
            with torch.inference_mode():
                outputs = (
                    [
                        model.extract(
                            selected[0],
                            schema,
                            include_spans=True,
                            include_confidence=True,
                        )
                    ]
                    if batch == 1
                    else model.batch_extract(
                        selected,
                        schema,
                        batch_size=batch,
                        include_spans=True,
                        include_confidence=True,
                    )
                )
            torch.cuda.synchronize()
            expected.append(
                dict(
                    length=length,
                    batch=batch,
                    outputs=outputs,
                    input_ids=[ids[i % len(ids)] for i in range(batch)],
                )
            )
            print(f"upstream s{length} b{batch}", flush=True)
    (args.out / "cases.json").write_text(
        json.dumps(dict(format_version=1, cases=fixture))
    )
    (args.out / "python.json").write_text(json.dumps(expected))


def stop(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def native_core(args, fixture):
    with (args.out / "native.stderr").open("w") as log:
        proc = subprocess.Popen(
            [
                str(args.native),
                "--model-dir",
                str(args.model),
                "--backend",
                "cuda",
                "--cases",
                str(args.out / "cases.json"),
                "--worker",
                "1",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            start_new_session=True,
        )
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)

        def receive():
            if not selector.select(300):
                raise TimeoutError("native CUDA worker timed out")
            return json.loads(proc.stdout.readline())

        try:
            receive()
            outputs = []
            with (args.out / "native.jsonl").open("w") as raw:
                for index in range(len(fixture)):
                    proc.stdin.write(
                        json.dumps(dict(op="run", request_id=index, case_index=index))
                        + "\n"
                    )
                    proc.stdin.flush()
                    row = receive()
                    raw.write(json.dumps(row) + "\n")
                    raw.flush()
                    outputs.append(row)
            return outputs
        finally:
            stop(proc)
            selector.close()


def native_http(args, fixture):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with (args.out / "http.stderr").open("w") as log:
        proc = subprocess.Popen(
            [
                str(args.http),
                "run",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--models-dir",
                str(args.model.parent),
                "--ml-dir",
                str(args.out / "state"),
                "--max-loaded-models",
                "1",
            ],
            stdout=log,
            stderr=log,
            start_new_session=True,
            env={**os.environ, "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda"},
        )
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
        try:
            deadline = time.monotonic() + 60
            while True:
                try:
                    client.request("GET", "/healthz")
                    response = client.getresponse()
                    response.read()
                    if response.status == 200:
                        break
                except OSError:
                    client.close()
                if proc.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("HTTP server failed readiness")
                time.sleep(0.1)
            outputs = []
            with (args.out / "http.jsonl").open("w") as raw:
                for case in fixture:
                    schema = {
                        "classifications": [
                            dict(
                                name=k,
                                labels=v["labels"],
                                **(
                                    {
                                        "multi_label": True,
                                        "threshold": v["cls_threshold"],
                                    }
                                    if v.get("multi_label")
                                    else {}
                                ),
                            )
                            for k, v in TASKS.items()
                        ]
                    }
                    body = dict(
                        schema_version=2,
                        model=args.model.name,
                        schema=schema,
                        inputs=[
                            dict(id=str(i), content=text)
                            for i, text in enumerate(case["texts"])
                        ],
                        options=dict(include_spans=True, include_confidence=True),
                    )
                    client.request(
                        "POST",
                        "/ai/v1/extract",
                        json.dumps(body),
                        {"Content-Type": "application/json"},
                    )
                    response = client.getresponse()
                    data = json.loads(response.read())
                    raw.write(
                        json.dumps(
                            dict(case=case["id"], status=response.status, body=data)
                        )
                        + "\n"
                    )
                    raw.flush()
                    if response.status != 200:
                        raise AssertionError((case["id"], response.status, data))
                    rows = []
                    assert len(data["data"]) == len(case["texts"])
                    for i, row in enumerate(data["data"]):
                        assert row["id"] == str(i)
                        converted = {k: [] for k in TASKS}
                        for c in row["classifications"]:
                            converted[c["name"]].append(
                                dict(label=c["label"], confidence=c["score"])
                            )
                        rows.append(converted)
                    outputs.append(dict(outputs=rows))
            return outputs
        finally:
            client.close()
            stop(proc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--native", type=Path)
    parser.add_argument("--http", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--python-worker", action="store_true")
    args = parser.parse_args()
    for key in ("model", "native", "http", "out"):
        if getattr(args, key) is not None:
            setattr(args, key, getattr(args, key).resolve())
    if args.python_worker:
        python_reference(args)
        return
    if args.native is None or args.http is None:
        parser.error("--native and --http binaries are required")
    args.out.mkdir(exist_ok=False)

    def sha(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    (args.out / "provenance.json").write_text(
        json.dumps(
            dict(
                model_sha256=sha(args.model / "model.safetensors"),
                native_sha256=sha(args.native),
                http_sha256=sha(args.http),
                verifier_sha256=sha(Path(__file__)),
                controls={
                    k: v
                    for k, v in os.environ.items()
                    if k.startswith(
                        (
                            "ANTFLY_CUDA_",
                            "ANTFLY_INFERENCE_CUDA_",
                            "TERMITE_CUDA_",
                            "CUBLAS_",
                            "NVIDIA_TF32_",
                            "CUDA_VISIBLE_DEVICES",
                        )
                    )
                },
                lengths=LENGTHS,
                batches=BATCHES,
            ),
            indent=2,
        )
        + "\n"
    )
    with open("/tmp/antfly-decide-cuda.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with (args.out / "python.log").open("w") as log:
            subprocess.run(
                [
                    sys.executable,
                    __file__,
                    "--python-worker",
                    "--model",
                    str(args.model),
                    "--out",
                    str(args.out),
                ],
                stdout=log,
                stderr=log,
                check=True,
                timeout=1200,
            )
        fixture = json.loads((args.out / "cases.json").read_text())["cases"]
        expected = json.loads((args.out / "python.json").read_text())
        core = native_core(args, fixture)
        http = native_http(args, fixture)
    checks = []
    for case, reference, native, served in zip(
        fixture, expected, core, http, strict=True
    ):
        assert native["prepared_input_ids"] == reference["input_ids"], case["id"]
        for arm, result in (("native", native), ("http", served)):
            left, right = confidences(arm, result), confidences("python", reference)
            same_labels = normalized(arm, result) == normalized("python", reference)
            errors = [
                abs(a[k] - b[k])
                for a, b in zip(left, right, strict=True)
                for k in a.keys() & b.keys()
            ]
            checks.append(
                dict(
                    case=case["id"],
                    arm=arm,
                    labels_match=same_labels,
                    max_selected_confidence_error=max(errors),
                    core_stream_f32_attention_calls=native["cuda_transfers"][
                        "stream_f32_attention_calls"
                    ],
                    core_materialized_f16_attention_calls=native["cuda_transfers"].get(
                        "materialized_f16_attention_calls"
                    ),
                )
            )
    (args.out / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")
    failures = [
        row
        for row in checks
        if not row["labels_match"]
        or row["max_selected_confidence_error"] > 5e-4
        or row["core_stream_f32_attention_calls"] != 24
        or row["core_materialized_f16_attention_calls"] != 0
    ]
    if failures:
        raise AssertionError(f"Length parity failed: {failures}")
    print(
        f"Passed {len(checks)} core/HTTP comparisons across exact prepared lengths {LENGTHS} and batches {BATCHES}"
    )


if __name__ == "__main__":
    main()
