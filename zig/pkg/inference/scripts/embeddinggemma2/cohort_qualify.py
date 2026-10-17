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

"""Compare native/Metal media cohorts with independent execution of each group.

Checks ordering, unequal audio lengths, mixed prompts, video captions, partial
errors and concurrent submissions. Same-backend replay complements the pinned
PyTorch oracle: this is a correctness/scheduling receipt, not a latency gate.
The owned server uses the declared process budget, and no codec/model runtime
libraries are installed by this tool. All generated reports remain local.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import sys
import time
import urllib.request
import wave

if __package__:
    from . import compare
else:
    import compare
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qualify_media_chunks as chunks


def sound(samples, frequency):
    data = io.BytesIO()
    with wave.open(data, "wb") as output:
        output.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        output.writeframes(
            b"".join(
                struct.pack(
                    "<h", round(8000 * math.sin(i * 2 * math.pi * frequency / 16000))
                )
                for i in range(samples)
            )
        )
    return chunks.media("audio/wav", data.getvalue())


def cases(video=None):
    image = chunks.media("image/png", chunks.png())
    clips = [
        sound(n, f) for n, f in [(4800, 220), (16000, 440), (7200, 330), (12000, 550)]
    ]
    movie = chunks.fixtures(video)["video"][0]

    def text(value):
        return {"type": "text", "text": value}

    return {
        "text": [
            {"title": f"row {i}", "content": [text(f"Describe text {i}.")]}
            for i in range(4)
        ],
        "image": [{"content": [image]} for _ in range(4)],
        "audio": [{"content": [clip]} for clip in clips],
        "mixed": [
            {
                "title": f"item {i}",
                "content": [text(f"Describe item {i}."), clips[i], image],
            }
            for i in range(4)
        ],
        "mixed_order": [
            {"content": parts}
            for parts in [
                [image, text("Describe."), clips[0]],
                [clips[0], image, text("Describe.")],
            ]
        ],
        "mixed_many": [
            {
                "content": [
                    text(f"Many parts {i}."),
                    image,
                    clips[i],
                    image,
                    clips[i + 1],
                    image,
                    clips[i + 2],
                ]
            }
            for i in range(2)
        ],
        # A ready image/text pair must run before this multi-wave audio group.
        "mixed_ready": [
            {"content": [text("Describe the ready image."), image]},
            {"content": [text("Describe these sounds."), *([clips[0]] * 9)]},
            {"content": [text("A ready text document.")]},
        ],
        "video": [{"content": [text(f"Describe video {i}."), movie]} for i in range(2)],
    }


@contextmanager
def server(args, serial):
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    log_path = args.output.with_suffix(".serial.log" if serial else ".cohort.log")
    env = dict(
        os.environ,
        ANTFLY_INFERENCE_REQUIRED_BACKEND=args.backend,
        ANTFLY_GEMMA4_GROUP_BROKER_TRACE="1",
        ANTFLY_GEMMA4_AUDIO_BROKER_TRACE="1",
    )
    for flag in (
        "ANTFLY_GEMMA4_DISABLE_IMAGE_BROKER",
        "ANTFLY_GEMMA4_DISABLE_AUDIO_BROKER",
    ):
        env[flag] = "1" if serial else "0"
    trace_dir = args.output.with_suffix(
        ".serial.traces" if serial else ".cohort.traces"
    )
    trace_dir = trace_dir / str(time.time_ns())
    trace_dir.mkdir(parents=True, exist_ok=True)
    args.active_trace_dir = trace_dir
    env["ANTFLY_EMBED_TRACE_DIR"] = str(trace_dir.resolve())
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                str(args.binary.resolve()),
                "run",
                "--models-dir",
                str(args.model_dir.resolve().parent),
                "--port",
                str(port),
                "--host-budget-mb",
                "6144",
                "--backend-budget-mb",
                "12288",
                "--combined-budget-mb",
                "18432",
                "--scratch-budget-mb",
                "8192",
                "--process-memory-budget-mb",
                str(args.process_memory_budget_mb),
            ],
            env=env,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        url = f"http://127.0.0.1:{port}"
        try:
            for _ in range(300):
                if process.poll() is not None:
                    raise RuntimeError(
                        "owned server exited: " + log_path.read_text()[-8000:]
                    )
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("server health deadline exceeded")
            yield url
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def embed(url, args, groups, policy="fail_fast"):
    started = time.perf_counter()
    with chunks.post(
        url,
        "embed",
        {
            "model": args.model_dir.name,
            "input": groups,
            "dimensions": 128,
            "error_policy": policy,
        },
    ) as response:
        result = json.load(response)
    return result, time.perf_counter() - started


def vectors(response):
    return [row["embedding"] for row in response["data"]]


def qualify(args):
    groups = cases(args.video)
    report = dict(
        version=1,
        status="running",
        backend=args.backend,
        binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        process_memory_budget_mb=args.process_memory_budget_mb,
        corpus_sha256=hashlib.sha256(
            json.dumps(groups, sort_keys=True).encode()
        ).hexdigest(),
        tool_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="same-backend independent groups versus prepared cohorts",
        production_qualified=False,
        cases={},
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    compare.publish(args.output, report)
    flat_texts = [
        "Explain this event. " * 128,
        "A red bicycle.",
        "Describe this scene. " * 64,
        "A blue car.",
        "A green train.",
        "A yellow bus.",
    ]
    flat_reference = []
    with server(args, True) as url:
        expected = {}
        identity = None
        for name, batch in groups.items():
            expected[name] = []
            for group in batch:
                result, _ = embed(url, args, [group])
                if identity is None:
                    identity = result["model_identity"]
                if result["model_identity"] != identity:
                    raise AssertionError("serial generation changed")
                expected[name].append(vectors(result)[0])
        for value in flat_texts:
            with chunks.post(
                url,
                "embed",
                {"model": args.model_dir.name, "input": [value], "dimensions": 128},
            ) as response:
                result = json.load(response)
            if result["model_identity"] != identity:
                raise AssertionError("flat text generation changed")
            flat_reference.append(vectors(result)[0])
    with server(args, False) as url:
        with chunks.post(
            url,
            "embed",
            {"model": args.model_dir.name, "input": flat_texts, "dimensions": 128},
        ) as response:
            result = json.load(response)
        if result["model_identity"] != identity:
            raise AssertionError("flat text cohort generation changed")
        actual = vectors(result)
        if len(actual) != len(flat_reference):
            raise AssertionError("flat text batch lost output rows")
        report["flat_text"] = [
            compare.assert_agreement(v, r, 1e-5) for v, r in zip(actual, flat_reference)
        ]
        for name, batch in groups.items():
            samples = []
            readiness = []
            for _ in range(args.iterations):
                log_path = args.output.with_suffix(".cohort.log")
                log_start = log_path.stat().st_size
                result, elapsed = embed(url, args, batch)
                if name == "mixed_ready":
                    trace = log_path.read_bytes()[log_start:].decode("utf-8")
                    ready = re.search(
                        r"gemma4-group-ready items=2 indexes=\{ 0, 2 \}", trace
                    )
                    later_audio = trace.find(
                        "gemma4-group-tower kind=audio", ready.end() if ready else 0
                    )
                    if ready is None or later_audio < 0:
                        raise AssertionError(
                            "ready image/text groups waited for later audio towers"
                        )
                    if (
                        "gemma4-group-prepare overlap=true"
                        not in trace[: ready.start()]
                    ):
                        raise AssertionError(
                            "next preparation wave was not started before ready work"
                        )
                    readiness.append(
                        {
                            "ready_groups": [0, 2],
                            "before_later_audio_tower": True,
                            "next_preparation_started": True,
                        }
                    )
                if result["model_identity"] != identity:
                    raise AssertionError("cohort generation changed")
                actual = vectors(result)
                if len(actual) != len(expected[name]):
                    raise AssertionError("missing/reordered group results")
                agreement = [
                    compare.assert_agreement(v, r, 1e-5)
                    for v, r in zip(actual, expected[name])
                ]
                samples.append(elapsed)
            report["cases"][name] = dict(
                parity=agreement, completed_timing=compare.timing(samples)
            )
            if readiness:
                report["cases"][name]["readiness"] = readiness
            print(name, "pass", flush=True)
            compare.publish(args.output, report)
        # Independent submitters borrow different request arenas and controls.
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(embed, url, args, [group]) for group in groups["mixed"]
            ]
            for future, reference in zip(futures, expected["mixed"]):
                result, _ = future.result()
                compare.assert_agreement(vectors(result)[0], reference, 1e-5)
        report["concurrent_requests"] = 4
        bad = {"content": [sound(1, 440)]}
        result, _ = embed(
            url, args, [groups["audio"][0], bad, groups["audio"][1]], "per_item"
        )
        successful = {row["index"]: row["embedding"] for row in result["data"]}
        if set(successful) != {0, 2} or not result.get("errors"):
            raise AssertionError("partial failure lost item identity")
        compare.assert_agreement(successful[0], expected["audio"][0], 1e-5)
        compare.assert_agreement(successful[2], expected["audio"][1], 1e-5)
        report["partial_errors"] = len(result["errors"])
        bad_mixed = {"content": [groups["mixed"][0]["content"][2], sound(1, 440)]}
        result, _ = embed(
            url, args, [groups["mixed"][0], bad_mixed, groups["mixed"][1]], "per_item"
        )
        successful = {row["index"]: row["embedding"] for row in result["data"]}
        if set(successful) != {0, 2} or not result.get("errors"):
            raise AssertionError("mixed tower failure lost group identity")
        compare.assert_agreement(successful[0], expected["mixed"][0], 1e-5)
        compare.assert_agreement(successful[2], expected["mixed"][1], 1e-5)
        report["mixed_partial_errors"] = len(result["errors"])
    flat_traces = [
        json.loads(path.read_text())
        for path in args.active_trace_dir.glob("embed-*.json")
    ]
    shapes = [
        shape
        for trace in flat_traces
        if trace.get("success") and trace.get("input") == flat_texts
        for shape in trace["shapes"]
    ]
    if (
        not shapes
        or max(shape["items"] for shape in shapes) <= 1
        or sum(shape["items"] for shape in shapes) != len(flat_texts)
        or any(shape["padded_tokens"] > 8192 for shape in shapes)
    ):
        raise AssertionError("no bounded physical flat-text batching observed")
    report["flat_text_encoder_shapes"] = shapes
    text = args.output.with_suffix(".cohort.log").read_text()
    trace = re.findall(r"gemma4-(group|audio)-cohort items=(\d+) execution=(\w+)", text)
    for expected_kind in ("group", "audio"):
        if not any(
            kind == expected_kind and int(n) > 1 and execution == "native_batch"
            for kind, n, execution in trace
        ):
            raise AssertionError(f"no real {expected_kind} cohort execution observed")
    towers = re.findall(r"gemma4-group-tower kind=(image|audio) items=(\d+)", text)
    for kind in ("image", "audio"):
        if not any(k == kind and int(n) > 1 for k, n in towers):
            raise AssertionError(f"no cross-group {kind} tower batching observed")
    if "gemma4-group-prepare overlap=true" not in text:
        raise AssertionError("no mixed tower preparation overlap observed")
    if hashlib.sha256(args.binary.read_bytes()).hexdigest() != report["binary_sha256"]:
        raise AssertionError("qualification binary changed")
    report.update(
        status="pass",
        model_identity=identity,
        execution_observations=trace,
        tower_observations=towers,
        preparation_overlap=True,
        serial_log_sha256=hashlib.sha256(
            args.output.with_suffix(".serial.log").read_bytes()
        ).hexdigest(),
        cohort_log_sha256=hashlib.sha256(
            args.output.with_suffix(".cohort.log").read_bytes()
        ).hexdigest(),
    )
    compare.publish(args.output, report)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--model-dir", type=Path, required=True)
    p.add_argument("--backend", choices=("native", "metal"), required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--video", type=Path)
    p.add_argument("--iterations", type=int, default=2)
    p.add_argument("--process-memory-budget-mb", type=int, default=8192)
    args = p.parse_args()
    if (
        not 1 <= args.iterations <= 100
        or not 1 <= args.process_memory_budget_mb <= 65536
    ):
        p.error("invalid qualification limits")
    qualify(args)


if __name__ == "__main__":
    main()
