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

"""Compare warm HTTP transcription on the same audio and greedy settings.

Start both servers with CUDA required, matching Whisper weights and full audio
context. whisper.cpp: flash attention on, --beam-size 1 --best-of 1. Both
servers must run on the same otherwise-idle GPU. Retain server placement logs.
"""

import argparse
import base64
import hashlib
import json
import math
import os
import random
import re
import statistics
import time
import urllib.request
import uuid
import wave
from pathlib import Path


def words(text):
    return re.findall(r"\w+", text.casefold())


def wer(reference, hypothesis):
    reference, hypothesis = words(reference), words(hypothesis)
    previous = list(range(len(hypothesis) + 1))
    for i, word in enumerate(reference, 1):
        current = [i]
        for j, token in enumerate(hypothesis, 1):
            current.append(
                min(current[-1] + 1, previous[j] + 1, previous[j - 1] + (word != token))
            )
        previous = current
    return previous[-1] / max(1, len(reference))


def paired_speedup_interval(antfly, whisper_cpp):
    """Deterministic paired bootstrap of the warm median latency ratio."""
    rng = random.Random(0)
    ratios = []
    for _ in range(2000):
        indices = [rng.randrange(len(antfly)) for _ in antfly]
        ratios.append(
            statistics.median(whisper_cpp[i] for i in indices)
            / statistics.median(antfly[i] for i in indices)
        )
    ratios.sort()
    return [ratios[49], ratios[1949]]


def post(url, body, content_type):
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": content_type}
    )
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    return time.perf_counter() - started, result


def multipart(audio, language, timestamps):
    boundary = "antfly-whisper-" + uuid.uuid4().hex
    body = bytearray()
    fields = {
        "language": language,
        "temperature": "0",
        "temperature_inc": "0.2",
        "beam_size": "1",
        "best_of": "1",
        "response_format": "verbose_json",
        "token_timestamps": "false",
        "no_context": "true",
        "no_timestamps": "false" if timestamps else "true",
    }
    for name, value in fields.items():
        body.extend(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode()
        )
    body.extend(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="audio.wav"\r\nContent-Type: audio/wav\r\n\r\n'.encode()
    )
    body.extend(audio)
    body.extend(f"\r\n--{boundary}--\r\n".encode())
    return bytes(body), "multipart/form-data; boundary=" + boundary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly-url", default="http://127.0.0.1:18081/ai/v1/read")
    parser.add_argument("--whisper-url", default="http://127.0.0.1:18082/inference")
    parser.add_argument("--model", default="openai/whisper-small")
    parser.add_argument("--audio", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--language", default="en")
    parser.add_argument("--trials", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--timestamps", action="store_true")
    parser.add_argument("--max-wer", type=float, default=0.10)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.trials < 3 or args.warmup < 1:
        parser.error("at least three measured trials and one warmup are required")
    audio = args.audio.read_bytes()
    with wave.open(str(args.audio)) as wav:
        seconds = wav.getnframes() / wav.getframerate()
    reference = args.reference.read_text()
    antfly = json.dumps(
        {
            "model": args.model,
            "audio": "data:audio/wav;base64," + base64.b64encode(audio).decode(),
            "audio_options": {"language": args.language, "timestamps": args.timestamps},
        }
    ).encode()
    whisper, whisper_type = multipart(audio, args.language, args.timestamps)
    requests = {
        "antfly": (args.antfly_url, antfly, "application/json"),
        "whisper_cpp": (args.whisper_url, whisper, whisper_type),
    }
    results = {name: [] for name in requests}
    for trial in range(args.warmup + args.trials):
        for name in list(requests) if trial % 2 == 0 else list(reversed(requests)):
            elapsed, result = post(*requests[name])
            if name == "antfly":
                item = result["data"][0]
                if item.get("error"):
                    raise RuntimeError(str(item["error"]))
                if item.get("status") != "complete":
                    raise RuntimeError(
                        f"incomplete Antfly recognition: {item.get('status')}"
                    )
                text = item["text"]
            else:
                text = result["text"]
            if trial >= args.warmup:
                results[name].append(
                    {"seconds": elapsed, "text": text, "wer": wer(reference, text)}
                )
    summary = {}
    for name, values in results.items():
        durations = sorted(item["seconds"] for item in values)
        summary[name] = {
            "median_seconds": statistics.median(durations),
            "p95_seconds": durations[math.ceil(len(durations) * 0.95) - 1],
            "max_wer": max(item["wer"] for item in values),
            "real_time_factor": statistics.median(durations) / seconds,
        }
    speedup = (
        summary["whisper_cpp"]["median_seconds"] / summary["antfly"]["median_seconds"]
    )
    interval = paired_speedup_interval(
        [item["seconds"] for item in results["antfly"]],
        [item["seconds"] for item in results["whisper_cpp"]],
    )
    quality_ok = all(item["max_wer"] <= args.max_wer for item in summary.values())
    quality_ok = (
        quality_ok and summary["antfly"]["max_wer"] <= summary["whisper_cpp"]["max_wer"]
    )
    report = {
        "model": args.model,
        "decoding": "greedy first pass; default temperature fallback",
        "audio_sha256": hashlib.sha256(audio).hexdigest(),
        "audio_seconds": seconds,
        "language": args.language,
        "timestamps": args.timestamps,
        "reference": reference,
        "maximum_wer": args.max_wer,
        "client_cpu_affinity": sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None,
        "speedup_95_percent_interval": interval,
        "warmup": args.warmup,
        "trials": args.trials,
        "summary": summary,
        "antfly_speedup": speedup,
        "quality_passed": quality_ok,
        "performance_passed": quality_ok and interval[0] > 1,
        "measurements": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "summary",
                    "antfly_speedup",
                    "speedup_95_percent_interval",
                    "quality_passed",
                    "performance_passed",
                )
            },
            indent=2,
        )
    )
    return 0 if report["performance_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
