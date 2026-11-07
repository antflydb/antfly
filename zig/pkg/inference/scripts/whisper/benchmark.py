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
import sys
import urllib.error
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


def extract_antfly_text(result):
    """One submitted audio item must finish successfully, including silence."""
    if not isinstance(result, dict) or result.get("error") is not None:
        raise ValueError("Antfly response contains an error or is not an object")
    data = result.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise ValueError("Antfly response must contain exactly one audio result")
    item = data[0]
    if item.get("error") is not None or item.get("status") != "complete":
        raise ValueError("Antfly audio recognition did not complete without error")
    if not isinstance(item.get("text"), str):
        raise ValueError("Antfly complete result is missing transcript text")
    return item["text"]


def post_record(url, body, content_type):
    """Keep the exact response bytes, including HTTP/JSON failures."""
    started = time.perf_counter()
    raw = None
    status = None
    result = None
    error = None
    try:
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": content_type}
        )
        try:
            response = urllib.request.urlopen(request, timeout=300)
        except urllib.error.HTTPError as failure:
            with failure:
                status = failure.code
                raw = failure.read()
            raise RuntimeError(f"HTTP {status}") from failure
        with response:
            status = response.status
            raw = response.read()
        if not 200 <= status < 300:
            raise RuntimeError(f"HTTP {status}")
        def reject_constant(value):
            raise ValueError("nonfinite JSON constant: " + value)
        result = json.loads(raw, parse_constant=reject_constant)
    except Exception as failure:
        error = {"type": type(failure).__name__, "message": str(failure)}
    return {
        "seconds": time.perf_counter() - started,
        "http_status": status,
        "response_bytes_base64": base64.b64encode(raw).decode() if raw is not None else None,
        "response": result,
        "error": error,
    }


def antfly_baseline(args, body, audio, audio_seconds, reference):
    # Share ONLY the existing deterministic statistics. Process/model/backend
    # identity remains the enclosing campaign's responsibility.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from paired_benchmark import balanced_pair_order, distribution, paired_log_ratio_ci

    results = {"antfly": [], "baseline_antfly": []}
    endpoints = {
        "antfly": args.antfly_url,
        "baseline_antfly": args.baseline_antfly_url,
    }
    report = {
        "schema": "antfly.whisper.baseline_comparison.v1",
        "scope": "Warm complete HTTP recognition; excludes model/process loading. No backend, hardware, precision or live model attestation inferred.",
        "model": args.model,
        "audio_sha256": hashlib.sha256(audio).hexdigest(),
        "audio_seconds": audio_seconds,
        "language": args.language,
        "timestamps": args.timestamps,
        "decoding": "Identical JSON request; greedy first pass and server default temperature fallback. Outer campaign must attest both server policies.",
        "request_sha256": hashlib.sha256(body).hexdigest(),
        "endpoints": endpoints,
        "reference": reference,
        "quality_scope": "Transcript WER only; raw timestamp fields are retained, not timestamp accuracy qualification.",
        "maximum_wer": args.max_wer,
        "quality_policy": "Both arms meet reference WER cap; candidate WER is no worse in every pair including warmup.",
        "warmup": args.warmup,
        "trials": args.trials,
        "require_faster": args.require_faster,
        "regression_limits": {"median_ratio_max": 1.05, "p95_ratio_max": 1.10},
        "client_cpu_affinity": sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None,
        "measurements": results,
        "complete": False,
        "aborted": None,
        "baseline_candidate_speedup_ci": None,
        "candidate_baseline_latency_ratio": None,
        "quality_passed": False,
        "regression_passed": False,
        "faster_passed": False,
        "performance_passed": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")

    save()
    aborted = False
    for pair in range(args.warmup + args.trials):
        phase = "warmup" if pair < args.warmup else "measurement"
        for target in balanced_pair_order(pair + 1, "antfly", "baseline_antfly"):
            # Persist even an interrupted pending attempt; completed error
            # responses remain literal failures and are never replaced/retried.
            row = {"pair": pair + 1, "phase": phase, "state": "pending"}
            results[target].append(row)
            save()
            row.update(post_record(endpoints[target], body, "application/json"))
            row["state"] = "failed"
            if row["error"] is None:
                try:
                    row["text"] = extract_antfly_text(row["response"])
                    row["wer"] = wer(reference, row["text"])
                    if not math.isfinite(row["seconds"]) or row["seconds"] <= 0:
                        raise ValueError("nonpositive or nonfinite HTTP duration")
                    row["state"] = "complete"
                except (ValueError, TypeError, KeyError) as failure:
                    row["error"] = {"type": type(failure).__name__, "message": str(failure)}
            save()
            if row["error"] is not None:
                report["aborted"] = {
                    "target": target,
                    "pair": pair + 1,
                    "error": row["error"],
                }
                aborted = True
                break
        if aborted:
            break
    report["complete"] = all(
        len(rows) == args.warmup + args.trials
        and all(row["state"] == "complete" for row in rows)
        for rows in results.values()
    )
    measured = {
        target: [
            row for row in rows
            if row["phase"] == "measurement" and row["state"] == "complete"
        ]
        for target, rows in results.items()
    }
    summary = {}
    for target, rows in measured.items():
        if rows:
            stats = distribution(row["seconds"] for row in rows)
            summary[target] = {
                "samples": len(rows),
                "median_seconds": stats["median"],
                "p95_seconds": stats["p95"],
                "max_wer": max(row["wer"] for row in rows),
                "real_time_factor": stats["median"] / audio_seconds,
            }
    report["summary"] = summary
    if report["complete"]:
        candidate, baseline = summary["antfly"], summary["baseline_antfly"]
        quality_ok = all(
            row["wer"] <= args.max_wer for rows in results.values() for row in rows
        )
        quality_ok = quality_ok and all(
            a["wer"] <= b["wer"]
            for a, b in zip(results["antfly"], results["baseline_antfly"])
        )
        report["quality_passed"] = quality_ok
        ratios = {
            "median": candidate["median_seconds"] / baseline["median_seconds"],
            "p95": candidate["p95_seconds"] / baseline["p95_seconds"],
        }
        report["candidate_baseline_latency_ratio"] = ratios
        report["regression_passed"] = (
            quality_ok and ratios["median"] <= 1.05 and ratios["p95"] <= 1.10
        )
        interval = paired_log_ratio_ci([
            (b["seconds"], a["seconds"])
            for a, b in zip(measured["antfly"], measured["baseline_antfly"])
        ])
        report["baseline_candidate_speedup_ci"] = interval
        report["faster_passed"] = quality_ok and interval["lower_95"] > 1
        report["performance_passed"] = report["regression_passed"] and (
            not args.require_faster or report["faster_passed"]
        )
    save()
    print(json.dumps({
        key: report[key]
        for key in (
            "complete", "summary", "quality_passed", "regression_passed",
            "faster_passed", "performance_passed",
        )
    }, indent=2))
    return 0 if report["performance_passed"] else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly-url", default="http://127.0.0.1:18081/ai/v1/read")
    comparison = parser.add_mutually_exclusive_group()
    comparison.add_argument("--whisper-url")
    comparison.add_argument("--baseline-antfly-url")
    parser.add_argument("--require-faster", action="store_true", help="Also require paired speedup CI lower bound > 1 in Antfly baseline mode")
    parser.add_argument("--model", default="openai/whisper-small")
    parser.add_argument("--audio", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--language", default="en")
    parser.add_argument("--trials", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--timestamps", action="store_true")
    parser.add_argument("--max-wer", type=float, default=0.10)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.trials < 3 or args.warmup < 1:
        parser.error("at least three measured trials and one warmup are required")
    if args.require_faster and not args.baseline_antfly_url:
        parser.error("--require-faster requires --baseline-antfly-url")
    if args.baseline_antfly_url and (not math.isfinite(args.max_wer) or args.max_wer < 0):
        parser.error("--max-wer must be finite and nonnegative")
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
    if args.baseline_antfly_url:
        if seconds <= 0:
            parser.error("audio must have a positive duration")
        return antfly_baseline(args, antfly, audio, seconds, reference)
    args.whisper_url = args.whisper_url or "http://127.0.0.1:18082/inference"
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
