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

"""Qualify a local Gemma 4 audio reader and measure warm SSE generation.

Requires pyarrow, av, and numpy. Corpus metadata must pin a parquet SHA256;
immutable downloaded corpora belong in ~/.cache/antfly/conformance-fixtures.
Results are fixture-scoped evidence, not a broad ASR production certification.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import socket
import statistics
import subprocess
import time
import unicodedata
import urllib.error
import urllib.request
import wave
from fractions import Fraction

DEFAULT_MODEL = "google/gemma-4-E4B-it-qat-q4_0-gguf"
ENGLISH = "The quick brown fox jumps over the lazy dog."
SPANISH = "Buenos días. Esta es una prueba de reconocimiento automático del idioma."


def normalized_words(text):
    text = unicodedata.normalize("NFKC", text).casefold().replace("’", "'")
    return re.findall(r"[^\W_]+", text.replace("'", ""), re.UNICODE)


def edit_counts(reference, hypothesis):
    # Keep substitutions/deletions/insertions, not a word-presence score.
    previous = [(j, 0, 0, j) for j in range(len(hypothesis) + 1)]
    for i, word in enumerate(reference, 1):
        current = [(i, 0, i, 0)]
        for j, observed in enumerate(hypothesis, 1):
            if word == observed:
                current.append(previous[j - 1])
                continue
            substitute = previous[j - 1]
            delete = previous[j]
            insert = current[j - 1]
            current.append(min(
                (substitute[0] + 1, substitute[1] + 1, substitute[2], substitute[3]),
                (delete[0] + 1, delete[1], delete[2] + 1, delete[3]),
                (insert[0] + 1, insert[1], insert[2], insert[3] + 1),
            ))
        previous = current
    distance, substitutions, deletions, insertions = previous[-1]
    return {"errors": distance, "substitutions": substitutions,
            "deletions": deletions, "insertions": insertions,
            "reference_words": len(reference),
            "wer": distance / len(reference) if reference else None}


def score(reference, hypothesis):
    return edit_counts(normalized_words(reference), normalized_words(hypothesis))


def file_sha(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def post(base, route, body, timeout=600):
    request = urllib.request.Request(base + route, json.dumps(body).encode(), {
        "Content-Type": "application/json", "X-Antfly-Inference-Timeout-Ms": "600000"})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            value = json.load(response)
            headers = dict(response.headers)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode(errors='replace')}") from error
    return value, time.perf_counter() - started, headers


def generation_request(model, tokens, *, prompt=None, backend="cuda", extra_body=None):
    """One request constructor shared by timed generation and untimed preflight."""
    body = {"model": model, "temperature": 0,
            "messages": [{"role": "user", "content": prompt or
                "Write a detailed factual explanation of how a compiler works, covering lexing, parsing, type checking, optimization, and machine code. Continue for at least 1500 words."}],
            "chat_template_kwargs": {"enable_thinking": False},
            "stream": True, "stream_options": {"include_usage": True}, "max_tokens": tokens}
    if backend is not None:
        body["backend"] = backend
    if extra_body:
        body.update(extra_body)
    return body


def sse_generate(base, model, tokens, route="/ai/v1/generate", extra_body=None, prompt=None, backend="cuda"):
    body = generation_request(model, tokens, prompt=prompt, backend=backend, extra_body=extra_body)
    request = urllib.request.Request(base + route, json.dumps(body).encode(), {
        "Content-Type": "application/json", "X-Antfly-Inference-Timeout-Ms": "600000"})
    started = time.perf_counter()
    first = usage_time = None
    usage = None
    chunks = []
    engine_timings = []
    first_content_characters = None
    done = False
    finish = None
    try:
        stream = urllib.request.urlopen(request, timeout=600)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode(errors='replace')}") from error
    with stream as response:
        headers = dict(response.headers)
        for line in response:
            if not line.startswith(b"data:"):
                continue
            raw = line[5:].strip()
            if raw == b"[DONE]":
                done = True
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError as error:
                raise RuntimeError(f"Non-JSON SSE data: {raw.decode(errors='replace')[:1000]}") from error
            if event.get("error"):
                raise RuntimeError(str(event["error"]))
            if event.get("timings") is not None:
                engine_timings.append(event["timings"])
            if event.get("usage") is not None:
                usage = event["usage"]
                usage_time = time.perf_counter()
            for choice in event.get("choices", []):
                delta = choice.get("delta", {})
                content = delta.get("content", "")
                if content:
                    if first is None:
                        first = time.perf_counter()
                        first_content_characters = len(content)
                    chunks.append(content)
                if choice.get("finish_reason") is not None:
                    finish = choice["finish_reason"]
    ended = time.perf_counter()
    if not done or usage is None or first is None or usage_time is None:
        raise RuntimeError("SSE stream lacks DONE, visible text, or authoritative terminal usage")
    count = usage["completion_tokens"]
    if count <= 1 or usage_time <= first:
        raise RuntimeError("Insufficient completion tokens to measure decode throughput")
    return {"usage": usage, "seconds": ended - started, "ttft_seconds": first - started,
            "decode_seconds": usage_time - first,
            "decode_tokens_per_second": (count - 1) / (usage_time - first),
            "end_to_end_tokens_per_second": count / (ended - started),
            "stream_accounting": {"content_chunks": len(chunks),
                "first_content_characters": first_content_characters,
                "decode_metric": "(completion_tokens - 1) / (terminal_usage_time - first_visible_content_time)",
                "decode_metric_assumption": "first visible content represents one generated token; chunk sizes do not establish token counts"},
            "engine_reported_timings": engine_timings,
            "finish_reason": finish, "text": "".join(chunks), "headers": headers}


def decoded_pcm(data, rate=16000, layout="mono"):
    import av
    import numpy as np
    frames = []
    resampler = av.AudioResampler(format="s16", layout=layout, rate=rate)
    with av.open(io.BytesIO(data)) as container:
        for frame in container.decode(audio=0):
            frames.extend(resampler.resample(frame))
        frames.extend(resampler.resample(None))
    return np.concatenate([f.to_ndarray().reshape(-1) for f in frames]).astype("int16")


def wav_bytes(samples, rate=16000, channels=1):
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(channels)
        writer.setsampwidth(2)
        writer.setframerate(rate)
        writer.writeframes(samples.tobytes())
    return output.getvalue()


def video_bytes(samples, kind):
    import av
    import numpy as np
    rate = 48000 if kind == "webm" else 16000
    if rate != 16000:
        samples = decoded_pcm(wav_bytes(samples), rate)
    output = io.BytesIO()
    with av.open(output, mode="w", format=kind) as container:
        # Video first matches ordinary camera/container stream ordering.
        video = container.add_stream("libvpx" if kind == "webm" else "mpeg4", rate=1)
        video.width = video.height = 64
        video.pix_fmt = "yuv420p"
        audio = container.add_stream("libopus" if kind == "webm" else "aac", rate=rate)
        audio.layout = "mono"
        frame = av.AudioFrame.from_ndarray((samples.astype("float32") / 32768).reshape(1, -1), format="fltp", layout="mono")
        frame.sample_rate = rate
        frame.pts = 0
        frame.time_base = Fraction(1, rate)
        for packet in audio.encode(frame):
            container.mux(packet)
        for packet in audio.encode(None):
            container.mux(packet)
        for i in range(max(1, math.ceil(len(samples) / rate))):
            picture = av.VideoFrame.from_ndarray(np.zeros((64, 64, 3), dtype="uint8"), format="rgb24")
            picture.pts = i
            for packet in video.encode(picture):
                container.mux(packet)
        for packet in video.encode(None):
            container.mux(packet)
    return output.getvalue()


def fixtures(args):
    import numpy as np
    import pyarrow.parquet as pq
    source = json.loads(args.corpus_source.read_text())
    if file_sha(source["cache_path"]) != source["file"]["lfs"]["sha256"]:
        raise ValueError("Corpus checksum mismatch")
    rows = pq.read_table(source["cache_path"]).to_pylist()
    rows = rows[args.offset:]
    if args.limit:
        rows = rows[:args.limit]
    cases = []
    for row in rows:
        data = row["audio"]["bytes"]
        duration = len(decoded_pcm(data)) / 16000
        cases.append({"id": row["id"], "group": "corpus", "data": data,
                      "mime": "audio/flac", "reference": row["text"], "seconds": duration,
                      "speaker_id": row["speaker_id"]})
    # Paired lossless WAV controls isolate container decoding from ASR errors.
    for case in list(cases[:2]):
        cases.append({**case, "id": case["id"] + "-pcm-control", "group": "diagnostic",
                      "data": wav_bytes(decoded_pcm(case["data"])), "mime": "audio/wav"})
    directory = Path(__file__).resolve().parents[4] / "e2e/inference/testdata"
    english = (directory / "whisper_quality.wav").read_bytes()
    spanish = (directory / "whisper_spanish_quality.wav").read_bytes()
    mono = decoded_pcm(english)
    stereo = decoded_pcm(english, 44100, "stereo")
    variants = [
        ("english-wav", english, "audio/wav", ENGLISH),
        ("spanish-8khz-wav", spanish, "audio/wav", SPANISH),
        ("english-stereo-44100", wav_bytes(stereo, 44100, 2), "audio/wav", ENGLISH),
        ("english-mp4-video", video_bytes(mono, "mp4"), "video/mp4", ENGLISH),
        ("english-webm-video", video_bytes(mono, "webm"), "video/webm", ENGLISH),
        ("silence", wav_bytes(np.zeros(16000, dtype="int16")), "audio/wav", ""),
    ]
    for name, data, mime, reference in variants:
        cases.append({"id": name, "group": "regression", "data": data, "mime": mime,
                      "reference": reference, "seconds": len(decoded_pcm(data)) / 16000})
    # Separate waveform transport/resampling from the original 8 kHz input.
    canonical_spanish = wav_bytes(decoded_pcm(spanish))
    cases.append({"id": "spanish-pcm-16000-control", "group": "diagnostic", "data": canonical_spanish,
                  "mime": "audio/wav", "reference": SPANISH,
                  "seconds": len(decoded_pcm(canonical_spanish)) / 16000})
    return cases, source


def read_case(base, model, case, planner=False, max_tokens=256):
    uri = f"data:{case['mime']};base64," + base64.b64encode(case["data"]).decode()
    body = {"model": "qualified-audio" if planner else model, "max_tokens": max_tokens}
    if planner:
        body["documents"] = [{"id": case["id"], "url": uri, "media_type": case["mime"]}]
    else:
        body["audio"] = uri
    response, seconds, headers = post(base, "/ai/v1/read", body)
    if len(response.get("data", [])) != 1:
        raise RuntimeError("Read response cardinality mismatch")
    row = response["data"][0]
    if row.get("status") != "complete" or row.get("origin") != "transcription":
        raise RuntimeError(f"Incomplete or non-transcription result: {row}")
    if planner:
        if row.get("source_id") != case["id"] or row.get("kind") != "audio":
            raise RuntimeError("Planner lost recording identity/type")
        if response.get("document", {}).get("status") != "complete":
            raise RuntimeError("Planner did not complete")
    with urllib.request.urlopen(base + "/ai/v1/models/instances", timeout=15) as inventory:
        instances = json.load(inventory).get("data", [])
    backends = [item["backend"] for item in instances if item.get("backend")]
    if not backends or any(backend != "cuda" for backend in backends):
        raise RuntimeError(f"Read request did not stay on CUDA: {backends}")
    return {"id": case["id"], "group": case["group"], "planner": planner, "backends": backends,
            "reference": case["reference"], "transcript": row["text"],
            "audio_seconds": case["seconds"], "elapsed_seconds": seconds,
            "real_time_factor": seconds / case["seconds"],
            "input_sha256": hashlib.sha256(case["data"]).hexdigest(),
            "mime": case["mime"], "usage": response.get("usage"),
            "score": score(case["reference"], row["text"]), "response": response, "headers": headers}


def gpu_snapshot():
    result = subprocess.run(["nvidia-smi", "--query-gpu=name,driver_version,utilization.gpu,temperature.gpu,power.draw", "--format=csv,noheader"], capture_output=True, text=True)
    processes = subprocess.run(["nvidia-smi", "--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader"], capture_output=True, text=True)
    return {"gpu": result.stdout.strip(), "processes": processes.stdout.strip()}


def check_audio_duration_limit(base, model):
    import numpy as np
    data = wav_bytes(np.zeros(31 * 16000, dtype="int16"))
    uri = "data:audio/wav;base64," + base64.b64encode(data).decode()
    results = []
    for planner in (False, True):
        body = {"model": "qualified-audio" if planner else model, "max_tokens": 256}
        if planner:
            body["documents"] = [{"id": "overlong-silence", "url": uri, "media_type": "audio/wav"}]
        else:
            body["audio"] = uri
        try:
            post(base, "/ai/v1/read", body)
        except RuntimeError as error:
            if not isinstance(error.__cause__, urllib.error.HTTPError) or error.__cause__.code != 413 or "AUDIO_TOO_LONG" not in str(error):
                raise
            results.append({"planner": planner, "status": 413, "error": "AUDIO_TOO_LONG", "input_sha256": hashlib.sha256(data).hexdigest()})
        else:
            raise RuntimeError(f"Overlong audio was accepted or silently truncated (planner={planner})")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--corpus-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--max-work-units", type=int, default=256)
    parser.add_argument("--generation-tokens", type=int, default=256)
    parser.add_argument("--generation-repeats", type=int, default=5)
    parser.add_argument("--generation-warmups", type=int, default=1)
    parser.add_argument("--max-wer", type=float, default=0.10)
    parser.add_argument("--max-regression-wer", type=float, default=0.20)
    args = parser.parse_args()
    if args.offset < 0 or args.max_work_units < 1 or args.limit < 0 or args.generation_repeats < 1 or args.generation_warmups < 0 or args.generation_tokens < 2:
        parser.error("invalid limit, generation count, or token budget")
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "production_ready": False,
              "scope": "Pinned clean LibriSpeech subset (one speaker), synthetic regressions, and audio/video transport; no general ASR certification",
              "model": args.model, "binary_sha256": file_sha(args.binary),
              "harness_sha256": file_sha(Path(__file__)),
              "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "git_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "--binary", "HEAD"])).hexdigest(),
              "runtime_cuda_environment": {k: v for k, v in os.environ.items() if k.startswith("ANTFLY_INFERENCE_CUDA_") or k == "TERMITE_GEMMA4_AUDIO_HOST_OPS"},
              "thresholds": {"corpus_wer": args.max_wer, "regression_wer": args.max_regression_wer, "silence_words": 0},
              "gpu_before": gpu_snapshot(), "transcription": [], "generation": []}
    cases, report["corpus"] = fixtures(args)
    report["coverage"] = {"corpus_clips": sum(c["group"] == "corpus" for c in cases),
                          "speakers": sorted({c["speaker_id"] for c in cases if "speaker_id" in c}),
                          "corpus_audio_seconds": sum(c["seconds"] for c in cases if c["group"] == "corpus")}
    save(args.output / "fixtures.json", [{k: v for k, v in c.items() if k != "data"} for c in cases])
    plan = args.models_dir / "readers/qualified-audio"
    plan.mkdir(parents=True, exist_ok=True)
    save(plan / "document_agent.json", {"version": 1, "models": [{"name": "speech", "model": args.model, "operation": "read", "input": "audio"}],
                                      "read": {"reader": "speech"}, "budget": {"deadline_ms": 600000}})
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    command = [str(args.binary.resolve()), "run", "--host", "127.0.0.1", "--port", str(port), "--models-dir", str(args.models_dir.resolve()), "--preload-model", f"generator:cuda:{args.model}", "--max-work-units", str(args.max_work_units), "--max-concurrent-requests", "2"]
    report["server_command"] = command
    process = None
    with (args.output / "server.log").open("wb") as log:
        try:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 180
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Server exited {process.returncode}")
                try:
                    with urllib.request.urlopen(base + "/healthz", timeout=1):
                        break
                except (OSError, urllib.error.URLError):
                    if time.monotonic() > deadline:
                        raise RuntimeError("Server startup timed out")
                    time.sleep(0.25)
            print(f"server ready {base}", flush=True)
            for i in range(args.generation_warmups + args.generation_repeats):
                result = sse_generate(base, args.model, args.generation_tokens)
                result["warmup"] = i < args.generation_warmups
                result["gpu"] = gpu_snapshot()
                report["generation"].append(result)
                save(args.output / "report.json", report)
                print(f"generation {i}: {result['decode_tokens_per_second']:.2f} decode tok/s; {result['end_to_end_tokens_per_second']:.2f} end-to-end tok/s", flush=True)
            report["duration_limit"] = check_audio_duration_limit(base, args.model)
            for case in cases:
                result = read_case(base, args.model, case)
                report["transcription"].append(result)
                save(args.output / "report.json", report)
                print(f"read {case['id']}: WER={result['score']['wer']} RTF={result['real_time_factor']:.3f} {result['transcript'][:130]!r}", flush=True)
                if case["group"] == "regression":
                    planned = read_case(base, args.model, case, planner=True)
                    planned["normalized_parity"] = normalized_words(planned["transcript"]) == normalized_words(result["transcript"])
                    report["transcription"].append(planned)
                    save(args.output / "report.json", report)
                    print(f"planner {case['id']}: parity={planned['normalized_parity']} WER={planned['score']['wer']}", flush=True)
            with urllib.request.urlopen(base + "/ai/v1/models/instances", timeout=15) as response:
                report["instances"] = json.load(response)
            instances = report["instances"].get("data", [])
            backends = [item["backend"] for item in instances if item.get("backend")]
            if not backends or any(backend != "cuda" for backend in backends):
                raise RuntimeError(f"Qualification did not stay on CUDA: {backends}")
            report["observed_backends"] = backends
            samples = [r for r in report["generation"] if not r["warmup"]]
            corpus = [r for r in report["transcription"] if r["group"] == "corpus"]
            regressions = [r for r in report["transcription"] if r["group"] == "regression" and r["reference"]]
            silence = next(r for r in report["transcription"] if r["id"] == "silence")
            reference_words = sum(r["score"]["reference_words"] for r in corpus)
            errors = sum(r["score"]["errors"] for r in corpus)
            report["summary"] = {"corpus_wer": errors / reference_words,
                "corpus_word_errors": errors, "corpus_reference_words": reference_words,
                "worst_clip_wer": max(r["score"]["wer"] for r in corpus),
                "silence_generated_words": len(normalized_words(silence["transcript"])),
                "decode_tokens_per_second_median": statistics.median(r["decode_tokens_per_second"] for r in samples),
                "end_to_end_tokens_per_second_median": statistics.median(r["end_to_end_tokens_per_second"] for r in samples),
                "decode_tokens_per_second_min": min(r["decode_tokens_per_second"] for r in samples),
                "decode_tokens_per_second_max": max(r["decode_tokens_per_second"] for r in samples),
                "generation_completion_tokens": [r["usage"]["completion_tokens"] for r in samples],
                "generation_text_hashes": sorted({hashlib.sha256(r["text"].encode()).hexdigest() for r in samples}),
                "ttft_seconds_median": statistics.median(r["ttft_seconds"] for r in samples),
                "generation_accounting": "authoritative final SSE usage; decode=(completion_tokens-1)/(terminal_usage_time-first_visible_token_time)"}
            report["passed"] = (report["summary"]["corpus_wer"] <= args.max_wer
                and all(r["score"]["wer"] <= args.max_regression_wer for r in regressions)
                and all(r.get("normalized_parity", True) for r in report["transcription"])
                and not normalized_words(silence["transcript"]))
            print(json.dumps(report["summary"], indent=2), flush=True)
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            report["gpu_after"] = gpu_snapshot()
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            save(args.output / "report.json", report)
    if not report["passed"]:
        raise SystemExit("Transcription qualification gates failed; see report.json")


if __name__ == "__main__":
    main()
