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

"""Compare E4B QAT transcription with a pinned upstream llama.cpp CUDA build.

Shares the pinned fixtures, normalization and SSE timing of qualify_transcription.
Video inputs use explicitly hashed decoded PCM controls because the reference
requires an external ffmpeg executable for those containers.
"""
import argparse
import base64
import hashlib
from pathlib import Path
import socket
import statistics
import subprocess
import time
import urllib.request
from types import SimpleNamespace

import qualify_transcription as q


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--llama-root", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--corpus-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--generation-tokens", type=int, default=256)
    parser.add_argument("--generation-repeats", type=int, default=5)
    parser.add_argument("--generation-warmups", type=int, default=1)
    args = parser.parse_args()
    if args.limit < 0 or args.generation_tokens < 2 or args.generation_repeats < 1 or args.generation_warmups < 0:
        parser.error("invalid corpus limit or generation budget")
    args.output.mkdir(parents=True, exist_ok=False)
    decoder = args.model_dir.resolve() / "gemma-4-E4B_q4_0-it.gguf"
    projector = args.model_dir.resolve() / "gemma-4-E4B-it-mmproj.gguf"
    cases, corpus = q.fixtures(SimpleNamespace(corpus_source=args.corpus_source, limit=args.limit, offset=0))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    command = [str(args.binary.resolve()), "-m", str(decoder), "--mmproj", str(projector),
               "--host", "127.0.0.1", "--port", str(port), "-ngl", "99", "-np", "1",
               "-c", "8192", "-b", "2048", "-ub", "2048", "-ctk", "f32", "-ctv", "f32",
               "-t", "8", "--reasoning-format", "none", "-lv", "4"]
    report = {"runtime": "llama.cpp", "model": q.DEFAULT_MODEL,
              "revision": subprocess.check_output(["git", "-C", str(args.llama_root), "rev-parse", "HEAD"], text=True).strip(),
              "binary_sha256": q.file_sha(args.binary), "harness_sha256": q.file_sha(Path(__file__)),
              "decoder_sha256": q.file_sha(decoder), "projector_sha256": q.file_sha(projector),
              "command": command, "gpu_before": q.gpu_snapshot(), "corpus": corpus,
              "cache_prompt": False, "generation": [], "transcription": []}
    process = None
    with (args.output / "server.log").open("wb") as log:
        try:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 180
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Reference server exited {process.returncode}")
                try:
                    with urllib.request.urlopen(base + "/health", timeout=1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("Reference startup timed out")
                    time.sleep(0.25)
            print("reference ready", base, flush=True)
            for i in range(args.generation_warmups + args.generation_repeats):
                result = q.sse_generate(base, q.DEFAULT_MODEL, args.generation_tokens,
                                        route="/v1/chat/completions", extra_body={"cache_prompt": False})
                result["warmup"] = i < args.generation_warmups
                result["gpu"] = q.gpu_snapshot()
                report["generation"].append(result)
                q.save(args.output / "report.json", report)
                print(f"generation {i}: {result['decode_tokens_per_second']:.2f} decode tok/s", flush=True)
            for case in cases:
                data = case["data"]
                video = case["mime"].startswith("video/")
                transport = q.wav_bytes(q.decoded_pcm(data)) if video else data
                body = {"model": q.DEFAULT_MODEL, "messages": [{"role": "user", "content": [
                    {"type": "input_audio", "input_audio": {"data": base64.b64encode(transport).decode(), "format": "wav" if video else case["mime"]}},
                    {"type": "text", "text": "Transcribe the speech in this audio. Return only the transcript, without commentary."}]}],
                    "temperature": 0, "max_tokens": 256, "chat_template_kwargs": {"enable_thinking": False}, "cache_prompt": False}
                response, elapsed, _headers = q.post(base, "/v1/chat/completions", body)
                transcript = response["choices"][0]["message"]["content"]
                result = {k: v for k, v in case.items() if k != "data"}
                result.update(transcript=transcript, score=q.score(case["reference"], transcript),
                              elapsed_seconds=elapsed, response=response,
                              input_sha256=hashlib.sha256(data).hexdigest(),
                              reference_transport_sha256=hashlib.sha256(transport).hexdigest(),
                              transport="pcm-wav" if video else "original")
                report["transcription"].append(result)
                q.save(args.output / "report.json", report)
                print(f"read {case['id']}: WER={result['score']['wer']} {transcript!r}", flush=True)
            corpus_results = [r for r in report["transcription"] if r["group"] == "corpus"]
            errors = sum(r["score"]["errors"] for r in corpus_results)
            words = sum(r["score"]["reference_words"] for r in corpus_results)
            report["corpus_summary"] = {"clips": len(corpus_results), "reference_words": words, "errors": errors, "wer": errors / words}
            measured = [r for r in report["generation"] if not r["warmup"]]
            report["generation_summary"] = {k: statistics.median(r[k] for r in measured) for k in [
                "decode_tokens_per_second", "end_to_end_tokens_per_second", "ttft_seconds"]}
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            report["gpu_after"] = q.gpu_snapshot()
            q.save(args.output / "report.json", report)


if __name__ == "__main__":
    main()
