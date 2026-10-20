#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare generation over the same windowed history with a llama.cpp server.

Measurements only: this does not equate llama.cpp context shifting with Antfly
structured compaction, or qualify either backend. Start both servers with the
same GGUF, f16 KV, one request slot, and sufficient context capacity. Run on an
otherwise idle GPU. Prompt caching is disabled on both sides.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from pathlib import Path
from typing import Any

from qualify_context_management import Client, cuda_device_evidence, latency_summary, validate_managed


def fixture(turns: int) -> list[dict[str, str]]:
    messages = [{"role": "system", "content": "You are a helpful assistant."}]
    for index in range(turns):
        messages.extend([
            {"role": "user", "content": f"Question {index}: " + "context " * 24},
            {"role": "assistant", "content": f"Answer {index}: " + "response " * 24},
        ])
    messages.append({"role": "user", "content": "Write a detailed 500-word explanation of how ants organize a colony."})
    return messages


def observation(response: dict[str, Any], timing: dict[str, Any]) -> dict[str, Any]:
    usage = response.get("usage", {})
    for field in ("prompt_tokens", "completion_tokens"):
        if type(usage.get(field)) is not int or usage[field] <= 0:
            raise AssertionError(f"response omitted positive {field}")
    choices = response.get("choices", [])
    if not choices or not isinstance(choices[0].get("message", {}).get("content"), str):
        raise AssertionError("response omitted generated text")
    return {
        "elapsed_ms": timing["elapsed_ms"],
        "usage": usage,
        "finish_reason": choices[0].get("finish_reason"),
        "output_sha256": hashlib.sha256(choices[0]["message"]["content"].encode()).hexdigest(),
        "runtime": {key: timing.get(key) for key in ("inference_backend", "inference_route", "inference_pid")},
        "server_timings": response.get("timings"),
    }


def summarize(records: list[dict[str, Any]], max_tokens: int) -> dict[str, Any]:
    measured = [record for record in records if not record["warmup"]]
    comparable = bool(measured) and all(
        record["antfly"]["usage"]["prompt_tokens"] == record["llama_cpp"]["usage"]["prompt_tokens"]
        and record["antfly"]["usage"]["completion_tokens"] == record["llama_cpp"]["usage"]["completion_tokens"] == max_tokens
        for record in measured
    )
    return {
        "comparable_token_counts": comparable,
        "antfly_latency_ms": latency_summary([record["antfly"]["elapsed_ms"] for record in measured]),
        "llama_cpp_latency_ms": latency_summary([record["llama_cpp"]["elapsed_ms"] for record in measured]),
        # Suppress a win/loss ratio when templates or early EOS differ.
        "median_antfly_over_llama_latency": statistics.median(
            record["antfly"]["elapsed_ms"] / record["llama_cpp"]["elapsed_ms"] for record in measured
        ) if comparable else None,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    antfly = Client(args.antfly_url, args.timeout, None, 16 * 1024 * 1024)
    llama = Client(args.llama_url, args.timeout, None, 16 * 1024 * 1024)
    common = {"max_tokens": args.max_tokens, "temperature": 0, "chat_template_kwargs": {"enable_thinking": False}}
    _, prepared = antfly.post("/ai/v1/chat/context/prepare", {
        "model": args.model, "messages": fixture(max(192, args.input_tokens // 48)),
        "mode": "window", "max_input_tokens": args.input_tokens, "keep_recent_turns": 2,
        "max_tokens": args.max_tokens, "chat_template_kwargs": common["chat_template_kwargs"],
    })
    if not prepared["context"]["windowed"] or prepared["context"]["effective_input_tokens"] < args.input_tokens * 0.85:
        raise AssertionError("fixture did not exercise a near-budget window")
    common["messages"] = prepared["messages"]
    native = {**common, "model": args.model, "prompt_cache": False, "cache_dtype": "f16",
              "context_management": {"mode": "reject", "max_input_tokens": args.input_tokens},
              "kv_cache": {"mode": "exact_hybrid", "version": "v1"}}
    baseline = {**common, "model": args.llama_model, "cache_prompt": False}
    records = []
    for index in range(args.warmups + args.runs):
        order = ["antfly", "llama_cpp"] if index % 2 == 0 else ["llama_cpp", "antfly"]
        record: dict[str, Any] = {"warmup": index < args.warmups, "order": order}
        for name in order:
            client, path, request = (antfly, "/ai/v1/generate", native) if name == "antfly" else (llama, "/v1/chat/completions", baseline)
            _, response = client.post(path, request)
            if name == "antfly":
                validate_managed(response, target=args.input_tokens, max_tokens=args.max_tokens, checkpoint=False)
                if client.timings_ms[-1].get("inference_backend") != "cuda":
                    raise AssertionError("Antfly response did not attest CUDA")
            record[name] = observation(response, client.timings_ms[-1])
        records.append(record)
    return {
        "schema": "antfly.gemma4.context_comparison.v1", "claim": "measurement_only", "qualified": False,
        "scope": "Generation over the same windowed messages; compaction and window-planning latency excluded",
        "model": args.model, "llama_model": args.llama_model, "devices": cuda_device_evidence(),
        "requests": {"antfly": native, "llama_cpp": baseline},
        "window": prepared["context"], "preparation_ms": antfly.timings_ms[0]["elapsed_ms"],
        "records": records, "summary": summarize(records, args.max_tokens),
        "limitations": ["Identical message JSON and token counts do not prove identical rendered token IDs.",
                        "Record server binaries, model hashes, launch settings and GPU placement alongside this artifact.",
                        "This does not measure summarization quality or claim feature equivalence."],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly-url", default="http://127.0.0.1:8090")
    parser.add_argument("--llama-url", default="http://127.0.0.1:8091")
    parser.add_argument("--model", required=True)
    parser.add_argument("--llama-model", default="gemma4")
    parser.add_argument("--input-tokens", type=int, default=8192)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.runs, args.input_tokens, args.max_tokens) < 1 or args.warmups < 0:
        parser.error("runs and token limits must be positive; warmups must be nonnegative")
    started = time.time()
    failure = None
    try:
        evidence = run(args)
        evidence["evidence_passed"] = True
    except Exception as error:
        failure = error
        evidence = {
            "schema": "antfly.gemma4.context_comparison.v1", "claim": "measurement_only", "qualified": False,
            "evidence_passed": False, "model": args.model, "llama_model": args.llama_model,
            "error_type": type(error).__name__, "error": str(error),
        }
    evidence["started_unix_ms"] = round(started * 1000)
    evidence["finished_unix_ms"] = round(time.time() * 1000)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    if failure is not None:
        raise failure
    print(json.dumps(evidence["summary"], indent=2))


if __name__ == "__main__":
    main()
