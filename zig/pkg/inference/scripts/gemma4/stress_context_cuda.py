#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Gemma 4 E4B CUDA context stress evidence (not a qualification gate)."""

from __future__ import annotations

import argparse
import copy
import json
import statistics
import time
from pathlib import Path
from typing import Any

from qualify_context_management import (
    Client,
    assert_tool_atomic,
    fixture_messages,
    instruction_pairs,
    validate_checkpoint,
    validate_fixture_memory,
    validate_managed,
    validate_stream,
    validate_window,
)

TARGET_INPUT_TOKENS = 8192
MAX_TOKENS = 64
MIN_LONG_CONTEXT_TOKENS = 7 * 1024


def response_text(response: dict[str, Any]) -> str:
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise AssertionError("generation returned no choices")
    message = choices[0].get("message")
    if not isinstance(message, dict) or not isinstance(message.get("content"), str):
        raise AssertionError("generation returned no text message")
    return message["content"]


def assert_managed(response: dict[str, Any], *, checkpoint: bool) -> None:
    validate_managed(
        response,
        target=TARGET_INPUT_TOKENS,
        max_tokens=MAX_TOKENS,
        checkpoint=checkpoint,
    )


def assert_cuda_timings(timings: list[dict[str, Any]]) -> None:
    generated = [item for item in timings if item.get("path") == "/ai/v1/generate"]
    if not generated:
        raise AssertionError("stress run recorded no generation requests")
    for item in generated:
        if item.get("inference_backend") != "cuda":
            raise AssertionError(f"generation did not attest CUDA: {item}")
        if not item.get("inference_route") or not item.get("inference_pid"):
            raise AssertionError(f"generation omitted route/PID evidence: {item}")


def assert_grammar_answer(text: str) -> dict[str, Any]:
    decoded = json.loads(text)
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"answer", "source"}
        or not isinstance(decoded["answer"], str)
        or decoded["source"] != "checkpoint"
    ):
        raise AssertionError(f"grammar result violated the requested schema: {decoded}")
    if "amber-ant" not in decoded["answer"].lower():
        raise AssertionError("grammar checkpoint answer lost the known codename")
    return decoded


def assert_stream(events: list[dict[str, Any]]) -> dict[str, Any]:
    return assert_grammar_answer(
        validate_stream(
            events, target=TARGET_INPUT_TOKENS, max_tokens=MAX_TOKENS, checkpoint=True
        )
    )


def latency_summary(timings: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(item["elapsed_ms"]) for item in timings]
    ordered = sorted(values)
    return {
        "count": len(values),
        "mean_ms": round(statistics.fmean(values), 3) if values else None,
        "p95_ms": round(ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)], 3)
        if values
        else None,
        "max_ms": round(max(values), 3) if values else None,
    }


def run_stress(
    client: Client,
    model: str,
    filler_turns: int,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if evidence is None:
        evidence = {}
    evidence.update(
        {
            "schema": "antfly.gemma4.cuda_context_stress.v1",
            "claim": "measurement_only",
            "qualified": False,
            "evidence_passed": False,
            "model": model,
            "cases": {},
            "request_timings": client.timings_ms,
        }
    )
    cases = evidence["cases"]
    source = fixture_messages(max(filler_turns, TARGET_INPUT_TOKENS // 48))
    original_instructions = instruction_pairs(source)
    tools = [
        {
            "type": "function",
            "function": {
                "name": "lookup_fact",
                "description": "Look up a fixture fact",
                "parameters": {
                    "type": "object",
                    "properties": {"key": {"type": "string"}},
                    "required": ["key"],
                },
            },
        }
    ]
    common = {
        "model": model,
        "tools": tools,
        "tool_choice": "auto",
        "chat_template_kwargs": {"enable_thinking": False},
        "max_tokens": MAX_TOKENS,
        "max_input_tokens": TARGET_INPUT_TOKENS,
        "keep_recent_turns": 2,
    }

    _, window = client.post(
        "/ai/v1/generate/context/prepare",
        {**common, "mode": "window", "messages": copy.deepcopy(source)},
    )
    cases["window"] = window.get("context")
    validate_window(source, window, TARGET_INPUT_TOKENS)
    assert_tool_atomic(window["messages"])
    if window["messages"][-1] != source[-1]:
        raise AssertionError("windowing did not preserve the current user turn")

    window_base = {
        "model": model,
        "messages": window["messages"],
        "tools": tools,
        "tool_choice": "auto",
        "chat_template_kwargs": {"enable_thinking": False},
        "max_tokens": MAX_TOKENS,
        "context_management": {
            "mode": "reject",
            "max_input_tokens": TARGET_INPUT_TOKENS,
        },
        "kv_cache": {"mode": "exact_hybrid", "version": "v1"},
    }
    long_context = []
    cases["long_context_generation"] = long_context
    for sampling in ({"temperature": 0}, {"temperature": 0.7, "top_p": 0.9}):
        _, response = client.post("/ai/v1/generate", {**window_base, **sampling})
        assert_managed(response, checkpoint=False)
        effective = response["context"]["effective_input_tokens"]
        if effective != window["context"]["effective_input_tokens"]:
            raise AssertionError(
                "window prepare/generate exact prompt-token parity failed"
            )
        if effective < MIN_LONG_CONTEXT_TOKENS:
            raise AssertionError(
                f"window generation was not a near-8k prompt: {effective} tokens"
            )
        long_context.append(
            {
                "sampling": sampling,
                "context": response["context"],
                "usage": response.get("usage"),
                "text_length": len(response_text(response)),
            }
        )

    _, compacted = client.post(
        "/ai/v1/generate/context/prepare",
        {**common, "mode": "compact", "messages": copy.deepcopy(source)},
    )
    checkpoint = compacted.get("checkpoint")
    cases["compact"] = compacted.get("context")
    cases["compact_checkpoint"] = checkpoint
    if not isinstance(checkpoint, dict):
        raise AssertionError("compaction did not return a checkpoint")
    validate_checkpoint(checkpoint)
    validate_fixture_memory(checkpoint)
    if instruction_pairs(compacted["messages"]) != original_instructions:
        raise AssertionError("compaction changed system/developer instructions")
    assert_tool_atomic(compacted["messages"])
    if compacted["messages"][-1] != source[-1]:
        raise AssertionError("compaction did not preserve the current user turn")

    managed = {
        "mode": "reject",
        "checkpoint": checkpoint,
        "max_input_tokens": TARGET_INPUT_TOKENS,
    }
    base = {
        "model": model,
        "messages": compacted["messages"],
        "tools": tools,
        "tool_choice": "auto",
        "chat_template_kwargs": {"enable_thinking": False},
        "max_tokens": MAX_TOKENS,
        "context_management": managed,
        "kv_cache": {"mode": "exact_hybrid", "version": "v1"},
    }
    sampled_request = {**base, "temperature": 0.7, "top_p": 0.9}
    sampled = []
    cases["sampled_checkpoint_reuse"] = sampled
    for _ in range(2):
        _, response = client.post("/ai/v1/generate", sampled_request)
        assert_managed(response, checkpoint=True)
        if "amber-ant" not in response_text(response).lower():
            raise AssertionError("sampled checkpoint answer lost the known codename")
        if (
            response["context"]["effective_input_tokens"]
            != compacted["context"]["effective_input_tokens"]
        ):
            raise AssertionError("prepare/generate exact prompt-token parity failed")
        sampled.append(
            {
                "context": response["context"],
                "usage": response.get("usage"),
                "text_length": len(response_text(response)),
            }
        )

    schema = {
        "name": "checkpoint_answer",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "answer": {"type": "string"},
                "source": {"type": "string", "enum": ["checkpoint"]},
            },
            "required": ["answer", "source"],
            "additionalProperties": False,
        },
    }
    grammar_messages = list(compacted["messages"])
    grammar_messages.append(
        {
            "role": "user",
            "content": "Return JSON with the remembered codename in answer and source set to checkpoint.",
        }
    )
    grammar_request = {
        **base,
        "messages": grammar_messages,
        "temperature": 0,
        "response_format": {"type": "json_schema", "json_schema": schema},
    }
    _, grammar = client.post("/ai/v1/generate", grammar_request)
    assert_managed(grammar, checkpoint=True)
    cases["grammar_checkpoint"] = {
        "context": grammar.get("context"),
        "usage": grammar.get("usage"),
        "text": response_text(grammar),
    }
    decoded = assert_grammar_answer(response_text(grammar))
    cases["grammar_checkpoint"]["decoded"] = decoded

    events = client.stream("/ai/v1/generate", {**grammar_request, "stream": True})
    cases["grammar_checkpoint_sse"] = {"events": len(events), "raw_events": events}
    streamed = assert_stream(events)
    cases["grammar_checkpoint_sse"]["decoded"] = streamed
    assert_cuda_timings(client.timings_ms)
    generation_timings = [
        item for item in client.timings_ms if item.get("path") == "/ai/v1/generate"
    ]
    evidence.update(
        {
            "limits": {
                "target_input_tokens": TARGET_INPUT_TOKENS,
                "max_tokens": MAX_TOKENS,
            },
            "generation_latency": latency_summary(generation_timings),
            "limitations": [
                "No latency or memory SLO was supplied; measurements are not a production qualification.",
                "The campaign covers one E4B model revision and exact_hybrid KV only.",
            ],
        }
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8090")
    parser.add_argument("--model", required=True)
    parser.add_argument("--filler-turns", type=int, default=192)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--max-response-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--token")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if "e4b" not in args.model.lower():
        parser.error("this stress campaign is scoped to a Gemma 4 E4B model")
    if args.filler_turns < 1:
        parser.error("--filler-turns must be positive")
    client = Client(args.base_url, args.timeout, args.token, args.max_response_bytes)
    started = time.time()
    failure = None
    evidence: dict[str, Any] = {}
    try:
        run_stress(client, args.model, args.filler_turns, evidence)
        evidence["evidence_passed"] = True
    except Exception as error:
        failure = error
        evidence.update(
            {
                "schema": "antfly.gemma4.cuda_context_stress.v1",
                "claim": "measurement_only",
                "qualified": False,
                "evidence_passed": False,
                "model": args.model,
                "error_type": type(error).__name__,
                "error": str(error),
                "request_timings": client.timings_ms,
            }
        )
    evidence["started_unix_ms"] = round(started * 1000)
    evidence["finished_unix_ms"] = round(time.time() * 1000)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(args.output)
    if failure is not None:
        raise failure
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
