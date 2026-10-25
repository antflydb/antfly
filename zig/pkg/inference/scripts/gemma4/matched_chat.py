"""Chat-to-chat benchmark accounting for pinned llama.cpp b11669.

Preflight checks run outside measured requests. Timed requests use ordinary
streaming chat, without verbose prompt echoes or per-token log probabilities.
"""

import hashlib
import http.client
import json
import math
import struct
import time


def request_identity(body):
    fields = (
        "messages",
        "max_tokens",
        "temperature",
        "stream",
        "stream_options",
        "chat_template_kwargs",
    )
    value = {key: body[key] for key in fields}
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def post_json(port, endpoint, body):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1800)
    try:
        connection.request(
            "POST",
            endpoint,
            json.dumps(body).encode(),
            {"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        if response.status != 200:
            raise AssertionError(
                f"preflight HTTP {response.status}: {response.read(4096)!r}"
            )
        return json.load(response)
    finally:
        connection.close()


def verify_template(port, body, token_ids):
    rendered = post_json(
        port,
        "/apply-template",
        {
            "messages": body["messages"],
            "chat_template_kwargs": body["chat_template_kwargs"],
            "add_generation_prompt": True,
        },
    )["prompt"]
    # Match the server's completion tokenizer, including special-token parsing.
    actual = post_json(
        port,
        "/tokenize",
        {
            "content": rendered,
            "add_special": True,
            "parse_special": True,
        },
    )["tokens"]
    if actual != token_ids or any(type(token) is not int for token in actual):
        raise AssertionError(
            "chat template/tokenizer differs from exact Antfly checkpoint token IDs"
        )
    return {
        "verified": True,
        "logical_tokens": len(actual),
        "rendered_prompt_sha256": hashlib.sha256(rendered.encode()).hexdigest(),
        "token_bytes_sha256": hashlib.sha256(
            struct.pack(f"<{len(actual)}I", *actual)
        ).hexdigest(),
    }


def read_chat(response, started):
    content = []
    first_content = None
    finish = None
    usage = None
    timings = None
    for _ in range(1024):
        raw = response.readline(1024 * 1024 + 1)
        if not raw or len(raw) > 1024 * 1024:
            raise AssertionError("missing DONE or oversized chat event")
        line = raw.decode().strip()
        if line.startswith("event:") and line[6:].strip() == "error":
            raise AssertionError("chat SSE error")
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            if (
                first_content is None
                or finish != "stop"
                or usage is None
                or timings is None
            ):
                raise AssertionError("incomplete chat accounting")
            total = time.monotonic() - started
            return {
                "result": {
                    "content": "".join(content),
                    "finish_reason": finish,
                    "usage": usage,
                    "timings": timings,
                },
                "total_seconds": total,
                "first_content_seconds": first_content,
                "first_content_to_done_seconds": total - first_content,
            }
        event = json.loads(data)
        if event.get("error") or "__verbose" in event:
            raise AssertionError("error or verbose diagnostics in timed chat")
        if event.get("usage") is not None:
            if usage is not None:
                raise AssertionError("duplicate usage")
            usage = event["usage"]
        if event.get("timings") is not None:
            timings = event["timings"]
        for choice in event.get("choices", []):
            if choice.get("index") != 0:
                raise AssertionError("unexpected chat choice")
            delta = choice.get("delta", {})
            if any(
                value for key, value in delta.items() if key not in ("role", "content")
            ):
                raise AssertionError("unexpected reasoning or tool output")
            part = delta.get("content")
            if part is not None and not isinstance(part, str):
                raise AssertionError("invalid chat content")
            if part:
                if finish is not None:
                    raise AssertionError("content after finish")
                if first_content is None:
                    first_content = time.monotonic() - started
                content.append(part)
            if choice.get("finish_reason") is not None:
                if finish is not None:
                    raise AssertionError("duplicate finish")
                finish = choice["finish_reason"]
    raise AssertionError("excessive chat events")


def validate_chat(result, logical_tokens, preflight):
    usage = result["usage"]
    timings = result["timings"]
    if (
        result.get("finish_reason") != "stop"
        or result["content"] != preflight["content"]
        or usage.get("prompt_tokens") != logical_tokens
        or usage.get("completion_tokens") != preflight["tokens_predicted"]
        or usage.get("total_tokens") != logical_tokens + preflight["tokens_predicted"]
    ):
        raise AssertionError(
            "chat answer, termination or token accounting differs from preflight"
        )
    if usage.get("prompt_tokens_details", {}).get("cached_tokens") != 0:
        raise AssertionError("chat usage does not attest zero cached tokens")
    if any(
        type(usage.get(key)) is not int
        for key in ("prompt_tokens", "completion_tokens", "total_tokens")
    ):
        raise AssertionError("invalid chat usage counters")
    for key, expected in [
        ("prompt_n", logical_tokens),
        ("cache_n", 0),
        ("predicted_n", preflight["tokens_predicted"]),
    ]:
        if type(timings.get(key)) is not int or timings[key] != expected:
            raise AssertionError("chat replay reused or changed the workload")
    for key in ("prompt_ms", "predicted_ms"):
        value = timings.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise AssertionError("invalid chat phase timing")
