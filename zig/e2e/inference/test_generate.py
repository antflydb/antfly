# Copyright 2026 Antfly, Inc.
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

"""Tests for /api/generate endpoint.

Matches Go termite's generator_test.go patterns.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from .helpers import TINY_PNG_URI
from .models import default_generator_model_name, find_multimodal_generator_model_name, find_tool_model_name, run_large_model_tests

pytestmark = pytest.mark.model_integration


def _first_generator_model(api):
    models = api.models()
    generators = models.get("generators", {})
    if (model := default_generator_model_name(set(generators.keys()))) is not None:
        return model
    if generators:
        return next(iter(generators.keys()))
    pytest.skip("No generator models available for /api/generate tests")


def _first_tool_generator_model(api):
    generators = api.models().get("generators", {})
    model = find_tool_model_name(set(generators.keys()))
    if model:
        return model
    if os.environ.get("TERMITE_TOOL_MODEL"):
        return os.environ["TERMITE_TOOL_MODEL"]
    pytest.skip("No tool-capable generator model available; set TERMITE_TOOL_MODEL or place one under models")


def _skip_unloadable_tool_model_response(response):
    if response.status_code == 400:
        try:
            payload = response.json()
        except Exception:
            return
        if payload.get("error") == "INVALID_MODEL" and "tool" in payload.get("message", ""):
            pytest.skip("Tool-capable model is present in listing, but this runtime does not support tool calling for it")
        return
    if response.status_code != 500:
        return
    try:
        payload = response.json()
    except Exception:
        return
    if payload.get("error") == "MODEL_LOAD_FAILED" and payload.get("message") == "NoModelFileFound":
        pytest.skip("Tool-capable model is present but not loadable in this build/backend configuration")


def _generate_or_skip_unsupported(api, body: dict) -> dict:
    response = api.post("/generate", json=body)
    if response.status_code == 500 and response.headers.get("content-type", "").startswith("application/json"):
        payload = response.json()
        if payload.get("error") == "GENERATION_FAILED" and payload.get("message") in {
            "UnsupportedKvHeadDim",
        }:
            pytest.skip("Generation feature is not supported for this model/backend combination")
    response.raise_for_status()
    return response.json()


def _first_multimodal_generator_model(api):
    generators = api.models().get("generators", {})
    model = find_multimodal_generator_model_name(set(generators.keys()))
    if model:
        return model
    if os.environ.get("TERMITE_MULTIMODAL_GENERATOR_MODEL"):
        return os.environ["TERMITE_MULTIMODAL_GENERATOR_MODEL"]
    pytest.skip("No multimodal generator model available; set TERMITE_MULTIMODAL_GENERATOR_MODEL or place one under models")


def _assert_chat_completion(resp: dict) -> dict:
    assert isinstance(resp.get("id"), str) and resp["id"].startswith("chatcmpl-"), resp
    assert resp.get("object") == "chat.completion", resp
    assert isinstance(resp.get("created"), int), resp
    assert isinstance(resp.get("model"), str), resp
    choices = resp.get("choices")
    assert isinstance(choices, list) and choices, resp
    choice = choices[0]
    assert choice.get("index") == 0, resp
    assert choice.get("finish_reason") in ("stop", "length", "tool_calls"), resp
    usage = resp.get("usage")
    assert isinstance(usage, dict), resp
    assert isinstance(usage.get("prompt_tokens"), int), resp
    assert isinstance(usage.get("completion_tokens"), int), resp
    assert isinstance(usage.get("total_tokens"), int), resp
    assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"], resp
    return choice


def _message_content(resp: dict) -> str | None:
    choice = _assert_chat_completion(resp)
    message = choice.get("message", {})
    assert message.get("role") == "assistant", resp
    return message.get("content")


def _first_choice(resp: dict) -> dict:
    return _assert_chat_completion(resp)


def test_basic_generation(api):
    messages = [{"role": "user", "content": "Hello"}]
    resp = api.generate(messages, max_tokens=50)
    content = _message_content(resp)
    assert content, f"No generated content in response: {resp}"


def test_max_tokens_respected(api):
    messages = [{"role": "user", "content": "Write a long essay about AI"}]
    resp = api.generate(messages, max_tokens=10)
    content = _message_content(resp)
    assert content is not None, "Should return some content even with low max_tokens"


def test_generate_response_format_json_object(api):
    model = _first_generator_model(api)
    resp = api.generate(
        [{"role": "user", "content": "Return a tiny JSON object"}],
        model=model,
        max_tokens=128,
        response_format={"type": "json_object"},
    )

    choice = _first_choice(resp)
    content = choice.get("message", {}).get("content")
    assert content, f"No generated content in response: {resp}"
    if choice.get("finish_reason") == "length":
        assert content.lstrip().startswith("{"), content
    else:
        json.loads(content)


def test_generate_response_format_json_schema(api):
    model = _first_generator_model(api)
    resp = api.generate(
        [{"role": "user", "content": "Return JSON with a string field named answer"}],
        model=model,
        max_tokens=128,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "answer_payload",
                "schema": {
                    "type": "object",
                    "properties": {
                        "answer": {"type": "string"},
                    },
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            },
        },
    )

    content = _message_content(resp)
    assert content, f"No generated content in response: {resp}"
    payload = json.loads(content)
    assert isinstance(payload.get("answer"), str), payload


def test_generate_invalid_grammar_rejected(api):
    model = _first_generator_model(api)
    r = api.post("/generate", json={
        "model": model,
        "messages": [{"role": "user", "content": "Say hello"}],
        "max_tokens": 8,
        "grammar": "this is not valid GBNF syntax %%%",
    })
    assert r.status_code == 400, f"Expected 400, got {r.status_code}: {r.text}"


def test_generate_with_draft_model_smoke(api):
    model = _first_generator_model(api)
    resp = _generate_or_skip_unsupported(api, {
        "model": model,
        "messages": [{"role": "user", "content": "Say hello briefly"}],
        "max_tokens": 8,
        "draft_model": model,
        "speculative_k": 2,
    })

    content = _message_content(resp)
    assert content, f"No generated content in response: {resp}"


def test_generate_json_schema_with_draft_model(api):
    model = _first_generator_model(api)
    resp = _generate_or_skip_unsupported(api, {
        "model": model,
        "messages": [{"role": "user", "content": "Return JSON with a string field named answer"}],
        "max_tokens": 128,
        "draft_model": model,
        "speculative_k": 2,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "answer_payload",
                "schema": {
                    "type": "object",
                    "properties": {
                        "answer": {"type": "string"},
                    },
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            },
        },
    })

    content = _message_content(resp)
    assert content, f"No generated content in response: {resp}"
    payload = json.loads(content)
    assert isinstance(payload.get("answer"), str), payload


# -- SSE Streaming --


@pytest.mark.streaming
def test_stream_content_type(api):
    """Streaming response should have text/event-stream content type."""
    r = api.generate(
        [{"role": "user", "content": "Say hello"}],
        max_tokens=10,
        stream=True,
    )
    assert "text/event-stream" in r.headers.get("content-type", ""), (
        f"Expected text/event-stream, got: {r.headers.get('content-type')}"
    )
    r.close()


@pytest.mark.streaming
def test_stream_has_data_lines(api):
    """SSE stream should contain 'data: ' lines ending with [DONE]."""
    r = api.generate(
        [{"role": "user", "content": "Say hello"}],
        max_tokens=10,
        stream=True,
    )
    body = r.text
    r.close()

    data_lines = [line for line in body.splitlines() if line.startswith("data: ")]
    assert len(data_lines) >= 1, f"No 'data: ' lines in stream: {body[:500]}"

    last = data_lines[-1]
    assert "[DONE]" in last, f"Expected [DONE] in last data line: {last}"


@pytest.mark.streaming
def test_stream_emits_finish_chunk_before_done(api):
    """SSE stream should include a final JSON chunk with finish_reason before [DONE]."""
    r = api.generate(
        [{"role": "user", "content": "Say hello"}],
        max_tokens=10,
        stream=True,
    )
    body = r.text
    r.close()

    data_lines = [line[len("data: "):] for line in body.splitlines() if line.startswith("data: ")]
    assert len(data_lines) >= 2, f"Expected at least one JSON event and [DONE], got: {body[:500]}"
    assert data_lines[-1] == "[DONE]"

    finish_event = json.loads(data_lines[-2])
    assert isinstance(finish_event.get("id"), str), finish_event
    assert finish_event.get("object") == "chat.completion.chunk", finish_event
    assert isinstance(finish_event.get("created"), int), finish_event
    finish_reason = finish_event.get("choices", [{}])[0].get("finish_reason")
    assert finish_reason in ("stop", "length", "tool_calls"), finish_event


@pytest.mark.streaming
def test_stream_delta_structure(api):
    """Stream should begin with assistant role and later emit content deltas."""

    r = api.generate(
        [{"role": "user", "content": "Say hello"}],
        max_tokens=10,
        stream=True,
    )
    body = r.text
    r.close()

    data_lines = [
        line[len("data: "):]
        for line in body.splitlines()
        if line.startswith("data: ") and "[DONE]" not in line
    ]
    assert len(data_lines) >= 1, "Should have at least one non-[DONE] data event"

    role_event = json.loads(data_lines[0])
    assert isinstance(role_event.get("id"), str), role_event
    assert role_event.get("object") == "chat.completion.chunk", role_event
    assert isinstance(role_event.get("created"), int), role_event
    assert role_event.get("choices", [{}])[0].get("delta", {}).get("role") == "assistant", role_event

    content_event = next(
        (
            json.loads(line)
            for line in data_lines[1:]
            if json.loads(line).get("choices", [{}])[0].get("delta", {}).get("content") is not None
        ),
        None,
    )
    assert content_event is not None, f"No content delta found in stream: {data_lines}"
    content = content_event.get("choices", [{}])[0].get("delta", {}).get("content")
    assert content is not None, f"Delta missing choices[0].delta.content: {content_event}"


# -- Multimodal generation --


@pytest.mark.multimodal
@pytest.mark.slow
def test_multimodal_generation(api):
    """Multimodal generation should either succeed or fail explicitly."""
    if not run_large_model_tests():
        pytest.skip("Multimodal generation uses a large model; set RUN_LARGE_MODEL_TESTS=1 to run it")
    model = _first_multimodal_generator_model(api)
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": "Describe this image in one short sentence."},
            {"type": "image_url", "image_url": {"url": TINY_PNG_URI}},
        ],
    }]

    r = api.post("/generate", json={
        "model": model,
        "messages": messages,
        "max_tokens": 20,
    })
    if r.status_code == 400:
        payload = r.json()
        assert payload.get("error") == "INVALID_REQUEST", payload
        assert "native multimodal generation is not implemented yet" in payload.get("message", ""), payload
        return

    r.raise_for_status()
    resp = r.json()
    content = _message_content(resp)
    assert content, f"No multimodal generated content in response: {resp}"


def test_generate_rejects_tool_choice_without_tools(api):
    model = _first_generator_model(api)
    r = api.post("/generate", json={
        "model": model,
        "messages": [{"role": "user", "content": "What is the weather?"}],
        "tool_choice": "required",
    })
    assert r.status_code == 400
    assert "tools are required when tool_choice is set" in r.text


def test_generate_rejects_invalid_tool_choice(api):
    model = _first_generator_model(api)
    r = api.post("/generate", json={
        "model": model,
        "messages": [{"role": "user", "content": "Call the weather function"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the weather for a location",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string"},
                    },
                    "required": ["location"],
                },
            },
        }],
        "tool_choice": {"type": "function"},
    })
    assert r.status_code == 400
    assert "invalid tool_choice" in r.text


def test_generate_with_tools_tool_model(api):
    model = _first_tool_generator_model(api)
    r = api.post("/generate", json={
        "model": model,
        "messages": [{"role": "user", "content": "Use the get_weather function for San Francisco."}],
        "max_tokens": 96,
        "temperature": 0.1,
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the current weather for a location",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string"},
                    },
                    "required": ["location"],
                },
            },
        }],
        "tool_choice": "required",
    })
    _skip_unloadable_tool_model_response(r)
    r.raise_for_status()
    resp = r.json()
    choice = _assert_chat_completion(resp)
    message = choice.get("message", {})
    tool_calls = message.get("tool_calls") or []
    content = message.get("content")

    assert choice.get("finish_reason") in ("tool_calls", "stop", "length"), resp
    assert tool_calls or content, f"Expected tool_calls or content in response: {resp}"
    if tool_calls:
        call = tool_calls[0]
        assert call.get("type") == "function", call
        assert call.get("function", {}).get("name"), call
        json.loads(call.get("function", {}).get("arguments", "{}"))


@pytest.mark.streaming
def test_stream_generate_with_tools_tool_model(api):
    model = _first_tool_generator_model(api)
    r = api.post("/generate", json={
        "model": model,
        "messages": [{"role": "user", "content": "Use the lookup function to find order 42."}],
        "max_tokens": 96,
        "temperature": 0.1,
        "stream": True,
        "tools": [{
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "Look up an order by id",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                    },
                    "required": ["id"],
                },
            },
        }],
        "tool_choice": "required",
    }, stream=True)
    _skip_unloadable_tool_model_response(r)
    r.raise_for_status()
    body = r.text
    r.close()

    data_lines = [line[len("data: "):] for line in body.splitlines() if line.startswith("data: ")]
    assert data_lines, f"No SSE events returned: {body[:500]}"
    assert data_lines[-1] == "[DONE]"

    events = [json.loads(line) for line in data_lines[:-1]]
    for event in events:
        assert isinstance(event.get("id"), str), event
        assert event.get("object") == "chat.completion.chunk", event
        assert isinstance(event.get("created"), int), event
    tool_deltas = [
        event
        for event in events
        if event.get("choices", [{}])[0].get("delta", {}).get("tool_calls")
    ]
    finish_reason = events[-1].get("choices", [{}])[0].get("finish_reason")

    assert finish_reason in ("tool_calls", "stop", "length"), events[-1]
    if tool_deltas:
        streamed_args = []
        saw_named_call = False
        for event in tool_deltas:
            delta = event["choices"][0]["delta"]["tool_calls"][0]
            assert delta.get("index") == 0, delta
            function = delta.get("function", {})
            if function.get("name"):
                saw_named_call = True
            if "arguments" in function:
                assert isinstance(function["arguments"], str), delta
                streamed_args.append(function["arguments"])

        assert saw_named_call, tool_deltas
        if streamed_args:
            json.loads("".join(streamed_args))


# -- Verification tests --


@pytest.mark.verification
def test_speculative_decode_matches_greedy(api):
    """Speculative decode with greedy sampling (temperature=0) should produce
    the same output as standard decode."""
    model = _first_generator_model(api)
    messages = [{"role": "user", "content": "Count from 1 to 5"}]
    kwargs = dict(model=model, max_tokens=20, temperature=0, backend="native")

    # Standard decode (no draft model)
    standard = api.generate(messages, **kwargs)
    standard_content = _message_content(standard) or ""

    # Speculative decode (same model as draft)
    speculative = api.generate(messages, draft_model=model, speculative_k=3, **kwargs)
    speculative_content = _message_content(speculative) or ""

    assert standard_content, "Standard decode produced no content"
    assert speculative_content, "Speculative decode produced no content"
    assert standard_content == speculative_content, (
        f"Speculative output differs from standard:\n"
        f"  standard:    {standard_content!r}\n"
        f"  speculative: {speculative_content!r}"
    )


@pytest.mark.verification
@pytest.mark.parametrize("cache_dtype", ["f16", "int8", "fp8", "int4"])
def test_generate_with_cache_dtype(api, cache_dtype):
    """Generation should succeed with each KV cache quantization format."""
    model = _first_generator_model(api)
    messages = [{"role": "user", "content": "Say hello"}]
    resp = api.generate(messages, model=model, max_tokens=10, cache_dtype=cache_dtype)
    content = _message_content(resp)
    assert content, f"No generated content with cache_dtype={cache_dtype}: {resp}"


@pytest.mark.verification
def test_concurrent_requests_throughput(api):
    """Multiple concurrent requests should all complete successfully.
    Measures throughput improvement over sequential as a sanity check."""
    model = _first_generator_model(api)
    num_requests = 4
    messages = [{"role": "user", "content": "Say hello"}]
    kwargs = dict(model=model, max_tokens=8)

    # Sequential baseline
    seq_start = time.monotonic()
    for _ in range(num_requests):
        resp = api.generate(messages, **kwargs)
        content = _message_content(resp) or ""
        assert content, f"Sequential request returned no content: {resp}"
    seq_elapsed = time.monotonic() - seq_start

    # Concurrent requests
    def do_request():
        resp = api.generate(messages, **kwargs)
        content = _message_content(resp) or ""
        assert content, f"Concurrent request returned no content: {resp}"
        return content

    conc_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=num_requests) as pool:
        futures = [pool.submit(do_request) for _ in range(num_requests)]
        results = [f.result() for f in as_completed(futures)]
    conc_elapsed = time.monotonic() - conc_start

    assert len(results) == num_requests, f"Expected {num_requests} results, got {len(results)}"

    # Log timing for manual inspection (not a hard assertion since
    # throughput depends on hardware and model)
    print(f"\nThroughput test: {num_requests} requests")
    print(f"  Sequential: {seq_elapsed:.2f}s ({seq_elapsed/num_requests:.2f}s/req)")
    print(f"  Concurrent: {conc_elapsed:.2f}s ({conc_elapsed/num_requests:.2f}s/req)")
    if seq_elapsed > 0:
        print(f"  Speedup: {seq_elapsed/conc_elapsed:.2f}x")
