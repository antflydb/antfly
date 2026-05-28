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

"""Compatibility tests against the actual OpenAI Python SDK."""

import json
import os

import pytest

from .models import default_generator_model_name, find_tool_model_name

pytestmark = pytest.mark.model_integration


def _first_generator_model(api):
    generators = api.models().get("generators", {})
    if (model := default_generator_model_name(set(generators.keys()))) is not None:
        return model
    if generators:
        return next(iter(generators.keys()))
    pytest.skip("No generator models available for OpenAI SDK tests")


def _first_tool_generator_model(api):
    generators = api.models().get("generators", {})
    model = find_tool_model_name(set(generators.keys()))
    if model:
        return model
    if os.environ.get("TERMITE_TOOL_MODEL"):
        return os.environ["TERMITE_TOOL_MODEL"]
    pytest.skip("No tool-capable generator model available for OpenAI SDK tests")


def test_openai_chat_completion(api, openai_client):
    model = _first_generator_model(api)
    resp = openai_client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": "Say hello briefly"}],
        max_tokens=16,
    )
    assert isinstance(resp.id, str) and resp.id.startswith("chatcmpl-"), resp
    assert resp.object == "chat.completion", resp
    assert isinstance(resp.created, int), resp
    assert resp.choices and resp.choices[0].message.role == "assistant", resp
    assert resp.usage is not None, resp
    assert isinstance(resp.usage.prompt_tokens, int), resp
    assert isinstance(resp.usage.completion_tokens, int), resp
    assert isinstance(resp.usage.total_tokens, int), resp


def test_openai_chat_completion_streaming(api, openai_client):
    model = _first_generator_model(api)
    chunks = list(
        openai_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Say hello briefly"}],
            max_tokens=16,
            stream=True,
        )
    )
    assert chunks, "No streaming chunks returned"
    assert chunks[0].choices[0].delta.role == "assistant", chunks[0]
    for chunk in chunks:
        assert isinstance(chunk.id, str), chunk
        assert chunk.object == "chat.completion.chunk", chunk
        assert isinstance(chunk.created, int), chunk


def test_openai_list_models(openai_client):
    models = openai_client.models.list()
    assert models.object == "list", models
    assert isinstance(models.data, list), models
    for model in models.data:
        assert isinstance(model.id, str), model
        assert model.object == "model", model


def test_openai_chat_with_tools(api, openai_client):
    model = _first_tool_generator_model(api)
    resp = openai_client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": "Use the lookup function to find order 42."}],
        max_tokens=96,
        temperature=0.1,
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "lookup",
                    "description": "Look up an order by id",
                    "parameters": {
                        "type": "object",
                        "properties": {"id": {"type": "integer"}},
                        "required": ["id"],
                    },
                },
            }
        ],
        tool_choice="required",
    )
    assert resp.choices, resp
    choice = resp.choices[0]
    assert choice.finish_reason in ("tool_calls", "stop", "length"), resp
    if choice.message.tool_calls:
        call = choice.message.tool_calls[0]
        assert call.type == "function", call
        assert call.function.name, call
        json.loads(call.function.arguments)
