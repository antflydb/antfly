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

"""Tests for /api/chat/completions endpoint."""

import json

import pytest
from .models import default_generator_model_name

pytestmark = pytest.mark.model_integration


def _first_generator_model(api):
    generators = api.models().get("generators", {})
    if (model := default_generator_model_name(set(generators.keys()))) is not None:
        return model
    if generators:
        return next(iter(generators.keys()))
    pytest.skip("No generator models available for /api/chat/completions tests")


def test_chat_basic(api):
    model = _first_generator_model(api)
    resp = api.chat(
        [{"role": "user", "content": "Say hello briefly"}],
        model=model,
        max_tokens=16,
    )
    assert isinstance(resp.get("id"), str) and resp["id"].startswith("chatcmpl-"), resp
    assert resp.get("object") == "chat.completion", resp
    assert isinstance(resp.get("created"), int), resp
    assert resp.get("choices", [{}])[0].get("message", {}).get("role") == "assistant", resp
    assert isinstance(resp.get("usage"), dict), resp


@pytest.mark.streaming
def test_chat_streaming(api):
    model = _first_generator_model(api)
    r = api.chat(
        [{"role": "user", "content": "Say hello briefly"}],
        model=model,
        max_tokens=16,
        stream=True,
    )
    body = r.text
    r.close()

    data_lines = [line[len("data: "):] for line in body.splitlines() if line.startswith("data: ")]
    assert data_lines and data_lines[-1] == "[DONE]", body

    events = [json.loads(line) for line in data_lines[:-1]]
    assert events[0].get("choices", [{}])[0].get("delta", {}).get("role") == "assistant", events[0]
    assert any(event.get("choices", [{}])[0].get("delta", {}).get("content") is not None for event in events[1:]), events
