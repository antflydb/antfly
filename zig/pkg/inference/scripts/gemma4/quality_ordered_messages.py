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

"""Strict ordered serving-message identity shared with Zig (schemas 1 and 2)."""
import hashlib
import json
import math
import struct

SCHEMA_VERSION = 1
TOOL_HISTORY_SCHEMA_VERSION = 2
MAX_MESSAGES = 64
MAX_PARTS_PER_MESSAGE = 16
MAX_TOTAL_TEXT_BYTES = 8 * 1024 * 1024
MAX_TOOL_CALLS_PER_MESSAGE = 8
MAX_TOTAL_TOOL_CALLS = 32
MAX_TOOL_ID_BYTES = 128
MAX_TOOL_NAME_BYTES = 128
MAX_TOOL_ARGUMENTS_BYTES = 4 * 1024
MAX_TOOL_ARGUMENT_VALUES = 256
ROLE_TAGS = {"system": 1, "user": 2, "assistant": 3, "tool": 4}
_DOMAIN_V1 = b"antfly.context-serving.structured-messages.v1\0"
_DOMAIN_V2 = b"antfly.context-serving.structured-messages.v2\0"


def _reject_constant(value):
    raise ValueError("non-finite JSON constant: " + value)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field: " + key)
        result[key] = value
    return result


def loads(raw):
    try:
        value = json.loads(raw, object_pairs_hook=_object, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid ordered-message JSON") from exc
    validate(value)
    return value


def _bounded_utf8(value, limit):
    if type(value) is not str:
        return False
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return False
    return 0 < size <= limit


def _arguments(value):
    if not _bounded_utf8(value, MAX_TOOL_ARGUMENTS_BYTES):
        raise ValueError("invalid tool arguments")
    try:
        parsed = json.loads(value, object_pairs_hook=_object, parse_constant=_reject_constant,
                            parse_int=_parse_i64, parse_float=_parse_finite_f64)
    except (json.JSONDecodeError, RecursionError, OverflowError) as exc:
        raise ValueError("invalid tool arguments") from exc
    if type(parsed) is not dict:
        raise ValueError("tool arguments must be an object")
    pending = [(parsed, 1)]
    count = 0
    while pending:
        current, depth = pending.pop()
        count += 1
        if count > MAX_TOOL_ARGUMENT_VALUES:
            raise ValueError("tool arguments exceed value-count bound")
        if type(current) in (dict, list) and depth > 64:
            raise ValueError("tool arguments exceed nesting bound")
        if type(current) is dict:
            pending.extend((item, depth + 1) for item in current.values())
        elif type(current) is list:
            pending.extend((item, depth + 1) for item in current)


def _parse_i64(raw):
    value = int(raw)
    if value < -(1 << 63) or value > (1 << 63) - 1:
        raise ValueError("tool argument integer is outside signed 64-bit range")
    return value


def _parse_finite_f64(raw):
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("tool argument float is not finite")
    return value


def validate(value):
    if type(value) is not dict or set(value) != {"schema_version", "messages"} or \
            type(value["schema_version"]) is not int or value["schema_version"] not in (1, 2):
        raise ValueError("invalid ordered-message artifact")
    schema = value["schema_version"]
    messages = value["messages"]
    if type(messages) is not list or not 1 <= len(messages) <= MAX_MESSAGES:
        raise ValueError("invalid ordered-message count")
    selected = total = total_calls = 0
    seen_ids = set()
    for index, message in enumerate(messages):
        if type(message) is not dict or message.get("role") not in ROLE_TAGS:
            raise ValueError("invalid ordered message")
        if schema == 1:
            if set(message) != {"role", "parts"}:
                raise ValueError("schema 1 metadata is forbidden")
        elif message["role"] == "assistant" and "tool_calls" in message:
            if set(message) != {"role", "parts", "content_is_null", "tool_calls"}:
                raise ValueError("invalid assistant tool-call shape")
        elif message["role"] == "tool":
            if set(message) != {"role", "parts", "tool_call_id"}:
                raise ValueError("invalid tool-response shape")
        elif set(message) != {"role", "parts"}:
            raise ValueError("invalid ordered-message metadata")
        parts = message["parts"]
        if type(parts) is not list or len(parts) > MAX_PARTS_PER_MESSAGE:
            raise ValueError("invalid ordered-message parts")
        content_is_null = message.get("content_is_null", False)
        calls = message.get("tool_calls")
        response_id = message.get("tool_call_id")
        if type(content_is_null) is not bool:
            raise ValueError("invalid null-content marker")
        if schema == 1 and not parts:
            raise ValueError("invalid ordered-message parts")
        if schema == 2:
            if message["role"] == "assistant" and "tool_calls" in message:
                if not content_is_null or parts or response_id is not None or type(calls) is not list or \
                        not 1 <= len(calls) <= MAX_TOOL_CALLS_PER_MESSAGE:
                    raise ValueError("invalid assistant tool call")
                total_calls += len(calls)
                if total_calls > MAX_TOTAL_TOOL_CALLS or index + len(calls) >= len(messages):
                    raise ValueError("invalid tool-call bounds")
                for offset, call in enumerate(calls):
                    if type(call) is not dict or set(call) != {"id", "type", "function"} or \
                            call["type"] != "function" or not _bounded_utf8(call["id"], MAX_TOOL_ID_BYTES) or \
                            call["id"] in seen_ids:
                        raise ValueError("invalid or duplicate tool call")
                    function = call["function"]
                    if type(function) is not dict or set(function) != {"name", "arguments_json"} or \
                            not _bounded_utf8(function["name"], MAX_TOOL_NAME_BYTES):
                        raise ValueError("invalid tool function")
                    _arguments(function["arguments_json"])
                    seen_ids.add(call["id"])
                    response = messages[index + 1 + offset]
                    if type(response) is not dict or response.get("role") != "tool" or \
                            response.get("tool_call_id") != call["id"]:
                        raise ValueError("missing adjacent tool response")
            elif message["role"] == "tool":
                if content_is_null or not parts or calls is not None or not _bounded_utf8(response_id, MAX_TOOL_ID_BYTES):
                    raise ValueError("invalid tool response")
                cursor = index - 1
                while cursor >= 0 and messages[cursor].get("role") == "tool":
                    cursor -= 1
                if cursor < 0 or messages[cursor].get("role") != "assistant":
                    raise ValueError("orphan tool response")
                prior_calls = messages[cursor].get("tool_calls")
                offset = index - cursor - 1
                if type(prior_calls) is not list or offset >= len(prior_calls) or prior_calls[offset].get("id") != response_id:
                    raise ValueError("mismatched tool response")
            elif content_is_null or not parts or calls is not None or response_id is not None:
                raise ValueError("metadata is restricted to tool history")
        for part in parts:
            if type(part) is not dict or not {"text"} <= set(part) <= {"text", "selected"}:
                raise ValueError("invalid ordered-message part")
            text, chosen = part["text"], part.get("selected", False)
            if type(text) is not str or not text or type(chosen) is not bool:
                raise ValueError("invalid ordered-message text")
            raw = text.encode("utf-8")
            total += len(raw)
            if chosen and message["role"] not in ("user", "tool"):
                raise ValueError("selected ordered-message part lacks authoritative role")
            selected += int(chosen)
    if total > MAX_TOTAL_TEXT_BYTES or not 1 <= selected <= 64:
        raise ValueError("invalid ordered-message bounds")
    return value


def _field(h, raw):
    h.update(struct.pack("<Q", len(raw)))
    h.update(raw)


def semantic_digest(value):
    validate(value)
    schema = value["schema_version"]
    h = hashlib.sha256()
    h.update(_DOMAIN_V1 if schema == 1 else _DOMAIN_V2)
    h.update(struct.pack("<I", len(value["messages"])))
    for message in value["messages"]:
        h.update(bytes((ROLE_TAGS[message["role"]],)))
        h.update(struct.pack("<I", len(message["parts"])))
        for part in message["parts"]:
            raw = part["text"].encode("utf-8")
            h.update(bytes((int(part.get("selected", False)),)))
            _field(h, raw)
        if schema == 2:
            h.update(bytes((int(message.get("content_is_null", False)),)))
            calls = message.get("tool_calls") or []
            h.update(struct.pack("<I", len(calls)))
            for call in calls:
                for value_part in (call["id"], call["type"], call["function"]["name"],
                                   call["function"]["arguments_json"]):
                    _field(h, value_part.encode("utf-8"))
            _field(h, (message.get("tool_call_id") or "").encode("utf-8"))
    return h.hexdigest()


def selected_source_digest(value):
    """Digest selected source bytes independently of unselected questions."""
    validate(value)
    selected = [part["text"].encode("utf-8")
                for message in value["messages"] for part in message["parts"]
                if part.get("selected", False)]
    h = hashlib.sha256()
    h.update(b"antfly.context-serving.selected-source.v1\0")
    h.update(struct.pack("<I", len(selected)))
    for raw in selected:
        _field(h, raw)
    return h.hexdigest()


def selected_part_digests(value):
    """Raw SHA-256 identities for every selected part in encounter order."""
    validate(value)
    return [hashlib.sha256(part["text"].encode("utf-8")).hexdigest()
            for message in value["messages"] for part in message["parts"]
            if part.get("selected", False)]
