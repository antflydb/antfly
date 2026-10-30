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

"""Export the official SDK's Messages models as our pinned OpenAPI input.

Run with `uv run scripts/generate_anthropic_openapi.py`; ordinary code generation
uses the committed JSON and does not install the SDK or access the network.
"""

import argparse
import hashlib
import json
from pathlib import Path

import anthropic
from anthropic.types import Message
from anthropic.types.message_create_params import MessageCreateParamsBase
from pydantic import TypeAdapter

ROOT = Path(__file__).resolve().parents[1]


def export():
    schemas = {}

    def rewrite(value):
        if isinstance(value, dict):
            return {
                key: (
                    item.replace("#/$defs/", "#/components/schemas/")
                    if key == "$ref"
                    else rewrite(item)
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [rewrite(item) for item in value]
        return value

    for name, raw in (
        ("CreateMessageRequest", TypeAdapter(MessageCreateParamsBase).json_schema()),
        ("Message", Message.model_json_schema()),
    ):
        definitions = raw.pop("$defs", {})
        for key, value in definitions.items():
            value = rewrite(value)
            if key in schemas and schemas[key] != value:
                raise ValueError(f"conflicting SDK definition: {key}")
            schemas[key] = value
        schemas[name] = rewrite(raw)
    # The official streaming/non-streaming TypedDict variants differ only in
    # their stream literal. Their common base plus a boolean represents both.
    schemas["CreateMessageRequest"]["properties"]["stream"] = {"type": "boolean"}
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Anthropic Messages API (official SDK models)",
            "version": anthropic.__version__,
        },
        "servers": [{"url": "https://api.anthropic.com"}],
        "paths": {
            "/v1/messages": {
                "post": {
                    "operationId": "createMessage",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/CreateMessageRequest"
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Message response (non-streaming)",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Message"}
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {"schemas": dict(sorted(schemas.items()))},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    spec = ROOT / "zig/specs/anthropic-openapi.json"
    data = (json.dumps(export(), indent=2, ensure_ascii=False) + "\n").encode()
    metadata = ROOT / "zig/specs/anthropic/sources.json"
    source = {
        "sdk_version": anthropic.__version__,
        "sdk_commit": "202db72c87d7c6eaa72129c67ce67fd343a3a20c",
        "sdk_source": "https://github.com/anthropics/anthropic-sdk-python/tree/v2.3.0/src/anthropic/types",
        "api_version": "2026-10-21",
        "pydantic_version": "2.13.4",
        "spec_sha256": hashlib.sha256(data).hexdigest(),
    }
    source_data = (json.dumps(source, indent=2) + "\n").encode()
    if args.check:
        for path, expected in ((spec, data), (metadata, source_data)):
            if path.read_bytes() != expected:
                raise SystemExit(f"out of date: {path}")
    else:
        spec.write_bytes(data)
        metadata.write_bytes(source_data)
    print(f"{spec}: SHA-256 {hashlib.sha256(data).hexdigest()}")


if __name__ == "__main__":
    main()
