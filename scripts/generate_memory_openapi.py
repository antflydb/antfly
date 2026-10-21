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

"""Generate the memory API source from the native memory wire contracts."""

import argparse
import json
from pathlib import Path
import subprocess
import yaml

ROOT = Path(__file__).resolve().parent.parent


def convert(value):
    if isinstance(value, list):
        return [convert(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "anyOf" in value and any(item.get("type") == "null" for item in value["anyOf"]):
        inner = next(item for item in value["anyOf"] if item.get("type") != "null")
        result = {
            **convert(inner),
            **{key: convert(child) for key, child in value.items() if key != "anyOf"},
            "nullable": True,
        }
    else:
        result = {key: convert(child) for key, child in value.items()}
    scalar = result.get("type") in ("integer", "number", "boolean", "string")
    if scalar and (result.get("nullable") or "default" in result):
        # Omission, explicit zero/false, and native defaults are distinct.
        result["x-go-type-skip-optional-pointer"] = False
    if result.get("type") == "integer" and result.get("format") in {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    }:
        result["x-go-type"] = result["format"]
    return result


def optional(schema, required=()):
    schema.pop("required", None)
    if required:
        schema["required"] = list(required)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    native = subprocess.check_output(
        [
            args.zig,
            "run",
            "--cache-dir",
            str(ROOT / "zig/.zig-cache"),
            "pkg/antfly-embedded/src/memory_schema_export.zig",
        ],
        cwd=ROOT / "zig",
    )
    exported = json.loads(native)
    mcp_tools = exported.pop("_memory_mcp_tools")
    schemas = convert(exported)

    def pin_integer_contract(value):
        if isinstance(value, list):
            for child in value:
                pin_integer_contract(child)
        elif isinstance(value, dict):
            props = value.get("properties", {})
            if {
                "database",
                "namespace",
                "table",
                "table_id",
                "kind",
                "id",
                "revision",
            }.issubset(props):
                for name in ("table_id", "revision"):
                    props[name].update(
                        {
                            "minimum": 1,
                            "maximum": 18446744073709551615,
                            "format": "uint64",
                            "x-go-type": "uint64",
                        }
                    )
            for child in value.values():
                pin_integer_contract(child)

    pin_integer_contract(schemas)

    def ref(name):
        return {"$ref": f"#/components/schemas/{name}"}

    schemas["MemoryObservationRequest"]["properties"]["evidence"]["items"] = ref(
        "MemoryEvidence"
    )
    schemas["MemoryObservationRequest"]["properties"]["assertions"]["items"] = ref(
        "MemoryAssertion"
    )
    schemas["MemoryAssertionRequest"]["properties"]["assertions"]["items"] = ref(
        "MemoryAssertion"
    )
    schemas["MemoryConfigureRequest"]["properties"]["config"] = ref("MemorySpaceConfig")
    schemas["MemoryCreateSpaceRequest"] = {
        "type": "object",
        "additionalProperties": False,
        "required": ["space"],
        "properties": {
            "space": {"type": "string", "maxLength": 128},
            "config": ref("MemorySpaceConfig"),
        },
    }
    schemas["MemoryResult"] = {
        "type": "object",
        "additionalProperties": True,
        "description": "Space state, a memory/history page, or recall with items, revision, completeness, token bounds and cursor. Temporal status is authoritative, not inferred from retrieved candidates.",
    }
    schemas["MemoryError"] = {
        "type": "object",
        "required": ["code", "error", "retryable"],
        "properties": {
            "code": {"type": "string"},
            "error": {"type": "string"},
            "retryable": {"type": "boolean"},
        },
    }
    paths = {}

    def operation(
        path, method, name, request=None, response="MemoryOperation", mutation=False
    ):
        params = [
            {
                "name": field,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
            for field in ("space", "id")
            if "{" + field + "}" in path
        ]
        params.extend(
            {
                "name": name,
                "in": "header",
                "schema": {
                    "type": "string",
                    "default": value,
                    "x-go-type-skip-optional-pointer": False,
                },
                "description": "Catalog scope for this memory space.",
            }
            for name, value in (
                ("X-Antfly-Database", "default"),
                ("X-Antfly-Namespace", "public"),
            )
        )
        if method == "get" and name in (
            "listMemories",
            "getMemoryHistory",
            "listMemoryEntities",
        ):
            fields = {
                "cursor": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 256},
            }
            if name == "listMemories":
                fields.update(
                    {
                        key: {"type": "string"}
                        for key in (
                            "subject",
                            "predicate",
                            "scope",
                            "task",
                            "environment",
                            "kind",
                            "relationship",
                            "target_id",
                            "missing_relationship",
                        )
                    }
                )
                fields.update(
                    {
                        key: {"type": "boolean"}
                        for key in ("include_history", "include_provisional")
                    }
                )
                for field in ("valid_at", "known_at"):
                    fields[field] = {
                        **schemas["MemoryRecallRequest"]["properties"][field],
                        "format": "int64",
                    }
            params.extend(
                {
                    "name": key,
                    "in": "query",
                    "schema": {**schema, "x-go-type-skip-optional-pointer": False},
                }
                for key, schema in fields.items()
            )
        if mutation:
            params.append(
                {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "required": True,
                    "schema": {"type": "string", "maxLength": 256},
                    "description": "Stable per-principal operation key. Conflicting reuse returns 409.",
                }
            )
        item = {
            "operationId": name,
            "tags": ["memory"],
            "parameters": params,
            "responses": {
                "201" if name == "createMemorySpace" else "200": {
                    "description": "Accepted durable operation or requested memory result.",
                    "content": {"application/json": {"schema": ref(response)}},
                }
            },
        }
        if request:
            item["requestBody"] = {
                "required": True,
                "content": {"application/json": {"schema": ref(request)}},
            }
        for code in (400, 401, 403, 404, 409, 413, 422, 429, 503):
            item["responses"][str(code)] = {
                "description": "Memory validation, authorization, concurrency, capability, or execution error.",
                "content": {"application/json": {"schema": ref("MemoryError")}},
            }
        paths.setdefault(path, {})[method] = item

    prefix = "/memory/v1/spaces"
    operation(
        prefix, "post", "createMemorySpace", "MemoryCreateSpaceRequest", mutation=True
    )
    operation(prefix + "/{space}", "get", "getMemorySpace", response="MemoryInspection")
    operation(
        prefix + "/{space}",
        "patch",
        "configureMemorySpace",
        "MemoryConfigureRequest",
        mutation=True,
    )
    for suffix, name, request in (
        ("embeddings", "stageMemoryEmbeddings", "MemoryEmbeddingBatch"),
        ("observations", "observeMemory", "MemoryObservationRequest"),
        ("assertions", "storeMemoryAssertions", "MemoryAssertionRequest"),
        ("corrections", "correctMemory", "MemoryCorrection"),
        ("forget", "forgetMemory", "MemoryForgetRequest"),
        ("outcomes", "recordMemoryOutcome", "MemoryOutcome"),
    ):
        operation(prefix + "/{space}/" + suffix, "post", name, request, mutation=True)
    operation(
        prefix + "/{space}/explain",
        "post",
        "explainMemoryResource",
        "MemoryExplainRequest",
        "MemoryExplanation",
    )
    operation(
        prefix + "/{space}/memories/{id}/explain",
        "post",
        "explainMemory",
        "MemoryExplainRequest",
        "MemoryExplanation",
    )
    operation(
        prefix + "/{space}/recall",
        "post",
        "recallMemory",
        "MemoryRecallRequest",
        "MemoryRecallResult",
    )
    operation(
        prefix + "/{space}/memories", "get", "listMemories", response="MemoryRecordPage"
    )
    operation(
        prefix + "/{space}/memories/{id}", "get", "getMemory", response="MemoryRecord"
    )
    operation(
        prefix + "/{space}/memories/{id}/history",
        "get",
        "getMemoryHistory",
        response="MemoryRecordPage",
    )
    operation(prefix + "/{space}/operations/{id}", "get", "getMemoryOperation")
    operation(
        prefix + "/{space}/operations/{id}",
        "post",
        "advanceMemoryOperation",
        mutation=True,
    )
    operation(
        prefix + "/{space}/maintenance", "post", "maintainMemorySpace", mutation=True
    )
    operation(
        prefix + "/{space}/configurations/{id}",
        "get",
        "getMemoryConfiguration",
        response="MemorySpaceConfig",
    )
    operation(
        prefix + "/{space}/evidence/{id}",
        "get",
        "getMemoryEvidence",
        response="MemoryResult",
    )
    operation(
        prefix + "/{space}/entity-corrections",
        "post",
        "curateMemoryEntity",
        "MemoryEntityCorrection",
        mutation=True,
    )
    operation(
        prefix + "/{space}/entities/{id}",
        "get",
        "resolveMemoryEntity",
        response="MemoryResult",
    )
    operation(
        prefix + "/{space}/entities",
        "get",
        "listMemoryEntities",
        response="MemoryEntityPage",
    )
    spec = {
        "openapi": "3.0.3",
        "info": {
            "title": "Antfly Memory API",
            "version": "1.0.0",
            "description": "One native service for HTTP, SDK, MCP and embedded memory. Spaces default to model-free processing; model dependencies are feature based.",
        },
        "servers": [{"url": "/"}],
        "tags": [
            {"name": "memory", "description": "Evidence-backed temporal agent memory"}
        ],
        "paths": paths,
        "components": {"schemas": schemas},
    }
    target = ROOT / "specs/openapi/memory/api.yaml"
    output = (
        "# Generated by scripts/generate_memory_openapi.py from native memory contracts.\n"
        + yaml.safe_dump(spec, sort_keys=False, allow_unicode=True)
    )
    mcp_target = ROOT / "zig/pkg/antfly/src/api/generated/mcp_memory_tools.zig"
    mcp_output = (
        "// Generated by scripts/generate_memory_openapi.py from native operation contracts.\n"
        "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: Elastic-2.0\n"
        "pub const Tool = struct { action: []const u8, name: []const u8, description: []const u8, input_schema_json: []const u8 };\n"
        "pub const tools = [_]Tool{\n"
        + "".join(
            "    .{ "
            + ", ".join(f".{key} = {json.dumps(value)}" for key, value in tool.items())
            + " },\n"
            for tool in mcp_tools
        )
        + "};\n"
    )
    mcp_output = subprocess.check_output(
        [args.zig, "fmt", "--stdin"], input=mcp_output.encode()
    ).decode()
    if args.check:
        if not mcp_target.exists() or mcp_target.read_text() != mcp_output:
            raise SystemExit(
                "memory MCP tools are stale; run scripts/generate_memory_openapi.py"
            )
        if not target.exists() or target.read_text() != output:
            raise SystemExit(
                "memory OpenAPI source is stale; run scripts/generate_memory_openapi.py"
            )
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(output)
        mcp_target.write_text(mcp_output)


if __name__ == "__main__":
    main()
