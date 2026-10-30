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

"""Project pinned AWS restJson1 Converse shapes into our OpenAPI type input.

This deliberately supports the Bedrock Runtime model's JSON shape vocabulary,
not arbitrary Smithy services. It is offline and uses only the Python stdlib.
"""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "zig/specs/bedrock/bedrock-runtime-2023-09-30.json"
OUTPUT = ROOT / "zig/specs/bedrock-openapi.json"
SOURCES = ROOT / "zig/specs/bedrock/sources.json"
REVISION = "d6bbb4d64e2c82927834dfa9791bebc0000f70c0"
NAMESPACE = "com.amazonaws.bedrockruntime#"
API = "smithy.api#"
OPERATIONS = ("Converse", "ConverseStream")
HTTP_BINDINGS = ("httpLabel", "httpHeader", "httpQuery", "httpPrefixHeaders")


def export(model):
    shapes = model["shapes"]
    schemas = {}

    def ref(target):
        name = target.split("#")[1]
        if name not in schemas:
            # Register before recursing through document/tool/content graphs.
            schemas[name] = {}
            shape = shapes.get(target)
            if shape is None:
                if not target.startswith(API):
                    raise ValueError(f"Unknown shape: {target}")
                shape = {"type": name.lower()}
            schemas[name] = convert(shape, name)
        return {"$ref": f"#/components/schemas/{name}"}

    def traits(schema, source):
        metadata = source.get("traits", {})
        if API + "documentation" in metadata:
            schema["description"] = metadata[API + "documentation"]
        if API + "pattern" in metadata:
            schema["pattern"] = metadata[API + "pattern"]
        for bound, keyword in (("min", "minimum"), ("max", "maximum")):
            if bound in metadata.get(API + "range", {}):
                schema[keyword] = metadata[API + "range"][bound]
        if API + "length" in metadata:
            kind = source.get("type")
            if kind is None:
                target = source["target"]
                kind = shapes.get(target, {}).get("type", target.split("#")[1].lower())
            suffix = {"list": "Items", "map": "Properties"}.get(kind, "Length")
            for bound in ("min", "max"):
                if bound in metadata[API + "length"]:
                    schema[bound + suffix] = metadata[API + "length"][bound]
        return schema

    def member_schema(member):
        return traits(ref(member["target"]), member)

    def structure(shape, members=None):
        members = shape.get("members", {}) if members is None else members
        properties = {}
        required = []
        for key, member in members.items():
            wire_name = member.get("traits", {}).get(API + "jsonName", key)
            properties[wire_name] = member_schema(member)
            if API + "required" in member.get("traits", {}):
                required.append(wire_name)
        result = {"type": "object", "properties": properties}
        if required:
            result["required"] = required
        return result

    def convert(shape, owner):
        kind = shape["type"]
        if kind == "structure":
            result = structure(shape)
        elif kind == "union":
            # Named one-member wrappers preserve the actual tagged JSON wire
            # shape and allow openapi-zig to generate structural union types.
            variants = []
            for key, member in shape["members"].items():
                name = owner + key[0].upper() + key[1:] + "Variant"
                if name in schemas:
                    raise ValueError(f"Variant name collision: {name}")
                wire_name = member.get("traits", {}).get(API + "jsonName", key)
                schemas[name] = {
                    "type": "object",
                    "properties": {wire_name: member_schema(member)},
                    "required": [wire_name],
                    "additionalProperties": False,
                }
                variants.append({"$ref": f"#/components/schemas/{name}"})
            result = {"oneOf": variants}
        elif kind == "list":
            result = {"type": "array", "items": member_schema(shape["member"])}
        elif kind == "map":
            result = {
                "type": "object",
                "propertyNames": member_schema(shape["key"]),
                "additionalProperties": member_schema(shape["value"]),
            }
        elif kind in ("string", "enum"):
            result = {"type": "string"}
            if kind == "enum":
                result["enum"] = [
                    m["traits"][API + "enumValue"] for m in shape["members"].values()
                ]
        elif kind in ("integer", "long", "short", "byte"):
            result = {"type": "integer"}
        elif kind in ("float", "double"):
            result = {"type": "number"}
        elif kind == "boolean":
            result = {"type": "boolean"}
        elif kind == "blob":
            result = {"type": "string", "format": "byte"}
        elif kind == "document":
            result = {}  # Arbitrary JSON, including scalars and null.
        else:
            raise ValueError(f"Unsupported reachable Smithy type: {kind}")
        return traits(result, shape)

    paths = {}
    for operation in OPERATIONS:
        op = shapes[NAMESPACE + operation]
        binding = op["traits"][API + "http"]
        input_shape = shapes[op["input"]["target"]]
        body_members = {}
        parameters = []
        for key, member in input_shape["members"].items():
            metadata = member.get("traits", {})
            bindings = [name for name in HTTP_BINDINGS if API + name in metadata]
            if not bindings:
                body_members[key] = member
            elif bindings == ["httpLabel"]:
                parameters.append(
                    {
                        "name": key,
                        "in": "path",
                        "required": True,
                        "schema": member_schema(member),
                    }
                )
            else:
                raise ValueError(
                    f"Unsupported HTTP binding on {operation}.{key}: {bindings}"
                )
        body_name = operation + "RequestBody"
        schemas[body_name] = structure(input_shape, body_members)
        streaming = operation == "ConverseStream"
        if streaming:
            stream = shapes[op["output"]["target"]]["members"]["stream"]
            assert API + "httpPayload" in stream["traits"]
            assert API + "streaming" in shapes[stream["target"]]["traits"]
            # The response is binary AWS EventStream, not a JSON object. The
            # union describes decoded event envelopes for the existing framer.
            response_schema = {
                "type": "string",
                "format": "binary",
                "x-aws-event-payload": ref(stream["target"]),
            }
        else:
            response_schema = ref(op["output"]["target"])
        paths[binding["uri"]] = {
            binding["method"].lower(): {
                "operationId": operation,
                "parameters": parameters,
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {"$ref": f"#/components/schemas/{body_name}"}
                        }
                    },
                },
                "responses": {
                    str(binding["code"]): {
                        "description": "Successful response",
                        "content": {
                            (
                                "application/vnd.amazon.eventstream"
                                if streaming
                                else "application/json"
                            ): {"schema": response_schema}
                        },
                    }
                },
            }
        }
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Bedrock Runtime Converse (AWS Smithy projection)",
            "version": "2023-09-30",
        },
        "paths": paths,
        "components": {"schemas": dict(sorted(schemas.items()))},
    }


def render(value):
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    raw = MODEL.read_bytes()
    output = render(export(json.loads(raw)))
    provenance = render(
        {
            "repository": "https://github.com/aws/api-models-aws",
            "commit": REVISION,
            "model_path": "models/bedrock-runtime/service/2023-09-30/bedrock-runtime-2023-09-30.json",
            "model_sha256": hashlib.sha256(raw).hexdigest(),
            "openapi_sha256": hashlib.sha256(output.encode()).hexdigest(),
            "operations": list(OPERATIONS),
            "generator": "scripts/generate_bedrock_openapi.py",
        }
    )
    if args.check:
        for path, expected in ((OUTPUT, output), (SOURCES, provenance)):
            if not path.exists() or path.read_text() != expected:
                raise SystemExit(f"Out of date: {path.relative_to(ROOT)}")
        print("Bedrock Smithy projection and provenance match")
    else:
        OUTPUT.write_text(output)
        SOURCES.write_text(provenance)


if __name__ == "__main__":
    main()
