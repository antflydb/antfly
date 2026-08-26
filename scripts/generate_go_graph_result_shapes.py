#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Generate strict Go graph-result decoding shapes from the public OpenAPI schema."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = ROOT / "specs/openapi/antfly/indexes.yaml"
OUTPUT_PATH = ROOT / "go/pkg/sdk/oapi/graph_result_shapes_generated.go"
ROOT_SCHEMAS = ("GraphBindingsResult", "GraphAggregatesResult", "GraphNodesResult")
REF_PREFIX = "#/components/schemas/"


def load_schemas() -> dict[str, dict[str, Any]]:
    document = yaml.safe_load(SOURCE_PATH.read_text(encoding="utf-8"))
    return document["components"]["schemas"]


def referenced_schema_names(schema: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(schema, dict):
        ref = schema.get("$ref")
        if isinstance(ref, str) and ref.startswith(REF_PREFIX):
            names.add(ref[len(REF_PREFIX) :])
        for value in schema.values():
            names.update(referenced_schema_names(value))
    elif isinstance(schema, list):
        for value in schema:
            names.update(referenced_schema_names(value))
    return names


def reachable_schema_names(schemas: dict[str, dict[str, Any]]) -> list[str]:
    reachable: set[str] = set()

    def visit(name: str) -> None:
        if name in reachable:
            return
        if name not in schemas:
            raise ValueError(f"unknown local schema reference {name!r}")
        reachable.add(name)
        for dependency in sorted(referenced_schema_names(schemas[name])):
            visit(dependency)

    for root in ROOT_SCHEMAS:
        visit(root)
    return sorted(reachable)


def go_identifier(name: str) -> str:
    return "generated" + name + "Shape"


def go_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def nullable_branch(schema: dict[str, Any]) -> dict[str, Any] | None:
    alternatives = schema.get("oneOf")
    if not isinstance(alternatives, list):
        return None
    concrete: list[dict[str, Any]] = []
    has_null = False
    for alternative in alternatives:
        if alternative.get("enum") == [None] or alternative.get("type") == "null":
            has_null = True
        else:
            concrete.append(alternative)
    if has_null and len(concrete) == 1:
        return concrete[0]
    return None


def render_shape(schema: dict[str, Any], name: str, indent: str = "") -> str:
    nullable = nullable_branch(schema)
    if nullable is not None:
        return (
            f"&requiredJSONShape{{name: {go_string(name)}, nullable: true, "
            f"reference: {render_shape(nullable, name, indent)}}}"
        )
    if "oneOf" in schema:
        raise ValueError(f"unsupported non-nullable oneOf in result schema {name!r}")
    if "anyOf" in schema:
        raise ValueError(f"unsupported anyOf in result schema {name!r}")

    ref = schema.get("$ref")
    if isinstance(ref, str):
        if not ref.startswith(REF_PREFIX):
            raise ValueError(f"unsupported external schema reference {ref!r}")
        return go_identifier(ref[len(REF_PREFIX) :])

    all_of = schema.get("allOf")
    if all_of is not None:
        if isinstance(all_of, list) and len(all_of) == 1:
            return render_shape(all_of[0], name, indent)
        raise ValueError(f"unsupported allOf in result schema {name!r}")

    schema_type = schema.get("type")
    if schema_type == "array":
        item = render_shape(schema.get("items", {}), f"{name}.items", indent + "\t")
        return (
            f"&requiredJSONShape{{name: {go_string(name)}, array: true, items: {item}}}"
        )

    if (
        schema_type == "object"
        or "properties" in schema
        or "additionalProperties" in schema
    ):
        required = schema.get("required", [])
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        if not required and not properties and additional is True:
            return (
                f"&requiredJSONShape{{name: {go_string(name)}, opaqueObject: true}}"
            )

        fields = [f"name: {go_string(name)}", "object: true"]
        if required:
            values = ", ".join(go_string(value) for value in required)
            fields.append(f"required: []string{{{values}}}")
        if properties:
            entries = []
            for key in sorted(properties):
                child = render_shape(properties[key], f"{name}.{key}", indent + "\t")
                entries.append(f"{go_string(key)}: {child}")
            fields.append(
                "properties: map[string]*requiredJSONShape{" + ", ".join(entries) + "}"
            )
        if additional is not False:
            fields.append("allowAdditionalProperties: true")
            if isinstance(additional, dict):
                fields.append(
                    "additionalProperties: "
                    + render_shape(additional, f"{name}.*", indent + "\t")
                )
        return "&requiredJSONShape{" + ", ".join(fields) + "}"

    fields = [f"name: {go_string(name)}"]
    if schema_type == "string" and schema.get("minLength", 0) >= 1:
        fields.append("nonEmptyString: true")
    return "&requiredJSONShape{" + ", ".join(fields) + "}"


def render_go(schemas: dict[str, dict[str, Any]]) -> str:
    declarations = []
    for name in reachable_schema_names(schemas):
        declarations.append(
            f"var {go_identifier(name)} = {render_shape(schemas[name], name)}"
        )
    cases = "\n".join(
        f"\tcase *{name}:\n\t\treturn {go_identifier(name)}" for name in ROOT_SCHEMAS
    )
    source = f"""// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Code generated by scripts/generate_go_graph_result_shapes.py from
// specs/openapi/antfly/indexes.yaml; DO NOT EDIT.

package oapi

{chr(10).join(declarations)}

func canonicalGraphResultShape(value any) *requiredJSONShape {{
\tswitch value.(type) {{
{cases}
\tdefault:
\t\treturn nil
\t}}
}}
"""
    formatted = subprocess.run(
        ["gofmt"], input=source, text=True, capture_output=True, check=True
    )
    return formatted.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render_go(load_schemas())
    if args.check:
        if (
            not OUTPUT_PATH.exists()
            or OUTPUT_PATH.read_text(encoding="utf-8") != generated
        ):
            print(f"{OUTPUT_PATH.relative_to(ROOT)} is stale; run make generate")
            return 1
        return 0
    OUTPUT_PATH.write_text(generated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
