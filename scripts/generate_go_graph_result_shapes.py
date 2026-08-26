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
ANNOTATION_KEYS = {
    "default",
    "deprecated",
    "description",
    "example",
    "format",
    "readOnly",
    "title",
    "writeOnly",
}
COMPOSITION_ANNOTATION_KEYS = ANNOTATION_KEYS - {"format"}
SUPPORTED_SHAPE_KEYS = {
    "$ref",
    "additionalProperties",
    "allOf",
    "anyOf",
    "enum",
    "items",
    "maxItems",
    "maxLength",
    "maxProperties",
    "maximum",
    "minItems",
    "minLength",
    "minProperties",
    "minimum",
    "oneOf",
    "pattern",
    "properties",
    "required",
    "type",
}


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


def validate_shape_keywords(schema: dict[str, Any], name: str) -> None:
    unsupported = sorted(
        key
        for key in schema
        if key not in SUPPORTED_SHAPE_KEYS
        and key not in ANNOTATION_KEYS
        and not key.startswith("x-")
    )
    if unsupported:
        raise ValueError(f"unsupported schema keywords for {name!r}: {unsupported!r}")


def integer_constraint(schema: dict[str, Any], key: str, name: str) -> int | None:
    value = schema.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name}.{key} must be a non-negative integer")
    return value


def numeric_constraint(
    schema: dict[str, Any], key: str, name: str
) -> float | int | None:
    value = schema.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name}.{key} must be numeric")  # noqa: TRY004
    return value


def append_collection_constraints(
    fields: list[str], schema: dict[str, Any], name: str, noun: str
) -> None:
    minimum = integer_constraint(schema, f"min{noun}", name)
    maximum = integer_constraint(schema, f"max{noun}", name)
    if minimum is not None:
        fields.append(f"min{noun}: {minimum}")
    if maximum is not None:
        fields.extend((f"max{noun}: {maximum}", f"hasMax{noun}: true"))
    if minimum is not None and maximum is not None and minimum > maximum:
        raise ValueError(f"{name} has min{noun} greater than max{noun}")


def append_scalar_constraints(
    fields: list[str], schema: dict[str, Any], name: str, schema_type: str
) -> None:
    schema_format = schema.get("format")
    allowed_formats = {
        "string": set(),
        "boolean": set(),
        "integer": {"uint64"},
        "number": {"double"},
    }[schema_type]
    if schema_format is not None and schema_format not in allowed_formats:
        raise ValueError(
            f"unsupported {schema_type} format in result schema {name!r}: {schema_format!r}"
        )
    if schema_type == "string":
        append_collection_constraints(fields, schema, name, "Length")
        pattern = schema.get("pattern")
        if pattern is not None:
            if pattern != "^[0-9]+$":
                raise ValueError(
                    f"unsupported string pattern in result schema {name!r}: {pattern!r}"
                )
            fields.append("unsignedDecimalString: true")
    elif any(key in schema for key in ("minLength", "maxLength", "pattern")):
        raise ValueError(
            f"string constraints used by non-string result schema {name!r}"
        )

    minimum = numeric_constraint(schema, "minimum", name)
    maximum = numeric_constraint(schema, "maximum", name)
    if minimum is not None or maximum is not None:
        if schema_type not in ("integer", "number"):
            raise ValueError(
                f"numeric constraints used by non-numeric result schema {name!r}"
            )
        if minimum is not None:
            fields.extend((f"minimum: {minimum!r}", "hasMinimum: true"))
        if maximum is not None:
            fields.extend((f"maximum: {maximum!r}", "hasMaximum: true"))
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError(f"{name} has minimum greater than maximum")

    enum = schema.get("enum")
    if enum is None:
        return
    if not isinstance(enum, list) or not enum:
        raise ValueError(f"{name}.enum must be a non-empty list")
    if schema_type == "string" and all(isinstance(value, str) for value in enum):
        values = ", ".join(go_string(value) for value in enum)
        fields.append(f"allowedStrings: []string{{{values}}}")
        return
    if schema_type == "boolean" and all(isinstance(value, bool) for value in enum):
        mask = (1 if False in enum else 0) | (2 if True in enum else 0)
        fields.append(f"allowedBools: {mask}")
        return
    raise ValueError(f"unsupported enum in result schema {name!r}: {enum!r}")


def reject_constraints(
    schema: dict[str, Any], name: str, keys: tuple[str, ...]
) -> None:
    present = sorted(key for key in keys if key in schema)
    if present:
        raise ValueError(f"invalid constraints for result schema {name!r}: {present!r}")


def reject_composition_siblings(
    schema: dict[str, Any], name: str, composition_key: str
) -> None:
    siblings = sorted(
        key
        for key in schema
        if key != composition_key
        and key not in COMPOSITION_ANNOTATION_KEYS
        and not key.startswith("x-")
    )
    if siblings:
        raise ValueError(
            f"unsupported validation siblings beside {composition_key} "
            f"in result schema {name!r}: {siblings!r}"
        )


def nullable_branch(schema: dict[str, Any]) -> dict[str, Any] | None:
    alternatives = schema.get("oneOf")
    if not isinstance(alternatives, list):
        return None
    concrete: list[dict[str, Any]] = []
    has_null = False
    for alternative in alternatives:
        if not isinstance(alternative, dict):
            raise ValueError("oneOf alternatives must be schema objects")
        if alternative.get("enum") == [None] or alternative.get("type") == "null":
            has_null = True
        else:
            concrete.append(alternative)
    if has_null and len(concrete) == 1:
        return concrete[0]
    return None


def render_shape(schema: dict[str, Any], name: str, indent: str = "") -> str:
    validate_shape_keywords(schema, name)
    nullable = nullable_branch(schema)
    if nullable is not None:
        reject_composition_siblings(schema, name, "oneOf")
        return (
            f"&requiredJSONShape{{name: {go_string(name)}, nullable: true, "
            f"reference: {render_shape(nullable, name, indent)}}}"
        )
    if "oneOf" in schema:
        raise ValueError(f"unsupported non-nullable oneOf in result schema {name!r}")
    if "anyOf" in schema:
        raise ValueError(f"unsupported anyOf in result schema {name!r}")

    if "$ref" in schema:
        ref = schema["$ref"]
        if not isinstance(ref, str):
            raise ValueError(f"schema reference for {name!r} must be a string")
        if not ref.startswith(REF_PREFIX):
            raise ValueError(f"unsupported external schema reference {ref!r}")
        reject_composition_siblings(schema, name, "$ref")
        return go_identifier(ref[len(REF_PREFIX) :])

    if "allOf" in schema:
        all_of = schema["allOf"]
        if (
            isinstance(all_of, list)
            and len(all_of) == 1
            and isinstance(all_of[0], dict)
        ):
            reject_composition_siblings(schema, name, "allOf")
            return render_shape(all_of[0], name, indent)
        raise ValueError(f"unsupported allOf in result schema {name!r}")

    schema_type = schema.get("type")
    if schema_type == "array":
        reject_constraints(
            schema,
            name,
            (
                "enum",
                "format",
                "maxLength",
                "maxProperties",
                "maximum",
                "minLength",
                "minProperties",
                "minimum",
                "pattern",
                "properties",
                "required",
            ),
        )
        item = render_shape(schema.get("items", {}), f"{name}.items", indent + "\t")
        fields = [f"name: {go_string(name)}", "array: true", f"items: {item}"]
        append_collection_constraints(fields, schema, name, "Items")
        return "&requiredJSONShape{" + ", ".join(fields) + "}"

    if schema_type not in (None, "object") and (
        "properties" in schema or "additionalProperties" in schema
    ):
        raise ValueError(
            f"object constraints used by {schema_type!r} result schema {name!r}"
        )
    if (
        schema_type == "object"
        or "properties" in schema
        or "additionalProperties" in schema
    ):
        reject_constraints(
            schema,
            name,
            (
                "enum",
                "format",
                "items",
                "maxItems",
                "maxLength",
                "maximum",
                "minItems",
                "minLength",
                "minimum",
                "pattern",
            ),
        )
        required = schema.get("required", [])
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        if (
            not required
            and not properties
            and additional is True
            and "minProperties" not in schema
            and "maxProperties" not in schema
        ):
            return f"&requiredJSONShape{{name: {go_string(name)}, opaqueObject: true}}"

        fields = [f"name: {go_string(name)}", "object: true"]
        append_collection_constraints(fields, schema, name, "Properties")
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

    if schema_type not in ("string", "boolean", "integer", "number"):
        raise ValueError(
            f"unsupported or missing type in result schema {name!r}: {schema_type!r}"
        )
    if "items" in schema or any(key in schema for key in ("minItems", "maxItems")):
        raise ValueError(f"array constraints used by non-array result schema {name!r}")
    if any(key in schema for key in ("minProperties", "maxProperties")):
        raise ValueError(
            f"object constraints used by non-object result schema {name!r}"
        )
    fields = [f"name: {go_string(name)}"]
    append_scalar_constraints(fields, schema, name, schema_type)
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
