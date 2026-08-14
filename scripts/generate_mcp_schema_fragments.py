#!/usr/bin/env python3
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

"""Generate compact MCP JSON Schema fragments from the public OpenAPI contract."""

from __future__ import annotations

import argparse
import copy
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "specs/openapi/antfly/metadata.yaml"
OUTPUT_DIR = ROOT / "zig/pkg/antfly/src/api/generated"


@dataclass(frozen=True)
class Fragment:
    component: str
    output: str
    tool_input: bool = False
    aliases: dict[str, str] = field(default_factory=dict)
    exclude_deprecated: bool = False


FRAGMENTS = (
    Fragment(
        "QueryHierarchy",
        "mcp_query_hierarchy_schema.json",
        exclude_deprecated=True,
    ),
    Fragment("BackupRequest", "mcp_backup_input_schema.json", tool_input=True),
    Fragment("RestoreRequest", "mcp_restore_input_schema.json", tool_input=True),
    Fragment(
        "BatchRequest",
        "mcp_batch_input_schema.json",
        tool_input=True,
        aliases={"writes": "inserts"},
    ),
)


def resolve_local_refs(value: Any, schemas: dict[str, Any]) -> Any:
    if isinstance(value, list):
        return [resolve_local_refs(item, schemas) for item in value]
    if not isinstance(value, dict):
        return value

    ref = value.get("$ref")
    if ref is not None:
        prefix = "#/components/schemas/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            raise ValueError(f"unsupported MCP schema reference: {ref!r}")
        resolved = resolve_local_refs(schemas[ref.removeprefix(prefix)], schemas)
        siblings = {key: item for key, item in value.items() if key != "$ref"}
        if siblings:
            resolved = {**resolved, **resolve_local_refs(siblings, schemas)}
        return resolved

    return {key: resolve_local_refs(item, schemas) for key, item in value.items()}


def lower_camel(name: str) -> str:
    return re.sub(r"_([a-z])", lambda match: match.group(1).upper(), name)


def strip_annotations(value: Any) -> Any:
    """Keep validation and short defaults while excluding verbose API documentation."""
    if isinstance(value, list):
        return [strip_annotations(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: strip_annotations(item)
        for key, item in value.items()
        if key not in {"description", "example", "examples", "title", "externalDocs"}
    }


def strip_vendor_extensions(value: Any) -> Any:
    """Remove language- and generator-specific OpenAPI extensions from JSON Schema."""
    if isinstance(value, list):
        return [strip_vendor_extensions(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: strip_vendor_extensions(item)
        for key, item in value.items()
        if not key.startswith("x-")
    }


def without_deprecated_properties(value: Any) -> Any:
    """Remove deprecated properties from a schema view intended for new callers."""
    if isinstance(value, list):
        return [without_deprecated_properties(item) for item in value]
    if not isinstance(value, dict):
        return value

    result: dict[str, Any] = {}
    for key, item in value.items():
        if key == "deprecated":
            continue
        if key == "properties" and isinstance(item, dict):
            result[key] = {
                name: without_deprecated_properties(schema)
                for name, schema in item.items()
                if not (isinstance(schema, dict) and schema.get("deprecated") is True)
            }
        else:
            result[key] = without_deprecated_properties(item)
    return result


def deprecated_property_names(value: dict[str, Any]) -> set[str]:
    properties = value.get("properties", {})
    if not isinstance(properties, dict):
        return set()
    return {
        name
        for name, schema in properties.items()
        if isinstance(schema, dict) and schema.get("deprecated") is True
    }


def constraint_requires_any(value: Any, property_names: set[str]) -> bool:
    if isinstance(value, list):
        return any(constraint_requires_any(item, property_names) for item in value)
    if not isinstance(value, dict):
        return False
    required = value.get("required")
    if isinstance(required, list) and any(name in property_names for name in required):
        return True
    return any(constraint_requires_any(item, property_names) for item in value.values())


def without_constraints_referencing_properties(value: Any, property_names: set[str]) -> Any:
    """Drop only combinator branches made stale by removed properties.

    Keeping unrelated allOf/anyOf/oneOf branches ensures a future canonical
    validation rule remains visible to MCP clients.
    """
    if isinstance(value, list):
        return [without_constraints_referencing_properties(item, property_names) for item in value]
    if not isinstance(value, dict):
        return value

    result: dict[str, Any] = {}
    for key, item in value.items():
        if key in {"allOf", "anyOf", "oneOf"} and isinstance(item, list):
            retained = [
                without_constraints_referencing_properties(branch, property_names)
                for branch in item
                if not constraint_requires_any(branch, property_names)
            ]
            if retained:
                result[key] = retained
        else:
            result[key] = without_constraints_referencing_properties(item, property_names)
    return result


def tool_input_schema(component: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    compact = strip_annotations(component)
    properties = compact.get("properties", {})
    renamed_properties = {lower_camel(name): schema for name, schema in properties.items()}
    renamed_properties = {"tableName": {"type": "string"}, **renamed_properties}

    for alias, source in aliases.items():
        source_name = lower_camel(source)
        alias_schema = copy.deepcopy(renamed_properties[source_name])
        alias_schema["deprecated"] = True
        alias_schema["description"] = f"Compatibility alias for {source_name}."
        renamed_properties[alias] = alias_schema

    required = ["tableName", *(lower_camel(name) for name in compact.get("required", []))]
    result: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": renamed_properties,
    }
    if aliases:
        result["allOf"] = [
            {"not": {"required": [lower_camel(source), alias]}}
            for alias, source in aliases.items()
        ]
    return result


def generated_content(fragment: Fragment, schemas: dict[str, Any]) -> str:
    schema = resolve_local_refs(schemas[fragment.component], schemas)
    schema = strip_vendor_extensions(schema)
    if fragment.exclude_deprecated:
        removed_properties = deprecated_property_names(schema)
        schema = without_deprecated_properties(schema)
        schema = without_constraints_referencing_properties(schema, removed_properties)
        if constraint_requires_any(schema, removed_properties):
            raise ValueError(
                f"MCP schema {fragment.component} still constrains removed properties: "
                f"{sorted(removed_properties)}"
            )
    if fragment.tool_input:
        schema = tool_input_schema(schema, fragment.aliases)
    return json.dumps(schema, separators=(",", ":"), ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    with SPEC.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    schemas = document["components"]["schemas"]

    stale: list[Path] = []
    for fragment in FRAGMENTS:
        output = OUTPUT_DIR / fragment.output
        expected = generated_content(fragment, schemas)
        if args.check:
            actual = output.read_text(encoding="utf-8") if output.exists() else ""
            if actual != expected:
                stale.append(output.relative_to(ROOT))
            continue
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(expected, encoding="utf-8")

    if stale:
        parser.error(
            "stale MCP schemas: "
            + ", ".join(str(path) for path in stale)
            + "; run this script without --check"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
