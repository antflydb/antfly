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

from __future__ import annotations

import copy
import sys
from pathlib import Path

import yaml
from openapi_spec_validator import validate_spec


ROOT = Path(__file__).resolve().parent.parent
ANTFLY_SPEC = ROOT / "specs/openapi/antfly/public.yaml"
TERMITE_SPEC = ROOT / "specs/openapi/termite/api.yaml"
OUTPUT = ROOT / "specs/openapi/public.yaml"


class Dumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)

    def ignore_aliases(self, data):
        return True


def represent_str(dumper: yaml.Dumper, value: str):
    if "\n" in value:
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=">")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


Dumper.add_representer(str, represent_str)


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise RuntimeError(f"expected mapping at {path}")
    return data


def prefixed_path(prefix: str, path: str) -> str:
    return f"{prefix.rstrip('/')}/{path.lstrip('/')}"


def antfly_public_path(path: str) -> str:
    if path.startswith("/auth/v1/"):
        return path
    return prefixed_path("/api/v1", path)


def walk_refs(value: object, rename_schema) -> object:
    if isinstance(value, dict):
        out = {}
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str):
                prefix = "#/components/schemas/"
                if child.startswith(prefix):
                    out[key] = prefix + rename_schema(child[len(prefix) :])
                else:
                    out[key] = child
                continue
            if key == "mapping" and isinstance(child, dict):
                out[key] = {
                    mapping_key: (
                        "#/components/schemas/" + rename_schema(mapping_value.rsplit("/", 1)[-1])
                        if isinstance(mapping_value, str)
                        and mapping_value.startswith("#/components/schemas/")
                        else walk_refs(mapping_value, rename_schema)
                    )
                    for mapping_key, mapping_value in child.items()
                }
                continue
            out[key] = walk_refs(child, rename_schema)
        return out
    if isinstance(value, list):
        return [walk_refs(child, rename_schema) for child in value]
    return value


def termite_schema_name(name: str) -> str:
    return f"Termite{name}"


def merge_components(antfly: dict, termite: dict) -> dict:
    merged = copy.deepcopy(antfly.get("components", {}))
    termite_components = copy.deepcopy(termite.get("components", {}))

    termite_schemas = termite_components.pop("schemas", {})
    schemas = merged.setdefault("schemas", {})
    for name, schema in termite_schemas.items():
        schemas[termite_schema_name(name)] = walk_refs(schema, termite_schema_name)

    for section, section_map in termite_components.items():
        if not isinstance(section_map, dict):
            if section not in merged:
                merged[section] = copy.deepcopy(section_map)
            elif merged[section] != section_map:
                raise RuntimeError(f"components/{section} conflict")
            continue
        target = merged.setdefault(section, {})
        for name, value in section_map.items():
            if name in target and target[name] != value:
                raise RuntimeError(f"components/{section}/{name} conflict")
            target[name] = walk_refs(value, termite_schema_name)

    return merged


def join_specs() -> dict:
    antfly = load_yaml(ANTFLY_SPEC)
    termite = load_yaml(TERMITE_SPEC)

    paths = {}
    for path, item in antfly.get("paths", {}).items():
        paths[antfly_public_path(path)] = copy.deepcopy(item)
    for path, item in termite.get("paths", {}).items():
        paths[prefixed_path("/ml/v1", path)] = walk_refs(item, termite_schema_name)

    tags = []
    seen_tags = set()
    for item in antfly.get("tags", []) + termite.get("tags", []):
        name = item.get("name") if isinstance(item, dict) else None
        if not name or name in seen_tags:
            continue
        seen_tags.add(name)
        tags.append(copy.deepcopy(item))

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Antfly Public API",
            "version": antfly.get("info", {}).get("version", "0.1.0"),
            "description": (
                "Joined public contract for the Antfly server. Antfly APIs are served under "
                "`/api/v1`, auth APIs under `/auth/v1`, and Termite ML APIs under `/ml/v1`."
            ),
        },
        "servers": [{"url": "/"}],
        "tags": tags,
        "security": copy.deepcopy(antfly.get("security", [])),
        "paths": paths,
        "components": merge_components(antfly, termite),
        "x-tagGroups": [
            {
                "name": "Antfly",
                "tags": [
                    "cluster_management",
                    "table_management",
                    "data_operations",
                    "query_operations",
                    "index_management",
                ],
            },
            {
                "name": "Auth",
                "tags": ["User", "Permission", "ApiKey", "RowFilter"],
            },
            {
                "name": "Termite",
                "tags": [
                    tag.get("name")
                    for tag in termite.get("tags", [])
                    if isinstance(tag, dict) and tag.get("name")
                ],
            },
        ],
    }


def dump_yaml(data: dict, output: Path) -> None:
    with output.open("w", encoding="utf-8") as fh:
        yaml.dump(data, fh, Dumper=Dumper, sort_keys=False, allow_unicode=True, width=100)


def main(argv: list[str]) -> int:
    output = ROOT / argv[0] if argv else OUTPUT
    spec = join_specs()
    dump_yaml(spec, output)
    validate_spec(spec, base_uri=output.resolve().as_uri())
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
