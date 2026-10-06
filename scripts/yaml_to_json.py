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

import argparse
import copy
import json
import sys
from pathlib import Path

import yaml


class OpenApiLoader(yaml.SafeLoader):
    yaml_implicit_resolvers = {
        key: list(resolvers)
        for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


for key, resolvers in list(OpenApiLoader.yaml_implicit_resolvers.items()):
    OpenApiLoader.yaml_implicit_resolvers[key] = [
        (tag, pattern)
        for tag, pattern in resolvers
        if tag != "tag:yaml.org,2002:timestamp"
    ]


def normalize(value: object) -> None:
    if isinstance(value, dict):
        for key, child in list(value.items()):
            if str(key) == "description" and isinstance(child, str):
                value[key] = " ".join(child.split())
            else:
                normalize(child)
        return
    if isinstance(value, list):
        for child in value:
            normalize(child)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Convert OpenAPI YAML to JSON")
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--schema-alias",
        action="append",
        default=[],
        metavar="NAME=/JSON/POINTER",
        help="Expose an existing inline schema as a named component for type generation",
    )
    args = parser.parse_args(argv)
    input_path = args.input
    output_path = args.output
    with input_path.open("r", encoding="utf-8") as fh:
        data = yaml.load(fh, Loader=OpenApiLoader)
    for alias in args.schema_alias:
        name, separator, pointer = alias.partition("=")
        if not name or not separator or not pointer.startswith("/"):
            parser.error(f"invalid schema alias: {alias}")
        schema = data
        try:
            for token in pointer[1:].split("/"):
                key = token.replace("~1", "/").replace("~0", "~")
                schema = schema[int(key)] if isinstance(schema, list) else schema[key]
        except (KeyError, IndexError, TypeError, ValueError):
            parser.error(f"schema alias target not found: {alias}")
        if not isinstance(schema, dict):
            parser.error(f"schema alias target is not an object: {alias}")
        schemas = data.setdefault("components", {}).setdefault("schemas", {})
        if name in schemas:
            parser.error(f"schema alias would replace an existing component: {name}")
        schemas[name] = copy.deepcopy(schema)
    normalize(data)
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
