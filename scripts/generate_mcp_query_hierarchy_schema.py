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

"""Generate the compact MCP hierarchy schema from the public OpenAPI contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "specs/openapi/antfly/metadata.yaml"
OUTPUT = ROOT / "zig/pkg/antfly/src/api/generated/mcp_query_hierarchy_schema.json"


def resolve_local_refs(value: Any, schemas: dict[str, Any]) -> Any:
    if isinstance(value, list):
        return [resolve_local_refs(item, schemas) for item in value]
    if not isinstance(value, dict):
        return value

    ref = value.get("$ref")
    if ref is not None:
        prefix = "#/components/schemas/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            raise ValueError(f"unsupported hierarchy schema reference: {ref!r}")
        resolved = resolve_local_refs(schemas[ref.removeprefix(prefix)], schemas)
        siblings = {key: item for key, item in value.items() if key != "$ref"}
        if siblings:
            resolved = {**resolved, **resolve_local_refs(siblings, schemas)}
        return resolved

    return {key: resolve_local_refs(item, schemas) for key, item in value.items()}


def generated_content() -> str:
    with SPEC.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    schemas = document["components"]["schemas"]
    hierarchy = resolve_local_refs(schemas["QueryHierarchy"], schemas)
    return json.dumps(hierarchy, separators=(",", ":"), ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = generated_content()

    if args.check:
        actual = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if actual != expected:
            parser.error(f"{OUTPUT.relative_to(ROOT)} is stale; run this script without --check")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(expected, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
