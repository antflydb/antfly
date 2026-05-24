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

import importlib.util
import copy
import subprocess
import sys
from pathlib import Path

import yaml
from openapi_spec_validator import validate_spec


ROOT = Path(__file__).resolve().parent.parent
SHARED_JOINER = ROOT / "scripts/openapi_joiner.py"
HTTP_METHODS = {
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "trace",
}


class RedoclyLikeDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)

    def ignore_aliases(self, data):
        return True


def represent_str(dumper: yaml.Dumper, value: str):
    if "\n" in value:
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=">")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


def represent_float(dumper: yaml.Dumper, value: float):
    if value.is_integer():
        return dumper.represent_scalar("tag:yaml.org,2002:int", str(int(value)))
    return dumper.represent_scalar("tag:yaml.org,2002:float", repr(value))


RedoclyLikeDumper.add_representer(str, represent_str)
RedoclyLikeDumper.add_representer(float, represent_float)


def dump_yaml(data: dict, output: Path) -> None:
    data = redocly_like_top_level_order(data)
    with output.open("w", encoding="utf-8") as fh:
        yaml.dump(
            data,
            fh,
            Dumper=RedoclyLikeDumper,
            sort_keys=False,
            allow_unicode=True,
            width=100,
        )


def redocly_like_top_level_order(data: dict) -> dict:
    ordered: dict = {}
    for key in (
        "openapi",
        "info",
        "servers",
        "tags",
        "paths",
        "components",
        "x-tagGroups",
    ):
        if key in data:
            ordered[key] = data[key]
    for key, value in data.items():
        if key not in ordered and key != "security":
            ordered[key] = value
    return ordered


def load_order_reference(output: Path) -> dict | None:
    try:
        rel_output = output.relative_to(ROOT)
    except ValueError:
        rel_output = output

    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{rel_output}"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        result = None

    if result is not None:
        loaded = yaml.safe_load(result.stdout)
        return loaded if isinstance(loaded, dict) else None

    if output.exists():
        with output.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
        return loaded if isinstance(loaded, dict) else None

    return None


def order_like_reference(current: dict, reference: dict | None, map_path: tuple[str, ...]) -> None:
    if reference is None:
        return

    current_map: object = current
    reference_map: object = reference
    for key in map_path:
        if not isinstance(current_map, dict) or not isinstance(reference_map, dict):
            return
        current_map = current_map.get(key)
        reference_map = reference_map.get(key)

    if not isinstance(current_map, dict) or not isinstance(reference_map, dict):
        return

    ordered = {}
    for key in reference_map:
        if key in current_map:
            ordered[key] = current_map[key]
    for key, value in current_map.items():
        if key not in ordered:
            ordered[key] = value

    current_map.clear()
    current_map.update(ordered)


def order_openapi_like_reference(current: dict, output: Path) -> dict:
    reference = load_order_reference(output)
    for map_path in (
        ("paths",),
        ("components", "securitySchemes"),
        ("components", "schemas"),
        ("components", "responses"),
        ("components", "parameters"),
    ):
        order_like_reference(current, reference, map_path)
    return current


def push_root_security_to_operations(spec: dict) -> dict:
    root_security = spec.get("security")
    if not root_security:
        return spec

    for path_item in (spec.get("paths") or {}).values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            operation.setdefault("security", copy.deepcopy(root_security))
    return spec


def add_redocly_tag_groups(spec: dict) -> dict:
    spec["x-tagGroups"] = [
        {
            "name": "Antfly Public API",
            "tags": [
                "getting_started",
                "cluster_management",
                "table_management",
                "data_operations",
                "query_operations",
                "index_management",
            ],
        },
        {
            "name": "User Management API",
            "tags": [
                "User",
                "Permission",
                "ApiKey",
                "RowFilter",
            ],
        },
    ]
    return spec


def validate_openapi_spec(spec: dict, source: Path) -> None:
    validate_spec(spec, base_uri=source.resolve().as_uri())
    print(f"validated {source}")


def load_shared_joiner():
    spec = importlib.util.spec_from_file_location("antfly_openapi_joiner", SHARED_JOINER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load shared OpenAPI joiner at {SHARED_JOINER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configure_for_repo_contracts(module) -> None:
    module.ROOT = ROOT
    module.METADATA_SPEC = ROOT / "specs/openapi/antfly/metadata.yaml"
    module.USERMGR_SPEC = ROOT / "specs/openapi/antfly/usermgr.yaml"
    module.ROOT_SPEC = ROOT / "openapi.yaml"
    module.GO_SCHEMA_SPEC = ROOT / "go/pkg/antfly/lib/schema/openapi.yaml"
    module.GO_INDEX_SPEC = ROOT / "go/pkg/antfly/src/store/db/indexes/openapi.yaml"
    module.GO_INDEX_REF_PATHS = {
        "../store/db/indexes/openapi.yaml",
        "../../../go/pkg/antfly/src/store/db/indexes/openapi.yaml",
    }
    module.PATH_REWRITES = {
        "usermgr.yaml": "specs/openapi/antfly/usermgr.yaml",
        "query.yaml": "specs/openapi/antfly/query.yaml",
        "../../../src/": "go/pkg/antfly/src/",
        "../../../lib/": "go/pkg/antfly/lib/",
        "../../../": "",
        "../../": "",
        "../metadata/": "go/pkg/antfly/src/metadata",
        "../store/": "go/pkg/antfly/src/store",
        "../usermgr/": "go/pkg/antfly/src/usermgr",
    }

    def target_schema_name(source_path: Path, schema_name: str) -> str:
        if source_path.resolve() == module.GO_SCHEMA_SPEC and schema_name == "AntflyType":
            return "AntflyType-2"
        return schema_name

    def target_schema_name_for_ref(ref_path: str, schema_name: str) -> str:
        rewritten = module.rewrite_ref_path(ref_path)
        if (module.ROOT / rewritten).resolve() == module.GO_SCHEMA_SPEC and schema_name == "AntflyType":
            return "AntflyType-2"
        return schema_name

    module.target_schema_name = target_schema_name
    module.target_schema_name_for_ref = target_schema_name_for_ref


def main(argv: list[str]) -> int:
    joiner = load_shared_joiner()
    configure_for_repo_contracts(joiner)

    metadata = joiner.load_yaml(joiner.METADATA_SPEC)
    usermgr = joiner.load_yaml(joiner.USERMGR_SPEC)
    push_root_security_to_operations(metadata)
    push_root_security_to_operations(usermgr)

    if argv and argv[0] == "--joined-only":
        joined_modular = joiner.rewrite_external_refs(joiner.join_specs(metadata, usermgr))
        add_redocly_tag_groups(joined_modular)
        output = ROOT / (argv[1] if len(argv) > 1 else "openapi.joined.yaml")
        dump_yaml(joined_modular, output)
        print(f"wrote {output}")
        return 0

    if argv and argv[0] == "--compare":
        target = argv[1] if len(argv) > 1 else "openapi.yaml"
        current = joiner.load_yaml(ROOT / target)
        joined = joiner.bundle_joined_spec(joiner.join_specs(metadata, usermgr), current)
        joined.pop("security", None)
        add_redocly_tag_groups(joined)
        has_drift = joiner.compare_specs(joined, current)
        return 1 if has_drift else 0

    if argv and argv[0] == "--validate":
        target = ROOT / (argv[1] if len(argv) > 1 else "openapi.yaml")
        validate_openapi_spec(joiner.load_yaml(target), target)
        return 0

    joined = joiner.bundle_joined_spec(joiner.join_specs(metadata, usermgr))
    add_redocly_tag_groups(joined)
    output = ROOT / (argv[0] if argv else "openapi.joined.yaml")
    order_openapi_like_reference(joined, output)
    dump_yaml(joined, output)
    print(f"wrote {output}")
    validate_openapi_spec(joined, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
