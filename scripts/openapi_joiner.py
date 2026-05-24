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
import json
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parent.parent
METADATA_SPEC = ROOT / "specs/openapi/antfly/metadata.yaml"
USERMGR_SPEC = ROOT / "specs/openapi/antfly/usermgr.yaml"
ROOT_SPEC = ROOT / "openapi.yaml"
GO_SCHEMA_SPEC = (ROOT / "go/pkg/antfly/lib/schema/openapi.yaml").resolve()
GO_INDEX_SPEC = (ROOT / "go/pkg/antfly/src/store/db/indexes/openapi.yaml").resolve()
GO_INDEX_REF_PATHS = {
    "../../antfly/src/store/db/indexes/openapi.yaml",
    "../antfly/src/store/db/indexes/openapi.yaml",
    "../../../go/pkg/antfly/src/store/db/indexes/openapi.yaml",
}
PATH_REWRITES = {
    "usermgr.yaml": "specs/openapi/antfly/usermgr.yaml",
    "query.yaml": "specs/openapi/antfly/query.yaml",
    "../../../src/": "go/pkg/antfly/src/",
    "../../../lib/": "go/pkg/antfly/lib/",
    "../../../": "",
    "../../antfly/": "",
    "../antfly/": "",
    "../metadata/": "go/pkg/antfly/src/metadata/",
    "../usermgr/": "go/pkg/antfly/src/usermgr/",
}


def dedupe_array(items: list[object]) -> list[object]:
    seen: set[str] = set()
    out: list[object] = []
    for item in items:
        key = json.dumps(item, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def merge_named_arrays(left: list[dict], right: list[dict], label: str) -> list[dict]:
    by_name: dict[str, dict] = {}
    out: list[dict] = []
    for item in left + right:
        name = item.get("name")
        if not name:
            raise RuntimeError(f"{label} entry missing name: {item!r}")
        if name in by_name:
            existing = by_name[name]
            if existing != item:
                raise RuntimeError(f"{label} conflict for {name}")
            continue
        by_name[name] = item
        out.append(item)
    return out


def is_ref_to_section_key(value: object, section_label: str, key: str) -> bool:
    if not isinstance(value, dict):
        return False
    ref = value.get("$ref")
    if not isinstance(ref, str):
        return False
    section_name = section_label.split("/")[-1]
    suffix = f"/components/{section_name}/{key}"
    exact = f"#/components/{section_name}/{key}"
    return ref == exact or ref.endswith(suffix)


def merge_string_key_maps(left: dict, right: dict, label: str) -> dict:
    out = copy.deepcopy(left)
    for key, value in right.items():
        if key in out:
            if out[key] == value:
                continue
            if is_ref_to_section_key(out[key], label, key):
                out[key] = copy.deepcopy(value)
                continue
            if is_ref_to_section_key(value, label, key):
                continue
            raise RuntimeError(f"{label} conflict for {key}")
        out[key] = copy.deepcopy(value)
    return out


def join_specs(metadata_spec: dict, usermgr_spec: dict) -> dict:
    joined = copy.deepcopy(metadata_spec)
    joined["tags"] = merge_named_arrays(
        metadata_spec.get("tags", []),
        usermgr_spec.get("tags", []),
        "tag",
    )
    joined["security"] = dedupe_array(
        metadata_spec.get("security", []) + usermgr_spec.get("security", [])
    )
    joined["paths"] = merge_string_key_maps(
        metadata_spec.get("paths", {}),
        usermgr_spec.get("paths", {}),
        "path",
    )

    joined_components = copy.deepcopy(metadata_spec.get("components", {}))
    usermgr_components = usermgr_spec.get("components", {})
    for section in sorted(set(joined_components) | set(usermgr_components)):
        left = joined_components.get(section, {})
        right = usermgr_components.get(section, {})
        if isinstance(left, dict) and isinstance(right, dict):
            joined_components[section] = merge_string_key_maps(
                left,
                right,
                f"components/{section}",
            )
        elif left == {}:
            joined_components[section] = copy.deepcopy(right)
        elif right == {}:
            joined_components[section] = copy.deepcopy(left)
        else:
            raise RuntimeError(f"components/{section} conflict")
    joined["components"] = joined_components
    return joined


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise RuntimeError(f"expected mapping at {path}")
    return data


def resolve_pointer(doc: object, pointer: str) -> object:
    value = doc
    for raw_part in pointer.lstrip("/").split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(value, list):
            value = value[int(part)]
            continue
        if isinstance(value, dict):
            value = value[part]
            continue
        raise RuntimeError(f"cannot resolve pointer {pointer}")
    return value


def split_ref(ref: str) -> tuple[str, str]:
    if "#" not in ref:
        raise RuntimeError(f"expected json-pointer ref, got {ref}")
    path_part, pointer = ref.split("#", 1)
    return path_part, f"#{pointer}"


def resolve_ref_path(ref_path: str, source_path: Path | None) -> Path:
    candidates: list[Path] = []
    if source_path is not None:
        candidates.append((source_path.parent / ref_path).resolve())
    candidates.append((ROOT / ref_path).resolve())
    for old_prefix, new_prefix in PATH_REWRITES.items():
        if ref_path.startswith(old_prefix):
            candidates.append((ROOT / ref_path.replace(old_prefix, new_prefix, 1)).resolve())
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if ref_path in GO_INDEX_REF_PATHS:
        return GO_INDEX_SPEC
    raise RuntimeError(f"unable to resolve ref path {ref_path!r} from {source_path}")


def rewrite_ref_path(ref_path: str) -> str:
    for old_prefix, new_prefix in PATH_REWRITES.items():
        if ref_path.startswith(old_prefix):
            return ref_path.replace(old_prefix, new_prefix, 1)
    return ref_path


def load_cached_yaml(cache: dict[Path, dict], path: Path) -> dict:
    doc = cache.get(path)
    if doc is None:
        if path == GO_INDEX_SPEC and not path.exists():
            # CI checks out the antfly monorepo. The bundled root spec carries the
            # legacy Go index schemas that metadata/openapi.yaml still refs.
            doc = load_yaml(ROOT_SPEC)
        else:
            doc = load_yaml(path)
        cache[path] = doc
    return doc


def target_schema_name(source_path: Path, schema_name: str) -> str:
    if source_path.resolve() == GO_SCHEMA_SPEC and schema_name == "AntflyType":
        return "schemas-AntflyType"
    return schema_name


def target_schema_name_for_ref(ref_path: str, schema_name: str) -> str:
    rewritten = rewrite_ref_path(ref_path)
    if (ROOT / rewritten).resolve() == GO_SCHEMA_SPEC and schema_name == "AntflyType":
        return "schemas-AntflyType"
    return schema_name


def ensure_root_schema_from_fallback(
    root_spec: dict,
    root_schema_name: str,
    cache: dict[Path, dict],
    in_progress: set[tuple[Path, str]],
    fallback_schemas: dict | None,
) -> bool:
    components = root_spec.setdefault("components", {})
    root_schemas = components.setdefault("schemas", {})
    existing = root_schemas.get(root_schema_name)
    if existing is not None and not (
        isinstance(existing, dict) and isinstance(existing.get("$ref"), str)
    ):
        return True
    if fallback_schemas is None or root_schema_name not in fallback_schemas:
        return False

    schema_value = copy.deepcopy(fallback_schemas[root_schema_name])
    root_schemas[root_schema_name] = bundle_refs(
        schema_value,
        root_spec,
        ROOT_SPEC.resolve(),
        cache,
        in_progress,
        fallback_schemas,
    )
    return True


def ensure_root_schema(
    root_spec: dict,
    schema_name: str,
    source_path: Path,
    cache: dict[Path, dict],
    in_progress: set[tuple[Path, str]],
    fallback_schemas: dict | None = None,
) -> None:
    components = root_spec.setdefault("components", {})
    root_schemas = components.setdefault("schemas", {})
    resolved_source_path = source_path.resolve()
    root_schema_name = target_schema_name(resolved_source_path, schema_name)
    existing = root_schemas.get(root_schema_name)
    if existing is not None and not (
        isinstance(existing, dict) and isinstance(existing.get("$ref"), str)
    ):
        return

    key = (resolved_source_path, schema_name)
    if key in in_progress:
        return

    source_doc = load_cached_yaml(cache, resolved_source_path)
    source_components = source_doc.get("components", {})
    source_schemas = source_components.get("schemas", {})
    if schema_name not in source_schemas:
        raise RuntimeError(f"schema {schema_name} not found in {resolved_source_path}")

    in_progress.add(key)
    try:
        schema_value = copy.deepcopy(source_schemas[schema_name])
        bundled = bundle_refs(schema_value, root_spec, resolved_source_path, cache, in_progress, fallback_schemas)
        root_schemas[root_schema_name] = bundled
    finally:
        in_progress.remove(key)


def bundle_ref(
    ref: str,
    root_spec: dict,
    source_path: Path | None,
    cache: dict[Path, dict],
    in_progress: set[tuple[Path, str]],
    fallback_schemas: dict | None = None,
) -> str:
    if ref.startswith("#"):
        _, pointer = split_ref(f"{source_path or ROOT_SPEC}{ref}")
        if pointer.startswith("#/components/schemas/") and source_path is not None and source_path != ROOT_SPEC:
            schema_name = pointer.rsplit("/", 1)[-1]
            ensure_root_schema(root_spec, schema_name, source_path, cache, in_progress, fallback_schemas)
            return f"#/components/schemas/{target_schema_name(source_path, schema_name)}"
        return ref

    ref_path, pointer = split_ref(ref)
    if not pointer.startswith("#/components/schemas/"):
        raise RuntimeError(f"unsupported external ref target {ref}")
    schema_name = pointer.rsplit("/", 1)[-1]
    try:
        resolved_path = resolve_ref_path(ref_path, source_path)
    except RuntimeError:
        root_schema_name = target_schema_name_for_ref(ref_path, schema_name)
        if ensure_root_schema_from_fallback(root_spec, root_schema_name, cache, in_progress, fallback_schemas):
            return f"#/components/schemas/{root_schema_name}"
        raise
    ensure_root_schema(root_spec, schema_name, resolved_path, cache, in_progress, fallback_schemas)
    return f"#/components/schemas/{target_schema_name(resolved_path, schema_name)}"


def bundle_refs(
    value: object,
    root_spec: dict,
    source_path: Path | None,
    cache: dict[Path, dict],
    in_progress: set[tuple[Path, str]],
    fallback_schemas: dict | None = None,
) -> object:
    if isinstance(value, dict):
        if "$ref" in value and isinstance(value["$ref"], str):
            value["$ref"] = bundle_ref(value["$ref"], root_spec, source_path, cache, in_progress, fallback_schemas)
            return value
        for key, child in list(value.items()):
            if key == "mapping" and isinstance(child, dict):
                for mapping_key, mapping_value in list(child.items()):
                    if isinstance(mapping_value, str) and "#" in mapping_value:
                        child[mapping_key] = bundle_ref(mapping_value, root_spec, source_path, cache, in_progress, fallback_schemas)
            bundled_child = bundle_refs(child, root_spec, source_path, cache, in_progress, fallback_schemas)
            if (
                source_path == ROOT_SPEC.resolve()
                and key in (root_spec.get("components", {}).get("schemas", {}) or {})
                and is_ref_to_section_key(bundled_child, "components/schemas", key)
            ):
                value[key] = copy.deepcopy(root_spec["components"]["schemas"][key])
            else:
                value[key] = bundled_child
        return value
    if isinstance(value, list):
        for i, child in enumerate(value):
            value[i] = bundle_refs(child, root_spec, source_path, cache, in_progress, fallback_schemas)
        return value
    return value


def bundle_joined_spec(joined: dict, fallback_root: dict | None = None) -> dict:
    bundled = copy.deepcopy(joined)
    cache = {
        METADATA_SPEC.resolve(): load_yaml(METADATA_SPEC),
        USERMGR_SPEC.resolve(): load_yaml(USERMGR_SPEC),
        ROOT_SPEC.resolve(): bundled,
    }
    fallback_schemas = None
    if fallback_root is not None:
        fallback_schemas = fallback_root.get("components", {}).get("schemas") or {}
    bundle_refs(bundled, bundled, ROOT_SPEC.resolve(), cache, set(), fallback_schemas)
    return bundled


def rewrite_external_refs(value: object) -> object:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and not ref.startswith("#"):
            ref_path, pointer = split_ref(ref)
            value["$ref"] = f"{rewrite_ref_path(ref_path)}{pointer}"
        for key, child in list(value.items()):
            if key == "mapping" and isinstance(child, dict):
                for mapping_key, mapping_value in list(child.items()):
                    if isinstance(mapping_value, str) and "#" in mapping_value and not mapping_value.startswith("#"):
                        ref_path, pointer = split_ref(mapping_value)
                        child[mapping_key] = f"{rewrite_ref_path(ref_path)}{pointer}"
            rewrite_external_refs(child)
    elif isinstance(value, list):
        for child in value:
            rewrite_external_refs(child)
    return value


def compare_specs(joined: dict, current: dict) -> bool:
    joined_paths = list(joined.get("paths", {}).keys())
    current_paths = list(current.get("paths", {}).keys())
    joined_tags = [item["name"] for item in joined.get("tags", [])]
    current_tags = [item["name"] for item in current.get("tags", [])]
    joined_schemas = list((joined.get("components", {}).get("schemas") or {}).keys())
    current_schemas = list((current.get("components", {}).get("schemas") or {}).keys())
    joined_security = joined.get("security", [])
    current_security = current.get("security", [])
    only_in_joined_paths = sorted(set(joined_paths) - set(current_paths))
    only_in_current_paths = sorted(set(current_paths) - set(joined_paths))
    only_in_joined_tags = sorted(set(joined_tags) - set(current_tags))
    only_in_current_tags = sorted(set(current_tags) - set(joined_tags))
    only_in_joined_schemas = sorted(set(joined_schemas) - set(current_schemas))
    current_only_schema_sample = sorted(set(current_schemas) - set(joined_schemas))[:40]
    security_matches = joined_security == current_security
    actionable_drift = (
        bool(only_in_joined_paths)
        or bool(only_in_current_paths)
        or bool(only_in_joined_tags)
        or bool(only_in_current_tags)
        or not security_matches
        or bool(only_in_joined_schemas)
    )

    print(f"joined paths: {len(joined_paths)}")
    print(f"current paths: {len(current_paths)}")
    print(f"only in joined paths: {only_in_joined_paths!r}")
    print(f"only in current paths: {only_in_current_paths!r}")
    print(f"only in joined tags: {only_in_joined_tags!r}")
    print(f"only in current tags: {only_in_current_tags!r}")
    print(f"joined security: {joined_security!r}")
    print(f"current security: {current_security!r}")
    print(f"joined schemas: {len(joined_schemas)}")
    print(f"current schemas: {len(current_schemas)}")
    print(f"only in joined schemas: {only_in_joined_schemas!r}")
    print(f"only in current schemas sample: {current_only_schema_sample!r}")
    if actionable_drift:
        print("compare result: actionable drift detected")
    else:
        print("compare result: no actionable drift; remaining differences are bundling-only")
    return actionable_drift


def main(argv: list[str]) -> int:
    metadata = load_yaml(METADATA_SPEC)
    usermgr = load_yaml(USERMGR_SPEC)
    joined_modular = rewrite_external_refs(join_specs(metadata, usermgr))

    if argv and argv[0] == "--joined-only":
        output = ROOT / (argv[1] if len(argv) > 1 else "openapi.joined.yaml")
        with output.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(joined_modular, fh, sort_keys=False, allow_unicode=True)
        print(f"wrote {output}")
        return 0

    if argv and argv[0] == "--compare":
        target = argv[1] if len(argv) > 1 else "openapi.yaml"
        current = load_yaml(ROOT / target)
        joined = bundle_joined_spec(join_specs(metadata, usermgr), current)
        has_drift = compare_specs(joined, current)
        return 1 if has_drift else 0

    joined = bundle_joined_spec(join_specs(metadata, usermgr))
    output = ROOT / (argv[0] if argv else "openapi.joined.yaml")
    with output.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(joined, fh, sort_keys=False, allow_unicode=True)
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
