#!/usr/bin/env python3
"""Three-way audit for whether a merge captured incoming main changes.

The audit classifies files changed by the incoming ref since the selected base
and verifies files untouched by our side landed byte-for-byte, moved/split into
documented paths, or were explicitly documented as intentional integrations.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import pathlib
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "scripts/merge_audit/policy.json"

MOVED_PATHS: dict[str, list[str]] = {}
EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS: dict[str, str] = {}
EXPECTED_OURS_ONLY_RESOLUTIONS: dict[str, str] = {}
EXPECTED_MAIN_ONLY_RESOLUTIONS: dict[str, str] = {}

MANUAL_REVIEW_REQUIRED_PATHS: dict[str, str] = {}
MANUAL_REVIEW_ACKNOWLEDGEMENTS: dict[str, str] = {}
GENERATED_FILE_PROVENANCE: dict[str, list[str]] = {}
GENERATED_PROVENANCE_ALLOWLIST: dict[str, str] = {}
DECISION_SCOPE: dict[str, str] = {}

BUILD_SURFACE_CATEGORIES = {
    "options",
    "steps",
    "root_sources",
    "artifacts",
    "module_imports",
    "module_options",
    "step_dependencies",
    "system_libraries",
    "frameworks",
    "include_paths",
    "system_include_paths",
    "library_paths",
    "framework_paths",
    "native_sources",
}


def reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_json_file(path: pathlib.Path) -> Any:
    return json.loads(
        path.read_text(),
        object_pairs_hook=reject_duplicate_json_keys,
    )
SEMANTIC_BUILD_SURFACE_CATEGORIES = BUILD_SURFACE_CATEGORIES - {
    "options",
    "steps",
    "root_sources",
    "artifacts",
}

MANIFEST_SCHEMA: dict[str, str] = {
    "decision_scope": "map_string",
    "required_split_migrations": "list_nonempty_string",
    "split_migrations": "split_migrations",
    "moved_paths": "map_list",
    "expected_incoming_only_integration_diffs": "map_string",
    "expected_ours_only_resolutions": "map_string",
    "expected_main_only_resolutions": "map_string",
    "expected_generated_without_spec_changes": "map_string",
    "manual_review_required_paths": "map_string",
    "manual_review_acknowledgements": "map_string",
    "generated_file_provenance": "map_list",
    "same_path_const_false_positives": "map_list",
    "same_path_const_aliases": "nested_map_list",
    "test_name_aliases": "map_list",
    "declaration_name_aliases": "map_list",
    "changed_helper_aliases": "map_list",
    "generated_workflows": "list_string",
    "zig_relative_import_roots": "list_string",
    "zig_relative_import_exceptions": "relative_import_exceptions",
}


def ensure_manifest_shape(data: object, path: pathlib.Path) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError(f"{path}: manifest root must be a JSON object")

    errors: list[str] = []
    for key in sorted(data):
        if key not in MANIFEST_SCHEMA:
            errors.append(f"unknown key {key!r}")
            continue
        expected = MANIFEST_SCHEMA[key]
        value = data[key]
        if expected == "map_string":
            if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
                errors.append(f"{key}: expected object of string -> string")
        elif expected == "map_list":
            if not isinstance(value, dict) or not all(
                isinstance(k, str) and isinstance(v, list) and all(isinstance(item, str) for item in v)
                for k, v in value.items()
            ):
                errors.append(f"{key}: expected object of string -> list[string]")
        elif expected == "nested_map_list":
            if not isinstance(value, dict) or not all(
                isinstance(k, str)
                and isinstance(nested, dict)
                and all(
                    isinstance(nested_key, str)
                    and isinstance(nested_value, list)
                    and all(isinstance(item, str) for item in nested_value)
                    for nested_key, nested_value in nested.items()
                )
                for k, nested in value.items()
            ):
                errors.append(f"{key}: expected object of string -> object of string -> list[string]")
        elif expected == "list_string":
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                errors.append(f"{key}: expected list[string]")
        elif expected == "list_nonempty_string":
            if not isinstance(value, list) or not all(
                isinstance(item, str) and item for item in value
            ):
                errors.append(f"{key}: expected list[non-empty string]")
        elif expected == "relative_import_exceptions":
            if not isinstance(value, list):
                errors.append(f"{key}: expected list")
            else:
                for index, rule in enumerate(value):
                    if (
                        not isinstance(rule, dict)
                        or set(rule) != {"path", "import", "reason"}
                        or any(
                            not isinstance(rule.get(field), str)
                            or not rule[field]
                            for field in ("path", "import", "reason")
                        )
                    ):
                        errors.append(
                            f"{key}[{index}]: expected exactly non-empty "
                            "path, import, and reason strings"
                        )
        elif expected == "split_migrations":
            if not isinstance(value, dict):
                errors.append(f"{key}: expected object")
                continue
            for migration_name, migration in value.items():
                prefix = f"{key}.{migration_name}"
                if not isinstance(migration_name, str) or not migration_name:
                    errors.append(f"{key}: migration names must be non-empty strings")
                    continue
                if not isinstance(migration, dict):
                    errors.append(f"{prefix}: expected object")
                    continue
                unknown = set(migration) - {
                    "source",
                    "source_facade_removed",
                    "destinations",
                    "build_surface_helpers",
                    "build_surface_dependency_helpers",
                    "build_surface_delta_only_categories",
                    "build_surface_aliases",
                    "build_surface_retention_aliases",
                    "build_companion_globs",
                    "build_surface_conditional_steps",
                    "build_surface_omissions",
                    "container_field_migrations",
                    "declaration_mixins",
                    "declaration_companion_globs",
                    "declaration_companion_policies",
                    "declaration_placement_ranges",
                    "declaration_placements",
                    "declaration_owner_migrations",
                    "intentional_declaration_deletions",
                    "module_path_migrations",
                    "relative_import_exceptions",
                    "retained_deletions",
                    "reviewed_compositions",
                    "reviewed_resolutions",
                    "symbol_call_migrations",
                    "symbol_reference_migrations",
                    "test_name_rewrites",
                }
                if unknown:
                    errors.append(
                        f"{prefix}: unknown keys {', '.join(sorted(unknown))}"
                    )
                container_field_migrations = migration.get(
                    "container_field_migrations",
                    {},
                )
                if not isinstance(container_field_migrations, dict):
                    errors.append(
                        f"{prefix}.container_field_migrations: expected object"
                    )
                else:
                    for container_key, fields in container_field_migrations.items():
                        fields_prefix = (
                            f"{prefix}.container_field_migrations.{container_key}"
                        )
                        if (
                            not isinstance(container_key, str)
                            or not container_key.startswith("container:")
                            or not isinstance(fields, dict)
                            or not fields
                        ):
                            errors.append(
                                f"{fields_prefix}: expected a container obligation "
                                "mapped to a non-empty object"
                            )
                            continue
                        for source_field, field_migration in fields.items():
                            field_prefix = f"{fields_prefix}.{source_field}"
                            if (
                                not isinstance(source_field, str)
                                or not source_field
                                or not isinstance(field_migration, dict)
                                or set(field_migration) != {"target", "reason"}
                                or not isinstance(field_migration.get("target"), str)
                                or not field_migration["target"]
                                or not isinstance(field_migration.get("reason"), str)
                                or not field_migration["reason"]
                            ):
                                errors.append(
                                    f"{field_prefix}: expected exact non-empty "
                                    "target and reason strings"
                                )
                module_path_migrations = migration.get(
                    "module_path_migrations",
                    {},
                )
                if (
                    not isinstance(module_path_migrations, dict)
                    or any(
                        not isinstance(source, str)
                        or not source
                        or not isinstance(destinations, list)
                        or not destinations
                        or any(
                            not isinstance(destination, str) or not destination
                            for destination in destinations
                        )
                        for source, destinations in module_path_migrations.items()
                    )
                ):
                    errors.append(
                        f"{prefix}.module_path_migrations: expected non-empty "
                        "source paths mapped to non-empty path lists"
                    )
                symbol_call_migrations = migration.get(
                    "symbol_call_migrations",
                    {},
                )
                if (
                    not isinstance(symbol_call_migrations, dict)
                    or any(
                        not isinstance(source, str)
                        or not source
                        or not isinstance(targets, list)
                        or not targets
                        or any(
                            not isinstance(target, str) or not target
                            for target in targets
                        )
                        for source, targets in symbol_call_migrations.items()
                    )
                ):
                    errors.append(
                        f"{prefix}.symbol_call_migrations: expected non-empty "
                        "source symbols mapped to non-empty symbol lists"
                    )
                symbol_reference_migrations = migration.get(
                    "symbol_reference_migrations",
                    {},
                )
                if (
                    not isinstance(symbol_reference_migrations, dict)
                    or any(
                        not isinstance(source, str)
                        or not source
                        or not isinstance(targets, list)
                        or not targets
                        or any(
                            not isinstance(target, str) or not target
                            for target in targets
                        )
                        for source, targets in symbol_reference_migrations.items()
                    )
                ):
                    errors.append(
                        f"{prefix}.symbol_reference_migrations: expected "
                        "non-empty source symbols mapped to non-empty symbol lists"
                    )
                if not isinstance(migration.get("source"), str) or not migration["source"]:
                    errors.append(f"{prefix}.source: expected non-empty string")
                if not isinstance(
                    migration.get("source_facade_removed", False),
                    bool,
                ):
                    errors.append(
                        f"{prefix}.source_facade_removed: expected boolean"
                    )
                destinations = migration.get("destinations")
                if (
                    not isinstance(destinations, list)
                    or not destinations
                    or not all(
                        isinstance(destination, str) and destination
                        for destination in destinations
                    )
                ):
                    errors.append(
                        f"{prefix}.destinations: expected non-empty list[string]"
                    )
                surface_helpers = migration.get("build_surface_helpers", {})
                if not isinstance(surface_helpers, dict):
                    errors.append(f"{prefix}.build_surface_helpers: expected object")
                else:
                    for category, helpers in surface_helpers.items():
                        helper_prefix = f"{prefix}.build_surface_helpers.{category}"
                        if category not in BUILD_SURFACE_CATEGORIES:
                            errors.append(f"{helper_prefix}: unknown surface category")
                            continue
                        if not isinstance(helpers, list):
                            errors.append(f"{helper_prefix}: expected list")
                            continue
                        for index, helper in enumerate(helpers):
                            item_prefix = f"{helper_prefix}[{index}]"
                            if (
                                not isinstance(helper, dict)
                                or set(helper) != {"function", "argument"}
                                or not isinstance(helper.get("function"), str)
                                or not helper["function"]
                                or not isinstance(helper.get("argument"), int)
                                or isinstance(helper.get("argument"), bool)
                                or helper["argument"] < 0
                            ):
                                errors.append(
                                    f"{item_prefix}: expected exactly a non-empty "
                                    "function string and non-negative argument integer"
                                )
                dependency_helpers = migration.get(
                    "build_surface_dependency_helpers", []
                )
                if not isinstance(dependency_helpers, list):
                    errors.append(
                        f"{prefix}.build_surface_dependency_helpers: expected list"
                    )
                else:
                    for index, helper in enumerate(dependency_helpers):
                        item_prefix = (
                            f"{prefix}.build_surface_dependency_helpers[{index}]"
                        )
                        keys = set(helper) if isinstance(helper, dict) else set()
                        if (
                            not isinstance(helper, dict)
                            or not {"function", "step_argument"} <= keys
                            or not keys
                            <= {
                                "function",
                                "step_argument",
                                "step_field",
                                "target_argument",
                            }
                            or not isinstance(helper.get("function"), str)
                            or not helper["function"]
                            or not isinstance(helper.get("step_argument"), int)
                            or isinstance(helper.get("step_argument"), bool)
                            or helper["step_argument"] < 0
                            or "step_field" in helper
                            and (
                                not isinstance(helper["step_field"], str)
                                or not helper["step_field"]
                            )
                            or "target_argument" in helper
                            and (
                                not isinstance(helper["target_argument"], int)
                                or isinstance(helper["target_argument"], bool)
                                or helper["target_argument"] < 0
                            )
                        ):
                            errors.append(
                                f"{item_prefix}: expected function and "
                                "non-negative step_argument, with optional "
                                "step_field or target_argument"
                            )
                delta_only_categories = migration.get(
                    "build_surface_delta_only_categories", []
                )
                if (
                    not isinstance(delta_only_categories, list)
                    or len(delta_only_categories) != len(set(delta_only_categories))
                    or any(
                        not isinstance(category, str)
                        or category not in SEMANTIC_BUILD_SURFACE_CATEGORIES
                        for category in delta_only_categories
                    )
                ):
                    errors.append(
                        f"{prefix}.build_surface_delta_only_categories: "
                        "expected unique known semantic category strings"
                    )
                companion_globs = migration.get("build_companion_globs", [])
                if (
                    not isinstance(companion_globs, list)
                    or any(
                        not isinstance(pattern, str) or not pattern
                        for pattern in companion_globs
                    )
                ):
                    errors.append(
                        f"{prefix}.build_companion_globs: expected list of "
                        "non-empty strings"
                    )
                declaration_companion_globs = migration.get(
                    "declaration_companion_globs",
                    [],
                )
                if (
                    not isinstance(declaration_companion_globs, list)
                    or any(
                        not isinstance(pattern, str) or not pattern
                        for pattern in declaration_companion_globs
                    )
                ):
                    errors.append(
                        f"{prefix}.declaration_companion_globs: expected list "
                        "of non-empty strings"
                    )
                companion_policies = migration.get(
                    "declaration_companion_policies",
                    {},
                )
                companion_policy_keys = {
                    "container_field_migrations",
                    "declaration_mixins",
                    "declaration_placement_ranges",
                    "declaration_placements",
                    "declaration_owner_migrations",
                    "destinations",
                    "intentional_declaration_deletions",
                    "retained_deletions",
                    "reviewed_compositions",
                    "reviewed_resolutions",
                    "symbol_call_migrations",
                    "symbol_reference_migrations",
                    "test_name_rewrites",
                }
                if not isinstance(companion_policies, dict):
                    errors.append(
                        f"{prefix}.declaration_companion_policies: expected object"
                    )
                else:
                    for companion_path, companion_policy in companion_policies.items():
                        companion_prefix = (
                            f"{prefix}.declaration_companion_policies."
                            f"{companion_path}"
                        )
                        if not isinstance(companion_path, str) or not companion_path:
                            errors.append(
                                f"{prefix}.declaration_companion_policies: "
                                "paths must be non-empty strings"
                            )
                            continue
                        if not isinstance(companion_policy, dict):
                            errors.append(f"{companion_prefix}: expected object")
                            continue
                        companion_unknown = set(companion_policy) - companion_policy_keys
                        if companion_unknown:
                            errors.append(
                                f"{companion_prefix}: unknown keys "
                                + ", ".join(sorted(companion_unknown))
                            )
                            continue
                        companion_destinations = companion_policy.get(
                            "destinations",
                            [companion_path],
                        )
                        if (
                            not isinstance(companion_destinations, list)
                            or not companion_destinations
                            or companion_path not in companion_destinations
                            or any(
                                not isinstance(destination, str) or not destination
                                for destination in companion_destinations
                            )
                        ):
                            errors.append(
                                f"{companion_prefix}.destinations: expected a "
                                "non-empty string list containing the companion path"
                            )
                            continue

                        # Reuse the complete declaration-policy schema so path
                        # scopes cannot bypass hash, ownership, or mixin checks.
                        synthetic_migration = dict(companion_policy)
                        synthetic_migration["source"] = companion_path
                        synthetic_migration["destinations"] = companion_destinations
                        try:
                            ensure_manifest_shape(
                                {
                                    "split_migrations": {
                                        "companion": synthetic_migration,
                                    },
                                },
                                path,
                            )
                        except ValueError as exc:
                            errors.append(f"{companion_prefix}: {exc}")
                for alias_key in (
                    "build_surface_aliases",
                    "build_surface_retention_aliases",
                ):
                    surface_aliases = migration.get(alias_key, {})
                    if not isinstance(surface_aliases, dict):
                        errors.append(f"{prefix}.{alias_key}: expected object")
                        continue
                    for category, aliases in surface_aliases.items():
                        alias_prefix = f"{prefix}.{alias_key}.{category}"
                        if (
                            category not in BUILD_SURFACE_CATEGORIES
                            or not isinstance(aliases, dict)
                        ):
                            errors.append(
                                f"{alias_prefix}: expected known category object"
                            )
                            continue
                        for source_name, targets in aliases.items():
                            valid_targets = (
                                isinstance(targets, str) and bool(targets)
                            ) or (
                                isinstance(targets, list)
                                and bool(targets)
                                and all(
                                    isinstance(target, str) and target
                                    for target in targets
                                )
                            )
                            if (
                                not isinstance(source_name, str)
                                or not source_name
                                or not valid_targets
                            ):
                                errors.append(
                                    f"{alias_prefix}: expected non-empty string "
                                    "aliases to string or non-empty list[string]"
                                )
                surface_omissions = migration.get("build_surface_omissions", {})
                if not isinstance(surface_omissions, dict):
                    errors.append(f"{prefix}.build_surface_omissions: expected object")
                else:
                    for category, omissions in surface_omissions.items():
                        omission_prefix = f"{prefix}.build_surface_omissions.{category}"
                        if (
                            category not in BUILD_SURFACE_CATEGORIES
                            or not isinstance(omissions, dict)
                            or not all(
                                isinstance(name, str)
                                and name
                                and isinstance(reason, str)
                                and reason
                                for name, reason in omissions.items()
                            )
                        ):
                            errors.append(
                                f"{omission_prefix}: expected known category "
                                "object of non-empty reasons"
                            )
                conditional_steps = migration.get(
                    "build_surface_conditional_steps",
                    {},
                )
                if (
                    not isinstance(conditional_steps, dict)
                    or not all(
                        isinstance(name, str)
                        and name
                        and isinstance(reason, str)
                        and reason
                        for name, reason in conditional_steps.items()
                    )
                ):
                    errors.append(
                        f"{prefix}.build_surface_conditional_steps: expected "
                        "non-empty step names mapped to non-empty reasons"
                    )
                placements = migration.get("declaration_placements", {})
                if not isinstance(placements, dict) or not all(
                    isinstance(obligation, str)
                    and obligation
                    and isinstance(destination, str)
                    and destination
                    for obligation, destination in placements.items()
                ):
                    errors.append(
                        f"{prefix}.declaration_placements: "
                        "expected object of non-empty string -> non-empty string"
                    )
                placement_ranges = migration.get(
                    "declaration_placement_ranges",
                    [],
                )
                if not isinstance(placement_ranges, list):
                    errors.append(
                        f"{prefix}.declaration_placement_ranges: expected list"
                    )
                else:
                    for index, rule in enumerate(placement_ranges):
                        rule_prefix = (
                            f"{prefix}.declaration_placement_ranges[{index}]"
                        )
                        if (
                            not isinstance(rule, dict)
                            or set(rule) != {"start", "end", "path", "reason"}
                            or any(
                                not isinstance(rule.get(field), str)
                                or not rule[field]
                                for field in ("start", "end", "path", "reason")
                            )
                        ):
                            errors.append(
                                f"{rule_prefix}: expected exactly non-empty "
                                "start, end, path, and reason strings"
                            )
                mixins = migration.get("declaration_mixins", [])
                if not isinstance(mixins, list):
                    errors.append(
                        f"{prefix}.declaration_mixins: expected list"
                    )
                else:
                    for index, rule in enumerate(mixins):
                        rule_prefix = (
                            f"{prefix}.declaration_mixins[{index}]"
                        )
                        if (
                            not isinstance(rule, dict)
                            or set(rule) != {"path", "factory", "owner"}
                            or any(
                                not isinstance(rule.get(field), str)
                                or not rule[field]
                                for field in ("path", "factory", "owner")
                            )
                        ):
                            errors.append(
                                f"{rule_prefix}: expected exactly non-empty "
                                "path, factory, and owner strings"
                            )
                test_name_rewrites = migration.get("test_name_rewrites", [])
                if not isinstance(test_name_rewrites, list):
                    errors.append(
                        f"{prefix}.test_name_rewrites: expected list"
                    )
                else:
                    for index, rule in enumerate(test_name_rewrites):
                        rule_prefix = (
                            f"{prefix}.test_name_rewrites[{index}]"
                        )
                        if (
                            not isinstance(rule, dict)
                            or set(rule)
                            != {
                                "path",
                                "source_prefix",
                                "destination_prefix",
                            }
                            or not isinstance(rule.get("path"), str)
                            or not rule["path"]
                            or not isinstance(rule.get("source_prefix"), str)
                            or not isinstance(
                                rule.get("destination_prefix"),
                                str,
                            )
                        ):
                            errors.append(
                                f"{rule_prefix}: expected exactly a non-empty "
                                "path and string source_prefix and "
                                "destination_prefix"
                            )
                relative_import_exceptions = migration.get(
                    "relative_import_exceptions",
                    [],
                )
                if not isinstance(relative_import_exceptions, list):
                    errors.append(
                        f"{prefix}.relative_import_exceptions: expected list"
                    )
                else:
                    for index, rule in enumerate(relative_import_exceptions):
                        rule_prefix = (
                            f"{prefix}.relative_import_exceptions[{index}]"
                        )
                        if (
                            not isinstance(rule, dict)
                            or set(rule) != {"path", "import", "reason"}
                            or any(
                                not isinstance(rule.get(field), str)
                                or not rule[field]
                                for field in ("path", "import", "reason")
                            )
                        ):
                            errors.append(
                                f"{rule_prefix}: expected exactly non-empty "
                                "path, import, and reason strings"
                            )
                owner_migrations = migration.get(
                    "declaration_owner_migrations",
                    {},
                )
                if not isinstance(owner_migrations, dict):
                    errors.append(
                        f"{prefix}.declaration_owner_migrations: expected object"
                    )
                    continue
                for obligation, target in owner_migrations.items():
                    target_prefix = (
                        f"{prefix}.declaration_owner_migrations.{obligation}"
                    )
                    if not isinstance(obligation, str) or not obligation:
                        errors.append(
                            f"{prefix}.declaration_owner_migrations: "
                            "obligation keys must be non-empty strings"
                        )
                        continue
                    if not isinstance(target, dict):
                        errors.append(f"{target_prefix}: expected object")
                        continue
                    target_unknown = set(target) - {
                        "owner",
                        "path",
                        "name",
                        "kind",
                    }
                    if target_unknown:
                        errors.append(
                            f"{target_prefix}: unknown keys "
                            + ", ".join(sorted(target_unknown))
                        )
                    if "owner" not in target or (
                        target["owner"] is not None
                        and (
                            not isinstance(target["owner"], str)
                            or not target["owner"]
                        )
                    ):
                        errors.append(
                            f"{target_prefix}.owner: "
                            "expected null or non-empty string"
                        )
                    if not isinstance(target.get("path"), str) or not target["path"]:
                        errors.append(
                            f"{target_prefix}.path: expected non-empty string"
                        )
                    if "name" in target and (
                        not isinstance(target["name"], str) or not target["name"]
                    ):
                        errors.append(
                            f"{target_prefix}.name: expected non-empty string"
                        )
                    if target.get("kind", "function") not in {
                        "binding",
                        "container",
                        "function",
                        "test",
                    }:
                        errors.append(
                            f"{target_prefix}.kind: expected binding, "
                            "container, function, or test"
                        )
                intentional_deletions = migration.get(
                    "intentional_declaration_deletions",
                    {},
                )
                if not isinstance(intentional_deletions, dict):
                    errors.append(
                        f"{prefix}.intentional_declaration_deletions: "
                        "expected object"
                    )
                else:
                    for obligation, deletion in intentional_deletions.items():
                        deletion_prefix = (
                            f"{prefix}.intentional_declaration_deletions."
                            f"{obligation}"
                        )
                        if (
                            not isinstance(obligation, str)
                            or not obligation
                            or not isinstance(deletion, dict)
                            or set(deletion) != {"incoming_sha256", "reason"}
                            or not isinstance(deletion.get("incoming_sha256"), str)
                            or len(deletion["incoming_sha256"]) != 64
                            or not isinstance(deletion.get("reason"), str)
                            or not deletion["reason"]
                        ):
                            errors.append(
                                f"{deletion_prefix}: expected exact non-empty "
                                "incoming_sha256 and reason strings"
                            )
                retained_deletions = migration.get("retained_deletions", {})
                retention_fields = {
                    "base_sha256",
                    "current_sha256",
                    "path",
                    "reason",
                }
                if not isinstance(retained_deletions, dict):
                    errors.append(
                        f"{prefix}.retained_deletions: expected object"
                    )
                else:
                    for obligation, retention in retained_deletions.items():
                        retention_prefix = (
                            f"{prefix}.retained_deletions.{obligation}"
                        )
                        if (
                            not isinstance(obligation, str)
                            or not obligation
                            or not isinstance(retention, dict)
                            or set(retention) != retention_fields
                            or any(
                                not isinstance(retention.get(field), str)
                                or not retention[field]
                                for field in retention_fields
                            )
                            or any(
                                len(retention[field]) != 64
                                or any(
                                    char not in "0123456789abcdef"
                                    for char in retention[field]
                                )
                                for field in (
                                    "base_sha256",
                                    "current_sha256",
                                )
                            )
                        ):
                            errors.append(
                                f"{retention_prefix}: expected exact "
                                "base_sha256, current_sha256, path, and reason "
                                "fields with lowercase SHA-256 hashes"
                            )
                reviewed_compositions = migration.get(
                    "reviewed_compositions",
                    {},
                )
                composition_fields = {
                    "base_sha256",
                    "incoming_sha256",
                    "current_sha256",
                    "path",
                    "reason",
                }
                if not isinstance(reviewed_compositions, dict):
                    errors.append(
                        f"{prefix}.reviewed_compositions: expected object"
                    )
                else:
                    for obligation, review in reviewed_compositions.items():
                        review_prefix = (
                            f"{prefix}.reviewed_compositions.{obligation}"
                        )
                        if (
                            not isinstance(obligation, str)
                            or not obligation
                            or not isinstance(review, dict)
                            or set(review) != composition_fields
                            or any(
                                not isinstance(review.get(field), str)
                                or not review[field]
                                for field in composition_fields
                            )
                            or any(
                                len(review[field]) != 64
                                or any(
                                    char not in "0123456789abcdef"
                                    for char in review[field]
                                )
                                for field in (
                                    "base_sha256",
                                    "incoming_sha256",
                                    "current_sha256",
                                )
                            )
                        ):
                            errors.append(
                                f"{review_prefix}: expected exact base_sha256, "
                                "incoming_sha256, current_sha256, path, and "
                                "reason fields with lowercase SHA-256 hashes"
                            )
                reviewed_resolutions = migration.get(
                    "reviewed_resolutions",
                    {},
                )
                resolution_fields = {
                    "status",
                    "base_sha256",
                    "incoming_sha256",
                    "current_sha256",
                    "path",
                    "reason",
                }
                reviewable_statuses = {
                    "added_name_collision",
                    "clean_candidate",
                    "container_fields_diverged",
                    "container_variants_diverged",
                    "three_way_conflict",
                }
                if not isinstance(reviewed_resolutions, dict):
                    errors.append(
                        f"{prefix}.reviewed_resolutions: expected object"
                    )
                else:
                    for obligation, review in reviewed_resolutions.items():
                        review_prefix = (
                            f"{prefix}.reviewed_resolutions.{obligation}"
                        )
                        hashes: list[object] = []
                        if isinstance(review, dict):
                            hashes = [
                                review.get("incoming_sha256"),
                                review.get("current_sha256"),
                            ]
                            if review.get("base_sha256") is not None:
                                hashes.append(review.get("base_sha256"))
                        if (
                            not isinstance(obligation, str)
                            or not obligation
                            or not isinstance(review, dict)
                            or set(review) != resolution_fields
                            or review.get("status") not in reviewable_statuses
                            or (
                                review.get("base_sha256") is not None
                                and not isinstance(
                                    review.get("base_sha256"),
                                    str,
                                )
                            )
                            or any(
                                not isinstance(review.get(field), str)
                                or not review[field]
                                for field in (
                                    "incoming_sha256",
                                    "current_sha256",
                                    "path",
                                    "reason",
                                )
                            )
                            or any(
                                not isinstance(value, str)
                                or len(value) != 64
                                or any(
                                    char not in "0123456789abcdef"
                                    for char in value
                                )
                                for value in hashes
                            )
                        ):
                            errors.append(
                                f"{review_prefix}: expected exact reviewable "
                                "status, nullable base_sha256, incoming_sha256, "
                                "current_sha256, path, and reason fields with "
                                "lowercase SHA-256 hashes"
                            )
        else:
            errors.append(f"{key}: internal schema error for {expected!r}")

    required_migrations = data.get("required_split_migrations", [])
    split_migrations = data.get("split_migrations", {})
    moved_paths = data.get("moved_paths", {})
    if isinstance(required_migrations, list):
        valid_required = [
            name
            for name in required_migrations
            if isinstance(name, str) and name
        ]
        duplicate_required = sorted(
            {
                name
                for name in valid_required
                if valid_required.count(name) > 1
            }
        )
        if duplicate_required:
            errors.append(
                "required_split_migrations: duplicate entries "
                + ", ".join(duplicate_required)
            )
        if isinstance(split_migrations, dict):
            missing_required = sorted(
                set(valid_required) - set(split_migrations)
            )
            if missing_required:
                errors.append(
                    "required_split_migrations: missing split_migrations "
                    + ", ".join(missing_required)
                )
            if isinstance(moved_paths, dict):
                for migration_name in valid_required:
                    migration = split_migrations.get(migration_name)
                    if not isinstance(migration, dict):
                        continue
                    source = migration.get("source")
                    destinations = migration.get("destinations")
                    if not isinstance(source, str) or not isinstance(
                        destinations,
                        list,
                    ):
                        continue
                    mapped_paths = moved_paths.get(source)
                    if not isinstance(mapped_paths, list) or not mapped_paths:
                        errors.append(
                            f"required split migration {migration_name}: "
                            f"moved_paths has no entry for {source}"
                        )
                        continue
                    source_facade_removed = migration.get(
                        "source_facade_removed",
                        False,
                    )
                    if source_facade_removed is True and source in mapped_paths:
                        errors.append(
                            f"required split migration {migration_name}: "
                            f"moved_paths.{source} must omit its removed source facade"
                        )
                    elif source_facade_removed is not True and source not in mapped_paths:
                        errors.append(
                            f"required split migration {migration_name}: "
                            f"moved_paths.{source} must include its source facade"
                        )
                    valid_destinations = [
                        destination
                        for destination in destinations
                        if isinstance(destination, str) and destination
                    ]

                    def covered_by_destination(candidate: str) -> bool:
                        return any(
                            candidate == destination
                            or candidate.startswith(destination.rstrip("/") + "/")
                            for destination in valid_destinations
                        )

                    outside_destinations = sorted(
                        mapped
                        for mapped in mapped_paths
                        if isinstance(mapped, str)
                        and not covered_by_destination(mapped)
                    )
                    if outside_destinations:
                        errors.append(
                            f"required split migration {migration_name}: "
                            "moved_paths contains paths outside destinations: "
                            + ", ".join(outside_destinations)
                        )
                    uncovered_destinations = sorted(
                        destination
                        for destination in valid_destinations
                        if not any(
                            isinstance(mapped, str)
                            and (
                                mapped == destination
                                or mapped.startswith(
                                    destination.rstrip("/") + "/"
                                )
                            )
                            for mapped in mapped_paths
                        )
                    )
                    if uncovered_destinations:
                        errors.append(
                            f"required split migration {migration_name}: "
                            "destinations missing from moved_paths coverage: "
                            + ", ".join(uncovered_destinations)
                        )

    decision_fields = (
        "expected_incoming_only_integration_diffs",
        "expected_ours_only_resolutions",
        "expected_main_only_resolutions",
        "expected_generated_without_spec_changes",
        "manual_review_acknowledgements",
    )
    if any(data.get(key) for key in decision_fields):
        scope = data.get("decision_scope", {})
        if set(scope) != {"ours_sha", "incoming_sha"}:
            errors.append(
                "decision_scope: per-merge decisions require exactly ours_sha and incoming_sha"
            )
        else:
            for key, value in scope.items():
                if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
                    errors.append(
                        f"decision_scope.{key}: expected full lowercase 40-character commit SHA"
                    )

    if errors:
        joined = "\n  - ".join(errors)
        raise ValueError(f"{path}: invalid manifest\n  - {joined}")
    return data


def load_policy(path: pathlib.Path) -> None:
    if not path.exists():
        return
    data = ensure_manifest_shape(load_json_file(path), path)
    DECISION_SCOPE.update(data.get("decision_scope", {}))
    MOVED_PATHS.update({key: list(value) for key, value in data.get("moved_paths", {}).items()})
    EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS.update(data.get("expected_incoming_only_integration_diffs", {}))
    EXPECTED_OURS_ONLY_RESOLUTIONS.update(data.get("expected_ours_only_resolutions", {}))
    EXPECTED_MAIN_ONLY_RESOLUTIONS.update(data.get("expected_main_only_resolutions", {}))
    GENERATED_PROVENANCE_ALLOWLIST.update(data.get("expected_generated_without_spec_changes", {}))
    MANUAL_REVIEW_REQUIRED_PATHS.update(data.get("manual_review_required_paths", {}))
    MANUAL_REVIEW_ACKNOWLEDGEMENTS.update(data.get("manual_review_acknowledgements", {}))
    GENERATED_FILE_PROVENANCE.update({key: list(value) for key, value in data.get("generated_file_provenance", {}).items()})


def run_git(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc


def lines(args: list[str]) -> list[str]:
    return [line for line in run_git(args).stdout.splitlines() if line]


def blob(rev: str, path: str) -> str | None:
    proc = run_git(["rev-parse", f"{rev}:{path}"], check=False)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def index_blob(path: str) -> str | None:
    proc = run_git(["rev-parse", f":{path}"], check=False)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def path_category(path: str) -> str:
    if path.startswith("zig/pkg/antfly/src/storage/db/"):
        return "zig-db"
    if path.startswith("zig/pkg/antfly/src/api/table_reads"):
        return "zig-table-reads"
    if path.startswith("zig/pkg/antfly/src/api/table_writes"):
        return "zig-table-writes"
    if path.startswith("zig/pkg/antfly/src/api/") or path.startswith("zig/pkg/antfly/src/query/"):
        return "zig-api-query"
    if path.startswith("zig/pkg/antfly/src/openapi/generated/"):
        return "zig-generated-openapi"
    if path.startswith("specs/openapi/"):
        return "spec"
    if path.startswith("go/"):
        return "go"
    if path.startswith("py/"):
        return "python"
    if path.startswith("ts/"):
        return "typescript"
    if path.startswith(".github/"):
        return "ci"
    return path.split("/", 1)[0]


def result_captured_moved_path(incoming_path: str) -> bool:
    paths = MOVED_PATHS.get(incoming_path)
    if not paths:
        return False
    return any(index_blob(path) is not None for path in paths)


def audited_ref(raw: str) -> str:
    return run_git(["rev-parse", "--verify", raw]).stdout.strip()


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def matches_pattern(path: str, pattern: str) -> bool:
    if pattern.endswith("/"):
        return path.startswith(pattern)
    if pattern.endswith("*") or any(token in pattern for token in ("?", "[")):
        return fnmatch.fnmatch(path, pattern)
    return path == pattern or path.startswith(pattern.rstrip("/") + "/")


def matching_policy(path: str, policy: dict[str, str]) -> str | None:
    for pattern, reason in policy.items():
        if matches_pattern(path, pattern):
            return reason
    return None


def matching_provenance(path: str) -> tuple[str, list[str]] | None:
    for pattern, specs in GENERATED_FILE_PROVENANCE.items():
        if matches_pattern(path, pattern):
            return pattern, specs
    return None


def merge_parents(rev: str) -> list[str]:
    parent_line = run_git(["show", "--no-patch", "--pretty=%P", rev]).stdout.strip()
    return parent_line.split() if parent_line else []


def is_ancestor(candidate: str, descendant: str) -> bool:
    return run_git(["merge-base", "--is-ancestor", candidate, descendant], check=False).returncode == 0


def resolved_previous_main_baseline(previous_main_merge: str, incoming_ref: str) -> str:
    """Resolve a prior `origin/main` merge commit to the main-side parent.

    Comparing a feature-branch merge commit directly to a newer main ref can
    misclassify branch-only changes as incoming deletions. For ordinary
    `git merge origin/main` commits, the main-side parent is the parent that is
    also an ancestor of the newer incoming main ref; prefer the second parent
    when it satisfies that contract.
    """
    rev = audited_ref(previous_main_merge)
    parents = merge_parents(rev)
    if len(parents) < 2:
        return rev

    for parent in parents[1:] + parents[:1]:
        if is_ancestor(parent, incoming_ref):
            return parent
    return rev


def load_snapshot(path: pathlib.Path) -> dict[str, Any]:
    data = load_json_file(path)
    required = {
        "schema_version": int,
        "mode": str,
        "base": str,
        "incoming_ref": str,
        "incoming_sha": str,
        "incoming_paths": list,
    }
    for key, expected_type in required.items():
        if key not in data or not isinstance(data[key], expected_type):
            raise ValueError(f"{path}: snapshot missing {key!r} with type {expected_type.__name__}")
    if data["schema_version"] != 1:
        raise ValueError(f"{path}: unsupported snapshot schema_version {data['schema_version']!r}")
    if not all(isinstance(item, str) for item in data["incoming_paths"]):
        raise ValueError(f"{path}: incoming_paths must be list[string]")
    return data


def validate_snapshot_ref(snapshot: dict[str, Any]) -> None:
    current_sha = audited_ref(snapshot["incoming_ref"])
    if current_sha != snapshot["incoming_sha"]:
        raise ValueError(
            "snapshot incoming ref moved: "
            f"{snapshot['incoming_ref']} is {current_sha}, snapshot recorded {snapshot['incoming_sha']}"
        )


def write_snapshot(path: pathlib.Path, incoming_ref: str, previous_main_merge: str | None) -> dict[str, Any]:
    incoming_sha = audited_ref(incoming_ref)
    previous_baseline_input = audited_ref(previous_main_merge) if previous_main_merge else None
    base = (
        resolved_previous_main_baseline(previous_main_merge, incoming_sha)
        if previous_main_merge
        else run_git(["merge-base", "HEAD", incoming_ref]).stdout.strip()
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "mode": "previous-main-baseline" if previous_main_merge else "active-merge-base",
        "created_at": now_utc(),
        "base": base,
        "head_at_snapshot": audited_ref("HEAD"),
        "incoming_ref": incoming_ref,
        "incoming_sha": incoming_sha,
        "previous_main_merge_input": previous_baseline_input,
        "incoming_paths": lines(["diff", "--name-only", f"{base}..{incoming_sha}"]),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def audit_merge(
    incoming_ref: str,
    previous_main_merge: str | None,
    ours_ref: str,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if snapshot:
        merge_head = snapshot["incoming_sha"]
        base = snapshot["base"]
        incoming_paths = list(snapshot["incoming_paths"])
        previous_baseline_input = snapshot.get("previous_main_merge_input")
        mode = f"snapshot:{snapshot['mode']}"
    else:
        merge_head = audited_ref(incoming_ref)
        previous_baseline_input = audited_ref(previous_main_merge) if previous_main_merge else None
        base = (
            resolved_previous_main_baseline(previous_main_merge, merge_head)
            if previous_main_merge
            else run_git(["merge-base", "HEAD", incoming_ref]).stdout.strip()
        )
        incoming_paths = lines(["diff", "--name-only", f"{base}..{merge_head}"])
        mode = "previous-main-baseline" if previous_main_merge else "active-merge"

    head = audited_ref(ours_ref)
    decision_scope_active = (
        DECISION_SCOPE.get("ours_sha") == head
        and DECISION_SCOPE.get("incoming_sha") == merge_head
    )
    expected_incoming_only_integration_diffs = (
        EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS if decision_scope_active else {}
    )
    expected_ours_only_resolutions = EXPECTED_OURS_ONLY_RESOLUTIONS if decision_scope_active else {}
    expected_main_only_resolutions = EXPECTED_MAIN_ONLY_RESOLUTIONS if decision_scope_active else {}
    manual_review_acknowledgements = MANUAL_REVIEW_ACKNOWLEDGEMENTS if decision_scope_active else {}
    generated_provenance_allowlist = GENERATED_PROVENANCE_ALLOWLIST if decision_scope_active else {}
    staged_paths = set(lines(["diff", "--cached", "--name-only"]))
    result_changed_paths = set(incoming_paths) | staged_paths
    both_changed: list[str] = []
    ours_only_resolutions: list[str] = []
    main_only_resolutions: list[str] = []
    same_as_both_resolutions: list[str] = []
    integrated_resolutions = 0
    incoming_only_mismatches: list[str] = []
    expected_integration_diffs: list[str] = []
    incoming_deleted_not_deleted: list[str] = []
    result_missing: list[str] = []
    moved_paths: list[str] = []
    manual_review_required: dict[str, str] = {}
    manual_review_missing: dict[str, str] = {}
    generated_without_spec_changes: dict[str, str] = {}
    incoming_only_ok = 0
    both_counts: dict[str, int] = {}
    incoming_counts: dict[str, int] = {}

    for path in incoming_paths:
        incoming_counts[path_category(path)] = incoming_counts.get(path_category(path), 0) + 1
        base_blob = blob(base, path)
        head_blob = blob(head, path)
        merge_blob = blob(merge_head, path)
        result_blob = index_blob(path)
        ours_changed = head_blob != base_blob

        if not ours_changed:
            if merge_blob is None:
                if result_blob is not None:
                    incoming_deleted_not_deleted.append(path)
                else:
                    incoming_only_ok += 1
                continue
            if result_blob == merge_blob:
                incoming_only_ok += 1
                continue
            if result_captured_moved_path(path):
                moved_paths.append(path)
                continue
            if path in expected_incoming_only_integration_diffs:
                expected_integration_diffs.append(path)
            elif result_blob is None:
                result_missing.append(path)
            else:
                incoming_only_mismatches.append(path)
            continue

        both_changed.append(path)
        both_counts[path_category(path)] = both_counts.get(path_category(path), 0) + 1
        if result_blob == head_blob and result_blob != merge_blob:
            ours_only_resolutions.append(path)
        elif result_blob == merge_blob and result_blob != head_blob:
            main_only_resolutions.append(path)
        elif result_blob == head_blob and result_blob == merge_blob:
            same_as_both_resolutions.append(path)
        elif result_blob != head_blob and result_blob != merge_blob:
            integrated_resolutions += 1

    for path in sorted(result_changed_paths):
        review_reason = matching_policy(path, MANUAL_REVIEW_REQUIRED_PATHS)
        if review_reason:
            manual_review_required[path] = review_reason
            if not matching_policy(path, manual_review_acknowledgements):
                manual_review_missing[path] = review_reason

        provenance = matching_provenance(path)
        if provenance is None:
            continue
        generated_pattern, spec_patterns = provenance
        relevant_spec_changed = any(
            any(matches_pattern(changed_path, spec_pattern) for changed_path in result_changed_paths)
            for spec_pattern in spec_patterns
        )
        if not relevant_spec_changed and not matching_policy(path, generated_provenance_allowlist):
            generated_without_spec_changes[path] = generated_pattern

    failures: dict[str, list[str]] = {}
    if result_missing:
        failures["incoming-only result missing"] = result_missing
    if incoming_only_mismatches:
        failures["incoming-only result differs from incoming ref"] = incoming_only_mismatches
    if incoming_deleted_not_deleted:
        failures["incoming-only deletion not reflected"] = incoming_deleted_not_deleted
    unexpected_ours_only = [
        path for path in ours_only_resolutions
        if path not in expected_ours_only_resolutions
    ]
    unexpected_main_only = [
        path for path in main_only_resolutions
        if path not in expected_main_only_resolutions
    ]
    if unexpected_ours_only:
        failures["unexpected both-changed result identical to ours"] = unexpected_ours_only
    if unexpected_main_only:
        failures["unexpected both-changed result identical to main"] = unexpected_main_only
    if manual_review_missing:
        failures["manual review acknowledgement missing"] = sorted(manual_review_missing)
    if generated_without_spec_changes:
        failures["generated file changed without matching spec provenance"] = sorted(generated_without_spec_changes)

    return {
        "schema_version": 1,
        "generated_at": now_utc(),
        "mode": mode,
        "base": base,
        "head": head,
        "ours_ref": ours_ref,
        "incoming_ref": incoming_ref,
        "incoming_sha": merge_head,
        "previous_main_merge_input": previous_baseline_input,
        "previous_main_baseline_resolved": base if previous_baseline_input else None,
        "decision_scope": {
            "configured": dict(DECISION_SCOPE),
            "active": decision_scope_active,
        },
        "incoming_changed_files": len(incoming_paths),
        "incoming_only_exact_or_deleted_ok": incoming_only_ok,
        "incoming_only_moved_ok": len(moved_paths),
        "both_changed_files": len(both_changed),
        "incoming_categories": dict(sorted(incoming_counts.items())),
        "both_changed_categories": dict(sorted(both_counts.items())),
        "moved_paths": moved_paths,
        "expected_integration_diffs": {
            path: expected_incoming_only_integration_diffs[path]
            for path in expected_integration_diffs
        },
        "both_changed_paths": both_changed,
        "both_changed_exact_side_resolution": {
            "integrated_other": integrated_resolutions,
            "same_as_both": same_as_both_resolutions,
            "ours_only": {
                path: expected_ours_only_resolutions.get(path, "UNEXPECTED")
                for path in ours_only_resolutions
            },
            "main_only": {
                path: expected_main_only_resolutions.get(path, "UNEXPECTED")
                for path in main_only_resolutions
            },
        },
        "manual_review": {
            "required": manual_review_required,
            "acknowledged": {
                path: reason
                for path, reason in manual_review_acknowledgements.items()
                if any(matches_pattern(changed_path, path) for changed_path in result_changed_paths)
            },
            "missing": manual_review_missing,
        },
        "generated_provenance": {
            "rules": GENERATED_FILE_PROVENANCE,
            "allowlist": generated_provenance_allowlist,
            "changed_without_matching_spec": generated_without_spec_changes,
        },
        "failures": failures,
        "ok": not failures,
    }


def render_text(result: dict[str, Any], summary_only: bool) -> str:
    out: list[str] = [
        f"mode={result['mode']}",
        f"base={result['base']}",
    ]
    if result.get("previous_main_merge_input"):
        out.append(f"previous_main_merge_input={result['previous_main_merge_input']}")
        out.append(f"previous_main_baseline_resolved={result['previous_main_baseline_resolved']}")
    out.extend([
        f"ours_ref={result['ours_ref']}",
        f"head={result['head']}",
        f"incoming_sha={result['incoming_sha']}",
        f"decision_scope_active={str(result['decision_scope']['active']).lower()}",
        f"incoming_changed_files={result['incoming_changed_files']}",
        f"incoming_only_exact_or_deleted_ok={result['incoming_only_exact_or_deleted_ok']}",
        f"incoming_only_moved_ok={result['incoming_only_moved_ok']}",
        f"both_changed_files={result['both_changed_files']}",
        "",
        "incoming categories:",
    ])
    for category, count in result["incoming_categories"].items():
        out.append(f"  {category}: {count}")
    out.append("both-changed categories:")
    for category, count in result["both_changed_categories"].items():
        out.append(f"  {category}: {count}")

    if result["moved_paths"] and not summary_only:
        out.extend(["", "moved/split incoming paths accepted for manual symbol audit:"])
        out.extend(f"  {path}" for path in result["moved_paths"])
    if result["expected_integration_diffs"] and not summary_only:
        out.extend(["", "expected incoming-only integration diffs:"])
        out.extend(f"  {path}: {reason}" for path, reason in result["expected_integration_diffs"].items())
    if not summary_only:
        out.extend(["", "both-changed paths requiring semantic review:"])
        paths = result["both_changed_paths"]
        out.extend(f"  {path}" for path in paths[:120])
        if len(paths) > 120:
            out.append(f"  ... +{len(paths) - 120} more")

    exact = result["both_changed_exact_side_resolution"]
    out.extend([
        "",
        "both-changed exact-side resolution audit:",
        f"  integrated_other: {exact['integrated_other']}",
        f"  same_as_both: {len(exact['same_as_both'])}",
        f"  ours_only: {len(exact['ours_only'])}",
        f"  main_only: {len(exact['main_only'])}",
        "",
        "manual-review audit:",
        f"  required_changed_paths: {len(result['manual_review']['required'])}",
        f"  missing_acknowledgements: {len(result['manual_review']['missing'])}",
        "",
        "generated provenance audit:",
        f"  changed_without_matching_spec: {len(result['generated_provenance']['changed_without_matching_spec'])}",
    ])

    if result["failures"]:
        out.extend(["", "FAILURES:"])
        for title, paths in result["failures"].items():
            out.append(f"{title}:")
            out.extend(f"  {path}" for path in paths)
    else:
        out.extend([
            "",
            "OK: every incoming-only path is exact, deleted, moved/split, integrated by policy, and required review/provenance checks are satisfied.",
        ])
    return "\n".join(out)


def render_markdown(result: dict[str, Any]) -> str:
    out: list[str] = [
        "# Merge Capture Audit",
        "",
        f"Generated: {result['generated_at']}",
        f"Mode: `{result['mode']}`",
        f"Base: `{result['base']}`",
        f"Incoming: `{result['incoming_ref']}` (`{result['incoming_sha']}`)",
        f"Ours ref: `{result['ours_ref']}`",
        f"HEAD: `{result['head']}`",
        "",
        "## Summary",
        "",
        f"- Incoming changed files: {result['incoming_changed_files']}",
        f"- Both-changed files: {result['both_changed_files']}",
        f"- Incoming-only exact/deleted: {result['incoming_only_exact_or_deleted_ok']}",
        f"- Incoming-only moved/split: {result['incoming_only_moved_ok']}",
        f"- Manual-review paths touched: {len(result['manual_review']['required'])}",
        f"- Missing manual-review acknowledgements: {len(result['manual_review']['missing'])}",
        f"- Generated files without matching spec provenance: {len(result['generated_provenance']['changed_without_matching_spec'])}",
        "",
        "## Categories",
        "",
    ]
    for category, count in result["incoming_categories"].items():
        out.append(f"- {category}: {count}")
    if result["expected_integration_diffs"]:
        out.extend(["", "## Documented Integrations", ""])
        for path, reason in result["expected_integration_diffs"].items():
            out.append(f"- `{path}`: {reason}")
    if result["manual_review"]["required"]:
        out.extend(["", "## Manual Review", ""])
        for path, reason in result["manual_review"]["required"].items():
            status = "acknowledged" if path not in result["manual_review"]["missing"] else "missing"
            out.append(f"- `{path}` ({status}): {reason}")
    if result["failures"]:
        out.extend(["", "## Failures", ""])
        for title, paths in result["failures"].items():
            out.append(f"### {title}")
            out.extend(f"- `{path}`" for path in paths)
    else:
        out.extend(["", "## Result", "", "OK"])
    out.append("")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=str(DEFAULT_POLICY_PATH), help="JSON merge-audit policy manifest")
    parser.add_argument("--incoming-ref", default="MERGE_HEAD", help="incoming ref to audit; defaults to active MERGE_HEAD")
    parser.add_argument("--ours-ref", default="HEAD", help="ours/pre-merge ref; use HEAD^1 when auditing after a merge commit")
    parser.add_argument(
        "--previous-main-merge",
        help="baseline mode: audit changes from this previous main-merge baseline through --incoming-ref",
    )
    parser.add_argument("--snapshot", help="read pre-merge snapshot JSON and audit against its frozen incoming sha/path set")
    parser.add_argument("--write-snapshot", help="write a pre-merge snapshot JSON and exit")
    parser.add_argument("--strict-snapshot-ref", action="store_true", help="fail when --snapshot incoming_ref no longer points at the recorded incoming_sha")
    parser.add_argument("--check-snapshot-ref", action="store_true", help="only validate that --snapshot incoming_ref still points at incoming_sha")
    parser.add_argument("--json-out", help="write machine-readable audit JSON")
    parser.add_argument("--write-report", help="write a markdown audit report")
    parser.add_argument("--validate-manifest", action="store_true", help="validate manifest and exit")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="omit long path listings and print only counts plus failures",
    )
    args = parser.parse_args()

    try:
        load_policy(pathlib.Path(args.manifest))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"manifest error: {exc}", file=sys.stderr)
        return 2

    if args.validate_manifest:
        print(f"OK: manifest valid: {args.manifest}")
        return 0

    if args.write_snapshot:
        try:
            snapshot_payload = write_snapshot(pathlib.Path(args.write_snapshot), args.incoming_ref, args.previous_main_merge)
        except (RuntimeError, ValueError) as exc:
            print(f"snapshot error: {exc}", file=sys.stderr)
            return 2
        print(f"Wrote snapshot {args.write_snapshot} for {snapshot_payload['incoming_ref']} ({snapshot_payload['incoming_sha']})")
        return 0

    try:
        snapshot_payload = load_snapshot(pathlib.Path(args.snapshot)) if args.snapshot else None
        if args.check_snapshot_ref:
            if not snapshot_payload:
                raise ValueError("--check-snapshot-ref requires --snapshot")
            validate_snapshot_ref(snapshot_payload)
            print(f"OK: snapshot incoming ref unchanged: {snapshot_payload['incoming_ref']} ({snapshot_payload['incoming_sha']})")
            return 0
        if args.strict_snapshot_ref:
            if not snapshot_payload:
                raise ValueError("--strict-snapshot-ref requires --snapshot")
            validate_snapshot_ref(snapshot_payload)
        result = audit_merge(args.incoming_ref, args.previous_main_merge, args.ours_ref, snapshot_payload)
    except (OSError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        print(f"audit error: {exc}", file=sys.stderr)
        return 2

    print(render_text(result, args.summary_only))

    if args.json_out:
        path = pathlib.Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if args.write_report:
        path = pathlib.Path(args.write_report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_markdown(result))

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
