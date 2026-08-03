#!/usr/bin/env python3
"""Audit incoming Zig build registrations against a split build tree."""

from __future__ import annotations

import argparse
import bisect
import fnmatch
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.merge_audit import audit_split_declarations as split_audit
from scripts.merge_audit.audit_main_capture import load_json_file

INCOMING_PATTERNS = {
    "options": re.compile(r'\bb\.option\([^,]+,\s*"([^"]+)"'),
    "steps": re.compile(r'\bb\.step\(\s*"([^"]+)"'),
    "root_sources": re.compile(
        r'\.root_source_file\s*=\s*b\.path\(\s*"([^"]+)"'
    ),
}

ARTIFACT_CONSTRUCTORS = {
    "addExecutable",
    "addObject",
    "addSharedLibrary",
    "addStaticLibrary",
}

SEMANTIC_METHOD_CATEGORIES = {
    "module_imports": "addImport",
    "module_options": "addOptions",
    "system_libraries": "linkSystemLibrary",
    "frameworks": "linkFramework",
    "include_paths": "addIncludePath",
    "system_include_paths": "addSystemIncludePath",
    "library_paths": "addLibraryPath",
    "framework_paths": "addFrameworkPath",
}

CURRENT_STEP_PATTERNS = (
    INCOMING_PATTERNS["steps"],
    re.compile(r'\.name\s*=\s*"([^"]+)"'),
    re.compile(r'\baddFocusedAPITestStep\(\s*b,\s*"([^"]+)"'),
    re.compile(
        r'\baddAPIFocusedTestRun\(\s*b,\s*[^,]+,\s*"([^"]+)"'
    ),
    re.compile(
        r'\baddModuleTestStep\(\s*b,\s*[^,]+,\s*"([^"]+)"'
    ),
)

CURRENT_ROOT_SOURCE_PATTERNS = (
    INCOMING_PATTERNS["root_sources"],
    re.compile(r'\.path\s*=\s*"([^"]+\.zig)"'),
)


def names(pattern: re.Pattern[str], text: str) -> set[str]:
    return set(pattern.findall(text))


def split_call_arguments(text: str, start: int) -> list[str]:
    """Split a call whose opening parenthesis immediately precedes start."""
    args: list[str] = []
    token_start = start
    depths = {"(": 1, "[": 0, "{": 0}
    pairs = {")": "(", "]": "[", "}": "{"}
    in_string = False
    escaped = False
    index = start
    while index < len(text):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
        elif char in depths:
            depths[char] += 1
        elif char in pairs:
            opener = pairs[char]
            depths[opener] -= 1
            if char == ")" and depths["("] == 0:
                final_arg = text[token_start:index].strip()
                if final_arg:
                    args.append(final_arg)
                return args
        elif (
            char == ","
            and depths["("] == 1
            and depths["["] == 0
            and depths["{"] == 0
        ):
            args.append(text[token_start:index].strip())
            token_start = index + 1
        index += 1
    return []


def compact_expression(expression: str) -> str:
    """Remove insignificant whitespace while preserving string contents."""
    result: list[str] = []
    in_string = False
    escaped = False
    for char in expression:
        if in_string:
            result.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
            result.append(char)
        elif not char.isspace():
            result.append(char)
    return "".join(result)


def method_call_facts(text: str, method: str) -> set[str]:
    """Return portable argument contracts for direct Zig build method calls."""
    result: set[str] = set()
    pattern = re.compile(
        rf"\b(?P<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        rf"\.{re.escape(method)}\s*\("
    )
    for match in pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if not args:
            continue
        normalized_args = [
            compact_expression(arg).replace("ctx.", "") for arg in args
        ]
        result.add(f"{method}({','.join(normalized_args)})")
    return result


def artifact_names(text: str) -> set[str]:
    """Return literal names from actual Zig compile artifact constructors."""
    result: set[str] = set()
    constructors = "|".join(sorted(ARTIFACT_CONSTRUCTORS))
    pattern = re.compile(
        rf"\b[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"
        rf"\.(?:{constructors})\s*\("
    )
    for match in pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if not args:
            continue
        name = re.search(r'\.name\s*=\s*"([^"]+)"', args[0])
        if name is not None:
            result.add(name.group(1))
    return result


def normalize_dependency_endpoint(expression: str) -> str:
    endpoint = compact_expression(expression)
    if endpoint.startswith("&"):
        endpoint = endpoint[1:]
    endpoint = endpoint.removesuffix(".?")
    endpoint = endpoint.removesuffix(".step")
    parts = endpoint.split(".")
    endpoint = parts[-2] if parts[-1:] == ["run"] and len(parts) > 1 else parts[-1]
    if endpoint.startswith("run_"):
        endpoint = endpoint[len("run_") :]
    if endpoint.endswith("_tests"):
        endpoint = endpoint[: -len("_tests")]
    return endpoint


def step_dependency_facts(text: str) -> set[str]:
    """Normalize dependency edges by public step name and dependency role."""
    result: set[str] = set()
    function_starts = [match.start() for match in re.finditer(r"\bfn\s+", text)]

    def function_scope(position: int) -> int:
        index = bisect.bisect_right(function_starts, position) - 1
        return function_starts[index] if index >= 0 else -1

    step_bindings: dict[tuple[int, str], list[tuple[int, str]]] = {}
    binding_pattern = re.compile(
        r'\bconst\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*b\.step\(\s*"([^"]+)"'
    )
    for binding in binding_pattern.finditer(text):
        key = (function_scope(binding.start()), binding.group(1))
        step_bindings.setdefault(key, []).append(
            (binding.start(), binding.group(2))
        )
    pattern = re.compile(
        r"\b(?P<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        r"\.dependOn\s*\("
    )
    for match in pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if not args:
            continue
        receiver = match.group("receiver")
        candidates = step_bindings.get(
            (function_scope(match.start()), receiver), []
        )
        candidate_index = bisect.bisect_left(candidates, (match.start(), "")) - 1
        source = (
            candidates[candidate_index][1]
            if candidate_index >= 0
            else normalize_dependency_endpoint(receiver)
        )
        target = normalize_dependency_endpoint(args[0])
        result.add(f"{source}->{target}")

    inline_pattern = re.compile(r'\bb\.step\(\s*"(?P<name>[^"]+)"[^;]*?\)\.dependOn\s*\(')
    for match in inline_pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if args:
            result.add(
                f"{match.group('name')}->{normalize_dependency_endpoint(args[0])}"
            )
    return result


def helper_dependency_facts(
    text: str,
    helpers: list[dict[str, Any]] | None,
) -> set[str]:
    result: set[str] = set()
    for helper in helpers or []:
        function = helper.get("function")
        step_argument = helper.get("step_argument")
        step_field = helper.get("step_field")
        target_argument = helper.get("target_argument")
        if not isinstance(function, str) or not function:
            raise ValueError("build dependency helper function must be a non-empty string")
        if not isinstance(step_argument, int) or step_argument < 0:
            raise ValueError("build dependency helper step_argument must be non-negative")
        if step_field is not None and (
            not isinstance(step_field, str) or not step_field
        ):
            raise ValueError("build dependency helper step_field must be non-empty")
        if target_argument is not None and (
            not isinstance(target_argument, int) or target_argument < 0
        ):
            raise ValueError("build dependency helper target_argument must be non-negative")
        pattern = re.compile(
            rf"(?:(?:const\s+(?P<binding>[A-Za-z_][A-Za-z0-9_]*)|_)\s*=\s*)?"
            rf"\b{re.escape(function)}\s*\("
        )
        for match in pattern.finditer(text):
            args = split_call_arguments(text, match.end())
            if step_argument >= len(args):
                continue
            step_expression = args[step_argument]
            if step_field is not None:
                field_match = re.search(
                    rf'\.{re.escape(step_field)}\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"',
                    step_expression,
                )
                literal = field_match
            else:
                literal = re.fullmatch(
                    r'"([^"\\]*(?:\\.[^"\\]*)*)"', step_expression
                )
            if literal is None:
                continue
            step_name = bytes(literal.group(1), "utf-8").decode("unicode_escape")
            binding = match.group("binding")
            if target_argument is not None and target_argument < len(args):
                target = normalize_dependency_endpoint(args[target_argument])
            elif binding is not None:
                target = normalize_dependency_endpoint(binding)
            else:
                target = step_name.removesuffix("-test").replace("-", "_")
            result.add(f"{step_name}->{target}")
    return result


def native_source_facts(text: str) -> set[str]:
    """Inventory literal native source files and the module compiling them."""
    result: set[str] = set()
    pattern = re.compile(
        r"\b(?P<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        r"\.addCSourceFiles?\s*\("
    )
    source_pattern = re.compile(
        r'"([^"\n]+\.(?:c|cc|cpp|cxx|m|mm|s|S))"'
    )
    for match in pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if not args:
            continue
        for source in source_pattern.findall(" ".join(args)):
            result.add(f"{match.group('receiver')}.native_source({source})")
    return result


def semantic_surface(text: str) -> dict[str, set[str]]:
    result = {
        category: method_call_facts(text, method)
        for category, method in SEMANTIC_METHOD_CATEGORIES.items()
    }
    result["step_dependencies"] = step_dependency_facts(text)
    result["native_sources"] = native_source_facts(text)
    return result


def incoming_surface(text: str) -> dict[str, set[str]]:
    result = {
        category: names(pattern, text)
        for category, pattern in INCOMING_PATTERNS.items()
    }
    result["artifacts"] = artifact_names(text)
    result.update(semantic_surface(text))
    return result


def call_string_arguments(text: str, function: str, argument: int) -> set[str]:
    """Extract literal top-level arguments from calls to a configured helper."""
    result: set[str] = set()
    call_pattern = re.compile(rf"\b{re.escape(function)}\s*\(")
    for match in call_pattern.finditer(text):
        args = split_call_arguments(text, match.end())
        if argument >= len(args):
            continue
        literal = re.fullmatch(r'"([^"\\]*(?:\\.[^"\\]*)*)"', args[argument])
        if literal:
            result.add(bytes(literal.group(1), "utf-8").decode("unicode_escape"))
    return result


def current_surface(
    text: str,
    helper_arguments: dict[str, list[dict[str, Any]]] | None = None,
    dependency_helpers: list[dict[str, Any]] | None = None,
) -> dict[str, set[str]]:
    result = incoming_surface(text)
    result["root_sources"] = set().union(
        *(names(pattern, text) for pattern in CURRENT_ROOT_SOURCE_PATTERNS)
    )
    string_bindings = dict(
        re.findall(r'\bconst\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]+)"', text)
    )
    for binding in re.findall(
        r'\.root_source_file\s*=\s*b\.path\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)',
        text,
    ):
        if binding in string_bindings:
            result["root_sources"].add(string_bindings[binding])
    result["steps"] = set().union(
        *(names(pattern, text) for pattern in CURRENT_STEP_PATTERNS)
    )
    result["step_dependencies"].update(
        helper_dependency_facts(text, dependency_helpers)
    )
    for category, helpers in (helper_arguments or {}).items():
        if category not in result:
            raise ValueError(f"unknown build surface helper category: {category}")
        for helper in helpers:
            function = helper.get("function")
            argument = helper.get("argument")
            if not isinstance(function, str) or not function:
                raise ValueError("build surface helper function must be a non-empty string")
            if not isinstance(argument, int) or argument < 0:
                raise ValueError("build surface helper argument must be a non-negative integer")
            result[category].update(call_string_arguments(text, function, argument))
    return result


def runtime_step_inventory(build_dir: pathlib.Path) -> set[str]:
    proc = subprocess.run(
        ["zig", "build", "-l", "-fincremental"],
        cwd=build_dir,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"zig build -l failed in {build_dir}: {detail}")
    return {
        match.group(1)
        for line in proc.stdout.splitlines()
        if (match := re.match(r"^\s{2}(\S+)\s", line)) is not None
    }


def parse_name_status(raw: bytes) -> set[str]:
    fields = [field for field in raw.split(b"\0") if field]
    result: set[str] = set()
    index = 0
    while index < len(fields):
        status = fields[index].decode("ascii")
        index += 1
        if not status:
            raise ValueError("git diff emitted an empty build companion status")
        kind = status[0]
        if kind in {"R", "C"}:
            if index + 1 >= len(fields):
                raise ValueError("git diff emitted a truncated rename/copy record")
            old_path = fields[index].decode("utf-8")
            new_path = fields[index + 1].decode("utf-8")
            index += 2
            if kind == "R":
                result.add(old_path)
            result.add(new_path)
            continue
        if index >= len(fields):
            raise ValueError("git diff emitted a truncated build companion record")
        result.add(fields[index].decode("utf-8"))
        index += 1
    return result


def changed_paths(base: str, incoming: str) -> set[str]:
    proc = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "-z",
            "--find-renames",
            f"{base}..{incoming}",
            "--",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git diff failed while inventorying build companions: {detail}")
    return parse_name_status(proc.stdout)


def ref_blob(ref: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if proc.returncode == 0:
        return proc.stdout
    if proc.returncode == 128:
        return None
    raise RuntimeError(f"git show failed for {ref}:{path}")


def current_blob(path: str) -> bytes | None:
    candidate = (ROOT / path).resolve()
    try:
        candidate.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError(f"build companion escapes repository: {path}") from exc
    if not candidate.is_file():
        return None
    return candidate.read_bytes()


def blob_sha256(blob: bytes | None) -> str | None:
    return hashlib.sha256(blob).hexdigest() if blob is not None else None


def classify_companion(
    path: str,
    incoming_blob: bytes | None,
    destination_blob: bytes | None,
    owner_migration: str | None,
) -> dict[str, Any]:
    if incoming_blob == destination_blob:
        status = "exact" if incoming_blob is not None else "exact_deleted"
    elif owner_migration is not None:
        status = "declaration_audit_required"
    elif incoming_blob is None:
        status = "retained_deleted_unowned"
    elif destination_blob is None:
        status = "missing_current"
    else:
        status = "divergent_unowned"
    return {
        "path": path,
        "status": status,
        "owner_migration": owner_migration,
        "incoming_sha256": blob_sha256(incoming_blob),
        "current_sha256": blob_sha256(destination_blob),
    }


def companion_report(
    base: str,
    incoming: str,
    source: str,
    globs: list[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    for pattern in globs:
        if (
            not isinstance(pattern, str)
            or not pattern
            or pattern.startswith("/")
            or ".." in pathlib.PurePosixPath(pattern).parts
        ):
            raise ValueError(
                "build_companion_globs must contain non-empty repository-relative patterns"
            )

    migrations = policy.get("split_migrations", {})
    required = set(policy.get("required_split_migrations", []))
    owners: dict[str, str] = {}
    destination_owners: dict[str, set[str]] = {}
    for name, migration in migrations.items():
        if not isinstance(migration, dict):
            continue
        migration_source = migration.get("source")
        if isinstance(migration_source, str) and name in required:
            owners[migration_source] = name
        raw_destinations = migration.get("destinations")
        if (
            name in required
            and isinstance(raw_destinations, list)
            and raw_destinations
            and all(isinstance(item, str) and item for item in raw_destinations)
        ):
            for destination in split_audit.expand_destinations(raw_destinations):
                relative = destination.relative_to(ROOT).as_posix()
                destination_owners.setdefault(relative, set()).add(name)

    for path, migration_names in destination_owners.items():
        owners.setdefault(path, ",".join(sorted(migration_names)))

    paths = sorted(
        path
        for path in changed_paths(base, incoming)
        if path != source
        and any(fnmatch.fnmatchcase(path, pattern) for pattern in globs)
    )
    files = [
        classify_companion(
            path,
            ref_blob(incoming, path),
            current_blob(path),
            owners.get(path),
        )
        for path in paths
    ]
    unresolved_statuses = {
        "divergent_unowned",
        "missing_current",
        "retained_deleted_unowned",
    }
    unresolved = [item for item in files if item["status"] in unresolved_statuses]
    return {
        "globs": globs,
        "files": files,
        "summary": {
            "changed": len(files),
            "exact": sum(
                item["status"] in {"exact", "exact_deleted"} for item in files
            ),
            "delegated": sum(
                item["status"] == "declaration_audit_required" for item in files
            ),
            "unresolved": len(unresolved),
        },
    }


def added_surface(base_text: str, incoming_text: str) -> dict[str, set[str]]:
    base_surface = incoming_surface(base_text)
    next_surface = incoming_surface(incoming_text)
    return {
        category: values - base_surface[category]
        for category, values in next_surface.items()
    }


def merge_surface_aliases(
    aliases: dict[str, dict[str, str | list[str]]],
    retention_aliases: dict[str, dict[str, str | list[str]]],
) -> dict[str, dict[str, str | list[str]]]:
    result = {
        category: dict(category_aliases)
        for category, category_aliases in aliases.items()
    }
    for category, category_aliases in retention_aliases.items():
        target = result.setdefault(category, {})
        for source, replacement in category_aliases.items():
            if source in target and target[source] != replacement:
                raise ValueError(
                    f"conflicting build surface retention alias: {category}.{source}"
                )
            target[source] = replacement
    return result


def build_report(
    base: str,
    incoming: str,
    source: str,
    destination_paths: list[pathlib.Path],
    helper_arguments: dict[str, list[dict[str, Any]]] | None = None,
    runtime_steps: set[str] | None = None,
    surface_aliases: dict[str, dict[str, str | list[str]]] | None = None,
    surface_omissions: dict[str, dict[str, str]] | None = None,
    conditional_steps: dict[str, str] | None = None,
    dependency_helpers: list[dict[str, Any]] | None = None,
    delta_only_categories: set[str] | None = None,
    allow_inapplicable_policy: bool = False,
) -> dict[str, Any]:
    base_text = split_audit.ref_text(base, source)
    incoming_text = split_audit.ref_text(incoming, source)
    destination_text = "\n".join(path.read_text() for path in destination_paths)
    added = added_surface(base_text, incoming_text)
    incoming_registrations = incoming_surface(incoming_text)
    current = current_surface(
        destination_text,
        helper_arguments,
        dependency_helpers,
    )
    if runtime_steps is not None:
        # The instantiated graph is authoritative. Static `.name` fields are
        # needed when no Zig build is available, but they can also name
        # artifacts or descriptors that never become invokable build steps.
        current["steps"] = set(runtime_steps)
    covered = {category: set(values) for category, values in current.items()}
    inapplicable_policy: dict[str, list[str]] = {
        "aliases": [],
        "conditional_steps": [],
        "omissions": [],
    }
    direct_static_steps = names(INCOMING_PATTERNS["steps"], destination_text)
    for step_name, reason in (conditional_steps or {}).items():
        if (
            not isinstance(step_name, str)
            or not step_name
            or not isinstance(reason, str)
            or not reason
        ):
            raise ValueError(
                "conditional build steps must map non-empty names to non-empty reasons"
            )
        if step_name not in incoming_registrations["steps"]:
            if allow_inapplicable_policy:
                inapplicable_policy["conditional_steps"].append(step_name)
                continue
            raise ValueError(f"stale conditional build step: {step_name}")
        if step_name not in direct_static_steps:
            raise ValueError(
                f"conditional build step has no direct b.step registration: {step_name}"
            )
        covered["steps"].add(step_name)
    for category, aliases in (surface_aliases or {}).items():
        if category not in incoming_registrations:
            raise ValueError(f"unknown build surface alias category: {category}")
        for source_name, raw_targets in aliases.items():
            targets = [raw_targets] if isinstance(raw_targets, str) else raw_targets
            if (
                not isinstance(source_name, str)
                or not source_name
                or not isinstance(targets, list)
                or not targets
                or not all(isinstance(target, str) and target for target in targets)
            ):
                raise ValueError(
                    "build surface aliases must map non-empty names to a "
                    "non-empty string or list of strings"
                )
            if source_name not in incoming_registrations[category]:
                if allow_inapplicable_policy:
                    inapplicable_policy["aliases"].append(
                        f"{category}.{source_name}"
                    )
                    continue
                raise ValueError(f"stale build surface alias source: {category}.{source_name}")
            missing_targets = [target for target in targets if target not in current[category]]
            if missing_targets:
                raise ValueError(
                    f"build surface alias {category}.{source_name} has missing targets: "
                    + ", ".join(missing_targets)
                )
            covered[category].add(source_name)
    for category, omissions in (surface_omissions or {}).items():
        if category not in incoming_registrations:
            raise ValueError(f"unknown build surface omission category: {category}")
        for source_name, reason in omissions.items():
            if (
                not isinstance(source_name, str)
                or not source_name
                or not isinstance(reason, str)
                or not reason
            ):
                raise ValueError(
                    "build surface omissions must map non-empty names to "
                    "non-empty reasons"
                )
            if source_name not in incoming_registrations[category]:
                if allow_inapplicable_policy:
                    inapplicable_policy["omissions"].append(
                        f"{category}.{source_name}"
                    )
                    continue
                raise ValueError(f"stale build surface omission: {category}.{source_name}")
            covered[category].add(source_name)
    missing = {
        category: sorted(values - covered[category])
        for category, values in added.items()
    }
    raw_missing_incoming = {
        category: sorted(values - covered[category])
        for category, values in incoming_registrations.items()
    }
    unknown_delta_only = set(delta_only_categories or ()) - set(incoming_registrations)
    if unknown_delta_only:
        raise ValueError(
            "unknown delta-only build surface categories: "
            + ", ".join(sorted(unknown_delta_only))
        )
    carried_not_required = {
        category: values
        for category, values in raw_missing_incoming.items()
        if category in (delta_only_categories or set()) and values
    }
    missing_incoming = {
        category: ([] if category in (delta_only_categories or set()) else values)
        for category, values in raw_missing_incoming.items()
    }
    return {
        "base": split_audit.resolve_ref(base),
        "incoming_ref": split_audit.resolve_ref(incoming),
        "source": source,
        "destinations": [str(path.relative_to(ROOT)) for path in destination_paths],
        "runtime_step_inventory": runtime_steps is not None,
        "surface_aliases": surface_aliases or {},
        "surface_omissions": surface_omissions or {},
        "conditional_steps": conditional_steps or {},
        "inapplicable_policy": inapplicable_policy,
        "delta_only_categories": sorted(delta_only_categories or set()),
        "carried_not_required": carried_not_required,
        "added": {key: sorted(value) for key, value in added.items()},
        "missing": missing,
        "incoming": {
            key: sorted(value) for key, value in incoming_registrations.items()
        },
        "missing_incoming": missing_incoming,
        "summary": {
            "added": sum(len(value) for value in added.values()),
            "missing": sum(len(value) for value in missing.values()),
            "incoming": sum(
                len(value) for value in incoming_registrations.values()
            ),
            "missing_incoming": sum(
                len(value) for value in missing_incoming.values()
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--incoming", required=True)
    parser.add_argument("--policy", required=True, type=pathlib.Path)
    parser.add_argument("--migration", default="zig_build")
    parser.add_argument("--json-out", required=True, type=pathlib.Path)
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument(
        "--include-carried",
        action="store_true",
        help="also fail on any incoming registration absent from the split tree",
    )
    parser.add_argument(
        "--retention-ref",
        help=(
            "also require every registration changed or carried by this pre-merge "
            "branch ref; policy entries absent from that historical ref are reported "
            "as inapplicable instead of stale"
        ),
    )
    parser.add_argument(
        "--zig-build-dir",
        action="append",
        type=pathlib.Path,
        help="verify steps with an authoritative `zig build -l -fincremental` inventory; repeat for moved cross-build registrations",
    )
    args = parser.parse_args()

    policy_path = args.policy if args.policy.is_absolute() else ROOT / args.policy
    try:
        policy = load_json_file(policy_path)
        source, raw_destinations = split_audit.split_migration(
            policy,
            args.migration,
        )
        migration = policy["split_migrations"][args.migration]
        helper_arguments = migration.get("build_surface_helpers", {})
        if not isinstance(helper_arguments, dict):
            raise ValueError("build_surface_helpers must be an object")
        dependency_helpers = migration.get("build_surface_dependency_helpers", [])
        if not isinstance(dependency_helpers, list):
            raise ValueError("build_surface_dependency_helpers must be a list")
        surface_aliases = migration.get("build_surface_aliases", {})
        if not isinstance(surface_aliases, dict):
            raise ValueError("build_surface_aliases must be an object")
        retention_surface_aliases = migration.get(
            "build_surface_retention_aliases", {}
        )
        if not isinstance(retention_surface_aliases, dict):
            raise ValueError("build_surface_retention_aliases must be an object")
        surface_omissions = migration.get("build_surface_omissions", {})
        if not isinstance(surface_omissions, dict):
            raise ValueError("build_surface_omissions must be an object")
        conditional_steps = migration.get("build_surface_conditional_steps", {})
        if not isinstance(conditional_steps, dict):
            raise ValueError("build_surface_conditional_steps must be an object")
        delta_only_categories = migration.get(
            "build_surface_delta_only_categories", []
        )
        if not isinstance(delta_only_categories, list) or not all(
            isinstance(category, str) and category
            for category in delta_only_categories
        ):
            raise ValueError("build_surface_delta_only_categories must be a string list")
        companion_globs = migration.get("build_companion_globs", [])
        if not isinstance(companion_globs, list):
            raise ValueError("build_companion_globs must be a list")
        destination_paths = split_audit.expand_destinations(raw_destinations)
        runtime_steps = None
        if args.zig_build_dir is not None:
            runtime_steps = set()
            for raw_build_dir in args.zig_build_dir:
                build_dir = (
                    raw_build_dir
                    if raw_build_dir.is_absolute()
                    else ROOT / raw_build_dir
                ).resolve()
                try:
                    build_dir.relative_to(ROOT)
                except ValueError as exc:
                    raise ValueError("zig build directory must be inside the repository") from exc
                runtime_steps.update(runtime_step_inventory(build_dir))
        report = build_report(
            args.base,
            args.incoming,
            source,
            destination_paths,
            helper_arguments,
            runtime_steps,
            surface_aliases,
            surface_omissions,
            conditional_steps,
            dependency_helpers,
            set(delta_only_categories),
        )
        companions = companion_report(
            args.base,
            args.incoming,
            source,
            companion_globs,
            policy,
        )
        report["companions"] = companions
        report["summary"]["unresolved_companions"] = companions["summary"][
            "unresolved"
        ]
        if args.retention_ref is not None:
            combined_retention_aliases = merge_surface_aliases(
                surface_aliases,
                retention_surface_aliases,
            )
            retention = build_report(
                args.base,
                args.retention_ref,
                source,
                destination_paths,
                helper_arguments,
                runtime_steps,
                combined_retention_aliases,
                surface_omissions,
                conditional_steps,
                dependency_helpers,
                set(),
                allow_inapplicable_policy=True,
            )
            retention_companions = companion_report(
                args.base,
                args.retention_ref,
                source,
                companion_globs,
                policy,
            )
            retention["companions"] = retention_companions
            retention["summary"]["unresolved_companions"] = (
                retention_companions["summary"]["unresolved"]
            )
            report["retention"] = retention
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for category, values in report["missing"].items():
        if values:
            print(f"{category}: {len(values)} missing")
            for value in values:
                print(f"  {value}")
    if args.include_carried:
        for category, values in report["missing_incoming"].items():
            if values:
                print(f"all-incoming {category}: {len(values)} missing")
                for value in values:
                    print(f"  {value}")
    for item in report["companions"]["files"]:
        if item["status"] in {
            "divergent_unowned",
            "missing_current",
            "retained_deleted_unowned",
        }:
            print(f"companion {item['status']}: {item['path']}")
    retention = report.get("retention")
    if retention is not None:
        for category, values in retention["missing_incoming"].items():
            if values:
                print(f"retention {category}: {len(values)} missing")
                for value in values:
                    print(f"  {value}")
        for item in retention["companions"]["files"]:
            if item["status"] in {
                "divergent_unowned",
                "missing_current",
                "retained_deleted_unowned",
            }:
                print(f"retention companion {item['status']}: {item['path']}")
    print(f"build surface report: {args.json_out}")
    failed = report["summary"]["missing"] or (
        args.include_carried and report["summary"]["missing_incoming"]
    ) or report["summary"]["unresolved_companions"]
    if retention is not None:
        failed = failed or retention["summary"]["missing_incoming"] or retention[
            "summary"
        ]["unresolved_companions"]
    if failed and not args.report_only:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
