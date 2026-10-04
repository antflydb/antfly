#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Audit the local engine's authored production imports, before its package move.

The default check follows authored relative imports. Build targets additionally
pass their actual named module tables, resolving each import in its source owner.
Test bodies and explicit test-only owners are excluded; target import alternatives
are selected only when the target proves the condition. Missing sources fail rather than disappear
from the inventory. It deliberately does not infer licensing from directory names.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

LITERALS = re.compile(
    r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|//[^\n]*|(?m:^[ \t]*\\\\[^\n]*)'
)
IMPORT = re.compile(r'@import\s*\(\s*"([^"\n]+)"\s*\)')
FORBIDDEN = ("raft/", "data/", "standalone/", "cmd/", "storage/hot_standby/")
SERVER_METADATA = {
    "api.zig",
    "server.zig",
    "table_provisioner.zig",
    "provision_contract.zig",
}


def mask_literals(source: str) -> str:
    return LITERALS.sub(
        lambda m: "".join("\n" if c == "\n" else " " for c in m[0]), source
    )


def production_exclusions(
    source: str, *, target_os: str | None = None, options: dict[str, bool] | None = None
) -> tuple[str, list[tuple[int, int]]]:
    """Skip comments, strings and complete test bodies, preserving lazy imports."""
    masked = mask_literals(source)
    stack: list[int] = []
    ends: dict[int, int] = {}
    for offset, char in enumerate(masked):
        if char == "{":
            stack.append(offset)
        elif char == "}":
            if not stack:
                raise ValueError("unbalanced Zig braces")
            ends[stack.pop()] = offset + 1
    if stack:
        raise ValueError("unbalanced Zig braces")
    excluded = []
    for test in re.finditer(r"\btest\s*\{", masked):
        brace = masked.index("{", test.start(), test.end())
        excluded.append((test.start(), ends[brace]))
    for container in re.finditer(
        r"\bif\s*\(\s*builtin\.is_test\s*\)\s*struct\s*\{", masked
    ):
        brace = masked.index("{", container.start(), container.end())
        excluded.append((brace, ends[brace]))
    # The false branch must be an empty owner: a production call then fails
    # compilation instead of silently reaching a server fixture.
    test_owner = re.compile(
        r"\bconst\s+\w+\s*=\s*if\s*\(\s*builtin\.is_test\s*\)"
        r'\s*@import\(\s*"[^"\n]+"\s*\)\s*else\s*struct\s*\{\s*\}\s*;'
    )
    for owner in test_owner.finditer(source):
        if masked[owner.start() : owner.start() + 5] == "const":
            excluded.append((owner.start(), owner.end()))
    for test_import in re.finditer(
        r'\bif\s*\(\s*builtin\.is_test\s*\)\s*(@import\(\s*"[^"\n]+"\s*\))\s*else\b',
        source,
    ):
        if masked[test_import.start() : test_import.start() + 2] == "if":
            excluded.append((test_import.start(1), test_import.end(1)))
    if target_os:

        def skip_space(offset: int) -> int:
            while offset < len(masked) and masked[offset].isspace():
                offset += 1
            return offset

        def expression_end(offset: int) -> int | None:
            if masked.startswith("struct", offset):
                brace = skip_space(offset + len("struct"))
                if brace < len(masked) and masked[brace] == "{":
                    return ends[brace]
            imported = IMPORT.match(source, offset)
            if imported:
                suffix = re.match(r"(?:\.\w+)*", source[imported.end() :])
                return imported.end() + len(suffix[0])
            return None

        for conditional in re.finditer(r"\bif\s*\(", masked):
            opening = masked.index("(", conditional.start(), conditional.end())
            depth, closing = 1, opening + 1
            while closing < len(masked) and depth:
                depth += (masked[closing] == "(") - (masked[closing] == ")")
                closing += 1
            condition = source[opening + 1 : closing - 1].replace(
                '@import("builtin")', "builtin"
            )
            terms = []
            for term in re.split(r"\s+or\s+", condition):
                term = term.strip().removeprefix("comptime ")
                os_check = re.fullmatch(r"builtin\.os\.tag\s*(==|!=)\s*\.(\w+)", term)
                if os_check:
                    terms.append((target_os == os_check[2]) == (os_check[1] == "=="))
                elif term == "builtin.is_test":
                    terms.append(False)
                elif options is not None and term.startswith("build_options."):
                    terms.append(options.get(term.removeprefix("build_options.")))
                else:
                    terms.append(None)
            value = (
                True
                if True in terms
                else False
                if all(term is False for term in terms)
                else None
            )
            if value is None:
                continue
            first = skip_space(closing)
            first_end = expression_end(first)
            if first_end is None:
                continue
            otherwise = skip_space(first_end)
            if not re.match(r"else\b", masked[otherwise:]):
                continue
            second = skip_space(otherwise + 4)
            second_end = expression_end(second)
            if second_end is not None:
                excluded.append((second, second_end) if value else (first, first_end))
    if options is not None:
        for guard in re.finditer(
            r"\bif\s*\(\s*(?:comptime\s+)?(!?)build_options\.(\w+)\s*\)\s*return(?:\s+[^;]*)?;",
            masked,
        ):
            # Only a standalone statement proves the rest of this block dead.
            # An unbraced runtime if/loop, else arm, or expression can decide
            # whether the guard runs at all. Unknown syntax stays visible.
            preceding = masked[: guard.start()].rstrip()
            if not preceding or preceding[-1] not in "{;}":
                continue
            value = options.get(guard[2])
            if value is None or (not value if guard[1] else value) is not True:
                continue
            containers = [
                (start, end)
                for start, end in ends.items()
                if start < guard.start() < end
            ]
            if containers:
                _, end = min(containers, key=lambda pair: pair[1] - pair[0])
                excluded.append((guard.end(), end))
    return masked, excluded


def production_source(source: str) -> str:
    """Preserve source offsets while removing explicitly test-owned code."""
    _, excluded = production_exclusions(source)
    chars = list(source)
    for start, end in excluded:
        for offset in range(start, end):
            if chars[offset] != "\n":
                chars[offset] = " "
    return "".join(chars)


def production_imports(
    source: str,
    *,
    include_named: bool = False,
    target_os: str | None = None,
    options: dict[str, bool] | None = None,
) -> list[str]:
    masked, excluded = production_exclusions(
        source, target_os=target_os, options=options
    )
    result = []
    for call in re.finditer(r"@import\b", masked):
        if any(start <= call.start() < end for start, end in excluded):
            continue
        match = IMPORT.match(source, call.start())
        if match is None:
            raise ValueError("production imports must declare a literal source owner")
        if include_named or match[1].endswith(".zig"):
            result.append(match[1])
    return result


def server_source(relative: str) -> bool:
    return (
        relative.startswith(FORBIDDEN)
        or relative.startswith("storage/server_")
        or (
            relative.startswith("metadata/")
            and (
                relative.removeprefix("metadata/") in SERVER_METADATA
                or relative.startswith("metadata/storage/")
            )
        )
        or relative
        in {
            "server_db_integration_test_root.zig",
            "system_catalog/server_call.zig",
            "storage/server_db_adapter.zig",
            "storage/metadata_hot_standby_port.zig",
            "tracing/server_raft_writer.zig",
            "tracing/raft_trace_logger.zig",
            "tracing/mod.zig",
            "capi/server_owner.zig",
            "capi_root.zig",
            "capi_dependencies.zig",
        }
    )


def check_local_observation_contracts(relative: str, source: str) -> None:
    """Keep routing identity and destination policy out of local event/planning ports."""
    production = production_source(source)
    masked = mask_literals(production)
    if relative == "storage/db/db.zig":
        hook = re.search(
            r"\bpub\s+const\s+QueryVisibilityHook\s*=\s*struct\s*\{(.*?)\n\};",
            masked,
            re.DOTALL,
        )
        if hook and {"table_name", "group_id", "DB"}.intersection(
            re.findall(r"\b\w+\b", hook[1])
        ):
            raise ValueError(
                f"{relative} exposes server routing in local visibility hook"
            )
    if relative == "storage/db/document_child_range_effects.zig":
        if re.search(r"\broute_status\b", masked) or any(
            match[0] == '"remote_committed"' for match in LITERALS.finditer(production)
        ):
            raise ValueError(
                f"{relative} interprets server child-range destination policy"
            )


def check_replication_contract(relative: str, source: str) -> None:
    """Reject server policy leaking back through the engine's borrowed ports."""
    check_local_observation_contracts(relative, source)
    if relative not in {
        "storage/db/replication_contract.zig",
        "storage/db/commit_integration.zig",
        "storage/db/db.zig",
        "storage/db/document_child_range_effects.zig",
        "storage/db/document_child_range_outbox.zig",
        "storage/db/portable_activation_recovery.zig",
        "storage/db/quarantine_recovery.zig",
        "storage/db/independent_maintenance.zig",
        "storage/db/index_repair_scheduler.zig",
        "storage/db/graph_cleanup_owner.zig",
        "storage/db/native_projection_owner.zig",
        "storage/db/runtime_restart_owner.zig",
    }:
        return
    source = mask_literals(production_source(source))
    forbidden = {
        "sync_policy",
        "standby_names",
        "sync_wait_fn",
        "sync_wait_ctx",
        "failure_count",
        "last_gate_action",
        "sync_reject_count",
        "sync_degraded_count",
        "is_standby",
        "isStandbyRole",
        "fenced_primary",
        "FencedWriteGate",
        "RaftAppliedEntryIdentity",
        "raft_applied_entry_marker",
        "HAMirrorUnavailable",
        "primary_ha",
        "getGroupCreatedAtMillis",
        "ensureGroupCreatedAtMillis",
        "groupCreatedAtMetadataKeyAlloc",
    }
    found = forbidden.intersection(re.findall(r"\b\w+\b", source))
    if found:
        raise ValueError(
            f"{relative} exposes server replication policy: {', '.join(sorted(found))}"
        )


def audit(root: Path, entries: list[str]) -> dict[str, list[str]]:
    root = root.resolve()
    pending = collections.deque(root / entry for entry in entries)
    parents: dict[Path, Path | None] = {path: None for path in pending}
    graph: dict[str, list[str]] = {}
    while pending:
        path = pending.popleft()
        relative = path.relative_to(root).as_posix()
        if server_source(relative):
            chain = []
            cursor: Path | None = path
            while cursor is not None:
                chain.append(cursor.relative_to(root).as_posix())
                cursor = parents[cursor]
            raise ValueError(
                "engine imports server coordination: " + " -> ".join(reversed(chain))
            )
        source = path.read_text()
        check_replication_contract(relative, source)
        dependencies = []
        for imported in production_imports(source):
            dependency = (path.parent / imported).resolve()
            if not dependency.is_relative_to(root):
                raise ValueError(
                    f"{relative} imports outside its source owner: {imported}"
                )
            if not dependency.is_file():
                raise ValueError(f"{relative} imports missing source: {imported}")
            dependencies.append(dependency.relative_to(root).as_posix())
            if dependency not in parents:
                parents[dependency] = path
                pending.append(dependency)
        graph[relative] = dependencies
    return graph


def audit_modules(
    project: Path,
    modules: dict[str, Path],
    edges: dict[tuple[str, str], str],
    entry: str,
    target_os: str | None = None,
    external_modules: set[str] | None = None,
) -> int:
    """Resolve source imports against the actual target's Build.Module table."""
    project = project.resolve()
    source_root = project / "pkg/antfly/src"
    external_modules = external_modules or set()
    unknown = external_modules.difference(modules)
    if unknown:
        raise ValueError(f"unknown external module owners: {sorted(unknown)}")
    if entry not in modules or any(
        owner not in modules or target not in modules
        for (owner, _), target in edges.items()
    ):
        raise ValueError("module graph references an unknown source owner")
    pending = collections.deque([(entry, modules[entry].resolve(), [])])
    visited: set[tuple[str, Path]] = set()
    while pending:
        module, path, chain = pending.popleft()
        if (module, path) in visited:
            continue
        visited.add((module, path))
        if path.is_relative_to(source_root) and server_source(
            path.relative_to(source_root).as_posix()
        ):
            raise ValueError(
                "embedded module imports server coordination: "
                + " -> ".join(chain + [str(path)])
            )
        if path.is_relative_to(source_root):
            check_replication_contract(
                path.relative_to(source_root).as_posix(), path.read_text()
            )
        # Exempt only dependency-owned modules declared by the build, never
        # Antfly-generated sources merely because their cache is external.
        if module in external_modules and not path.is_relative_to(project):
            # A dependency may be configured with Antfly-owned named imports.
            # Preserve its ownership graph even while excluding its sources,
            # including re-entry through another dependency-owned module.
            for (owner, _), target in edges.items():
                if owner == module:
                    pending.append(
                        (target, modules[target].resolve(), chain + [str(path)])
                    )
            continue
        if not path.is_file():
            raise ValueError(f"missing module source: {path}")
        options = {}
        option_owner = edges.get((module, "build_options"))
        if option_owner in modules and modules[option_owner].is_file():
            options = {
                name: value == "true"
                for name, value in re.findall(
                    r"pub const (\w+): bool = (true|false);",
                    modules[option_owner].read_text(),
                )
            }
        for imported in production_imports(
            path.read_text(), include_named=True, target_os=target_os, options=options
        ):
            if imported in {"std", "builtin"}:
                continue
            next_chain = chain + [str(path)]
            if imported.endswith(".zig"):
                dependency = (path.parent / imported).resolve()
                # Generated sibling files belong to their declared module's
                # source directory. Other external imports need a named owner.
                if not dependency.is_relative_to(
                    project
                ) and not dependency.is_relative_to(modules[module].resolve().parent):
                    raise ValueError(
                        f"{path} imports outside Antfly's source owner: {imported}"
                    )
                pending.append((module, dependency, next_chain))
            else:
                target = entry if imported == "root" else edges.get((module, imported))
                if target is None or target not in modules:
                    raise ValueError(
                        f"unresolved module import {imported!r} in {path} (owner {module})"
                    )
                pending.append((target, modules[target].resolve(), next_chain))
    return len(visited)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "pkg/antfly/src",
    )
    parser.add_argument("--entry", action="append")
    parser.add_argument("--json", type=Path)
    parser.add_argument("--project", type=Path)
    parser.add_argument("--module", nargs=2, action="append", default=[])
    parser.add_argument("--module-import", nargs=3, action="append", default=[])
    parser.add_argument("--external-module", action="append", default=[])
    parser.add_argument("--entry-module")
    parser.add_argument("--target-os")
    args = parser.parse_args()
    try:
        if args.entry_module:
            count = audit_modules(
                args.project or Path(__file__).resolve().parents[1],
                {name: Path(path) for name, path in args.module},
                {(owner, name): target for owner, name, target in args.module_import},
                args.entry_module,
                args.target_os,
                set(args.external_module),
            )
            print(
                f"Embedded module boundary: {count} resolved sources, no server coordination imports."
            )
            return
        graph = audit(
            args.root, args.entry or ["embedded_root.zig", "storage/db/db.zig"]
        )
    except (ValueError, OSError) as error:
        parser.exit(1, f"{error}\n")
    if args.json:
        args.json.write_text(json.dumps(graph, indent=2) + "\n")
    print(
        f"Embedded production boundary: {len(graph)} sources, no server coordination imports."
    )


if __name__ == "__main__":
    main()
