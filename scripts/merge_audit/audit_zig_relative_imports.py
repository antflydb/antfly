#!/usr/bin/env python3
"""Validate file-relative Zig imports across manifest-declared split owners."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
import json
import pathlib
import subprocess
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.merge_audit import audit_split_declarations as split_audit
from scripts.merge_audit import audit_split_migrations as split_migrations
from scripts.merge_audit.audit_main_capture import load_json_file


@dataclass(frozen=True)
class ImportReference:
    value: str
    line: int


@dataclass(frozen=True)
class ImportFinding:
    migration: str
    source: str
    line: int
    imported: str
    resolved: str
    status: str
    reason: str | None = None


def scan_file_imports(text: str) -> list[ImportReference]:
    """Return uncommented @import string literals that name Zig files."""
    imports: list[ImportReference] = []
    index = 0
    line = 1
    length = len(text)

    while index < length:
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            if newline == -1:
                break
            index = newline
            continue
        if text.startswith("\\\\", index):
            line_start = text.rfind("\n", 0, index) + 1
            if not text[line_start:index].strip():
                newline = text.find("\n", index + 2)
                if newline == -1:
                    break
                index = newline
                continue
        if text[index] in {'"', "'"}:
            quote = text[index]
            index += 1
            while index < length:
                if text[index] == "\n":
                    line += 1
                if text[index] == "\\":
                    index += 2
                    continue
                if text[index] == quote:
                    index += 1
                    break
                index += 1
            continue
        if text[index] == "\n":
            line += 1
            index += 1
            continue
        if not text.startswith("@import", index):
            index += 1
            continue

        import_line = line
        cursor = index + len("@import")
        while cursor < length and text[cursor].isspace():
            cursor += 1
        if cursor >= length or text[cursor] != "(":
            index += 1
            continue
        cursor += 1
        while cursor < length and text[cursor].isspace():
            cursor += 1
        if cursor >= length or text[cursor] != '"':
            index += 1
            continue
        cursor += 1
        value_start = cursor
        escaped = False
        while cursor < length and text[cursor] != '"':
            if text[cursor] == "\\":
                escaped = True
                cursor += 2
                continue
            cursor += 1
        if cursor >= length:
            index += 1
            continue
        value = text[value_start:cursor]
        if value.endswith(".zig"):
            imports.append(ImportReference(value=value, line=import_line))
        line += text[index : cursor + 1].count("\n")
        index = cursor + 1

    return imports


def load_exception_list(
    label: str,
    raw: Any,
) -> dict[tuple[str, str], str]:
    if not isinstance(raw, list):
        raise ValueError(
            f"{label} relative import exceptions must be a list"
        )
    exceptions: dict[tuple[str, str], str] = {}
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"path", "import", "reason"}:
            raise ValueError(
                f"{label} relative import exceptions must contain "
                "exactly path, import, and reason"
            )
        path = item.get("path")
        imported = item.get("import")
        reason = item.get("reason")
        if not all(isinstance(value, str) and value for value in (path, imported, reason)):
            raise ValueError(
                f"{label} relative import exceptions require "
                "non-empty path, import, and reason strings"
            )
        key = (path, imported)
        if key in exceptions:
            raise ValueError(
                f"{label} has duplicate relative import exception: "
                f"{path}: {imported}"
            )
        exceptions[key] = reason
    return exceptions


def load_exceptions(
    migration: str,
    config: dict[str, Any],
) -> dict[tuple[str, str], str]:
    return load_exception_list(
        f"split migration {migration}",
        config.get("relative_import_exceptions", []),
    )


def merge_exceptions(
    destination: dict[tuple[str, str], str],
    incoming: dict[tuple[str, str], str],
    label: str,
) -> None:
    for key, reason in incoming.items():
        previous = destination.get(key)
        if previous is not None and previous != reason:
            raise ValueError(
                f"conflicting relative import exceptions for {key[0]}: "
                f"{key[1]} ({label})"
            )
        destination[key] = reason


def repository_zig_paths(raw_roots: list[str]) -> list[pathlib.Path]:
    if not raw_roots or any(
        not isinstance(raw, str)
        or not raw
        or pathlib.PurePosixPath(raw).is_absolute()
        or ".." in pathlib.PurePosixPath(raw).parts
        for raw in raw_roots
    ):
        raise ValueError(
            "zig_relative_import_roots must be a non-empty list of repository paths"
        )
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            *raw_roots,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git ls-files failed: {result.stderr.strip()}")
    paths = {
        (ROOT / name).resolve()
        for name in result.stdout.splitlines()
        if name.endswith(".zig") and (ROOT / name).is_file()
    }
    if not paths:
        raise ValueError(
            "zig_relative_import_roots contain no tracked or untracked Zig files"
        )
    return sorted(paths)


def audit_paths(
    migration: str,
    paths: list[pathlib.Path],
    exceptions: dict[tuple[str, str], str],
    root: pathlib.Path = ROOT,
) -> tuple[list[ImportFinding], list[dict[str, str]]]:
    findings: list[ImportFinding] = []
    used_exceptions: set[tuple[str, str]] = set()
    resolved_root = root.resolve()

    for path in paths:
        source = str(path.resolve().relative_to(resolved_root))
        for imported in scan_file_imports(path.read_text()):
            target = (path.parent / imported.value).resolve()
            try:
                resolved = str(target.relative_to(resolved_root))
                status = "present" if target.is_file() else "missing"
            except ValueError:
                resolved = str(target)
                status = "escapes_repository"

            key = (source, imported.value)
            reason = exceptions.get(key)
            if status != "present" and reason is not None:
                status = "excepted"
                used_exceptions.add(key)
            findings.append(
                ImportFinding(
                    migration=migration,
                    source=source,
                    line=imported.line,
                    imported=imported.value,
                    resolved=resolved,
                    status=status,
                    reason=reason,
                )
            )

    unused = [
        {"path": path, "import": imported, "reason": reason}
        for (path, imported), reason in sorted(exceptions.items())
        if (path, imported) not in used_exceptions
    ]
    return findings, unused


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", required=True, type=pathlib.Path)
    parser.add_argument(
        "--migration",
        action="append",
        default=[],
        help=(
            "audit only this split migration; repeat as needed "
            "(default: manifest repository roots)"
        ),
    )
    parser.add_argument("--json-out", required=True, type=pathlib.Path)
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()

    policy_path = args.policy if args.policy.is_absolute() else ROOT / args.policy
    try:
        policy = load_json_file(policy_path)
        migration_configs = policy["split_migrations"]
        all_findings: list[ImportFinding] = []
        unused_exceptions: dict[str, list[dict[str, str]]] = {}
        audited_files: set[str] = set()
        if args.migration:
            scopes = []
            migrations = split_migrations.load_migration_names(
                policy,
                args.migration,
            )
            for migration in migrations:
                config = migration_configs[migration]
                _, raw_destinations = split_audit.split_migration(
                    policy,
                    migration,
                )
                scopes.append(
                    (
                        migration,
                        split_audit.expand_destinations(raw_destinations),
                        load_exceptions(migration, config),
                    )
                )
        elif policy.get("zig_relative_import_roots"):
            migrations = []
            exceptions = load_exception_list(
                "repository",
                policy.get("zig_relative_import_exceptions", []),
            )
            for migration, config in migration_configs.items():
                merge_exceptions(
                    exceptions,
                    load_exceptions(migration, config),
                    migration,
                )
            scopes = [
                (
                    "repository",
                    repository_zig_paths(policy["zig_relative_import_roots"]),
                    exceptions,
                )
            ]
        else:
            migrations = split_migrations.load_migration_names(policy, [])
            scopes = []
            for migration in migrations:
                config = migration_configs[migration]
                _, raw_destinations = split_audit.split_migration(
                    policy,
                    migration,
                )
                scopes.append(
                    (
                        migration,
                        split_audit.expand_destinations(raw_destinations),
                        load_exceptions(migration, config),
                    )
                )

        for scope, destination_paths, exceptions in scopes:
            audited_files.update(
                str(path.resolve().relative_to(ROOT)) for path in destination_paths
            )
            findings, unused = audit_paths(
                scope,
                destination_paths,
                exceptions,
            )
            all_findings.extend(findings)
            if unused:
                unused_exceptions[scope] = unused
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))

    statuses = Counter(finding.status for finding in all_findings)
    unresolved = [
        finding
        for finding in all_findings
        if finding.status in {"missing", "escapes_repository"}
    ]
    report = {
        "policy": str(policy_path.resolve().relative_to(ROOT)),
        "migrations": migrations,
        "scopes": [scope for scope, _, _ in scopes],
        "audited_files": sorted(audited_files),
        "findings": [asdict(finding) for finding in all_findings],
        "unused_exceptions": unused_exceptions,
        "summary": {
            "files": len(audited_files),
            "imports": len(all_findings),
            "unresolved": len(unresolved),
            "unused_exceptions": sum(
                len(items) for items in unused_exceptions.values()
            ),
            "statuses": dict(sorted(statuses.items())),
        },
    }
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    for finding in unresolved:
        print(
            f"{finding.migration}: {finding.source}:{finding.line}: "
            f"{finding.status}: {finding.imported} -> {finding.resolved}"
        )
    for migration, items in sorted(unused_exceptions.items()):
        for item in items:
            print(f"{migration}: unused exception: {item['path']}: {item['import']}")
    print(f"relative import report: {args.json_out}")
    if (unresolved or unused_exceptions) and not args.report_only:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
