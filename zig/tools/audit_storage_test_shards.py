#!/usr/bin/env python3
"""Audit authoritative storage-test discovery and exactly-one shard ownership."""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, Sequence


TEST_DECLARATION = re.compile(r'(?m)^\s*(?:pub\s+)?test(?:\s|")')
QUOTED_TEST_DECLARATION = re.compile(r'(?m)^\s*(?:pub\s+)?test\s+"([^"]+)"')
MANIFEST_IMPORT = re.compile(r'(?m)^\s*_\s*=\s*@import\("([^"]+\.zig)"\);\s*$')


def module_names(root: Path, source: Path) -> tuple[str, ...]:
    relative = source.relative_to(root).with_suffix("")
    parts = ("storage", *relative.parts)
    names = [".".join(parts) + "."]
    if relative.name == "mod":
        names.append(".".join(parts[:-1]) + ".")
    return tuple(names)


def test_modules(root: Path) -> Iterable[tuple[Path, tuple[str, ...]]]:
    for source in sorted(root.rglob("*.zig")):
        if source.name == "test_manifest.zig":
            continue
        if TEST_DECLARATION.search(source.read_text(encoding="utf-8")):
            yield source, module_names(root, source)


def manifest_sources(root: Path, manifest: Path) -> list[Path]:
    imports = MANIFEST_IMPORT.findall(manifest.read_text(encoding="utf-8"))
    return [(manifest.parent / imported).resolve() for imported in imports]


def audit_manifest(root: Path, manifest: Path, filters: Sequence[str]) -> list[str]:
    root = root.resolve()
    manifest = manifest.resolve()
    declared = {source.resolve(): names for source, names in test_modules(root)}
    imported = manifest_sources(root, manifest)
    counts = Counter(imported)
    failures: list[str] = []

    for source, count in sorted(counts.items(), key=lambda item: str(item[0])):
        try:
            relative = source.relative_to(root)
        except ValueError:
            failures.append(f"manifest import escapes storage root: {source}")
            continue
        if not source.is_file():
            failures.append(f"manifest import does not exist: {relative}")
            continue
        if count != 1:
            failures.append(f"manifest imports {relative} {count} times")
        if source not in declared:
            failures.append(f"manifest entry has no test declarations: {relative}")

    for source, names in sorted(declared.items(), key=lambda item: str(item[0])):
        relative = source.relative_to(root)
        count = counts.get(source, 0)
        if count == 0:
            failures.append(f"test source missing from manifest: {relative}")
            continue
        owners = [test_filter for test_filter in filters if any(test_filter in name for name in names)]
        if len(owners) == 0:
            failures.append(f"test source has no shard owner: {relative} ({', '.join(names)})")
        elif len(owners) > 1:
            failures.append(
                f"test source has multiple shard owners: {relative} ({', '.join(owners)})"
            )

    return failures


def audit_runtime_partition(source: Path, filters: Sequence[str]) -> list[str]:
    """Verify a name-filtered lane and its complement both retain tests."""
    if not source.is_file():
        return [f"runtime partition source does not exist: {source}"]

    names = QUOTED_TEST_DECLARATION.findall(source.read_text(encoding="utf-8"))
    failures: list[str] = []
    for test_filter in filters:
        # The custom runner applies filters containing a dot to fully-qualified
        # names. Lane filters deliberately target declared names so module path
        # changes cannot silently alter the partition.
        if "." in test_filter:
            failures.append(
                f"runtime partition filter must target a declared name: {test_filter}"
            )
        elif not any(test_filter in name for name in names):
            failures.append(f"runtime partition filter matches no tests: {test_filter}")

    selected = [name for name in names if any(test_filter in name for test_filter in filters)]
    if not selected:
        failures.append("runtime partition selects no tests")
    elif len(selected) == len(names):
        failures.append("runtime partition leaves no tests for its complement")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--filter", action="append", default=[])
    parser.add_argument("--runtime-partition-source", type=Path)
    parser.add_argument("--runtime-partition-filter", action="append", default=[])
    args = parser.parse_args()
    if not args.filter:
        parser.error("at least one --filter is required")
    if bool(args.runtime_partition_source) != bool(args.runtime_partition_filter):
        parser.error(
            "--runtime-partition-source and --runtime-partition-filter must be used together"
        )

    failures = audit_manifest(args.root, args.manifest, args.filter)
    if args.runtime_partition_source:
        failures.extend(
            audit_runtime_partition(
                args.runtime_partition_source,
                args.runtime_partition_filter,
            )
        )
    if not failures:
        return 0
    print("storage test shard audit failed:")
    for failure in failures:
        print(f"  {failure}")
    print("update storage/test_manifest.zig and the disjoint shard filters in zig/build.zig")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
