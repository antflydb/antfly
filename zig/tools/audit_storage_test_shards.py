#!/usr/bin/env python3
"""Fail when a storage source containing tests is outside every codegen shard."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Sequence


TEST_DECLARATION = re.compile(r"(?m)^\s*(?:pub\s+)?test(?:\s|\")")


def module_names(root: Path, source: Path) -> tuple[str, ...]:
    relative = source.relative_to(root).with_suffix("")
    parts = ("storage", *relative.parts)
    names = [".".join(parts) + "."]
    if relative.name == "mod":
        names.append(".".join(parts[:-1]) + ".")
    return tuple(names)


def test_modules(root: Path) -> Iterable[tuple[Path, tuple[str, ...]]]:
    for source in sorted(root.rglob("*.zig")):
        if TEST_DECLARATION.search(source.read_text(encoding="utf-8")):
            yield source, module_names(root, source)


def uncovered_modules(root: Path, filters: Sequence[str]) -> list[str]:
    uncovered: list[str] = []
    for source, names in test_modules(root):
        if any(test_filter in name for test_filter in filters for name in names):
            continue
        candidates = ", ".join(names)
        uncovered.append(f"{source.relative_to(root)} ({candidates})")
    return uncovered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--filter", action="append", default=[])
    args = parser.parse_args()
    if not args.filter:
        parser.error("at least one --filter is required")

    missing = uncovered_modules(args.root, args.filter)
    if not missing:
        return 0
    print("storage test modules missing from every unit-test codegen shard:")
    for module in missing:
        print(f"  {module}")
    print("add the module to exactly one bounded storage shard in zig/build.zig")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
