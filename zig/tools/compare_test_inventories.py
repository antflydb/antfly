#!/usr/bin/env python3
"""Compare unions of declared tests from compiled Zig test executables."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Sequence


INVENTORY_PREFIX = "TEST\t"


def parse_inventory(output: str, *, include_unnamed: bool = False) -> frozenset[str]:
    names = [
        line.removeprefix(INVENTORY_PREFIX)
        for line in output.splitlines()
        if line.startswith(INVENTORY_PREFIX)
    ]
    if not names:
        raise ValueError("test executable produced no inventory entries")
    if len(names) != len(set(names)):
        duplicates = sorted(name for name in set(names) if names.count(name) > 1)
        raise ValueError(f"test executable produced duplicate inventory entries: {duplicates}")
    if not include_unnamed:
        names = [name for name in names if not name.endswith(".test_0")]
    return frozenset(names)


def executable_inventory(executable: Path, *, include_unnamed: bool = False) -> frozenset[str]:
    completed = subprocess.run(
        [str(executable), "--list-tests"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{executable} --list-tests exited with {completed.returncode}:\n"
            f"{completed.stdout}"
        )
    return parse_inventory(completed.stdout, include_unnamed=include_unnamed)


def combined_executable_inventory(
    executables: Sequence[Path], *, include_unnamed: bool = False
) -> frozenset[str]:
    combined: set[str] = set()
    for executable in executables:
        inventory = executable_inventory(executable, include_unnamed=include_unnamed)
        overlap = sorted(combined.intersection(inventory))
        if overlap:
            raise ValueError(f"test executables contain duplicate inventory entries: {overlap}")
        combined.update(inventory)
    return frozenset(combined)


def inventory_diff(
    baseline: frozenset[str],
    candidate: frozenset[str],
) -> tuple[list[str], list[str]]:
    return sorted(baseline - candidate), sorted(candidate - baseline)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, nargs="+", required=True)
    parser.add_argument("--candidate", type=Path, nargs="+", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--include-unnamed", action="store_true")
    args = parser.parse_args(argv)

    baseline = combined_executable_inventory(
        args.baseline, include_unnamed=args.include_unnamed
    )
    candidate = combined_executable_inventory(
        args.candidate, include_unnamed=args.include_unnamed
    )
    missing, added = inventory_diff(baseline, candidate)
    if not missing and not added:
        kind = "all" if args.include_unnamed else "named"
        print(f"{args.label}: identical {kind} inventory ({len(candidate)} tests)")
        return 0

    print(f"{args.label}: test inventory mismatch")
    for name in missing:
        print(f"  missing: {name}")
    for name in added:
        print(f"  added: {name}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
