#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Validate a requested selection across structurally separate test owners."""

from __future__ import annotations

import argparse
from pathlib import Path


def matches(name: str, pattern: str) -> bool:
    target = name if "." in pattern else name.partition(".test.")[2] or name
    return pattern in target


def audit(
    inventories: list[str],
    filters: list[str],
    skip_filters: list[str] = (),
    allow_empty: bool = False,
) -> list[str]:
    owners: dict[str, int] = {}
    errors = []
    for index, inventory in enumerate(inventories):
        for line in inventory.splitlines():
            if not line.startswith("TEST\t"):
                continue
            name = line.removeprefix("TEST\t")
            if name.endswith(".test_0"):
                continue
            if name in owners:
                errors.append(f"test has multiple owners: {name}")
            owners[name] = index
    selected = [
        name
        for name in owners
        if (not filters or any(matches(name, pattern) for pattern in filters))
        and not any(matches(name, pattern) for pattern in skip_filters)
    ]
    if not selected and not allow_empty:
        errors.append("test selection matched no runnable tests")
    for pattern in filters:
        if not allow_empty and not any(matches(name, pattern) for name in owners):
            errors.append(f"test filter matched no declared tests: {pattern}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, action="append", required=True)
    parser.add_argument("--filter", action="append", default=[])
    parser.add_argument("--skip-filter", action="append", default=[])
    parser.add_argument("--allow-empty", action="store_true")
    args = parser.parse_args()
    errors = audit(
        [path.read_text() for path in args.inventory],
        args.filter,
        args.skip_filter,
        args.allow_empty,
    )
    for error in errors:
        print(error)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
