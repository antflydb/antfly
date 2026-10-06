#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Validate a requested selection across structurally separate test owners."""

from __future__ import annotations

import argparse
import sys
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
    argv = sys.argv[1:]
    separator = argv.index("--") if "--" in argv else len(argv)
    args = parser.parse_args(argv[:separator])
    runtime = argparse.ArgumentParser(
        description="Runtime test selection", allow_abbrev=False
    )
    runtime.add_argument("--test-filter", "--suite-filter", action="append", default=[])
    runtime.add_argument("--skip-test-filter", action="append", default=[])
    runtime.add_argument("--allow-empty-test-filter", action="store_true")
    runtime.add_argument("--list-tests", action="store_true")
    runtime.add_argument("--seed")
    runtime.add_argument("--cache-dir")
    runtime.add_argument("--listen")
    runtime.add_argument("--require-no-skips", action="store_true")
    runtime.add_argument("--timeout-ms")
    runtime.add_argument("patterns", nargs="*")
    controls = runtime.parse_intermixed_args(argv[separator + 1 :])
    args.filter.extend(controls.test_filter + controls.patterns)
    args.skip_filter.extend(controls.skip_test_filter)
    args.allow_empty |= controls.allow_empty_test_filter
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
