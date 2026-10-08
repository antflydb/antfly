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

"""Format GLiNER2.5 family captures without expanding numeric arrays.

The captures are reviewable JSON rather than minified blobs, but token IDs,
masks, offsets, logits, and probabilities share width-bounded lines instead of
occupying one line per scalar. Run with ``--check`` in validation workflows.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
CAPTURE_DIR = HERE.parents[1] / "testdata" / "gliner25" / "family"
LINE_WIDTH = 120


def scalar(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def numeric_array(value: list[Any], depth: int) -> str:
    encoded = [scalar(item) for item in value]
    one_line = "[" + ", ".join(encoded) + "]"
    indent = "  " * depth
    if len(indent) + len(one_line) <= LINE_WIDTH:
        return one_line

    rows: list[str] = []
    current = ""
    available = max(24, LINE_WIDTH - len(indent) - 2)
    for item in encoded:
        candidate = item if not current else current + ", " + item
        if current and len(candidate) > available:
            rows.append(current + ",")
            current = item
        else:
            current = candidate
    rows.append(current)
    inner = "  " * (depth + 1)
    return "[\n" + "\n".join(inner + row for row in rows) + "\n" + indent + "]"


def render(value: Any, depth: int = 0) -> str:
    indent = "  " * depth
    child_indent = "  " * (depth + 1)
    if isinstance(value, dict):
        if not value:
            return "{}"
        rows = []
        items = sorted(value.items())
        for index, (key, item) in enumerate(items):
            rendered = render(item, depth + 1)
            suffix = "," if index + 1 < len(items) else ""
            rows.append(child_indent + scalar(key) + ": " + rendered + suffix)
        return "{\n" + "\n".join(rows) + "\n" + indent + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(is_number(item) for item in value):
            return numeric_array(value, depth)
        rows = []
        for index, item in enumerate(value):
            suffix = "," if index + 1 < len(value) else ""
            rows.append(child_indent + render(item, depth + 1) + suffix)
        return "[\n" + "\n".join(rows) + "\n" + indent + "]"
    return scalar(value)


def formatted(path: Path) -> str:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    return render(value) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="fail if a capture would change"
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="capture paths (defaults to the family directory)",
    )
    args = parser.parse_args()

    paths = args.paths or sorted(CAPTURE_DIR.glob("*.json"))
    changed = []
    for path in paths:
        output = formatted(path)
        if path.read_text(encoding="utf-8") == output:
            continue
        changed.append(path)
        if not args.check:
            path.write_text(output, encoding="utf-8")
    if changed:
        verb = "need formatting" if args.check else "formatted"
        print(f"{verb}: " + ", ".join(str(path) for path in changed))
    return int(args.check and bool(changed))


if __name__ == "__main__":
    raise SystemExit(main())
