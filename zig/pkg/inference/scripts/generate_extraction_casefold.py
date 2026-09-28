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

"""Generate inference case-fold mappings from pinned CPython Unicode 15.0.0."""

from __future__ import annotations

import argparse
import sys
import unicodedata
from pathlib import Path

repository_root = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "scripts/generated_source_licenses.py").is_file()
)
sys.path.insert(0, str(repository_root / "scripts"))
from generated_source_licenses import unicode_source_header

OUTPUT = (
    Path(__file__).resolve().parents[1] / "src/pipelines/extraction_casefold_data.zig"
)


def render() -> str:
    if unicodedata.unidata_version != "15.0.0":
        raise SystemExit(f"expected Unicode 15.0.0, got {unicodedata.unidata_version}")
    lines = [
        "// Generated from the pinned Unicode casefold table; do not edit.",
        "// Unicode 15.0.0 full default case folding from pinned CPython 3.12.",
        'pub const unicode_version = "15.0.0";',
        "pub const Entry = struct { source: u21, target: [3]u21, len: u2 };",
        "pub const entries = [_]Entry{",
    ]
    for codepoint in range(0x110000):
        folded = chr(codepoint).casefold()
        if folded == chr(codepoint):
            continue
        target = [ord(char) for char in folded]
        if len(target) > 3:
            raise ValueError("case-fold mapping exceeds the pinned contract")
        values = ", ".join(f"0x{value:x}" for value in target + [0] * (3 - len(target)))
        lines.append(
            f"    .{{ .source = 0x{codepoint:x}, .target = .{{ {values} }}, .len = {len(target)} }},"
        )
    lines += ["};", ""]
    return unicode_source_header(__file__) + "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    generated = render()
    if args.write:
        OUTPUT.write_text(generated)
        return 0
    if OUTPUT.read_text() != generated:
        print(
            "stale extraction case-fold table; regenerate with --write", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
