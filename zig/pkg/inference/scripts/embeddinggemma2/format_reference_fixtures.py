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

"""Format reference vectors in rows without changing numeric lexemes or metadata."""

import argparse
import json
from pathlib import Path
import re

NUMBER = r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?"
ARRAY = re.compile(
    r'(?P<head>"(?:vector|embedding)"\s*:\s*\[)'
    r"(?P<body>\s*" + NUMBER + r"(?:\s*,\s*" + NUMBER + r")*\s*)(?P<tail>\])"
)


def format_reference(source):
    def replace(match):
        values = re.findall(NUMBER, match["body"])
        if len(values) < 16:
            return match.group(0)
        start = source.rfind("\n", 0, match.start()) + 1
        indent = re.match(r" *", source[start : match.start()]).group()
        rows = [", ".join(values[i : i + 8]) for i in range(0, len(values), 8)]
        return (
            match["head"]
            + "\n"
            + ",\n".join(indent + "  " + row for row in rows)
            + "\n"
            + indent
            + match["tail"]
        )

    result = ARRAY.sub(replace, source)
    if json.loads(result) != json.loads(source):
        raise ValueError("reference values changed")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    changed = []
    for path in args.paths:
        source = path.read_text()
        formatted = format_reference(source)
        if source != formatted:
            changed.append(str(path))
            if not args.check:
                path.write_text(formatted)
    if args.check and changed:
        parser.exit(1, "Reference formatting required: " + ", ".join(changed) + "\n")


if __name__ == "__main__":
    main()
