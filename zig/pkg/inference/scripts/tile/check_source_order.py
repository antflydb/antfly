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

"""Check that every Metal kernel in metal_kernels.m only uses helpers,
structs and macros defined above it in the same MSL source string.

MSL is only compiled on macOS, so a generated region that lands above a
helper it calls would otherwise go unnoticed until a device build.

    python3 -I check_source_order.py <metal_kernels.m> [--baseline <old metal_kernels.m>]

With --baseline, only names that were not already reported for the old file
count (the scan is heuristic and knows a few false positives).
"""

import re
import sys

STR = re.compile(r'^\s*@?"(.*)\\n"\s*$')
DEF = re.compile(r"^(?:template\s*<[^>]*>\s*)?(?:(?:static|inline|constexpr|METAL_FUNC)\s+)*(?:const\s+)?(?!return\b|else\b|kernel\b)[A-Za-z_][\w:]*(?:<[^()]*>)?\s*[\*&]?\s+[\*&]?(\w+)\s*\(")


def problems(path):
    """{(name, user line text)} for uses that precede the definition."""
    lines = open(path).read().split("\n")
    block_defs, block = {}, None
    for i, line in enumerate(lines):
        s = STR.match(line)
        if not s and not re.match(r"^\s*(//.*)?$", line):
            block = None
            continue
        if not s:
            continue
        if block is None:
            block = i
            block_defs[block] = {}
        text = s.group(1)
        if text.startswith((" ", "\t", "}", "#")) and not text.startswith("#define"):
            continue
        m = re.match(r"#define\s+(\w+)", text) or re.match(r"(?:typedef\s+)?struct\s+(\w+)", text) or DEF.match(text)
        if m and m.group(1) != "decltype":
            block_defs[block].setdefault(m.group(1), i)
    found, block = {}, None
    for i, line in enumerate(lines):
        s = STR.match(line)
        if not s and not re.match(r"^\s*(//.*)?$", line):
            block = None
            continue
        if not s:
            continue
        if block is None:
            block = i
        defs = block_defs.get(block, {})
        for tok in set(re.findall(r"\b[A-Za-z_]\w*\b", s.group(1))):
            d = defs.get(tok)
            if d is not None and d > i:
                found.setdefault(tok, (i + 1, d + 1))
    return found


def main():
    new = problems(sys.argv[1])
    old = problems(sys.argv[sys.argv.index("--baseline") + 1]) if "--baseline" in sys.argv else {}
    fresh = {k: v for k, v in new.items() if k not in old}
    for tok, (use, d) in sorted(fresh.items(), key=lambda x: x[1]):
        print(f"line {use}: uses {tok}, defined below at line {d}")
    print(f"{len(fresh)} use(s) before definition" + (" not in the baseline" if old else ""))
    sys.exit(1 if fresh else 0)

if __name__ == "__main__":
    main()
