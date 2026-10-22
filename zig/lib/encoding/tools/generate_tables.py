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

"""Generate native mappings from WHATWG Encoding indexes.json (BSD-3-Clause).

Usage: generate_tables.py /path/to/indexes.json
Download source: https://encoding.spec.whatwg.org/indexes.json
The input SHA256 is recorded; neither Python nor a download is needed to build.
"""

import hashlib
import json
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_bytes()
x = json.loads(source)
root = Path(__file__).resolve().parents[4]
license = (root / "LICENSES/third-party/WHATWG-Encoding-BSD-3-Clause.txt").read_text()
header = (root / "scripts/license-header-apache.txt").read_text()
out = [
    "\n".join("// " + line if line else "//" for line in header.rstrip().splitlines())
    + "\n\n",
    "\n".join("// " + line if line else "//" for line in license.rstrip().splitlines())
    + "\n\n",
    "// Generated from https://encoding.spec.whatwg.org/indexes.json\n// SHA256: "
    + hashlib.sha256(source).hexdigest()
    + "\n",
]


def table(name, values):
    out.append("pub const " + name + " = [_]u21{\n")
    for pos in range(0, len(values), 16):
        out.append(
            "    "
            + ",".join(
                hex(v if v is not None else 0xFFFD) for v in values[pos : pos + 16]
            )
            + ",\n"
        )
    out.append("};\n")


for name, values in x.items():
    if name in [
        "big5",
        "euc-kr",
        "gb18030",
        "gb18030-ranges",
        "jis0208",
        "jis0212",
        "iso-2022-jp-katakana",
    ]:
        continue
    target = (
        name.replace("windows-", "cp")
        .replace("ibm", "cp")
        .replace("macintosh", "macroman")
        .replace("x-mac-cyrillic", "maccyrillic")
        .replace("-", "")
    )
    table(target, values)


def lookup(name, p):
    return x[name][p] if p < len(x[name]) else None


maps = {name: [None] * 65536 for name in ["sjis", "big5", "gbk", "euckr", "eucjp"]}
for lead in range(0x81, 0xFF):
    for trail in range(0x40, 0xFF):
        if trail == 0x7F:
            continue
        index = lead * 256 + trail
        pointer = (lead - 0x81) * 190 + trail - (0x40 if trail < 0x7F else 0x41)
        maps["gbk"][index] = lookup("gb18030", pointer)
        if trail >= 0x41:
            maps["euckr"][index] = lookup("euc-kr", (lead - 0x81) * 190 + trail - 0x41)
        if trail < 0x7F or trail >= 0xA1:
            maps["big5"][index] = lookup(
                "big5", (lead - 0x81) * 157 + trail - (0x40 if trail < 0x7F else 0x62)
            )
        if ((lead <= 0x9F) or (0xE0 <= lead <= 0xFC)) and trail <= 0xFC:
            p = (
                (lead - (0x81 if lead < 0xA0 else 0xC1)) * 188
                + trail
                - (0x40 if trail < 0x7F else 0x41)
            )
            maps["sjis"][index] = (
                0xE000 + p - 8836 if 8836 <= p <= 10715 else lookup("jis0208", p)
            )
for lead in range(0xA1, 0xFF):
    for trail in range(0xA1, 0xFF):
        maps["eucjp"][lead * 256 + trail] = lookup(
            "jis0208", (lead - 0xA1) * 94 + trail - 0xA1
        )
for trail in range(0xA1, 0xE0):
    maps["eucjp"][0x8E * 256 + trail] = 0xFF61 + trail - 0xA1
for name, values in maps.items():
    table(name, values)
table("jis212", x["jis0212"])
out.append("pub const gb18030_ranges = [_]struct { first: u32, last: u32, cp: u21 }{\n")
for i, (first, cp) in enumerate(x["gb18030-ranges"]):
    last = (
        x["gb18030-ranges"][i + 1][0] - 1
        if i + 1 < len(x["gb18030-ranges"])
        else first + 0x10FFFF - cp
    )
    if first < 189000:
        last = min(last, 39419)
    out.append(f"    .{{ .first={first}, .last={last}, .cp={cp} }},\n")
out.append("};\n")
if len(sys.argv) > 2:
    labels = json.loads(Path(sys.argv[2]).read_text())

    def normalize(label):
        return "".join(c.lower() for c in label if c not in "-_ ")

    aliases = {
        normalize(label): normalize(encoding["name"])
        for group in labels
        for encoding in group["encodings"]
        for label in encoding["labels"]
    }
    out.append(
        "pub const aliases = [_]struct { name: []const u8, canonical: []const u8 }{\n"
    )
    for name, canonical in sorted(aliases.items()):
        out.append(
            "    .{ .name="
            + json.dumps(name)
            + ", .canonical="
            + json.dumps(canonical)
            + " },\n"
        )
    out.append("};\n")
Path(__file__).resolve().parents[1].joinpath("src/tables.zig").write_text("".join(out))
