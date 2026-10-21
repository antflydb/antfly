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

"""Generate compact Unicode 16 Script_Extensions masks for native script runs.

Usage: generate_script_runs.py UCD_DIRECTORY OUTPUT
The maintenance inputs must match the reviewed hashes in unicode_data.zig.
"""

import hashlib
import re
import sys
from pathlib import Path

root, output = map(Path, sys.argv[1:])
repo = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(repo / "scripts"))
from generated_source_licenses import unicode_source_header

source = (Path(__file__).resolve().parents[1] / "src/unicode_data.zig").read_text()
names = ["Scripts.txt", "ScriptExtensions.txt", "PropertyValueAliases.txt"]
for name in names:
    expected = re.search(re.escape(name) + r": sha256 ([0-9a-f]+)", source)[1]
    if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
        raise ValueError(f"unreviewed Unicode input: {name}")


def records(name):
    for line in (root / name).read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            yield [part.strip() for part in line.split(";")]


aliases = {}
for fields in records("PropertyValueAliases.txt"):
    if fields[0] == "sc":
        for name in fields[1:]:
            aliases[name] = fields[2]
scripts = sorted(set(aliases.values()))
ids = {name: i for i, name in enumerate(scripts)}
virtual = {
    "Han": [len(ids), len(ids) + 1, len(ids) + 2],
    "Hiragana": [len(ids)],
    "Katakana": [len(ids)],
    "Hangul": [len(ids) + 1],
    "Bopomofo": [len(ids) + 2],
}
assert len(ids) + 3 <= 256
neutral = (1 << 256) - 1
palette = [0, neutral]
lookup = {mask: i for i, mask in enumerate(palette)}


def index(names):
    if names == {"Unknown"}:
        return 0
    if names in [{"Common"}, {"Inherited"}]:
        return 1
    mask = 0
    for name in names:
        mask |= 1 << ids[name]
        for bit in virtual.get(name, []):
            mask |= 1 << bit
    if mask not in lookup:
        lookup[mask] = len(palette)
        palette.append(mask)
    return lookup[mask]


values = [0] * 0x110000
for fields in records("Scripts.txt"):
    bounds = fields[0].split("..")
    lo, hi = int(bounds[0], 16), int(bounds[-1], 16)
    values[lo : hi + 1] = [index({fields[1]})] * (hi - lo + 1)
for fields in records("ScriptExtensions.txt"):
    bounds = fields[0].split("..")
    lo, hi = int(bounds[0], 16), int(bounds[-1], 16)
    values[lo : hi + 1] = [index({aliases[name] for name in fields[1].split()})] * (
        hi - lo + 1
    )
intervals = []
lo = 0
for i in range(1, len(values) + 1):
    if i == len(values) or values[i] != values[lo]:
        intervals.append((lo, i - 1, values[lo]))
        lo = i
out = [
    unicode_source_header(__file__),
    "// Generated Unicode 16 script-run masks. Do not edit.\n",
]
for name in names:
    out.append(
        f"// {name}: sha256 {hashlib.sha256((root / name).read_bytes()).hexdigest()}\n"
    )
out.append("pub const masks = [_][4]u64{\n")
for mask in palette:
    out.append(
        "    .{"
        + ",".join(hex((mask >> (i * 64)) & ((1 << 64) - 1)) for i in range(4))
        + "},\n"
    )
out.append("};\npub const latin: u16 = " + str(values[ord("A")]) + ";\n")
out.append("pub const intervals = [_]struct { lo: u21, hi: u21, mask: u16 }{\n")
for lo, hi, mask in intervals:
    out.append(f"    .{{ .lo=0x{lo:x},.hi=0x{hi:x},.mask={mask} }},\n")
out.append("};\n")
output.write_text("".join(out))
print(f"{len(intervals)} intervals, {len(palette)} masks")
