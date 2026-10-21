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

"""Generate regex Unicode 16 ranges from official UCD and emoji input files.

Usage: generate_unicode.py INPUT_DIRECTORY OUTPUT
Input names: UnicodeData.txt, Scripts.txt, ScriptExtensions.txt, CaseFolding.txt,
DerivedCoreProperties.txt, PropList.txt, PropertyValueAliases.txt,
PropertyAliases.txt, DerivedAge.txt, GraphemeBreakProperty.txt,
WordBreakProperty.txt, SentenceBreakProperty.txt, emoji-data.txt,
BidiMirroring.txt. All inputs are pinned to Unicode 16.0.0.
"""

import hashlib
import sys
from collections import defaultdict
from pathlib import Path

root, output = map(Path, sys.argv[1:])
repo = next(
    p
    for p in Path(__file__).resolve().parents
    if (p / "scripts/generated_source_licenses.py").exists()
)
sys.path.insert(0, str(repo / "scripts"))
from generated_source_licenses import unicode_source_header

props = defaultdict(list)
category = ["Cn"] * 0x110000
first = None
for line in (root / "UnicodeData.txt").read_text().splitlines():
    f = line.split(";")
    cp = int(f[0], 16)
    if f[1].endswith(", First>"):
        first = cp
    elif f[1].endswith(", Last>"):
        category[first : cp + 1] = [f[2]] * (cp + 1 - first)
        first = None
    else:
        category[cp] = f[2]
for cp, cat in enumerate(category):
    props[cat].append((cp, cp))
    props[cat[0]].append((cp, cp))


def lines(name):
    for raw in (root / name).read_text().splitlines():
        value = raw.split("#", 1)[0].strip()
        if value:
            yield [f.strip() for f in value.split(";")]


def load(name):
    for f in lines(name):
        bounds = f[0].split("..")
        lo, hi = int(bounds[0], 16), int(bounds[-1], 16)
        if len(f) == 2:
            props[f[1]].append((lo, hi))


for line in (root / "UnicodeData.txt").read_text().splitlines():
    fields = line.split(";")
    if fields[9] == "Y":
        cp = int(fields[0], 16)
        props["Bidi_Mirrored"].append((cp, cp))

for name in [
    "Scripts.txt",
    "DerivedCoreProperties.txt",
    "PropList.txt",
    "emoji-data.txt",
]:
    load(name)
props["word"] = [
    item
    for name in ["Alphabetic", "M", "Nd", "Pc", "Join_Control"]
    for item in props[name]
]
props["digit"] = props["Nd"]
props["space"] = props["White_Space"]
props["Any"] = [(0, 0x10FFFF)]
props["ASCII"] = [(0, 127)]
props["Assigned"] = [
    item
    for key, ranges in props.items()
    if len(key) == 2 and key[0].isupper() and key != "Cn"
    for item in ranges
]
props["LC"] = props["Lu"] + props["Ll"] + props["Lt"]
props["alnum"] = props["Alphabetic"] + props["Nd"]
alias = {}
normalize = lambda s: "".join(c.lower() for c in s if c not in "_- ")
for f in lines("PropertyValueAliases.txt"):
    if f[0] in ("gc", "sc"):
        target = f[1] if f[0] == "gc" else f[2]
        if target in props:
            for value in f[1:]:
                if value:
                    alias[normalize(value)] = target
                    alias[f"{f[0]}:{normalize(value)}"] = target
for key in props:
    alias[normalize(key)] = key
folds = []
for f in lines("CaseFolding.txt"):
    if f[1] in ("C", "S"):
        folds.append((int(f[0], 16), int(f[2], 16)))


def merge(ranges):
    out = []
    for lo, hi in sorted(ranges):
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def subtract(ranges, exclusions):
    result = []
    cuts = merge(exclusions)
    j = 0
    for lo, hi in merge(ranges):
        while j < len(cuts) and cuts[j][1] < lo:
            j += 1
        k = j
        while k < len(cuts) and cuts[k][0] <= hi:
            if cuts[k][0] > lo:
                result.append((lo, cuts[k][0] - 1))
            lo = max(lo, cuts[k][1] + 1)
            if lo > hi:
                break
            k += 1
        if lo <= hi:
            result.append((lo, hi))
    return result


script_aliases = {}
script_names = {f[1] for f in lines("Scripts.txt")}
props["Unknown"] = subtract(
    [(0, 0x10FFFF)], [r for name in script_names for r in props[name]]
)
script_names.add("Unknown")
for f in lines("PropertyValueAliases.txt"):
    if f[0] == "sc":
        script_names.add(f[2])
        _ = props[f[2]]
        for value in f[1:]:
            script_aliases[value] = f[2]
            alias[f"sc:{normalize(value)}"] = f[2]
            alias[normalize(value)] = f[2]
    elif f[0] == "gc":
        for value in f[1:]:
            alias[f"gc:{normalize(value)}"] = f[1]
# Script_Extensions overrides Script on listed code points, with Script as the
# default everywhere else. Common/Inherited are not added to override sets.
overrides = []
extra_scripts = defaultdict(list)
for f in lines("ScriptExtensions.txt"):
    bounds = f[0].split("..")
    span = (int(bounds[0], 16), int(bounds[-1], 16))
    overrides.append(span)
    for short in f[1].split():
        extra_scripts[script_aliases[short]].append(span)
for script in script_names:
    props[f"scx:{script}"] = subtract(props[script], overrides) + extra_scripts[script]
for short, script in script_aliases.items():
    alias[f"scx:{normalize(short)}"] = f"scx:{script}"
for f in lines("PropertyAliases.txt"):
    target = next((value for value in reversed(f) if value in props), None)
    if target is not None:
        for value in f:
            alias[normalize(value)] = target

# Age matches characters introduced at or before the requested release.
ages = defaultdict(list)
for f in lines("DerivedAge.txt"):
    bounds = f[0].split("..")
    ages[f[1]].append((int(bounds[0], 16), int(bounds[-1], 16)))
accumulated = []
for age in sorted(ages, key=lambda x: tuple(map(int, x.split(".")))):
    accumulated = accumulated + ages[age]
    props[f"age:{age}"] = accumulated
for f in lines("PropertyValueAliases.txt"):
    if f[0] == "age" and f"age:{f[1]}" in props:
        for value in f[1:]:
            alias[f"age:{normalize(value)}"] = f"age:{f[1]}"
for namespace, filename in [
    ("gcb", "GraphemeBreakProperty.txt"),
    ("wb", "WordBreakProperty.txt"),
    ("sb", "SentenceBreakProperty.txt"),
]:
    values = set()
    for f in lines(filename):
        bounds = f[0].split("..")
        props[f"{namespace}:{f[1]}"].append((int(bounds[0], 16), int(bounds[-1], 16)))
        values.add(f[1])
    props[f"{namespace}:Other"] = subtract(
        [(0, 0x10FFFF)], [r for value in values for r in props[f"{namespace}:{value}"]]
    )
    for f in lines("PropertyValueAliases.txt"):
        if f[0].lower() == namespace and f"{namespace}:{f[2]}" in props:
            for value in f[1:]:
                alias[f"{namespace}:{normalize(value)}"] = f"{namespace}:{f[2]}"

# Keep named, documented Unicode properties, not derived value tables such as
# InCB's detailed third-column partitions, which need a separate namespace.
keys = sorted(props)
chunks = [
    unicode_source_header(__file__),
    "// Generated Unicode 16.0.0. Do not edit by hand.\n",
]
for name in [
    "UnicodeData.txt",
    "Scripts.txt",
    "CaseFolding.txt",
    "DerivedCoreProperties.txt",
    "PropList.txt",
    "PropertyValueAliases.txt",
    "PropertyAliases.txt",
    "ScriptExtensions.txt",
    "DerivedAge.txt",
    "GraphemeBreakProperty.txt",
    "WordBreakProperty.txt",
    "SentenceBreakProperty.txt",
    "emoji-data.txt",
]:
    chunks.append(
        f"// {name}: sha256 {hashlib.sha256((root / name).read_bytes()).hexdigest()}\n"
    )
chunks.append("pub const Range = struct { lo: u21, hi: u21 };\n")
for i, key in enumerate(keys):
    chunks.append(f"const p{i} = [_]Range{{\n")
    chunks.extend(
        f"    .{{ .lo = 0x{lo:x}, .hi = 0x{hi:x} }},\n" for lo, hi in merge(props[key])
    )
    chunks.append("};\n")
chunks.append(
    "pub const Property = struct { name: []const u8, ranges: []const Range };\npub const properties = [_]Property{\n"
)
for name, key in sorted(alias.items()):
    chunks.append(f'    .{{ .name = "{name}", .ranges = &p{keys.index(key)} }},\n')
chunks.append(
    "};\npub const Fold = struct { from: u21, to: u21 };\npub const folds = [_]Fold{\n"
)
chunks.extend(f"    .{{ .from = 0x{a:x}, .to = 0x{b:x} }},\n" for a, b in folds)
chunks.append("};\npub const reverse_folds = [_]Fold{\n")
chunks.extend(
    f"    .{{ .from = 0x{b:x}, .to = 0x{a:x} }},\n"
    for a, b in sorted(folds, key=lambda p: (p[1], p[0]))
)
chunks.append("};\n")
output.write_text("".join(chunks))
