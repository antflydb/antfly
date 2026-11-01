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

"""Rank emitted implementation families across real compiled Zig owners.

ELF symbol sizes are used directly. Mach-O has no nm symbol sizes: address gaps
within each object's text symbols are estimates, including padding. The final
text symbol has no inferred extent. Aliases at the same address count once.
ELF aliases may overlap. Shared names identify investigation candidates, not identical machine code or
compiler time. Input archives may be installed products or cached build outputs.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import subprocess


def parse_text_symbols(output):
    objects = defaultdict(list)
    for line in output.splitlines():
        # --print-file-name produces archive[member]: (or object:) prefixes.
        match = re.match(r"^(.*): (.+) ([tT]) (\d+) (\d+)$", line)
        if match:
            obj, name, _, address, size = match.groups()
            objects[obj].append((int(address), name, int(size)))
    symbols = []
    estimated = False
    for obj, rows in objects.items():
        if any(size for _, _, size in rows):
            # Relocatable ELF function sections commonly all start at zero;
            # equal addresses do not imply aliases across different sections.
            symbols.extend(
                dict(object=obj, name=name, bytes=size, estimated=False)
                for _, name, size in rows
                if size
            )
            continue
        addresses = sorted({address for address, _, _ in rows})
        extents = dict(zip(addresses, addresses[1:]))
        grouped = defaultdict(list)
        for address, name, size in rows:
            # LLVM/Mach-O labels do not name authored functions.
            if not re.fullmatch(r"l?tmp\d+", name):
                grouped[address].append((name, size))
        for address, aliases in grouped.items():
            name, size = max(aliases, key=lambda row: (row[1], len(row[0]), row[0]))
            inferred = size == 0
            if inferred:
                estimated = True
                size = extents.get(address, address) - address
            if size:
                symbols.append(
                    dict(object=obj, name=name, bytes=size, estimated=inferred)
                )
    return symbols, estimated


def family(name):
    name = re.sub(r"^(?:l_?|_)(?=[A-Za-z])", "", name)
    # Disambiguators differ across independently compiled objects.
    return re.sub(r"(?:__anon_|\.__struct_|\.)(\d+)(?=\D|$)", "#", name)


def category(name):
    if "json.Stringify" in name or "json.stringify" in name:
        return "json_encoding"
    if "json.static" in name or "json.parse" in name or "json.Scanner" in name:
        return "json_decoding"
    if "openapi" in name:
        return "generated_contracts"
    if "mutation_" in name:
        return "mutation"
    if "aggregation" in name:
        return "aggregation"
    return "other"


def report(owners):
    repeated = defaultdict(list)
    units = []
    for owner, symbols, estimated in owners:
        categories = defaultdict(int)
        for symbol in symbols:
            categories[category(symbol["name"])] += symbol["bytes"]
            repeated[family(symbol["name"])].append(dict(owner=owner, **symbol))
        units.append(
            dict(
                owner=owner,
                text_symbol_bytes=sum(s["bytes"] for s in symbols),
                extents_estimated=estimated,
                categories=dict(categories),
                top_symbols=sorted(symbols, key=lambda s: -s["bytes"])[:50],
            )
        )
    overlap = []
    for name, variants in repeated.items():
        unit_names = sorted({v["owner"] for v in variants})
        if len(unit_names) > 1:
            overlap.append(
                dict(
                    family=name,
                    owners=unit_names,
                    bytes=sum(v["bytes"] for v in variants),
                    variants=variants,
                )
            )
    return dict(
        format_version=1,
        units=units,
        repeated_named_families=sorted(overlap, key=lambda s: -s["bytes"])[:100],
        limitations=__doc__.split("\n\n", 1)[1].strip(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact", action="append", required=True, metavar="OWNER=PATH"
    )
    parser.add_argument("--nm", default="llvm-nm")
    parser.add_argument(
        "--forbid",
        action="append",
        default=[],
        metavar="OWNER=SYMBOL_SUBSTRING",
        help="Fail if a consumer emits a provider implementation family",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    owners = []
    names = set()
    for artifact in args.artifact:
        owner, separator, filename = artifact.partition("=")
        if not separator or not owner or owner in names:
            parser.error("artifacts require unique OWNER=PATH labels")
        names.add(owner)
        path = Path(filename)
        result = subprocess.run(
            [
                args.nm,
                "--defined-only",
                "--print-size",
                "--radix=d",
                "--format=posix",
                "--numeric-sort",
                "--print-file-name",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        symbols, estimated = parse_text_symbols(result.stdout)
        if not symbols:
            raise SystemExit(
                f"No attributable text symbols in {path}; retain symbol tables"
            )
        owners.append((owner, symbols, estimated))
    value = report(owners)
    for unit, artifact in zip(value["units"], args.artifact, strict=True):
        path = Path(artifact.partition("=")[2])
        unit.update(artifact=str(path), file_bytes=path.stat().st_size)
    value["violations"] = []
    for rule in args.forbid:
        owner, separator, substring = rule.partition("=")
        if not separator or owner not in names or not substring:
            parser.error("forbid rules require a declared OWNER=SYMBOL_SUBSTRING")
        for label, symbols, _ in owners:
            if label == owner:
                value["violations"].extend(
                    dict(owner=owner, symbol=symbol["name"])
                    for symbol in symbols
                    if substring in symbol["name"]
                )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + "\n")
    if value["violations"]:
        raise SystemExit(
            "Consumer emitted forbidden provider code; inspect " + str(args.output)
        )


if __name__ == "__main__":
    main()
