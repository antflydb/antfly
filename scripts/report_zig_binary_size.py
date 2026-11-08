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

"""Attribute an unstripped ELF binary and inventory literal embedded assets.

Symbol sizes describe linked code/data, not compiler work. Generic families
are candidates for inspection, not proof that implementations are identical.
The asset inventory is lexical: it does not establish linked reachability.
"""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import re
import subprocess


def parse_symbols(output):
    symbols = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 4 or len(fields[1]) != 1:
            continue
        name, kind, address, size = fields
        if not address.isdecimal() or not size.isdecimal():
            continue
        symbols.append({"name": name, "kind": kind, "bytes": int(size)})
    return sorted(symbols, key=lambda row: (-row["bytes"], row["name"]))


def symbol_report(symbols):
    mutation = [s for s in symbols if "mutation_" in s["name"] and s["kind"] in "tT"]
    families = defaultdict(list)
    for symbol in mutation:
        # Ignore LLVM's numeric disambiguator, then group terminal function
        # names across receiver specializations. Keep names for manual review.
        name = re.sub(r"\.\d+$", "", symbol["name"])
        families[name.rsplit(".", 1)[-1]].append(symbol)
    return {
        "sized_symbol_count": len(symbols),
        "top_symbols": symbols[:100],
        "mutation_text_bytes": sum(s["bytes"] for s in mutation),
        "mutation_families": sorted(
            (
                {
                    "function": name,
                    "variants": len(rows),
                    "bytes": sum(s["bytes"] for s in rows),
                    "symbols": rows,
                }
                for name, rows in families.items()
            ),
            key=lambda row: (-row["bytes"], row["function"]),
        ),
        "symbol_bytes_are_not_file_size": True,
    }


def embedded_assets(root):
    assets = {}
    unresolved = []
    sources = []
    for directory, children, files in os.walk(root):
        children[:] = sorted(
            child
            for child in children
            if child
            not in (
                ".zig-cache",
                ".zig-global-cache",
                "zig-out",
                "node_modules",
                ".worktrees",
                ".git",
            )
        )
        sources.extend(
            Path(directory) / name for name in sorted(files) if name.endswith(".zig")
        )
    for source in sources:
        source_text = source.read_text(errors="replace")
        for match in re.finditer(r'@embedFile\("([^"\n]+)"\)', source_text):
            path = (source.parent / match[1]).resolve()
            site = {
                "source": str(source.relative_to(root)),
                "line": source_text.count("\n", 0, match.start()) + 1,
            }
            if not path.is_relative_to(root.resolve()) or not path.is_file():
                unresolved.append({**site, "literal": match[1]})
                continue
            key = str(path.relative_to(root.resolve()))
            row = assets.setdefault(
                key, {"path": key, "bytes": path.stat().st_size, "sites": []}
            )
            row["sites"].append(site)
    return {
        "lexical_only": True,
        "assets": sorted(assets.values(), key=lambda row: (-row["bytes"], row["path"])),
        "unresolved_literals": unresolved,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path, nargs="?")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--nm", default="llvm-nm")
    parser.add_argument("--size", default="llvm-size")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.binary is None and args.source_root is None:
        parser.error("supply a binary or --source-root")
    report = {"format_version": 1}
    if args.binary:
        output = subprocess.check_output(
            [
                args.nm,
                "--defined-only",
                "--print-size",
                "--radix=d",
                "--format=posix",
                str(args.binary),
            ],
            text=True,
        )
        symbols = parse_symbols(output)
        if not symbols:
            raise SystemExit(
                "No sized symbols: supply an unstripped ELF binary and llvm-nm"
            )
        report.update(
            binary=str(args.binary),
            file_bytes=args.binary.stat().st_size,
            **symbol_report(symbols),
        )
        report["sections"] = subprocess.check_output(
            [args.size, "--format=sysv", "--radix=10", str(args.binary)], text=True
        )
    if args.source_root:
        report["embedded_assets"] = embedded_assets(args.source_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
