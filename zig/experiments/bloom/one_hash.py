#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build a split-block storage variant with one input hash per Bloom key."""

import argparse
import difflib
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--zig", required=True)
parser.add_argument("--patch-only", action="store_true")
args = parser.parse_args()
module = ROOT / "zig/lib/bloom/src/mod.zig"
table = ROOT / "zig/pkg/antfly-embedded/src/storage/lsm/table_file.zig"
original_module = module.read_bytes()
original_table = table.read_text()
text = original_table
entry_start = text.index("fn entryHashes(")
entry_end = text.index("fn entryPrefixHashes(", entry_start)
text = (
    text[:entry_start]
    + """// Experimental single-input-hash derivation, paired with ABLOOM3 only.
fn experimentalBloomHashPair(h1: u64) [2]u64 {
    var h2 = h1;
    h2 ^= h2 >> 30;
    h2 *%= 0xbf58476d1ce4e5b9;
    h2 ^= h2 >> 27;
    h2 *%= 0x94d049bb133111eb;
    h2 ^= h2 >> 31;
    return .{ h1, h2 };
}

fn entryHashes(namespace_name: ?[]const u8, key: []const u8) [2]u64 {
    return experimentalBloomHashPair(hashEntryWithSeed(0x243f6a8885a308d3, namespace_name, key));
}

"""
    + text[entry_end:]
)
prefix_start = text.index("fn prefixHashes(")
prefix_end = text.index("fn hashEntryWithSeed(", prefix_start)
text = (
    text[:prefix_start]
    + """fn prefixHashes(namespace_name: ?[]const u8, prefix: []const u8) [2]u64 {
    return experimentalBloomHashPair(hashEntryWithSeed(0x452821e638d01377, namespace_name, prefix));
}

"""
    + text[prefix_end:]
)
(HERE / "one-hash-table.patch").write_text(
    "".join(
        difflib.unified_diff(
            original_table.splitlines(True),
            text.splitlines(True),
            fromfile="a/zig/pkg/antfly-embedded/src/storage/lsm/table_file.zig",
            tofile="b/zig/pkg/antfly-embedded/src/storage/lsm/table_file.zig",
            n=0,
        )
    )
)
if args.patch_only:
    raise SystemExit(0)
try:
    split = (
        (HERE / "split_module.zig")
        .read_text()
        .replace('"ABLOOM2"', '"ABLOOM3"')
        .replace("pub const version: u32 = 2;", "pub const version: u32 = 3;")
    )
    module.write_text(split)
    table.write_text(text)
    command = [args.zig, "build", "lsm-backend-bench", "-Doptimize=ReleaseFast", "-j2"]
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT / "zig", check=True)
    shutil.copy2(ROOT / "zig/zig-out/bin/lsm_backend_bench", HERE / "lsm-onehash")
finally:
    module.write_bytes(original_module)
    table.write_text(original_table)
