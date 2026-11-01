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

"""Verify emitted ownership metadata against shipped sources and inventory native leaves.

Run both generator freshness checks before exporting quant-kernel-codegen
--inventory. This report consumes that export; it is not another kernel registry.
Native surrounds and local include leaves remain canonical authored programs.
"""
from pathlib import Path
import argparse
import hashlib
import json
import re

MARKER = re.compile(rb"(?m)^[ \t]*// (tile-codegen|quant-kernel-codegen):(begin|end) ([^\n]+)\n")
INCLUDE = re.compile(rb'(?m)^\s*#include\s+"([^"\n]+)"')


def fingerprint(data):
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def source_regions(data):
    regions = []
    opened = None
    for match in MARKER.finditer(data):
        generator, boundary, label = (v.decode() for v in match.groups())
        label = label.split(" (do not edit;", 1)[0]
        if generator == "tile-codegen":
            label = label.removesuffix(" family")
        if boundary == "begin":
            if opened is not None:
                raise ValueError("overlapping native ownership")
            opened = (generator, label, match.start(), match.end())
        else:
            if opened is None or opened[:2] != (generator, label):
                raise ValueError("mismatched native ownership")
            regions.append((*opened[:2], opened[2], match.end(), data[opened[3]:match.start()]))
            opened = None
    if opened is not None:
        raise ValueError("unclosed native ownership")
    return regions


def verify(root, inventory):
    if inventory["schema"] != 1:
        raise ValueError("unknown inventory schema")
    package = (root / "zig/pkg/inference").resolve()
    def path(name):
        result = (package / name).resolve()
        if not result.is_relative_to(package):
            raise ValueError("inventory path escapes inference package")
        return result
    indexed = {}
    outputs = {}
    for entry in inventory["native_regions"]:
        owner, name = entry["owner"], entry["family_region"]
        if owner == "quant_region":
            name = name.strip().removeprefix("// quant-kernel-codegen:begin ").split(" (do not edit;", 1)[0]
        key = (entry["output"], owner, name)
        if key in indexed:
            raise ValueError("duplicate native ownership")
        indexed[key] = entry
        outputs.setdefault(entry["output"], []).append(entry)
    canonical, verified = [], set()
    queue = []
    for output, entries in sorted(outputs.items()):
        file = path(output)
        data = file.read_bytes()
        queue.append(file)
        if entries[0]["owner"] == "generated_file":
            if len(entries) != 1:
                raise ValueError("whole-file and region ownership overlap")
            entry = entries[0]
            if fingerprint(data) != {k: entry[k] for k in ("bytes", "sha256")}:
                raise ValueError("stale generated file: " + output)
            verified.add((output, "generated_file", entry["family_region"]))
            continue
        cursor, surrounds = 0, []
        for generator, label, start, end, body in source_regions(data):
            owner = "tile_region" if generator == "tile-codegen" else "quant_region"
            key = (output, owner, label)
            if key not in indexed:
                raise ValueError("unregistered native region: " + repr(key))
            entry = indexed[key]
            if fingerprint(body) != {k: entry[k] for k in ("bytes", "sha256")}:
                raise ValueError("stale native region: " + repr(key))
            if key in verified:
                raise ValueError("duplicate native region: " + repr(key))
            verified.add(key)
            surrounds.append(data[cursor:start])
            cursor = end
        surrounds.append(data[cursor:])
        canonical.append({"output": output, "classification": "canonical_native_surrounds", **fingerprint(b"".join(surrounds))})
    if verified != indexed.keys():
        raise ValueError("inventory region absent from shipped sources: " + repr(indexed.keys() - verified))
    # Follow actual local includes, retaining templates and macro-only helpers.
    # System headers are deliberately outside this owned native source closure.
    standalone = []
    for profile in inventory.get("standalone_cuda", []):
        image = path("src/ops/cuda/" + profile["file"])
        producer = path("src/ops/cuda/prototypes/" + profile["producer"])
        standalone.append({"image": str(image.relative_to(package)), "image_identity": fingerprint(image.read_bytes()), "producer": str(producer.relative_to(package)), "producer_identity": fingerprint(producer.read_bytes())})
        queue.append(producer)
    visited, leaves = set(), []
    while queue:
        file = queue.pop()
        if file in visited:
            continue
        visited.add(file)
        for include in INCLUDE.findall(file.read_bytes()):
            child = (file.parent / include.decode()).resolve()
            if child.is_relative_to(package) and child.is_file():
                queue.append(child)
                name = str(child.relative_to(package))
                if name not in outputs and not any(row["output"] == name for row in leaves):
                    leaves.append({"output": name, "classification": "canonical_native_include", **fingerprint(child.read_bytes())})
    return {**inventory, "verification": {"regions_and_files": len(verified), "standalone_cuda_inputs": standalone, "canonical_native": canonical + sorted(leaves, key=lambda row: row["output"]), "retention": "Canonical surrounds and include leaves are authored implementation, not dead-code candidates. Compatibility exports remain ABI adapters; lexical single-reference counts do not establish liveness."}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = verify(args.root, json.loads(args.inventory.read_text()))
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verified": report["verification"]["regions_and_files"], "canonical_native": len(report["verification"]["canonical_native"])}))


if __name__ == "__main__":
    main()
