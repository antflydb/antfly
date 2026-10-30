#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare every existing CUDA native program and resource record, per arch.

Accepts native cubins or fatbins; added exports are reported separately. Parsing
asserts that instruction and resource keys match, so a format mismatch cannot
silently produce an empty successful comparison. --strict requires identity.
"""

import argparse
import json
import re
import subprocess
from pathlib import Path


def records(text, resources, default_arch=None):
    result = {}
    arch = default_arch
    function = None
    for line in text.splitlines():
        found = re.search(r"(?:arch = |code for )(sm_\d+)", line)
        if found:
            arch = found[1]
            function = None
        found = (
            re.search(r"Function\s*:\s*(\S+)", line)
            if not resources
            else re.search(r"Function\s+(\S+):", line)
        )
        if found:
            assert arch is not None, "missing architecture"
            function = found[1]
            key = (arch, function)
            assert key not in result, ("duplicate function", key)
            result[key] = []
        if function and (
            (resources and "REG:" in line)
            or (not resources and line.strip().startswith("/*"))
        ):
            result[(arch, function)].append(" ".join(line.split()))
    assert result and all(result.values()), "missing native program/resource records"
    return result


def compare(tool, original, candidate, output):
    reports = []
    for label, image in (("original", original), ("candidate", candidate)):
        parsed = []
        for kind, flag in (
            ("sass", "--dump-sass"),
            ("resources", "--dump-resource-usage"),
        ):
            text = subprocess.check_output([str(tool), flag, str(image)], text=True)
            (output / f"{label}.{kind}").write_text(text)
            arches = {key[0] for key in parsed[0]} if parsed else set()
            fallback = next(iter(arches)) if len(arches) == 1 else None
            parsed.append(records(text, kind == "resources", fallback))
        assert parsed[0].keys() == parsed[1].keys(), (
            label,
            "instruction/resource key mismatch",
        )
        reports.append(parsed)
    (old_code, old_resources), (new_code, new_resources) = reports
    assert old_code.keys() <= new_code.keys(), "missing existing entrypoints"
    result = {
        "original_entries": len(old_code),
        "candidate_entries": len(new_code),
        "architectures": sorted({arch for arch, _ in old_code}),
        "added": [list(k) for k in sorted(new_code.keys() - old_code.keys())],
        "changed_instructions": [
            list(k) for k in old_code if old_code[k] != new_code[k]
        ],
        "changed_resources": [
            {
                "arch": k[0],
                "function": k[1],
                "original": old_resources[k],
                "candidate": new_resources[k],
            }
            for k in old_resources
            if old_resources[k] != new_resources[k]
        ],
    }
    (output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("original", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--cuobjdump", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    result = compare(args.cuobjdump, args.original, args.candidate, args.output)
    print(json.dumps(result))
    if args.strict:
        assert not result["changed_instructions"] and not result["changed_resources"], (
            "existing native programs changed"
        )


if __name__ == "__main__":
    main()
