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

"""Compare every existing CUDA native program and resource record, per arch.

Accepts native cubins or fatbins; added exports are reported separately. Parsing
asserts that instruction and resource keys match, so a format mismatch cannot
silently produce an empty successful comparison. --strict requires identity.
"""

import argparse
import json
import hashlib
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


def select_programs(parsed, functions):
    selected = [{key: value for key, value in rows.items() if key[1] in functions} for rows in parsed]
    for rows in selected:
        assert {key[1] for key in rows} == set(functions), "missing requested native program"
    return selected


def retired_programs(original, candidate, retired):
    removed = original.keys() - candidate.keys()
    assert {key[1] for key in removed} == set(retired), "unexpected or unverified retired entrypoints"
    assert not any(key[1] in retired for key in candidate), "retired entrypoint remains"
    return removed


def compare(tool, original, candidate, output, functions=None, retired=()):
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
        reports.append(select_programs(parsed, functions) if functions else parsed)
    (old_code, old_resources), (new_code, new_resources) = reports
    original_entries = len(old_code)
    removed = retired_programs(old_code, new_code, retired)
    old_code = {key: value for key, value in old_code.items() if key not in removed}
    old_resources = {key: value for key, value in old_resources.items() if key not in removed}
    assert old_code.keys() <= new_code.keys(), "missing existing entrypoints"
    result = {
        "original_entries": original_entries,
        "compared_entries": len(old_code),
        "retired": [list(key) for key in sorted(removed)],
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
    if functions:
        def digest(value):
            return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()
        result.update(
            original_image_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),
            candidate_image_sha256=hashlib.sha256(candidate.read_bytes()).hexdigest(),
            programs=[dict(
                arch=key[0], function=key[1],
                original_instructions_sha256=digest(old_code[key]),
                candidate_instructions_sha256=digest(new_code[key]),
                original_resources_sha256=digest(old_resources[key]),
                candidate_resources_sha256=digest(new_resources[key]),
            ) for key in sorted(old_code)],
        )
    (output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("original", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--cuobjdump", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--function", action="append", help="Bind a historical proof to these exact exported programs")
    parser.add_argument("--retired-function", action="append", default=[], help="Require exactly these intentional removals; keep every other entrypoint")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    result = compare(args.cuobjdump, args.original, args.candidate, args.output, args.function, args.retired_function)
    print(json.dumps(result))
    if args.strict:
        assert not result["changed_instructions"] and not result["changed_resources"], (
            "existing native programs changed"
        )


if __name__ == "__main__":
    main()
