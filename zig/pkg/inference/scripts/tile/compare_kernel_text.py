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

"""Report which kernels are character-identical between two source versions.

A regenerated kernel whose text equals the hand-written one needs no further
check. Metal kernels are compared as the MSL inside metal_kernels.m's
C-string lines (indentation of the string lines is ignored); CUDA kernels
from `extern "C"` to the closing brace.

    python3 -I compare_kernel_text.py <metal|cuda> <old> <new> [name ...]

Without names, every kernel of the old file that also exists in the new one
is compared. Exits 1 if any compared kernel differs.
"""

import re
import sys

STR = re.compile(r'^\s*@?"(.*)\\n"\s*$')


def metal_kernels(path):
    lines = open(path).read().split("\n")
    out = {}
    for i, line in enumerate(lines):
        m = re.match(r'^\s*@?"kernel void (\w+)\(', line)
        if not m:
            continue
        depth, started, body = 0, False, []
        for j in range(i, len(lines)):
            s = STR.match(lines[j])
            if not s:
                break
            body.append(s.group(1))
            for ch in s.group(1):
                if ch == "{":
                    depth, started = depth + 1, True
                elif ch == "}":
                    depth -= 1
            if started and depth == 0:
                out[m.group(1)] = "\n".join(body)
                break
    return out


def cuda_kernels(path):
    text = open(path).read()
    out = {}
    for m in re.finditer(r'(?m)^extern "C" __global__\s+(?:void\s+)?(?:__launch_bounds__\([^)]*\)\s*)?(?:void\s+)?(\w+)\s*\(', text):
        b = text.index("{", m.end())
        depth = 0
        for j in range(b, len(text)):
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
                if depth == 0:
                    out[m.group(1)] = text[m.start():j + 1]
                    break
    return out


def main():
    backend, old_path, new_path, names = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
    read = metal_kernels if backend == "metal" else cuda_kernels
    old, new = read(old_path), read(new_path)
    names = names or sorted(n for n in old if n in new)
    same, differ, missing = [], [], []
    for n in names:
        if n not in old or n not in new:
            missing.append(n)
        elif old[n] == new[n]:
            same.append(n)
        else:
            differ.append(n)
    for n in differ:
        print("DIFFERENT", n)
    for n in missing:
        print("MISSING", n)
    print(f"{len(same)} identical, {len(differ)} different, {len(missing)} missing")
    sys.exit(1 if differ or missing else 0)


if __name__ == "__main__":
    main()
