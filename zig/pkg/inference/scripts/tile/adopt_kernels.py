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

"""Hand a family of hand-written kernels over to the tile generator.

Removes the named kernels from metal_kernels.m (C-string MSL) and
inference_cuda_kernels.cu, and leaves an empty `tile-codegen` marker region
where the first one was. Then `zig build tile-kernel-codegen -- --write`
fills the regions and scripts/tile/check_kernels.py compares old and new.

    python adopt_kernels.py <family> <kernel> [<kernel> ...]
"""

import re
import sys

METAL = "src/backends/metal_kernels.m"
CUDA = "src/ops/cuda/artifacts/inference_cuda_kernels.cu"


def marker(indent, family):
    return [f"{indent}// tile-codegen:begin {family} family (do not edit; run: zig build tile-kernel-codegen -- --write)", f"{indent}// tile-codegen:end {family} family"]


def adopt_metal(family, names):
    lines = open(METAL).read().split("\n")
    drop, first = set(), None
    for n in names:
        hits = [i for i, l in enumerate(lines) if re.match(r'^\s*"kernel void ' + re.escape(n) + r"\(", l)]
        if not hits:
            continue
        if len(hits) != 1:
            sys.exit(f"{n}: {len(hits)} definitions")
        i = j = hits[0]
        while lines[j].strip() != '"}\\n"':
            j += 1
        drop.update(range(i, j + 1))
        first = i if first is None else min(first, i)
    if first is None:
        return 0
    out = []
    for i, l in enumerate(lines):
        if i == first:
            out += marker("           ", family)
        if i not in drop:
            out.append(l)
    open(METAL, "w").write("\n".join(out))
    return len(drop)


def adopt_cuda(family, names):
    text = open(CUDA).read()
    spans = []
    for n in names:
        m = re.search(r'(?m)^extern "C" __global__\s+(?:void\s+)?(?:__launch_bounds__\([^)]*\)\s*)?(?:void\s+)?' + re.escape(n) + r"\s*\(", text)
        if not m:
            continue
        start = m.start()
        # Keep comment lines directly above the kernel with it.
        while True:
            prev = text.rfind("\n", 0, start - 1)
            line = text[prev + 1: start - 1] if start > 0 else ""
            if line.startswith("//") and "codegen:" not in line:
                start = prev + 1
            else:
                break
        depth, j = 0, text.index("{", m.end())
        while True:
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        end = j + 1
        if text[end: end + 2] == "\n\n":
            end += 1
        spans.append((start, end))
    if not spans:
        return 0
    spans.sort()
    first = spans[0][0]
    out, pos = [], 0
    for a, b in spans:
        out.append(text[pos:a])
        if a == first:
            out.append("\n".join(marker("", family)) + "\n")
        pos = b + 1 if text[b: b + 1] == "\n" else b
    out.append(text[pos:])
    open(CUDA, "w").write("".join(out))
    return len(spans)


if __name__ == "__main__":
    family, names = sys.argv[1], sys.argv[2:]
    print(f"metal: removed {adopt_metal(family, names)} lines; cuda: removed {adopt_cuda(family, names)} kernels")
