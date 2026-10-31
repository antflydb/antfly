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

"""Check MSL kernel templates against the hand-written kernels they replaced.

For each `template <...>` kernel in the new metal_kernels.m, substitutes the
arguments of every `template [[host_name("x")]]` instance and requires the
result to equal kernel `x` in the old file, character for character.

    python3 -I verify_msl_templates.py <old metal_kernels.m> <new metal_kernels.m>
"""
import re, sys
STR = re.compile(r'^(\s*)"(.*)\\n"$')
def unq(lines): return [STR.match(l).group(2) for l in lines]
old = open(sys.argv[1]).read().split("\n"); new = open(sys.argv[2]).read().split("\n")
def old_kernel(name):
    for i, l in enumerate(old):
        if re.match(r'^\s*"kernel void ' + name + r"\(", l):
            j = i
            while old[j].strip() != '"}\\n"': j += 1
            return "\n".join(unq(old[i:j + 1]))
    return None
checked = bad = skipped = 0
for i, l in enumerate(new):
    m = re.match(r'^\s*"template <([^>]*)>\\n"$', l)
    if not m: continue
    params = [p.split()[-1] for p in m.group(1).split(",")]
    j = i + 1
    while new[j].strip() != '"}\\n"': j += 1
    body = "\n".join(unq(new[i + 1:j + 1]))
    km = re.match(r"kernel void (\w+)\(", body)
    if not km: continue
    tname = km.group(1)
    k = j + 1
    while k < len(new) and ("host_name" in new[k] or "typedef decltype" in new[k]):
        im = re.search(r'host_name\(\\"(\w+)\\"\)\]\] kernel \w+ ' + tname + r"<(.*)>;", new[k])
        if im:
            host, args = im.group(1), [a.strip() for a in im.group(2).split(",")]
            assert len(args) == len(params)
            exp = body.replace("kernel void " + tname + "(", "kernel void " + host + "(", 1)
            for p, a in zip(params, args): exp = re.sub(r"\b" + p + r"\b", a, exp)
            orig = old_kernel(host)
            if orig is None: skipped += 1
            elif orig == exp: checked += 1
            else: bad += 1; print("MISMATCH", host)
        k += 1
print(f"{checked} instances identical to the old kernels, {bad} mismatched, {skipped} without an old kernel")
sys.exit(1 if bad else 0)
