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

"""Check CUDA kernel wrappers against the hand-written kernels they replaced.

Each `extern "C"` kernel whose body is a call to a `*_impl<...>` template
must keep its old signature, pass its parameters through in order, and the
template body with the call's arguments substituted must equal the old
kernel body (array bounds `[4u]` and `[4]` count as equal).

    python3 -I verify_cuda_templates.py <old .cu> <new .cu>
"""
import re, sys

old, new = open(sys.argv[1]).read(), open(sys.argv[2]).read()
HDR = re.compile(r'(?m)^extern "C" __global__\s+(?:void\s+)?(?:__launch_bounds__\([^)]*\)\s*)?(?:void\s+)?(\w+)\s*\(')
def block(t, i):
    d = 0
    while True:
        if t[i] == "{": d += 1
        elif t[i] == "}":
            d -= 1
            if d == 0: return i
        i += 1
def kernels(t):
    out = {}
    for m in HDR.finditer(t):
        b = t.index("{", m.end()); out[m.group(1)] = (t[m.start():b], t[b:block(t, b) + 1])
    return out
O, N = kernels(old), kernels(new)
impls = {}
for m in re.finditer(r"template <([^>]*)>\n__device__ __forceinline__ void (\w+_impl)\(", new):
    po = m.end() - 1; d = 0; j = po
    while True:
        if new[j] == "(": d += 1
        elif new[j] == ")":
            d -= 1
            if d == 0: break
        j += 1
    b = new.index("{", j)
    impls[m.group(2)] = ([x.split()[-1] for x in m.group(1).split(",")], new[po + 1:j], new[b:block(new, b) + 1])
norm = lambda x: re.sub(r"\[(\d+)u\]", r"[\1]", x)
ok = bad = 0
for name, (hdr, body) in N.items():
    w = re.fullmatch(r"\{ (\w+_impl)<([^>]*)>\(([^)]*)\); \}", body)
    if not w: continue
    impl, args, call = w.group(1), [a.strip() for a in w.group(2).split(",")], [a.strip() for a in w.group(3).split(",")]
    tparams, plist, ibody = impls[impl]
    exp_b, exp_p = ibody, plist
    for p, a in zip(tparams, args):
        exp_b = re.sub(r"\b" + p + r"\b", a, exp_b); exp_p = re.sub(r"\b" + p + r"\b", a, exp_p)
    ohdr, obody = O[name]
    pnames = [re.search(r"(\w+)\s*$", " ".join(x.split())).group(1) for x in exp_p.split(",") if x.strip()]
    good = hdr == ohdr and "(" + exp_p + ")" in ohdr and call == pnames and norm(exp_b) == norm(obody)
    if good: ok += 1
    else: bad += 1; print("MISMATCH", name)
print(f"{ok} wrappers expand to their old kernels, {bad} mismatched")
sys.exit(1 if bad else 0)
