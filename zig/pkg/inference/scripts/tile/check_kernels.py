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

"""Differential check of GPU kernels across two source versions, on the CPU.

Extracts the named kernels (plus the helper functions and params structs
they use) from an old and a new Metal (`metal_kernels.m`) or CUDA
(`inference_cuda_kernels.cu`) source, compiles both as C++ against a small
shim, runs every launch with one OS thread per GPU thread (barriers, warp
shuffles and `simd_sum` are real rendezvous points), and requires every
output buffer to match bit for bit. Families and their launch grids live in
kernel_specs.py. Needs clang++ (C++20); no GPU or vendor toolchain.

    python check_kernels.py metal rms_norm <old metal_kernels.m> <new metal_kernels.m>
    python check_kernels.py cuda layer_norm <old .cu> <new .cu>
"""

import os
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kernel_specs  # noqa: E402

COMMON = r"""
#include <barrier>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>
typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned char uchar;
// One block runs at a time.
static std::barrier<> *g_barrier;
static float g_lane_f[1024];
static uint g_lane_u[1024];
static thread_local uint t_tid;
template <class T> static T *lane_slot();
template <> float *lane_slot<float>() { return g_lane_f; }
template <> uint *lane_slot<uint>() { return g_lane_u; }
/// Value of lane (lane + delta) of the caller's 32-lane group, or its own.
template <class T> static T shuffle_down(T v, uint delta) {
    g_barrier->arrive_and_wait();
    lane_slot<T>()[t_tid] = v;
    g_barrier->arrive_and_wait();
    uint lane = t_tid & 31u;
    T r = lane + delta < 32u ? lane_slot<T>()[t_tid + delta] : v;
    g_barrier->arrive_and_wait();
    return r;
}
template <class T> static T shuffle_xor(T v, uint mask) {
    g_barrier->arrive_and_wait();
    lane_slot<T>()[t_tid] = v;
    g_barrier->arrive_and_wait();
    T r = lane_slot<T>()[(t_tid & ~31u) | ((t_tid & 31u) ^ mask)];
    g_barrier->arrive_and_wait();
    return r;
}
static float group_sum(float v) {
    g_barrier->arrive_and_wait();
    g_lane_f[t_tid] = v;
    g_barrier->arrive_and_wait();
    float s = 0.0f;
    for (uint i = 0; i < 32u; ++i) s += g_lane_f[(t_tid & ~31u) + i];
    g_barrier->arrive_and_wait();
    return s;
}
static float group_max(float v) {
    g_barrier->arrive_and_wait();
    g_lane_f[t_tid] = v;
    g_barrier->arrive_and_wait();
    float s = g_lane_f[t_tid & ~31u];
    for (uint i = 1; i < 32u; ++i) s = std::fmax(s, g_lane_f[(t_tid & ~31u) + i]);
    g_barrier->arrive_and_wait();
    return s;
}
"""

METAL_SHIM = r"""
typedef _Float16 half;
struct float4 {
    float x, y, z, w;
    float4() : x(0), y(0), z(0), w(0) {}
    explicit float4(float s) : x(s), y(s), z(s), w(s) {}
    float4(float a, float b, float c, float d) : x(a), y(b), z(c), w(d) {}
    float4 &operator*=(float s) { x *= s; y *= s; z *= s; w *= s; return *this; }
};
static inline float4 operator+(float4 a, float4 b) { return float4(a.x + b.x, a.y + b.y, a.z + b.z, a.w + b.w); }
static inline float4 operator*(float4 a, float4 b) { return float4(a.x * b.x, a.y * b.y, a.z * b.z, a.w * b.w); }
static inline float4 operator*(float s, float4 a) { return float4(s * a.x, s * a.y, s * a.z, s * a.w); }
static inline float4 operator*(float4 a, float s) { return float4(a.x * s, a.y * s, a.z * s, a.w * s); }
static inline float dot(float4 a, float4 b) { return a.x * b.x + a.y * b.y + a.z * b.z + a.w * b.w; }
static inline float4 max(float4 a, float4 b) { return float4(std::fmax(a.x, b.x), std::fmax(a.y, b.y), std::fmax(a.z, b.z), std::fmax(a.w, b.w)); }
static inline float4 min(float4 a, float4 b) { return float4(std::fmin(a.x, b.x), std::fmin(a.y, b.y), std::fmin(a.z, b.z), std::fmin(a.w, b.w)); }
static inline float max(float a, float b) { return std::fmax(a, b); }
static inline float min(float a, float b) { return std::fmin(a, b); }
static inline uint max(uint a, uint b) { return a > b ? a : b; }
static inline uint min(uint a, uint b) { return a < b ? a : b; }
static inline float rsqrt(float x) { return 1.0f / std::sqrt(x); }
using std::exp; using std::log; using std::isnan; using std::isinf; using std::sqrt; using std::fabs;
struct uint3 { uint x, y, z; };
struct ushort3 { ushort x, y, z; };
namespace mem_flags { enum { mem_none = 0, mem_threadgroup = 1, mem_device = 2 }; }
static inline void threadgroup_barrier(int) { g_barrier->arrive_and_wait(); }
static inline float simd_sum(float v) { return group_sum(v); }
static inline float simd_max(float v) { return group_max(v); }
template <class T> static T simd_shuffle_down(T v, uint d) { return shuffle_down<T>(v, d); }
template <class T> static T simd_shuffle_xor(T v, uint m) { return shuffle_xor<T>(v, m); }
template <class T, class U> static T as_type(U v) { T t; std::memcpy(&t, &v, sizeof t); return t; }
"""

CUDA_SHIM = r"""
struct dim3_ { unsigned int x, y, z; };
static thread_local dim3_ threadIdx, blockIdx;
static dim3_ blockDim, gridDim;
static inline void __syncthreads() { g_barrier->arrive_and_wait(); }
static inline float rsqrtf(float v) { return 1.0f / std::sqrt(v); }
static inline unsigned int __float_as_uint(float v) { unsigned int u; std::memcpy(&u, &v, 4); return u; }
static inline float __uint_as_float(unsigned int u) { float v; std::memcpy(&v, &u, 4); return v; }
static inline float __int_as_float(int u) { float v; std::memcpy(&v, &u, 4); return v; }
template <class T> static T __shfl_down_sync(unsigned int, T v, unsigned int d) { return shuffle_down<T>(v, d); }
template <class T> static T __shfl_xor_sync(unsigned int, T v, unsigned int m) { return shuffle_xor<T>(v, m); }
#include <math.h>
"""


def unstring(line):
    m = re.match(r'^\s*"(.*)"\s*$', line)
    if not m:
        return None
    body = m.group(1)
    if body.endswith("\\n"):
        body = body[:-2]
    return body.replace('\\"', '"').replace("\\\\", "\\")


def braced(text, start):
    """End index (inclusive) of the brace block opening at or after start."""
    i = text.index("{", start)
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return j
    raise ValueError("unbalanced braces")


def metal_source(path):
    """All MSL embedded in a metal_kernels.m, joined."""
    lines = [unstring(l) for l in open(path).read().split("\n")]
    return "\n".join(l for l in lines if l is not None)


def collect(src, names, kernel_re):
    """Kernel texts plus the helper functions and structs they reference."""
    kernels = {}
    for n in names:
        m = re.search(kernel_re.format(re.escape(n)), src)
        if not m:
            sys.exit(f"missing kernel {n}")
        kernels[n] = src[m.start(): braced(src, m.end()) + 1]
    helpers, structs = {}, {}
    for m in re.finditer(r"(?m)^(?:static\s+)?(?:inline|__device__(?:\s+__forceinline__)?)\s+[\w ]+?\b(\w+)\s*\(", src):
        helpers.setdefault(m.group(1), src[m.start(): braced(src, m.end()) + 1])
    for m in re.finditer(r"(?m)^(?:typedef\s+)?struct\s+(\w+)\s*\{[^{}]*\}\s*\w*\s*;", src):
        structs.setdefault(m.group(1), m.group(0))
    need, order = [], []
    pending = list(kernels.values())
    while pending:
        text = pending.pop()
        for word in set(re.findall(r"\b(\w+)\b", text)):
            if word in helpers and word not in need:
                need.append(word)
                pending.append(helpers[word])
            if word in structs and word not in order:
                order.append(word)
    return kernels, [helpers[h] for h in reversed(need)], [structs[s] for s in order]


BUILTIN = {
    "thread_position_in_grid": "gid",
    "thread_index_in_threadgroup": "tid",
    "threads_per_threadgroup": "tpg",
    "threadgroup_position_in_grid": "group",
    "simdgroup_index_in_threadgroup": "tid / 32u",
    "thread_index_in_simdgroup": "tid % 32u",
    "simdgroups_per_threadgroup": "(tpg + 31u) / 32u",
    "threads_per_simdgroup": "32u",
}


def metal_to_cpp(text):
    head, rest = text.split("{", 1)
    sig = head[head.index("(") + 1: head.rindex(")")]
    params, cpp = [], []
    for p in [s.strip() for s in sig.split(",")]:
        attr = re.search(r"\[\[(.*)\]\]", p)
        attr = attr.group(1) if attr else ""
        decl = re.sub(r"\s*\[\[.*\]\]", "", p)
        name = re.search(r"(\w+)\s*$", decl).group(1)
        base = re.sub(r"^(device|constant|threadgroup)\s+", "", decl[: decl.rindex(name)]).strip()
        if attr.startswith("buffer("):
            params.append({"kind": "buffer", "slot": int(attr[7:-1]), "name": name, "type": base.replace("const", "").replace("*", "").replace("&", "").strip(), "out": "const" not in base and "*" in base, "ref": "&" in base})
            cpp.append(f"{base} {name}")
        elif attr.startswith("threadgroup("):
            params.append({"kind": "shared", "slot": int(attr[12:-1]), "name": name, "type": base.replace("*", "").strip()})
            cpp.append(f"{base} {name}")
        else:
            expr = BUILTIN[attr]
            vec = base.endswith("3")
            params.append({"kind": "builtin", "expr": expr, "type": base, "vec": vec})
            cpp.append(f"{base} {name}")
    body = re.sub(r"\bthreadgroup ([\w ]+?) (\w+)\[", r"static \1 \2[", rest)
    body = re.sub(r"\bthreadgroup\b\s+", "", body)
    body = re.sub(r"\bdevice volatile\b", "volatile", body)
    body = re.sub(r"\b(device|constant|thread)\b\s*", "", body)
    name = re.search(r"kernel void (\w+)", head).group(1)
    return f"void {name}({', '.join(cpp)}) {{{body}", params


def cuda_to_cpp(text):
    text = text.replace('extern "C" ', "").replace("__global__ ", "")
    text = re.sub(r"__launch_bounds__\([^)]*\)\s*", "", text)
    text = re.sub(r"__shared__ ([\w ]+?) (\w+)\[", r"static \1 \2[", text)
    head = text[: text.index("{")]
    sig = head[head.index("(") + 1: head.rindex(")")]
    params = []
    for p in [" ".join(s.split()) for s in sig.split(",")]:
        name = re.search(r"(\w+)$", p).group(1)
        ty = p[: -len(name)].strip()
        params.append({"kind": "buffer" if "*" in ty else "scalar", "name": name, "type": ty.replace("const", "").replace("*", "").replace("__restrict__", "").strip(), "out": "*" in ty and "const" not in ty})
    return text, params


def strip_device(text):
    return re.sub(r"\b(__device__|__forceinline__|__host__)\b\s*", "", text)


def main():
    backend, family, old_path, new_path = sys.argv[1:5]
    spec = getattr(kernel_specs, family)
    names = list(spec["kernels"].keys())
    grid = spec["grid"]
    if backend == "metal":
        names = [n for n in names if spec["kernels"][n].get("backend", "metal") == "metal"]
        old_src, new_src = metal_source(old_path), metal_source(new_path)
        kernel_re = r"kernel void {}\("
    else:
        names = [n for n in names if spec["kernels"][n].get("backend") == "cuda"]
        old_src, new_src = open(old_path).read(), open(new_path).read()
        kernel_re = r'extern "C" __global__\s+(?:void\s+)?(?:__launch_bounds__\([^)]*\)\s*)?(?:void\s+)?{}\s*\('
    src = COMMON + (METAL_SHIM if backend == "metal" else CUDA_SHIM)
    sigs = {}
    for ns, text in (("before", old_src), ("after", new_src)):
        kernels, helpers, structs = collect(text, names, kernel_re)
        src += f"namespace {ns} {{\n" + "\n".join(structs) + "\n"
        src += "\n".join(strip_device(h) if backend == "cuda" else re.sub(r"\b(device|constant|thread)\b\s*", "", h) for h in helpers) + "\n"
        for n in names:
            cpp, params = (metal_to_cpp if backend == "metal" else cuda_to_cpp)(kernels[n])
            src += cpp + "\n"
            sigs[(ns, n)] = params
        src += "}\n"

    out = ["int main() {", "    int checks = 0, failures = 0;"]
    loops = "".join(f"for (long {k} : {{{', '.join(str(x) for x in v)}}}) " for k, v in grid.items())
    for n in names:
        k = spec["kernels"][n]
        pa, pb = sigs[("before", n)], sigs[("after", n)]
        strip = lambda ps: [(p["kind"], p.get("slot"), p.get("name") if backend == "cuda" else None, p.get("type")) for p in ps if p["kind"] != "builtin"]
        if strip(pa) != strip(pb):
            sys.exit(f"{n}: ABI differs between versions:\n {strip(pa)}\n {strip(pb)}")
        out.append(f'    {{ const char *name = "{n}"; {loops}{{')
        out.append(f"        if (!({k.get('when', 'true')})) continue;")
        out.append("        std::mt19937 rng((unsigned)(rows * 7919 + dim * 131 + threads * 17 + variant));")
        out.append("        std::vector<std::shared_ptr<std::vector<char>>> keep;")
        out.append("        auto alloc = [&](size_t bytes) { keep.push_back(std::make_shared<std::vector<char>>(bytes + 64, 0)); return (void *)keep.back()->data(); };")
        for ns in ("before", "after"):
            out.append(f"        void *{ns}_buf[32] = {{}};")
        bufs = [p for p in pa if p["kind"] == "buffer" and not p.get("ref")]
        for p in bufs:
            key = p["slot"] if backend == "metal" else p["name"]
            bspec = k["buffers"].get(p["name"])
            if bspec is None:
                sys.exit(f"{n}: no spec for buffer {p['name']}")
            count, fill = bspec
            ty = p["type"]
            idx = p["slot"] if backend == "metal" else bufs.index(p)
            if p["out"]:
                sentinel = {"float": "-12345.0f", "uint": "0xA5A5A5A5u", "unsigned int": "0xA5A5A5A5u", "unsigned short": "(unsigned short)0xBEEF", "int": "-7"}[ty]
                out.append(f"        {{ size_t n = (size_t)({count}); for (const char *ns : {{\"a\", \"b\"}}) {{ {ty} *q = ({ty} *)alloc(n * sizeof({ty})); for (size_t i = 0; i < n; ++i) q[i] = {sentinel}; if (ns[0] == 'a') before_buf[{idx}] = q; else after_buf[{idx}] = q; }} }}")
            else:
                out.append(f"        {{ size_t n = (size_t)({count}); {ty} *q = ({ty} *)alloc(n * sizeof({ty}) + 4); for (size_t i = 0; i < n; ++i) {{ {fill}; }} before_buf[{idx}] = after_buf[{idx}] = q; }}")
        if backend == "metal":
            prm = next((p for p in pa if p["kind"] == "buffer" and p["ref"]), None)
            for p in pa:
                if p["kind"] == "buffer" and p["ref"]:
                    ty = p["type"]
                    if p["name"] in k.get("scalars", {}):
                        out.append(f"        {{ {ty} *v = ({ty} *)alloc(sizeof({ty})); *v = ({ty})({k['scalars'][p['name']]}); before_buf[{p['slot']}] = after_buf[{p['slot']}] = v; }}")
                    else:
                        out.append(f"        {{ before::{ty} *v = (before::{ty} *)alloc(sizeof(before::{ty})); std::memset(v, 0, sizeof *v); " + " ".join(f"v->{f} = {e};" for f, e in k["params"].items()) + f" before_buf[{p['slot']}] = after_buf[{p['slot']}] = v; }}")
        groups_x, groups_y = k["launch"]["groups"] if isinstance(k["launch"]["groups"], tuple) else (k["launch"]["groups"], "1")
        threads = k["launch"].get("threads", "threads")
        out.append(f"        unsigned int gx = (unsigned int)({groups_x}), gy = (unsigned int)({groups_y}), tx = (unsigned int)({threads});")
        for ns in ("before", "after"):
            params = sigs[(ns, n)]
            args = []
            for p in params:
                if p["kind"] == "builtin":
                    e = p["expr"]
                    if p["vec"]:
                        fields = {"gid": "gid, group_y, 0", "tid": "tid, 0, 0", "tpg": "tpg, 1, 1", "group": "group, group_y, 0"}[e]
                        elem = p["type"][:-1]
                        args.append(f"{p['type']}{{" + ", ".join(f"({elem})({f})" for f in fields.split(", ")) + "}")
                    else:
                        args.append(f"({p['type']})({e})")
                elif p["kind"] == "shared":
                    args.append(f"({p['type']} *)shared[{p['slot']}].data()")
                elif p["kind"] == "scalar":
                    args.append(f"({p['type']})({k['scalars'][p['name']]})")
                else:
                    idx = p["slot"] if backend == "metal" else [q for q in params if q["kind"] == "buffer"].index(p)
                    cast = (f"*({ns}::{p['type']} *)" if p["ref"] and p["name"] not in k.get("scalars", {}) else f"*({p['type']} *)") if backend == "metal" and p["ref"] else f"({p['type']} *)"
                    args.append(f"{cast}{ns}_buf[{idx}]")
            call = f"{ns}::{n}({', '.join(args)})"
            if backend == "metal":
                body = f"uint tid = t; uint tpg = tx; uint gid = group * tx + t; t_tid = t; (void)tid; (void)tpg; (void)gid; {call};"
            else:
                body = f"threadIdx = {{t, 0, 0}}; blockIdx = {{group, group_y, 0}}; t_tid = t; {call};"
            out.append(f"""        {{ {"blockDim = {tx, 1, 1}; gridDim = {gx, gy, 1};" if backend == "cuda" else ""}
            std::vector<std::vector<char>> shared(4, std::vector<char>(65536, 0));
            for (unsigned int group_y = 0; group_y < gy; ++group_y) for (unsigned int group = 0; group < gx; ++group) {{
                for (auto &s : shared) std::memset(s.data(), 0, s.size());
                std::barrier<> bar(tx); g_barrier = &bar; std::vector<std::thread> ts;
                for (unsigned int t = 0; t < tx; ++t) ts.emplace_back([&, t] {{ {body} bar.arrive_and_drop(); }});
                for (auto &th : ts) th.join();
            }} }}""")
        for p in bufs:
            if not p["out"]:
                continue
            idx = p["slot"] if backend == "metal" else bufs.index(p)
            count = k["buffers"][p["name"]][0]
            ty = p["type"]
            out.append(f'        ++checks; if (std::memcmp(before_buf[{idx}], after_buf[{idx}], (size_t)({count}) * sizeof({ty})) != 0) {{ ++failures; std::printf("MISMATCH %s {p["name"]} rows=%ld dim=%ld threads=%ld variant=%ld\\n", name, rows, dim, threads, variant); }}')
        if "oracle" in k:
            out.append("        " + k["oracle"])
        out.append("    } }")
    out += ['    std::printf("%d output comparisons, %d mismatches\\n", checks, failures);', "    return failures != 0;", "}"]
    src += "\n".join(out) + "\n"
    work = tempfile.mkdtemp(prefix=f"check_{family}_")
    cpp, exe = os.path.join(work, "check.cpp"), os.path.join(work, "check")
    open(cpp, "w").write(src)
    flags = os.environ.get("CHECK_FLAGS", "-O1 -ffp-contract=off").split()
    r = subprocess.run(["clang++", "-std=c++20", "-pthread", "-Wno-unknown-attributes", "-Wno-unused-value", *flags, cpp, "-o", exe], capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[:8000])
        sys.exit(f"compile failed; source kept at {cpp}")
    r = subprocess.run([exe], capture_output=True, text=True)
    print(r.stdout[-6000:])
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
