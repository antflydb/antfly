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

"""Differential check of Metal kernels across two versions of metal_kernels.m.

Extracts the named kernels' MSL from both files, compiles each set as C++
against a small Metal shim, and runs every kernel on the CPU with one OS
thread per GPU thread (threadgroup barriers and simd_sum are real
rendezvous points). Outputs must match bit for bit across a grid of shapes
and threadgroup sizes. Needs clang++ (C++20); no Metal toolchain.

    python check_metal_rms_norm.py <old metal_kernels.m> <new metal_kernels.m>
    (e.g. old = `git show origin/main:zig/pkg/inference/src/backends/metal_kernels.m`)
"""

import os
import re
import subprocess
import sys
import tempfile

KERNELS = [
    "termite_apply_rms_norm_1x",
    "termite_apply_rms_norm_1x_reduce",
    "termite_compute_rms_inv_scale_1x",
    "termite_apply_rms_norm_rows",
    "termite_apply_rms_norm_rows_reduce",
    "termite_apply_rms_norm_rows_simd4_reduce",
    "antfly_norm_clamp",
    "termite_apply_rms_norm_rows_triple_simd4_reduce",
    "termite_apply_rms_norm_add_1x",
    "termite_apply_rms_norm_add_rows_canonical",
    "termite_apply_rms_norm_add_1x_sumsq",
    "termite_apply_rms_norm_add_rows_sumsq",
    "termite_apply_rms_norm_add_scale_1x_sumsq",
    "termite_apply_rms_norm_add_1x_f16_input",
    "termite_apply_rms_norm_add_scale_1x",
    "termite_apply_rms_norm_add_scale_rows",
    "termite_apply_rms_norm_add_scale_simd4",
]

SHIM = r"""
#include <barrier>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <memory>
#include <random>
#include <thread>
#include <vector>
typedef unsigned int uint;
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
static inline float rsqrt(float x) { return 1.0f / std::sqrt(x); }
namespace mem_flags { enum { mem_threadgroup = 1, mem_device = 2, mem_none = 0 }; }
// One threadgroup runs at a time.
static std::barrier<> *g_barrier;
static float g_simd_scratch[1024];
static thread_local uint t_tid;
static inline void threadgroup_barrier(int) { g_barrier->arrive_and_wait(); }
static inline float simd_sum(float v) {
    g_barrier->arrive_and_wait();
    g_simd_scratch[t_tid] = v;
    g_barrier->arrive_and_wait();
    float s = 0.0f;
    uint base = t_tid & ~31u;
    for (uint i = 0; i < 32u; ++i) s += g_simd_scratch[base + i];
    g_barrier->arrive_and_wait();
    return s;
}
struct termite_metal_apply_layer_norm_params { uint hidden_size; float eps; };
struct termite_metal_apply_layer_norm_scale_params { uint hidden_size; float eps; float scale; uint reserved; };
struct termite_metal_apply_row_norm_params { uint rows; uint hidden_size; float eps; };
struct termite_metal_apply_row_norm_scale_params { uint rows; uint hidden_size; float eps; float scale; };
"""


def unstring(line):
    m = re.match(r'^\s*"(.*)"\s*$', line)
    if not m:
        return None
    body = m.group(1)
    if body.endswith("\\n"):
        body = body[:-2]
    return body.replace('\\"', '"').replace("\\\\", "\\")


def extract(path):
    """MSL text of each kernel in KERNELS, plus antfly_norm_clamp_params."""
    lines = [unstring(l) for l in open(path).read().split("\n")]
    out, struct = {}, None
    for i, l in enumerate(lines):
        if l is None:
            continue
        if "struct antfly_norm_clamp_params" in l:
            struct = l[l.index("struct antfly_norm_clamp_params"):]
        for n in KERNELS:
            if f"kernel void {n}(" in l:
                body, j = [l[l.index("kernel void"):]], i + 1
                while lines[j] != "}":
                    if lines[j] is not None:
                        body.append(lines[j])
                    j += 1
                body.append("}")
                out[n] = "\n".join(body)
    missing = [n for n in KERNELS if n not in out]
    if missing:
        sys.exit(f"{path}: missing kernels {missing}")
    return out, struct


BUILTINS = {
    "thread_position_in_grid": "gid",
    "thread_index_in_threadgroup": "tid",
    "threads_per_threadgroup": "tpg",
    "threadgroup_position_in_grid": "group",
    "simdgroup_index_in_threadgroup": "tid / 32u",
    "thread_index_in_simdgroup": "tid % 32u",
    "threadgroup(0)": "shared",
}


def to_cpp(msl):
    """MSL kernel -> (C++ function, parameter descriptions)."""
    head, rest = msl.split("{", 1)
    sig = head[head.index("(") + 1: head.rindex(")")]
    params = []
    for p in sig.split(","):
        p = p.strip()
        attr = re.search(r"\[\[(.*)\]\]", p)
        attr = attr.group(1) if attr else ""
        decl = re.sub(r"\s*\[\[.*\]\]", "", p)
        name = re.search(r"(\w+)\s*$", decl).group(1)
        if attr.startswith("buffer("):
            kind = "params" if decl.startswith("constant") else ("out" if "const" not in decl else "in")
            ty = re.sub(r"^(device|constant)\s+(const\s+)?", "", decl)
            ty = ty[: ty.rindex(name)].replace("*", "").replace("&", "").strip()
            params.append({"kind": kind, "type": ty, "name": name, "slot": int(attr[7:-1])})
        else:
            params.append({"kind": "builtin", "expr": BUILTINS[attr], "name": name})
    cpp_sig = []
    for p in params:
        if p["kind"] == "builtin":
            cpp_sig.append(("float *" if p["expr"] == "shared" else "uint ") + p["name"])
        elif p["kind"] == "params":
            cpp_sig.append(f"const {p['type']} &{p['name']}")
        elif p["kind"] == "in":
            cpp_sig.append(f"const {p['type']} *{p['name']}")
        else:
            cpp_sig.append(f"{p['type']} *{p['name']}")
    body = rest
    body = re.sub(r"\bthreadgroup float (\w+)\[", r"static float \1[", body)
    body = re.sub(r"\bdevice volatile\b", "volatile", body)
    body = re.sub(r"\bdevice\b\s*", "", body)
    name = re.search(r"kernel void (\w+)", head).group(1)
    return f"void {name}({', '.join(cpp_sig)}) {{{body}", params


def launcher(name, params, ns):
    """C++ function running one launch: groups x tpg threads."""
    args = []
    for p in params:
        if p["kind"] == "builtin":
            args.append(p["expr"] if p["expr"] != "shared" else "shared")
        elif p["kind"] == "params":
            args.append("*(const " + p["type"] + " *)bufs[" + str(p["slot"]) + "]")
        else:
            args.append("(" + p["type"] + " *)bufs[" + str(p["slot"]) + "]")
    call = f"{ns}::{name}({', '.join(args)})"
    return f"""
static void run_{ns}_{name}(void **bufs, uint groups, uint tpg) {{
    std::vector<float> shared_mem(4096, 0.0f);
    for (uint group = 0; group < groups; ++group) {{
        std::barrier<> bar(tpg);
        g_barrier = &bar;
        float *shared = shared_mem.data();
        std::vector<std::thread> threads;
        for (uint t = 0; t < tpg; ++t) threads.emplace_back([&, t] {{
            uint tid = t; t_tid = t; uint gid = group * tpg + t;
            (void)gid; (void)tid; (void)shared;
            {call};
            bar.arrive_and_drop();
        }});
        for (auto &th : threads) th.join();
    }}
}}
"""


def main():
    old_path, new_path = sys.argv[1:3]
    old, old_struct = extract(old_path)
    new, new_struct = extract(new_path)
    src = SHIM + (old_struct or new_struct) + "\n"
    sigs = {}
    for ns, kernels in (("before", old), ("after", new)):
        src += f"namespace {ns} {{\n"
        for n in KERNELS:
            cpp, params = to_cpp(kernels[n])
            src += cpp + "\n"
            sigs[(ns, n)] = params
        src += "}\n"
    for (ns, n), params in sigs.items():
        src += launcher(n, params, ns)

    # Test driver: identical random inputs for both versions, compare outputs.
    driver = ["static int failures = 0, checks = 0;"]
    driver.append("""
static void fill(float *p, size_t n, std::mt19937 &rng, float lo, float hi) { std::uniform_real_distribution<float> d(lo, hi); for (size_t i = 0; i < n; ++i) p[i] = d(rng); }
""")
    driver.append("int main() {")
    for n in KERNELS:
        params = sigs[("before", n)]
        if [ (p["kind"], p.get("slot"), p.get("type")) for p in params if p["kind"] != "builtin"] != [ (p["kind"], p.get("slot"), p.get("type")) for p in sigs[("after", n)] if p["kind"] != "builtin"]:
            sys.exit(f"{n}: buffer ABI differs between versions")
        builtins = {p["expr"] for p in params if p["kind"] == "builtin"}
        ptype = next(p["type"] for p in params if p["kind"] == "params")
        bufs = [p for p in params if p["kind"] in ("in", "out")]
        nslots = max(p["slot"] for p in params if p["kind"] != "builtin") + 1
        simd4 = "tid / 32u" in builtins
        serial = "gid" in builtins
        has_rows_field = "row_norm" in ptype or "clamp" in ptype
        uses_group = "group" in builtins
        dims = "{4, 6, 8, 12, 64, 384, 1024}" if simd4 else "{1, 3, 7, 64, 100, 384, 1000}"
        tpgs = "{32u, 64u, 128u, 256u}" if not serial else "{1u}"
        driver.append(f'    {{ const char *name = "{n}"; for (uint rows : {{1u, 3u}}) for (uint dim : {dims}) for (uint tpg0 : {tpgs}) for (int variant = 0; variant < 3; ++variant) {{')
        driver.append("        std::mt19937 rng(dim * 131u + rows * 7u + tpg0 + variant);")
        driver.append("        uint R = rows; uint partials = (dim + 7u) / 8u;")
        if not has_rows_field and not uses_group:
            driver.append("        R = 1u;")
        driver.append(f"        void *bufs_a[{nslots}] = {{}}; void *bufs_b[{nslots}] = {{}};")
        driver.append("        std::vector<std::unique_ptr<char[]>> keep;")
        driver.append("        auto alloc = [&](size_t bytes) { keep.emplace_back(new char[bytes]); return (void *)keep.back().get(); };")
        for p in bufs:
            nm, slot, ty = p["name"], p["slot"], p["type"]
            if p["kind"] == "out":
                size = "1" if nm == "inv_scale" else "R * dim"
                driver.append(f"        {{ size_t n = {size}; float *a = (float *)alloc(n * 4), *b = (float *)alloc(n * 4); for (size_t i = 0; i < n; ++i) a[i] = b[i] = -12345.0f; bufs_a[{slot}] = a; bufs_b[{slot}] = b; }}")
            elif nm == "sumsq":
                driver.append(f"        {{ float *s = (float *)alloc(R * partials * 4); fill(s, R * partials, rng, 0.0f, 3.0f); bufs_a[{slot}] = bufs_b[{slot}] = s; }}")
            elif ty == "half":
                driver.append(f"        {{ half *h = (half *)alloc(R * dim * 2); for (size_t i = 0; i < R * dim; ++i) {{ std::uniform_real_distribution<float> d(-3.0f, 3.0f); h[i] = (half)d(rng); }} bufs_a[{slot}] = bufs_b[{slot}] = h; }}")
            else:
                size = "dim" if "weight" in nm else "R * dim"
                driver.append(f"        {{ size_t n = {size}; float *x = (float *)alloc(n * 4); fill(x, n, rng, -3.0f, 3.0f); bufs_a[{slot}] = bufs_b[{slot}] = x; }}")
        pslot = next(p["slot"] for p in params if p["kind"] == "params")
        driver.append(f"        {ptype} prm; std::memset(&prm, 0, sizeof prm);")
        fields = {"termite_metal_apply_layer_norm_params": ["hidden_size", "eps"],
                  "termite_metal_apply_layer_norm_scale_params": ["hidden_size", "eps", "scale"],
                  "termite_metal_apply_row_norm_params": ["rows", "hidden_size", "eps"],
                  "termite_metal_apply_row_norm_scale_params": ["rows", "hidden_size", "eps", "scale"],
                  "antfly_norm_clamp_params": ["rows", "dim", "flags", "eps", "lo", "hi"]}[ptype]
        assigns = {"rows": "prm.rows = R;", "hidden_size": "prm.hidden_size = dim;", "dim": "prm.dim = dim;",
                   "eps": "prm.eps = variant == 2 ? 1e-6f : 1e-5f;", "scale": "prm.scale = variant == 0 ? 1.0f : 0.75f;",
                   "flags": "prm.flags = (uint)variant;", "lo": "prm.lo = -0.5f;", "hi": "prm.hi = 0.75f;"}
        driver.append("        " + " ".join(assigns[f] for f in fields))
        driver.append(f"        bufs_a[{pslot}] = bufs_b[{pslot}] = &prm;")
        if serial:
            launch = "uint groups = 1u, tpg = has_rows ? R + 1u : 3u;"
        else:
            launch = "uint groups = has_rows ? R + 1u : (uses_group ? R : 1u), tpg = tpg0;"
        driver.append(f"        const bool has_rows = {str(has_rows_field).lower()}; const bool uses_group = {str(uses_group).lower()}; (void)uses_group; (void)has_rows; {launch}")
        driver.append(f"        run_before_{n}(bufs_a, groups, tpg); run_after_{n}(bufs_b, groups, tpg);")
        if n in ("termite_apply_rms_norm_rows_reduce", "termite_apply_rms_norm_add_scale_rows"):
            # The emulator must compute RMSNorm, not just agree with itself.
            res = "(double)((float *)bufs_a[2])[r * dim + c]" if "add" in n else "0.0"
            out_slot = 3 if "add" in n else 2
            scl = "prm.scale" if "scale" in n else "1.0"
            driver.append(f"""        for (uint r = 0; r < R; ++r) {{ const float *xin = (const float *)bufs_a[0] + r * dim; double ss = 0; for (uint c = 0; c < dim; ++c) ss += (double)xin[c] * xin[c];
            double invd = 1.0 / std::sqrt(ss / dim + prm.eps); for (uint c = 0; c < dim; ++c) {{ double want = ((double)xin[c] * invd * ((const float *)bufs_a[1])[c] + {res}) * {scl}; double got = ((float *)bufs_a[{out_slot}])[r * dim + c];
            ++checks; if (std::fabs(got - want) > 1e-5 * (1.0 + std::fabs(want))) {{ ++failures; std::printf("ORACLE %s dim=%u got %g want %g\\n", name, dim, got, want); break; }} }} }}""")
        for p in bufs:
            if p["kind"] != "out":
                continue
            size = "1" if p["name"] == "inv_scale" else "R * dim"
            driver.append(f"        ++checks; if (std::memcmp(bufs_a[{p['slot']}], bufs_b[{p['slot']}], {size} * 4) != 0) {{ ++failures; std::printf(\"MISMATCH %s rows=%u dim=%u tpg=%u variant=%d buffer={p['name']}\\n\", name, R, dim, tpg, variant); }}")
        driver.append("    } }")
    driver.append('    std::printf("%d output comparisons, %d mismatches\\n", checks, failures);')
    driver.append("    return failures != 0;")
    driver.append("}")
    src += "\n".join(driver) + "\n"

    work = tempfile.mkdtemp(prefix="metal_rms_check_")
    cpp = os.path.join(work, "check.cpp")
    exe = os.path.join(work, "check")
    open(cpp, "w").write(src)
    flags = os.environ.get("CHECK_FLAGS", "-O1 -ffp-contract=off").split()
    r = subprocess.run(["clang++", "-std=c++20", "-pthread", "-Wno-unknown-attributes", *flags, cpp, "-o", exe], capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[:6000])
        sys.exit(f"compile failed; source kept at {cpp}")
    r = subprocess.run([exe], capture_output=True, text=True)
    print(r.stdout[-4000:])
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
