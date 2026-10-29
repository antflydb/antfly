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

"""Differential check of the CUDA RMSNorm kernels across two versions of
inference_cuda_kernels.cu, on the CPU (no GPU or CUDA toolkit).

Compiles both versions' kernels as C++ against a small CUDA shim and runs
each block with one OS thread per CUDA thread (`__syncthreads` is a real
barrier). Outputs must match bit for bit over a grid of shapes and block
sizes; `termite_rms_norm_f32` is also checked against an f64 reference.

    python check_cuda_rms_norm.py <old inference_cuda_kernels.cu> <new inference_cuda_kernels.cu>
"""

import os
import re
import subprocess
import sys
import tempfile

KERNELS = [
    "termite_rms_norm_f32",
    "termite_rms_norm_f32_bf16",
    "termite_rms_norm_add_f32",
    "termite_rms_norm_add_f32_bf16",
    "termite_rms_norm_add_mul_scalar_f32",
    "termite_rms_norm_add_output_scale_f32",
    "termite_rms_norm_bare_f32",
    "termite_gemma4_a4b_rms_norm_triple_f32",
]

SHIM = r"""
#include <barrier>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>
struct dim3_ { unsigned int x, y, z; };
static thread_local dim3_ threadIdx, blockIdx;
static dim3_ blockDim;
static std::barrier<> *g_barrier;
static inline void __syncthreads() { g_barrier->arrive_and_wait(); }
static inline float rsqrtf(float v) { return 1.0f / std::sqrt(v); }
static inline unsigned int __float_as_uint(float v) { unsigned int u; std::memcpy(&u, &v, 4); return u; }
static inline unsigned short termite_f32_to_bf16(float value) {
    unsigned int bits = __float_as_uint(value);
    unsigned int lsb = (bits >> 16) & 1u;
    unsigned int rounding_bias = 0x7fffu + lsb;
    return (unsigned short)((bits + rounding_bias) >> 16);
}
"""


def extract(path):
    src = open(path).read()
    out = {}
    for n in KERNELS:
        m = re.search(r'extern "C" __global__ void ' + n + r"\(", src)
        if not m:
            sys.exit(f"{path}: missing {n}")
        depth, i = 0, src.index("{", m.end())
        j = i
        while True:
            if src[j] == "{":
                depth += 1
            elif src[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        out[n] = src[m.start(): j + 1]
    return out


def to_cpp(text):
    text = text.replace('extern "C" __global__ ', "")
    return re.sub(r"__shared__ float (\w+)\[", r"static float \1[", text)


def params_of(text):
    sig = text[text.index("(") + 1: text.index(")")]
    out = []
    for p in sig.split(","):
        p = " ".join(p.split())
        name = re.search(r"(\w+)$", p).group(1)
        out.append((p[: -len(name)].strip(), name))
    return out


def main():
    old_path, new_path = sys.argv[1:3]
    old, new = extract(old_path), extract(new_path)
    src = SHIM
    for ns, ks in (("before", old), ("after", new)):
        src += f"namespace {ns} {{\n" + "\n".join(to_cpp(ks[n]) for n in KERNELS) + "\n}\n"
    lines = ["int main() {", "    int checks = 0, failures = 0;"]
    for n in KERNELS:
        ps, ps_new = params_of(old[n]), params_of(new[n])
        if ps != ps_new:
            sys.exit(f"{n}: argument ABI differs: {ps} vs {ps_new}")
        lines.append(f'    for (unsigned int rows : {{1u, 3u}}) for (unsigned int dim : {{1u, 7u, 64u, 100u, 384u, 1000u}}) for (unsigned int threads : {{32u, 64u, 128u, 256u}}) {{')
        lines.append("        std::mt19937 rng(rows * 1000u + dim * 7u + threads); std::uniform_real_distribution<float> d(-3.0f, 3.0f);")
        lines.append("        std::vector<std::vector<float>> fa, fb; std::vector<std::vector<unsigned short>> ha, hb; float eps = dim % 2 ? 1e-6f : 1e-5f;")
        call_a, call_b, outs = [], [], []
        for ty, name in ps:
            if ty == "float*":
                lines.append(f"        fa.emplace_back(rows * dim, -12345.0f); fb.emplace_back(rows * dim, -12345.0f); float *{name}_a = fa.back().data(); float *{name}_b = fb.back().data();")
                call_a.append(f"{name}_a"); call_b.append(f"{name}_b"); outs.append((f"{name}", "float"))
            elif ty == "unsigned short*":
                lines.append(f"        ha.emplace_back(rows * dim, 0xBEEF); hb.emplace_back(rows * dim, 0xBEEF); unsigned short *{name}_a = ha.back().data(); unsigned short *{name}_b = hb.back().data();")
                call_a.append(f"{name}_a"); call_b.append(f"{name}_b"); outs.append((f"{name}", "unsigned short"))
            elif ty == "const float*":
                size = "dim" if "weight" in name else ("1" if name == "scalar" else "rows * dim")
                init = "0.8125f" if name == "scalar" else "d(rng)"
                lines.append(f"        std::vector<float> {name}({size}); for (auto &v : {name}) v = {init};")
                call_a.append(f"{name}.data()"); call_b.append(f"{name}.data()")
            elif name in ("rows", "dim", "eps"):
                call_a.append(name); call_b.append(name)
            else:
                sys.exit(f"{n}: unknown parameter {ty} {name}")
        for ns, call in (("before", call_a), ("after", call_b)):
            lines.append(f"""        blockDim = {{threads, 1, 1}};
        for (unsigned int block = 0; block < rows + 1u; ++block) {{
            std::barrier<> bar(threads); g_barrier = &bar; std::vector<std::thread> ts;
            for (unsigned int t = 0; t < threads; ++t) ts.emplace_back([&, t] {{ threadIdx = {{t, 0, 0}}; blockIdx = {{block, 0, 0}}; {ns}::{n}({', '.join(call)}); bar.arrive_and_drop(); }});
            for (auto &th : ts) th.join();
        }}""")
        for name, ty in outs:
            lines.append(f'        ++checks; if (std::memcmp({name}_a, {name}_b, rows * dim * sizeof({ty})) != 0) {{ ++failures; std::printf("MISMATCH {n} {name} rows=%u dim=%u threads=%u\\n", rows, dim, threads); }}')
        if n == "termite_rms_norm_f32":
            lines.append("""        for (unsigned int r = 0; r < rows; ++r) { double ss = 0; for (unsigned int c = 0; c < dim; ++c) ss += (double)input[r * dim + c] * input[r * dim + c];
            double inv = 1.0 / std::sqrt(ss / dim + eps); for (unsigned int c = 0; c < dim; ++c) { double want = input[r * dim + c] * inv * weight[c];
            ++checks; if (std::fabs(dst_a[r * dim + c] - want) > 1e-5 * (1 + std::fabs(want))) { ++failures; std::printf("ORACLE rows=%u dim=%u\\n", rows, dim); break; } } }""")
        lines.append("    }")
    lines += ['    std::printf("%d output comparisons, %d mismatches\\n", checks, failures);', "    return failures != 0;", "}"]
    src += "\n".join(lines) + "\n"
    work = tempfile.mkdtemp(prefix="cuda_rms_check_")
    cpp, exe = os.path.join(work, "check.cpp"), os.path.join(work, "check")
    open(cpp, "w").write(src)
    flags = os.environ.get("CHECK_FLAGS", "-O1 -ffp-contract=off").split()
    r = subprocess.run(["clang++", "-std=c++20", "-pthread", *flags, cpp, "-o", exe], capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[:6000])
        sys.exit(f"compile failed; source kept at {cpp}")
    r = subprocess.run([exe], capture_output=True, text=True)
    print(r.stdout[-4000:])
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
