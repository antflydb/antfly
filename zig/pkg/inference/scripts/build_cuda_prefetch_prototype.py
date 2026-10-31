#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Extract isolated exact E4B kernels and add L2-prefetch-only variants.

This only writes CUDA source. It never compiles, loads a GPU, or changes runtime
dispatch. Compile the emitted file with the same flags as the canonical cubin.
The copied baseline and every candidate are compiled together for fair timing.
"""
import argparse
import hashlib
from pathlib import Path
import re

KERNELS = {
    "pair": "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn",
    "down": "termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down",
    "head": "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b",
}


def extract(source, name):
    pattern = rf'(?m)^(?:__device__ __forceinline__|extern "C" __global__) [^\n]*\b{re.escape(name)}\s*\('
    match = re.search(pattern, source)
    if match is None:
        raise ValueError(f"missing definition {name}")
    body = source.index("{", match.end())
    depth = 1
    end = body + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[match.start():end]


def generate(source):
    helpers, visiting = [], set()

    def visit(name):
        if name in visiting:
            return
        visiting.add(name)
        definition = extract(source, name)
        for dependency in sorted(set(re.findall(r"\b(termite_\w+)\s*\(", definition)) - {name}):
            visit(dependency)
        if name not in KERNELS.values():
            helpers.append(definition)

    for name in KERNELS.values():
        visit(name)
    struct = re.search(r"struct termite_q4_0_centered_i8x4_pair \{.*?\};", source, re.S).group()
    result = [
        "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: Apache-2.0",
        "// Standalone experiment; no production dispatch. Canonical SHA256: " + hashlib.sha256(source.encode()).hexdigest(),
        "#include <cuda_fp16.h>\n#include <math.h>\n#include <stddef.h>",
        struct,
        "\n\n".join(helpers),
        '__device__ __forceinline__ void antfly_prefetch_l2(const void* address) {\n    asm volatile("prefetch.global.L2 [%0];" :: "l"(address));\n}',
    ]
    for kind, name in KERNELS.items():
        original = extract(source, name)
        result.append(original.replace(name, f"antfly_prefetch_{kind}_baseline", 1))
        # K is fully parallel in gate/up and the head. Prefetch future output
        # columns, rather than adding an unreachable future-K branch there.
        for distance in (1, 2):
            candidate = original.replace(name, f"antfly_prefetch_{kind}_col{distance}", 1)
            if kind == "head":
                marker = "            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;"
                hint = f"""            if (c + {distance}u < cols && sub == 0u) {{
                const unsigned char* future = weight + ((col_tile + c + {distance}u) * row_blocks + block) * 210u;
                antfly_prefetch_l2(future);
                antfly_prefetch_l2(future + 128u);
            }}
"""
            else:
                marker = "            unsigned int col = col_tile + c;"
                weights = ("weight_gate", "weight_up") if kind == "pair" else ("weight",)
                hint = f"            if (c + {distance}u < cols && (tid & 1u) == 0u) {{\n"
                for weight in weights:
                    hint += f"                antfly_prefetch_l2({weight} + ((col_tile + c + {distance}u) * row_blocks + block) * 18u);\n"
                hint += "            }\n"
            assert candidate.count(marker) == 1
            result.append(candidate.replace(marker, hint + marker, 1))
        if kind == "down":
            candidate = original.replace(name, "antfly_prefetch_down_k1", 1)
            marker = "        const unsigned char* q8_bp = q8_input + block * 36u;"
            hint = """        unsigned int future_block = block + (blockDim.x >> 1u);
        if (future_block < row_blocks && (tid & 1u) == 0u) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c)
                antfly_prefetch_l2(weight + ((col_tile + c) * row_blocks + future_block) * 18u);
        }
"""
            assert candidate.count(marker) == 1
            result.append(candidate.replace(marker, hint + marker, 1))
    return "\n\n".join(result) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1] / "src/ops/cuda/artifacts/inference_cuda_kernels.cu")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(generate(args.source.read_text()))
    print(args.output)


if __name__ == "__main__":
    main()
