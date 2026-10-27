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

"""Build a qualification image using the shipped CUDA reference operations."""

import argparse
from pathlib import Path


def extract(source, header):
    start = source.index(header)
    cursor = source.index("{", start) + 1
    depth = 1
    while depth:
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
        cursor += 1
    return source[start:cursor]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prototype", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = args.reference.read_text()
    headers = [
        "__device__ __forceinline__ unsigned short termite_f32_to_bf16(",
        'extern "C" __global__ void termite_f32_to_bf16(',
        'extern "C" __global__ void termite_layer_norm_f32(',
        'extern "C" __global__ void termite_add_bias_rows_f32(',
        'extern "C" __global__ void termite_elementwise_f32(',
        'extern "C" __global__ void termite_qwen3vl_vision_rope_f32(',
    ]
    functions = "\n\n".join(extract(reference, header) for header in headers)
    args.output.write_text(args.prototype.read_text() + "\n\n" + functions + "\n")


if __name__ == "__main__":
    main()
