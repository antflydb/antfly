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

"""Compare exact E4B PLE fusion with existing unfused and legacy CUDA paths."""

import argparse
import ctypes as c
import hashlib
import json
import subprocess
from pathlib import Path

import numpy as np
from gguf import GGUFReader
from qualify_gemma4_head import Cuda, identical, quantize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["gguf", "baseline-cubin", "candidate-cubin", "out"]:
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error("Output must be new")
    if subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip():
        raise RuntimeError("GPU busy")
    g = Cuda()
    try:
        r = GGUFReader(str(args.gguf))
        t = next(
            t
            for t in r.tensors
            if list(t.shape) == [2560, 256] and int(t.tensor_type) == 2
        )
        print("Real PLE weight", t.name, flush=True)
        weight = g.array(t.data)
        baseline = args.baseline_cubin
        candidate = args.candidate_cubin
        linear = g.function(baseline, "termite_linear_q4_0_q8_1_f32_tile4_w8")
        activation = g.function(baseline, "termite_activation_multiply_f32")
        old = g.function(
            baseline, "termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4"
        )
        fused = g.function(
            candidate, "termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4"
        )
        dst = g.array(np.zeros(256, np.float32))
        linear_dst = g.array(np.zeros(256, np.float32))
        source = g.array(np.zeros(781, np.float32))
        selected = g.array(np.zeros(256, np.float32))
        x = g.array(np.zeros(80 * 36, np.uint8))
        rng = np.random.default_rng(93)
        checks = []
        for trial in range(18):
            v = rng.normal(size=2560).astype(np.float32)
            up = rng.normal(size=781).astype(np.float32)
            if trial == 0:
                v[:] = 0
            elif trial == 1:
                v *= 1e-4
            elif trial == 2:
                v *= 100
            g.upload(x, quantize(v))
            g.upload(source, up)
            g.upload(selected, up[257:513])
            act = c.c_uint(trial % 6)
            linargs = [
                linear_dst,
                x,
                weight,
                c.c_uint(1),
                c.c_uint(2560),
                c.c_uint(256),
            ]
            actargs = [dst, linear_dst, selected, c.c_uint(256), act]
            parameters = [
                dst,
                x,
                weight,
                source,
                c.c_uint(1),
                c.c_uint(2560),
                c.c_uint(256),
                c.c_uint(781),
                c.c_uint(257),
                act,
            ]
            g.launch(linear, linargs, 64, 256)
            g.launch(activation, actargs, 1, 256)
            expected = g.read(dst, 256, np.float32)
            g.launch(fused, parameters, 64, 256)
            identical(g.read(dst, 256, np.float32), expected, f"unfused/{trial}")
            g.launch(old, parameters, 64, 128)
            legacy = g.read(dst, 256, np.float32)
            g.launch(fused, parameters, 64, 128)
            identical(g.read(dst, 256, np.float32), legacy, f"legacy/{trial}")
            checks.append(
                {
                    "trial": trial,
                    "activation": act.value,
                    "wide_matches_unfused_bits": True,
                    "narrow_matches_legacy_bits": True,
                }
            )
        args.out.write_text(
            json.dumps(
                {
                    "tensor": t.name,
                    "tensor_sha256": hashlib.sha256(t.data.tobytes()).hexdigest(),
                    "artifacts": {
                        "baseline": hashlib.sha256(baseline.read_bytes()).hexdigest(),
                        "candidate": hashlib.sha256(candidate.read_bytes()).hexdigest(),
                    },
                    "scope": "Same actual E4B PLE Q4 weights and prepared Q8 bytes; wide fusion equals existing unfused projection plus activation/multiply, narrow fusion preserves legacy bits; no model speed claim",
                    "checks": checks,
                },
                indent=2,
            )
            + "\n"
        )
        print(
            "36 bitwise checks passed, six activations and nonzero source slice offset",
            flush=True,
        )
    finally:
        g.close()


if __name__ == "__main__":
    main()
