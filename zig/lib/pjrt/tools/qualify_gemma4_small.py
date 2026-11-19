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

"""Check E4B small decode tiles against existing CUDA kernels bit for bit."""

import argparse
import ctypes as c
import hashlib
import json
import subprocess
from pathlib import Path

import numpy as np
from gguf import GGUFReader
from qualify_gemma4_head import Cuda, identical, quantize


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


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
    gpu = Cuda()
    try:
        reader = GGUFReader(str(args.gguf))
        tensors = {t.name: t for t in reader.tensors}
        weights = {}
        hashes = {}
        for name in ["ffn_gate", "ffn_up", "ffn_down", "inp_gate", "proj"]:
            tensor = tensors[f"blk.0.{name}.weight"]
            if int(tensor.tensor_type) != 2:
                raise ValueError(f"Expected Q4_0: {tensor.name}")
            weights[name] = gpu.array(tensor.data)
            hashes[tensor.name] = hashlib.sha256(tensor.data.tobytes()).hexdigest()
        rng = np.random.default_rng(72)
        checks = []
        for kind, indim, outdim, threads, oldcols, newcols, oldname in [
            (
                "ffn_pair",
                2560,
                10240,
                160,
                4,
                1,
                "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn",
            ),
            (
                "ffn_down",
                10240,
                2560,
                256,
                4,
                1,
                "termite_linear_q4_0_q8_1_f32_tile4_w8",
            ),
            ("ple_gate", 2560, 256, 256, 4, 1, "termite_linear_q4_0_q8_1_f32_tile4_w8"),
            (
                "ple_projection",
                256,
                2560,
                128,
                4,
                8,
                "termite_linear_q4_0_q8_1_f32_tile4",
            ),
        ]:
            old = gpu.function(args.baseline_cubin, oldname)
            new = gpu.function(
                args.candidate_cubin, f"termite_gemma4_small_{kind}_q8_1_f32"
            )
            dst = gpu.array(np.zeros(outdim, np.float32))
            source = gpu.array(np.zeros(indim // 32 * 36, np.uint8))
            for trial in range(18):
                values = rng.normal(size=indim).astype(np.float32)
                if trial == 0:
                    values[:] = 0
                elif trial == 1:
                    values *= 1e-4
                elif trial == 2:
                    values *= 100
                gpu.upload(source, quantize(values))
                activation = c.c_uint(trial % 6)
                if kind == "ffn_pair":
                    parameters = [
                        dst,
                        source,
                        weights["ffn_gate"],
                        weights["ffn_up"],
                        activation,
                    ]
                else:
                    weight = weights[
                        {
                            "ffn_down": "ffn_down",
                            "ple_gate": "inp_gate",
                            "ple_projection": "proj",
                        }[kind]
                    ]
                    parameters = [dst, source, weight]
                old_parameters = (
                    parameters
                    if kind == "ffn_pair"
                    else [*parameters, c.c_uint(1), c.c_uint(indim), c.c_uint(outdim)]
                )
                new_parameters = (
                    parameters if kind.startswith("ffn") else old_parameters
                )
                gpu.launch(old, old_parameters, outdim // oldcols, threads)
                expected = gpu.read(dst, outdim, np.float32)
                gpu.launch(new, new_parameters, outdim // newcols, threads)
                identical(
                    gpu.read(dst, outdim, np.float32), expected, f"{kind}/{trial}"
                )
                checks.append(
                    {
                        "kind": kind,
                        "trial": trial,
                        "activation": activation.value if kind == "ffn_pair" else None,
                        "bitwise_equal": True,
                    }
                )
        args.out.write_text(
            json.dumps(
                {
                    "scope": "Layer-zero E4B Q4 weights and identical prepared Q8 bytes; "
                    "four kernel outputs compared bit for bit; no model or latency claim",
                    "artifacts_sha256": {
                        "baseline": digest(args.baseline_cubin),
                        "candidate": digest(args.candidate_cubin),
                    },
                    "gguf_sha256": digest(args.gguf),
                    "tensor_sha256": hashes,
                    "checks": checks,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"{len(checks)} bitwise checks passed", flush=True)
    finally:
        gpu.close()


if __name__ == "__main__":
    main()
