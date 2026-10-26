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

"""Check fused vision QKV/RoPE inputs bitwise against shipped CUDA operations."""

import argparse
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import pointer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--artifact", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    torch.set_num_threads(4)
    torch.manual_seed(961)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for hidden in [768, 1024]:
            heads = hidden // 64
            for rows in [1, 129, 1728, 2592, 6912, 10368]:
                guarded = []

                def output():
                    t = torch.full(
                        (rows * hidden + 16,), 4321, device="cuda", dtype=torch.uint16
                    )
                    guarded.append(t)
                    return t[: rows * hidden]

                packed = (
                    torch.randn((rows, 3 * hidden), device="cuda", dtype=torch.float32)
                    * 0.3
                )
                # Include exact conversion ties and both signs in values, no tolerance.
                packed.view(-1)[:8] = torch.tensor(
                    [
                        0.0,
                        -0.0,
                        1.00390625,
                        -1.00390625,
                        1.01171875,
                        -1.01171875,
                        1e-20,
                        -1e-20,
                    ],
                    device="cuda",
                )
                frozen = packed.clone()
                positions = torch.stack(
                    [
                        (torch.arange(rows, device="cuda", dtype=torch.int32) * 7)
                        % 127,
                        (torch.arange(rows, device="cuda", dtype=torch.int32) * 13)
                        % 97,
                    ]
                ).contiguous()
                pos_frozen = positions.clone()
                parts = [
                    packed[:, i * hidden : (i + 1) * hidden].contiguous()
                    for i in range(3)
                ]
                expected = [output() for _ in range(3)]
                actual = [output() for _ in range(3)]
                theta = 10000.0
                torch.cuda.synchronize()
                for i in range(3):
                    source = parts[i]
                    if i < 2:
                        rotated = torch.empty_like(source)
                        gpu.launch(
                            "termite_qwen3vl_vision_rope_f32",
                            ((rows * hidden + 255) // 256, 1),
                            256,
                            pointer(rotated),
                            pointer(source),
                            pointer(positions),
                            rows * heads,
                            rows,
                            heads,
                            64,
                            theta,
                        )
                        source = rotated
                    gpu.launch(
                        "termite_f32_to_bf16",
                        ((rows * hidden + 255) // 256, 1),
                        256,
                        pointer(expected[i]),
                        pointer(source),
                        rows * hidden,
                    )
                    # Retain scratch until the driver stream finishes before Python drops it.
                    gpu.call("cuCtxSynchronize")
                gpu.launch(
                    "lighton_vision_qkv_rope_bf16",
                    ((rows * hidden // 2 + 255) // 256, 1),
                    256,
                    *map(pointer, actual),
                    pointer(packed),
                    pointer(positions),
                    rows,
                    heads,
                    64,
                    theta,
                )
                gpu.call("cuCtxSynchronize")
                for i in range(3):
                    assert torch.equal(actual[i], expected[i]), (
                        hidden,
                        rows,
                        i,
                        int((actual[i] != expected[i]).sum()),
                    )
                assert torch.equal(packed.view(torch.int32), frozen.view(torch.int32))
                assert torch.equal(positions, pos_frozen)
                for t in guarded:
                    assert torch.all(t[-16:] == 4321)
                record = dict(
                    hidden=hidden, rows=rows, bitwise=True, readonly=True, canaries=True
                )
                results.append(record)
                print(json.dumps(record), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=results), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
