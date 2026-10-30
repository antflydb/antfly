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

"""Compare private BF16 input strips bitwise with the shipped F32 operations."""

import argparse
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import pointer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.manual_seed(949)
    torch.set_num_threads(4)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for dim, wide in [(768, 3072), (1024, 4096)]:
            for rows in [1, 129, 1728, 2592, 6912, 10368]:
                guarded = []

                def buffer(n, half=False, random=False):
                    dtype = torch.uint16 if half else torch.float32
                    value = torch.full((n + 16,), 4321, device="cuda", dtype=dtype)
                    if random:
                        value[:n].normal_(std=0.3)
                    else:
                        value[:n].zero_()
                    guarded.append(value)
                    return value[:n]

                x = buffer(rows * dim, random=True)
                gamma = buffer(dim, random=True)
                beta = buffer(dim, random=True)
                original = [t.clone() for t in (x, gamma, beta)]
                norm = buffer(rows * dim)
                expected = buffer(rows * dim, half=True)
                actual = buffer(rows * dim, half=True)
                torch.cuda.synchronize()
                gpu.launch(
                    "termite_layer_norm_f32",
                    (rows, 1),
                    256,
                    *map(pointer, (norm, x, gamma, beta)),
                    rows,
                    dim,
                    1e-6,
                )
                gpu.launch(
                    "termite_f32_to_bf16",
                    ((rows * dim + 255) // 256, 1),
                    256,
                    pointer(expected),
                    pointer(norm),
                    rows * dim,
                )
                gpu.launch(
                    "lighton_vision_norm_bf16",
                    (rows, 1),
                    256,
                    *map(pointer, (actual, x, gamma, beta)),
                    rows,
                    dim,
                    1e-6,
                )
                gpu.call("cuCtxSynchronize")
                assert torch.equal(actual, expected), (
                    "norm",
                    rows,
                    dim,
                    (actual != expected).sum().item(),
                )
                for source, frozen in zip((x, gamma, beta), original):
                    assert torch.equal(
                        source.view(torch.int32), frozen.view(torch.int32)
                    )
                projected = buffer(rows * wide, random=True)
                bias = buffer(wide, random=True)
                biased = projected.clone()
                activated = buffer(rows * wide)
                gold = buffer(rows * wide, half=True)
                fused = buffer(rows * wide, half=True)
                saved = [t.clone() for t in (projected, bias)]
                torch.cuda.synchronize()
                gpu.launch(
                    "termite_add_bias_rows_f32",
                    ((rows * wide + 255) // 256, 1),
                    256,
                    pointer(biased),
                    pointer(bias),
                    rows,
                    wide,
                )
                gpu.launch(
                    "termite_elementwise_f32",
                    ((rows * wide + 255) // 256, 1),
                    256,
                    pointer(activated),
                    pointer(biased),
                    pointer(biased),
                    rows * wide,
                    3,
                )
                gpu.launch(
                    "termite_f32_to_bf16",
                    ((rows * wide + 255) // 256, 1),
                    256,
                    pointer(gold),
                    pointer(activated),
                    rows * wide,
                )
                gpu.launch(
                    "lighton_vision_bias_gelu_bf16",
                    ((rows * wide + 255) // 256, 1),
                    256,
                    *map(pointer, (fused, projected, bias)),
                    rows * wide,
                    wide,
                )
                gpu.call("cuCtxSynchronize")
                assert torch.equal(fused, gold), (
                    "bias/gelu",
                    rows,
                    wide,
                    (fused != gold).sum().item(),
                )
                for source, frozen in zip((projected, bias), saved):
                    assert torch.equal(
                        source.view(torch.int32), frozen.view(torch.int32)
                    )
                projection = buffer(rows * dim, random=True)
                residual = buffer(rows * dim, random=True)
                out_bias = buffer(dim, random=True)
                biased_out = projection.clone()
                wanted = buffer(rows * dim)
                destination = projection.clone()
                torch.cuda.synchronize()
                gpu.launch(
                    "termite_add_bias_rows_f32",
                    ((rows * dim + 255) // 256, 1),
                    256,
                    pointer(biased_out),
                    pointer(out_bias),
                    rows,
                    dim,
                )
                gpu.launch(
                    "termite_elementwise_f32",
                    ((rows * dim + 255) // 256, 1),
                    256,
                    *map(pointer, (wanted, residual, biased_out)),
                    rows * dim,
                    0,
                )
                gpu.launch(
                    "lighton_vision_bias_residual_f32",
                    ((rows * dim + 255) // 256, 1),
                    256,
                    *map(pointer, (destination, destination, out_bias, residual)),
                    rows * dim,
                    dim,
                )
                gpu.call("cuCtxSynchronize")
                assert torch.equal(
                    destination.view(torch.int32), wanted.view(torch.int32)
                ), ("bias/residual", rows, dim)
                for value in guarded:
                    assert torch.all(value[-16:] == 4321), "output guard overwritten"
                record = dict(rows=rows, dim=dim, wide=wide, bitwise=True)
                results.append(record)
                print(json.dumps(record), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=results), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
