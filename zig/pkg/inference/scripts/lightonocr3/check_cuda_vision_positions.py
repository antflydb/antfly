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

"""Check learned-position CUDA interpolation against independent F32/F64 CPU math.

The merge-major indexing follows the Qwen projector's align-corners contract.
Checks also cover F32/BF16 position tables, read-only inputs and output guards.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import pointer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.set_num_threads(4)
    torch.manual_seed(198)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for dim in [768, 1024]:
            for dtype in [torch.float32, torch.bfloat16]:
                table = (torch.randn((48, 48, dim)) * 0.125).to(dtype)
                table_gpu = table.cuda()
                frozen_table = table_gpu.clone()
                t = table.float().numpy()
                for gx, gy in [(2, 2), (2, 64), (34, 50), (64, 64), (128, 128)]:
                    rows = gx * gy
                    row = np.arange(rows)
                    inner = row % 4
                    group = row // 4
                    x = (group % (gx // 2)) * 2 + inner % 2
                    y = (group // (gx // 2)) * 2 + inner // 2
                    fx = (x.astype(np.float32) * np.float32(47)) / np.float32(gx - 1)
                    fy = (y.astype(np.float32) * np.float32(47)) / np.float32(gy - 1)
                    x0 = np.floor(fx).astype(np.int32)
                    y0 = np.floor(fy).astype(np.int32)
                    x1 = np.minimum(x0 + 1, 47)
                    y1 = np.minimum(y0 + 1, 47)
                    wx = (fx - x0.astype(np.float32))[:, None]
                    wy = (fy - y0.astype(np.float32))[:, None]
                    source = torch.randn((rows, dim)) * 0.3
                    original = source.numpy()
                    input_gpu = source.cuda()
                    frozen = input_gpu.clone()
                    expected = original + (
                        (t[y0, x0] * (np.float32(1) - wx) + t[y0, x1] * wx)
                        * (np.float32(1) - wy)
                        + (t[y1, x0] * (np.float32(1) - wx) + t[y1, x1] * wx) * wy
                    )
                    expected64 = original.astype(np.float64) + (
                        (
                            t[y0, x0].astype(np.float64) * (1 - wx.astype(np.float64))
                            + t[y0, x1] * wx.astype(np.float64)
                        )
                        * (1 - wy.astype(np.float64))
                        + (
                            t[y1, x0].astype(np.float64) * (1 - wx.astype(np.float64))
                            + t[y1, x1] * wx.astype(np.float64)
                        )
                        * wy.astype(np.float64)
                    )
                    backing = torch.full((rows * dim + 128,), 4321.0, device="cuda")
                    out = backing[64:-64].view(rows, dim)
                    torch.cuda.synchronize()
                    gpu.launch(
                        "lighton_vision_learned_positions_f32",
                        ((rows * dim + 255) // 256, 1),
                        256,
                        pointer(out),
                        pointer(input_gpu),
                        pointer(table_gpu),
                        gx,
                        gy,
                        2,
                        48,
                        dim,
                        int(dtype == torch.bfloat16),
                    )
                    gpu.call("cuCtxSynchronize")
                    actual = out.cpu().numpy()
                    error = float(np.abs(actual - expected).max())
                    error64 = float(
                        np.abs(actual.astype(np.float64) - expected64).max()
                    )
                    assert error <= 1e-6, (dim, dtype, gx, gy, error)
                    assert error64 <= 1e-6, (dim, dtype, gx, gy, error64)
                    assert bool(torch.all(backing[:64] == 4321)) and bool(
                        torch.all(backing[-64:] == 4321)
                    )
                    assert torch.equal(input_gpu, frozen) and torch.equal(
                        table_gpu, frozen_table
                    )
                    results.append(
                        {
                            "dim": dim,
                            "dtype": str(dtype),
                            "grid": [gx, gy],
                            "max_abs_error_f32": error,
                            "max_abs_error_f64": error64,
                        }
                    )
                    print(results[-1], flush=True)
        args.output.write_text(
            json.dumps(
                {
                    "artifact_sha256": hashlib.sha256(
                        args.artifact.read_bytes()
                    ).hexdigest(),
                    "cases": results,
                    "threshold": 1e-6,
                },
                indent=2,
            )
            + "\n"
        )
    finally:
        gpu.close()


if __name__ == "__main__":
    main()
