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

"""Check bundled portable Q4_0 kernels against independent CPU FP64 matmul.

Requires CUDA-enabled PyTorch. Numerical qualification only; no timing claim.
Use the same script/artifact on A100, L4, Hopper, and Spark.
"""

import argparse
import ctypes as c
import hashlib
import json
from pathlib import Path

import torch


class Module:
    def __init__(self, artifact):
        # Borrow PyTorch's current primary context, including its selected
        # CUDA_VISIBLE_DEVICES mapping; never assume physical device ordinal 0.
        self.driver = c.CDLL("libcuda.so.1")
        self.module = c.c_void_p()
        context = c.c_void_p()
        self.call("cuCtxGetCurrent", c.byref(context))
        if not context.value:
            raise RuntimeError("PyTorch must initialize the selected CUDA context")
        self.call("cuModuleLoad", c.byref(self.module), c.c_char_p(bytes(artifact)))

    def call(self, name, *args):
        status = getattr(self.driver, name)(*args)
        if status:
            raise RuntimeError(f"{name} failed: {status}")

    def launch(self, name, x, weights, output, rows, hidden, columns, stream):
        function = c.c_void_p()
        self.call(
            "cuModuleGetFunction",
            c.byref(function),
            self.module,
            c.c_char_p(name.encode()),
        )
        values = [c.c_uint64(t.data_ptr()) for t in (x, weights, output)] + [
            c.c_int(n) for n in (rows, hidden, columns)
        ]
        args = (c.c_void_p * len(values))(*(c.addressof(v) for v in values))
        self.call(
            "cuLaunchKernel",
            function,
            c.c_uint((columns + 3) // 4),
            c.c_uint(1 if rows == 1 else (rows + 7) // 8),
            c.c_uint(1),
            c.c_uint(256),
            c.c_uint(1),
            c.c_uint(1),
            c.c_uint(0),
            c.c_void_p(stream),
            args,
            c.c_void_p(),
        )

    def close(self):
        if self.module.value:
            self.call("cuModuleUnload", self.module)
            self.module = c.c_void_p()


def check_case(module, device, rows, hidden, columns):
    blocks = hidden // 32
    scale = (torch.randn(columns, blocks) * 0.02).half()
    codes = torch.randint(0, 256, (columns, blocks, 16), dtype=torch.uint8)
    packed = torch.empty(columns, blocks, 18, dtype=torch.uint8)
    packed[:, :, :2] = scale.contiguous().view(torch.uint8).reshape(columns, blocks, 2)
    packed[:, :, 2:] = codes
    # Decode the documented block format independently of the CUDA reduction
    # schedule, then compute the complete matrix product in CPU FP64.
    low = (codes & 15).double() - 8
    high = (codes >> 4).double() - 8
    decoded = (torch.cat((low, high), dim=2) * scale.double().unsqueeze(-1)).reshape(
        columns, hidden
    )
    x = torch.randn(rows, hidden) * 0.1
    expected = x.double() @ decoded.T
    x_gpu, weights_gpu = x.to(device), packed.to(device)
    guarded = torch.full((rows * columns + 32,), 123456.0, device=device)
    output = guarded[16:-16]
    name = "antfly_q4_0_mmv_f32_v1" if rows == 1 else "antfly_q4_0_mm_f32_v1"
    module.launch(
        name,
        x_gpu,
        weights_gpu,
        output,
        rows,
        hidden,
        columns,
        torch.cuda.current_stream(device).cuda_stream,
    )
    torch.cuda.synchronize(device)
    actual = output.cpu().reshape(rows, columns).double()
    torch.testing.assert_close(actual, expected, rtol=0, atol=1e-3)
    assert bool((guarded[:16] == 123456).all() and (guarded[-16:] == 123456).all()), (
        "output guard changed"
    )
    assert torch.equal(x_gpu.cpu(), x), "input modified"
    assert torch.equal(weights_gpu.cpu(), packed), "quantized weights modified"
    return {
        "kernel": name,
        "rows": rows,
        "input_dim": hidden,
        "output_dim": columns,
        "max_abs_error": float((actual - expected).abs().max()),
        "output_guards": True,
        "readonly_inputs": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "src/ops/cuda/artifacts/inference_cuda_kernels.fatbin",
    )
    parser.add_argument("--device", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.manual_seed(1447)
    torch.set_num_threads(4)
    device = torch.device("cuda", args.device)
    torch.cuda.set_device(device)
    torch.cuda.synchronize(device)
    artifact = args.artifact.resolve()
    receipt = {
        "scope": "numerical only; CPU FP64 reference",
        "artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "torch_version": str(torch.__version__),
        "device": torch.cuda.get_device_name(device),
        "compute_capability": list(torch.cuda.get_device_capability(device)),
        "atol": 1e-3,
        "cases": [],
        "status": "failed",
    }
    module = None
    try:
        module = Module(artifact)
        for shape in (
            (1, 256, 1536),
            (1, 1536, 6144),
            (1, 1536, 1537),
            (9, 256, 1536),
            (16, 1536, 6144),
            (64, 1536, 1537),
        ):
            result = check_case(module, device, *shape)
            receipt["cases"].append(result)
            print(json.dumps(result), flush=True)
        receipt["status"] = "passed"
    finally:
        if module is not None:
            module.close()
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"status": receipt["status"], "cases": len(receipt["cases"])}))


if __name__ == "__main__":
    main()
