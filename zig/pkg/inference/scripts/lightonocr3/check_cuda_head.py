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

"""Check generated head PTX against the original cuBLASLt F32 projection.

Checks every matrix element, token selection, runtime row boundaries, ties,
read-only operands and allocation guards. No checkpoint download is needed.
"""

import argparse
import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import sys


class Algo(c.Structure):
    _fields_ = [("data", c.c_uint64 * 8)]


class Heuristic(c.Structure):
    _fields_ = [
        ("algo", Algo),
        ("workspace", c.c_size_t),
        ("state", c.c_int),
        ("waves", c.c_float),
        ("reserved", c.c_int * 4),
    ]


class Projection:
    """Original BF16-operand/F32-output descriptors, F32 accumulation, zero workspace."""

    def __init__(self, lib, handle, x, weight, output):
        self.lib, self.handle = lib, handle
        self.x, self.weight, self.output = x, weight, output
        self.alpha, self.beta = c.c_float(1), c.c_float(0)
        self.desc, self.preference = c.c_void_p(), c.c_void_p()
        self.layouts = []
        self.call(
            "cublasLtMatmulDescCreate", c.byref(self.desc), c.c_int(68), c.c_int(0)
        )
        for attr, value in [(3, 1), (4, 0)]:
            v = c.c_int(value)
            self.call(
                "cublasLtMatmulDescSetAttribute",
                self.desc,
                c.c_int(attr),
                c.byref(v),
                c.c_size_t(4),
            )
        m, k = x.shape
        n = weight.shape[0]
        for dtype, rows, columns, stride in [
            (14, k, n, k),
            (14, k, m, k),
            (0, n, m, n),
        ]:
            layout = c.c_void_p()
            self.call(
                "cublasLtMatrixLayoutCreate",
                c.byref(layout),
                c.c_int(dtype),
                c.c_uint64(rows),
                c.c_uint64(columns),
                c.c_int64(stride),
            )
            self.layouts.append(layout)
        self.call("cublasLtMatmulPreferenceCreate", c.byref(self.preference))
        size = c.c_size_t(0)
        self.call(
            "cublasLtMatmulPreferenceSetAttribute",
            self.preference,
            c.c_int(1),
            c.byref(size),
            c.c_size_t(8),
        )
        self.result = (Heuristic * 1)()
        count = c.c_int()
        a, b, out = self.layouts
        self.call(
            "cublasLtMatmulAlgoGetHeuristic",
            handle,
            self.desc,
            a,
            b,
            out,
            out,
            self.preference,
            c.c_int(1),
            self.result,
            c.byref(count),
        )
        assert (
            count.value == 1
            and self.result[0].state == 0
            and self.result[0].workspace == 0
        )

    def call(self, name, *args):
        status = getattr(self.lib, name)(*args)
        if status:
            raise RuntimeError((name, status))

    def run(self, stream):
        a, b, out = self.layouts
        self.call(
            "cublasLtMatmul",
            self.handle,
            self.desc,
            c.byref(self.alpha),
            c.c_void_p(self.weight.data_ptr()),
            a,
            c.c_void_p(self.x.data_ptr()),
            b,
            c.byref(self.beta),
            c.c_void_p(self.output.data_ptr()),
            out,
            c.c_void_p(self.output.data_ptr()),
            out,
            c.byref(self.result[0].algo),
            c.c_void_p(),
            c.c_size_t(0),
            c.c_void_p(stream),
        )

    def close(self):
        self.call("cublasLtMatmulPreferenceDestroy", self.preference)
        for layout in self.layouts:
            self.call("cublasLtMatrixLayoutDestroy", layout)
        self.call("cublasLtMatmulDescDestroy", self.desc)


def launch(driver, metadata, grid, values):
    function = c.c_void_p()
    driver.call(
        "cuModuleGetFunction",
        c.byref(function),
        driver.module,
        c.c_char_p(metadata["entrypoint"].encode()),
    )
    # Keep every pointee alive until cuLaunchKernel has copied the arguments.
    args = [c.c_uint32(v) if kind == "u32" else c.c_uint64(v) for kind, v in values]
    pointers = (c.c_void_p * len(args))(*(c.addressof(value) for value in args))
    driver.call(
        "cuLaunchKernel",
        function,
        c.c_uint(grid[0]),
        c.c_uint(grid[1]),
        c.c_uint(1),
        c.c_uint(metadata["threads"]),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(metadata["shared_bytes"]),
        c.c_void_p(),
        pointers,
        c.c_void_p(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[5]
    output = args.artifact_dir.resolve()
    if not output.is_relative_to(root):
        parser.error("artifacts must be inside the worktree")
    for variable, dirname in {
        "TRITON_CACHE_DIR": "triton-check",
        "CUDA_CACHE_PATH": "cuda-check",
        "TMPDIR": "tmp-check",
    }.items():
        path = output / dirname
        path.mkdir(parents=True, exist_ok=True)
        os.environ[variable] = str(path)
    os.environ.update(
        TRITON_PTXAS_PATH="/usr/local/cuda/bin/ptxas",
        TRITON_PTXAS_BLACKWELL_PATH="/usr/local/cuda/bin/ptxas",
        TRITON_DISABLE_LINE_INFO="1",
    )
    import torch
    import triton
    from check_cuda_recurrent import Driver

    sys.path.insert(0, str(root / "zig/pkg/inference/src/ops/cuda/prototypes"))
    from lighton_head import lighton_head

    if torch.cuda.get_device_capability(0) != (12, 1):
        parser.error("SM12.1 is required")
    torch.set_num_threads(4)
    torch.manual_seed(986)
    torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction = False
    manifest = json.loads((output / "manifest.json").read_text())
    for name, info in manifest["kernels"].items():
        assert (
            hashlib.sha256((output / name).read_bytes()).hexdigest() == info["sha256"]
        )
    lib = c.CDLL("libcublasLt.so.13")
    handle = c.c_void_p()
    assert lib.cublasLtCreate(c.byref(handle)) == 0
    results = []
    try:
        for label, hidden, tile_rows in [("08", 1024, 64), ("4b", 2560, 32)]:
            vocab, tiles = 248320, 1940
            weights = (
                torch.randn(vocab * hidden + 16, device="cuda", dtype=torch.bfloat16)
                * 0.015
            )
            weights[-16:] = 123
            weight = weights[:-16].view(vocab, hidden)
            saved_weight = weight.clone()
            gpu = Driver(output / f"lighton_head_{label}.ptx")
            final = Driver(output / "lighton_head_finish.ptx")
            try:
                for rows in [1, 2, 7, 16, 24, 31, 32, 33, 47, 64]:
                    inputs = (
                        torch.randn(
                            rows * hidden + 16, device="cuda", dtype=torch.bfloat16
                        )
                        * 0.3
                    )
                    inputs[-16:] = 123
                    x = inputs[:-16].view(rows, hidden)
                    x[0] = 0
                    saved_input = x.clone()
                    gold = torch.empty(rows, vocab, device="cuda")
                    projection = Projection(lib, handle, x, weight, gold)
                    projection.run(torch.cuda.current_stream().cuda_stream)
                    full = torch.full((rows * vocab + 16,), 4321.0, device="cuda")
                    maxima = torch.full((rows * tiles + 16,), 4321.0, device="cuda")
                    indices = torch.full(
                        (rows * tiles + 16,), 123456, device="cuda", dtype=torch.int32
                    )
                    tokens = torch.full(
                        (rows + 16,), 123456, device="cuda", dtype=torch.int32
                    )
                    try:
                        for suppressed in [(), (0, 16, 4097)]:
                            lighton_head[(triton.cdiv(rows, tile_rows), tiles)](
                                x,
                                weight,
                                full,
                                maxima,
                                indices,
                                rows,
                                vocab,
                                hidden,
                                tile_rows,
                                128,
                                64,
                                True,
                                suppressed,
                                num_warps=4,
                                num_stages=2,
                                ptx_version=90,
                            )
                            torch.cuda.synchronize()
                            torch.testing.assert_close(
                                full[:-16].view(rows, vocab), gold, atol=2e-4, rtol=0
                            )
                            # Tile selection is checked separately from the full matrix.
                            reference = gold.clone()
                            if suppressed:
                                reference[:, list(suppressed)] = float("-inf")
                            launch(
                                final,
                                manifest["kernels"]["lighton_head_finish.ptx"],
                                (rows, 1),
                                [
                                    ("u64", maxima.data_ptr()),
                                    ("u64", indices.data_ptr()),
                                    ("u64", tokens.data_ptr()),
                                    ("u64", 0),
                                    ("u64", 0),
                                ],
                            )
                            final.call("cuCtxSynchronize")
                            assert torch.equal(
                                tokens[:rows], reference.argmax(-1).int()
                            )
                        torch.cuda.synchronize()
                        tokens[:rows] = -1
                        torch.cuda.synchronize()
                        launch(
                            gpu,
                            manifest["kernels"][f"lighton_head_{label}.ptx"],
                            (triton.cdiv(rows, tile_rows), tiles),
                            [
                                ("u64", x.data_ptr()),
                                ("u64", weight.data_ptr()),
                                ("u64", 0),
                                ("u64", maxima.data_ptr()),
                                ("u64", indices.data_ptr()),
                                ("u32", rows),
                                ("u64", 0),
                                ("u64", 0),
                            ],
                        )
                        launch(
                            final,
                            manifest["kernels"]["lighton_head_finish.ptx"],
                            (rows, 1),
                            [
                                ("u64", maxima.data_ptr()),
                                ("u64", indices.data_ptr()),
                                ("u64", tokens.data_ptr()),
                                ("u64", 0),
                                ("u64", 0),
                            ],
                        )
                        gpu.call("cuCtxSynchronize")
                        assert torch.equal(tokens[:rows], gold.argmax(-1).int())
                        assert int(tokens[0]) == 0
                        assert torch.equal(x, saved_input) and torch.equal(
                            weight, saved_weight
                        )
                        assert torch.all(inputs[-16:] == 123) and torch.all(
                            weights[-16:] == 123
                        )
                        assert torch.all(full[-16:] == 4321.0) and torch.all(
                            maxima[-16:] == 4321.0
                        )
                        assert torch.all(indices[-16:] == 123456) and torch.all(
                            tokens[-16:] == 123456
                        )
                        row = dict(
                            model=label,
                            rows=rows,
                            max_abs_error=float(
                                (full[:-16].view(rows, vocab) - gold).abs().max()
                            ),
                            tokens_exact=True,
                            driver_abi=True,
                            readonly=True,
                            canaries=True,
                        )
                        results.append(row)
                        print(json.dumps(row), flush=True)
                    finally:
                        projection.close()
            finally:
                gpu.close()
                final.close()
    finally:
        assert lib.cublasLtDestroy(handle) == 0
    (output / "qualification.json").write_text(
        json.dumps({"status": "ok", "cases": results}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
