#!/usr/bin/env python3
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

"""Differential check of chunked CUDA DeltaNet against its FP32 recurrence."""

import argparse
import ctypes as C
from pathlib import Path
import torch


class Params(C.Structure):
    _fields_ = [
        (name, C.c_uint64)
        for name in (
            "conv",
            "gates",
            "w",
            "attention",
            "scaled_k",
            "u",
            "decay",
            "state",
            "out",
        )
    ] + [("tokens", C.c_uint32), ("heads", C.c_uint32)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--ptx", type=Path)
    parser.add_argument("--tokens", help="Comma-separated token counts")
    parser.add_argument("--heads", type=int)
    parser.add_argument("--scan-threads", type=int, default=256)
    args = parser.parse_args()
    torch.manual_seed(1649)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.empty(1, device="cuda")
    driver = C.CDLL("libcuda.so.1")

    def check(code):
        if code:
            raise RuntimeError(f"CUDA driver error {code}")

    module = C.c_void_p()
    image = C.create_string_buffer(
        (
            args.ptx
            or Path(__file__).resolve().parents[2] / "src/ops/cuda/glide_delta.ptx"
        ).read_bytes()
    )
    check(driver.cuModuleLoadDataEx(C.byref(module), image, 0, None, None))
    funcs = []
    try:
        for name in ("glide_delta_prepare", "glide_delta_scan"):
            f = C.c_void_p()
            check(driver.cuModuleGetFunction(C.byref(f), module, name.encode()))
            funcs.append(f)
        lengths = (
            list(map(int, args.tokens.split(",")))
            if args.tokens
            else ([106, 5129] if args.bench else [1, 31, 32, 33, 65, 129, 513])
        )
        for n in lengths:
            h = args.heads or (16 if args.bench else 2)
            q, k = [
                torch.nn.functional.normalize(
                    torch.randn(n, h, 128, device="cuda"), dim=-1
                )
                for _ in range(2)
            ]
            q /= 128**0.5
            v = torch.randn(n, h, 128, device="cuda")
            g = torch.rand(n, h, device="cuda") * 0.2 + 0.8
            beta = torch.rand(n, h, device="cuda")
            if n > 32:
                g[32] = 0  # exact reset; no division by cumulative decay
            conv = torch.stack([q, k, v], dim=1).contiguous()
            gates = torch.zeros(n, h, 128, device="cuda")
            gates[:, :, 0] = g
            gates[:, :, 1] = beta
            chunks = (n + 31) // 32
            w = torch.empty(chunks, h, 32, 128, device="cuda", dtype=torch.float16)
            attention = torch.empty(
                chunks, h, 32, 32, device="cuda", dtype=torch.float16
            )
            scaled_k = torch.empty_like(w)
            u = torch.empty_like(w, dtype=torch.float32)
            decay = torch.empty(chunks, h, 32, device="cuda")
            state = torch.empty(h, 128, 128, device="cuda")
            out = torch.empty(n, h, 128, device="cuda")
            p = Params(
                *(
                    t.data_ptr()
                    for t in (conv, gates, w, attention, scaled_k, u, decay, state, out)
                ),
                n,
                h,
            )
            packet = (C.c_void_p * 1)(C.addressof(p))

            def launch():
                for index, (f, gx) in enumerate(zip(funcs, [chunks, 4])):
                    check(
                        driver.cuLaunchKernel(
                            f,
                            gx,
                            h,
                            1,
                            128 if index == 0 else args.scan_threads,
                            1,
                            1,
                            0,
                            C.c_void_p(torch.cuda.current_stream().cuda_stream),
                            packet,
                            None,
                        )
                    )

            launch()
            if args.bench:
                for _ in range(5):
                    launch()
                begin, end = (
                    torch.cuda.Event(enable_timing=True),
                    torch.cuda.Event(enable_timing=True),
                )
                begin.record()
                for _ in range(10):
                    launch()
                end.record()
                end.synchronize()
                print(
                    f"BENCH tokens={n} heads={h} ms={begin.elapsed_time(end) / 10:.3f}",
                    flush=True,
                )
                continue
            expected_state = torch.zeros_like(state)
            expected = []
            for t in range(n):
                expected_state *= g[t, :, None, None]
                delta = (
                    v[t] - torch.einsum("hk,hkv->hv", k[t], expected_state)
                ) * beta[t, :, None]
                expected_state += k[t, :, :, None] * delta[:, :, None].transpose(1, 2)
                expected.append(torch.einsum("hk,hkv->hv", q[t], expected_state))
            expected = torch.stack(expected)
            torch.testing.assert_close(out, expected, atol=3e-4, rtol=3e-3)
            torch.testing.assert_close(state, expected_state, atol=3e-3, rtol=3e-3)
            print(
                f"PASS tokens={n} max_output={(out - expected).abs().max().item():.3g} max_state={(state - expected_state).abs().max().item():.3g}",
                flush=True,
            )
    finally:
        torch.cuda.synchronize()
        check(driver.cuModuleUnload(module))


if __name__ == "__main__":
    main()
