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

"""Check packed Q/K/V, softmax, and output CUDA kernels against dense Torch.

Torch performs the two matrix products here; the native benchmark separately
checks the production cuBLASLt dispatch and full-model parity.
"""

import ctypes as C
import argparse
from pathlib import Path
import torch


class Params(C.Structure):
    _fields_ = (
        [
            (n, C.c_uint64)
            for n in (
                "q",
                "k",
                "v",
                "out",
                "qp",
                "kp",
                "vp",
                "scores",
                "probabilities",
                "op",
            )
        ]
        + [
            (n, C.c_uint32)
            for n in (
                "query_start",
                "key_start",
                "queries",
                "keys",
                "heads",
                "kv_heads",
            )
        ]
        + [("scale", C.c_float), ("padded_keys", C.c_uint32)]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", action="store_true")
    parser.add_argument(
        "--expanded-kv",
        dest="compact",
        action="store_false",
        help="Check older PTX that repeats K/V for every query head",
    )
    parser.add_argument(
        "--tile",
        type=int,
        default=128,
        help="Query rows in the long attention check/benchmark",
    )
    parser.add_argument("--ptx", type=Path)
    parser.add_argument("--legacy-kv-layout", action="store_true")
    options = parser.parse_args()
    if options.legacy_kv_layout:
        options.compact = False
    torch.manual_seed(1649)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction = False
    torch.empty(1, device="cuda")
    lib = C.CDLL("libcuda.so.1")

    def check(code):
        if code:
            raise RuntimeError(f"CUDA driver error {code}")

    module = C.c_void_p()
    image = C.create_string_buffer(
        (
            options.ptx
            or Path(__file__).resolve().parents[2] / "src/ops/cuda/glide_attention.ptx"
        ).read_bytes()
    )
    check(lib.cuModuleLoadDataEx(C.byref(module), image, 0, None, None))
    funcs = []
    try:
        for name in (
            "glide_pack_kv",
            "glide_pack_q",
            "glide_softmax",
            "glide_unpack_o",
        ):
            fun = C.c_void_p()
            check(lib.cuModuleGetFunction(C.byref(fun), module, name.encode()))
            funcs.append(fun)
        for queries, keys in (
            (1, 1),
            (3, 33),
            (17, 257),
            (129, 513),
            (options.tile, 5129),
            (3, 8192),
            (3, 8193),
        ):
            h, kh, d = 8, 2, 256
            packed_h = kh if options.compact else h
            padded = (keys + 15) // 16 * 16
            q = torch.randn(queries + 7, h, d, device="cuda")
            k, v = [torch.randn(keys + 11, kh, d, device="cuda") for _ in range(2)]
            out = torch.full_like(q, float("nan"))
            qp = torch.empty(h, queries, d, device="cuda", dtype=torch.float16)
            kp = torch.empty(packed_h, padded, d, device="cuda", dtype=torch.float16)
            vp = torch.empty(packed_h, d, padded, device="cuda", dtype=torch.float16)
            scores = torch.empty(h, queries, padded, device="cuda")
            probabilities = torch.empty_like(scores, dtype=torch.float16)
            op = torch.empty(h, queries, d, device="cuda")
            p = Params(
                *(
                    t.data_ptr()
                    for t in (q, k, v, out, qp, kp, vp, scores, probabilities, op)
                ),
                3,
                5,
                queries,
                keys,
                h,
                kh,
                1 / 16,
                padded,
            )
            args = (C.c_void_p * 1)(C.addressof(p))

            def launch(i):
                blocks = (packed_h if i == 0 else h) * (
                    (
                        (padded + 31) // 32 * 8
                        if not options.legacy_kv_layout
                        else padded
                    )
                    if i == 0
                    else queries
                )
                check(
                    lib.cuLaunchKernel(
                        funcs[i],
                        blocks,
                        1,
                        1,
                        256,
                        1,
                        1,
                        0,
                        C.c_void_p(torch.cuda.current_stream().cuda_stream),
                        args,
                        None,
                    )
                )

            launch(0)
            if options.bench and keys == 5129:
                for _ in range(5):
                    launch(0)
                begin, end = (torch.cuda.Event(enable_timing=True) for _ in range(2))
                begin.record()
                for _ in range(100):
                    launch(0)
                end.record()
                end.synchronize()
                print(f"pack_kv keys={keys} ms={begin.elapsed_time(end) / 100:.6f}")
            launch(1)
            torch.testing.assert_close(
                qp, q[3 : 3 + queries].transpose(0, 1).half(), atol=0, rtol=0
            )
            torch.testing.assert_close(
                kp[:, :keys],
                (
                    k[5 : 5 + keys]
                    if options.compact
                    else k[5 : 5 + keys].repeat_interleave(4, dim=1)
                )
                .transpose(0, 1)
                .half(),
                atol=0,
                rtol=0,
            )
            torch.testing.assert_close(
                vp[:, :, :keys],
                (
                    v[5 : 5 + keys]
                    if options.compact
                    else v[5 : 5 + keys].repeat_interleave(4, dim=1)
                )
                .permute(1, 2, 0)
                .half(),
                atol=0,
                rtol=0,
            )
            assert (
                not kp[:, keys:].count_nonzero() and not vp[:, :, keys:].count_nonzero()
            )

            def qk():
                if options.compact:
                    for group in range(kh):
                        start = group * (h // kh)
                        end = start + h // kh
                        torch.mm(
                            qp[start:end].reshape(-1, d),
                            kp[group].T,
                            out_dtype=torch.float32,
                            out=scores[start:end].reshape(-1, padded),
                        )
                else:
                    torch.bmm(
                        qp, kp.transpose(1, 2), out_dtype=torch.float32, out=scores
                    )

            def av():
                if options.compact:
                    for group in range(kh):
                        start = group * (h // kh)
                        end = start + h // kh
                        torch.mm(
                            probabilities[start:end].reshape(-1, padded),
                            vp[group].T,
                            out_dtype=torch.float32,
                            out=op[start:end].reshape(-1, d),
                        )
                else:
                    torch.bmm(
                        probabilities,
                        vp.transpose(1, 2),
                        out_dtype=torch.float32,
                        out=op,
                    )

            qk()
            launch(2)
            torch.testing.assert_close(
                probabilities[:, :, :keys],
                torch.softmax(scores[:, :, :keys] / 16, dim=-1).half(),
                atol=1e-6,
                rtol=1e-3,
            )
            assert not probabilities[:, :, keys:].count_nonzero()
            if options.bench and keys == 5129:
                for _ in range(5):
                    launch(2)
                start, end = (torch.cuda.Event(enable_timing=True) for _ in range(2))
                start.record()
                for _ in range(100):
                    launch(2)
                end.record()
                end.synchronize()
                print(
                    f"softmax queries={queries} keys={keys} ms={start.elapsed_time(end) / 100:.6f}"
                )
            av()
            if options.bench and keys == 5129:

                def run():
                    launch(1)
                    qk()
                    launch(2)
                    av()
                    launch(3)

                for _ in range(5):
                    run()
                begin, end = (torch.cuda.Event(enable_timing=True) for _ in range(2))
                begin.record()
                for _ in range(30):
                    run()
                end.record()
                end.synchronize()
                ms = begin.elapsed_time(end) / 30
                print(
                    f"attention_tile compact={options.compact} queries={queries} keys={keys} ms={ms:.6f}",
                    flush=True,
                )
            launch(3)
            want = torch.softmax(
                torch.bmm(
                    q[3 : 3 + queries].transpose(0, 1),
                    k[5 : 5 + keys].repeat_interleave(4, dim=1).permute(1, 2, 0),
                )
                / 16,
                dim=-1,
            )
            want = torch.bmm(
                want, v[5 : 5 + keys].repeat_interleave(4, dim=1).transpose(0, 1)
            ).transpose(0, 1)
            torch.testing.assert_close(out[3 : 3 + queries], want, atol=2e-3, rtol=2e-3)
            assert out[:3].isnan().all() and out[3 + queries :].isnan().all()
            print(
                f"PASS queries={queries} keys={keys} offset_guards=intact max_error={(out[3 : 3 + queries] - want).abs().max().item():.3g}"
            )
    finally:
        torch.cuda.synchronize()
        check(lib.cuModuleUnload(module))


if __name__ == "__main__":
    main()
