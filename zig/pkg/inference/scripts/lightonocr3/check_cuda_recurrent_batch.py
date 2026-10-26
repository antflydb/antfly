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

"""Compare batched recurrent PTX against independently qualified single pages.

The stdlib CPU oracle is check_cuda_recurrent.py. This additional GPU check
covers the two OCR head geometries at 2, 16 and 64 pages, multiple continuations,
permuted bindings, read-only inputs, and buffer canaries. Requires PyTorch.
"""

import argparse
import ctypes as c
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver


class Page(c.Structure):
    _fields_ = [(name, c.c_uint64) for name in ("history", "next_history", "state")]


class Params(c.Structure):
    _fields_ = (
        [
            (name, c.c_uint64)
            for name in (
                "mixed",
                "conv_weight",
                "conv",
                "z",
                "beta",
                "a",
                "a_log",
                "dt_bias",
                "core",
                "norm_weight",
                "out",
            )
        ]
        + [(name, c.c_uint32) for name in ("key_heads", "value_heads", "items")]
        + [
            ("eps", c.c_float),
            ("pages", Page * 64),
        ]
    )


assert c.sizeof(Page) == 24 and c.sizeof(Params) == 1640


def packet_launch(gpu, name, grid, params, block=128):
    function = c.c_void_p()
    gpu.call(
        "cuModuleGetFunction", c.byref(function), gpu.module, c.c_char_p(name.encode())
    )
    args = (c.c_void_p * 1)(c.addressof(params))
    gpu.call(
        "cuLaunchKernel",
        function,
        c.c_uint(grid[0]),
        c.c_uint(grid[1]),
        c.c_uint(1),
        c.c_uint(block),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(0),
        c.c_void_p(),
        args,
        c.c_void_p(),
    )


def pointer(value):
    return (c.c_uint64(value.data_ptr()), value.numel())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fused", action="store_true")
    parser.add_argument("--cached-gates", action="store_true")
    args = parser.parse_args()
    if args.cached_gates and not args.fused:
        parser.error("cached gates require --fused")
    torch.manual_seed(927)
    torch.set_num_threads(4)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for kh, vh in [(16, 16), (16, 32)]:
            channels, width = (2 * kh + vh) * 128, vh * 128
            for count in (2, 16, 64):
                guarded = []

                def buffer(size, random=False):
                    value = torch.full((size + 16,), 123456.0, device="cuda")
                    if random:
                        value[:size].normal_(std=0.3)
                    else:
                        value[:size].zero_()
                    guarded.append(value)
                    return value[:size]

                cw = buffer(channels * 4, True)
                alog = buffer(vh)
                dt = buffer(vh, True)
                norm = buffer(128, True)
                serial = [
                    (buffer(channels * 4), buffer(channels * 4), buffer(vh * 128 * 128))
                    for _ in range(count)
                ]
                batched = [
                    (buffer(channels * 4), buffer(channels * 4), buffer(vh * 128 * 128))
                    for _ in range(count)
                ]
                max_error = 0.0
                for step in range(4):
                    seq = 3 if step == 0 else 1
                    mixed, z, beta, a = [
                        buffer(count * seq * n, True) for n in (channels, width, vh, vh)
                    ]
                    conv = buffer(count * seq * channels)
                    core = buffer(count * seq * width)
                    out = buffer(count * seq * width)
                    frozen = [
                        x.clone() for x in (mixed, z, beta, a, cw, alog, dt, norm)
                    ]
                    gold = []
                    # Torch data creation uses its own stream; PTX runs on the
                    # default stream of the same primary context.
                    torch.cuda.synchronize()
                    for page, (history, next_history, state) in enumerate(serial):
                        xx = mixed[page * seq * channels :]
                        zz = z[page * seq * width :]
                        bb = beta[page * seq * vh :]
                        aa = a[page * seq * vh :]
                        cc = conv[page * seq * channels :]
                        yy = core[page * seq * width :]
                        oo = out[page * seq * width :]
                        gpu.launch(
                            "lighton_conv",
                            ((channels + 127) // 128, 1),
                            128,
                            pointer(xx),
                            pointer(cw),
                            pointer(history),
                            pointer(next_history),
                            pointer(cc),
                            seq,
                            channels,
                            int(step > 0),
                        )
                        gpu.launch(
                            "lighton_normalize",
                            (seq * 2 * kh, 1),
                            128,
                            pointer(cc),
                            channels,
                            kh,
                        )
                        gpu.launch(
                            "lighton_recurrent",
                            (4, vh),
                            128,
                            pointer(cc),
                            pointer(bb),
                            pointer(aa),
                            pointer(alog),
                            pointer(dt),
                            pointer(state),
                            pointer(yy),
                            seq,
                            kh,
                            vh,
                            int(step > 0),
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (seq * vh, 1),
                            128,
                            pointer(yy),
                            pointer(zz),
                            pointer(norm),
                            pointer(oo),
                            vh,
                            1e-6,
                        )
                        serial[page] = (next_history, history, state)
                    gpu.call("cuCtxSynchronize")
                    gold = out.clone()
                    if step == 0:
                        for source, destination in zip(serial, batched):
                            # Both histories are copied to preserve the exact
                            # double-buffer phase after the prefix.
                            for src, dst in zip(source, destination):
                                dst.copy_(src)
                        torch.cuda.synchronize()
                        continue
                    # Physical allocation order is unrelated to request order.
                    order = list(reversed(range(count)))
                    # Bind reordered inputs and expected rows together.
                    x = mixed.reshape(count, channels)[order].contiguous()
                    zz = z.reshape(count, width)[order].contiguous()
                    bb = beta.reshape(count, vh)[order].contiguous()
                    aa = a.reshape(count, vh)[order].contiguous()
                    p = Params(
                        mixed=x.data_ptr(),
                        conv_weight=cw.data_ptr(),
                        conv=conv.data_ptr(),
                        z=zz.data_ptr(),
                        beta=bb.data_ptr(),
                        a=aa.data_ptr(),
                        a_log=alog.data_ptr(),
                        dt_bias=dt.data_ptr(),
                        core=core.data_ptr(),
                        norm_weight=norm.data_ptr(),
                        out=out.data_ptr(),
                        key_heads=kh,
                        value_heads=vh,
                        items=count,
                        eps=1e-6,
                    )
                    for row, page in enumerate(order):
                        h, nh, state = batched[page]
                        p.pages[row] = Page(
                            h.data_ptr(), nh.data_ptr(), state.data_ptr()
                        )
                    torch.cuda.synchronize()
                    if args.fused:
                        packet_launch(
                            gpu,
                            "lighton_batch_conv_normalize_cached_gates"
                            if args.cached_gates
                            else "lighton_batch_conv_normalize",
                            (channels // 128, count),
                            p,
                        )
                        packet_launch(
                            gpu,
                            "lighton_batch_recurrent_norm_cached_gates"
                            if args.cached_gates
                            else "lighton_batch_recurrent_norm",
                            (1, count * vh),
                            p,
                            block=512,
                        )
                    else:
                        packet_launch(
                            gpu, "lighton_batch_conv", (channels // 128, count), p
                        )
                        gpu.launch(
                            "lighton_normalize",
                            (count * 2 * kh, 1),
                            128,
                            pointer(conv),
                            channels,
                            kh,
                        )
                        packet_launch(
                            gpu, "lighton_batch_recurrent", (4, count * vh), p
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (count * vh, 1),
                            128,
                            pointer(core),
                            pointer(zz),
                            pointer(norm),
                            pointer(out),
                            vh,
                            1e-6,
                        )
                    gpu.call("cuCtxSynchronize")
                    expected = gold.reshape(count, width)[order]
                    error = (out.reshape(count, width) - expected).abs().max().item()
                    max_error = max(max_error, error)
                    torch.testing.assert_close(
                        out.reshape(count, width), expected, rtol=2e-5, atol=2e-5
                    )
                    for page, (h, nh, state) in enumerate(batched):
                        batched[page] = (nh, h, state)
                        for got, want in zip(batched[page], serial[page]):
                            torch.testing.assert_close(got, want, rtol=2e-5, atol=2e-5)
                    for value, original in zip(
                        (mixed, z, beta, a, cw, alog, dt, norm), frozen
                    ):
                        torch.testing.assert_close(value, original, rtol=0, atol=0)
                    for value in guarded:
                        assert bool((value[-16:] == 123456.0).all()), (
                            "buffer canary changed"
                        )
                results.append(
                    {
                        "key_heads": kh,
                        "value_heads": vh,
                        "pages": count,
                        "continuations": 3,
                        "fused": args.fused,
                        "cached_gates": args.cached_gates,
                        "max_abs_error": max_error,
                    }
                )
                print(json.dumps(results[-1]), flush=True)
                guarded.clear()
    finally:
        gpu.close()
    args.output.write_text(json.dumps({"status": "passed", "cases": results}, indent=2))


if __name__ == "__main__":
    main()
