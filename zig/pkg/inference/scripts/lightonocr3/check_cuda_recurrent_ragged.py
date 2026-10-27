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

"""Check compact recurrent groups against the original single-page kernels.

Both OCR geometries cover fresh short prefixes, mixed query lengths, continued
state, resets with poisoned storage, permuted page bindings and canaries.
Every output and both history/state buffers must match bit for bit.
"""

import argparse
import ctypes as c
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import Page, Params, packet_launch, pointer


class Ragged(c.Structure):
    _fields_ = [
        ("base", Params),
        ("offsets", c.c_int * 64),
        ("lengths", c.c_int * 64),
        ("initialized", c.c_int * 64),
    ]


assert c.sizeof(Ragged) == 2408


def launch_fused_decode(gpu, params):
    function = c.c_void_p()
    gpu.call(
        "cuModuleGetFunction",
        c.byref(function),
        gpu.module,
        c.c_char_p(b"lighton_decode_fused_p2"),
    )
    packet = (c.c_void_p * 1)(c.addressof(params))
    gpu.call(
        "cuLaunchKernel",
        function,
        c.c_uint(1),
        c.c_uint(params.base.items * params.base.value_heads),
        c.c_uint(1),
        c.c_uint(256),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(0),
        c.c_void_p(),
        packet,
        c.c_void_p(),
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--artifact", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--fused-decode", action="store_true")
    args = p.parse_args()
    torch.set_num_threads(4)
    torch.manual_seed(963)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for vh in [16, 32]:
            kh = 16
            channels = (2 * kh + vh) * 128
            width = vh * 128
            for count in [4, 16, 64]:
                persistent = []

                def buf(n, random=False, poison=False, guarded=None):
                    t = torch.full(
                        (n + 16,), 123456.0, device="cuda", dtype=torch.float32
                    )
                    if poison:
                        t[:n].fill_(float("nan"))
                    elif random:
                        t[:n].normal_(std=0.2)
                    else:
                        t[:n].zero_()
                    (persistent if guarded is None else guarded).append(t)
                    return t[:n]

                weights = [buf(channels * 4, random=True), buf(vh), buf(vh), buf(128)]
                weights[1].fill_(-0.8)
                weights[2].fill_(0.1)
                weights[3].fill_(1.0)
                serial = [
                    [
                        buf(channels * 4, poison=True),
                        buf(channels * 4, poison=True),
                        buf(vh * 128 * 128, poison=True),
                    ]
                    for _ in range(count)
                ]
                batched = [
                    [
                        buf(channels * 4, poison=True),
                        buf(channels * 4, poison=True),
                        buf(vh * 128 * 128, poison=True),
                    ]
                    for _ in range(count)
                ]
                initialized = [False] * count
                frozen_weights = [x.clone() for x in weights]
                for turn in range(4):
                    order = (
                        list(reversed(range(count)))
                        if turn >= 2
                        else list(range(count))
                    )
                    if turn == 0:
                        lengths = [[1, 3, 17, 33][i % 4] for i in range(count)]
                    elif turn == 1:
                        lengths = [
                            [432, 554, 662][i] if i < 3 else 1 for i in range(count)
                        ]
                    elif turn == 2:
                        lengths = [1] * count
                    else:
                        lengths = [5 if i % 13 == 0 else 1 for i in range(count)]
                    if turn == 2:
                        initialized[0] = False
                        for t in serial[0] + batched[0]:
                            t.fill_(float("nan"))
                    offsets = []
                    total = 0
                    for n in lengths:
                        offsets.append(total)
                        total += n
                    transient = []
                    inputs = [
                        buf(total * dim, random=True, guarded=transient)
                        for dim in [channels, width, vh, vh]
                    ]
                    immutable = [x.clone() for x in inputs]
                    x, z, b, a = inputs
                    cw, alog, dt, norm = weights
                    gold = buf(total * width, guarded=transient)
                    out = buf(total * width, guarded=transient)
                    cc = buf(total * channels, guarded=transient)
                    core = buf(total * width, guarded=transient)
                    conv = buf(total * channels, guarded=transient)
                    group_core = buf(total * width, guarded=transient)
                    torch.cuda.synchronize()
                    for slot, page in enumerate(order):
                        n = lengths[slot]
                        offset = offsets[slot]
                        h, nh, state = serial[page]
                        xx = x[offset * channels :]
                        zz = z[offset * width :]
                        bb = b[offset * vh :]
                        aa = a[offset * vh :]
                        cv = cc[offset * channels :]
                        co = core[offset * width :]
                        go = gold[offset * width :]
                        gpu.launch(
                            "lighton_conv",
                            ((channels + 127) // 128, 1),
                            128,
                            *map(pointer, (xx, cw, h, nh, cv)),
                            n,
                            channels,
                            int(initialized[page]),
                        )
                        gpu.launch(
                            "lighton_normalize",
                            (n * 2 * kh, 1),
                            128,
                            pointer(cv),
                            channels,
                            kh,
                        )
                        gpu.launch(
                            "lighton_recurrent",
                            (4, vh),
                            128,
                            *map(pointer, (cv, bb, aa, alog, dt, state, co)),
                            n,
                            kh,
                            vh,
                            int(initialized[page]),
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (n * vh, 1),
                            128,
                            *map(pointer, (co, zz, norm, go)),
                            vh,
                            1e-6,
                        )
                        serial[page] = [nh, h, state]
                    params = Ragged()
                    base = params.base
                    for field, t in [
                        ("mixed", x),
                        ("conv_weight", cw),
                        ("conv", conv),
                        ("z", z),
                        ("beta", b),
                        ("a", a),
                        ("a_log", alog),
                        ("dt_bias", dt),
                        ("core", group_core),
                        ("norm_weight", norm),
                        ("out", out),
                    ]:
                        setattr(base, field, t.data_ptr())
                    base.key_heads = kh
                    base.value_heads = vh
                    base.items = count
                    base.eps = 1e-6
                    for slot, page in enumerate(order):
                        h, nh, state = batched[page]
                        base.pages[slot] = Page(
                            h.data_ptr(), nh.data_ptr(), state.data_ptr()
                        )
                        params.offsets[slot] = offsets[slot]
                        params.lengths[slot] = lengths[slot]
                        params.initialized[slot] = int(initialized[page])
                    if args.fused_decode:
                        if any(n > 1 for n in lengths):
                            packet_launch(
                                gpu,
                                "lighton_ragged_prefix_conv",
                                (2 * kh + vh, total),
                                params,
                            )
                            packet_launch(
                                gpu,
                                "lighton_ragged_recurrent_prefix_only",
                                (4, count * vh),
                                params,
                            )
                            packet_launch(
                                gpu,
                                "lighton_ragged_prefix_gated_norm",
                                (total * vh, 1),
                                params,
                            )
                        else:
                            # Native pure decode allocates neither temporary.
                            base.conv = 0
                            base.core = 0
                        if any(n == 1 for n in lengths):
                            launch_fused_decode(gpu, params)
                    else:
                        packet_launch(
                            gpu,
                            "lighton_ragged_conv_normalize_cached_gates",
                            (2 * kh + vh, total),
                            params,
                        )
                        packet_launch(
                            gpu,
                            "lighton_ragged_recurrent_shared_qk",
                            (4, count * vh),
                            params,
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (total * vh, 1),
                            128,
                            *map(pointer, (group_core, z, norm, out)),
                            vh,
                            1e-6,
                        )
                    gpu.call("cuCtxSynchronize")
                    assert torch.equal(out.view(torch.int32), gold.view(torch.int32)), (
                        "output",
                        vh,
                        count,
                        turn,
                        int((out.view(torch.int32) != gold.view(torch.int32)).sum()),
                    )
                    for page in range(count):
                        h, nh, state = batched[page]
                        batched[page] = [nh, h, state]
                        for j, (got, want) in enumerate(
                            zip(batched[page], serial[page])
                        ):
                            # The inactive history may still contain poison; compare exact bits.
                            assert torch.equal(
                                got.view(torch.int32), want.view(torch.int32)
                            ), ("state", vh, count, turn, page, j)
                        initialized[page] = True
                    for got, want in zip(inputs, immutable):
                        assert torch.equal(
                            got.view(torch.int32), want.view(torch.int32)
                        )
                    for got, want in zip(weights, frozen_weights):
                        assert torch.equal(
                            got.view(torch.int32), want.view(torch.int32)
                        )
                    for guarded in persistent + transient:
                        assert torch.all(guarded[-16:] == 123456.0)
                    row = dict(
                        value_heads=vh,
                        pages=count,
                        turn=turn,
                        rows=total,
                        bitwise=True,
                        fused_decode=args.fused_decode,
                        readonly=True,
                        canaries=True,
                    )
                    results.append(row)
                    print(json.dumps(row), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=results), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
