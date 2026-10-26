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

"""Check independent prefix pages against the qualified F32 single-page path."""

import argparse
import ctypes as c
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import Page, Params, packet_launch, pointer


class PrefixParams(c.Structure):
    _fields_ = [("base", Params), ("seq", c.c_uint32), ("reserved", c.c_uint32)]


assert c.sizeof(PrefixParams) == 1648 and PrefixParams.seq.offset == 1640


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shared-qk", action="store_true")
    args = parser.parse_args()
    torch.manual_seed(946)
    torch.set_num_threads(4)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for kh, vh in [(16, 16), (16, 32)]:
            channels, width = (2 * kh + vh) * 128, vh * 128
            for count, seq in [(2, 3), (4, 432), (8, 662), (12, 554), (16, 432)]:
                for reverse in [False, True]:
                    guarded = []

                    def buffer(size, random=False, poison=False):
                        value = torch.full((size + 16,), 123456.0, device="cuda")
                        if random:
                            value[:size].normal_(std=0.3)
                        else:
                            value[:size].fill_(float("nan") if poison else 0.0)
                        guarded.append(value)
                        return value[:size]

                    mixed, z, beta, a = [
                        buffer(count * seq * n, True) for n in (channels, width, vh, vh)
                    ]
                    cw, alog, dt, norm = [
                        buffer(n, True) for n in (channels * 4, vh, vh, 128)
                    ]
                    inputs = [mixed, z, beta, a, cw, alog, dt, norm]
                    frozen = [x.clone() for x in inputs]
                    states = [
                        [
                            (
                                buffer(channels * 4, poison=True),
                                buffer(channels * 4, poison=True),
                                buffer(vh * 128 * 128, poison=True),
                            )
                            for _ in range(count)
                        ]
                        for _ in range(2)
                    ]
                    conv, core, out = [
                        buffer(count * seq * n) for n in (channels, width, width)
                    ]
                    torch.cuda.synchronize()
                    for page, (history, next_history, state) in enumerate(states[0]):
                        xx, zz, bb, aa, cc, yy, oo = [
                            value[page * seq * n :]
                            for value, n in zip(
                                (mixed, z, beta, a, conv, core, out),
                                (channels, width, vh, vh, channels, width, width),
                            )
                        ]
                        gpu.launch(
                            "lighton_conv_cached_gates",
                            (channels // 128, 1),
                            128,
                            *map(pointer, (xx, cw, history, next_history, cc)),
                            seq,
                            channels,
                            0,
                            *map(pointer, (bb, aa, alog, dt, oo)),
                            vh,
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
                            "lighton_recurrent_cached_gates",
                            (4, vh),
                            128,
                            *map(pointer, (cc, bb, aa, alog, dt, state, yy)),
                            seq,
                            kh,
                            vh,
                            0,
                            pointer(oo),
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (seq * vh, 1),
                            128,
                            *map(pointer, (yy, zz, norm, oo)),
                            vh,
                            1e-6,
                        )
                    gpu.call("cuCtxSynchronize")
                    gold = out.clone()
                    order = (
                        list(reversed(range(count))) if reverse else list(range(count))
                    )
                    reordered = [
                        value.reshape(count, seq, n)[order].contiguous()
                        for value, n in zip(
                            (mixed, z, beta, a), (channels, width, vh, vh)
                        )
                    ]
                    batch_conv, batch_core, batch_out = [
                        buffer(count * seq * n) for n in (channels, width, width)
                    ]
                    p = PrefixParams(
                        base=Params(
                            mixed=reordered[0].data_ptr(),
                            z=reordered[1].data_ptr(),
                            beta=reordered[2].data_ptr(),
                            a=reordered[3].data_ptr(),
                            conv_weight=cw.data_ptr(),
                            a_log=alog.data_ptr(),
                            dt_bias=dt.data_ptr(),
                            norm_weight=norm.data_ptr(),
                            conv=batch_conv.data_ptr(),
                            core=batch_core.data_ptr(),
                            out=batch_out.data_ptr(),
                            key_heads=kh,
                            value_heads=vh,
                            items=count,
                            eps=1e-6,
                        ),
                        seq=seq,
                    )
                    for slot, page in enumerate(order):
                        p.base.pages[slot] = Page(
                            *(t.data_ptr() for t in states[1][page])
                        )
                    torch.cuda.synchronize()
                    packet_launch(
                        gpu,
                        "lighton_prefix_batch_conv_normalize",
                        (channels // 128, count * seq),
                        p,
                    )
                    packet_launch(
                        gpu,
                        "lighton_prefix_batch_recurrent_shared_qk"
                        if args.shared_qk
                        else "lighton_prefix_batch_recurrent",
                        (4, count * vh),
                        p,
                    )
                    gpu.launch(
                        "lighton_gated_norm",
                        (count * seq * vh, 1),
                        128,
                        *map(pointer, (batch_core, reordered[1], norm, batch_out)),
                        vh,
                        1e-6,
                    )
                    gpu.call("cuCtxSynchronize")
                    expected = gold.reshape(count, seq, width)[order].flatten()
                    assert torch.equal(
                        batch_out.view(torch.int32), expected.view(torch.int32)
                    ), (
                        "output",
                        kh,
                        vh,
                        count,
                        seq,
                        reverse,
                        (batch_out - expected).abs().max().item(),
                    )
                    for page in range(count):
                        for i in (1, 2):
                            assert torch.equal(
                                states[0][page][i], states[1][page][i]
                            ), ("state/history", page, i)
                    for source, saved in zip(inputs, frozen):
                        assert torch.equal(
                            source.view(torch.int32), saved.view(torch.int32)
                        ), "read-only input modified"
                    for value in guarded:
                        assert torch.all(value[-16:] == 123456.0), (
                            "buffer guard overwritten"
                        )
                    row = dict(
                        kh=kh,
                        vh=vh,
                        pages=count,
                        seq=seq,
                        reversed=reverse,
                        bitwise=True,
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
