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

"""Qualify the optional native BF16 prefill library independently of F32 tests."""

import ctypes as c
import json
import argparse
from pathlib import Path
import torch
from check_cuda_recurrent_ragged import Ragged
from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import packet_launch
from check_cuda_recurrent_batch import Page
from flashinfer.gdn_kernels.delta_rule_dsl.delta_rule_sm120 import (
    delta_rule_prefill_dsl,
)

torch.set_num_threads(2)
torch.manual_seed(128)
parser = argparse.ArgumentParser(
    description="Qualify optional BF16 prefill against the actual pinned FlashInfer wrapper"
)
parser.add_argument("--library", type=Path, required=True)
parser.add_argument(
    "--artifact",
    type=Path,
    default=Path(__file__).resolve().parents[2] / "src/ops/cuda/lighton_recurrent.ptx",
)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
lib = c.CDLL(str(args.library.resolve()))
lib.antfly_lighton_prefill_abi.restype = c.c_uint64
assert lib.antfly_lighton_prefill_abi() == (1 << 32) | 2408
lib.antfly_lighton_prefill_workspace.argtypes = [c.POINTER(Ragged)]
lib.antfly_lighton_prefill_workspace.restype = c.c_size_t
lib.antfly_lighton_prefill_execute.argtypes = [
    c.POINTER(Ragged),
    c.c_void_p,
    c.c_size_t,
    c.c_int,
    c.c_void_p,
]
lib.antfly_lighton_prefill_execute.restype = c.c_int
gpu = Driver(args.artifact)
results = []
for h in [16, 32]:
    for lengths in [
        [2, 1, 3, 1],
        [3, 2, 1, 1],
        [17, 1, 64, 1],
        [256] * 16,
        [1] * 64,
        [128 if i % 4 == 0 else 1 for i in range(64)],
    ]:
        count = len(lengths)
        total = sum(lengths)
        width = h * 128
        channels = (32 + h) * 128
        guards = []

        def alloc(shape, dtype=torch.float32):
            n = 1
            for v in shape:
                n *= v
            size = n * torch.tensor([], dtype=dtype).element_size()
            base = torch.full((size + 512,), 165, dtype=torch.uint8, device="cuda")
            guards.append(base)
            return base[256 : 256 + size].view(dtype).reshape(shape)

        conv = alloc((total, channels))
        conv.normal_(std=0.1)
        conv[:, : 16 * 128].copy_(
            torch.nn.functional.normalize(
                conv[:, : 16 * 128].reshape(total, 16, 128), dim=-1
            ).reshape(total, -1)
            * 128**-0.5
        )
        conv[:, 16 * 128 : 32 * 128].copy_(
            torch.nn.functional.normalize(
                conv[:, 16 * 128 : 32 * 128].reshape(total, 16, 128), dim=-1
            ).reshape(total, -1)
        )
        gates = alloc((total, h, 128))
        gates.fill_(-12345.0)
        gates[:, :, 0].copy_(torch.exp(-torch.rand(total, h, device="cuda") * 0.1))
        gates[:, :, 1].copy_(torch.sigmoid(torch.randn(total, h, device="cuda")))
        core = alloc((total, h, 128))
        core.fill_(-12345.0)
        states = [alloc((h, 128, 128)) for _ in lengths]
        histories = [alloc((channels * 4,)) for _ in lengths]
        next_histories = [alloc((channels * 4,)) for _ in lengths]
        for v in states + histories + next_histories:
            v.normal_(std=0.01)
        offsets = []
        at = 0
        for n in lengths:
            offsets.append(at)
            at += n
        prefixes = [i for i, n in enumerate(lengths) if n > 1]
        p = Ragged()
        p.base.items = count
        p.base.key_heads = 16
        p.base.value_heads = h
        p.base.conv = conv.data_ptr()
        p.base.out = gates.data_ptr()
        p.base.core = core.data_ptr()
        mixed = alloc((total, channels))
        mixed.normal_(std=0.2)
        cw = alloc((channels, 4))
        cw.normal_(std=0.2)
        raw_a = alloc((total, h))
        raw_a.normal_(std=0.2)
        raw_b = alloc((total, h))
        raw_b.normal_(std=0.2)
        alog = alloc((h,))
        alog.fill_(-0.8)
        dt = alloc((h,))
        dt.fill_(0.1)
        for name, tensor in [
            ("mixed", mixed),
            ("conv_weight", cw),
            ("a", raw_a),
            ("beta", raw_b),
            ("a_log", alog),
            ("dt_bias", dt),
        ]:
            setattr(p.base, name, tensor.data_ptr())
        for turn in range(4):
            order = list(range(count)) if turn < 2 else list(reversed(range(count)))
            initialized = [
                turn != 0 and not (turn == 2 and j == order[0]) for j in order
            ]
            for i, j in enumerate(order):
                if not initialized[i] and i in prefixes:
                    states[j].fill_(float("nan"))
                p.offsets[i] = offsets[i]
                p.lengths[i] = lengths[i]
                p.initialized[i] = int(initialized[i])
                p.base.pages[i] = Page(
                    histories[j].data_ptr(),
                    next_histories[j].data_ptr(),
                    states[j].data_ptr(),
                )
            before_next = [x.clone() for x in next_histories]
            torch.cuda.synchronize()
            packet_launch(
                gpu, "lighton_ragged_prefix_conv", (channels // 128, total), p
            )
            torch.cuda.synchronize()
            expected_next = [x.clone() for x in next_histories]
            # Derive fresh history directly from input placement, independently of
            # both the candidate and the convolution qualification oracle.
            for i, n in enumerate(lengths):
                if not initialized[i] and n > 1:
                    expected = torch.zeros((channels, 4), device="cuda")
                    tail = mixed[offsets[i] + max(0, n - 4) : offsets[i] + n].T
                    expected[:, 4 - tail.shape[1] :] = tail
                    assert torch.equal(
                        expected_next[order[i]].reshape(channels, 4), expected
                    ), "independent left padding"
            for actual, before in zip(next_histories, before_next):
                actual.copy_(before)
            immutable = [conv, gates, *histories, mixed, cw, raw_a, raw_b, alog, dt]
            snapshots = [x.clone() for x in immutable]
            before_states = [x.clone() for x in states]
            before_core = core.clone()
            expected_core = core.clone()
            expected_states = [x.clone() for x in states]
            if prefixes:
                q = (
                    torch.cat(
                        [
                            conv[
                                offsets[i] : offsets[i] + lengths[i], : 16 * 128
                            ].reshape(lengths[i], 16, 128)
                            for i in prefixes
                        ]
                    )
                    .bfloat16()
                    .contiguous()
                )
                k = (
                    torch.cat(
                        [
                            conv[
                                offsets[i] : offsets[i] + lengths[i],
                                16 * 128 : 32 * 128,
                            ].reshape(lengths[i], 16, 128)
                            for i in prefixes
                        ]
                    )
                    .bfloat16()
                    .contiguous()
                )
                v = (
                    torch.cat(
                        [
                            conv[
                                offsets[i] : offsets[i] + lengths[i], 32 * 128 :
                            ].reshape(lengths[i], h, 128)
                            for i in prefixes
                        ]
                    )
                    .bfloat16()
                    .contiguous()
                )
                alpha = torch.cat(
                    [
                        gates[offsets[i] : offsets[i] + lengths[i], :, 0]
                        for i in prefixes
                    ]
                ).contiguous()
                beta = torch.cat(
                    [
                        gates[offsets[i] : offsets[i] + lengths[i], :, 1]
                        for i in prefixes
                    ]
                ).contiguous()
                initial = torch.stack(
                    [
                        states[order[i]].transpose(-1, -2).contiguous()
                        if initialized[i]
                        else torch.zeros_like(states[order[i]])
                        for i in prefixes
                    ]
                )
                expected_o = torch.empty_like(v)
                expected_s = torch.empty_like(initial)
                cu = torch.tensor(
                    [0]
                    + list(
                        torch.tensor([lengths[i] for i in prefixes]).cumsum(0).tolist()
                    ),
                    device="cuda",
                    dtype=torch.int64,
                )
                delta_rule_prefill_dsl(
                    expected_o, expected_s, q, k, v, initial, alpha, beta, cu, 1.0
                )
                row = 0
                for n, i in enumerate(prefixes):
                    expected_core[offsets[i] : offsets[i] + lengths[i]] = expected_o[
                        row : row + lengths[i]
                    ].float()
                    expected_states[order[i]] = (
                        expected_s[n].transpose(-1, -2).contiguous()
                    )
                    row += lengths[i]
            needed = lib.antfly_lighton_prefill_workspace(c.byref(p))
            assert needed != (1 << 64) - 1
            scratch = alloc((needed,), torch.uint8) if needed else None
            torch.cuda.synchronize()
            if needed:
                rc = lib.antfly_lighton_prefill_execute(
                    c.byref(p),
                    scratch.data_ptr(),
                    needed - 1,
                    0,
                    torch.cuda.current_stream().cuda_stream,
                )
                assert rc == 3, rc
                for actual, before in zip(states, before_states):
                    assert torch.equal(
                        actual.view(torch.uint8), before.view(torch.uint8)
                    )
                assert torch.equal(core, before_core)
                for actual, before in zip(next_histories, before_next):
                    assert torch.equal(actual, before), "denied history mutation"
            rc = lib.antfly_lighton_prefill_execute(
                c.byref(p),
                scratch.data_ptr() if scratch is not None else 0,
                needed,
                0,
                torch.cuda.current_stream().cuda_stream,
            )
            assert rc == 0, rc
            torch.cuda.synchronize()
            assert torch.equal(core, expected_core), (
                h,
                lengths,
                turn,
                "core",
                (core - expected_core).abs().max().item(),
            )
            for actual, expected in zip(states, expected_states):
                assert torch.equal(
                    actual.view(torch.uint8), expected.view(torch.uint8)
                ), (h, lengths, turn, "state")
            for actual, before in zip(immutable, snapshots):
                assert torch.equal(actual, before), "readonly"
            for actual, expected in zip(next_histories, expected_next):
                assert torch.equal(actual, expected), "history"
            for base in guards:
                assert bool((base[:256] == 165).all() and (base[-256:] == 165).all()), (
                    "canaries"
                )
            result = {
                "heads": h,
                "pages": count,
                "rows": total,
                "prefixes": len(prefixes),
                "turn": turn,
                "scratch_bytes": needed,
                "wrapper_output_state_bitwise": True,
                "readonly": True,
                "guards": True,
                "short_scratch_denied_before_mutation": bool(needed),
            }
            results.append(result)
            print(result, flush=True)
gpu.close()
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(
    json.dumps(
        {
            "cases": results,
            "precision": "BF16 recurrent prefill operands/output; F32 state. Separate wrapper oracle; original F32 tolerances unchanged.",
        },
        indent=2,
    )
    + "\n"
)
print(f"{len(results)} BF16 prefill cases passed", flush=True)
