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

"""Check F32 SiLU/multiply fused with the original BF16 input conversion."""

import argparse
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import pointer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--artifact", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    torch.set_num_threads(4)
    torch.manual_seed(965)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    records = []
    try:
        for dim in [3584, 9216]:
            for rows in [1, 129, 1728, 2592, 6912, 10368]:
                n = rows * dim
                guards = []

                def output():
                    t = torch.full((n + 16,), 4321, device="cuda", dtype=torch.uint16)
                    guards.append(t)
                    return t[:n]

                backing = torch.full((rows * dim * 2 + 16,), 4321.0, device="cuda")
                joined = backing[: rows * dim * 2]
                joined.normal_(std=0.3)
                joined[:8] = torch.tensor(
                    [0.0, -0.0, -100.0, -20.0, 20.0, 100.0, 1.00390625, -1.00390625],
                    device="cuda",
                )
                original = joined.clone()
                gate = joined.reshape(rows, 2 * dim)[:, :dim].contiguous().view(-1)
                up = joined.reshape(rows, 2 * dim)[:, dim:].contiguous().view(-1)
                gate_before = gate.clone()
                up_before = up.clone()
                scratch = torch.empty((n,), device="cuda")
                gold = output()
                packed = output()
                separate = output()
                gold_separate = output()
                torch.cuda.synchronize()
                gpu.launch(
                    "termite_activation_multiply_fused_gate_up_f32",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(scratch),
                    pointer(joined),
                    rows,
                    dim,
                    2,
                )
                gpu.launch(
                    "termite_f32_to_bf16",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(gold),
                    pointer(scratch),
                    n,
                )
                gpu.launch(
                    "lighton_packed_silu_up_bf16",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(packed),
                    pointer(joined),
                    rows,
                    dim,
                )
                gpu.launch(
                    "termite_silu_multiply_f32",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(scratch),
                    pointer(gate),
                    pointer(up),
                    n,
                )
                gpu.launch(
                    "termite_f32_to_bf16",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(gold_separate),
                    pointer(scratch),
                    n,
                )
                gpu.launch(
                    "lighton_silu_up_bf16",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(separate),
                    pointer(gate),
                    pointer(up),
                    n,
                )
                gpu.call("cuCtxSynchronize")
                assert torch.equal(packed, gold), (
                    "packed",
                    dim,
                    rows,
                    int((packed != gold).sum()),
                )
                assert torch.equal(separate, gold_separate), (
                    "separate",
                    dim,
                    rows,
                    int((separate != gold_separate).sum()),
                )
                assert torch.equal(gold, gold_separate)
                for x, y in [(joined, original), (gate, gate_before), (up, up_before)]:
                    assert torch.equal(x.view(torch.int32), y.view(torch.int32))
                assert torch.all(backing[-16:] == 4321.0)
                for t in guards:
                    assert torch.all(t[-16:] == 4321)
                # The compact BF16 profile has a separate reference: original
                # F32 SiLU/multiply applied to exactly widened BF16 operands.
                bf_gate = torch.full(
                    (n + 16,), 8.0, device="cuda", dtype=torch.bfloat16
                )
                bf_up = torch.full_like(bf_gate, 8.0)
                bf_gate[:n] = gate.to(torch.bfloat16)
                bf_up[:n] = up.to(torch.bfloat16)
                bf_gate_before, bf_up_before = bf_gate.clone(), bf_up.clone()
                widened_gate, widened_up = bf_gate[:n].float(), bf_up[:n].float()
                compact, compact_gold = output(), output()
                residual_backing = torch.full((n + 16,), 4321.0, device="cuda")
                residual = residual_backing[:n]
                residual_gold = widened_gate + up
                torch.cuda.synchronize()
                gpu.launch(
                    "termite_silu_multiply_f32",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(scratch),
                    pointer(widened_gate),
                    pointer(widened_up),
                    n,
                )
                gpu.launch(
                    "termite_f32_to_bf16",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(compact_gold),
                    pointer(scratch),
                    n,
                )
                gpu.launch(
                    "lighton_silu_up_bf16_inputs",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(compact),
                    pointer(bf_gate),
                    pointer(bf_up),
                    n,
                )
                gpu.launch(
                    "lighton_bf16_residual_f32",
                    ((n + 255) // 256, 1),
                    256,
                    pointer(residual),
                    pointer(bf_gate),
                    pointer(up),
                    n,
                )
                gpu.call("cuCtxSynchronize")
                assert torch.equal(compact, compact_gold), ("compact_bf16", dim, rows)
                assert torch.equal(
                    residual.view(torch.int32), residual_gold.view(torch.int32)
                )
                assert torch.equal(
                    bf_gate.view(torch.int16), bf_gate_before.view(torch.int16)
                )
                assert torch.equal(
                    bf_up.view(torch.int16), bf_up_before.view(torch.int16)
                )
                assert torch.equal(up.view(torch.int32), up_before.view(torch.int32))
                assert torch.all(bf_gate[-16:] == 8.0) and torch.all(bf_up[-16:] == 8.0)
                assert torch.all(residual_backing[-16:] == 4321.0)
                for t in guards:
                    assert torch.all(t[-16:] == 4321)
                row = dict(
                    dim=dim,
                    rows=rows,
                    compact_bf16_bitwise=True,
                    bf16_residual_bitwise=True,
                    packed_bitwise=True,
                    separate_bitwise=True,
                    readonly=True,
                    canaries=True,
                )
                records.append(row)
                print(json.dumps(row), flush=True)
        for dim in [1024, 2560]:
            for rows in [1, 7, 32, 64, 129, 2316]:
                for offset in [0.0, 1.0]:
                    n = rows * dim
                    values = torch.randn((n + 16,), device="cuda")
                    values[-16:] = 4321.0
                    source = values[:n]
                    source_before = source.clone()
                    weight = torch.randn((dim + 16,), device="cuda") * 0.1
                    weight[-16:] = 4321.0
                    weight[0] = -0.0
                    raw = weight[:dim]
                    raw_before = raw.clone()
                    adjusted = torch.empty_like(raw)
                    norm = torch.empty_like(source)
                    gold_backing = torch.full(
                        (n + 16,), 4321, device="cuda", dtype=torch.uint16
                    )
                    got_backing = torch.full_like(gold_backing, 4321)
                    gold, got = gold_backing[:n], got_backing[:n]
                    torch.cuda.synchronize()
                    if offset:
                        gpu.launch(
                            "termite_add_scalar_f32",
                            ((dim + 255) // 256, 1),
                            256,
                            pointer(adjusted),
                            pointer(raw),
                            dim,
                            offset,
                        )
                    else:
                        adjusted = raw
                    gpu.launch(
                        "termite_rms_norm_f32",
                        (rows, 1),
                        256,
                        pointer(norm),
                        pointer(source),
                        pointer(adjusted),
                        rows,
                        dim,
                        1e-6,
                    )
                    gpu.launch(
                        "termite_f32_to_bf16",
                        ((n + 255) // 256, 1),
                        256,
                        pointer(gold),
                        pointer(norm),
                        n,
                    )
                    gpu.launch(
                        "lighton_rms_input_bf16",
                        (rows, 1),
                        256,
                        pointer(got),
                        pointer(source),
                        pointer(raw),
                        rows,
                        dim,
                        1e-6,
                        offset,
                    )
                    gpu.call("cuCtxSynchronize")
                    assert torch.equal(gold, got), (
                        "rms",
                        dim,
                        rows,
                        offset,
                        int((gold != got).sum()),
                    )
                    assert torch.equal(
                        source.view(torch.int32), source_before.view(torch.int32)
                    )
                    assert torch.equal(
                        raw.view(torch.int32), raw_before.view(torch.int32)
                    )
                    assert torch.all(values[-16:] == 4321.0) and torch.all(
                        weight[-16:] == 4321.0
                    )
                    assert torch.all(gold_backing[-16:] == 4321) and torch.all(
                        got_backing[-16:] == 4321
                    )
                    row = dict(
                        kind="rms_bf16_input",
                        dim=dim,
                        rows=rows,
                        offset=offset,
                        bitwise=True,
                        readonly=True,
                        canaries=True,
                    )
                    records.append(row)
                    print(json.dumps(row), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=records), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
