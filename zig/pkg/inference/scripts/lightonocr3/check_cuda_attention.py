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

"""Compare the opt-in batched attention PTX with a CPU PyTorch oracle.

Requires PyTorch for F16 rounding and the independent CPU attention reference.
Checks page permutations, unequal contexts, causal masking and output canaries.
"""

import argparse
import ctypes as c
import json
import math
from pathlib import Path
import torch
from check_cuda_recurrent import Driver


class Page(c.Structure):
    _fields_ = [
        ("tokens", c.c_uint32),
        ("query_position", c.c_uint32),
        ("block_count", c.c_uint32),
        ("block_offset", c.c_uint32),
    ]


class Params(c.Structure):
    _fields_ = (
        [
            (name, c.c_uint64)
            for name in [
                "q",
                "k",
                "v",
                "out",
                "partials",
                "statistics",
                "new_k",
                "new_v",
            ]
        ]
        + [
            (name, c.c_uint32)
            for name in [
                "items",
                "heads",
                "kv_heads",
                "chunks",
                "page_size",
                "row_values",
                "capacity",
            ]
        ]
        + [("pages", Page * 64), ("blocks", c.c_uint32 * 4096)]
        + [
            ("graph_progress", c.c_uint64),
            ("graph_step", c.c_uint32),
            ("reserved", c.c_uint32),
        ]
    )


assert c.sizeof(Page) == 16 and c.sizeof(Params) == 17520


def launch(driver, name, grid, p):
    f = c.c_void_p()
    driver.call(
        "cuModuleGetFunction", c.byref(f), driver.module, c.c_char_p(name.encode())
    )
    args = (c.c_void_p * 1)(c.addressof(p))
    driver.call(
        "cuLaunchKernel",
        f,
        c.c_uint(grid[0]),
        c.c_uint(grid[1]),
        c.c_uint(1),
        c.c_uint(128),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(0),
        c.c_void_p(),
        args,
        c.c_void_p(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chunk128", action="store_true")
    parser.add_argument("--grouped", action="store_true")
    args = parser.parse_args()
    chunk_name = (
        (
            "lighton_attention_grouped_chunks128"
            if args.chunk128
            else "lighton_attention_grouped_chunks32"
        )
        if args.grouped
        else (
            "lighton_attention_chunks128"
            if args.chunk128
            else "lighton_attention_chunks"
        )
    )
    torch.manual_seed(371)
    torch.set_num_threads(4)
    d = Driver(args.artifact)
    results = []
    try:
        for h, kh in [(8, 2), (16, 4)]:
            for b in [1, 2, 4, 16, 64]:
                lengths = (
                    [
                        1,
                        17,
                        31,
                        32,
                        33,
                        65,
                        127,
                        128,
                        129,
                        256,
                        432,
                        554,
                        662,
                        864,
                        1000,
                        1024,
                        1914,
                        2697,
                        4096,
                        8192,
                    ]
                    * 4
                )[:b]
                if b == 1:
                    lengths = [432]
                elif b in (2, 4):
                    lengths = [1914, 2697, 4096, 8192][:b]
                size = 16 if b < 16 else 64
                blocks = sum(math.ceil(n / size) for n in lengths)
                capacity = (2 * blocks + 2) * size
                k = torch.randn(capacity, kh, 256).half()
                v = torch.randn_like(k)
                k.reshape(-1, size, kh, 256)[::2].fill_(float("nan"))
                v.reshape(-1, size, kh, 256)[::2].fill_(float("nan"))
                q = torch.randn(b, h, 256)
                qg, kg, vg = q.cuda(), k.cuda(), v.cuda()
                chunks = math.ceil(max(lengths) / (128 if args.chunk128 else 32))

                def guarded(n):
                    return torch.full((n + 16,), 123456.0, device="cuda")

                out = guarded(b * h * 256)
                partial = guarded(b * h * chunks * 256)
                stats = guarded(b * h * chunks * 2)
                p = Params(
                    q=qg.data_ptr(),
                    k=kg.data_ptr(),
                    v=vg.data_ptr(),
                    out=out.data_ptr(),
                    partials=partial.data_ptr(),
                    statistics=stats.data_ptr(),
                    items=b,
                    heads=h,
                    kv_heads=kh,
                    chunks=chunks,
                    page_size=size,
                    row_values=kh * 256,
                    capacity=capacity,
                )
                order = torch.randperm(blocks).tolist()
                cursor = 0
                expected = []
                for i, n in enumerate(lengths):
                    count = math.ceil(n / size)
                    ids = [2 * x + 1 for x in order[cursor : cursor + count]]
                    offset = cursor
                    cursor += count
                    assert cursor <= 4096
                    query = n - 1 if i % 3 else max(0, n - 7)
                    p.pages[i] = Page(n, query, count, offset)
                    p.blocks[offset : offset + count] = ids
                    rows = torch.cat(
                        [torch.arange(x * size, (x + 1) * size) for x in ids]
                    )[:n]
                    kk = k[rows].float().repeat_interleave(h // kh, dim=1)
                    vv = v[rows].float().repeat_interleave(h // kh, dim=1)
                    scores = (kk * q[i]).sum(-1) / 16
                    scores[query + 1 :] = float("-inf")
                    expected.append(
                        (scores.softmax(dim=0).unsqueeze(-1) * vv).sum(dim=0)
                    )
                expected = torch.stack(expected)
                torch.cuda.synchronize()
                launch(
                    d,
                    chunk_name,
                    (kh if args.grouped else h, b * chunks),
                    p,
                )
                launch(d, "lighton_attention_combine", (b * h, 1), p)
                d.call("cuCtxSynchronize")
                actual = out[: b * h * 256].cpu().reshape(b, h, 256)
                error = float((actual - expected).abs().max())
                assert actual.isfinite().all() and error < 2e-5, (b, h, error)
                for value in [out, partial, stats]:
                    assert bool((value[-16:] == 123456).all()), "output guard changed"
                torch.testing.assert_close(qg.cpu(), q, rtol=0, atol=0)
                torch.testing.assert_close(kg.cpu(), k, rtol=0, atol=0, equal_nan=True)
                torch.testing.assert_close(vg.cpu(), v, rtol=0, atol=0, equal_nan=True)
                # Change only each page's writable suffix through the batch
                # kernel, then repeat the independent attention oracle. Whole
                # KV comparisons protect all prefixes, unused slots and holes.
                new_k = torch.randn(b, kh, 256)
                new_v = torch.randn_like(new_k)
                new_k.flatten()[0] = 1.00048828125  # half-way RN-even
                new_v.flatten()[0] = -1.00048828125
                new_kg, new_vg = new_k.cuda(), new_v.cuda()
                p.new_k, p.new_v = new_kg.data_ptr(), new_vg.data_ptr()
                for i, n in enumerate(lengths):
                    page = p.pages[i]
                    physical = (
                        p.blocks[page.block_offset + (n - 1) // size] * size
                        + (n - 1) % size
                    )
                    k[physical] = new_k[i].half()
                    v[physical] = new_v[i].half()
                launch(
                    d,
                    "lighton_attention_write_suffix",
                    (math.ceil(kh * 256 / 128), b),
                    p,
                )
                launch(
                    d,
                    chunk_name,
                    (kh if args.grouped else h, b * chunks),
                    p,
                )
                launch(d, "lighton_attention_combine", (b * h, 1), p)
                d.call("cuCtxSynchronize")
                expected = []
                for i, n in enumerate(lengths):
                    page = p.pages[i]
                    rows = torch.cat(
                        [
                            torch.arange(x * size, (x + 1) * size)
                            for x in p.blocks[
                                page.block_offset : page.block_offset + page.block_count
                            ]
                        ]
                    )[:n]
                    kk = k[rows].float().repeat_interleave(h // kh, dim=1)
                    vv = v[rows].float().repeat_interleave(h // kh, dim=1)
                    scores = (kk * q[i]).sum(-1) / 16
                    scores[page.query_position + 1 :] = float("-inf")
                    expected.append(
                        (scores.softmax(dim=0).unsqueeze(-1) * vv).sum(dim=0)
                    )
                actual = out[: b * h * 256].cpu().reshape(b, h, 256)
                suffix_error = float((actual - torch.stack(expected)).abs().max())
                assert actual.isfinite().all() and suffix_error < 2e-5
                for value in [out, partial, stats]:
                    assert bool((value[-16:] == 123456).all()), "suffix guard changed"
                torch.testing.assert_close(kg.cpu(), k, rtol=0, atol=0, equal_nan=True)
                torch.testing.assert_close(vg.cpu(), v, rtol=0, atol=0, equal_nan=True)
                torch.testing.assert_close(qg.cpu(), q, rtol=0, atol=0)
                torch.testing.assert_close(new_kg.cpu(), new_k, rtol=0, atol=0)
                torch.testing.assert_close(new_vg.cpu(), new_v, rtol=0, atol=0)
                results.append(
                    dict(
                        items=b,
                        heads=h,
                        kv_heads=kh,
                        page_size=size,
                        lengths=lengths,
                        max_abs=error,
                        guards=True,
                        readonly_inputs=True,
                        batched_suffix_write_max_abs=suffix_error,
                        kv_prefixes_and_holes_preserved=True,
                    )
                )
                print(json.dumps(results[-1]), flush=True)
                del qg, kg, vg, out, partial, stats, new_kg, new_vg
    finally:
        d.close()
    args.output.write_text(json.dumps(dict(status="passed", cases=results), indent=2))


if __name__ == "__main__":
    main()
