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

"""Qualify two-token decoder-graph attention against an independent CPU oracle.

Checks repeated replay and reset across page boundaries, complete KV contents,
unused slots, exact GPU positions/progress, read-only inputs and output canaries.
The F32 tolerance remains 2e-5; this does not qualify full-model graph ownership.
"""

import argparse
import hashlib
import ctypes as c
import json
import math
from pathlib import Path
import torch
import check_cuda_attention as oracle
from check_cuda_recurrent import Driver

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--artifact", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
cli = parser.parse_args()
Params = oracle.Params
assert c.sizeof(Params) == 17264
D = Driver(cli.artifact)
results = []
functions = {}
for name in [
    "lighton_attention_write_suffix",
    "lighton_attention_chunks",
    "lighton_attention_chunks128",
    "lighton_attention_grouped_chunks32",
    "lighton_attention_grouped_chunks128",
    "lighton_attention_combine",
    "lighton_graph_advance",
]:
    f = c.c_void_p()
    D.call("cuModuleGetFunction", c.byref(f), D.module, c.c_char_p(name.encode()))
    functions[name] = f


def launch(name, grid, args, stream, threads=128):
    D.call(
        "cuLaunchKernel",
        functions[name],
        c.c_uint(grid[0]),
        c.c_uint(grid[1]),
        c.c_uint(1),
        c.c_uint(threads),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(0),
        c.c_void_p(stream.cuda_stream),
        args,
        c.c_void_p(),
    )


graph = None
try:
    torch.set_num_threads(4)
    for heads, kh in [(8, 2), (16, 4)]:
        for batch in [2, 16, 64]:
            for chunk in [32, 128]:
                torch.manual_seed(152)
                page_size = 16
                lengths = ([31, 32, 63, 127, 255, 432, 554, 662] * 8)[:batch]
                replays = 5
                counts = [math.ceil((n + replays * 2) / page_size) for n in lengths]
                blocks = sum(counts)
                capacity = (2 * blocks + 2) * page_size
                q = torch.randn(batch, heads, 256)
                k = torch.randn(capacity, kh, 256).half()
                v = torch.randn_like(k)
                k.reshape(-1, page_size, kh, 256)[::2].fill_(float("nan"))
                v.reshape(-1, page_size, kh, 256)[::2].fill_(float("nan"))
                new_k, new_v = torch.randn(batch, kh, 256), torch.randn(batch, kh, 256)
                new_k.flatten()[0], new_v.flatten()[0] = 1.00048828125, -1.00048828125
                qg, kg, vg = q.cuda(), k.cuda(), v.cuda()
                nk, nv = new_k.cuda(), new_v.cuda()
                progress = torch.zeros(1, dtype=torch.int32, device="cuda")
                base_positions = torch.arange(3 * batch, dtype=torch.int32) + 400
                positions = base_positions.cuda()
                chunks = math.ceil(max(n + replays * 2 for n in lengths) / chunk)
                grouped = (heads == 8 and chunk == 32 and batch >= 8) or (
                    heads == 16 and chunk == 128 and batch >= 16
                )
                chunk_name = (
                    (
                        "lighton_attention_grouped_chunks128"
                        if chunk == 128
                        else "lighton_attention_grouped_chunks32"
                    )
                    if grouped
                    else (
                        "lighton_attention_chunks128"
                        if chunk == 128
                        else "lighton_attention_chunks"
                    )
                )
                packets, outputs, partials, statistics = [], [], [], []
                order = torch.randperm(blocks).tolist()
                tables = []
                cursor = 0
                for count in counts:
                    tables.append([2 * x + 1 for x in order[cursor : cursor + count]])
                    cursor += count
                for step in range(2):
                    output = torch.full(
                        (batch * heads * 256 + 16,), 123456.0, device="cuda"
                    )
                    partial = torch.full(
                        (batch * heads * chunks * 256 + 16,), 123456.0, device="cuda"
                    )
                    stats = torch.full(
                        (batch * heads * chunks * 2 + 16,), 123456.0, device="cuda"
                    )
                    p = Params(
                        q=qg.data_ptr(),
                        k=kg.data_ptr(),
                        v=vg.data_ptr(),
                        out=output.data_ptr(),
                        partials=partial.data_ptr(),
                        statistics=stats.data_ptr(),
                        new_k=nk.data_ptr(),
                        new_v=nv.data_ptr(),
                        items=batch,
                        heads=heads,
                        kv_heads=kh,
                        chunks=chunks,
                        page_size=page_size,
                        row_values=kh * 256,
                        capacity=capacity,
                        graph_progress=progress.data_ptr(),
                        graph_step=step,
                    )
                    for i, n in enumerate(lengths):
                        p.pages[i] = oracle.Page(
                            n + step,
                            n + step - 1,
                            counts[i],
                            (c.c_uint32 * 64)(*tables[i]),
                        )
                    packets.append(p)
                    outputs.append(output)
                    partials.append(partial)
                    statistics.append(stats)
                torch.cuda.synchronize()
                stream = torch.cuda.Stream()
                graph = torch.cuda.CUDAGraph()
                with torch.cuda.graph(graph, stream=stream):
                    for p in packets:
                        args = (c.c_void_p * 1)(c.addressof(p))
                        launch(
                            "lighton_attention_write_suffix",
                            (math.ceil(kh * 256 / 128), batch),
                            args,
                            stream,
                        )
                        launch(
                            chunk_name,
                            (kh if grouped else heads, batch * chunks),
                            args,
                            stream,
                        )
                        launch(
                            "lighton_attention_combine",
                            (batch * heads, 1),
                            args,
                            stream,
                        )
                        pos, counter, count = (
                            c.c_uint64(positions.data_ptr()),
                            c.c_uint64(progress.data_ptr()),
                            c.c_uint32(3 * batch),
                        )
                        advance_args = (c.c_void_p * 3)(
                            c.addressof(pos), c.addressof(counter), c.addressof(count)
                        )
                        launch(
                            "lighton_graph_advance", (1, 1), advance_args, stream, 256
                        )
                max_error = 0.0
                for index in range(replays + 1):
                    replay = index
                    if index == replays:
                        # Reuse the captured executable after resetting its
                        # GPU-owned positions and progress. Future KV slots
                        # remain populated and must stay causally invisible.
                        progress.zero_()
                        positions.copy_(base_positions.to(device="cuda"))
                        replay = 0
                    graph.replay()
                    torch.cuda.synchronize()
                    for step in range(2):
                        expected = []
                        for i, n in enumerate(lengths):
                            tokens = n + replay * 2 + step
                            physical = (
                                tables[i][(tokens - 1) // page_size] * page_size
                                + (tokens - 1) % page_size
                            )
                            k[physical], v[physical] = new_k[i].half(), new_v[i].half()
                            rows = torch.cat(
                                [
                                    torch.arange(b * page_size, (b + 1) * page_size)
                                    for b in tables[i]
                                ]
                            )[:tokens]
                            kk = k[rows].float().repeat_interleave(heads // kh, dim=1)
                            vv = v[rows].float().repeat_interleave(heads // kh, dim=1)
                            scores = (kk * q[i]).sum(-1) / 16
                            expected.append(
                                (scores.softmax(dim=0).unsqueeze(-1) * vv).sum(0)
                            )
                        actual = outputs[step][:-16].cpu().reshape(batch, heads, 256)
                        error = float((actual - torch.stack(expected)).abs().max())
                        assert actual.isfinite().all() and error < 2e-5, (
                            heads,
                            batch,
                            chunk,
                            replay,
                            step,
                            error,
                        )
                        max_error = max(max_error, error)
                    assert progress.item() == (replay + 1) * 2
                    torch.testing.assert_close(
                        positions.cpu(),
                        base_positions + (replay + 1) * 2,
                        rtol=0,
                        atol=0,
                    )
                    torch.testing.assert_close(
                        kg.cpu(), k, rtol=0, atol=0, equal_nan=True
                    )
                    torch.testing.assert_close(
                        vg.cpu(), v, rtol=0, atol=0, equal_nan=True
                    )
                    torch.testing.assert_close(qg.cpu(), q, rtol=0, atol=0)
                    torch.testing.assert_close(nk.cpu(), new_k, rtol=0, atol=0)
                    torch.testing.assert_close(nv.cpu(), new_v, rtol=0, atol=0)
                    for tensor in outputs + partials + statistics:
                        assert bool((tensor[-16:] == 123456.0).all())
                graph.reset()
                graph = None
                result = dict(
                    heads=heads,
                    items=batch,
                    grouped=grouped,
                    chunk=chunk,
                    replayed_steps=(replays + 1) * 2,
                    resets=1,
                    max_abs=max_error,
                    full_kv_and_holes=True,
                    positions_and_counter=True,
                    canaries=True,
                )
                results.append(result)
                print(json.dumps(result), flush=True)
finally:
    torch.cuda.synchronize()
    if graph is not None:
        graph.reset()
    D.close()
cli.output.parent.mkdir(parents=True, exist_ok=True)
cli.output.write_text(
    json.dumps(
        dict(
            status="passed",
            artifact_sha256=hashlib.sha256(cli.artifact.read_bytes()).hexdigest(),
            cases=results,
        ),
        indent=2,
    )
    + "\n"
)
