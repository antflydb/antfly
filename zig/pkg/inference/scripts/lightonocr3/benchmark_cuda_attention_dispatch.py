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

"""Compare existing grouped and per-head attention on synthetic reader geometry.

Requires an otherwise idle GPU 0. CUDA graph replay and events share one
explicit stream; each trial executes 100 complete attention/combine operations.
Five warm device-time trials exclude Python dispatch and CPU oracle work.
Every path is checked against an independent FP64 oracle, output/scratch
canaries and read-only inputs. This is not a full-model throughput result.
"""

import argparse
import ctypes as c
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess

import torch
from check_cuda_attention import Params, Page
from check_cuda_recurrent import Driver

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--artifact", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
cli = parser.parse_args()
if subprocess.check_output(
    ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
).strip():
    raise RuntimeError("GPU is not isolated")

torch.set_num_threads(4)
torch.manual_seed(42)
artifact = cli.artifact
driver = Driver(artifact)
functions = {}
for name in [
    "lighton_attention_chunks",
    "lighton_attention_chunks128",
    "lighton_attention_grouped_chunks32",
    "lighton_attention_grouped_chunks128",
    "lighton_attention_combine",
]:
    f = c.c_void_p()
    driver.call(
        "cuModuleGetFunction", c.byref(f), driver.module, c.c_char_p(name.encode())
    )
    functions[name] = f
stream = torch.cuda.Stream()
results = []


def launch(name, grid, args):
    driver.call(
        "cuLaunchKernel",
        functions[name],
        c.c_uint(grid[0]),
        c.c_uint(grid[1]),
        c.c_uint(1),
        c.c_uint(128),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(0),
        c.c_void_p(stream.cuda_stream),
        args,
        c.c_void_p(),
    )


try:
    for heads, kh, chunk in [(8, 2, 32), (16, 4, 128)]:
        for rows in [1, 2, 4, 8, 16, 32]:
            for context in [256, 1914, 2697, 8192]:
                pages = math.ceil(context / 16)
                if rows * pages > 4096:
                    continue
                future = 3962 if context in [1914, 2697] else context
                chunks = math.ceil(future / chunk)
                capacity = rows * pages * 16
                q = torch.randn(rows, heads, 256)
                k = torch.randn(capacity, kh, 256).half()
                v = torch.randn_like(k)
                qg, kg, vg = q.cuda(), k.cuda(), v.cuda()
                output = torch.full((rows * heads * 256 + 16,), 123456.0, device="cuda")
                partial = torch.full(
                    (rows * heads * chunks * 256 + 16,), 123456.0, device="cuda"
                )
                stats = torch.full(
                    (rows * heads * chunks * 2 + 16,), 123456.0, device="cuda"
                )
                p = Params(
                    q=qg.data_ptr(),
                    k=kg.data_ptr(),
                    v=vg.data_ptr(),
                    out=output.data_ptr(),
                    partials=partial.data_ptr(),
                    statistics=stats.data_ptr(),
                    items=rows,
                    heads=heads,
                    kv_heads=kh,
                    chunks=chunks,
                    page_size=16,
                    row_values=kh * 256,
                    capacity=capacity,
                )
                for row in range(rows):
                    p.pages[row] = Page(context, context - 1, pages, row * pages)
                    p.blocks[row * pages : (row + 1) * pages] = range(
                        row * pages, (row + 1) * pages
                    )
                args = (c.c_void_p * 1)(c.addressof(p))
                measurements = {}
                outputs = {}
                for grouped in [False, True]:
                    selected = (
                        ("lighton_attention_grouped_chunks" + str(chunk))
                        if grouped
                        else (
                            "lighton_attention_chunks128"
                            if chunk == 128
                            else "lighton_attention_chunks"
                        )
                    )

                    def operation():
                        launch(
                            selected, (kh if grouped else heads, rows * chunks), args
                        )
                        launch("lighton_attention_combine", (rows * heads, 1), args)

                    for _ in range(5):
                        operation()
                    torch.cuda.synchronize()
                    graph = torch.cuda.CUDAGraph()
                    with torch.cuda.graph(graph, stream=stream):
                        for _ in range(100):
                            operation()
                    times = []
                    with torch.cuda.stream(stream):
                        assert (
                            torch.cuda.current_stream().cuda_stream
                            == stream.cuda_stream
                        )
                        for _ in range(5):
                            start, end = (
                                torch.cuda.Event(enable_timing=True),
                                torch.cuda.Event(enable_timing=True),
                            )
                            start.record()
                            graph.replay()
                            end.record()
                            end.synchronize()
                            times.append(start.elapsed_time(end) * 10)
                    assert all(value > 0 for value in times), (
                        "unmeasurable device interval"
                    )
                    graph.reset()
                    measurements["grouped" if grouped else "ungrouped"] = (
                        statistics.median(times)
                    )
                    outputs[grouped] = (
                        output[:-16].cpu().clone().reshape(rows, kh, heads // kh, 256)
                    )
                    for tensor in [output, partial, stats]:
                        assert bool((tensor[-16:] == 123456.0).all())
                maximum = 0.0
                for row in range(rows):
                    keys = k[row * pages * 16 : row * pages * 16 + context].double()
                    values = v[row * pages * 16 : row * pages * 16 + context].double()
                    query = q[row].double().reshape(kh, heads // kh, 256)
                    weights = (
                        torch.einsum("tkd,khd->tkh", keys, query).div(16).softmax(dim=0)
                    )
                    expected = torch.einsum("tkh,tkd->khd", weights, values)
                    for actual in outputs.values():
                        maximum = max(
                            maximum,
                            float((actual[row].double() - expected).abs().max()),
                        )
                assert maximum < 2e-5, maximum
                assert (
                    torch.equal(qg.cpu(), q)
                    and torch.equal(kg.cpu(), k)
                    and torch.equal(vg.cpu(), v)
                )
                result = {
                    "heads": heads,
                    "kv_heads": kh,
                    "rows": rows,
                    "context": context,
                    "chunks": chunks,
                    "chunk": chunk,
                    "max_abs": maximum,
                    "canaries": True,
                    "readonly": True,
                    "microseconds": measurements,
                    "grouped_over_ungrouped": measurements["grouped"]
                    / measurements["ungrouped"],
                }
                results.append(result)
                print(json.dumps(result), flush=True)
finally:
    driver.close()
report = {
    "gpu": torch.cuda.get_device_name(0),
    "sms": torch.cuda.get_device_properties(0).multi_processor_count,
    "artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
    "scope": "Synthetic attention+combine device time, graph replay, CPU FP64 oracle and output canaries; no full-model throughput claim",
    "torch_version": torch.__version__,
    "seed": 42,
    "operations_per_graph": 100,
    "trials": 5,
    "cases": results,
}
cli.output.parent.mkdir(parents=True, exist_ok=True)
cli.output.write_text(json.dumps(report, indent=2) + "\n")
