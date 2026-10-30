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

"""Check exact F32 KV copies, including unused pages, special bits and guards."""

import argparse
import json
from pathlib import Path

import torch

from check_cuda_recurrent import Driver
from check_cuda_recurrent_batch import pointer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.manual_seed(948)
    torch.set_num_threads(4)
    torch.cuda.init()
    gpu = Driver(args.artifact)
    results = []
    try:
        for batch in [1, 2, 16, 32, 64]:
            capacity = 64
            dim = 768
            # Physical suffix pages model a smaller active cohort after EOS.
            physical = batch + 2
            for old in [0, 1, 17, 63]:
                guarded = []

                def buffer(n):
                    bits = torch.randint(
                        -(2**31), 2**31 - 1, (n + 16,), device="cuda", dtype=torch.int32
                    )
                    bits[-16:] = 0x456789AB
                    guarded.append(bits)
                    return bits[:n].view(torch.float32)

                slabs = [buffer(physical * capacity * dim) for _ in range(2)]
                rows = [buffer(batch * dim) for _ in range(2)]
                # Signed zeros, infinities and NaN payloads must survive copies.
                for row in rows:
                    row.view(torch.int32)[:8] = torch.tensor(
                        [
                            0,
                            -2147483648,
                            0x7F800000,
                            -8388608,
                            0x7FC12345,
                            -4119739,
                            0x3F800000,
                            -1082130432,
                        ],
                        device="cuda",
                        dtype=torch.int32,
                    )
                before = [t.clone() for t in slabs]
                saved_rows = [t.clone() for t in rows]
                outputs = [buffer(batch * (old + 1) * dim) for _ in range(2)]
                expected = []
                expected_slabs = []
                for slab, row in zip(before, rows):
                    s = slab.view(torch.int32).reshape(physical, capacity, dim).clone()
                    s[:batch, old, :] = row.view(torch.int32).reshape(batch, dim)
                    expected.append(s[:batch, : old + 1, :].contiguous().flatten())
                    expected_slabs.append(s.flatten())
                torch.cuda.synchronize()
                count = batch * (old + 1) * dim
                gpu.launch(
                    "florence_append_self_kv_f32",
                    ((count + 255) // 256, 1),
                    256,
                    *map(pointer, (*outputs, *slabs, *rows)),
                    count,
                    capacity,
                    old,
                    dim,
                )
                gpu.call("cuCtxSynchronize")
                for actual, wanted in zip(outputs, expected):
                    assert torch.equal(actual.view(torch.int32), wanted), (
                        "output",
                        batch,
                        old,
                    )
                for actual, wanted in zip(slabs, expected_slabs):
                    assert torch.equal(actual.view(torch.int32), wanted), (
                        "slab/suffix",
                        batch,
                        old,
                    )
                for actual, saved in zip(rows, saved_rows):
                    assert torch.equal(
                        actual.view(torch.int32), saved.view(torch.int32)
                    ), "read-only row changed"
                for bits in guarded:
                    assert torch.all(bits[-16:] == 0x456789AB), "guard overwritten"
                result = dict(
                    batch=batch,
                    physical_pages=physical,
                    capacity=capacity,
                    old_length=old,
                    bitwise=True,
                )
                results.append(result)
                print(json.dumps(result), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=results), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
