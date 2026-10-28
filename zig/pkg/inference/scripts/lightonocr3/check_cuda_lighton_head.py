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

"""Check serving SM121 head PTX against independent IEEE F32 PyTorch logits.

Weights and inputs are BF16-rounded exactly as in native projection. This tests
winning-score error and exact suppressed top1, not just a repeated kernel oracle.
The caller supplies an existing local original 0.8B checkpoint; no model downloads.
"""

import argparse
import ctypes as c
import hashlib
import json
from pathlib import Path

import torch
from safetensors import safe_open

from check_cuda_recurrent import Driver


def fingerprint(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def launch(driver, name, pointers, scalars, grid, threads, shared):
    fn = c.c_void_p()
    driver.call(
        "cuModuleGetFunction", c.byref(fn), driver.module, c.c_char_p(name.encode())
    )
    values = [c.c_uint64(t.data_ptr()) for t in pointers]
    values += [c.c_uint32(v) for v in scalars]
    # Both sizes are zero in the pinned generator. These ABI pointers are null.
    values += [c.c_uint64(0), c.c_uint64(0)]
    arguments = (c.c_void_p * len(values))(*[c.addressof(v) for v in values])
    driver.call(
        "cuLaunchKernel",
        fn,
        c.c_uint(grid),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(threads),
        c.c_uint(1),
        c.c_uint(1),
        c.c_uint(shared),
        c.c_void_p(),
        arguments,
        c.c_void_p(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--partial-ptx", type=Path, required=True)
    parser.add_argument("--finish-ptx", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, nargs="+", default=[1, 7, 21, 64, 85, 128])
    args = parser.parse_args()
    if args.output.exists() or any(n < 1 or n > 128 for n in args.rows):
        parser.error("output must be new and every row count must be in 1..128")
    torch.set_num_threads(4)
    torch.set_float32_matmul_precision("highest")
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.cuda.init()
    assert torch.cuda.get_device_capability() == (12, 1)
    assert torch.cuda.current_stream().cuda_stream == 0
    config = json.loads((args.model_dir / "config.json").read_text())
    tied = config.get("tie_word_embeddings") or config.get("text_config", {}).get(
        "tie_word_embeddings"
    )
    candidates = {}
    for path in sorted(args.model_dir.glob("*.safetensors")):
        with safe_open(str(path), framework="pt", device="cpu") as source:
            for key in source.keys():
                if key.endswith("lm_head.weight") or (
                    tied and key.endswith("embed_tokens.weight")
                ):
                    candidates[key] = (path, source.get_tensor(key))
    keys = [key for key in candidates if key.endswith("lm_head.weight")]
    if not keys:
        keys = [key for key in candidates if key.endswith("embed_tokens.weight")]
    assert len(keys) == 1, keys
    key = keys[0]
    path, host = candidates[key]
    assert tuple(host.shape) == (248320, 1024) and host.dtype == torch.bfloat16
    weight = host.cuda().contiguous()
    unchanged_weight = weight.clone()
    # FP32 operands prevent the reference from reusing the native BF16 GEMM.
    reference_weight = weight.float()
    partial_driver, finish_driver = Driver(args.partial_ptx), Driver(args.finish_ptx)
    records = []
    for rows in args.rows:
        torch.manual_seed(714 + rows)
        x = (torch.randn(rows * 1024 + 1, device="cuda") * 0.3)[1:].reshape(rows, 1024)
        original = x.clone()
        expected_logits = x.bfloat16().float() @ reference_weight.T
        for suppressed in [[], [0, 2, 17, 248319, -1, 300000]]:
            suppress = torch.tensor(suppressed, device="cuda", dtype=torch.int32)
            values = torch.full((rows * 970 + 32,), 123456.0, device="cuda")
            ids = torch.full(
                (rows * 970 + 32,), 123456, device="cuda", dtype=torch.int32
            )
            bank = torch.full((rows + 33,), 123456, device="cuda", dtype=torch.int32)
            partial = values[16:-16].reshape(rows, 970)
            indices = ids[16:-16].reshape(rows, 970)
            output = bank[17:-16]
            launch(
                partial_driver,
                "lighton_head_top1_partial",
                [x, weight, partial, indices, suppress],
                [rows, len(suppressed)],
                970,
                256,
                32768,
            )
            launch(
                finish_driver,
                "lighton_head_top1_finish",
                [partial, indices, output],
                [],
                rows,
                128,
                16,
            )
            logits = expected_logits.clone()
            valid = [token for token in suppressed if 0 <= token < logits.shape[1]]
            logits[:, valid] = float("-inf")
            expected = torch.argmax(logits, dim=1).to(torch.int32)
            torch.cuda.synchronize()
            error = float(
                (partial.max(dim=1).values - logits.max(dim=1).values).abs().max()
            )
            assert error <= 2e-4, (rows, suppressed, error)
            assert torch.equal(output, expected), (
                rows,
                output.tolist(),
                expected.tolist(),
            )
            assert bool(torch.all(bank[:17] == 123456)) and bool(
                torch.all(bank[-16:] == 123456)
            )
            for guarded in [values, ids]:
                assert bool(torch.all(guarded[:16] == 123456)) and bool(
                    torch.all(guarded[-16:] == 123456)
                )
            assert torch.equal(x.view(torch.int32), original.view(torch.int32))
            records.append(
                dict(
                    rows=rows,
                    suppressed=suppressed,
                    exact_tokens=True,
                    max_abs_winning_score=error,
                    output_alignment=output.data_ptr() % 16,
                    input_alignment=x.data_ptr() % 16,
                )
            )
    assert torch.equal(weight.view(torch.int16), unchanged_weight.view(torch.int16))
    report = dict(
        status="ok",
        scope="Independent IEEE F32 PyTorch logits with original BF16 operands; exact top1 and unchanged 2e-4 winning-score budget, readonly/canaries. No throughput claim.",
        gpu=torch.cuda.get_device_name(),
        torch_version=torch.__version__,
        checkpoint_file=path.name,
        checkpoint_file_sha256=fingerprint(path),
        weight_key=key,
        partial_ptx_sha256=fingerprint(args.partial_ptx),
        finish_ptx_sha256=fingerprint(args.finish_ptx),
        checker_sha256=fingerprint(Path(__file__)),
        cases=records,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Independent head oracle passed {len(records)} real-checkpoint cases")


if __name__ == "__main__":
    main()
