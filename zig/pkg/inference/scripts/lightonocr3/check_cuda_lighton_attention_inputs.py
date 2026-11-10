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

"""Check the serving F32 attention-input PTX against independent Transformers ops.

Uses real checkpoint Q/K norm weights, partial interleaved image/text rotary
positions, readonly snapshots and output canaries. This is a correctness check,
not an OCR throughput benchmark. Requires CUDA PyTorch, Transformers and safetensors.
"""

import argparse
import ctypes as c
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path

import torch
from safetensors import safe_open
from transformers.models.qwen3_5 import modeling_qwen3_5 as hf
from transformers.models.qwen3_5.configuration_qwen3_5 import Qwen3_5TextConfig


class Params(c.Structure):
    _fields_ = (
        [
            (name, c.c_uint64)
            for name in (
                "query_gate",
                "key",
                "query_weight",
                "key_weight",
                "positions",
                "query_out",
                "key_out",
                "gate_out",
            )
        ]
        + [
            (name, c.c_uint32)
            for name in ("rows", "query_heads", "key_heads", "dim", "rotary_dim")
        ]
        + [(name, c.c_float) for name in ("theta", "frequency_scale", "epsilon")]
        + [(name, c.c_uint32) for name in ("section_height", "section_width")]
    )


def checkpoint_norms(directory):
    weights = {}
    for path in sorted(directory.glob("*.safetensors")):
        with safe_open(str(path), framework="pt", device="cpu") as source:
            for key in source.keys():
                if key.endswith(
                    (".self_attn.q_norm.weight", ".self_attn.k_norm.weight")
                ):
                    weights[key] = source.get_tensor(key).float()
    pairs = []
    for key in sorted(weights):
        if key.endswith(".q_norm.weight"):
            other = key.removesuffix(".q_norm.weight") + ".k_norm.weight"
            assert weights[key].numel() == weights[other].numel() == 256
            pairs.append((key, weights[key], weights[other]))
    assert pairs, "Checkpoint has no dense attention head norm weights"
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, default=Path.cwd())
    parser.add_argument("--ptx", type=Path, required=True)
    parser.add_argument("--model-08", type=Path, required=True)
    parser.add_argument("--model-4b", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "Preserve previous qualification reports"
    assert c.sizeof(Params) == 104 and Params.rows.offset == 64
    driver_path = (
        args.runtime_root
        / "zig/pkg/inference/scripts/lightonocr3/check_cuda_recurrent.py"
    )
    spec = importlib.util.spec_from_file_location("cuda_checker", driver_path)
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    torch.cuda.init()
    driver = checker.Driver(args.ptx)
    function = c.c_void_p()
    driver.call(
        "cuModuleGetFunction",
        c.byref(function),
        driver.module,
        c.c_char_p(b"lighton_qk_norm_partial_mrope_gate"),
    )
    cases = []
    for label, directory, heads, kv in (
        ("0.8B", args.model_08, 8, 2),
        ("4B", args.model_4b, 16, 4),
    ):
        config = Qwen3_5TextConfig.from_pretrained(directory, local_files_only=True)
        assert config.head_dim == 256
        assert config.rope_parameters["partial_rotary_factor"] == 0.25
        assert config.rope_parameters["rope_theta"] == 10000000
        assert config.rope_parameters["mrope_section"] == [11, 11, 10]
        assert config.rope_parameters["mrope_interleaved"]
        rope = hf.Qwen3_5TextRotaryEmbedding(config, device="cuda")
        weights = checkpoint_norms(directory)
        covered = set()
        for rows in (1, 2, 7, 21, 64, 85, 128, 256, 662, 2316):
            for coordinates in ("text", "image"):
                torch.manual_seed(539 + rows + heads)
                raw = torch.randn(rows, heads, 512, device="cuda")
                key = torch.randn(rows, kv, 256, device="cuda")
                name, qw_host, kw_host = weights[len(cases) % len(weights)]
                covered.add(name)
                q_norm = hf.Qwen3_5RMSNorm(256, eps=1e-6).cuda()
                k_norm = hf.Qwen3_5RMSNorm(256, eps=1e-6).cuda()
                with torch.no_grad():
                    q_norm.weight.copy_(qw_host)
                    k_norm.weight.copy_(kw_host)
                qw = (1 + q_norm.weight.detach()).contiguous()
                kw = (1 + k_norm.weight.detach()).contiguous()
                positions = (
                    (torch.arange(rows, device="cuda") % 1024).repeat(3, 1)
                    if coordinates == "text"
                    else torch.randint(0, 1024, (3, rows), device="cuda")
                )
                pos = positions.to(torch.int32).contiguous()
                q_in, gate_in = raw.chunk(2, dim=-1)
                q_ref = q_norm(q_in).unsqueeze(0)
                k_ref = k_norm(key).unsqueeze(0)
                cos, sin = rope(q_ref, positions.unsqueeze(1))
                q_ref, k_ref = hf.apply_rotary_pos_emb(
                    q_ref, k_ref, cos, sin, unsqueeze_dim=2
                )
                references = (
                    q_ref.flatten(),
                    k_ref.flatten(),
                    torch.sigmoid(gate_in).flatten(),
                )
                outputs = [
                    torch.full((ref.numel() + 64,), 123456.0, device="cuda")
                    for ref in references
                ]
                inputs = (raw, key, qw, kw, pos)
                readonly = [value.clone() for value in inputs]
                params = Params(
                    *[value.data_ptr() for value in inputs],
                    *[value.data_ptr() for value in outputs],
                    rows,
                    heads,
                    kv,
                    256,
                    64,
                    10000000.0,
                    1.0,
                    1e-6,
                    11,
                    10,
                )
                arguments = (c.c_void_p * 1)(c.addressof(params))
                driver.call(
                    "cuLaunchKernel",
                    function,
                    c.c_uint(heads + kv),
                    c.c_uint(rows),
                    c.c_uint(1),
                    c.c_uint(256),
                    c.c_uint(1),
                    c.c_uint(1),
                    c.c_uint(0),
                    c.c_void_p(torch.cuda.current_stream().cuda_stream),
                    arguments,
                    c.c_void_p(),
                )
                torch.cuda.synchronize()
                errors = []
                for output, reference in zip(outputs, references):
                    error = float((output[: reference.numel()] - reference).abs().max())
                    assert error <= 2e-4, (label, rows, coordinates, error)
                    assert bool(torch.all(output[reference.numel() :] == 123456.0)), (
                        "Output canary changed"
                    )
                    errors.append(error)
                for value, original in zip(inputs, readonly):
                    assert torch.equal(
                        value.view(torch.int32), original.view(torch.int32)
                    ), "Input changed"
                case = dict(
                    model=label,
                    rows=rows,
                    coordinates=coordinates,
                    checkpoint_weight=name,
                    max_abs_q_k_gate=errors,
                )
                cases.append(case)
                print(json.dumps(case), flush=True)
        assert covered == {name for name, _, _ in weights}, (
            "Missing a checkpoint norm pair"
        )
    report = dict(
        status="ok",
        fixed_abs_budget=2e-4,
        cases=cases,
        ptx_sha256=hashlib.sha256(args.ptx.read_bytes()).hexdigest(),
        checker_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        oracle_source_sha256=hashlib.sha256(
            Path(inspect.getfile(hf)).read_bytes()
        ).hexdigest(),
        oracle="Transformers Qwen3_5RMSNorm, Qwen3_5TextRotaryEmbedding and apply_rotary_pos_emb with real checkpoint norms",
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
