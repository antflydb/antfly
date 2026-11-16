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

"""Compare the actual native BF16 vision block with independent Transformers.

Use the qualification-only C ABI library, never a copied kernel implementation.
The reference changes only the attention batch dispatch to matching Torch cuDNN.
"""

import ctypes as c
import hashlib
import inspect
import json
import os
from pathlib import Path
import torch
from safetensors import safe_open
import torch.nn.functional as F
import transformers.models.qwen3_5.modeling_qwen3_5 as hf
from torch.nn.attention import sdpa_kernel, SDPBackend
from transformers.models.qwen3_5.configuration_qwen3_5 import Qwen3_5VisionConfig
from transformers.models.qwen3_5.modeling_qwen3_5 import (
    Qwen3_5VisionBlock,
    Qwen3_5VisionRotaryEmbedding,
)
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--library", type=Path, required=True)
parser.add_argument("--library-sha256", required=True)
parser.add_argument("--model-08", type=Path, required=True)
parser.add_argument("--model-4b", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
parser.add_argument("--save-failure", action="store_true")
args = parser.parse_args()
p = args.output_dir
p.mkdir(parents=True, exist_ok=False)
library = args.library
assert hashlib.sha256(library.read_bytes()).hexdigest() == args.library_sha256, (
    "Qualification library SHA256 mismatch"
)
metadata = {
    "library_sha256": args.library_sha256,
    "library": str(library),
    "scope": "Qualification-only ABI invokes the actual serving Module.block; independently built from the current source using the documented command.",
}


class Packet(c.Structure):
    _fields_ = (
        [
            ("input", c.c_uint64),
            ("output", c.c_uint64),
            ("weights", c.c_uint64 * 12),
            ("positions", c.c_uint64),
        ]
        + [(n, c.c_uint32) for n in ["rows", "hidden", "intermediate", "batch"]]
        + [("epsilon", c.c_float), ("theta", c.c_float)]
    )


assert c.sizeof(Packet) == 144
lib = c.CDLL(str(library.resolve()))
lib.antfly_vision_block_abi.restype = c.c_uint64
assert lib.antfly_vision_block_abi() == (1 << 32) | 144
lib.antfly_vision_block_run.argtypes = [c.POINTER(Packet)]
lib.antfly_vision_block_run.restype = c.c_int
lib.antfly_vision_block_init.restype = c.c_int
lib.antfly_vision_block_deinit.restype = None
assert lib.antfly_vision_block_run(None) == -2
backend = SDPBackend.CUDNN_ATTENTION


def batched_attention(
    module, q, k, v, attention_mask=None, scaling=None, cu_seq_lens_q=None, **kwargs
):
    lengths = cu_seq_lens_q[1:] - cu_seq_lens_q[:-1]
    assert torch.all(lengths == lengths[0])
    b = len(lengths)
    n = int(lengths[0])
    heads = module.num_heads
    dim = module.head_dim
    assert q.shape == (1, heads, b * n, dim)

    def image_lanes(t):
        return t.transpose(1, 2).reshape(b, n, heads, dim).transpose(1, 2)

    with sdpa_kernel(backend):
        out = F.scaled_dot_product_attention(
            image_lanes(q), image_lanes(k), image_lanes(v), scale=scaling
        )
    return out.transpose(1, 2).reshape(1, b * n, heads, dim), None


original_flash = hf.is_flash_attention_requested
hf.ALL_ATTENTION_FUNCTIONS.register("qualified_batched_sdpa", batched_attention)
hf.is_flash_attention_requested = lambda config: (
    config._attn_implementation == "qualified_batched_sdpa" or original_flash(config)
)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


records = []
report = {
    "checkpoints": {},
    "reference_precision": {
        "BF16_activations": True,
        "allow_bf16_reduced_precision_reduction": False,
        "allow_split_k": False,
        "attention_backend": "CUDNN_ATTENTION; equal-size image batch adapter",
        "native_gemm_workspace_bytes": 0,
        "F32_oracles_and_BF16_thresholds": "unchanged",
    },
    "scope": "Independent installed Transformers BF16 Qwen3.5 vision blocks with every real checkpoint layer versus exact serving Module.block through qualification-only C ABI. Not a whole-model throughput result or a full BF16 decoder profile.",
    "oracle": "Transformers Qwen3_5VisionBlock and Qwen3_5VisionRotaryEmbedding; PyTorch cuDNN SDPA using equal-size image batches with BF16 reduced-precision and split-K reductions both disabled to match the native zero-workspace policy; original BF16 checkpoint parameters. Only attention batch dispatch adapted through independent Torch SDPA; no native attention/rotary/operator formulas copied. This qualifies the matching cuDNN backend only; it does not establish numerical equivalence to other attention backends.",
    "criteria": {
        "maximum_element_error": "2 BF16 ULP at independent reference output + original F32 2e-4 + abs(reference)*2e-5",
        "relative_L2_limit": 1 / 256,
        "default_F32_oracles": "Unchanged and separately qualified; this is a distinct BF16 activation profile.",
    },
    "ffi": metadata,
    "transformers_source_sha256": hashlib.sha256(
        Path(inspect.getfile(Qwen3_5VisionBlock)).read_bytes()
    ).hexdigest(),
    "versions": {"torch": torch.__version__},
    "records": records,
}


def save():
    (p / "report.json").write_text(json.dumps(report, indent=2) + "\n")


torch.manual_seed(480)
torch.set_num_threads(4)
torch.cuda.init()
torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction = (False, False)
torch.backends.cuda.matmul.allow_tf32 = False
torch.backends.cudnn.allow_tf32 = False
assert torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction is False
assert (
    torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction_split_k is False
)
torch.cuda.synchronize()
assert lib.antfly_vision_block_init() == 0
try:
    with torch.inference_mode():
        for model in ["0.8B", "4B"]:
            model_dir = args.model_08 if model == "0.8B" else args.model_4b
            config = json.loads((model_dir / "config.json").read_text())
            report["checkpoints"][model] = {
                "config_sha256": file_sha256(model_dir / "config.json"),
                "safetensors_sha256": file_sha256(model_dir / "model.safetensors"),
            }
            cfg = Qwen3_5VisionConfig(**config["vision_config"])
            cfg._attn_implementation = "qualified_batched_sdpa"
            hidden = cfg.hidden_size
            wide = cfg.intermediate_size
            depth = cfg.depth
            theta = float(cfg.rope_parameters["rope_theta"])
            rotary = Qwen3_5VisionRotaryEmbedding(cfg).cuda()
            block = (
                Qwen3_5VisionBlock(cfg).to(device="cuda", dtype=torch.bfloat16).eval()
            )
            with safe_open(
                str(model_dir / "model.safetensors"), framework="pt", device="cpu"
            ) as f:
                for layer in range(depth):
                    prefix = "model.visual.blocks." + str(layer) + "."
                    weights = {
                        k[len(prefix) :]: f.get_tensor(k)
                        for k in f.keys()
                        if k.startswith(prefix)
                    }
                    assert weights and all(
                        t.dtype == torch.bfloat16 for t in weights.values()
                    ), "Checkpoint dtype changed"
                    block.load_state_dict(weights, strict=True)
                    names = [
                        "norm1.weight",
                        "norm1.bias",
                        "attn.qkv.weight",
                        "attn.qkv.bias",
                        "attn.proj.weight",
                        "attn.proj.bias",
                        "norm2.weight",
                        "norm2.bias",
                        "mlp.linear_fc1.weight",
                        "mlp.linear_fc1.bias",
                        "mlp.linear_fc2.weight",
                        "mlp.linear_fc2.bias",
                    ]
                    params = dict(block.named_parameters())
                    native_weights = [
                        params[n].float().contiguous()
                        if i in [0, 1, 6, 7]
                        else params[n].contiguous()
                        for i, n in enumerate(names)
                    ]
                    snapshots = [w.clone() for w in native_weights]
                    cases = [(4, 4, 1), (4, 4, 3), (16, 16, 1)]
                    if layer in [0, depth - 1]:
                        cases += [(36, 72, 1), (4, 4, 16)]
                    if layer == 0:
                        cases += [(64, 64, 16)]
                    for height, width, batch in cases:
                        per_image = height * width
                        rows = per_image * batch
                        if rows == 65536:
                            # The checkpoint hash above reads the full file.
                            # On UMA, its reclaimable page cache also reduces
                            # CUDA free memory. Retire only unused test memory
                            # and these immutable inputs' closed file pages.
                            torch.cuda.synchronize()
                            torch.cuda.empty_cache()
                            if hasattr(os, "posix_fadvise"):
                                for checkpoint_dir in [args.model_08, args.model_4b]:
                                    with (checkpoint_dir / "model.safetensors").open(
                                        "rb"
                                    ) as checkpoint:
                                        os.posix_fadvise(
                                            checkpoint.fileno(),
                                            0,
                                            0,
                                            os.POSIX_FADV_DONTNEED,
                                        )
                            free, _ = torch.cuda.mem_get_info()
                            assert free >= 60 * 1024**3, (
                                "Insufficient memory for required maximum-shape independent cuDNN reference",
                                free,
                            )
                        hp = (
                            torch.arange(height, device="cuda")
                            .view(height, 1)
                            .expand(height, width)
                        )
                        wp = (
                            torch.arange(width, device="cuda")
                            .view(1, width)
                            .expand(height, width)
                        )
                        h = (
                            hp.reshape(height // 2, 2, width // 2, 2)
                            .permute(0, 2, 1, 3)
                            .reshape(-1)
                            .repeat(batch)
                        )
                        w = (
                            wp.reshape(height // 2, 2, width // 2, 2)
                            .permute(0, 2, 1, 3)
                            .reshape(-1)
                            .repeat(batch)
                        )
                        positions = torch.stack([h, w]).to(torch.int32).contiguous()
                        pos_snapshot = positions.clone()
                        image_offsets = (
                            torch.arange(rows, device="cuda") // per_image
                        ).float().unsqueeze(1) * 0.125
                        x = (
                            torch.randn(rows, hidden, device="cuda") * 0.3
                            + image_offsets
                        ).bfloat16()
                        x_snapshot = x.clone()
                        storage = torch.full(
                            (rows * hidden + 16,),
                            123.0,
                            device="cuda",
                            dtype=torch.bfloat16,
                        )
                        actual = storage[8:-8].view(rows, hidden)
                        cu_seqlens = (
                            torch.arange(batch + 1, device="cuda", dtype=torch.int32)
                            * per_image
                        )
                        cos, sin = rotary(x, positions.T.contiguous())
                        with sdpa_kernel(SDPBackend.CUDNN_ATTENTION):
                            expected = block(
                                x,
                                cu_seqlens,
                                position_embeddings=(cos, sin),
                                max_seqlen=per_image,
                            )
                        torch.cuda.synchronize()
                        packet = Packet(
                            x.data_ptr(),
                            actual.data_ptr(),
                            (c.c_uint64 * 12)(*(t.data_ptr() for t in native_weights)),
                            positions.data_ptr(),
                            rows,
                            hidden,
                            wide,
                            batch,
                            1e-6,
                            theta,
                        )
                        status = lib.antfly_vision_block_run(c.byref(packet))
                        assert status == 0, (model, layer, rows, status)
                        torch.cuda.synchronize()
                        ref = expected.float()
                        error = (actual.float() - ref).abs()
                        above = torch.nextafter(
                            expected, torch.full_like(expected, float("inf"))
                        ).float()
                        below = torch.nextafter(
                            expected, torch.full_like(expected, float("-inf"))
                        ).float()
                        ulp = torch.maximum((above - ref).abs(), (below - ref).abs())
                        budget = 2 * ulp + 2e-4 + ref.abs() * 2e-5
                        relative = float(
                            torch.linalg.vector_norm(actual.float() - ref)
                            / torch.linalg.vector_norm(ref).clamp_min(1e-12)
                        )
                        qualified = (
                            bool(torch.all(error <= budget)) and relative <= 1 / 256
                        )
                        assert torch.all(storage[:8] == 123) and torch.all(
                            storage[-8:] == 123
                        ), "Output guard overwritten"
                        assert torch.equal(x, x_snapshot) and torch.equal(
                            positions, pos_snapshot
                        ), "Input or position mutation"
                        for weight, snapshot in zip(native_weights, snapshots):
                            assert torch.equal(weight, snapshot), (
                                "Checkpoint weight mutation"
                            )
                        row = {
                            "model": model,
                            "layer": layer,
                            "height": height,
                            "width": width,
                            "batch": batch,
                            "rows": rows,
                            "max_abs": float(error.max()),
                            "maximum_error_beyond_fixed_budget": float(
                                (error - budget).max()
                            ),
                            "relative_L2": relative,
                            "bit_difference_count": int(
                                (
                                    actual.view(torch.int16)
                                    != expected.view(torch.int16)
                                ).sum()
                            ),
                            "qualified": qualified,
                            "input_and_weights_readonly": True,
                            "guards": True,
                        }
                        records.append(row)
                        save()
                        print(row, flush=True)
                        if not qualified and args.save_failure:
                            torch.save(
                                {
                                    "input": x.cpu(),
                                    "positions": positions.cpu(),
                                    "expected": expected.cpu(),
                                    "actual": actual.cpu(),
                                },
                                p / "failure.pt",
                            )
                        assert qualified, (
                            "Independent full checkpoint block differential failed; original criteria retained"
                        )
                        if model == "0.8B" and layer == 0 and batch == 3:
                            wrong = torch.empty_like(actual)
                            bad = Packet.from_buffer_copy(packet)
                            bad.output = wrong.data_ptr()
                            bad.batch = 1
                            assert lib.antfly_vision_block_run(c.byref(bad)) == 0
                            torch.cuda.synchronize()
                            wrong_error = (wrong.float() - ref).abs()
                            wrong_relative = float(
                                torch.linalg.vector_norm(wrong.float() - ref)
                                / torch.linalg.vector_norm(ref).clamp_min(1e-12)
                            )
                            assert not (
                                bool(torch.all(wrong_error <= budget))
                                and wrong_relative <= 1 / 256
                            ), "Oracle accepted crossed image attention"
                            assert torch.equal(x, x_snapshot) and torch.equal(
                                positions, pos_snapshot
                            )
                            report["crossed_image_negative_control"] = {
                                "rejected": True,
                                "max_abs": float(wrong_error.max()),
                                "relative_L2": wrong_relative,
                            }
                            save()
                        if len(records) == 1:
                            shifted = expected.roll(1, 0)
                            shifted_error = (shifted.float() - ref).abs()
                            shifted_relative = float(
                                torch.linalg.vector_norm(shifted.float() - ref)
                                / torch.linalg.vector_norm(ref).clamp_min(1e-12)
                            )
                            assert not (
                                bool(torch.all(shifted_error <= budget))
                                and shifted_relative <= 1 / 256
                            ), "Oracle accepted wrong row order"
                            report["row_order_negative_control"] = {
                                "rejected": True,
                                "max_abs": float(shifted_error.max()),
                                "relative_L2": shifted_relative,
                            }
                            save()
                            bad = Packet.from_buffer_copy(packet)
                            bad.output = bad.input
                            assert lib.antfly_vision_block_run(c.byref(bad)) == -1
                            bad = Packet.from_buffer_copy(packet)
                            bad.rows = 65537
                            assert lib.antfly_vision_block_run(c.byref(bad)) == -1
                            assert torch.equal(x, x_snapshot), (
                                "Declined request mutated input"
                            )
                            report["actual_module_alias_and_shape_guards"] = True
                            save()
                            for field, delta in [
                                ("input", 2),
                                ("output", 1),
                                ("positions", 2),
                            ]:
                                bad = Packet.from_buffer_copy(packet)
                                setattr(bad, field, getattr(bad, field) + delta)
                                assert (
                                    lib.antfly_vision_block_run(c.byref(bad)) == -1
                                ), (field, "Alignment not rejected before launch")
                            bad = Packet.from_buffer_copy(packet)
                            bad.weights[0] += 4
                            assert lib.antfly_vision_block_run(c.byref(bad)) == -1, (
                                "Vector weight alignment not rejected"
                            )
                            assert torch.equal(x, x_snapshot) and torch.equal(
                                positions, pos_snapshot
                            )
                            for weight, snapshot in zip(native_weights, snapshots):
                                assert torch.equal(weight, snapshot)
                            report["actual_module_alignment_guards"] = True
                            save()
                        del (
                            x,
                            actual,
                            expected,
                            storage,
                            cos,
                            sin,
                            positions,
                            pos_snapshot,
                            x_snapshot,
                        )
                    del weights, native_weights, snapshots
            del block, rotary
            torch.cuda.empty_cache()
finally:
    lib.antfly_vision_block_deinit()
    hf.is_flash_attention_requested = original_flash
report["status"] = "pass"
save()
(p / "finished").touch()
print(
    "Independent actual serving vision block qualification passed",
    len(records),
    flush=True,
)
