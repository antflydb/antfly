#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Explicit CPU-only qualification of the patched reference's GGUF loaders.

Run with the installed vLLM environment, on one CPU outside measured requests.
Loads only selected packed Q tensor bytes, never a model or CUDA context.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from patch_vllm_gemma4_gguf import verify_unused_kv_route
from prepare_vllm_gguf_reference import read_header


def require(condition, message):
    """Qualification checks must remain active under python -O."""
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gguf", type=Path, required=True)
    parser.add_argument("--metadata-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    os.environ["OMP_NUM_THREADS"] = "1"
    import torch
    torch.set_num_threads(1)
    def forbid_cuda(*_args, **_kwargs):
        raise AssertionError("CPU qualification attempted CUDA initialization")
    torch.cuda._lazy_init = forbid_cuda
    from gguf import GGMLQuantizationType
    from gguf.quants import dequantize
    import numpy as np
    import vllm
    from vllm.model_executor.layers.linear import QKVParallelLinear, ReplicatedLinear
    from vllm.model_executor.utils import set_weight_attrs
    from vllm_gguf_plugin.quantization.config import GGUFConfig
    from vllm_gguf_plugin.quantization.linear import GGUFLinearMethod
    from vllm_gguf_plugin.weights_adapter.gemma4 import gemma4_shared_q_only_layers, gemma4_expand_shared_qkv
    from vllm_gguf_plugin.quantization.params import (
        GGUFUninitializedWeightParameter, GGUFUninitializedWeightTypeParameter,
        _resolve_gguf_weight_loader, _resolve_gguf_weight_type_loader,
        _materialize_gguf_weight_parameter, _materialize_gguf_weight_type_parameter,
    )
    report = {"passed": False, "cuda_initialized": False, "shared_qkv": []}
    try:
        report["stock_route_proof"] = verify_unused_kv_route(Path(vllm.__file__).parent)
        # Only distributed scalar getters are stubbed; all parameter classes,
        # storage callbacks and packed-shard assembly remain actual code.
        with patch("vllm.model_executor.parameter.get_tensor_model_parallel_rank", return_value=0), \
             patch("vllm.model_executor.parameter.get_tensor_model_parallel_world_size", return_value=1):
            layer = object.__new__(ReplicatedLinear)
            torch.nn.Module.__init__(layer)
            packed = torch.arange(256 * 1440, dtype=torch.int64).to(torch.uint8).reshape(256, 1440)
            weight = GGUFUninitializedWeightParameter(requires_grad=False, device="cpu")
            set_weight_attrs(weight, {"weight_loader": _resolve_gguf_weight_loader(layer, layer.weight_loader),
                "input_dim": 1, "output_dim": 0, "tensor_shape": (256, 2560), "data_container": [], "shard_id": [], "shard_id_map": {}})
            layer.register_parameter("weight", weight)
            wtype = GGUFUninitializedWeightTypeParameter(requires_grad=False, device="cpu")
            set_weight_attrs(wtype, {"weight_loader": _resolve_gguf_weight_type_loader(layer, layer.weight_loader),
                "weight_type": 0, "shard_weight_type": {}, "num_elements": 1})
            layer.register_parameter("weight_type", wtype)
            wtype.weight_loader(wtype, torch.tensor(2))
            weight.weight_loader(weight, packed)
            _materialize_gguf_weight_parameter(layer, "weight")
            _materialize_gguf_weight_type_parameter(layer, "weight_type")
            require(torch.equal(layer.weight, packed) and layer.weight.shape == (256, 1440),
                    "replicated loader changed packed weights or shape")
            require(layer.weight_type.item() == 2 and layer.weight_type.weight_type == 2,
                    "replicated loader changed the GGUF weight type")
            try:
                layer.weight.weight_loader(layer.weight, packed, "q")
            except ValueError:
                pass
            else:
                raise AssertionError("replicated loader accepted TP shards")
            dense = torch.nn.Parameter(torch.empty((2, 3), dtype=torch.float16), requires_grad=False)
            values = torch.tensor([[1, 2, 3], [4, 5, 6]], dtype=torch.float16)
            layer.weight_loader(dense, values)
            require(torch.equal(dense, values), "replicated loader changed dense F16 values")
            report["replicated_loader"] = {"packed_bytes_preserved": True, "weight_type_preserved": True,
                "non_tp_rows_preserved": True, "tp_shards_rejected": True, "dense_f16_unchanged": True}
            header = read_header(args.gguf)
            config = SimpleNamespace(**json.loads((args.metadata_dir / "config.json").read_text())["text_config"])
            shared = gemma4_shared_q_only_layers(set(header["tensors"]), config)
            require(shared == set(range(24, 42)), "unexpected shared Q-only layers")
            with args.gguf.open("rb") as source:
                for index in sorted(shared):
                    dim = config.global_head_dim if config.layer_types[index] == "full_attention" else config.head_dim
                    tensor = header["tensors"][f"blk.{index}.attn_q.weight"]
                    source.seek(header["data_offset"] + tensor["offset"])
                    data = source.read(8 * dim * 1440)
                    require(tensor["ggml_type"] == 2 and len(data) == 8 * dim * 1440,
                            "learned Q tensor type or payload length differs")
                    original = torch.frombuffer(bytearray(data), dtype=torch.uint8).reshape(8 * dim, 1440)
                    name = f"model.language_model.layers.{index}.self_attn.q_proj."
                    types = list(gemma4_expand_shared_qkv(name + "weight_type", torch.tensor(2), shared, config))
                    weights = list(gemma4_expand_shared_qkv(name + "weight", original, shared, config))
                    require(weights[0][1] is original and original.numpy().tobytes() == data,
                            "shared QKV expansion changed learned Q bytes")
                    for derived_name, derived in weights[1:]:
                        require(derived.shape == (2 * dim, 1440) and derived.dtype == torch.uint8,
                                "derived K/V packed shape or dtype differs")
                        decoded = dequantize(derived.numpy(), GGMLQuantizationType.Q4_0)
                        require(decoded.shape == (2 * dim, 2560) and np.isfinite(decoded).all() and not np.any(decoded),
                                "derived unused K/V does not dequantize to finite exact zero")
                    qkv = object.__new__(QKVParallelLinear)
                    torch.nn.Module.__init__(qkv)
                    qkv.num_heads, qkv.num_kv_heads, qkv.num_kv_head_replicas = 8, 2, 1
                    qkv.head_size = qkv.v_head_size = dim
                    method = GGUFLinearMethod(GGUFConfig())
                    method.create_weights(qkv, 2560, [8 * dim, 2 * dim, 2 * dim], 2560, 12 * dim, torch.float16,
                                          weight_loader=qkv.weight_loader)
                    for derived_name, value in types:
                        qkv.weight_type.weight_loader(qkv.weight_type, value, derived_name.split("_proj.")[0][-1])
                    for derived_name, value in weights:
                        qkv.weight.weight_loader(qkv.weight, value, derived_name.split("_proj.")[0][-1])
                    method.process_weights_after_loading(qkv)
                    require(qkv.weight.shape == (12 * dim, 1440), "assembled QKV packed shape differs")
                    require(torch.equal(qkv.weight[:8 * dim], original) and not torch.any(qkv.weight[8 * dim:]),
                            "assembled QKV changed Q or uninitialized unused K/V")
                    require(qkv.weight.shard_id == ["q", "k", "v"], "assembled QKV shard order differs")
                    require(qkv.weight_type.shard_weight_type == {"q": 2, "k": 2, "v": 2},
                            "assembled QKV shard weight types differ")
                    report["shared_qkv"].append({"layer": index, "head_dim": dim,
                        "learned_q_sha256": hashlib.sha256(data).hexdigest(), "learned_bytes_preserved": True,
                        "unused_kv_cpu_dequantized_exact_zero": True, "assembled_shape": list(qkv.weight.shape),
                        "weight_type": 2, "assembled_qkv_initialized": True})
            require(not torch.cuda.is_initialized(), "CPU qualification initialized CUDA")
            report["passed"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "qualified_shared_layers": len(report["shared_qkv"]), "cuda_initialized": False}))


if __name__ == "__main__":
    main()
