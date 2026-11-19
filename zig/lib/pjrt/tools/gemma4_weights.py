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

"""Strict GGUF -> official Gemma 4 names and physical layouts, including QAT bounds."""

import re

FIXED = {
    "v.patch_embd.weight": "vision_tower.patch_embedder.input_proj.weight",
    "v.position_embd.weight": "vision_tower.patch_embedder.position_embedding_table",
    "mm.input_projection.weight": "embed_vision.embedding_projection.weight",
    "mm.a.input_projection.weight": "embed_audio.embedding_projection.weight",
    "a.input_projection.weight": "audio_tower.subsample_conv_projection.input_proj_linear.weight",
    "a.pre_encode.out.weight": "audio_tower.output_proj.weight",
    "a.pre_encode.out.bias": "audio_tower.output_proj.bias",
}
VISION = {
    "ln1": "input_layernorm",
    "ln2": "pre_feedforward_layernorm",
    "attn_post_norm": "post_attention_layernorm",
    "ffn_post_norm": "post_feedforward_layernorm",
    "attn_q_norm": "self_attn.q_norm",
    "attn_k_norm": "self_attn.k_norm",
}
AUDIO = {
    "ln2": "norm_out",
    "attn_pre_norm": "norm_pre_attn",
    "attn_post_norm": "norm_post_attn",
    "conv_norm": "lconv1d.pre_layer_norm",
    "norm_conv": "lconv1d.conv_norm",
    "conv_dw": "lconv1d.depthwise_conv1d",
    "per_dim_scale": "self_attn.per_dim_scale",
    "attn_k_rel": "self_attn.relative_k_proj",
    "ffn_norm": "feed_forward1.pre_layer_norm",
    "ffn_post_norm": "feed_forward1.post_layer_norm",
    "ffn_norm_1": "feed_forward2.pre_layer_norm",
    "ffn_post_norm_1": "feed_forward2.post_layer_norm",
}
VL = {
    "attn_q": "self_attn.q_proj",
    "attn_k": "self_attn.k_proj",
    "attn_v": "self_attn.v_proj",
    "attn_out": "self_attn.o_proj",
    "ffn_gate": "mlp.gate_proj",
    "ffn_up": "mlp.up_proj",
    "ffn_down": "mlp.down_proj",
}
AL = {
    "attn_q": "self_attn.q_proj",
    "attn_k": "self_attn.k_proj",
    "attn_v": "self_attn.v_proj",
    "attn_out": "self_attn.post",
    "ffn_up": "feed_forward1.ffw_layer_1",
    "ffn_down": "feed_forward1.ffw_layer_2",
    "ffn_up_1": "feed_forward2.ffw_layer_1",
    "ffn_down_1": "feed_forward2.ffw_layer_2",
    "conv_pw1": "lconv1d.linear_start",
    "conv_pw2": "lconv1d.linear_end",
}


def projector_name(name):
    if name in FIXED:
        return "model." + FIXED[name]
    if m := re.fullmatch(r"a\.conv1d\.([01])\.(weight|norm\.weight)", name):
        return f"model.audio_tower.subsample_conv_projection.layer{m[1]}.{'conv.weight' if m[2] == 'weight' else 'norm.weight'}"
    m = re.fullmatch(
        r"([va])\.blk\.(\d+)\.(.+)\.(weight|input_min|input_max|output_min|output_max)",
        name,
    )
    if not m:
        raise ValueError(("unmapped projector tensor", name))
    vision, layer, key, suffix = m[1] == "v", m[2], m[3], m[4]
    prefix = (
        f"model.vision_tower.encoder.layers.{layer}."
        if vision
        else f"model.audio_tower.layers.{layer}."
    )
    vectors, linears = (VISION, VL) if vision else (AUDIO, AL)
    if key in vectors and suffix == "weight":
        target = vectors[key]
        return prefix + target + ("" if key == "per_dim_scale" else ".weight")
    if key in linears:
        return (
            prefix
            + linears[key]
            + (".linear.weight" if suffix == "weight" else "." + suffix)
        )
    raise ValueError(("unmapped projector layer tensor", name))


def text_mapping(config):
    import torch
    from transformers import Gemma4ForCausalLM
    from transformers.modeling_gguf_pytorch_utils import (
        TensorProcessor,
        get_gguf_hf_weights_map,
    )

    with torch.device("meta"):
        model = Gemma4ForCausalLM(config)
    mapping = get_gguf_hf_weights_map(model, TensorProcessor())
    result = {}
    for source, target in mapping.items():
        if source.endswith(".layer_output_scale"):
            source += ".weight"
        result[source] = (
            target.replace("model.", "model.language_model.", 1)
            if target.startswith("model.")
            else target
        )
    return result


def parameter_specs(model, gguf, mmproj):
    """Reject missing, duplicate or shape-incompatible mappings before export."""
    import math

    from gguf import GGUFReader

    params = {**dict(model.named_parameters()), **dict(model.named_buffers())}
    persistent = set(model.state_dict())
    mapped = {}
    for file, directory in [("gguf", gguf), ("mmproj", mmproj)]:
        reader = GGUFReader(str(directory))
        mapping = text_mapping(model.config.text_config) if file == "gguf" else None
        for tensor in reader.tensors:
            if file == "gguf" and tensor.name == "rope_freqs.weight":
                continue
            target = (
                mapping.get(tensor.name) if mapping else projector_name(tensor.name)
            )
            if target not in params:
                raise ValueError(("unmapped GGUF source", file, tensor.name, target))
            if target in mapped:
                raise ValueError(("duplicate mapping", target))
            shape = list(params[target].shape)
            if math.prod(shape) != math.prod(tensor.shape):
                raise ValueError(
                    ("shape mismatch", target, shape, tensor.shape.tolist())
                )
            spec = {"source": tensor.name, "file": file}
            physical = list(reversed(tensor.shape.tolist()))
            if tensor.name == "v.patch_embd.weight":
                if physical != [shape[0], 3, 16, 16] or shape != [shape[0], 768]:
                    raise ValueError(("unexpected patch layout", physical, shape))
                spec["transform"] = "patch_nchw_to_nhwc"
                spec["source_shape"] = physical
            elif (
                tensor.name.endswith(".conv_dw.weight")
                or tensor.name.startswith("a.conv1d.")
                or math.prod(shape) == 1
            ):
                if [d for d in physical if d != 1] != [d for d in shape if d != 1]:
                    raise ValueError(
                        ("nontrivial audio/scalar layout", tensor.name, physical, shape)
                    )
            elif physical != shape:
                raise ValueError(
                    ("unqualified physical layout", tensor.name, physical, shape)
                )
            mapped[target] = spec
    specs = []
    for name, value in sorted(params.items()):
        spec = {
            "name": name,
            "shape": list(value.shape),
            "dtype": "bfloat16" if name in persistent else "float32",
        }
        if name in persistent:
            if name not in mapped:
                raise ValueError(("missing model weight", name))
            spec.update(mapped[name])
        else:
            if value.is_meta:
                raise ValueError(("uninitialized derived buffer", name))
            spec["data"] = value.flatten().tolist()
        specs.append(spec)
    return specs
