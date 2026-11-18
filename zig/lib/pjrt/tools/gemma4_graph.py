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

"""Offline reference graph construction; not used in native timed inference."""

import inspect
import torch


def patch_casts():
    # torchax 0.0.13 type_as does not handle a View argument. Equivalent cast.
    from transformers.models.gemma4.modeling_gemma4 import Gemma4RMSNorm

    def norm(self, x):
        y = self._norm(x.float())
        if self.with_scale:
            y = y * self.weight.float()
        return y.to(dtype=x.dtype)

    Gemma4RMSNorm.forward = norm


def configure_model(cfg):
    from transformers import Gemma4ForCausalLM

    cfg._attn_implementation = "eager"
    with torch.device("meta"):
        m = Gemma4ForCausalLM(cfg)
    m.eval()
    m.requires_grad_(False)
    return m


def cache_state(cfg, capacity, device):
    values = []
    for i in range(cfg.num_hidden_layers - cfg.num_kv_shared_layers):
        dim = cfg.per_layer_config[i].head_dim
        heads = cfg.num_key_value_heads
        shape = (1, heads, capacity, dim)
        values.extend(
            [
                torch.zeros(shape, dtype=torch.bfloat16, device=device),
                torch.zeros(shape, dtype=torch.bfloat16, device=device),
            ]
        )
    return tuple(values)


def make_step(model, capacity, functional=True):
    from transformers.cache_utils import Cache, StaticLayer

    cfg = model.config
    cache_name = (
        "cache"
        if "cache" in inspect.signature(model.forward).parameters
        else "past_key_values"
    )
    donor_count = cfg.num_hidden_layers - cfg.num_kv_shared_layers

    def step(params, state, ids, position):
        layers = [
            StaticLayer(max_cache_len=capacity) for _ in range(cfg.num_hidden_layers)
        ]
        for i in range(donor_count):
            layer = layers[i]
            layer.keys, layer.values = state[2 * i : 2 * i + 2]
            layer.is_initialized = True
            layer.device = ids.device
            layer.dtype = layer.keys.dtype
            layer.batch_size = 1
            layer.num_heads = cfg.num_key_value_heads
            layer.k_head_dim = layer.keys.shape[-1]
            layer.v_head_dim = layer.values.shape[-1]
            layer.cumulative_length = position.clone()
        cache = Cache(layers=layers)
        positions = (
            torch.arange(ids.shape[1], device=ids.device) + position
        ).unsqueeze(0)
        keys = torch.arange(capacity, device=ids.device)[None, None, :]
        allowed = keys <= positions[:, :, None]
        full = torch.where(allowed, 0.0, torch.finfo(torch.float32).min)[:, None, :, :]
        sliding = torch.where(
            allowed & (keys > positions[:, :, None] - cfg.sliding_window),
            0.0,
            torch.finfo(torch.float32).min,
        )[:, None, :, :]
        kwargs = dict(
            input_ids=ids,
            position_ids=positions,
            attention_mask={"full_attention": full, "sliding_attention": sliding},
            use_cache=True,
            logits_to_keep=1,
        )
        kwargs[cache_name] = cache
        out = (
            torch.func.functional_call(model, params, (), kwargs)
            if functional
            else model(**kwargs)
        )
        state = tuple(
            value
            for layer in layers[:donor_count]
            for value in (layer.keys, layer.values)
        )
        return out.logits[:, -1, :], state, position + ids.shape[1]

    return step
