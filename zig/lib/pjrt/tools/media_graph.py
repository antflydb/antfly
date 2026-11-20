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

"""Static-shape full media graph; preserves official tower modules and boundaries.

Only data-dependent padding stripping and placeholder discovery are resolved from
processor metadata. All tower layers, pooling and projections remain in HLO.
"""

import types
from pathlib import Path

import numpy as np
import torch
from transformers import AutoConfig
from transformers.models.embedding_gemma2.modeling_embedding_gemma2 import (
    EmbeddingGemma2Model,
    EmbeddingGemma2RMSNorm,
    EmbeddingGemma2RotaryEmbedding,
)
from transformers.models.gemma4.modeling_gemma4 import (
    Gemma4AudioRelPositionalEncoding,
    Gemma4VisionRotaryEmbedding,
)


def load_meta(directory):
    from safetensors import safe_open

    cfg = AutoConfig.from_pretrained(directory, local_files_only=True)
    cfg._attn_implementation = "eager"
    cfg.text_config._attn_implementation = "eager"
    cfg.vision_config._attn_implementation = "eager"
    with torch.device("meta"):
        m = EmbeddingGemma2Model(cfg)
    m.eval().requires_grad_(False)
    m.language_model.rotary_emb = EmbeddingGemma2RotaryEmbedding(
        cfg.text_config, device="cpu"
    )
    m.vision_tower.encoder.rotary_emb = Gemma4VisionRotaryEmbedding(
        cfg.vision_config, device="cpu"
    )
    m.audio_tower.rel_pos_enc = Gemma4AudioRelPositionalEncoding(cfg.audio_config)
    for mod in m.modules():
        if hasattr(mod, "scalar_embed_scale"):
            mod.embed_scale = torch.tensor(mod.scalar_embed_scale, dtype=torch.float32)
        if hasattr(mod, "attention_logits_soft_cap") and hasattr(mod, "softcap"):
            mod.softcap = torch.tensor(
                mod.attention_logits_soft_cap, dtype=torch.float32
            )

    def norm(self, x):
        out = self._norm(x.float())
        if self.with_scale:
            out = out * self.weight.float()
        return out.to(dtype=x.dtype)

    EmbeddingGemma2RMSNorm.forward = norm
    state = set(m.state_dict())
    params = {**dict(m.named_parameters()), **dict(m.named_buffers())}
    spec = []
    with safe_open(
        Path(directory) / "model.safetensors", framework="pt", device="cpu"
    ) as f:
        keys = set(f.keys())
        for name, t in sorted(params.items()):
            p = {
                "name": name,
                "shape": list(t.shape),
                "dtype": "bfloat16" if name in state else "float32",
            }
            if name in state:
                assert name in keys, name
                assert list(f.get_slice(name).get_shape()) == p["shape"]
                p["source"] = name
            else:
                assert not t.is_meta, name
                p["data"] = t.reshape(-1).tolist()
            spec.append(p)
    return m, spec


def make_fn(m, inputs):
    ids_cpu = inputs["input_ids"][0].numpy()
    image_slots = np.flatnonzero(ids_cpu == m.config.image_token_id).tolist()
    audio_slots = np.flatnonzero(ids_cpu == m.config.audio_token_id).tolist()
    audio_valid = (
        np.flatnonzero(inputs["input_features_mask"][0, ::4].numpy()).tolist()
        if audio_slots
        else []
    )
    keys = list(inputs)

    def forward(self, **values):
        ids, mask = values["input_ids"], values["attention_mask"]
        modal = (ids == self.config.image_token_id) | (
            ids == self.config.audio_token_id
        )
        hard_ids = torch.where(modal, self.config.text_config.pad_token_id, ids)
        embeddings = self.language_model.embed_tokens(hard_ids)
        if image_slots:
            tower = self.vision_tower
            pixels, positions = values["pixel_values"], values["image_position_ids"]
            padding = (positions == -1).all(-1)
            hidden = tower.patch_embedder(pixels, positions, padding)
            vmask = torch.where(
                (~padding)[:, None, None, :], 0.0, torch.finfo(torch.float32).min
            )
            rotary = tower.encoder.rotary_emb(hidden, positions)
            for layer in tower.encoder.layers:
                hidden = layer(
                    hidden,
                    attention_mask=vmask,
                    position_embeddings=rotary,
                    position_ids=positions,
                )
            hidden, _ = tower.pooler(hidden, positions, padding, pixels.shape[1] // 9)
            # Official pooler emits row-major valid cells before padding.
            hidden = hidden[:, : len(image_slots)]
            if tower.config.standardize:
                hidden = (hidden - tower.std_bias.float()) * tower.std_scale.float()
            hidden = hidden.to(embeddings.dtype)
            projected = self.embed_vision(hidden)
            embeddings = embeddings.index_copy(
                1, torch.tensor(image_slots, device=ids.device), projected
            )
        if audio_slots:
            tower = self.audio_tower
            hidden, valid = tower.subsample_conv_projection(
                values["input_features"], values["input_features_mask"]
            )
            rotary = tower.rel_pos_enc(hidden)
            positions = torch.arange(hidden.shape[1], device=ids.device)
            delta = positions[None, :] - positions[:, None]
            allowed = (
                (delta <= 0) & (-delta < tower.config.attention_context_left - 1)
            ) | ((delta > 0) & (delta < tower.config.attention_context_right))
            allowed = allowed[None, None] & valid[:, None, None, :].bool()
            blocked = tower._convert_4d_mask_to_blocked_5d(
                torch.where(allowed, 0.0, torch.finfo(torch.float32).min)
            )
            for layer in tower.layers:
                hidden = layer(
                    hidden, attention_mask=blocked, position_embeddings=rotary
                )
            projected = self.embed_audio(tower.output_proj(hidden))
            projected = projected[:, torch.tensor(audio_valid, device=ids.device)]
            assert projected.shape[1] == len(audio_slots)
            embeddings = embeddings.index_copy(
                1,
                torch.tensor(audio_slots, device=ids.device),
                projected.to(embeddings.dtype),
            )
        n = ids.shape[1]
        positions = torch.arange(n, device=ids.device)
        allowed = mask[:, None, None, :].bool()
        neg = torch.finfo(torch.float32).min
        full = torch.where(allowed, 0.0, neg).expand(ids.shape[0], 1, n, n)
        sliding = torch.where(
            allowed
            & (
                (positions[None, :] - positions[:, None]).abs()
                <= self.config.text_config.sliding_window
            )[None, None],
            0.0,
            neg,
        )
        out = self.language_model(
            inputs_embeds=embeddings,
            attention_mask={"full_attention": full, "sliding_attention": sliding},
            position_ids=positions[None],
        ).last_hidden_state.float()
        weights = mask.unsqueeze(-1).float()
        return torch.nn.functional.normalize(
            (out * weights).sum(1) / weights.sum(1), dim=-1
        )

    m.forward = types.MethodType(forward, m)

    def fn(params, *args):
        return torch.func.functional_call(m, params, (), dict(zip(keys, args)))

    return fn
