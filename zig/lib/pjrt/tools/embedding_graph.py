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

"""Offline EmbeddingGemma2 text graph preparation and matching BF16 weights."""

import json, torch
from pathlib import Path
from transformers import EmbeddingGemma2TextConfig
from transformers.models.embedding_gemma2.modeling_embedding_gemma2 import (
    EmbeddingGemma2TextModel,
    EmbeddingGemma2RotaryEmbedding,
    EmbeddingGemma2RMSNorm,
)
from safetensors import safe_open


def load_meta(model_dir):
    MODEL = Path(model_dir)
    cfg = EmbeddingGemma2TextConfig(
        **json.loads((MODEL / "config.json").read_text())["text_config"]
    )
    cfg._attn_implementation = "eager"
    with torch.device("meta"):
        m = EmbeddingGemma2TextModel(cfg)
    m.eval()
    m.requires_grad_(False)
    m.rotary_emb = EmbeddingGemma2RotaryEmbedding(cfg, device="cpu")
    for mod in m.modules():
        if hasattr(mod, "scalar_embed_scale"):
            mod.embed_scale = torch.tensor(mod.scalar_embed_scale, dtype=torch.float32)

    def norm(self, x):
        out = self._norm(x.float())
        if self.with_scale:
            out = out * self.weight.float()
        return out.to(dtype=x.dtype)

    EmbeddingGemma2RMSNorm.forward = norm
    params = {**dict(m.named_parameters()), **dict(m.named_buffers())}
    state = set(m.state_dict())
    spec = []
    with safe_open(MODEL / "model.safetensors", framework="pt", device="cpu") as f:
        for name, t in sorted(params.items()):
            p = {
                "name": name,
                "shape": list(t.shape),
                "dtype": "bfloat16" if name in state else "float32",
            }
            if name in state:
                p["source"] = "language_model." + name
                assert p["source"] in f.keys(), p["source"]
                assert list(f.get_slice(p["source"]).get_shape()) == p["shape"]
            else:
                assert not t.is_meta, name
                p["data"] = t.reshape(-1).tolist()
            spec.append(p)
    return m, spec


def make_fn(m):
    cfg = m.config

    def fn(params, ids, mask):
        n = ids.shape[1]
        p = torch.arange(n, device=ids.device)
        allowed = mask[:, None, None, :].bool()
        zeros = torch.zeros((), device=ids.device, dtype=torch.float32)
        neg = torch.full(
            (), torch.finfo(torch.float32).min, device=ids.device, dtype=torch.float32
        )
        full = torch.where(allowed, zeros, neg).expand(ids.shape[0], 1, n, n)
        window = (p[None, :] - p[:, None]).abs() <= cfg.sliding_window
        sliding = torch.where(allowed & window[None, None, :, :], zeros, neg)
        out = torch.func.functional_call(
            m,
            params,
            (),
            {
                "input_ids": ids,
                "attention_mask": {
                    "full_attention": full,
                    "sliding_attention": sliding,
                },
                "position_ids": p[None, :],
            },
        )
        x = out.last_hidden_state.float()
        weights = mask.unsqueeze(-1).float()
        pooled = (x * weights).sum(1) / weights.sum(1)
        return torch.nn.functional.normalize(pooled, p=2, dim=-1)

    return fn
