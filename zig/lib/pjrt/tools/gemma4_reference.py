# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent Transformers oracle; no imports from the PJRT graph adapter."""

import base64
import hashlib
import importlib.metadata
import inspect
from pathlib import Path

import torch


def official_source_identity():
    """Require the installed model source to match its distribution RECORD."""
    import transformers.models.gemma4.modeling_gemma4 as modeling

    distribution = importlib.metadata.distribution("transformers")
    entry = next(
        path
        for path in distribution.files
        if str(path) == "transformers/models/gemma4/modeling_gemma4.py"
    )
    data = distribution.locate_file(entry).read_bytes()
    actual = (
        base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
    )
    if entry.hash.mode != "sha256" or actual != entry.hash.value:
        raise ValueError("Transformers model source differs from its wheel RECORD")
    source_path = Path(modeling.__file__).resolve()
    for cls in (
        modeling.Gemma4RMSNorm,
        modeling.Gemma4AudioAttention,
        modeling.Gemma4TextDecoderLayer,
        modeling.Gemma4ForConditionalGeneration,
    ):
        method = inspect.unwrap(cls.forward)
        if Path(method.__code__.co_filename).resolve() != source_path:
            raise ValueError(
                "Transformers forward method was monkeypatched: " + cls.__name__
            )
    return {
        "version": distribution.version,
        "modeling_sha256": hashlib.sha256(data).hexdigest(),
        "forward_module": modeling.Gemma4ForConditionalGeneration.forward.__module__,
    }


def load_reference_model(config):
    from transformers import Gemma4Config, Gemma4ForConditionalGeneration
    from transformers.models.gemma4 import modeling_gemma4 as modeling

    official_source_identity()
    cfg = Gemma4Config(**config)
    cfg._attn_implementation = "eager"
    for sub in (cfg.text_config, cfg.vision_config, cfg.audio_config):
        sub._attn_implementation = "eager"
    with torch.device("meta"):
        model = Gemma4ForConditionalGeneration(cfg)
    model.eval().requires_grad_(False)
    model.model.language_model.rotary_emb = modeling.Gemma4TextRotaryEmbedding(
        cfg.text_config, device="cpu"
    )
    model.model.vision_tower.encoder.rotary_emb = modeling.Gemma4VisionRotaryEmbedding(
        cfg.vision_config, device="cpu"
    )
    model.model.audio_tower.rel_pos_enc = modeling.Gemma4AudioRelPositionalEncoding(
        cfg.audio_config
    )
    for module in model.modules():
        if hasattr(module, "scalar_embed_scale"):
            module.embed_scale = torch.tensor(
                module.scalar_embed_scale, dtype=torch.float32
            )
        if hasattr(module, "attention_logits_soft_cap") and hasattr(module, "softcap"):
            module.softcap = torch.tensor(
                module.attention_logits_soft_cap, dtype=torch.float32
            )
    return model


def reference_weight(name, value):
    # GGUF stores softplus(query_scale). Restore the parameter representation
    # expected by the unmodified official forward, which applies softplus itself.
    if name.endswith(".self_attn.per_dim_scale"):
        if not torch.isfinite(value).all() or not (value > 0).all():
            raise ValueError("Invalid folded GGUF audio query scale")
        return value + torch.log(-torch.expm1(-value))
    return value
