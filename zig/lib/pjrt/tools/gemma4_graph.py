# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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


def load_multimodal_meta(config, precision_policy="bf16"):
    """Official modules; only GGUF representation and static export adapters."""
    import textwrap

    import transformers.models.gemma4.modeling_gemma4 as mm
    from transformers import Gemma4Config, Gemma4ForConditionalGeneration

    cfg = Gemma4Config(**config)
    cfg._attn_implementation = "eager"
    for sub in (cfg.text_config, cfg.vision_config, cfg.audio_config):
        sub._attn_implementation = "eager"
    patch_casts()
    if not getattr(mm.Gemma4AudioAttention, "_gguf_query_scale", False):
        source = textwrap.dedent(inspect.getsource(mm.Gemma4AudioAttention.forward))
        if source.count("F.softplus(self.per_dim_scale)") != 1:
            raise ValueError("Unsupported audio query-scale implementation")
        source = source.replace(
            "F.softplus(self.per_dim_scale)", "self.per_dim_scale.float()"
        )
        scope = dict(vars(mm))
        # Execute only the installed official method, after the exact adapter check.
        exec(source, scope)  # noqa: S102
        mm.Gemma4AudioAttention.forward = scope["forward"]
        mm.Gemma4AudioAttention._gguf_query_scale = True
    with torch.device("meta"):
        model = Gemma4ForConditionalGeneration(cfg)
    model.eval().requires_grad_(False)
    model.model.language_model.rotary_emb = mm.Gemma4TextRotaryEmbedding(
        cfg.text_config, device="cpu"
    )
    model.model.vision_tower.encoder.rotary_emb = mm.Gemma4VisionRotaryEmbedding(
        cfg.vision_config, device="cpu"
    )
    model.model.audio_tower.rel_pos_enc = mm.Gemma4AudioRelPositionalEncoding(
        cfg.audio_config
    )
    for mod in model.modules():
        if hasattr(mod, "scalar_embed_scale"):
            mod.embed_scale = torch.tensor(mod.scalar_embed_scale, dtype=torch.float32)
        if hasattr(mod, "attention_logits_soft_cap") and hasattr(mod, "softcap"):
            mod.softcap = torch.tensor(
                mod.attention_logits_soft_cap, dtype=torch.float32
            )
    if precision_policy == "f32-residual-media":
        enable_f32_text_residuals(model)
    elif precision_policy != "bf16":
        raise ValueError("Unknown graph precision policy")
    return model


def enable_f32_text_residuals(model):
    """PR #1404's separate residual publication policy, with BF16 matrix inputs.

    This changes only the export candidate. The independent Transformers oracle
    remains unmodified. Media tower parameter precision is selected by the exporter.
    """
    import types

    def forward(
        self,
        hidden_states,
        per_layer_input=None,
        shared_kv_states=None,
        position_embeddings=None,
        attention_mask=None,
        position_ids=None,
        past_key_values=None,
        **kwargs,
    ):
        dtype = self.self_attn.q_proj.weight.dtype
        residual = hidden_states.float()
        hidden = self.input_layernorm(hidden_states).to(dtype=dtype)
        hidden, _ = self.self_attn(
            hidden_states=hidden,
            position_embeddings=position_embeddings,
            attention_mask=attention_mask,
            shared_kv_states=shared_kv_states,
            position_ids=position_ids,
            past_key_values=past_key_values,
            **kwargs,
        )
        hidden = residual + self.post_attention_layernorm(hidden).float()
        residual = hidden
        branch = self.pre_feedforward_layernorm(hidden).to(dtype=dtype)
        branch = self.post_feedforward_layernorm(self.mlp(branch))
        hidden = residual + branch.float()
        if self.hidden_size_per_layer_input:
            residual = hidden
            branch = self.act_fn(self.per_layer_input_gate(hidden.to(dtype=dtype)))
            branch = self.per_layer_projection(branch * per_layer_input.to(dtype=dtype))
            hidden = residual + self.post_per_layer_input_norm(branch).float()
        return hidden * self.layer_scalar.float()

    for layer in model.model.language_model.layers:
        if layer.enable_moe_block:
            raise ValueError("The residual precision adapter qualifies dense E4B only")
        layer.forward = types.MethodType(forward, layer)


def state_shapes(config, capacity):
    result = []
    for i, layer in enumerate(
        config.per_layer_config[
            : config.num_hidden_layers - config.num_kv_shared_layers
        ]
    ):
        cap = (
            min(capacity, config.sliding_window)
            if config.layer_types[i] == "sliding_attention"
            else capacity
        )
        result.extend([[1, layer.num_key_value_heads, cap, layer.head_dim]] * 2)
    return result


def media_metadata(model, inputs):
    import numpy as np

    ids = inputs["input_ids"][0].numpy()
    result = {
        "keys": ["input_ids"] + sorted(k for k in inputs if k != "input_ids"),
        "slots": {},
    }
    for kind in ("image", "video", "audio"):
        slots = np.flatnonzero(
            ids == getattr(model.config, kind + "_token_id")
        ).tolist()
        result["slots"][kind] = slots
    for kind in ("image", "video"):
        key = "image_position_ids" if kind == "image" else "video_position_ids"
        if key in inputs:
            positions = inputs[key].numpy()
            result[kind + "_counts"] = ((positions != -1).all(-1).sum(-1) // 9).tolist()
    if "input_features_mask" in inputs:
        result["audio_valid"] = np.flatnonzero(
            inputs["input_features_mask"][0, ::4].numpy()
        ).tolist()
    return result


def _embed_media(model, values, meta):
    mm = model.model
    ids = values["input_ids"]
    text = mm.language_model
    modal = (
        (ids == model.config.image_token_id)
        | (ids == model.config.audio_token_id)
        | (ids == model.config.video_token_id)
    )
    hard = torch.where(modal, model.config.text_config.pad_token_id, ids)
    embeddings = text.embed_tokens(hard)
    # Token-identity PLE is computed from hard IDs before soft media insertion;
    # the context projection is subsequently computed from the merged embeddings.
    identity = text.get_per_layer_inputs(hard, embeddings)
    for kind in ("image", "video"):
        slots = meta["slots"][kind]
        if not slots:
            continue
        pixels = values["pixel_values" if kind == "image" else "pixel_values_videos"]
        positions = values[
            "image_position_ids" if kind == "image" else "video_position_ids"
        ]
        tower = mm.vision_tower
        padding = (positions == -1).all(-1)
        hidden = tower.patch_embedder(pixels, positions, padding)
        mask = torch.where(
            (~padding)[:, None, None, :], 0.0, torch.finfo(torch.float32).min
        )
        rotary = tower.encoder.rotary_emb(hidden, positions)
        for layer in tower.encoder.layers:
            hidden = layer(
                hidden,
                attention_mask=mask,
                position_embeddings=rotary,
                position_ids=positions,
            )
        hidden, _ = tower.pooler(hidden, positions, padding, pixels.shape[1] // 9)
        if tower.config.standardize:
            hidden = (hidden - tower.std_bias.float()) * tower.std_scale.float()
        selected = torch.cat(
            [hidden[b, :count] for b, count in enumerate(meta[kind + "_counts"])], dim=0
        ).unsqueeze(0)
        projected = mm.embed_vision(
            selected.to(mm.embed_vision.embedding_projection.weight.dtype)
        )
        if projected.shape[1] != len(slots):
            raise ValueError(("vision slot geometry", projected.shape, len(slots)))
        embeddings = embeddings.index_copy(
            1, torch.tensor(slots, device=ids.device), projected.to(embeddings.dtype)
        )
    if meta["slots"]["audio"]:
        tower = mm.audio_tower
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
            hidden = layer(hidden, attention_mask=blocked, position_embeddings=rotary)
        hidden = tower.output_proj(hidden)
        projected = mm.embed_audio(hidden)[
            :, torch.tensor(meta["audio_valid"], device=ids.device)
        ]
        if projected.shape[1] != len(meta["slots"]["audio"]):
            raise ValueError("Audio slot geometry mismatch")
        embeddings = embeddings.index_copy(
            1,
            torch.tensor(meta["slots"]["audio"], device=ids.device),
            projected.to(embeddings.dtype),
        )
    return embeddings, identity


def make_multimodal_step(model, capacity, meta, prefill, logits=False):
    """Functional KV, with bounded unordered sliding rings and full donor state."""
    import types

    from transformers.cache_utils import Cache, StaticLayer

    cfg = model.config.text_config
    donors = cfg.num_hidden_layers - cfg.num_kv_shared_layers

    def forward(self, inputs, state, position):
        values = (
            dict(zip(meta["keys"], inputs)) if prefill else {"input_ids": inputs[0]}
        )
        ids = values["input_ids"]
        n = ids.shape[1]

        class Layer(StaticLayer):
            def update(self, keys, vals, *args, **kwargs):
                cap = self.keys.shape[2]
                if prefill:
                    keep = min(n, cap)
                    slots = (
                        torch.arange(n - keep, n, device=ids.device) + position
                    ) % cap
                    self.keys = self.keys.index_copy(2, slots, keys[:, :, -keep:])
                    self.values = self.values.index_copy(2, slots, vals[:, :, -keep:])
                    return keys, vals
                slots = position.reshape(1) % cap
                self.keys = self.keys.index_copy(2, slots, keys)
                self.values = self.values.index_copy(2, slots, vals)
                return self.keys, self.values

        layers = [Layer(max_cache_len=capacity) for _ in range(cfg.num_hidden_layers)]
        for i in range(donors):
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
        pos = (torch.arange(n, device=ids.device) + position)[None]
        if prefill:
            keys = torch.arange(n, device=ids.device)[None, None, :] + position
            allowed = keys <= pos[:, :, None]
            sliding = allowed & (keys > pos[:, :, None] - cfg.sliding_window)
        else:
            keys = torch.arange(capacity, device=ids.device)[None, None, :]
            allowed = keys <= pos[:, :, None]
            ring = min(capacity, cfg.sliding_window)
            slots = torch.arange(ring, device=ids.device)
            absolute = position - ((position - slots) % ring)
            sliding = ((absolute >= 0) & (absolute > position - cfg.sliding_window))[
                None, None, :
            ]
        negative = torch.finfo(torch.float32).min
        masks = {
            "full_attention": torch.where(allowed, 0.0, negative)[:, None],
            "sliding_attention": torch.where(sliding, 0.0, negative)[:, None],
        }
        embeddings, identity = (
            _embed_media(self, values, meta)
            if prefill
            else (
                self.model.language_model.embed_tokens(ids),
                self.model.language_model.get_per_layer_inputs(ids),
            )
        )
        output = self.model.language_model(
            inputs_embeds=embeddings,
            per_layer_inputs=identity,
            attention_mask=masks,
            position_ids=pos,
            past_key_values=cache,
            use_cache=True,
            logits_to_keep=1,
        )
        result = self.lm_head(
            output.last_hidden_state[:, -1:].to(dtype=self.lm_head.weight.dtype)
        )
        cap = cfg.final_logit_softcapping
        if cap is not None:
            result = torch.tanh(result / cap) * cap
        state = tuple(
            v for layer in layers[:donors] for v in (layer.keys, layer.values)
        )
        return (
            (result[:, 0].float() if logits else result[:, 0].argmax(-1).unsqueeze(1)),
            state,
            position + n,
        )

    model.forward = types.MethodType(forward, model)

    def call(params, state, inputs, position):
        model.forward = types.MethodType(forward, model)
        return torch.func.functional_call(model, params, (inputs, state, position))

    return call


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
        kwargs = {
            "input_ids": ids,
            "position_ids": positions,
            "attention_mask": {"full_attention": full, "sliding_attention": sliding},
            "use_cache": True,
            "logits_to_keep": 1,
        }
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
