#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The Antenna GLiNER neck for the upstream PyTorch oracle.

An Antenna student may carry a neck: a learned linear map from the trunk's
final hidden states into the space the boundary heads read, applied to every
token before routing (``models.gliner_boundary.Neck`` in the native runtime).
Its config declares ``"antenna_neck": "linear"`` and its weights carry
``gliner_neck.weight`` ``[hidden, hidden]`` and ``gliner_neck.bias``.

Upstream GLiNER2 loads state dicts strictly and knows no neck, so ``load``
removes the neck tensors before upstream's load and ``attach`` installs them
as a ``gliner_neck`` module on the encoder's output. The module is a
registered submodule, so ``save_pretrained`` writes the tensors back and
upstream's trainer optimizes them in the task group (its name has no
"encoder").
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

NECK = "gliner_neck"


def declared(model_dir: Path) -> str | None:
    value = json.loads((Path(model_dir) / "config.json").read_text(encoding="utf-8")).get("antenna_neck")
    if value not in (None, "linear"):
        raise ValueError(f"unsupported antenna_neck: {value!r}")
    return value


def attach(model: Any, weight: Any, bias: Any) -> Any:
    """Install a linear neck between ``model.encoder`` and the heads."""
    import torch

    if getattr(model, NECK, None) is not None:
        raise ValueError("model already has a neck")
    parameter = next(model.parameters())
    neck = torch.nn.Linear(weight.shape[1], weight.shape[0]).to(device=parameter.device, dtype=parameter.dtype)
    with torch.no_grad():
        neck.weight.copy_(weight)
        neck.bias.copy_(bias)
    model.add_module(NECK, neck)
    encoder_forward = model.encoder.forward

    def forward(*args: Any, **kwargs: Any) -> Any:
        out = encoder_forward(*args, **kwargs)
        out.last_hidden_state = getattr(model, NECK)(out.last_hidden_state)
        return out

    model.encoder.forward = forward
    model.config.antenna_neck = "linear"
    return model


def identity(hidden: int) -> tuple[Any, Any]:
    import torch

    return torch.eye(hidden), torch.zeros(hidden)


def load(model_dir: Path | str, **kwargs: Any) -> Any:
    """``AutoExtractor.from_pretrained`` for checkpoints with or without a neck."""
    from gliner2 import AutoExtractor
    import gliner2.models.boundary.model as boundary_model

    if declared(Path(model_dir)) is None:
        return AutoExtractor.from_pretrained(str(model_dir), **kwargs)
    upstream_load = boundary_model.load_checkpoint_state_dict
    captured: dict[str, Any] = {}

    def without_neck(*args: Any, **inner: Any) -> Any:
        state = upstream_load(*args, **inner)
        for name in [name for name in state if name.startswith(NECK + ".")]:
            captured[name] = state.pop(name)
        return state

    boundary_model.load_checkpoint_state_dict = without_neck
    try:
        model = AutoExtractor.from_pretrained(str(model_dir), **kwargs)
    finally:
        boundary_model.load_checkpoint_state_dict = upstream_load
    if set(captured) != {NECK + ".weight", NECK + ".bias"}:
        raise ValueError(f"{model_dir}: antenna_neck declared but tensors are {sorted(captured)}")
    return attach(model, captured[NECK + ".weight"], captured[NECK + ".bias"])
