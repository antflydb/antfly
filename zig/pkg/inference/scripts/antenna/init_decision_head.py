#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build a Laya-format checkpoint: an Antenna trunk with a fresh decision head.

With ``--hf-encoder`` the trunk is a Hugging Face ModernBERT checkpoint instead
(LAYA.md roadmap 2d, a base-size student).

The Antenna student's encoder tensors already carry the names Laya's loader
expects (``encoder.embeddings.*``, ``encoder.layers.N.*``,
``encoder.final_norm.weight``), so they copy across unchanged; the GLiNER
heads and neck are dropped (the decision head reads raw trunk states, ANTENNA.md
decision 9). The decision head (``type_emb``, ``scorer``, ``act_head`` and
``head.layers``) is initialized as upstream Laya's PyTorch modules initialize
it, at the trunk's width. Train it with the trunk frozen
(``antfly-inference finetune train laya`` with ``freeze_layers`` set to
``num_hidden_layers + 1``), so the trunk stays shared with the GLiNER heads.

The decision settings (``head_layers``, ``max_len``, ``head_max_len``, action
costs, mask token) come from a released Laya checkpoint prepared by
``scripts/laya/prepare_laya.py``; temperatures reset to 1.

    python init_decision_head.py --student <antenna student dir> \\
        --laya <prepared laya dir> --output <dir outside Git>
    python init_decision_head.py --hf-encoder <ModernBERT dir> \\
        --laya <prepared laya dir> --output <dir outside Git>
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

HEAD_WIDTH = 256  # act_head hidden units, as upstream Laya
ACT_FEATURES = 4  # act_head reads the pooled state plus four decision features
TOKENIZER_FILES = ("tokenizer.json", "tokenizer_config.json", "special_tokens_map.json")


def decision_config(laya: dict[str, Any]) -> dict[str, Any]:
    kept = (
        "head_layers",
        "max_len",
        "head_max_len",
        "max_prefixes",
        "act_costs",
        "cost_wrong_act",
        "mask_token",
    )
    config = {key: laya[key] for key in kept if key in laya}
    config.update(
        encoder="antenna", model_name="antenna-decision", temperature=[1.0, 1.0, 1.0]
    )
    return config


def head_tensors(
    hidden: int, head_layers: int, n_act: int, seed: int
) -> dict[str, Any]:
    import torch
    from torch import nn

    torch.manual_seed(seed)
    tensors: dict[str, Any] = {"type_emb.weight": nn.Embedding(3, hidden).weight}
    scorer = nn.Sequential(
        nn.LayerNorm(hidden), nn.Linear(hidden, hidden), nn.GELU(), nn.Linear(hidden, 1)
    )
    act = nn.Sequential(
        nn.Linear(hidden + ACT_FEATURES, HEAD_WIDTH),
        nn.ReLU(),
        nn.Linear(HEAD_WIDTH, n_act),
    )
    for prefix, module in (("scorer", scorer), ("act_head", act)):
        tensors |= {
            f"{prefix}.{name}": value for name, value in module.state_dict().items()
        }
    for index in range(head_layers):
        layer = nn.TransformerEncoderLayer(
            hidden, hidden // 64, dim_feedforward=4 * hidden, batch_first=True
        )
        tensors |= {
            f"head.layers.{index}.{name}": value
            for name, value in layer.state_dict().items()
        }
    return {
        name: value.detach().float().contiguous() for name, value in tensors.items()
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    from safetensors.torch import load_file, save_file

    source = args.student or args.hf_encoder
    config_path = (
        args.student / "encoder_config" / "config.json"
        if args.student
        else args.hf_encoder / "config.json"
    )
    encoder_config = json.loads(config_path.read_text(encoding="utf-8"))
    if encoder_config.get("model_type") != "modernbert":
        raise ValueError(f"{source} is not a ModernBERT encoder")
    laya_source = json.loads((args.laya / "config.json").read_text(encoding="utf-8"))[
        "laya"
    ]
    laya = decision_config(laya_source)
    weights = load_file(str(source / "model.safetensors"))
    # An Antenna student names its trunk `encoder.*`; a Hugging Face
    # ModernBERT checkpoint names it `model.*` next to its masked-LM head.
    prefix = "encoder." if args.student else "model."
    tensors = {
        "encoder." + name[len(prefix) :]: value.float().contiguous()
        for name, value in weights.items()
        if name.startswith(prefix)
    }
    vocab = tensors["encoder.embeddings.tok_embeddings.weight"].shape[0]
    hidden = encoder_config["hidden_size"]
    if hidden % 64:
        raise ValueError(
            f"hidden size {hidden} is not a multiple of 64 (decision-head attention heads are 64 wide)"
        )
    n_act = len(laya.get("act_costs", {})) + 1
    tensors |= head_tensors(hidden, laya["head_layers"], n_act, args.seed)
    config = dict(
        encoder_config, architectures=["ModernBertModel"], vocab_size=vocab, laya=laya
    )

    args.output.mkdir(parents=True, exist_ok=False)
    save_file(tensors, str(args.output / "model.safetensors"))
    (args.output / "config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )
    (args.output / "rl_agent_config.json").write_text(
        json.dumps(laya, indent=2) + "\n", encoding="utf-8"
    )
    manifest = json.loads(
        (args.laya / "model_manifest.json").read_text(encoding="utf-8")
    )
    manifest["source"] = {
        ("antenna_student" if args.student else "hf_encoder"): str(source.resolve()),
        "decision_config_from": manifest.get("source"),
    }
    (args.output / "model_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    for name in TOKENIZER_FILES:
        if (source / name).exists():
            shutil.copyfile(source / name, args.output / name)
    return {
        "output": str(args.output),
        "encoder_tensors": sum(n.startswith("encoder.") for n in tensors),
        "head_tensors": sum(not n.startswith("encoder.") for n in tensors),
        "hidden": hidden,
        "vocab": vocab,
        "n_act": n_act,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    encoder = parser.add_mutually_exclusive_group(required=True)
    encoder.add_argument(
        "--student",
        type=Path,
        help="Antenna student (GLiNER2.5 boundary export)",
    )
    encoder.add_argument(
        "--hf-encoder",
        type=Path,
        help="Hugging Face ModernBERT checkpoint (e.g. answerdotai/ModernBERT-base)",
    )
    parser.add_argument(
        "--laya",
        type=Path,
        required=True,
        help="prepared released Laya dir, for the decision settings",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260930)
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
