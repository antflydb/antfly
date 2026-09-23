#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<6", "safetensors>=0.5", "numpy>=2"]
# ///
"""Generate Laya forward, objective, and parameter-gradient reference artifacts.

First run laya_reference.py, then pass its output as --fixture and the same
pinned upstream common.py as --common. No pretrained weights are downloaded.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

import torch
from safetensors.torch import load_file, save_file
from transformers import ModernBertConfig, ModernBertModel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--common", type=Path, required=True)
    parser.add_argument(
        "--precision",
        choices=("float32", "float64"),
        default="float32",
        help="Encoder/head arithmetic; upstream decision logits and loss remain FP32",
    )
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    torch.set_num_threads(1)
    torch.manual_seed(715)
    source = args.fixture / "upstream"
    if source.is_dir():
        cfg = ModernBertConfig.from_pretrained(source / "encoder")
        decision = json.loads((source / "rl_agent_config.json").read_text())
        head_layers = decision["head_layers"]
        weights = source / "model.safetensors"
    else:
        raw = json.loads((args.fixture / "model/config.json").read_text())
        decision = raw.pop("laya")
        cfg = ModernBertConfig.from_dict(raw)
        head_layers = decision["head_layers"]
        weights = args.fixture / "model/model.safetensors"
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg),
        head_layers=head_layers,
        n_act=len(decision["act_costs"]) + 1,
    ).eval()
    model.load_state_dict(load_file(weights), strict=True)
    if args.precision == "float64":
        model.double()
        # Upstream explicitly casts the pooled action features to FP32.
        # This auxiliary head is not supervised by the decision objective.
        model.act_head.float()
    activations = {}

    def capture(name):
        def hook(module, inputs, output):
            value = output[0] if isinstance(output, tuple) else output
            activations[name] = value.detach().float().contiguous()

        return hook

    for name, module in model.named_modules():
        if (
            name in ("encoder.embeddings.norm", "encoder.final_norm")
            or (name.startswith("encoder.layers.") and name.count(".") == 2)
            or (
                name.startswith("head.layers.")
                and (name.count(".") == 2 or name.endswith(".linear1"))
            )
        ):
            module.register_forward_hook(capture(name))
    fixture = json.loads((args.fixture / "reference.json").read_text())
    batch = common.collate_items([fixture["sequences"]], 0)
    logits, _ = model(
        *(
            batch[key]
            for key in (
                "input_ids",
                "attention_mask",
                "marker_pos",
                "marker_mask",
                "qtype",
            )
        )
    )
    logits.retain_grad()
    target = torch.tensor([[0.1, 0.7, 0.2], [0.2, 0.3, 0.5], [0.25, 0.75, 0]])
    noise = torch.randn(4, *logits.shape)
    mask = batch["marker_mask"]
    sigma = 0.4
    eps = noise * sigma * mask
    eps = (eps - eps.sum(-1, keepdim=True) / mask.sum(-1, keepdim=True)) * mask
    z = logits.detach().unsqueeze(0) + eps
    q = torch.softmax(z.masked_fill(~mask, -1e4), -1)
    with torch.no_grad():
        reward = common.proper_reward(
            q, target.unsqueeze(0), batch["qtype"], mask, w_sph=0.75, w_rps=1.0
        )
        advantage = reward - reward.mean(0, keepdim=True)
        advantage = advantage / (advantage.std() + 1e-6)
    logp = -(((z - logits.unsqueeze(0)) ** 2) * mask).sum(-1) / (2 * sigma**2)
    policy = -(advantage * logp).mean()
    ce = (
        -(target * torch.log_softmax(logits.masked_fill(~mask, -1e4), -1))
        .sum(-1)
        .mean()
    )
    loss = policy + ce
    loss.backward()
    save_file(activations, args.fixture / "activations.safetensors")
    save_file(
        {
            name: p.grad.float().contiguous()
            for name, p in model.named_parameters()
            if p.grad is not None
        },
        args.fixture / "gradients.safetensors",
    )
    (args.fixture / "training_reference.json").write_text(
        json.dumps(
            {
                "sequences": fixture["sequences"],
                "logits": logits.detach().tolist(),
                "targets": target.tolist(),
                "noise": noise.flatten().tolist(),
                "loss": loss.item(),
                "ce": ce.item(),
                "policy": policy.item(),
                "reward": reward.mean().item(),
                "cotangent": logits.grad.flatten().tolist(),
            },
            indent=2,
        )
        + "\n"
    )
    questions = [
        (
            "choice",
            "which tool is needed?",
            ["search", "fetch", "none"],
            [0.1, 0.7, 0.2],
        ),
        ("score", "urgency?", ["low", "medium", "high"], [0.2, 0.3, 0.5]),
        ("noul", "is search needed?", ["false", "true"], [0.25, 0.75]),
    ]
    for split, state in (
        ("train", "please find the document"),
        ("eval", "urgent hello world"),
    ):
        records = [
            {
                "id": f"{split}/{kind}",
                "group_id": split,
                "text": state,
                "kind": kind,
                "instruction": instruction,
                "labels": labels,
                "target": target,
            }
            for kind, instruction, labels, target in questions
        ]
        (args.fixture / f"{split}.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in records)
        )

    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    (args.fixture / "oracle_metadata.json").write_text(
        json.dumps(
            {
                "encoder_head_precision": args.precision,
                "decision_logits_precision": "float32",
                "torch_version": torch.__version__,
                "threads": torch.get_num_threads(),
                "weights_sha256": digest(weights),
                "common_sha256": digest(args.common),
                "sequence_reference_sha256": digest(args.fixture / "reference.json"),
            },
            indent=2,
        )
        + "\n"
    )
    print(
        json.dumps(
            {
                "precision": args.precision,
                "loss": loss.item(),
                "gradient_tensors": sum(p.grad is not None for p in model.parameters()),
            }
        )
    )


if __name__ == "__main__":
    main()
