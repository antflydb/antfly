#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<5", "safetensors>=0.5", "numpy>=2"]
# ///
"""Independent PyTorch oracle for tree-packed Laya (zig/pkg/inference/models/laya/LAYA.md).

Run after laya_reference.py on the same fixture directory:

    uv run --script laya_packed_reference.py --fixture <dir> --common <common.py>

The packer and the masked forward below are a second implementation of the
layout, written from the design rather than from the Zig code. Before writing
anything the script checks that a one-segment tree reproduces upstream
`DecisionModel` on the unpacked sequence. It then writes, per packing mode,
`packed-<mode>/` (the fixture model with `laya.packing` set) and
`packed_reference.json` with the rows, logits, and action logits.
"""

import argparse
import importlib.util
import json
import math
import shutil
from pathlib import Path

import torch
from safetensors.torch import load_file
from transformers import ModernBertConfig, ModernBertModel, PreTrainedTokenizerFast

MAX_OPTION_TOKENS = 48
QTYPES = {"choice": 0, "score": 1, "noul": 2}

QUESTIONS = [
    {
        "name": "tool",
        "t": "choice",
        "ins": "which tool is needed?",
        "labels": ["search", "fetch", "none"],
        "desc": ["", "", ""],
    },
    {
        "name": "urgency",
        "t": "score",
        "ins": "urgency?",
        "labels": ["low", "medium", "high"],
        "desc": ["", "", ""],
    },
    {
        "name": "needed",
        "t": "noul",
        "ins": "is search needed?",
        "labels": ["false", "true"],
        "desc": ["", ""],
    },
]
STATES = ["please find the document", "urgent hello world"]


def option_text(q, i):
    label, desc = q["labels"][i], q["desc"][i]
    if q["t"] == "choice":
        return f" {label}" if not desc else f" {label}: {desc}"
    if q["t"] == "score":
        return f" level {i}: {desc or label}"
    default = (
        "no, the statement does not hold" if i == 0 else "yes, the statement holds"
    )
    return f" {label}: {desc or default}"


def encode(tok, text):
    clean = text.replace(tok.mask_token, " ")
    return tok(clean, add_special_tokens=False)["input_ids"]


def question_tokens(tok, decision, q):
    """Upstream's shared head_max_len budget (common.build_sequence)."""
    head_max_len = decision["head_max_len"]
    head = encode(tok, f"{q['t']} question: {q['ins']}")
    options = [encode(tok, option_text(q, i)) for i in range(len(q["labels"]))]
    options_len = sum(1 + min(len(o), MAX_OPTION_TOKENS) for o in options)
    per = (
        max(4, (head_max_len - 16) // len(options))
        if options_len + 16 > head_max_len
        else MAX_OPTION_TOKENS + 1
    )
    runs = [min(1 + min(len(o), MAX_OPTION_TOKENS), per) for o in options]
    head_len = min(len(head), max(8, head_max_len - sum(runs)))
    return head, options, runs, head_len


def pack(tok, decision, mode, state, questions):
    """One row: trunk [CLS] state [SEP]; per question [CLS] head [SEP] then
    options in the question branch (question mode, closed by [SEP]) or one
    [MASK] option branch per label (candidate mode)."""
    row = {
        k: []
        for k in ("ids", "positions", "segments", "parents", "kinds", "anchors")
    }
    markers = []

    def put(token, segment, kind, position):
        row["ids"].append(token)
        row["positions"].append(position)
        row["segments"].append(segment)
        row["kinds"].append(kind)
        return position + 1

    row["parents"].append(-1)
    position = 0
    for token in [tok.cls_token_id, *encode(tok, state), tok.sep_token_id]:
        position = put(token, 0, -1, position)
    trunk = position
    for q in questions:
        kind = QTYPES[q["t"]]
        head, options, runs, head_len = question_tokens(tok, decision, q)
        question_segment = len(row["parents"])
        row["parents"].append(0)
        row["anchors"].append(len(row["ids"]))
        position = trunk
        if mode == "candidate":
            head_len = min(len(head), decision["head_max_len"])
        for token in [tok.cls_token_id, *head[:head_len], tok.sep_token_id]:
            position = put(token, question_segment, kind, position)
        start = position
        own = []
        for i, option in enumerate(options):
            segment = question_segment
            if mode == "candidate":
                segment = len(row["parents"])
                row["parents"].append(question_segment)
                position = start
                run = 1 + min(len(option), MAX_OPTION_TOKENS)
            else:
                run = runs[i]
            own.append(len(row["ids"]))
            for token in [tok.mask_token_id, *option[: run - 1]]:
                position = put(token, segment, kind, position)
        if mode == "question":
            position = put(tok.sep_token_id, question_segment, kind, position)
        markers.append(own)
    width = max(2, *(len(m) for m in markers))
    row["markers"] = [m + [-1] * (width - len(m)) for m in markers]
    row["width"] = width
    return row


def visibility(row):
    parents, segments = row["parents"], row["segments"]
    ancestors = []
    for s in range(len(parents)):
        chain, cur = set(), s
        while cur >= 0:
            chain.add(cur)
            cur = parents[cur]
        ancestors.append(chain)
    seg = torch.tensor(segments)
    return torch.tensor(
        [[int(segments[k]) in ancestors[int(s)] for k in range(len(seg))] for s in seg]
    )


def rope(x, positions, theta):
    d = x.shape[-1]
    inv = 1.0 / (theta ** (torch.arange(0, d, 2, dtype=torch.float32) / d))
    angle = positions.float()[:, None] * inv[None, :]
    cos, sin = angle.cos()[:, None, :], angle.sin()[:, None, :]
    x1, x2 = x[..., : d // 2], x[..., d // 2 :]
    return torch.cat([x1 * cos - x2 * sin, x1 * sin + x2 * cos], -1)


@torch.no_grad()
def packed_forward(model, cfg, row):
    """Masked encoder + decision head for one row, from the design."""
    enc = model.encoder
    ids = torch.tensor(row["ids"])[None]
    positions = torch.tensor(row["positions"])
    visible = visibility(row)
    n, hidden = ids.shape[1], cfg.hidden_size
    heads = cfg.num_attention_heads
    head_dim = hidden // heads
    window = (
        positions[:, None] - positions[None, :]
    ).abs() <= cfg.local_attention // 2
    h = enc.embeddings(input_ids=ids)[0]
    for i, layer in enumerate(enc.layers):
        is_global = i % cfg.global_attn_every_n_layers == 0
        allowed = visible if is_global else visible & window
        theta = cfg.global_rope_theta if is_global else cfg.local_rope_theta
        qkv = layer.attn.Wqkv(layer.attn_norm(h)).view(n, 3, heads, head_dim)
        q = rope(qkv[:, 0], positions, theta)
        k = rope(qkv[:, 1], positions, theta)
        scores = torch.einsum("qhd,khd->hqk", q, k) / math.sqrt(head_dim)
        probs = scores.masked_fill(~allowed[None], float("-inf")).softmax(-1)
        attended = torch.einsum("hqk,khd->qhd", probs, qkv[:, 2]).reshape(n, hidden)
        h = h + layer.attn.Wo(attended)
        h = h + layer.mlp(layer.mlp_norm(h))
    h = enc.final_norm(h)
    kinds = torch.tensor(row["kinds"])
    types = torch.zeros(n, hidden)
    types[kinds >= 0] = model.type_emb.weight[kinds[kinds >= 0]]
    h = h + types
    mask = torch.zeros(n, n).masked_fill(~visible, float("-inf"))
    for layer in model.head.layers:
        h = layer(h[None], src_mask=mask)[0]
    markers = torch.tensor(row["markers"])
    valid = markers >= 0
    logits = model.scorer(h[markers.clamp(min=0)]).squeeze(-1).float()
    logits = logits.masked_fill(~valid, -1e4)
    p = logits.softmax(-1)
    count = valid.sum(-1).clamp(min=2).float()
    entropy = -(p * p.clamp_min(1e-9).log()).sum(-1) / count.log()
    top2 = p.topk(2, -1).values
    features = torch.stack([top2[:, 0], top2[:, 0] - top2[:, 1], entropy, count / 255.0], -1)
    pooled = h[torch.tensor(row["anchors"])].float()
    actions = model.act_head(torch.cat([pooled, features], -1))
    return logits, actions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--common", type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    torch.set_num_threads(1)
    model_dir = args.fixture / "model"
    raw = json.loads((model_dir / "config.json").read_text())
    decision = raw.pop("laya")
    cfg = ModernBertConfig.from_dict(raw)
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg),
        head_layers=decision["head_layers"],
        n_act=len(decision.get("act_costs", {})) + 1,
    ).eval()
    model.load_state_dict(load_file(model_dir / "model.safetensors"), strict=True)
    tok = PreTrainedTokenizerFast.from_pretrained(model_dir)

    # A one-segment tree must reproduce upstream DecisionModel exactly.
    worst = 0.0
    for state in STATES:
        for q in QUESTIONS:
            upstream_q = {
                "t": q["t"],
                "ins": q["ins"],
                "crit": {label: None for label in q["labels"]}
                if q["t"] == "choice"
                else (q["labels"] if q["t"] == "score" else None),
            }
            ids, markers = common.build_sequence(
                tok, state, upstream_q, decision["max_len"], decision["head_max_len"]
            )
            batch = common.collate_items(
                [[{"ids": ids, "markers": markers, "qtype": QTYPES[q["t"]]}]],
                tok.pad_token_id,
            )
            with torch.no_grad():
                want_logits, want_actions = model(
                    *(
                        batch[k]
                        for k in (
                            "input_ids",
                            "attention_mask",
                            "marker_pos",
                            "marker_mask",
                            "qtype",
                        )
                    )
                )
            flat = {
                "ids": ids,
                "positions": list(range(len(ids))),
                "segments": [0] * len(ids),
                "parents": [-1],
                "kinds": [QTYPES[q["t"]]] * len(ids),
                "anchors": [0],
                "markers": [markers],
                "width": len(markers),
            }
            logits, actions = packed_forward(model, cfg, flat)
            worst = max(
                worst,
                (logits - want_logits).abs().max().item(),
                (actions - want_actions).abs().max().item(),
            )
    print(json.dumps({"one_segment_vs_upstream_max_error": worst}))
    if worst > 1e-5:
        raise SystemExit("packed oracle disagrees with upstream DecisionModel")

    reference = {"one_segment_vs_upstream_max_error": worst, "modes": {}}
    for mode in ("question", "candidate"):
        target = args.fixture / f"packed-{mode}"
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(model_dir, target)
        config = json.loads((target / "config.json").read_text())
        config["laya"]["packing"] = {"mode": mode, "max_packed_len": 512}
        (target / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        rows = []
        for state in STATES:
            row = pack(tok, decision, mode, state, QUESTIONS)
            logits, actions = packed_forward(model, cfg, row)
            rows.append(
                {
                    "state": state,
                    "row": row,
                    "logits": logits.tolist(),
                    "action_logits": actions.tolist(),
                }
            )
        reference["modes"][mode] = rows
    reference["questions"] = QUESTIONS
    reference["torch_version"] = torch.__version__
    (args.fixture / "packed_reference.json").write_text(
        json.dumps(reference, indent=2) + "\n"
    )
    print(args.fixture / "packed_reference.json")


if __name__ == "__main__":
    main()
