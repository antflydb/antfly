# Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<5", "safetensors>=0.5", "numpy>=2"]
# ///
"""Three-question browser smoke oracle, using the same upstream model as PR #815.

Pass an already-downloaded upstream model directory and common.py pinned at
https://github.com/NandhaKishorM/laya/blob/6a5819129eb220570792e417e49723d697efd76f/laya/common.py.
This is numerical integration coverage, not a task-accuracy qualification.
"""

import argparse
import importlib.util
import json
from pathlib import Path

import torch
from safetensors.torch import load_file
from transformers import AutoTokenizer, ModernBertConfig, ModernBertModel

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--model", type=Path, required=True)
parser.add_argument("--common", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument(
    "--long",
    action="store_true",
    help="Exercise attention beyond the 128-token local window",
)
args = parser.parse_args()
torch.set_num_threads(2)
spec = importlib.util.spec_from_file_location("laya_common", args.common)
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
raw = json.loads((args.model / "encoder/config.json").read_text())
cfg = json.loads((args.model / "rl_agent_config.json").read_text())
for kind, key in (
    ("full_attention", "global_rope_theta"),
    ("sliding_attention", "local_rope_theta"),
):
    raw[key] = raw["rope_parameters"][kind]["rope_theta"]
encoder = ModernBertConfig.from_dict(raw)
encoder.reference_compile = False
encoder._attn_implementation = "eager"
model = common.DecisionModel(
    ModernBertModel(encoder), cfg["head_layers"], len(cfg["act_costs"]) + 1
).eval()
model.load_state_dict(load_file(args.model / "model.safetensors"), strict=True)
tok = AutoTokenizer.from_pretrained(args.model / "tokenizer", local_files_only=True)
questions = [
    {
        "t": "choice",
        "ins": "Which tool is needed to handle this request?",
        "crit": dict.fromkeys(["search", "fetch", "none"]),
    },
    {
        "t": "score",
        "ins": "How urgent is this request?",
        "crit": ["low", "medium", "high"],
    },
    {
        "t": "noul",
        "ins": "Does this request require searching for information?",
        "crit": None,
    },
]
text = "Please search for the latest documentation about browser inference."
if args.long:
    text = "Archived notes discuss browser inference and model loading. " * 24 + text
rows = []
with torch.inference_mode():
    for q in questions:
        ids, markers = common.build_sequence(
            tok, text, q, cfg["max_len"], cfg["head_max_len"]
        )
        qt = common.QTYPES[q["t"]]
        batch = common.collate_items(
            [[{"ids": ids, "markers": markers, "qtype": qt}]], tok.pad_token_id
        )
        z, act = model(
            batch["input_ids"],
            batch["attention_mask"],
            batch["marker_pos"],
            batch["marker_mask"],
            batch["qtype"],
        )
        temperature = cfg["temperature_by_options"].get(
            common.temp_bucket(qt, len(markers)), cfg["temperature"][qt]
        )
        rows.append(
            {
                "text": text,
                "ids": ids,
                "markers": markers,
                "probabilities": (z[0] / max(temperature, 0.001)).softmax(-1).tolist(),
                "act_probability": act[0].softmax(-1)[0].item(),
            }
        )
with args.output.open("x") as output:
    json.dump(rows, output, indent=2)
print(
    json.dumps(
        {"torch": torch.__version__, "output": str(args.output), "questions": len(rows)}
    )
)
