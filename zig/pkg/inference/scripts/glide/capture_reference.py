#!/usr/bin/env python3
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

# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "torch==2.14.0",
#   "transformers==5.17.0",
#   "tokenizers==0.23.2",
#   "safetensors==0.8.0",
#   "huggingface_hub==1.33.0",
#   "jinja2==3.1.6",
#   "numpy==2.5.3",
#   "pillow==12.3.0",
# ]
# ///
"""Capture GLiDE Python reference fixtures for the Zig parity test and bench.

Each GLiDE checkpoint ships its reference implementation as a `glide/` package
beside the weights; this script imports that package from the checkpoint
directory and records, for every question prompt in `decide_batch` order, the
exact prompt text, the token ids, the raw 255-way readout logits and the
question's option count.

    uv run zig/pkg/inference/scripts/glide/capture_reference.py \\
        --glide-04b ./models/GLiDE-0.4B --glide-08b ./models/GLiDE-0.8B \\
        --out ./glide-reference

writes into `--out`:

    fix04_cpu_fp32.json  GLiDE-0.4B decision capture
    fix08_cpu_fp32.json  GLiDE-0.8B decision capture
    trunc04_tokens.json  over-long GLiDE-0.4B prompts (head/tail truncation;
                         text and ids only, no weights loaded)

Consumers:

    ANTFLY_GLIDE_REFERENCE=./glide-reference zig build test-glide
    zig build bench-glide -Doptimize=ReleaseFast -- \\
        --model-dir ./models/GLiDE-0.4B --fixture ./glide-reference/fix04_cpu_fp32.json

The parity test reads the CPU float32 captures; `--device mps --dtype bf16`
(or fp16) captures are useful as a bench baseline for Python's own precision
drift.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from pathlib import Path

TICKET = "I was charged twice for order #4821 and the second charge is still pending."

PARA = [
    "The quarterly operations review covered fulfilment latency, carrier performance, and the backlog of unresolved support tickets.",
    "Warehouse 3 reported a 14% increase in mis-picks after the new scanner firmware rollout, which the vendor attributes to a calibration bug.",
    "Customer satisfaction scores for the premium tier dropped from 4.6 to 4.1, driven mostly by delayed refunds and slow escalation handling.",
    "Finance flagged 212 duplicate charges in October; 180 were auto-reversed, but 32 remain pending manual review by the payments team.",
    "The infrastructure team migrated the order database to a new cluster; a 9-minute write outage occurred during the cutover window.",
    "Legal asked that any customer data exported for analytics be pseudonymised and retained for no longer than 90 days.",
    "Marketing proposes a loyalty credit for affected gold-tier customers, estimated to cost $41,000 if issued to everyone impacted.",
    "Engineering estimates that fixing the idempotency key handling in the payment retry path will take two sprints.",
]

INTENT = {
    "type": "choice",
    "instructions": "What does the customer want?",
    "criteria": {
        "refund": "Get money back",
        "cancel": "Cancel the order",
        "track": "Track a delivery",
        "other": "Something else",
    },
}
URGENT = {"type": "noul", "instructions": "Should this ticket be escalated today?"}
PRIORITY = {
    "type": "score",
    "instructions": "What support priority should this ticket get?",
    "criteria": [
        "low: no impact on the customer",
        "medium: the customer is inconvenienced",
        "high: the customer has lost money or cannot use the service",
    ],
}
TOPIC = {
    "type": "choice",
    "instructions": "Which department should own the most urgent follow-up?",
    "criteria": {
        "payments": "Payments and refunds",
        "warehouse": "Warehouse operations",
        "infra": "Infrastructure",
        "legal": "Legal and compliance",
        "marketing": "Marketing",
        "support": "Customer support",
    },
}
RISK = {
    "type": "noul",
    "instructions": "Does this report describe a compliance risk?",
    "criteria": {
        "true": "Yes, there is a compliance risk",
        "false": "No compliance risk",
    },
}
SEVERITY = {
    "type": "score",
    "instructions": "How severe is the overall situation?",
    "criteria": {
        "0": "fine",
        "1": "minor",
        "2": "moderate",
        "3": "serious",
        "4": "critical",
    },
}
MANY = {
    "type": "choice",
    "instructions": "Pick the product category.",
    "criteria": {f"cat_{i}": f"Category number {i}" for i in range(40)},
}


def long_state(n_paras: int) -> str:
    return "\n\n".join(PARA[i % len(PARA)] + f" (note {i})" for i in range(n_paras))


def decision_requests() -> list[dict]:
    """Short, medium and long states covering choice, noul and score questions."""
    return [
        {
            "state": {"ticket": TICKET, "customer_tier": "gold"},
            "questions": {"intent": INTENT, "urgent": URGENT, "priority": PRIORITY},
        },
        {
            "state": "Where is my package? It was supposed to arrive Monday.",
            "questions": {"intent": INTENT},
        },
        {
            "state": "Please cancel my subscription, I no longer need it.",
            "questions": {"intent": INTENT, "urgent": URGENT},
        },
        {"state": long_state(6), "questions": {"topic": TOPIC, "risk": RISK}},
        {
            "state": long_state(20),
            "questions": {"topic": TOPIC, "severity": SEVERITY, "risk": RISK},
        },
        {"state": long_state(60), "questions": {"topic": TOPIC, "severity": SEVERITY}},
        {
            "state": {
                "order": {"id": 4821, "items": ["lamp", "desk"], "status": "delayed"},
                "note": "customer called three times",
            },
            "questions": {"priority": PRIORITY, "category": MANY},
        },
        {"state": long_state(150), "questions": {"severity": SEVERITY}},
    ]


def import_reference(model_dir: Path):
    """Import the checkpoint's own `glide` package, replacing any previously imported one."""
    for name in list(sys.modules):
        if name == "glide" or name.startswith("glide."):
            del sys.modules[name]
    sys.path.insert(0, str(model_dir))
    try:
        return importlib.import_module("glide"), importlib.import_module("glide.prompt")
    finally:
        sys.path.remove(str(model_dir))


def capture_decisions(model_dir: Path, device: str, dtype_name: str) -> dict:
    import torch

    glide, prompt = import_reference(model_dir)
    dtype = {"fp32": torch.float32, "bf16": torch.bfloat16, "fp16": torch.float16}[
        dtype_name
    ]
    started = time.time()
    model = glide.Glide.from_pretrained(
        str(model_dir), device=device, dtype=dtype, vision=False
    )
    load_s = time.time() - started

    captured: list[dict] = []
    original = model._logits

    def spy(seqs, vis):
        result = original(seqs, vis)
        for ids, logits in zip(seqs, result):
            captured.append(
                {"ids": [int(t) for t in ids], "logits": [float(x) for x in logits]}
            )
        return result

    requests = decision_requests()
    model._logits = spy
    try:
        answers = model.decide_batch(requests)
    finally:
        model._logits = original

    texts: list[str] = []
    option_counts: list[int] = []
    for request in requests:
        entries, _ = model._prepare(request)
        texts += [entry[3] for entry in entries]
        option_counts += [
            len(prompt.options_for(q)) for q in request["questions"].values()
        ]
    if not (len(texts) == len(option_counts) == len(captured)):
        raise SystemExit(
            f"{model_dir}: {len(captured)} captured rows for {len(texts)} prompts"
        )
    for row, text, n in zip(captured, texts, option_counts):
        row["text"] = text
        row["n"] = n

    return {
        "model_dir": str(model_dir),
        "device": device,
        "dtype": dtype_name,
        "load_s": load_s,
        "requests": requests,
        "answers": answers,
        "prompts": captured,
    }


def capture_truncation(model_dir: Path) -> dict:
    """Over-long encoder prompts; tokenizer only, so no weights are loaded."""
    from transformers import AutoTokenizer

    import_reference(model_dir)
    encoder = importlib.import_module("glide.encoder")
    prompt = importlib.import_module("glide.prompt")
    config = json.loads((model_dir / "decision_config.json").read_text())
    render = AutoTokenizer.from_pretrained(str(model_dir / config["render_tokenizer"]))
    tokenizer = encoder.EncoderTokenizer(
        render,
        AutoTokenizer.from_pretrained(str(model_dir)),
        config["max_len"],
        config["trunc_head"],
    )
    prompter = prompt.Prompter(tokenizer, config["codes"])
    para = "The quarterly operations review covered fulfilment latency and carrier performance."
    question = {
        "type": "choice",
        "instructions": "Which team owns this?",
        "criteria": {
            "ops": "Operations",
            "carriers": "Carrier management",
            "other": "",
        },
    }
    requests, prompts = [], []
    for repeats in (700, 4000):
        # Whitespace runs exercise ModernBERT's added space tokens.
        state = " ".join([para] * repeats) + "  double  spaced\ttabs\n\nend"
        requests.append({"state": state, "questions": {"team": question}})
        options = prompt.options_for(question)
        text = prompter.text(state, question, options)
        prompts.append(
            {"text": text, "ids": tokenizer([text])["input_ids"][0], "n": len(options)}
        )
    return {"model_dir": str(model_dir), "requests": requests, "prompts": prompts}


def write(path: Path, capture: dict) -> None:
    path.write_text(json.dumps(capture))
    lengths = [len(p["ids"]) for p in capture["prompts"]]
    print(f"{path}: {len(lengths)} prompts, token lengths {lengths}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--glide-04b", type=Path, help="GLiDE-0.4B checkpoint directory"
    )
    parser.add_argument(
        "--glide-08b", type=Path, help="GLiDE-0.8B checkpoint directory"
    )
    parser.add_argument(
        "--out", type=Path, required=True, help="fixture output directory"
    )
    parser.add_argument("--device", default="cpu", help="torch device (default: cpu)")
    parser.add_argument("--dtype", default="fp32", choices=("fp32", "bf16", "fp16"))
    args = parser.parse_args()
    if args.glide_04b is None and args.glide_08b is None:
        parser.error("pass --glide-04b and/or --glide-08b")

    args.out.mkdir(parents=True, exist_ok=True)
    suffix = f"{args.device}_{args.dtype}"
    for tag, model_dir in (("04", args.glide_04b), ("08", args.glide_08b)):
        if model_dir is None:
            continue
        model_dir = Path(os.path.abspath(model_dir))
        write(
            args.out / f"fix{tag}_{suffix}.json",
            capture_decisions(model_dir, args.device, args.dtype),
        )
    if args.glide_04b is not None:
        write(
            args.out / "trunc04_tokens.json",
            capture_truncation(Path(os.path.abspath(args.glide_04b))),
        )


if __name__ == "__main__":
    main()
