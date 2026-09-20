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
"""Generate a deterministic, small Laya forward-pass oracle using upstream common.py.

Download common.py at revision 6a5819129eb220570792e417e49723d697efd76f,
then pass --common /path/to/common.py. No model weights are downloaded.
"""

import argparse
import importlib.util
import json
from pathlib import Path

import torch
from prepare_laya import prepare
from safetensors.torch import save_file
from tokenizers import Tokenizer, models, normalizers, pre_tokenizers, processors
from transformers import ModernBertConfig, ModernBertModel, PreTrainedTokenizerFast


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--common", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    torch.manual_seed(714)
    torch.set_num_threads(1)
    source = args.output / "upstream"
    source.mkdir(parents=True)
    vocabulary = [
        "[PAD]",
        "[UNK]",
        "[CLS]",
        "[SEP]",
        "[MASK]",
        "choice",
        "score",
        "noul",
        "question",
        ":",
        "?",
        "which",
        "tool",
        "is",
        "needed",
        "search",
        "fetch",
        "none",
        "urgency",
        "level",
        "0",
        "1",
        "2",
        "low",
        "medium",
        "high",
        "false",
        "true",
        "no",
        "yes",
        ",",
        "the",
        "statement",
        "does",
        "not",
        "hold",
        "holds",
        "find",
        "document",
        "please",
        "urgent",
        "hello",
        "world",
    ]
    tokenizer = Tokenizer(
        models.WordPiece(
            {word: i for i, word in enumerate(vocabulary)}, unk_token="[UNK]"
        )
    )
    tokenizer.normalizer = normalizers.BertNormalizer(lowercase=True)
    tokenizer.pre_tokenizer = pre_tokenizers.BertPreTokenizer()
    tokenizer.post_processor = processors.BertProcessing(("[SEP]", 3), ("[CLS]", 2))
    tok = PreTrainedTokenizerFast(
        tokenizer_object=tokenizer,
        pad_token="[PAD]",
        unk_token="[UNK]",
        cls_token="[CLS]",
        sep_token="[SEP]",
        mask_token="[MASK]",
    )
    tok.save_pretrained(source / "tokenizer")
    cfg = ModernBertConfig(
        vocab_size=len(vocabulary),
        hidden_size=64,
        num_hidden_layers=2,
        num_attention_heads=1,
        intermediate_size=96,
        max_position_embeddings=128,
        local_attention=16,
        global_attn_every_n_layers=3,
        pad_token_id=0,
        cls_token_id=2,
        sep_token_id=3,
        reference_compile=False,
    )
    cfg._attn_implementation = "eager"
    cfg.save_pretrained(source / "encoder")
    model = common.DecisionModel(ModernBertModel(cfg), head_layers=2).eval()
    agent_config = {
        "encoder": "synthetic-modernbert",
        "head_layers": 2,
        "max_len": 128,
        "head_max_len": 64,
        "act_costs": {"escalate": 0.5},
        "temperature": [1.2, 1.3, 1.4],
        "temperature_by_options": {"choice:3-5": 1.6},
    }
    (source / "rl_agent_config.json").write_text(json.dumps(agent_config))
    save_file(model.state_dict(), source / "model.safetensors")
    prepare(source, args.output / "model", "synthetic-laya", "deterministic-seed-714")
    questions = [
        {
            "t": "choice",
            "ins": "which tool is needed?",
            "crit": {"search": None, "fetch": None, "none": None},
        },
        {"t": "score", "ins": "urgency?", "crit": ["low", "medium", "high"]},
        {"t": "noul", "ins": "is search needed?", "crit": None},
    ]
    states = ["please find the document", "urgent", "hello [MASK] world"]
    items = []
    for state, q in zip(states, questions):
        ids, markers = common.build_sequence(tok, state, q, 128, 64)
        items.append({"ids": ids, "markers": markers, "qtype": common.QTYPES[q["t"]]})
    batch = common.collate_items([items], tok.pad_token_id)
    with torch.no_grad():
        logits, actions = model(
            batch["input_ids"],
            batch["attention_mask"],
            batch["marker_pos"],
            batch["marker_mask"],
            batch["qtype"],
        )
    fixture = {
        "states": states,
        "questions": questions,
        "sequences": items,
        "logits": logits.tolist(),
        "action_logits": actions.tolist(),
    }
    (args.output / "reference.json").write_text(json.dumps(fixture, indent=2) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
