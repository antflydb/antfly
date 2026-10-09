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

"""Write a tiny zero-weight decision model for native binding conformance.

The fixture uses Laya's encoder/head layout, with uniform distributions so
assertions exercise transport and answer semantics without testing accuracy.
Only the Python standard library is required; no model downloads or binaries.
"""

import json
import math
import struct
import sys
from pathlib import Path


def create_fixture(models_dir: Path) -> str:
    model = "decision-fixture"
    directory = models_dir / "extractors" / model
    directory.mkdir(parents=True, exist_ok=True)
    hidden, intermediate, layers, vocab_size = 64, 96, 3, 64
    config = {
        "model_type": "modernbert",
        "architectures": ["ModernBertModel"],
        "vocab_size": vocab_size,
        "hidden_size": hidden,
        "num_hidden_layers": layers,
        "num_attention_heads": 2,
        "intermediate_size": intermediate,
        "max_position_embeddings": 512,
        "local_attention": 8,
        "global_attn_every_n_layers": 3,
        "layer_norm_eps": 1e-5,
        "pad_token_id": 0,
        "cls_token_id": 2,
        "sep_token_id": 3,
        "laya": {
            "head_layers": 2,
            "max_len": 512,
            "head_max_len": 48,
            "act_costs": {"escalate": 0.5},
            "mask_token": "[MASK]",
        },
    }
    manifest = {
        "type": "classifier",
        "tasks": ["decide"],
        "capabilities": ["typed_decisions"],
        "inputs": ["text"],
    }
    vocabulary = {
        word: i for i, word in enumerate(["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"])
    }
    vocabulary.update({f"word{i}": i for i in range(5, vocab_size)})
    tokenizer = {
        "version": "1.0",
        "truncation": None,
        "padding": None,
        "added_tokens": [
            {
                "id": i,
                "content": token,
                "single_word": False,
                "lstrip": False,
                "rstrip": False,
                "normalized": False,
                "special": True,
            }
            for token, i in list(vocabulary.items())[:5]
        ],
        "normalizer": {
            "type": "BertNormalizer",
            "clean_text": True,
            "handle_chinese_chars": True,
            "strip_accents": None,
            "lowercase": True,
        },
        "pre_tokenizer": {"type": "BertPreTokenizer"},
        "post_processor": None,
        "decoder": {"type": "WordPiece", "prefix": "##", "cleanup": True},
        "model": {
            "type": "WordPiece",
            "unk_token": "[UNK]",
            "continuing_subword_prefix": "##",
            "max_input_chars_per_word": 100,
            "vocab": vocabulary,
        },
    }
    for name, value in [
        ("config.json", config),
        ("model_manifest.json", manifest),
        ("tokenizer.json", tokenizer),
        (
            "tokenizer_config.json",
            {
                "mask_token": "[MASK]",
                "pad_token": "[PAD]",
                "cls_token": "[CLS]",
                "sep_token": "[SEP]",
                "unk_token": "[UNK]",
            },
        ),
    ]:
        (directory / name).write_text(json.dumps(value))
    header, data = {}, bytearray()

    def tensor(name, shape, value=0.0):
        start = len(data)
        data.extend(struct.pack("<f", value) * math.prod(shape))
        header[name] = {
            "dtype": "F32",
            "shape": shape,
            "data_offsets": [start, len(data)],
        }

    tensor("encoder.embeddings.tok_embeddings.weight", [vocab_size, hidden])
    for name in ["encoder.embeddings.norm.weight", "encoder.final_norm.weight"]:
        tensor(name, [hidden], 1.0)
    for layer in range(layers):
        prefix = f"encoder.layers.{layer}"
        if layer:
            tensor(f"{prefix}.attn_norm.weight", [hidden], 1.0)
        tensor(f"{prefix}.mlp_norm.weight", [hidden], 1.0)
        for name, shape in [
            ("attn.Wqkv", [3 * hidden, hidden]),
            ("attn.Wo", [hidden, hidden]),
            ("mlp.Wi", [2 * intermediate, hidden]),
            ("mlp.Wo", [hidden, intermediate]),
        ]:
            tensor(f"{prefix}.{name}.weight", shape)
    for layer in range(2):
        prefix = f"head.layers.{layer}"
        for name, rows, cols in [
            ("self_attn.in_proj", 3 * hidden, hidden),
            ("self_attn.out_proj", hidden, hidden),
            ("linear1", 4 * hidden, hidden),
            ("linear2", hidden, 4 * hidden),
        ]:
            suffix = "_" if name == "self_attn.in_proj" else "."
            tensor(f"{prefix}.{name}{suffix}weight", [rows, cols])
            tensor(f"{prefix}.{name}{suffix}bias", [rows])
        for norm in ["norm1", "norm2"]:
            tensor(f"{prefix}.{norm}.weight", [hidden], 1.0)
            tensor(f"{prefix}.{norm}.bias", [hidden])
    tensor("type_emb.weight", [3, hidden])
    for name, shape in [
        ("scorer.0", [hidden]),
        ("scorer.1", [hidden, hidden]),
        ("scorer.3", [1, hidden]),
        ("act_head.0", [256, hidden + 4]),
        ("act_head.2", [2, 256]),
    ]:
        tensor(f"{name}.weight", shape, 1.0 if name.endswith(".0") else 0.0)
        tensor(f"{name}.bias", [shape[0]])
    encoded = json.dumps(header, separators=(",", ":")).encode()
    encoded += b" " * (-len(encoded) % 8)
    (directory / "model.safetensors").write_bytes(
        struct.pack("<Q", len(encoded)) + encoded + data
    )
    return model


if __name__ == "__main__":
    print(create_fixture(Path(sys.argv[1])))
