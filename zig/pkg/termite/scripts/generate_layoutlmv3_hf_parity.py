#!/usr/bin/env python3
"""Generate Hugging Face golden outputs for Termite LayoutLMv3 parity tests.

Example:
  python3 scripts/generate_layoutlmv3_hf_parity.py \
    --model-dir /models/layoutlmv3-token \
    --image /tmp/page.png \
    --tokens-json /tmp/page_tokens.json \
    --task token \
    --output /tmp/layoutlmv3_hf_golden.json

The token JSON must be either:
  [{"text": "Invoice", "bbox": [0, 0, 120, 24]}, ...]
or:
  {"tokens": [{"text": "Invoice", "bbox": [0, 0, 120, 24]}, ...]}
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import torch
from PIL import Image
from transformers import (
    AutoConfig,
    LayoutLMv3ForSequenceClassification,
    LayoutLMv3ForTokenClassification,
    LayoutLMv3Processor,
)


def load_tokens(path: Path) -> tuple[list[str], list[list[int]]]:
    raw = json.loads(path.read_text())
    items: list[dict[str, Any]]
    if isinstance(raw, dict):
        items = raw["tokens"]
    else:
        items = raw

    words: list[str] = []
    boxes: list[list[int]] = []
    for idx, item in enumerate(items):
        text = item["text"]
        bbox = item["bbox"]
        if len(bbox) != 4:
            raise ValueError(f"token {idx} bbox must have 4 coordinates")
        if any(coord < 0 or coord > 1000 for coord in bbox):
            raise ValueError(f"token {idx} bbox coordinates must be in 0..1000")
        words.append(text)
        boxes.append([int(coord) for coord in bbox])
    return words, boxes


def tensor_preview(tensor: torch.Tensor, limit: int = 16) -> list[Any]:
    flat = tensor.detach().cpu().reshape(-1)
    return flat[:limit].tolist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--image", required=True, type=Path)
    parser.add_argument("--tokens-json", required=True, type=Path)
    parser.add_argument("--task", required=True, choices=("sequence", "token"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-length", type=int, default=512)
    args = parser.parse_args()

    words, boxes = load_tokens(args.tokens_json)
    image = Image.open(args.image).convert("RGB")
    processor = LayoutLMv3Processor.from_pretrained(args.model_dir, apply_ocr=False)
    config = AutoConfig.from_pretrained(args.model_dir)

    encoded = processor(
        image,
        words,
        boxes=boxes,
        truncation=True,
        padding="max_length",
        max_length=args.max_length,
        return_tensors="pt",
    )

    if args.task == "sequence":
        model = LayoutLMv3ForSequenceClassification.from_pretrained(args.model_dir)
    else:
        model = LayoutLMv3ForTokenClassification.from_pretrained(args.model_dir)
    model.eval()

    with torch.no_grad():
        output = model(**encoded)

    label_map = getattr(config, "id2label", None) or {}
    rendered = {
        "task": args.task,
        "model_dir": str(args.model_dir),
        "image": str(args.image),
        "tokens_json": str(args.tokens_json),
        "max_length": args.max_length,
        "num_tokens": len(words),
        "labels": [label_map.get(i, str(i)) for i in range(getattr(config, "num_labels", len(label_map)))],
        "input_shapes": {name: list(value.shape) for name, value in encoded.items()},
        "input_ids_preview": tensor_preview(encoded["input_ids"]),
        "attention_mask_preview": tensor_preview(encoded["attention_mask"]),
        "bbox_preview": encoded["bbox"][0, : min(args.max_length, 8)].detach().cpu().tolist(),
        "pixel_values_shape": list(encoded["pixel_values"].shape),
        "pixel_values_preview": tensor_preview(encoded["pixel_values"]),
        "logits_shape": list(output.logits.shape),
        "logits": output.logits.detach().cpu().tolist(),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rendered, indent=2) + "\n")


if __name__ == "__main__":
    main()
