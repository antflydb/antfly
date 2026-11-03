#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Pinned independent upstream F32 text oracle for CPU serving qualification."""

import argparse
import json
import time
from pathlib import Path

import reference


def generate(model_dir, suite_path, output):
    reference.verify_reference(model_dir)
    import torch
    import transformers
    from transformers import AutoModel, AutoTokenizer

    torch.set_num_threads(4)
    torch.set_num_interop_threads(1)
    torch.manual_seed(17)
    suite = json.loads(suite_path.read_text())
    names = [case["name"] for case in suite["cases"]]
    if not names or len(names) != len(set(names)):
        raise ValueError("oracle requires nonempty unique case coverage")
    model = AutoModel.from_pretrained(
        model_dir, dtype=torch.float32, local_files_only=True
    ).eval()
    tokenizer = AutoTokenizer.from_pretrained(model_dir, local_files_only=True)
    result = {
        "version": 1,
        "precision": "float32",
        "model": reference.MODEL,
        "revision": reference.REVISION,
        "transformers_revision": reference.TRANSFORMERS_REVISION,
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "source_hashes": reference.SOURCE_HASHES,
        "checkpoint_receipt_sha256": reference.sha256(
            model_dir / "embeddinggemma2_receipt.json"
        ),
        "suite_sha256": reference.sha256(suite_path),
        "cases": {},
    }
    with torch.inference_mode():
        for case in suite["cases"]:
            task = case.get("task", "RETRIEVAL_QUERY")
            prefix = {
                "RETRIEVAL_QUERY": "task: search result | query: ",
                "RETRIEVAL_DOCUMENT": "title: none | text: ",
            }[task]
            inputs = tokenizer(prefix + case["text"], return_tensors="pt")
            started = time.perf_counter()
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            pooled = (hidden * mask).sum(1) / mask.sum(1)
            vector = torch.nn.functional.normalize(pooled, p=2, dim=-1)[0].tolist()
            if (
                not torch.isfinite(pooled).all()
                or abs(sum(x * x for x in vector) - 1) > 1e-5
            ):
                raise ValueError("upstream output must be finite and normalized")
            if len(vector) != 768:
                raise ValueError("upstream output dimensions differ")
            result["cases"][case["name"]] = {
                "text": case["text"],
                "task": task,
                "token_ids": inputs["input_ids"][0].tolist(),
                "vector": vector,
                "reference_inference_seconds": time.perf_counter() - started,
            }
            output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
            print(
                case["name"],
                "tokens",
                len(result["cases"][case["name"]]["token_ids"]),
                flush=True,
            )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    generate(args.model_dir, args.suite, args.output)


if __name__ == "__main__":
    main()
