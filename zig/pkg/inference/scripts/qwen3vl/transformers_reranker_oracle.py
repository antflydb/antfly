#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Run the pinned Qwen3-VL-Reranker-2B BF16 scoring oracle offline.

The implementation reproduces the official reference contract without loading
or executing remote Python code: render the model's pinned chat template, keep
the final hidden row, project it with ``W_yes - W_no``, and apply sigmoid.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time
from typing import Any

from qualify_qwen3vl_metal import validate_managed_reranker_bundle
from transformers_oracle import OracleError, sha256_file, tensor_f32le_bytes, verify_environment


MODEL_REPOSITORY = "Qwen/Qwen3-VL-Reranker-2B"
MODEL_REVISION = "4bd860ac4f15ad1897a214615cccc700f8f71818"
MODEL_SIZE = 4_255_140_312
MODEL_SHA256 = "466ec01961061e9d7f804b4fb1625fb6f406106cd1567e026096d4736fa9d5b9"
YES_TOKEN_ID = 9_693
NO_TOKEN_ID = 2_152
MAX_LENGTH = 8_192
PROTECTED_SUFFIX_TOKENS = 5
SYSTEM_PROMPT = (
    "Judge whether the Document meets the requirements based on the Query and the Instruct provided. "
    'Note that the answer can only be "yes" or "no".'
)
DEFAULT_INSTRUCTION = "Given a search query, retrieve relevant candidates that answer the query."


def render_expected_prompt(instruction: str, query: str, document: str) -> str:
    return (
        f"<|im_start|>system\n{SYSTEM_PROMPT}<|im_end|>\n"
        f"<|im_start|>user\n<Instruct>: {instruction}<Query>:{query}"
        f"\n<Document>:{document}<|im_end|>\n"
        "<|im_start|>assistant\n"
    )


IMAGE_MARKER = "<|vision_start|><|image_pad|><|vision_end|>"


def generic_messages(
    instruction: str, query: str, document: str, has_image: bool = False
) -> list[dict[str, Any]]:
    document_content: list[dict[str, Any]] = [
        {"type": "text", "text": "\n<Document>:"},
    ]
    if has_image:
        document_content.append({"type": "image", "image": "qualification-image"})
    document_content.append({"type": "text", "text": document})
    return [
        {"role": "system", "content": [{"type": "text", "text": SYSTEM_PROMPT}]},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": f"<Instruct>: {instruction}"},
                {"type": "text", "text": f"<Query>:{query}"},
                *document_content,
            ],
        },
    ]


def reranker_messages(
    instruction: str, query: str, document: str, has_image: bool = False
) -> list[dict[str, Any]]:
    document_content: list[dict[str, Any]] = []
    if has_image:
        document_content.append({"type": "image", "image": "qualification-image"})
    document_content.append({"type": "text", "text": document})
    return [
        {"role": "system", "content": [{"type": "text", "text": instruction}]},
        {"role": "query", "content": [{"type": "text", "text": query}]},
        {"role": "document", "content": document_content},
    ]


def truncate_upstream(ids: list[int], max_length: int, special_ids: set[int]) -> list[int]:
    """Reproduce the pinned helper, including its max_length + 5 behavior."""

    if len(ids) <= max_length:
        return ids
    if len(ids) < PROTECTED_SUFFIX_TOKENS:
        raise OracleError("reranker token sequence is shorter than the protected suffix")
    prefix = ids[:-PROTECTED_SUFFIX_TOKENS]
    suffix = ids[-PROTECTED_SUFFIX_TOKENS:]
    special_count = sum(token in special_ids for token in prefix)
    ordinary_budget = max_length - special_count
    if ordinary_budget < 0:
        raise OracleError("special reranker tokens exceed the upstream truncation budget")
    kept: list[int] = []
    ordinary_kept = 0
    for token in prefix:
        if token in special_ids:
            kept.append(token)
        elif ordinary_kept < ordinary_budget:
            kept.append(token)
            ordinary_kept += 1
    return kept + suffix


def synchronize(device: str) -> None:
    if device == "mps":
        import torch

        torch.mps.synchronize()


def render_and_tokenize(
    processor: Any,
    instruction: str,
    query: str,
    documents: list[str],
    max_length: int,
    image: Any | None = None,
    max_merged_tokens: int = 576,
) -> tuple[list[str], Any]:
    if image is not None and len(documents) != 1:
        raise OracleError("multimodal reranker oracle requires exactly one document")
    rendered: list[str] = []
    for document in documents:
        generic = processor.apply_chat_template(
            generic_messages(instruction, query, document, image is not None),
            tokenize=False,
            add_generation_prompt=True,
        )
        dedicated = processor.apply_chat_template(
            reranker_messages(instruction, query, document, image is not None),
            chat_template="reranker",
            tokenize=False,
            add_generation_prompt=True,
        )
        expected_document = (IMAGE_MARKER if image is not None else "") + document
        expected = render_expected_prompt(instruction, query, expected_document)
        if generic != expected:
            raise OracleError("generic pinned chat template drifted from the official reference prompt")
        if dedicated != expected:
            raise OracleError("dedicated reranker template drifted from the official reference prompt")
        rendered.append(expected)

    if image is None:
        encoded = processor(
            text=rendered,
            truncation=False,
            padding=False,
            do_resize=False,
        )
    else:
        patch_size = int(processor.image_processor.patch_size)
        merge_size = int(processor.image_processor.merge_size)
        factor_area = (patch_size * merge_size) ** 2
        max_pixels = max_merged_tokens * factor_area
        official_min_pixels = int(processor.image_processor.size["shortest_edge"])
        if max_pixels < official_min_pixels:
            raise OracleError("max merged tokens cannot satisfy the official image minimum")
        encoded = processor(
            text=rendered,
            images=[image],
            images_kwargs={
                "size": {
                    "longest_edge": max_pixels,
                    "shortest_edge": official_min_pixels,
                }
            },
            truncation=False,
            padding=False,
            return_tensors="pt",
        )
    special_ids = {int(token) for token in processor.tokenizer.all_special_ids}
    bounded = [
        truncate_upstream([int(token) for token in row], max_length, special_ids)
        for row in encoded["input_ids"]
    ]
    padded = processor.tokenizer.pad(
        {"input_ids": bounded},
        padding=True,
        return_tensors="pt",
        max_length=max_length,
    )
    encoded["input_ids"] = padded["input_ids"]
    encoded["attention_mask"] = padded["attention_mask"]
    return rendered, encoded


def run(args: argparse.Namespace) -> dict[str, Any]:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    versions = verify_environment()

    import numpy as np
    import torch
    from PIL import Image
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration
    from transformers.utils import logging as transformers_logging

    transformers_logging.disable_progress_bar()
    torch.set_num_threads(args.threads)
    if args.device == "mps" and not torch.backends.mps.is_available():
        raise OracleError("MPS is not available")

    bundle = validate_managed_reranker_bundle(args.model_dir)
    model_dir = Path(bundle["model_dir"])
    model_path = Path(bundle["model_path"])
    if model_path.stat().st_size != MODEL_SIZE or sha256_file(model_path) != MODEL_SHA256:
        raise OracleError("pinned BF16 reranker model identity mismatch")

    instruction = args.instruction or DEFAULT_INSTRUCTION
    processor = AutoProcessor.from_pretrained(
        model_dir,
        local_files_only=True,
        trust_remote_code=False,
        use_fast=True,
        padding_side="left",
    )
    yes_id = int(processor.tokenizer.get_vocab().get("yes", -1))
    no_id = int(processor.tokenizer.get_vocab().get("no", -1))
    if (yes_id, no_id) != (YES_TOKEN_ID, NO_TOKEN_ID):
        raise OracleError(
            f"reranker score-token drift: expected {(YES_TOKEN_ID, NO_TOKEN_ID)}, got {(yes_id, no_id)}"
        )
    image_path = args.image.resolve(strict=True) if args.image else None
    image_rgb = None
    source_size = None
    if image_path is not None:
        with Image.open(image_path) as opened:
            image_rgb = opened.convert("RGB")
            source_size = [int(image_rgb.width), int(image_rgb.height)]
    try:
        rendered, inputs = render_and_tokenize(
            processor,
            instruction,
            args.query,
            args.document,
            args.max_length,
            image_rgb,
            args.max_merged_tokens,
        )
    finally:
        if image_rgb is not None:
            image_rgb.close()

    load_started = time.monotonic()
    lm = Qwen3VLForConditionalGeneration.from_pretrained(
        model_dir,
        local_files_only=True,
        trust_remote_code=False,
        dtype=torch.bfloat16,
        attn_implementation="eager",
        low_cpu_mem_usage=True,
        device_map={"": args.device},
    )
    lm.eval()
    synchronize(args.device)
    loaded_at = time.monotonic()

    model_inputs = {name: value.to(args.device) for name, value in inputs.items()}
    with torch.inference_mode():
        position_ids, position_deltas = lm.model.get_rope_index(
            input_ids=model_inputs["input_ids"],
            image_grid_thw=model_inputs.get("image_grid_thw"),
            video_grid_thw=None,
            attention_mask=model_inputs["attention_mask"],
        )
        final_hidden = lm.model(**model_inputs, use_cache=False, return_dict=True).last_hidden_state[:, -1]
        yes_weight = lm.lm_head.weight[YES_TOKEN_ID]
        no_weight = lm.lm_head.weight[NO_TOKEN_ID]
        score_logits = final_hidden @ (yes_weight - no_weight)
        synchronize(args.device)
        final_hidden_f32 = final_hidden.to(device="cpu", dtype=torch.float32).contiguous()
        score_logits_f32 = score_logits.to(device="cpu", dtype=torch.float32).contiguous()
        # The BF16 model projection defines the reference logit. Probability
        # conversion is an API operation, not another model layer, and the
        # native reranker applies sigmoid in f32 as well.
        scores_f32 = torch.sigmoid(score_logits_f32).contiguous()
    finished_at = time.monotonic()

    if final_hidden_f32.shape != (len(args.document), int(lm.config.text_config.hidden_size)):
        raise OracleError(f"unexpected final hidden shape: {tuple(final_hidden_f32.shape)}")
    if not bool(torch.isfinite(final_hidden_f32).all().item()):
        raise OracleError("Transformers produced non-finite final hidden values")
    if not bool(torch.isfinite(scores_f32).all().item()):
        raise OracleError("Transformers produced non-finite reranker scores")

    hidden_bytes = tensor_f32le_bytes(final_hidden_f32)
    if args.hidden_output is not None:
        args.hidden_output.write_bytes(hidden_bytes)
    ids = inputs["input_ids"].tolist()
    masks = inputs["attention_mask"].tolist()
    positions_cpu = position_ids.to(device="cpu", dtype=torch.int64)
    deltas_cpu = position_deltas.to(device="cpu", dtype=torch.int64)
    active_mrope_positions = [
        [
            int(value)
            for value in positions_cpu[:, row_index, torch.tensor(mask_row, dtype=torch.bool)]
            .reshape(-1)
            .tolist()
        ]
        for row_index, mask_row in enumerate(masks)
    ]
    return {
        "schema": "antfly.qwen3vl.transformers_reranker_oracle.v1",
        "model": {
            "repository": MODEL_REPOSITORY,
            "revision": MODEL_REVISION,
            "path": str(model_path),
            "size": MODEL_SIZE,
            "sha256": MODEL_SHA256,
            "dtype": "bfloat16",
            "bundle_receipt_sha256": bundle["receipt_sha256"],
        },
        "runtime": {
            "python": platform.python_version(),
            "packages": versions,
            "device": args.device,
            "threads": args.threads,
            "torch_mps_available": bool(torch.backends.mps.is_available()),
        },
        "contract": {
            "yes_token_id": YES_TOKEN_ID,
            "no_token_id": NO_TOKEN_ID,
            "score": "sigmoid_f32(bf16(final_hidden @ (W_yes - W_no)))",
            "padding_side": "left",
            "max_length": args.max_length,
            "protected_suffix_tokens": PROTECTED_SUFFIX_TOKENS,
            "truncation": "pinned_upstream_compat",
        },
        "request": {
            "instruction": instruction,
            "query": args.query,
            "documents": args.document,
            "rendered_prompts": rendered,
            "rendered_prompt_sha256": [
                hashlib.sha256(prompt.encode()).hexdigest() for prompt in rendered
            ],
            "input_ids": [[int(token) for token in row] for row in ids],
            "attention_mask": [[int(value) for value in row] for row in masks],
            "active_mrope_position_ids": active_mrope_positions,
            "mrope_position_deltas": [int(value) for value in deltas_cpu.reshape(-1).tolist()],
            "image": (
                {
                    "path": str(image_path),
                    "sha256": sha256_file(image_path),
                    "source_size": source_size,
                    "max_merged_tokens": args.max_merged_tokens,
                    "grid_thw": [
                        [int(value) for value in row]
                        for row in inputs["image_grid_thw"].tolist()
                    ],
                    "visual_tokens": sum(
                        token == int(lm.config.image_token_id)
                        for row in ids
                        for token in row
                    ),
                }
                if image_path is not None
                else None
            ),
        },
        "output": {
            "score_logits": [float(value) for value in score_logits_f32.tolist()],
            "scores": [float(value) for value in scores_f32.tolist()],
            "ranking": sorted(
                range(len(args.document)),
                key=lambda index: (-float(scores_f32[index]), index),
            ),
            "final_hidden_shape": [int(dim) for dim in final_hidden_f32.shape],
            "final_hidden_f32le_sha256": hashlib.sha256(hidden_bytes).hexdigest(),
            "final_hidden_min": float(np.frombuffer(hidden_bytes, dtype="<f4").min()),
            "final_hidden_max": float(np.frombuffer(hidden_bytes, dtype="<f4").max()),
            "hidden_output": str(args.hidden_output.resolve()) if args.hidden_output else None,
        },
        "timing_seconds": {
            "load_and_move": loaded_at - load_started,
            "forward": finished_at - loaded_at,
            "total": finished_at - load_started,
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--document", action="append", required=True)
    parser.add_argument("--image", type=Path)
    parser.add_argument("--max-merged-tokens", type=int, default=576)
    parser.add_argument("--instruction", default=DEFAULT_INSTRUCTION)
    parser.add_argument("--max-length", type=int, default=MAX_LENGTH)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--device", choices=("cpu", "mps"), default="cpu")
    parser.add_argument("--hidden-output", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.max_length <= MAX_LENGTH + PROTECTED_SUFFIX_TOKENS:
        parser.error(f"--max-length must be in [1, {MAX_LENGTH + PROTECTED_SUFFIX_TOKENS}]")
    if not 1 <= args.threads <= 16:
        parser.error("--threads must be in [1, 16]")
    if not 4 <= args.max_merged_tokens <= 1024:
        parser.error("--max-merged-tokens must be in [4, 1024]")
    if args.image is not None and len(args.document) != 1:
        parser.error("--image requires exactly one --document")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        payload = run(args)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return 0
    except (OracleError, OSError, RuntimeError, ValueError) as exc:
        print(f"Qwen3-VL Transformers reranker oracle failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
