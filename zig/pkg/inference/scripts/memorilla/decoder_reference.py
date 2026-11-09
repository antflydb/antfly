#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Emit full soft-input Qwen logits for native prefill and paged-decode checks.

The default reference widens pinned BF16 source weights to F32, matching the
initial native execution policy. It preserves the original decoder vocabulary,
as the upstream vLLM prompt-embedding serving path does. It does not reproduce
the training wrapper's resize_token_embeddings vocabulary truncation.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
from pathlib import Path
import tempfile

import torch
from safetensors.torch import save_file

from assets import LOCK_PATH, verify_base_model
from exporter import load_bundle, sha256
from reference import (
    ReferenceMemory,
    deterministic_cpu,
    fixture_inputs,
    pack_attention_weights,
)

DEFAULT_SYSTEM = "Answer the question using the provided memory."
DEFAULT_QUESTION = "What information is relevant to the question?"


def select_features(
    config: dict, seed: int, encoder_reference: Path | None, question: str | None
):
    if encoder_reference is None:
        inputs = fixture_inputs(config["embedding_dim"], torch.float32, seed)[
            "single_document"
        ]
        return (
            inputs,
            question if question is not None else DEFAULT_QUESTION,
            {
                "feature_source": "synthetic-normalized-vectors-FP16-storage-BF16-input-widened-F32",
                "native_encoder_features_used": False,
            },
        )
    from feature_inputs import load_encoder_features

    inputs, encoded_question, provenance = load_encoder_features(
        encoder_reference, config
    )
    if question is not None and question.strip() != encoded_question:
        raise ValueError("decoder prompt question differs from the encoded question")
    return inputs, encoded_question, provenance


def prepare_prompt(
    tokenizer,
    embedding_lookup,
    memory_tokens,
    *,
    system_prompt: str,
    question: str,
    device=None,
):
    if (
        memory_tokens.ndim != 2
        or memory_tokens.shape[0] == 0
        or not torch.isfinite(memory_tokens).all()
    ):
        raise ValueError("memory tokens must be finite [nonzero slots, decoder width]")
    if "<|memory|>" in question or "<|memory|>" in system_prompt:
        raise ValueError("literal memory placeholders in user content are unsupported")
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.add_special_tokens({"additional_special_tokens": ["<|memory|>"]})
    memory_id = tokenizer.convert_tokens_to_ids("<|memory|>")
    messages = [
        {
            "role": "system",
            "content": system_prompt
            + "\nMemory"
            + "<|memory|>" * memory_tokens.shape[0]
            + "Memory",
        },
        {"role": "user", "content": question.strip()},
    ]
    rendered = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
    )
    ids = tokenizer(rendered, return_tensors="pt", add_special_tokens=False)[
        "input_ids"
    ][0]
    if device is not None:
        ids = ids.to(device)
    positions = (ids == memory_id).nonzero(as_tuple=False).flatten()
    if positions.numel() != memory_tokens.shape[0]:
        raise ValueError("chat template did not preserve exactly the memory slot count")
    # A newly registered ID can fit in the decoder's preallocated vocabulary.
    # Never silently resize/truncate the output head to tokenizer length.
    try:
        embeddings = embedding_lookup(ids).clone()
    except IndexError as error:
        raise ValueError(
            "placeholder is outside the frozen embedding vocabulary"
        ) from error
    if embeddings.ndim != 2 or embeddings.shape[1] != memory_tokens.shape[1]:
        raise ValueError("memory output width differs from decoder embedding width")
    values = memory_tokens.to(dtype=embeddings.dtype, device=embeddings.device)
    delta = values - embeddings.index_select(0, positions)
    embeddings.scatter_add_(0, positions[:, None].expand_as(delta), delta)
    return (
        ids,
        embeddings,
        {
            "messages": messages,
            "rendered_prompt": rendered,
            "memory_token_id": memory_id,
            "memory_positions": positions.tolist(),
            "injection": "scatter-add-delta",
            "enable_thinking": False,
        },
    )


def fp16_roundtrip_cache():
    """Round post-RoPE K/V before both prefill attention and cached decode."""
    from transformers import DynamicCache

    class FP16RoundtripCache(DynamicCache):
        def update(self, key_states, value_states, layer_idx, cache_kwargs=None):
            return super().update(
                key_states.half().float(),
                value_states.half().float(),
                layer_idx,
                cache_kwargs,
            )

    return FP16RoundtripCache()


def teacher_force(model, input_embeddings: torch.Tensor, steps: int, *, kv_dtype="f32"):
    if steps < 1 or steps > 32:
        raise ValueError("decode steps must be between one and 32")
    if kv_dtype not in ("f32", "f16"):
        raise ValueError("reference KV dtype must be f32 or f16")
    with torch.inference_mode():
        initial_cache = (
            {"past_key_values": fp16_roundtrip_cache()} if kv_dtype == "f16" else {}
        )
        output = model(
            inputs_embeds=input_embeddings.unsqueeze(0), use_cache=True, **initial_cache
        )
        prefill = output.logits[0, -1].float().cpu().contiguous()
        cache = output.past_key_values
        next_id = int(prefill.argmax().item())
        ids, logits = [], []
        for _ in range(steps):
            ids.append(next_id)
            output = model(
                input_ids=torch.tensor([[next_id]], dtype=torch.int64),
                past_key_values=cache,
                use_cache=True,
            )
            cache = output.past_key_values
            row = output.logits[0, -1].float().cpu().contiguous()
            logits.append(row)
            next_id = int(row.argmax().item())
    return prefill, torch.tensor(ids, dtype=torch.int64), torch.stack(logits)


def generate(
    model_dir: Path,
    bundle_dir: Path,
    output_dir: Path,
    *,
    steps: int = 3,
    seed: int = 23,
    threads: int = 4,
    system_prompt: str = DEFAULT_SYSTEM,
    question: str | None = None,
    encoder_reference: Path | None = None,
    kv_dtype: str = "f32",
):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    model_dir, bundle_dir, output_dir = (
        Path(model_dir),
        Path(bundle_dir),
        Path(output_dir),
    )
    if output_dir.exists():
        raise ValueError("output already exists")
    if not 1 <= threads <= 16:
        raise ValueError("reference thread count must be between one and 16")
    if kv_dtype not in ("f32", "f16"):
        raise ValueError("reference KV dtype must be f32 or f16")
    source = verify_base_model(model_dir, "decoder")
    config, weights, bundle = load_bundle(bundle_dir)
    if not bundle["release"]:
        raise ValueError("decoder qualification requires a released memory checkpoint")
    with deterministic_cpu(), torch.inference_mode():
        module = ReferenceMemory(config).float().eval()
        module.load_state_dict(
            {
                name: value.float()
                for name, value in pack_attention_weights(weights).items()
            },
            strict=True,
        )
        inputs, question, feature_provenance = select_features(
            config, seed, encoder_reference, question
        )
        memory_tokens = module.trace(inputs)["memory_tokens"][0]
        del module, weights
        torch.set_num_threads(threads)
        tokenizer = AutoTokenizer.from_pretrained(
            model_dir, local_files_only=True, trust_remote_code=False
        )
        model = AutoModelForCausalLM.from_pretrained(
            model_dir,
            local_files_only=True,
            trust_remote_code=False,
            torch_dtype=torch.float32,
            attn_implementation="eager",
        ).eval()
        model.requires_grad_(False)
        ids, embeddings, prompt = prepare_prompt(
            tokenizer,
            model.get_input_embeddings(),
            memory_tokens,
            system_prompt=system_prompt,
            question=question,
        )
        prefill, decode_ids, decode_logits = teacher_force(
            model, embeddings, steps, kv_dtype=kv_dtype
        )
        tensors = {
            "input_ids": ids.to(torch.int64),
            "input_embeddings": embeddings.float(),
            "prefill_logits": prefill,
            "decode_input_ids": decode_ids,
            "decode_logits": decode_logits,
            "memory_tokens": memory_tokens,
        }
        if any(not torch.isfinite(value).all() for value in tensors.values()):
            raise ValueError("nonfinite decoder fixture")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "fixture"
        staging.mkdir()
        save_file(
            {
                name: tensor.detach().cpu().contiguous().clone()
                for name, tensor in tensors.items()
            },
            str(staging / "reference.safetensors"),
        )
        receipt = {
            "schema": "antfly.memorilla.decoder-reference.v1",
            "model": source,
            "base_model_lock_sha256": sha256(LOCK_PATH),
            "memory_bundle_manifest_sha256": sha256(bundle_dir / "manifest.json"),
            "reference_script_sha256": sha256(Path(__file__)),
            "seed": seed,
            "precision": (
                "BF16-source-weights-widened-F32; F32-activations-and-KV"
                if kv_dtype == "f32"
                else "BF16-source-weights-widened-F32; F32-activations; FP16-KV-roundtrip-F32"
            ),
            "kv_dtype": kv_dtype,
            "document_features": feature_provenance["feature_source"],
            "feature_provenance": feature_provenance,
            "encoder_reference_used": encoder_reference is not None,
            "threads": threads,
            "encoder_executed": False,
            "native_executed": False,
            "prompt": prompt,
            "vocabulary_size": prefill.numel(),
            "versions": {
                name: importlib.metadata.version(name)
                for name in ("torch", "transformers", "safetensors")
            },
            "files": {
                "reference.safetensors": {
                    "sha256": sha256(staging / "reference.safetensors"),
                    "size_bytes": (staging / "reference.safetensors").stat().st_size,
                }
            },
        }
        (staging / "reference.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        )
        staging.rename(output_dir)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--encoder-reference", type=Path)
    parser.add_argument("--question")
    parser.add_argument("--kv-dtype", choices=("f32", "f16"), default="f32")
    args = parser.parse_args()
    receipt = generate(
        args.model_dir,
        args.bundle,
        args.output,
        steps=args.steps,
        seed=args.seed,
        threads=args.threads,
        encoder_reference=args.encoder_reference,
        question=args.question,
        kv_dtype=args.kv_dtype,
    )
    print(
        json.dumps(
            {"vocabulary_size": receipt["vocabulary_size"], "fixture": str(args.output)}
        )
    )


if __name__ == "__main__":
    main()
