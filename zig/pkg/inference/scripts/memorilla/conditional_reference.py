#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent CPU reader/decoder oracle conditioned on an exact native query.

This isolates encoder quantization differences from subsequent module/decoder
arithmetic. It does not qualify the encoder, authenticate caller features, or
replace the independent encoder oracle. No downloads or tolerances are added.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path
import tempfile

import torch
from safetensors.torch import save_file

from assets import LOCK_PATH, lock_model, verify_base_model
from decoder_reference import (
    DEFAULT_SYSTEM,
    prepare_prompt,
    select_features,
    teacher_force,
)
from exporter import load_bundle, sha256
from reference import (
    Inputs,
    ReferenceMemory,
    UPSTREAM_MEMORY_SHA256,
    UPSTREAM_REVISION,
    deterministic_cpu,
    load_upstream,
    pack_attention_weights,
    validate_inputs,
)

WIDTH = 2560
ENCODER_PROFILE = "qwen3_encoder_f32_v1"
QUESTION_RECIPE = "python-strip-unicode-v1;no-empty;no-memory-token"
ENCODER_MANIFEST = (
    '{"type":"embedder","pooling":"last","normalize":true,'
    '"embedding_style":"qwen3_embedding","embedding_trailing_eos_id":151643,'
    '"memorilla_profile":"qwen3_encoder_f32_v1","embedding_profile":'
    '{"task_contract":"profiled","query":{"prefix":""},'
    '"document":{"prefix":""}}}\n'
).encode()
CONDITION_SCHEMA = "antfly.memorilla.conditional-native-query-reference.v1"


def execution_identity() -> str:
    """Mirror the versioned production identity, without importing native code."""
    lock_model("encoder")
    return hashlib.sha256(
        b"antfly.memorilla.execution.v1\0"
        + ENCODER_PROFILE.encode()
        + b"\0"
        + ENCODER_MANIFEST
        + b"\0"
        + LOCK_PATH.read_bytes()
        + b"\0bf16-source/f32-resident/f32-activations\0sm_121\0"
        + QUESTION_RECIPE.encode()
    ).hexdigest()


def _hex(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value)
        and value != "0" * 64
    )


def load_native_query(
    path: Path, *, expected_question: str
) -> tuple[torch.Tensor, dict]:
    path = Path(path)
    if path.stat().st_size > 512 * 1024:
        raise ValueError("native query receipt exceeds its input budget")
    raw_receipt = path.read_bytes()
    if len(raw_receipt) > 512 * 1024:
        raise ValueError("native query receipt exceeds its input budget")
    receipt = json.loads(raw_receipt)
    if (
        not isinstance(receipt, dict)
        or set(receipt)
        != {"schema", "question_text", "stored_fp16", "memory_bf16_f32", "producer"}
        or receipt["schema"] != "antfly.memorilla.native-query.v1"
    ):
        raise ValueError("invalid native query receipt schema")
    question = receipt["question_text"]
    if (
        not isinstance(question, str)
        or not question
        or len(question.encode("utf-8")) > 8192
        or question != question.strip()
        or "<|memory|>" in question
        or question != expected_question
    ):
        raise ValueError(
            "native query canonical question differs from the feature/prompt question"
        )
    producer = receipt["producer"]
    if (
        not isinstance(producer, dict)
        or set(producer)
        != {"profile", "publication", "identity", "source_lock_sha256", "recipe_sha256"}
        or (
            producer["profile"] != ENCODER_PROFILE
            or not _hex(producer["publication"])
            or producer["identity"] != execution_identity()
            or producer["source_lock_sha256"] != sha256(LOCK_PATH)
            or producer["recipe_sha256"] != hashlib.sha256(ENCODER_MANIFEST).hexdigest()
        )
    ):
        raise ValueError(
            "native query producer differs from the pinned F32 encoder recipe"
        )
    values = []
    for name in ("stored_fp16", "memory_bf16_f32"):
        row = receipt[name]
        if (
            not isinstance(row, list)
            or len(row) != WIDTH
            or any(type(value) not in (int, float) for value in row)
        ):
            raise ValueError("native query arrays must be numeric full-width rows")
        tensor = torch.tensor(row, dtype=torch.float32)
        if not torch.isfinite(tensor).all():
            raise ValueError("native query arrays must be finite")
        values.append(tensor)
    stored, memory = values
    if not torch.equal(
        stored.view(torch.int32), stored.half().float().view(torch.int32)
    ):
        raise ValueError("stored native query values must exactly represent FP16")
    expected = stored.half().bfloat16().float()
    if not torch.equal(memory.view(torch.int32), expected.view(torch.int32)):
        raise ValueError(
            "native query does not preserve FP16-storage to BF16-readout values"
        )
    if abs(float(stored.norm()) - 1) > 0.002:
        raise ValueError("native query must originate from normalized encoder output")
    return memory.reshape(1, WIDTH), {
        "schema": "antfly.memorilla.native-query-binding.v1",
        "artifact_sha256": hashlib.sha256(raw_receipt).hexdigest(),
        "producer": producer,
        "question_text": question,
        "condition": "exact native BF16-valued query widened to F32",
        "encoder_independently_qualified_here": False,
        "cuda_execution_attested_here": False,
    }


def conditional_inputs(config: dict, encoder_reference: Path, native_query: Path):
    inputs, question, provenance = select_features(config, 23, encoder_reference, None)
    if inputs.documents.dtype != torch.float32 or config["embedding_dim"] != WIDTH:
        raise ValueError("conditional reference requires full-width F32 memory inputs")
    query, native = load_native_query(native_query, expected_question=question)
    conditional = Inputs(inputs.documents, inputs.padding_mask, query)
    validate_inputs(conditional, WIDTH)
    return (
        conditional,
        question,
        {
            **provenance,
            "native_query": native,
            "native_encoder_features_used": True,
            "native_document_features_used": False,
            "conditional_scope": "CPU document reference features plus exact native question features",
        },
    )


def verify_decoder(model_dir: Path) -> dict:
    # Transformers selects these before the pinned shard index. Required-file
    # checks alone must not authenticate an unrelated shadow checkpoint/adapter.
    for name in (
        "model.safetensors",
        "pytorch_model.bin",
        "pytorch_model.bin.index.json",
        "adapter_config.json",
        "adapter_model.safetensors",
        "adapter_model.bin",
        "added_tokens.json",
        "special_tokens_map.json",
    ):
        if (model_dir / name).exists():
            raise ValueError(
                "unlocked decoder weights, adapter or tokenizer metadata would shadow the pinned source"
            )
    return verify_base_model(model_dir, "decoder")


def generate(
    model_dir: Path,
    bundle_dir: Path,
    encoder_reference: Path,
    native_query: Path,
    output_dir: Path,
    *,
    steps: int = 3,
    threads: int = 4,
    upstream_source: Path | None = None,
):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    model_dir, bundle_dir, output_dir = (
        Path(model_dir),
        Path(bundle_dir),
        Path(output_dir),
    )
    if output_dir.exists():
        raise ValueError("output already exists")
    if not 1 <= threads <= 16 or not 1 <= steps <= 32:
        raise ValueError(
            "conditional reference requires 1..16 threads and 1..32 decode steps"
        )
    source = verify_decoder(model_dir)
    config, weights, bundle = load_bundle(bundle_dir)
    if not bundle["release"]:
        raise ValueError("conditional qualification requires a released memory module")
    inputs, question, features = conditional_inputs(
        config, encoder_reference, native_query
    )
    packed = pack_attention_weights(weights)
    with deterministic_cpu(), torch.inference_mode():
        with torch.device("meta"):
            module = ReferenceMemory(config)
        module.load_state_dict(
            {name: value.float() for name, value in packed.items()},
            strict=True,
            assign=True,
        )
        module.eval()
        trace = module.trace(inputs)
        if upstream_source is not None:
            original = load_upstream(upstream_source, config, packed).float().eval()
            actual = original(inputs.documents, inputs.padding_mask, inputs.question)
            torch.testing.assert_close(actual, trace["memory_tokens"], rtol=0, atol=0)
            del original
        memory_tokens = trace["memory_tokens"][0]
        module_tensors = {
            f"conditional_native_query.{name}": value
            for name, value in {**inputs.tensors(), **trace}.items()
        }
        del module, weights, packed
        gc.collect()
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
            system_prompt=DEFAULT_SYSTEM,
            question=question,
        )
        prefill, decode_ids, decode_logits = teacher_force(
            model, embeddings, steps, kv_dtype="f32"
        )
        decoder_tensors = {
            "input_ids": ids.to(torch.int64),
            "input_embeddings": embeddings.float(),
            "memory_tokens": memory_tokens,
            "prefill_logits": prefill,
            "decode_input_ids": decode_ids,
            "decode_logits": decode_logits,
        }
        if any(
            not torch.isfinite(t).all()
            for t in (*module_tensors.values(), *decoder_tensors.values())
        ):
            raise ValueError("nonfinite conditional reference")
    common = {
        "conditional_native_query": True,
        "feature_provenance": features,
        "conditional_reference_script_sha256": sha256(Path(__file__)),
        "base_model_lock_sha256": sha256(LOCK_PATH),
        "native_encoder_qualified_here": False,
        "acceptance_gates_changed": False,
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "reference"
        staging.mkdir()
        memory_dir, decoder_dir = staging / "memory", staging / "decoder"
        memory_dir.mkdir()
        decoder_dir.mkdir()
        for directory, tensors in (
            (memory_dir, module_tensors),
            (decoder_dir, decoder_tensors),
        ):
            save_file(
                {
                    name: tensor.detach().cpu().contiguous().clone()
                    for name, tensor in tensors.items()
                },
                directory / "reference.safetensors",
            )
        memory_receipt = {
            **common,
            "schema_version": 1,
            "kind": "memorilla_module_reference",
            "release_bundle": True,
            "native_parity_verified": False,
            "bundle_manifest_sha256": sha256(bundle_dir / "manifest.json"),
            "bundle_weights_sha256": sha256(bundle_dir / "memory.safetensors"),
            "execution": {"device": "cpu", "dtype": "float32", "threads": 1},
            "cases": ["conditional_native_query"],
            "upstream_revision": UPSTREAM_REVISION,
            "upstream_memory_sha256": UPSTREAM_MEMORY_SHA256,
            "upstream_forward_compared": upstream_source is not None,
            "tensors": {
                "path": "reference.safetensors",
                "sha256": sha256(memory_dir / "reference.safetensors"),
            },
        }
        decoder_path = decoder_dir / "reference.safetensors"
        decoder_receipt = {
            **common,
            "schema": "antfly.memorilla.decoder-reference.v1",
            "model": source,
            "memory_bundle_manifest_sha256": sha256(bundle_dir / "manifest.json"),
            "precision": "BF16-source-weights-widened-F32; F32-activations-and-KV",
            "kv_dtype": "f32",
            "threads": threads,
            "document_features": features["feature_source"],
            "encoder_reference_used": True,
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
                    "sha256": sha256(decoder_path),
                    "size_bytes": decoder_path.stat().st_size,
                }
            },
        }
        for directory, receipt in (
            (memory_dir, memory_receipt),
            (decoder_dir, decoder_receipt),
        ):
            (directory / "reference.json").write_text(
                json.dumps(receipt, indent=2, sort_keys=True) + "\n"
            )
        summary = {
            **common,
            "schema": CONDITION_SCHEMA,
            "memory_reference_sha256": sha256(memory_dir / "reference.json"),
            "decoder_reference_sha256": sha256(decoder_dir / "reference.json"),
        }
        (staging / "conditional.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n"
        )
        staging.rename(output_dir)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("model", "bundle", "encoder-reference", "native-query", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--upstream-source", type=Path)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    result = generate(
        args.model,
        args.bundle,
        args.encoder_reference,
        args.native_query,
        args.output,
        steps=args.steps,
        threads=args.threads,
        upstream_source=args.upstream_source,
    )
    print(
        json.dumps(
            {"schema": result["schema"], "output": str(args.output)}, sort_keys=True
        )
    )


if __name__ == "__main__":
    main()
