#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Offline CPU source-policy BF16 reproduction and complete F32 control.

This executes the pinned upstream MemoryModule and original memory.pt, without
vLLM or vocabulary resize. CPU Transformers/eager kernels are a fidelity
deviation from the released GPU vLLM engine, not bitwise vLLM reproduction.
No downloads, native runtime changes, or quality-rubric changes occur here.
"""

from __future__ import annotations

import argparse
import ast
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path
import resource
import subprocess
import time

import numpy as np
import torch
from safetensors.torch import save_file

from assets import LOCK_PATH, verify_base_model
from conditional_reference import verify_decoder
from decoder_reference import prepare_prompt
from encoder_reference import active_ids, normalize_features
from exporter import (
    CHECKPOINT_SHA256,
    MODEL,
    REVISION,
    read_json,
    sha256,
    validate_config,
    validate_tensors,
)
from quality_cases import DEFAULT_SUITE, load_suite, score_answer, score_suite
from reference import UPSTREAM_REVISION, deterministic_cpu, load_upstream

CASE_IDS = (
    "copper_badge_a",
    "copper_badge_b",
    "lantern_courier_a",
    "lantern_courier_b",
)
PROFILES = {"float32": torch.float32, "bfloat16": torch.bfloat16}
EOS_IDS = (151645, 151643)
SYSTEM = "You are a helpful assistant."
POOLER_SOURCES = (
    "https://docs.vllm.ai/en/v0.9.1/api/vllm/model_executor/layers/pooler.html",
    "https://docs.vllm.ai/en/v0.9.1/api/vllm/outputs.html",
)


def save_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def source_recipe(source: Path) -> dict:
    revision = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    if revision != UPSTREAM_REVISION:
        raise ValueError("upstream source revision differs from the pinned release")
    files = {}
    for name in (
        "memory.py",
        "embeddings.py",
        "data.py",
        "benchmarks.py",
        "llm.py",
        "vllm.py",
    ):
        path = source / "memorilla" / name
        raw = path.read_bytes()
        blob = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
        expected = subprocess.check_output(
            ["git", "-C", str(source), "rev-parse", f"HEAD:memorilla/{name}"], text=True
        ).strip()
        if blob != expected:
            raise ValueError(f"upstream file changed: {name}")
        files[name] = sha256(path)
    tree = ast.parse((source / "memorilla/benchmarks.py").read_text())
    helpful = [
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "HELPFUL_PROMPT" for t in node.targets
        )
    ]
    if helpful != [SYSTEM]:
        raise ValueError("upstream helpful prompt differs")
    tree = ast.parse((source / "memorilla/data.py").read_text())
    if not any(
        isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Name)
        and node.value.id == "HELPFUL_PROMPT"
        and any(
            isinstance(t, ast.Name) and t.id == "DEFAULT_SYSTEM_PROMPT"
            for t in node.targets
        )
        for node in tree.body
    ):
        raise ValueError("upstream default prompt is not the helpful prompt")
    return {
        "revision": revision,
        "files_sha256": files,
        "pooler_sources": list(POOLER_SOURCES),
        "feature_recipe": "BF16 last-token hidden -> BF16 L2 normalization -> vLLM F32/Python output -> NumPy FP16 -> redundant FP16 cache write -> BF16 reader",
        "prompt": SYSTEM,
        "memory_slots": 16,
        "decoder_vocabulary_policy": "original 151936 rows, serving vLLM policy; no training-wrapper resize",
    }


def numpy_storage(normalized: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    """Match embed_texts' NumPy FP16 assignment, then BF16 readout.

    vLLM widens pooled/normalized output to F32 for Python after normalization;
    that widening must not move BF16 normalization into F32. Cache writing's
    second FP16 conversion is idempotent. Direct F32->BF16 is not equivalent.
    """
    if normalized.ndim != 2 or not torch.isfinite(normalized).all():
        raise ValueError("expected finite pooled rows")
    stored = torch.from_numpy(
        normalized.detach().cpu().float().numpy().astype(np.float16)
    )
    stored = stored.to(torch.float16).contiguous()
    memory = stored.to(torch.bfloat16)
    if not torch.isfinite(memory).all():
        raise ValueError("nonfinite stored features")
    return stored, memory


def conditioning_features(
    normalized: torch.Tensor, profile: str
) -> tuple[torch.Tensor, torch.Tensor]:
    if profile not in PROFILES or normalized.dtype != PROFILES[profile]:
        raise ValueError("encoder output arithmetic differs from selected profile")
    stored, memory = numpy_storage(normalized)
    return stored, memory.to(PROFILES[profile])


def cache_dtype(cache, expected: torch.dtype) -> None:
    legacy = cache.to_legacy_cache() if hasattr(cache, "to_legacy_cache") else cache
    if not legacy or any(
        t.dtype != expected or t.device.type != "cpu" for layer in legacy for t in layer
    ):
        raise ValueError("KV cache precision/device mismatch")


def greedy(model, tokenizer, embeddings, maximum: int, dtype: torch.dtype):
    if not 1 <= maximum <= 128 or embeddings.dtype != dtype:
        raise ValueError("invalid greedy execution contract")
    output = model(inputs_embeds=embeddings.unsqueeze(0), use_cache=True)
    generated, rows, margins = [], [], []
    reason = "length"
    for step in range(maximum):
        row = output.logits[0, -1].float().cpu().contiguous().clone()
        if row.numel() != 151936 or not torch.isfinite(row).all():
            raise ValueError("unexpected full-vocabulary logits")
        token = int(row.argmax())
        runner = row.clone()
        runner[token] = -torch.inf
        second = int(runner.argmax())
        rows.append(row)
        margins.append(
            {
                "step": step,
                "top1_id": token,
                "top2_id": second,
                "margin": float(row[token].double() - row[second].double()),
            }
        )
        generated.append(token)
        cache = output.past_key_values
        cache_dtype(cache, dtype)
        if token in EOS_IDS:
            reason = "eos"
            break
        if step + 1 < maximum:
            del output
            output = model(
                input_ids=torch.tensor([[token]], dtype=torch.int64),
                past_key_values=cache,
                use_cache=True,
            )
    text = tokenizer.decode(generated, skip_special_tokens=True).strip()
    del output, cache
    return {
        "text": text,
        "token_ids": generated,
        "finish_reason": reason,
        "top_margins": margins,
    }, torch.stack(rows)


def checkpoint_proof(checkpoint: Path) -> tuple[dict, dict]:
    config = validate_config(read_json(checkpoint / "config.json"), release=True)
    digest = sha256(checkpoint / "memory.pt")
    if digest != CHECKPOINT_SHA256["stage3"]:
        raise ValueError("requires original pinned Stage 3 memory.pt")
    return config, {
        "model": MODEL,
        "revision": REVISION,
        "checkpoint": "stage3",
        "memory_pt_sha256": digest,
        "config_sha256": sha256(checkpoint / "config.json"),
    }


def selected_cases(suite: dict) -> list[dict]:
    lookup = {case["id"]: case for case in suite["cases"]}
    if any(key not in lookup for key in CASE_IDS):
        raise ValueError("suite lacks the predeclared four cases")
    return [lookup[key] for key in CASE_IDS]


def run_profile(args, profile: str, protocol: dict, cases: list[dict]) -> dict:
    from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer

    dtype = PROFILES[profile]
    output = args.output / profile
    output.mkdir()
    started = time.monotonic()
    result = {"profile": profile, "completed": False, "results": []}
    save_json(output / "result.json", result)
    texts = list(
        dict.fromkeys(
            [doc for case in cases for doc in case["documents"]]
            + [case["question"].strip() for case in cases]
        )
    )
    positions = {text: i for i, text in enumerate(texts)}
    torch.set_num_threads(args.threads)
    torch.manual_seed(0)
    torch.use_deterministic_algorithms(True)
    tokenizer = AutoTokenizer.from_pretrained(
        args.encoder,
        local_files_only=True,
        trust_remote_code=False,
        padding_side="left",
    )
    encoder = (
        AutoModel.from_pretrained(
            args.encoder,
            local_files_only=True,
            trust_remote_code=False,
            torch_dtype=dtype,
            attn_implementation="eager",
        )
        .eval()
        .requires_grad_(False)
    )
    if encoder.config.hidden_size != 2560 or encoder.config.num_hidden_layers != 36:
        raise ValueError("unexpected encoder geometry")
    pooled, token_ids = [], []
    with torch.inference_mode():
        for text in texts:
            encoded = tokenizer(
                [text], padding=True, truncation=False, return_tensors="pt"
            )
            ids = active_ids(encoded)[0]
            if len(ids) > 256:
                raise ValueError("encoder input exceeds fixed native text bound")
            row = normalize_features(
                encoder(**encoded, use_cache=False).last_hidden_state,
                encoded["attention_mask"],
            )
            if row.dtype != dtype:
                raise ValueError("normalization changed arithmetic dtype")
            pooled.append(row[0].clone())
            token_ids.append(ids)
        normalized = torch.stack(pooled)
        stored, features = conditioning_features(normalized, profile)
    save_file(
        {"normalized": normalized, "stored.fp16": stored, "reader.features": features},
        str(output / "encoder.safetensors"),
    )
    result["encoder"] = {
        "texts": texts,
        "token_ids": token_ids,
        "tensor_sha256": sha256(output / "encoder.safetensors"),
        "execution": "independent unpadded singleton CPU forwards; raw no-prefix text",
    }
    del encoder, tokenizer, pooled, row, encoded
    gc.collect()
    result["encoder_released_before_decoder"] = True
    config, _ = checkpoint_proof(args.checkpoint)
    source_weights = torch.load(
        args.checkpoint / "memory.pt", map_location="cpu", weights_only=True, mmap=True
    )
    validate_tensors(source_weights, config, split=False, release=True)
    memory_model = (
        load_upstream(args.upstream_source, config, source_weights)
        .to(dtype=dtype)
        .eval()
    )
    memories, memory_tensors = {}, {}
    with deterministic_cpu(), torch.inference_mode():
        for case in cases:
            docs = torch.stack(
                [features[positions[d]] for d in case["documents"]]
            ).unsqueeze(0)
            query = features[positions[case["question"].strip()]].unsqueeze(0)
            mask = torch.zeros(1, len(case["documents"]), dtype=torch.bool)
            memory = memory_model(docs, mask, query)
            if memory.dtype != dtype or not torch.isfinite(memory).all():
                raise ValueError("invalid reader output")
            memories[case["id"]] = memory[0].clone()
            for name, tensor in {
                "documents": docs,
                "question": query,
                "mask": mask,
                "memory_tokens": memory,
            }.items():
                memory_tensors[case["id"] + "." + name] = tensor.contiguous().clone()
        save_file(memory_tensors, str(output / "memory.safetensors"))
    del memory_model, source_weights
    gc.collect()
    result["memory_tensors_sha256"] = sha256(output / "memory.safetensors")
    save_json(output / "result.json", result)
    tokenizer = AutoTokenizer.from_pretrained(
        args.decoder, local_files_only=True, trust_remote_code=False
    )
    decoder = (
        AutoModelForCausalLM.from_pretrained(
            args.decoder,
            local_files_only=True,
            trust_remote_code=False,
            torch_dtype=dtype,
            attn_implementation="eager",
        )
        .eval()
        .requires_grad_(False)
    )
    if (
        decoder.config.vocab_size != 151936
        or decoder.get_output_embeddings().weight.shape[0] != 151936
    ):
        raise ValueError("decoder vocabulary changed")
    if set(decoder.generation_config.eos_token_id) != set(EOS_IDS):
        raise ValueError("decoder EOS policy changed")
    tensors = {}
    with torch.inference_mode():
        for case in cases:
            before = time.monotonic()
            ids, embeddings, prompt = prepare_prompt(
                tokenizer,
                decoder.get_input_embeddings(),
                memories[case["id"]],
                system_prompt=SYSTEM,
                question=case["question"],
            )
            if embeddings.dtype != dtype or len(prompt["memory_positions"]) != 16:
                raise ValueError("memory injection dtype/slot mismatch")
            # vLLM's source tokenizer call uses its default special-token policy.
            if (
                tokenizer(prompt["rendered_prompt"], return_tensors="pt")["input_ids"][
                    0
                ].tolist()
                != ids.tolist()
            ):
                raise ValueError("helper tokenization differs from source default")
            answer, logits = greedy(
                decoder, tokenizer, embeddings, args.max_new_tokens, dtype
            )
            tensors[case["id"] + ".logits"] = logits
            tensors[case["id"] + ".input_ids"] = ids.clone()
            tensors[case["id"] + ".input_embeddings"] = embeddings.clone()
            result["results"].append(
                {
                    "case_id": case["id"],
                    "answer": answer["text"],
                    **answer,
                    "score": score_answer(answer["text"], case),
                    "prompt": prompt,
                    "prompt_ids": ids.tolist(),
                    "seconds": time.monotonic() - before,
                }
            )
            save_json(output / "result.json", result)
    save_file(tensors, str(output / "decoder.safetensors"))
    del decoder, tokenizer
    gc.collect()
    result.update(
        completed=True,
        seconds=time.monotonic() - started,
        decoder_tensors_sha256=sha256(output / "decoder.safetensors"),
        scores=score_suite(load_suite(args.suite), result["results"]),
    )
    save_json(output / "result.json", result)
    return result


def prepare(args) -> dict:
    if args.profiles != ["float32", "bfloat16"]:
        raise ValueError(
            "run both predeclared arithmetic profiles before adapting the protocol"
        )
    suite = load_suite(args.suite)
    selected_cases(suite)
    config, checkpoint = checkpoint_proof(args.checkpoint)
    names = [
        "released_reference.py",
        "quality_cases.py",
        "encoder_reference.py",
        "decoder_reference.py",
        "conditional_reference.py",
        "reference.py",
        "exporter.py",
        "assets.py",
    ]
    sources = {name: sha256(Path(__file__).with_name(name)) for name in names}
    return {
        "schema": "antfly.memorilla.released-reference-protocol.v1",
        "input_paths": {
            name: str(getattr(args, name).resolve())
            for name in ("encoder", "decoder", "checkpoint", "suite", "upstream_source")
        },
        "case_ids": list(CASE_IDS),
        "profiles": args.profiles,
        "max_new_tokens": args.max_new_tokens,
        "threads": args.threads,
        "source_recipe": source_recipe(args.upstream_source),
        "checkpoint": checkpoint,
        "config": config,
        "suite_sha256": sha256(args.suite),
        "source_sha256": sources,
        "base_model_lock_sha256": sha256(LOCK_PATH),
        "fidelity_deviations": [
            "vLLM0.9.1 GPU engine not installed/executed; CPU Transformers4.53.3 eager is a substitute, not bitwise vLLM reproduction",
            "CPU BF16 kernels/reductions can differ from original GPU BF16 kernels; no cross-engine numerical parity claimed",
            "F32 control changes encoder normalization, reader and decoder/KV arithmetic together; it is not a single-component ablation",
            "Singleton unpadded CPU encoder forwards substitute for vLLM variable-length pooled embedding batches",
        ],
        "original_vocabulary": True,
        "resize_token_embeddings": False,
        "injection": "source BF16/F32 delta subtraction followed by scatter-add, not direct row assignment",
        "thinking": False,
        "greedy": True,
        "eos_ids": list(EOS_IDS),
        "torch": torch.__version__,
        "torch_cuda_build": torch.version.cuda,
        "device": "cpu",
        "rollout_qualified": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--encoder", type=Path, required=True)
    parser.add_argument("--decoder", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--upstream-source", type=Path, required=True)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profiles", nargs="+", default=["float32", "bfloat16"])
    parser.add_argument("--max-new-tokens", type=int, default=48)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--run",
        action="store_true",
        help="Explicitly load CPU models; otherwise only prepare a frozen protocol",
    )
    args = parser.parse_args()
    if not 1 <= args.threads <= 16 or not 1 <= args.max_new_tokens <= 128:
        parser.error("invalid bounded execution options")
    protocol = prepare(args)
    args.output.mkdir(parents=True, exist_ok=True)
    protocol_path = args.output / "protocol.json"
    if protocol_path.exists():
        if read_json(protocol_path) != protocol:
            raise ValueError(
                "prepared source/protocol changed; preserve old output and choose a new directory"
            )
    else:
        save_json(protocol_path, protocol)
    if not args.run:
        print(
            json.dumps(
                {
                    "prepared": True,
                    "models_loaded": False,
                    "protocol_sha256": sha256(protocol_path),
                }
            )
        )
        return
    if (args.output / "result.json").exists():
        raise ValueError("preserve previous run receipts")
    record = {
        "completed": False,
        "protocol_sha256": sha256(protocol_path),
        "profiles": [],
    }
    save_json(args.output / "result.json", record)
    before = time.monotonic()
    record["encoder_proof"] = verify_base_model(args.encoder, "encoder")
    record["decoder_proof"] = verify_decoder(args.decoder)
    for profile in args.profiles:
        record["profiles"].append(
            run_profile(args, profile, protocol, selected_cases(load_suite(args.suite)))
        )
        save_json(args.output / "result.json", record)
    if prepare(args) != protocol:
        raise ValueError("source or input changed during reproduction")
    record.update(
        completed=True,
        seconds=time.monotonic() - before,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        versions={
            name: importlib.metadata.version(name)
            for name in ("torch", "transformers", "safetensors")
        },
        rollout_qualified=False,
        original_vllm_engine_executed=False,
    )
    save_json(args.output / "result.json", record)
    print(
        json.dumps(
            {"completed": True, "receipt_sha256": sha256(args.output / "result.json")}
        )
    )


if __name__ == "__main__":
    main()
