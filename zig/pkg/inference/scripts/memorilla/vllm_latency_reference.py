#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Warm vLLM question -> reader -> answer timing with prepared document vectors.

Uses the official encoder and MemoryVLLM.generate, one live question-embedding
call per request, and the native system instruction. No document ingestion or
tensor-capture hooks run inside the timed region. BF16 is a different arithmetic
profile from the qualified native F32 API; this is a serving-path comparison.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
from pathlib import Path
import shutil
import statistics
import sys
import time

import numpy as np
import torch

from assets import verify_base_model
from decoder_reference import DEFAULT_SYSTEM
from exporter import sha256
from gpu_quality_reference import save
from quality_cases import DEFAULT_SUITE, load_suite, score_suite
from quality_eval import document_key, validate_store
from released_reference import checkpoint_proof, source_recipe


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in (
        "encoder",
        "decoder",
        "checkpoint",
        "upstream-source",
        "feature-store",
        "output",
    ):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    p.add_argument("--repeats", type=int, default=2)
    args = p.parse_args()
    if not 1 <= args.repeats <= 4:
        p.error("bounded repeat count required")
    if (
        importlib.metadata.version("vllm").split("+")[0] != "0.9.1"
        or torch.__version__.split("+")[0] != "2.7.0"
    ):
        raise ValueError("upstream vLLM/PyTorch versions required")
    recipe = source_recipe(args.upstream_source)
    _, proof = checkpoint_proof(args.checkpoint)
    suite = load_suite(args.suite)
    cases = suite["cases"]
    store = validate_store(
        json.loads(args.feature_store.read_text()),
        [d for c in cases for d in c["documents"]],
    )
    args.output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parent
    sources = {}
    for file in sorted(source.rglob("*")):
        if file.suffix not in {".py", ".json"}:
            continue
        relative = file.relative_to(source)
        target = args.output / "source" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, target)
        sources[str(relative)] = sha256(file)
        if sha256(target) != sources[str(relative)]:
            raise ValueError("source changed while archiving")
    save(args.output / "source-manifest.json", sources)
    result = {
        "completed": False,
        "protocol": {
            "schema": "antfly.memorilla.vllm-latency.v1",
            "upstream": recipe,
            "checkpoint": proof,
            "feature_store_sha256": sha256(args.feature_store),
            "suite_sha256": sha256(args.suite),
            "system_text": DEFAULT_SYSTEM,
            "dtype": "bfloat16",
            "max_new_tokens": 48,
            "repeats": args.repeats,
            "query_encoder_calls_per_request": 1,
            "document_ingestion_timed": False,
            "startup_timed": False,
            "tensor_capture_hooks": False,
            "timed_region": "question embedding, FP16 storage/BF16 conditioning, reader, prompt framing, vLLM scheduling and greedy decoding; resident models; prepared CPU document vectors",
            "comparison_limit": "in-process BF16 vLLM versus managed F32 native HTTP; not isolated kernel or matched-precision comparison",
            "versions": {
                n: importlib.metadata.version(n)
                for n in ("vllm", "torch", "transformers", "numpy")
            },
            "sources": sources,
        },
        "results": [],
    }
    save(args.output / "result.json", result)
    result["encoder_proof"] = verify_base_model(args.encoder, "encoder")
    result["decoder_proof"] = verify_base_model(args.decoder, "decoder")
    sys.path.insert(0, str(args.upstream_source.resolve()))
    from memorilla.vllm import MemoryVLLM
    from memorilla.data import build_messages, apply_chat_template

    if (
        Path(inspect.getfile(MemoryVLLM)).resolve()
        != (args.upstream_source / "memorilla/vllm.py").resolve()
    ):
        raise ValueError("different upstream wrapper imported")
    torch.set_num_threads(4)
    torch.manual_seed(0)
    model = MemoryVLLM.from_pretrained(
        args.checkpoint.resolve(),
        encoder=str(args.encoder.resolve()),
        decoder=str(args.decoder.resolve()),
    )
    result["gpu"] = torch.cuda.get_device_name()
    for name, engine in (
        ("encoder", model.encoder.llm_engine),
        ("decoder", model.engine.llm_engine),
    ):
        assert engine.model_config.dtype == torch.bfloat16
        assert engine.cache_config.enable_prefix_caching in (False, None)
        result[name + "_engine"] = {
            "class": type(engine).__module__ + "." + type(engine).__name__,
            "dtype": str(engine.model_config.dtype),
            "max_model_len": engine.model_config.max_model_len,
            "enforce_eager": engine.model_config.enforce_eager,
            "enable_prefix_caching": engine.cache_config.enable_prefix_caching,
        }
    # Exactly the same stored document values as the native request body, cast
    # through the released FP16-storage/BF16-input recipe. Keep them on CPU so
    # MemoryVLLM.generate includes their host-to-device transfer on each call.
    documents = {
        c["id"]: torch.tensor(
            [store[document_key(d)] for d in c["documents"]], dtype=torch.float32
        )
        .half()
        .bfloat16()[None]
        for c in cases
    }
    masks = {
        c["id"]: torch.zeros((1, len(c["documents"])), dtype=torch.bool) for c in cases
    }
    capture = {}
    original = model.engine.generate

    def observed_generate(*positional, **keywords):
        torch.cuda.synchronize()
        capture["decoder_started"] = time.perf_counter()
        outputs = original(*positional, **keywords)
        torch.cuda.synchronize()
        capture["decoder_finished"] = time.perf_counter()
        assert len(outputs) == len(outputs[0].outputs) == 1
        completion = outputs[0].outputs[0]
        capture["token_ids"] = list(completion.token_ids)
        capture["finish_reason"] = completion.finish_reason
        return outputs

    model.engine.generate = observed_generate
    try:
        for repeat in range(-1, args.repeats):
            for case in cases[:1] if repeat < 0 else cases:
                capture.clear()
                torch.cuda.synchronize()
                started = time.perf_counter()
                question = case["question"].strip()
                outputs = model.encoder.embed([question], use_tqdm=False)
                query = torch.from_numpy(
                    np.asarray([outputs[0].outputs.embedding], dtype=np.float16)
                )
                torch.cuda.synchronize()
                query_finished = time.perf_counter()
                messages = build_messages(
                    question, DEFAULT_SYSTEM, model.memory.num_memories
                )
                rendered = apply_chat_template(
                    model.tokenizer, messages, add_generation_prompt=True
                )
                tokens = model.tokenizer(rendered, return_tensors="pt")
                (answer,) = model.generate(
                    tokens["input_ids"],
                    tokens["attention_mask"],
                    documents[case["id"]],
                    masks[case["id"]],
                    query,
                    max_new_tokens=48,
                )
                torch.cuda.synchronize()
                finished = time.perf_counter()
                row = {
                    "case_id": case["id"],
                    "repeat": repeat,
                    "answer": answer.strip(),
                    "token_ids": capture["token_ids"],
                    "finish_reason": capture["finish_reason"],
                    "seconds": finished - started,
                    "query_seconds": query_finished - started,
                    "reader_and_framing_seconds": capture["decoder_started"]
                    - query_finished,
                    "decoder_seconds": capture["decoder_finished"]
                    - capture["decoder_started"],
                    "prompt_ids": tokens["input_ids"][0].tolist(),
                }
                if repeat < 0:
                    result["warmup"] = row
                else:
                    if repeat:
                        first = result["results"][
                            [c["id"] for c in cases].index(case["id"])
                        ]
                        assert (
                            row["token_ids"] == first["token_ids"]
                        ), "nondeterministic repeated output"
                    result["results"].append(row)
                save(args.output / "result.json", result)
        result["median_seconds"] = statistics.median(
            r["seconds"] for r in result["results"]
        )
        result["pass_medians_seconds"] = [
            statistics.median(
                r["seconds"] for r in result["results"] if r["repeat"] == i
            )
            for i in range(args.repeats)
        ]
        result["component_medians_seconds"] = {
            key: statistics.median(r[key] for r in result["results"])
            for key in (
                "query_seconds",
                "reader_and_framing_seconds",
                "decoder_seconds",
            )
        }
        result["scores"] = score_suite(suite, result["results"][: len(cases)])
        result["completed"] = True
        save(args.output / "result.json", result)
        print(
            json.dumps(
                {
                    k: result[k]
                    for k in (
                        "completed",
                        "median_seconds",
                        "pass_medians_seconds",
                        "component_medians_seconds",
                    )
                }
            ),
            flush=True,
        )
    finally:
        model.engine.generate = original


if __name__ == "__main__":
    main()
