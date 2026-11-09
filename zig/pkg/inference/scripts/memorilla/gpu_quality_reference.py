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

"""Bounded GPU quality/latency reference; not a reproduction of vLLM throughput.

Uses the pinned upstream reader and unresized decoder. Document ingestion is
timed separately from warm question -> reader -> autoregressive answer. No gold
labels enter model inputs. F32 disables TF32. BF16 is a separate arithmetic
experiment, never evidence that the native F32 gate can be widened.
"""

from __future__ import annotations

import argparse
import ast
import gc
import importlib.metadata
import json
from pathlib import Path
import shutil
import statistics
import re
import string
import time

import torch
from safetensors.torch import save_file

from assets import verify_base_model
from decoder_reference import DEFAULT_SYSTEM, prepare_prompt
from encoder_reference import active_ids, normalize_features
from exporter import read_json, sha256, validate_config, validate_tensors
from quality_cases import DEFAULT_SUITE, load_suite, score_answer, score_suite
from quality_eval import validate_store, document_key
from reference import load_upstream
from released_reference import checkpoint_proof, source_recipe, EOS_IDS, SYSTEM


def save(path, value):
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def synchronize():
    torch.cuda.synchronize()


def elapsed(start):
    synchronize()
    return time.perf_counter() - start


def condition(value, dtype):
    return value.half().bfloat16().to(device="cuda", dtype=dtype)


def validate_cache(cache, dtype, device_type="cuda"):
    layers = cache.to_legacy_cache() if hasattr(cache, "to_legacy_cache") else cache
    if not layers or any(
        t.dtype != dtype or t.device.type != device_type
        for layer in layers
        for t in layer
    ):
        raise ValueError("KV cache precision/device mismatch")


def released_cases(path, benchmark, source):
    """Read a preselected bounded sample, keeping query and suffix separate."""
    value = json.loads(path.read_text())
    constants = {}
    tree = ast.parse((source / "memorilla/benchmarks.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = node.value.value
    prefix = "FACT_CHECK" if benchmark == "factkg" else "TRIVIA"
    system, suffix = constants[prefix + "_PROMPT"], constants[prefix + "_SUFFIX"]
    cases = []
    for row in value["rows"]:
        if not 1 <= len(row["documents"]) <= 64:
            raise ValueError("sample exceeds native document bound")
        cases.append(
            {
                "id": benchmark + "_" + str(row["index"]),
                "question": row["question"].strip(),
                "documents": row["documents"],
                "decoder_question": row["question"].strip() + suffix,
                "benchmark_answer": row["answer"],
            }
        )
    return {"cases": cases, "sample_provenance": value}, cases, system


def score_case(answer, case):
    if "operational_answer" in case:

        def canonical(text):
            return " ".join(text.strip().rstrip(".").split()).casefold()

        return {
            "correct": canonical(answer) == canonical(case["operational_answer"]),
            "metric": "operational_exact_match",
        }
    if "benchmark_answer" not in case:
        return score_answer(answer, case)

    # Same normalisation as upstream metrics.normalize_answer. FactKG uses
    # exact True/False here, a stricter diagnostic than upstream choice scoring.
    def normalise(text):
        text = "".join(c for c in text.lower() if c not in string.punctuation)
        return " ".join(re.sub(r"\b(a|an|the)\b", " ", text).split())

    return {
        "correct": normalise(answer) == normalise(case["benchmark_answer"]),
        "metric": "normalised_exact_match",
    }


def candidate_proof(checkpoint, expected):
    """Explicit unqualified candidate identity; never impersonate a release."""
    config = validate_config(read_json(checkpoint / "config.json"), release=True)
    actual = sha256(checkpoint / "memory.pt")
    if len(expected) != 64 or actual != expected:
        raise ValueError("candidate checkpoint digest differs")
    weights = torch.load(
        checkpoint / "memory.pt", map_location="cpu", weights_only=True, mmap=True
    )
    validate_tensors(weights, config, split=False, release=True)
    return config, {
        "kind": "unqualified-operational-candidate",
        "memory_pt_sha256": actual,
        "config_sha256": sha256(checkpoint / "config.json"),
    }


def operational_cases(path, split, development_receipt=None):
    from train_operational_reader import validate_curriculum

    rows = validate_curriculum(read_json(path))
    if split == "test":
        if development_receipt is None:
            raise ValueError("final inference requires an exact development receipt")
        receipt = read_json(development_receipt)
        scores = receipt.get("scores", {})
        expected = {r["id"]: r for r in rows if r["split"] == "dev"}
        actual = receipt.get("results", [])
        ids = [r.get("case_id") for r in actual]
        answers_pass = (
            len(ids) == len(expected)
            and set(ids) == set(expected)
            and all(
                score_case(
                    r.get("answer", ""),
                    {"operational_answer": expected[r["case_id"]]["answer"]},
                )["correct"]
                for r in actual
            )
        )
        if (
            not receipt.get("completed")
            or scores.get("total", 0) != 96
            or scores.get("correct") != scores.get("total")
            or not answers_pass
            or receipt.get("protocol", {}).get("suite_sha256") != sha256(path)
        ):
            raise ValueError("development gate has not passed for this curriculum")
    cases = [
        {
            "id": r["id"],
            "question": r["question"],
            "documents": r["documents"],
            "family": r["family"],
            "operational_answer": r["answer"],
        }
        for r in rows
        if r["split"] == split
    ]
    return {"cases": cases}, cases


@torch.inference_mode()
def generate(model, tokenizer, embeddings, maximum):
    synchronize()
    started = time.perf_counter()
    output = model(
        inputs_embeds=embeddings.unsqueeze(0), use_cache=True, logits_to_keep=1
    )
    validate_cache(output.past_key_values, embeddings.dtype)
    if output.logits.shape[-1] != 151936:
        raise ValueError("unresized decoder vocabulary required")
    prefill = elapsed(started)
    ids = []
    decode_started = time.perf_counter()
    for step in range(maximum):
        token = int(output.logits[0, -1].argmax().item())
        ids.append(token)
        if token in EOS_IDS or step + 1 == maximum:
            break
        cache = output.past_key_values
        del output
        output = model(
            input_ids=torch.tensor([[token]], device="cuda"),
            past_key_values=cache,
            use_cache=True,
            logits_to_keep=1,
        )
        validate_cache(output.past_key_values, embeddings.dtype)
    decode = elapsed(decode_started)
    del output
    return {
        "answer": tokenizer.decode(ids, skip_special_tokens=True).strip(),
        "token_ids": ids,
        "finish_reason": "eos" if ids[-1] in EOS_IDS else "length",
        "kv_dtype": str(embeddings.dtype),
        "kv_device": embeddings.device.type,
        "prefill_seconds": prefill,
        "decode_seconds": decode,
        "decoder_seconds": prefill + decode,
    }


def main():
    from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer

    p = argparse.ArgumentParser(description=__doc__)
    for name in ("encoder", "decoder", "checkpoint", "upstream-source", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    p.add_argument("--released-benchmark", choices=("factkg", "triviaqa"))
    p.add_argument("--operational-curriculum", action="store_true")
    p.add_argument("--split", choices=("dev", "test"), default="dev")
    p.add_argument("--development-receipt", type=Path)
    p.add_argument("--candidate-sha256")
    p.add_argument("--dtype", choices=("float32", "bfloat16"), default="float32")
    p.add_argument("--system", choices=("native", "upstream"), default="native")
    p.add_argument("--feature-store", type=Path)
    p.add_argument("--case-ids", nargs="+")
    p.add_argument("--max-new-tokens", type=int, default=48)
    p.add_argument("--repeats", type=int, default=1)
    args = p.parse_args()
    if not 1 <= args.max_new_tokens <= 128 or not 1 <= args.repeats <= 10:
        p.error("bounded generation/repeat count required")
    args.output.mkdir(parents=True, exist_ok=False)
    source_archive = args.output / "source"
    source_archive.mkdir()
    source_root = Path(__file__).resolve().parent
    source_files = sorted(
        p for p in source_root.rglob("*") if p.suffix in {".py", ".json"}
    )
    source_hashes = {str(p.relative_to(source_root)): sha256(p) for p in source_files}
    for path in source_files:
        relative = path.relative_to(source_root)
        destination = source_archive / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
        if sha256(destination) != source_hashes[str(relative)]:
            raise ValueError("source changed while archiving; no model was loaded")
    save(args.output / "source-manifest.json", source_hashes)
    system = DEFAULT_SYSTEM if args.system == "native" else SYSTEM
    if args.operational_curriculum:
        if args.released_benchmark or args.case_ids:
            p.error("operational gates require a complete split")
        suite, cases = operational_cases(
            args.suite, args.split, args.development_receipt
        )
    elif args.released_benchmark:
        suite, cases, system = released_cases(
            args.suite, args.released_benchmark, args.upstream_source
        )
    else:
        suite = load_suite(args.suite)
        cases = suite["cases"]
    if args.case_ids:
        lookup = {c["id"]: c for c in cases}
        cases = [lookup[c] for c in args.case_ids]
    dtype = getattr(torch, args.dtype)
    torch.set_num_threads(4)
    torch.manual_seed(0)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")
    config, proof = (
        candidate_proof(args.checkpoint, args.candidate_sha256)
        if args.candidate_sha256
        else checkpoint_proof(args.checkpoint)
    )
    if args.operational_curriculum and args.split == "test":
        development = read_json(args.development_receipt)
        if (
            development["protocol"]["checkpoint"] != proof
            or development["protocol"]["dtype"] != args.dtype
            or development["protocol"]["system"] != args.system
        ):
            raise ValueError(
                "final model/precision/prompt differs from passed development run"
            )
    recipe = source_recipe(args.upstream_source)
    protocol = {
        "schema": "antfly.memorilla.gpu-quality-reference.v1",
        "engine": "transformers-cuda-eager",
        "original_vllm_engine_executed": False,
        "dtype": args.dtype,
        "tf32": False,
        "system": args.system,
        "system_text": system,
        "released_benchmark": args.released_benchmark,
        "operational_split": args.split if args.operational_curriculum else None,
        "suite_sha256": sha256(args.suite),
        "case_ids": [c["id"] for c in cases],
        "runner_sha256": sha256(Path(__file__)),
        "checkpoint": proof,
        "upstream": recipe,
        "max_new_tokens": args.max_new_tokens,
        "repeats": args.repeats,
        "document_ingestion_in_generation_latency": False,
        "gpu": torch.cuda.get_device_name(),
        "versions": {
            n: importlib.metadata.version(n)
            for n in ("torch", "transformers", "safetensors")
        },
        "sources": source_hashes,
    }
    result = {
        "protocol": protocol,
        "completed": False,
        "rollout_qualified": False,
        "results": [],
        "warmup_results": [],
    }
    save(args.output / "result.json", result)
    result["encoder_proof"] = verify_base_model(args.encoder, "encoder")
    result["decoder_proof"] = verify_base_model(args.decoder, "decoder")
    texts = list(
        dict.fromkeys(
            [d for c in cases for d in c["documents"]]
            + [c["question"].strip() for c in cases]
        )
    )
    stored = None
    if args.feature_store:
        store = json.loads(args.feature_store.read_text())
        stored = validate_store(store, [d for c in cases for d in c["documents"]])
        protocol["feature_store_sha256"] = sha256(args.feature_store)
        protocol["document_features"] = (
            "saved native F32; FP16 -> BF16 -> selected dtype"
        )
    else:
        protocol["document_features"] = (
            "GPU encoder in selected dtype; FP16 -> BF16 -> selected dtype"
        )
    started = time.perf_counter()
    tokenizer = AutoTokenizer.from_pretrained(args.encoder, local_files_only=True)
    encoder = (
        AutoModel.from_pretrained(
            args.encoder,
            local_files_only=True,
            torch_dtype=dtype,
            attn_implementation="eager",
        )
        .eval()
        .requires_grad_(False)
        .to("cuda")
    )
    result["encoder_load_seconds"] = elapsed(started)
    features, encoded_rows, ingestion, queries = {}, {}, {}, {}
    with torch.inference_mode():
        # One explicit untimed warmup; same token input, no answer labels.
        warm = tokenizer([texts[0]], return_tensors="pt").to("cuda")
        encoder(**warm, use_cache=False)
        synchronize()
        for text in texts:
            if stored is not None and document_key(text) in stored:
                features[text] = condition(
                    torch.tensor(stored[document_key(text)]), dtype
                )
                continue
            synchronize()
            before = time.perf_counter()
            tokens = tokenizer([text], return_tensors="pt").to("cuda")
            ids = active_ids(tokens)[0]
            if len(ids) > 256:
                raise ValueError("encoder input exceeds native 256-token bound")
            normalized = normalize_features(
                encoder(**tokens, use_cache=False).last_hidden_state,
                tokens["attention_mask"],
            )
            features[text] = condition(normalized[0], dtype)
            seconds = elapsed(before)
            encoded_rows[text] = {"token_ids": ids, "seconds": seconds}
            (queries if text in {c["question"].strip() for c in cases} else ingestion)[
                text
            ] = seconds
    save_file(
        {str(i): features[t].cpu().contiguous() for i, t in enumerate(texts)},
        str(args.output / "features.safetensors"),
    )
    result["encoder_rows"] = encoded_rows
    result["feature_texts"] = texts
    result["features_sha256"] = sha256(args.output / "features.safetensors")
    result["document_ingestion_seconds"] = sum(ingestion.values())
    encoder_tokenizer = tokenizer
    del normalized, tokens, warm
    gc.collect()
    torch.cuda.empty_cache()
    weights = torch.load(
        args.checkpoint / "memory.pt", map_location="cpu", weights_only=True, mmap=True
    )
    reader = (
        load_upstream(args.upstream_source, config, weights)
        .to(device="cuda", dtype=dtype)
        .eval()
    )
    del weights
    tokenizer = AutoTokenizer.from_pretrained(args.decoder, local_files_only=True)
    started = time.perf_counter()
    decoder = (
        AutoModelForCausalLM.from_pretrained(
            args.decoder,
            local_files_only=True,
            torch_dtype=dtype,
            attn_implementation="eager",
        )
        .eval()
        .requires_grad_(False)
        .to("cuda")
    )
    if decoder.config.vocab_size != 151936:
        raise ValueError("unresized decoder vocabulary required")
    result["decoder_load_seconds"] = elapsed(started)
    save(args.output / "result.json", result)
    with torch.inference_mode():
        for repeat in range(-1, args.repeats):
            for case in cases[:1] if repeat < 0 else cases:
                synchronize()
                started = time.perf_counter()
                tokens = encoder_tokenizer(
                    [case["question"].strip()], return_tensors="pt"
                ).to("cuda")
                query = condition(
                    normalize_features(
                        encoder(**tokens, use_cache=False).last_hidden_state,
                        tokens["attention_mask"],
                    )[0],
                    dtype,
                )[None]
                query_seconds = elapsed(started)
                docs = torch.stack([features[d] for d in case["documents"]])[None]
                mask = torch.zeros(
                    (1, len(case["documents"])), device="cuda", dtype=torch.bool
                )
                memory = reader(docs, mask, query)[0]
                reader_seconds = elapsed(started) - query_seconds
                ids, embeds, prompt = prepare_prompt(
                    tokenizer,
                    decoder.get_input_embeddings(),
                    memory,
                    system_prompt=system,
                    question=case.get("decoder_question", case["question"]),
                    device="cuda",
                )
                answer = generate(decoder, tokenizer, embeds, args.max_new_tokens)
                row = {
                    "case_id": case["id"],
                    "repeat": repeat,
                    **answer,
                    "reader_seconds": reader_seconds,
                    "generation_seconds": elapsed(started),
                    "latency_contract": "warm question encoding, reader, framing and decoding; encoder and decoder resident together",
                    "query_seconds": query_seconds,
                    "prompt_ids": ids.tolist(),
                    "prompt": prompt,
                    "score": score_case(answer["answer"], case),
                }
                result["warmup_results" if repeat < 0 else "results"].append(row)
                save(args.output / "result.json", result)
    if args.operational_curriculum:
        rows = result["results"][: len(cases)]
        result["scores"] = {
            "correct": sum(r["score"]["correct"] for r in rows),
            "total": len(rows),
            "gate_passed": all(r["score"]["correct"] for r in rows),
            "scope": "synthetic disjoint operational worlds; not production qualification",
        }
    elif args.released_benchmark:
        rows = result["results"][: len(cases)]
        result["scores"] = {
            "correct": sum(r["score"]["correct"] for r in rows),
            "total": len(rows),
            "scope": "document-bound subset; not the full released benchmark",
            "factkg_choice_scoring_reproduced": False,
        }
    else:
        result["scores"] = score_suite(suite, result["results"][: len(cases)])
    result["warm_generation_median_seconds"] = statistics.median(
        r["generation_seconds"] for r in result["results"]
    )
    result["cuda_peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
    result["completed"] = True
    save(args.output / "result.json", result)
    print(
        json.dumps(
            {
                "completed": True,
                "scores": result["scores"],
                "median_seconds": result["warm_generation_median_seconds"],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
