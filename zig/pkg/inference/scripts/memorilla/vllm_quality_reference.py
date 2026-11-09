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

"""Run the pinned upstream Memorilla wrapper through actual vLLM engines.

Requires an explicitly prepared local environment and immutable model assets.
Capturing hooks observe inputs/outputs without replacing any model operation.
Timings include diagnostic capture and are not a native performance comparison.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
from pathlib import Path
import shutil
import sys
import time
import traceback

import torch
from safetensors.torch import save_file

from assets import verify_base_model
from decoder_reference import DEFAULT_SYSTEM
from exporter import sha256
from gpu_quality_reference import released_cases, save, score_case
from quality_cases import DEFAULT_SUITE, load_suite, score_suite
from released_reference import SYSTEM, checkpoint_proof, source_recipe


EOS_IDS = {151643, 151645}


def content_ids(ids):
    """Compare content separately from engines' terminal-token conventions."""
    result = list(ids)
    if result and result[-1] in EOS_IDS:
        result.pop()
    return result


def compare_results(rows, receipt):
    reference = [r for r in receipt["results"] if r.get("repeat", 0) == 0]
    expected = {r["case_id"]: r for r in reference}
    actual_ids = {r["case_id"] for r in rows}
    if (
        len(expected) != len(reference)
        or len(actual_ids) != len(rows)
        or set(expected) != actual_ids
    ):
        raise ValueError("comparison case IDs differ")
    return {
        "total": len(rows),
        "answer_equal": sum(
            r["answer"] == expected[r["case_id"]]["answer"] for r in rows
        ),
        "raw_token_ids_equal": sum(
            r["token_ids"] == expected[r["case_id"]]["token_ids"] for r in rows
        ),
        "content_token_ids_equal": sum(
            content_ids(r["token_ids"])
            == content_ids(expected[r["case_id"]]["token_ids"])
            for r in rows
        ),
        "prompt_ids_equal": (
            sum(r["prompt_ids"] == expected[r["case_id"]]["prompt_ids"] for r in rows)
            if all(
                "prompt_ids" in r and "prompt_ids" in expected[r["case_id"]]
                for r in rows
            )
            else None
        ),
        "differences": [
            {
                "case_id": r["case_id"],
                "vllm_answer": r["answer"],
                "reference_answer": expected[r["case_id"]]["answer"],
                "vllm_token_ids": r["token_ids"],
                "reference_token_ids": expected[r["case_id"]]["token_ids"],
            }
            for r in rows
            if content_ids(r["token_ids"])
            != content_ids(expected[r["case_id"]]["token_ids"])
        ],
    }


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("encoder", "decoder", "checkpoint", "upstream-source", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    p.add_argument("--released-benchmark", choices=("factkg", "triviaqa"))
    p.add_argument("--system", choices=("native", "upstream"), default="upstream")
    p.add_argument("--compare", type=Path, action="append", default=[])
    p.add_argument("--max-new-tokens", type=int, default=48)
    p.add_argument("--followup-suite", type=Path)
    p.add_argument("--followup-output", type=Path)
    p.add_argument("--followup-benchmark", choices=("factkg", "triviaqa"))
    p.add_argument("--followup-compare", type=Path, action="append", default=[])
    args = p.parse_args()
    if not 1 <= args.max_new_tokens <= 128:
        p.error("bounded generation required")
    if any(
        (args.followup_suite, args.followup_output, args.followup_benchmark)
    ) and not all((args.followup_suite, args.followup_output, args.followup_benchmark)):
        p.error("followup suite, output and benchmark must be supplied together")
    return args


def run(args, model=None):
    if importlib.metadata.version("vllm").split("+")[0] != "0.9.1":
        raise ValueError("this reference requires the upstream-pinned vLLM 0.9.1")
    if torch.__version__.split("+")[0] != "2.7.0":
        raise ValueError("this reference requires the upstream-pinned PyTorch 2.7.0")
    recipe = source_recipe(args.upstream_source)
    config, proof = checkpoint_proof(args.checkpoint)
    args.output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parent
    sources = {
        str(f.relative_to(source)): sha256(f)
        for f in sorted(source.rglob("*"))
        if f.suffix in {".py", ".json"}
    }
    for name, digest in sources.items():
        dest = args.output / "source" / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, dest)
        if sha256(dest) != digest:
            raise ValueError("source changed while archiving")
    save(args.output / "source-manifest.json", sources)
    system = SYSTEM if args.system == "upstream" else DEFAULT_SYSTEM
    if args.released_benchmark:
        suite, cases, system = released_cases(
            args.suite, args.released_benchmark, args.upstream_source
        )
    else:
        suite = load_suite(args.suite)
        cases = suite["cases"]
    receipt = {
        "completed": False,
        "original_vllm_engine_executed": False,
        "production_qualified": False,
        "protocol": {
            "schema": "antfly.memorilla.vllm-quality-reference.v1",
            "upstream": recipe,
            "checkpoint": proof,
            "dtype": "bfloat16",
            "system": args.system,
            "system_text": system,
            "released_benchmark": args.released_benchmark,
            "suite_sha256": sha256(args.suite),
            "case_ids": [c["id"] for c in cases],
            "max_new_tokens": args.max_new_tokens,
            "sources": sources,
            "versions": {
                n: importlib.metadata.version(n)
                for n in ("vllm", "torch", "transformers", "safetensors", "numpy")
            },
            "timing_scope": "diagnostic captures included; not a native performance comparison",
            "engine_settings": "unchanged upstream constructor defaults; BF16 encoder and decoder, greedy generation",
        },
        "results": [],
    }
    destination = args.output / "result.json"
    save(destination, receipt)
    handles = []
    original_generate = None
    try:
        receipt["encoder_proof"] = verify_base_model(args.encoder, "encoder")
        receipt["decoder_proof"] = verify_base_model(args.decoder, "decoder")
        sys.path.insert(0, str(args.upstream_source.resolve()))
        from memorilla.vllm import MemoryVLLM
        from memorilla.data import build_messages, apply_chat_template
        from memorilla.embeddings import embed_texts

        if (
            Path(inspect.getfile(MemoryVLLM)).resolve()
            != (args.upstream_source / "memorilla/vllm.py").resolve()
        ):
            raise ValueError("imported a different upstream wrapper")
        torch.set_num_threads(4)
        torch.manual_seed(0)
        started = time.perf_counter()
        receipt["reused_engines"] = model is not None
        if model is None:
            model = MemoryVLLM.from_pretrained(
                args.checkpoint.resolve(),
                encoder=str(args.encoder.resolve()),
                decoder=str(args.decoder.resolve()),
            )
        torch.cuda.synchronize()
        receipt["load_seconds"] = time.perf_counter() - started
        receipt["gpu"] = torch.cuda.get_device_name()
        receipt["encoder_engine_class"] = (
            type(model.encoder.llm_engine).__module__
            + "."
            + type(model.encoder.llm_engine).__name__
        )
        receipt["decoder_engine_class"] = (
            type(model.engine.llm_engine).__module__
            + "."
            + type(model.engine.llm_engine).__name__
        )
        for name, engine in (
            ("encoder", model.encoder.llm_engine),
            ("decoder", model.engine.llm_engine),
        ):
            model_config = engine.model_config
            receipt[name + "_config"] = {
                "dtype": str(model_config.dtype),
                "task": model_config.task,
                "max_model_len": model_config.max_model_len,
                "vocab_size": model_config.hf_config.vocab_size,
                "pooler_config": repr(getattr(model_config, "pooler_config", None)),
                "enforce_eager": model_config.enforce_eager,
                "kv_cache_dtype": engine.cache_config.cache_dtype,
                "gpu_memory_utilization": engine.cache_config.gpu_memory_utilization,
            }
            if model_config.dtype != torch.bfloat16:
                raise ValueError("upstream engine did not select BF16")
        if receipt["decoder_config"]["vocab_size"] != 151936:
            raise ValueError("upstream decoder vocabulary was resized")
        capture = {}

        def memory_hook(module, inputs, output):
            capture["documents"] = inputs[0].detach().cpu().contiguous()
            capture["query"] = inputs[2].detach().cpu().contiguous()
            capture["memory"] = output.detach().cpu().contiguous()

        def embedding_hook(module, inputs, output):
            capture["prompt_ids"] = inputs[0].detach().cpu().tolist()

        handles.append(model.memory.register_forward_hook(memory_hook))
        handles.append(model.embed_tokens.register_forward_hook(embedding_hook))
        original_generate = model.engine.generate

        def observed_generate(*positional, **keywords):
            requests = positional[0] if positional else keywords["prompts"]
            if len(requests) != 1:
                raise ValueError("single diagnostic request required")
            capture["prompt_embeddings"] = (
                requests[0]["prompt_embeds"].detach().cpu().contiguous()
            )
            outputs = original_generate(*positional, **keywords)
            if len(outputs) != 1 or len(outputs[0].outputs) != 1:
                raise ValueError("single greedy completion required")
            completion = outputs[0].outputs[0]
            capture["completion"] = {
                "token_ids": list(completion.token_ids),
                "finish_reason": completion.finish_reason,
                "stop_reason": completion.stop_reason,
                "text": completion.text,
            }
            return outputs

        model.engine.generate = observed_generate
        for case in cases:
            capture.clear()
            started = time.perf_counter()
            if not args.released_benchmark:
                answer = model.answer(
                    case["question"],
                    case["documents"],
                    system_prompt=system,
                    max_new_tokens=args.max_new_tokens,
                )
            else:
                embeddings = torch.from_numpy(
                    embed_texts(model.encoder, [*case["documents"], case["question"]])
                )
                messages = build_messages(
                    case["question"],
                    system,
                    model.memory.num_memories,
                    suffix=case["decoder_question"][len(case["question"]) :],
                )
                rendered = apply_chat_template(
                    model.tokenizer, messages, add_generation_prompt=True
                )
                tokens = model.tokenizer(rendered, return_tensors="pt")
                (answer,) = model.generate(
                    tokens["input_ids"],
                    tokens["attention_mask"],
                    embeddings[None, :-1],
                    torch.zeros(1, len(case["documents"]), dtype=torch.bool),
                    embeddings[-1:],
                    max_new_tokens=args.max_new_tokens,
                )
                answer = answer.strip()
            torch.cuda.synchronize()
            completion = capture["completion"]
            if answer != completion["text"].strip():
                raise ValueError("wrapper answer differs from actual vLLM completion")
            if capture["memory"].shape != (
                1,
                config["num_memories"],
                config["output_dim"],
            ):
                raise ValueError("upstream memory shape differs")
            feature_path = args.output / (case["id"] + ".safetensors")
            if any(
                capture[k].dtype != torch.bfloat16
                for k in ("documents", "query", "memory", "prompt_embeddings")
            ):
                raise ValueError("upstream tensor precision differs from BF16")
            save_file(
                {
                    k: capture[k]
                    for k in ("documents", "query", "memory", "prompt_embeddings")
                },
                str(feature_path),
            )
            receipt["results"].append(
                {
                    "case_id": case["id"],
                    "answer": answer,
                    **completion,
                    "prompt_ids": capture["prompt_ids"][0],
                    "observed_seconds": time.perf_counter() - started,
                    "score": score_case(answer, case),
                    "feature_sha256": sha256(feature_path),
                }
            )
            receipt["original_vllm_engine_executed"] = True
            save(destination, receipt)
            print(
                json.dumps(
                    {
                        "case_id": case["id"],
                        "answer": answer,
                        "correct": receipt["results"][-1]["score"]["correct"],
                    }
                ),
                flush=True,
            )
        if args.released_benchmark:
            receipt["scores"] = {
                "correct": sum(r["score"]["correct"] for r in receipt["results"]),
                "total": len(cases),
                "scope": "bounded subset, not full published benchmark",
            }
        else:
            receipt["scores"] = score_suite(suite, receipt["results"])
        receipt["comparisons"] = {
            str(path): {
                "receipt_sha256": sha256(path),
                **compare_results(receipt["results"], json.loads(path.read_text())),
            }
            for path in args.compare
        }
        receipt["completed"] = True
        save(destination, receipt)
        print(json.dumps({"completed": True, "scores": receipt["scores"]}), flush=True)
        return model
    except BaseException as error:
        receipt["error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exc(),
        }
        save(destination, receipt)
        raise
    finally:
        for handle in handles:
            handle.remove()
        if original_generate is not None:
            model.engine.generate = original_generate


def main():
    args = parse_args()
    model = run(args)
    if args.followup_suite:
        followup = argparse.Namespace(**vars(args))
        followup.suite = args.followup_suite
        followup.output = args.followup_output
        followup.released_benchmark = args.followup_benchmark
        followup.compare = args.followup_compare
        run(followup, model=model)


if __name__ == "__main__":
    main()
