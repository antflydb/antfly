# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch==2.14.1", "torchvision", "transformers==5.5.4", "pillow"]
# ///
"""Order-of-magnitude LightOnOCR3 BF16/MPS benchmark; run with uv run."""

import argparse
import hashlib
import json
import platform
import resource
import time
from pathlib import Path

import torch
import transformers
from PIL import Image
from transformers import AutoProcessor, Qwen3_5ForConditionalGeneration


def profile_phases(model, processor, image, prompt, max_new_tokens):
    """Extra diagnostic request. MPS event intervals include queue idle time.

    Hook/event overhead is intentionally excluded from the ordinary warm runs.
    Forward totals include child scopes and must not be added to those scopes.
    """
    phase = "prefill"
    pending = {}
    intervals = []
    handles = []

    def pre(scope):
        def hook(module, args, kwargs):
            nonlocal phase
            if scope == "forward":
                ids = kwargs.get("input_ids")
                embeds = kwargs.get("inputs_embeds")
                length = ids.shape[-1] if ids is not None else embeds.shape[-2]
                phase = "prefill" if length > 1 else "decode"
            event = torch.mps.Event(enable_timing=True)
            event.record()
            pending[id(module)] = (scope, phase, event, time.perf_counter())

        return hook

    def post(module, args, kwargs, output):
        scope, current_phase, start, cpu_start = pending.pop(id(module))
        end = torch.mps.Event(enable_timing=True)
        end.record()
        intervals.append(
            (
                scope + "_" + current_phase,
                start,
                end,
                (time.perf_counter() - cpu_start) * 1000,
            )
        )

    for name, module in (
        ("forward", model),
        ("vision", model.model.visual),
        ("language", model.model.language_model),
        ("lm_head", model.lm_head),
    ):
        handles.append(module.register_forward_pre_hook(pre(name), with_kwargs=True))
        handles.append(module.register_forward_hook(post, with_kwargs=True))
    try:
        torch.mps.synchronize()
        started = time.perf_counter()
        inputs = processor(text=[prompt], images=[image], return_tensors="pt").to("mps")
        inputs = {
            key: value.to(torch.bfloat16) if value.is_floating_point() else value
            for key, value in inputs.items()
        }
        torch.mps.synchronize()
        preprocess_seconds = time.perf_counter() - started
        started = time.perf_counter()
        with torch.inference_mode():
            sequences = model.generate(
                **inputs, max_new_tokens=max_new_tokens, do_sample=False, use_cache=True
            )
        torch.mps.synchronize()
        seconds = time.perf_counter() - started
    finally:
        for handle in handles:
            handle.remove()
    scopes = {}
    for name, start, end, cpu_ms in intervals:
        item = scopes.setdefault(
            name, {"calls": 0, "mps_event_ms": 0.0, "cpu_forward_ms": 0.0}
        )
        item["calls"] += 1
        item["mps_event_ms"] += start.elapsed_time(end)
        item["cpu_forward_ms"] += cpu_ms
    token_ids = sequences[0, inputs["input_ids"].shape[1] :].tolist()
    return {
        "preprocess_seconds": preprocess_seconds,
        "instrumented_generation_seconds": seconds,
        "scopes": scopes,
        "token_ids": token_ids,
        "text": processor.decode(token_ids, skip_special_tokens=True),
        "notes": [
            "Separate instrumented request; excluded from warm latency statistics.",
            "MPS event intervals include device queue idle time.",
            "Forward scopes include vision/language/lm_head child scopes.",
            "CPU forward time measures submission and incidental synchronization.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", required=True)
    parser.add_argument(
        "--image",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "src/models/testdata/lightonocr3/invoice.png",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile-phases", action="store_true")
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--max-new-tokens", type=int, default=128)
    args = parser.parse_args()
    if args.warm_runs < 1 or args.max_new_tokens < 1:
        parser.error("warm-runs and max-new-tokens must be positive")
    image = Image.open(args.image).convert("RGB")
    if not torch.backends.mps.is_available():
        raise RuntimeError("MPS unavailable")
    metadata = {
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "platform": platform.platform(),
        "device": "mps",
        "dtype": "bfloat16",
        "image_size": image.size,
        "image_sha256": hashlib.sha256(args.image.read_bytes()).hexdigest(),
        "model_dir": args.model_dir,
        "max_new_tokens": args.max_new_tokens,
    }
    print(json.dumps(metadata), flush=True)
    started = time.perf_counter()
    processor = AutoProcessor.from_pretrained(args.model_dir, local_files_only=True)
    model = (
        Qwen3_5ForConditionalGeneration.from_pretrained(
            args.model_dir,
            local_files_only=True,
            dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .eval()
        .to("mps")
    )
    torch.mps.synchronize()
    metadata["load_seconds"] = time.perf_counter() - started
    print("load_seconds", metadata["load_seconds"], flush=True)
    messages = [{"role": "user", "content": [{"type": "image", "image": image}]}]
    prompt = processor.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
    )
    probe = processor(text=[prompt], images=[image], return_tensors="pt")
    metadata.update(
        prompt=prompt,
        input_tokens=probe.input_ids.shape[1],
        image_grid_thw=probe.image_grid_thw.tolist(),
    )
    print("request", json.dumps(metadata), flush=True)
    results = []
    for run in range(args.warm_runs + 1):
        torch.mps.synchronize()
        started = time.perf_counter()
        inputs = processor(text=[prompt], images=[image], return_tensors="pt").to("mps")
        inputs = {
            key: value.to(torch.bfloat16) if value.is_floating_point() else value
            for key, value in inputs.items()
        }
        with torch.inference_mode():
            sequences = model.generate(
                **inputs,
                max_new_tokens=args.max_new_tokens,
                do_sample=False,
                use_cache=True,
            )
        torch.mps.synchronize()
        elapsed = time.perf_counter() - started
        token_ids = sequences[0, inputs["input_ids"].shape[1] :].tolist()
        result = {
            "run": run,
            "warm": run > 0,
            "seconds": elapsed,
            "tokens": len(token_ids),
            "token_ids": token_ids,
            "text": processor.decode(token_ids, skip_special_tokens=True),
            "mps_allocated_bytes": torch.mps.current_allocated_memory(),
            "max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        results.append(result)
        print(json.dumps(result), flush=True)
    report = {"metadata": metadata, "runs": results}
    if args.profile_phases:
        report["profile"] = profile_phases(
            model, processor, image, prompt, args.max_new_tokens
        )
        print("phase_profile", json.dumps(report["profile"]), flush=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
