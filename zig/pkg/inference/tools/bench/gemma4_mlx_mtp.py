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

"""Paired MLX Gemma4 OCR measurements, with independent caches per request.

Run with a pinned mlx-vlm checkout on PYTHONPATH and local model snapshots.
Block size includes the pending token: size 3 drafts two candidates.
This compares MTP against the same MLX target, not different quantizations.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import platform
import subprocess
import time
from pathlib import Path

from pdf_ocr import PROMPT
from pdf_ocr_report import quality


def summarize(report, pages):
    """Attach work/quality gates before interpreting any generation timings."""
    references = {
        (p["document"], p["page"]): Path(p["reference"]).read_text() for p in pages
    }
    controls = [
        r for r in report["runs"] if r["block"] == 0 and len(r["pages"]) == len(pages)
    ]
    if not controls:
        return
    control = controls[0]["pages"]
    for run in report["runs"]:
        run["quality"] = quality(run["pages"], references)
        mismatches = []
        for actual, expected in zip(run["pages"], control):
            if actual["text_sha256"] != expected["text_sha256"]:
                ids, baseline_ids = actual["token_ids"], expected["token_ids"]
                first = next(
                    (i for i, (a, b) in enumerate(zip(ids, baseline_ids)) if a != b),
                    min(len(ids), len(baseline_ids)),
                )
                mismatches.append(
                    {
                        "document": actual["document"],
                        "page": actual["page"],
                        "first_different_token": first,
                    }
                )
        run["mismatches"] = mismatches
        run["matches_control"] = len(run["pages"]) == len(pages) and not mismatches
        run["matches_control_tokens"] = len(run["pages"]) == len(pages) and all(
            p["token_ids"] == c["token_ids"]
            and p["generation_tokens"] == c["generation_tokens"]
            for p, c in zip(run["pages"], control)
        )
        run["truncated_pages"] = sum(
            p["finish_reason"] == "length" for p in run["pages"]
        )
        run["mean_word_f1_proxy"] = sum(q["f1"] for q in run["quality"]) / len(
            run["quality"]
        )
    report["controls_repeatable"] = all(r["matches_control"] for r in controls)
    report["all_mtp_outputs_match_control"] = all(
        r["matches_control"] for r in report["runs"] if r["block"]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--assistant", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--blocks", default="0,3,6,0")
    parser.add_argument("--passes", type=int, default=2)
    parser.add_argument("--max-tokens", type=int, default=2048)
    parser.add_argument("--pages", type=int, default=0)
    parser.add_argument(
        "--float32",
        action="store_true",
        help="Diagnostic: promote floating parameters; keep packed quantized weights",
    )
    parser.add_argument(
        "--singleton-verify",
        action="store_true",
        help="Diagnostic: verify one target input at a time to isolate matrix numerical changes",
    )
    args = parser.parse_args()
    blocks = [int(value) for value in args.blocks.split(",")]
    if (
        args.passes < 1
        or args.max_tokens < 1
        or args.pages < 0
        or any(b < 0 or b == 1 for b in blocks)
    ):
        parser.error("passes/tokens must be positive; block sizes must be 0 or >=2")

    import mlx.core as mx
    import mlx_vlm
    from mlx_vlm.generate import stream_generate
    from mlx_vlm.prompt_utils import apply_chat_template
    from mlx_vlm.speculative.drafters import load_drafter
    from mlx_vlm.speculative.utils import (
        speculative_stats_since,
        speculative_stats_snapshot,
    )
    from mlx_vlm.utils import load

    pages = json.loads(args.corpus.read_text())["pages"]
    if args.pages:
        pages = pages[: args.pages]
    if not pages:
        parser.error("corpus has no selected pages")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    source = Path(mlx_vlm.__file__).resolve().parent.parent
    revision = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    report = {
        "runtime": "mlx-vlm",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "corpus_sha256": hashlib.sha256(args.corpus.read_bytes()).hexdigest(),
        "source": str(source),
        "source_revision": revision,
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("mlx", "mlx-vlm", "transformers", "numpy")
        },
        "platform": platform.platform(),
        "model": str(args.model.resolve()),
        "assistant": str(args.assistant.resolve()),
        "corpus": str(args.corpus.resolve()),
        "prompt": PROMPT,
        "blocks": blocks,
        "passes": args.passes,
        "max_tokens": args.max_tokens,
        "temperature": 0,
        "thinking": False,
        "batch_size": 1,
        "float32_parameters": args.float32,
        "singleton_verification_diagnostic": args.singleton_verify,
        "timing_scope": "image processing, image encoding, prefill, text decode; excludes load/PDF rendering/HTTP",
        "block_semantics": "pending bonus token plus block_size-1 drafted candidates; runtime may adapt",
        "runs": [],
    }

    def save():
        temporary = args.output.with_suffix(args.output.suffix + ".partial")
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(args.output)

    started = time.perf_counter()
    model, processor = load(str(args.model))
    draft, kind = load_drafter(str(args.assistant), kind="mtp")
    if args.float32:
        model.set_dtype(mx.float32)
        draft.set_dtype(mx.float32)
    if args.singleton_verify:
        lm = model.language_model

        def verify_singletons(input_ids, prompt_cache):
            shared_kv = {}
            hidden = [
                lm.model(
                    input_ids[:, i : i + 1],
                    cache=prompt_cache,
                    skip_final_norm=True,
                    shared_kv_sink=shared_kv,
                )
                for i in range(input_ids.shape[1])
            ]
            return mx.concatenate(hidden, axis=1), shared_kv

        # Benchmark-local adapter; leave the upstream checkout unchanged.
        lm.speculative_verify_hidden = verify_singletons
    mx.eval(model.parameters(), draft.parameters())
    mx.synchronize()
    report["model_load_seconds"] = time.perf_counter() - started
    report["image_max_soft_tokens"] = getattr(
        processor.image_processor, "max_soft_tokens", None
    )
    formatted = apply_chat_template(
        processor,
        model.config,
        PROMPT,
        num_images=1,
        enable_thinking=False,
    )

    def request(page, block, budget):
        before = speculative_stats_snapshot(draft)
        mx.synchronize()
        start = time.perf_counter()
        first_token_seconds = None
        text = []
        last = None
        options = dict(max_tokens=budget, temperature=0.0, enable_thinking=False)
        if block:
            options.update(draft_model=draft, draft_kind=kind, draft_block_size=block)
        for response in stream_generate(
            model,
            processor,
            formatted,
            image=page["path"],
            **options,
        ):
            if first_token_seconds is None:
                first_token_seconds = time.perf_counter() - start
            text.append(response.text)
            last = response
        mx.synchronize()
        seconds = time.perf_counter() - start
        if last is None:
            raise RuntimeError("MLX returned no generation result")
        output = "".join(text)
        rounds, accepted, drafted = (
            speculative_stats_since(draft, before) if block else (None, None, None)
        )
        return {
            "document": page["document"],
            "page": page["page"],
            "seconds": seconds,
            "first_token_seconds": first_token_seconds,
            "prompt_tokens": last.prompt_tokens,
            "generation_tokens": last.generation_tokens,
            "generation_tps": last.generation_tps,
            "finish_reason": last.finish_reason,
            "peak_memory_gb": last.peak_memory,
            "text": output,
            "text_sha256": hashlib.sha256(output.encode()).hexdigest(),
            "token_ids": last.token_ids,
            "rounds": rounds,
            "accepted": accepted,
            "drafted": drafted,
        }

    for block in dict.fromkeys(blocks):
        print(f"Warmup block={block}", flush=True)
        request(pages[0], block, 16)
    save()
    for repeat in range(args.passes):
        for slot, block in enumerate(blocks):
            run = {"pass": repeat, "slot": slot, "block": block, "pages": []}
            report["runs"].append(run)
            for page in pages:
                result = request(page, block, args.max_tokens)
                run["pages"].append(result)
                run["seconds"] = sum(p["seconds"] for p in run["pages"])
                run["generation_tokens"] = sum(
                    p["generation_tokens"] for p in run["pages"]
                )
                save()
                print(
                    f"pass={repeat} block={block} {page['document']}:{page['page']} "
                    f"seconds={result['seconds']:.3f} tokens={result['generation_tokens']} "
                    f"finish={result['finish_reason']} accepted={result['accepted']}/{result['drafted']}",
                    flush=True,
                )
    report["complete"] = True
    summarize(report, pages)
    save()


if __name__ == "__main__":
    main()
