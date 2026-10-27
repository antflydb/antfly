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

"""Measure the MLX Florence-2 reference when vLLM Metal lacks its adapter.

Run in a pinned mlx-vlm environment. Uses the original checkpoint, <OCR>,
greedy generation and fresh image preprocessing on every call. Batch sizes
measure offline batches, not an HTTP or continuously refilled server.
"""

import argparse
import importlib.metadata
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from benchmark_antfly_readers import checkpoint, digest, hardware


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--images", type=Path, nargs="+", required=True)
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--requests", type=int, default=16)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.requests < 1 or any(b < 1 or b > 16 for b in args.batch_sizes):
        parser.error("requests must be positive; batch sizes must be 1..16")
    import mlx.core as mx
    from mlx.utils import tree_flatten
    from mlx_vlm import apply_chat_template, batch_generate, generate, load

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (stock macOS Python 3.9)
        "hardware": hardware(),
        "versions": {
            name: importlib.metadata.version(name)
            for name in ["mlx", "mlx-vlm", "mlx-lm", "transformers"]
        },
        "model": checkpoint(args.model_dir.resolve()),
        "images": [
            {"path": str(p.resolve()), "sha256": digest(p)} for p in args.images
        ],
        "scope": "MLX-VLM Florence-2, original full precision checkpoint, greedy <OCR>, 256-token limit, repeated mixed pages. Offline batches include preprocessing, inference and text decoding; exclude loading and serial reference reads. No vision/prefix cache supplied. Other machine workloads uncontrolled. Separate reference: vLLM Metal 0.32.0 has no Florence-2 adapter.",
        "results": [],
        "status": "starting",
    }
    destination = args.output_dir / "report.json"

    def save():
        destination.write_text(json.dumps(report, indent=2) + "\n")

    def clean(text):
        return text.replace("<s>", "").replace("</s>", "").strip()

    save()
    try:
        model, processor = load(str(args.model_dir.resolve()))
        report["weight_dtypes"] = sorted(
            {str(v.dtype) for _, v in tree_flatten(model.parameters())}
        )
        prompt = apply_chat_template(processor, model.config, "<OCR>", num_images=1)
        report["prompt"] = prompt
        references = []
        for image in args.images:
            result = generate(
                model,
                processor,
                prompt=prompt,
                image=str(image.resolve()),
                max_tokens=args.max_tokens,
                temperature=0.0,
                verbose=False,
            )
            if result.generation_tokens >= args.max_tokens:
                raise RuntimeError("Serial reference truncated")
            references.append(
                {
                    "text": clean(result.text),
                    "generation_tokens": result.generation_tokens,
                }
            )
        report["references"] = references
        report["status"] = "running"
        save()
        for size in args.batch_sizes:
            outputs = []
            mx.synchronize()
            start = time.perf_counter()
            for offset in range(0, args.requests, size):
                indices = list(range(offset, min(offset + size, args.requests)))
                images = [
                    str(args.images[i % len(args.images)].resolve()) for i in indices
                ]
                if size == 1:
                    result = generate(
                        model,
                        processor,
                        prompt=prompt,
                        image=images[0],
                        max_tokens=args.max_tokens,
                        temperature=0.0,
                        verbose=False,
                    )
                    texts = [result.text]
                else:
                    result = batch_generate(
                        model,
                        processor,
                        images=images,
                        prompts=[prompt] * len(images),
                        max_tokens=args.max_tokens,
                        greedy_sampling=True,
                        verbose=False,
                    )
                    texts = result.texts
                outputs.extend(
                    {"image_index": i % len(args.images), "text": clean(text)}
                    for i, text in zip(indices, texts)
                )
            mx.synchronize()
            elapsed = time.perf_counter() - start
            parity = len(outputs) == args.requests and all(
                x["text"] == references[x["image_index"]]["text"] for x in outputs
            )
            entry = {
                "batch_size": size,
                "completed": len(outputs),
                "elapsed_seconds": elapsed,
                "pages_per_second": len(outputs) / elapsed,
                "serial_reference_parity": parity,
                "outputs": outputs,
            }
            report["results"].append(entry)
            save()
            print(
                json.dumps({k: v for k, v in entry.items() if k != "outputs"}),
                flush=True,
            )
            if not parity:
                raise RuntimeError(
                    "Batched Florence-2 output differs from the serial reference"
                )
        report["status"] = "ok"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        save()


if __name__ == "__main__":
    main()
