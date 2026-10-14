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

"""Deterministic Gemma4 OCR generation versus an Apple Vision corpus baseline.

Raster preparation is external and shared. Timings include HTTP/base64 decode,
multimodal preprocessing, prefill, autoregressive decode and full text response.
The default generation path serializes Metal media requests; experimental decode
batching is configured on the server, independently of the HTTP wave size.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

PROMPT = (
    "Transcribe all visible text in this document image in reading order. "
    "Return only the text, preserving paragraphs, headings and table entries. "
    "Do not summarize or add commentary."
)


def post(base_url, body, timeout, endpoint="/ai/v1/generate/batch"):
    request = urllib.request.Request(
        base_url.rstrip("/") + endpoint,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode()}") from error


def submit(args, body):
    if args.runtime == "antfly" and args.draft_model is None:
        return post(args.base_url, body, args.timeout)
    # Independent HTTP requests; llama.cpp --parallel controls active slots.
    endpoint = "/v1/chat/completions" if args.runtime == "llama" else "/ai/v1/generate"

    def complete(indexed):
        index, request = indexed
        payload = dict(request["body"])
        if args.runtime == "llama":
            payload.pop("backend", None)
            payload["cache_prompt"] = False
        value = post(args.base_url, payload, args.timeout, endpoint)
        return {"index": index, "custom_id": request["custom_id"], "response": value}

    with ThreadPoolExecutor(max_workers=len(body["requests"])) as pool:
        results = list(pool.map(complete, enumerate(body["requests"])))
    return {"summary": {"failed": 0}, "data": results}


def run(args):
    corpus = json.loads(args.corpus.read_text())
    pages = corpus["pages"]
    bodies = []
    for page in pages:
        data = base64.b64encode(Path(page["path"]).read_bytes()).decode()
        bodies.append(
            {
                "model": args.model,
                "backend": "metal",
                "max_tokens": args.max_tokens,
                "temperature": 0,
                "chat_template_kwargs": {"enable_thinking": False},
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": PROMPT},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": "data:image/png;base64," + data,
                                },
                            },
                        ],
                    }
                ],
            }
        )
    if args.draft_model:
        for body in bodies:
            body.update(
                draft_model=args.draft_model,
                speculative_k=args.speculative_k,
                speculation_policy="force",
            )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for batch in args.batches:
        waves = []
        totals = []
        saved = []
        # One per-page warmup per case, outside measured whole-corpus passes.
        for _ in range(args.warmups):
            submit(
                args,
                {"requests": [{"custom_id": "warmup", "body": bodies[0]}]},
            )
        for iteration in range(args.iterations):
            started = time.perf_counter()
            iteration_saved = []
            for offset in range(0, len(pages), batch):
                current = pages[offset : offset + batch]
                requests = [
                    {"custom_id": f"{p['document']}-{p['page']}", "body": b}
                    for p, b in zip(current, bodies[offset : offset + batch])
                ]
                began = time.perf_counter()
                response = submit(args, {"mode": "sync", "requests": requests})
                elapsed = time.perf_counter() - began
                if response["summary"]["failed"]:
                    raise RuntimeError(json.dumps(response))
                results = sorted(response["data"], key=lambda row: row["index"])
                if len(results) != len(current):
                    raise RuntimeError("Wrong result count")
                outputs = []
                for item, page, submitted in zip(results, current, requests):
                    if item["custom_id"] != submitted["custom_id"]:
                        raise RuntimeError("Wrong page identity")
                    value = item["response"]
                    choice = value["choices"][0]
                    output = {
                        "document": page["document"],
                        "page": page["page"],
                        "text": choice["message"]["content"],
                        "finish_reason": choice["finish_reason"],
                        "usage": value["usage"],
                        "speculation": value.get("speculation"),
                        "timings": value.get("timings"),
                    }
                    outputs.append(output)
                iteration_saved += outputs
                waves.append(
                    {
                        "iteration": iteration,
                        "offset": offset,
                        "pages": len(current),
                        "seconds": elapsed,
                        "outputs": outputs,
                    }
                )
                print(
                    f"batch={batch} iteration={iteration + 1} offset={offset} pages={len(current)} seconds={elapsed:.3f}",
                    flush=True,
                )
            totals.append(time.perf_counter() - started)
            saved.append(iteration_saved)
            # Save each completed pass, so interrupted campaigns remain reviewable.
            report = {
                "model": args.model,
                "runtime": args.runtime,
                "draft_model": args.draft_model,
                "speculative_k": args.speculative_k if args.draft_model else None,
                "batch": batch,
                "pages": len(pages),
                "warmup_pages": args.warmups,
                "iterations": len(totals),
                "max_tokens": args.max_tokens,
                "temperature": 0,
                "thinking": False,
                "prompt": PROMPT,
                "totals": totals,
                "waves": waves,
                "results": saved,
                "execution": "llama.cpp concurrent slots"
                if args.runtime == "llama"
                else "Antfly Metal (server batching flags recorded separately)",
                "scope": "Pre-rendered PNG through HTTP to complete OCR text; model load/file reads excluded",
            }
            (args.output_dir / f"gemma4-b{batch}.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--runtime", choices=["antfly", "llama"], default="antfly")
    parser.add_argument("--draft-model")
    parser.add_argument("--speculative-k", type=int, default=2)
    parser.add_argument("--base-url", default="http://127.0.0.1:18097")
    parser.add_argument("--batches", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=2048)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if (
        any(batch < 1 or batch > 128 for batch in args.batches)
        or args.iterations < 1
        or args.warmups < 0
    ):
        parser.error("Invalid batch/iteration limits")
    run(args)


if __name__ == "__main__":
    main()
