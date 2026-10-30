#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Measure an embedder's cost curve for the LEANN benchmark.

Times POST {url}/embeddings at several batch sizes with passages of a fixed
length and prints one "batch_size median_ms" line per size, the format
`leann_bench --cost FILE` reads. Works with Antfly's inference server or any
OpenAI-compatible embeddings endpoint.

    python3 scripts/leann_embed_cost.py --url http://127.0.0.1:8090/ai/v1 \
        --model BAAI/bge-small-en-v1.5 > cost.txt
"""

import argparse
import json
import random
import statistics
import sys
import time
import urllib.request

WORDS = (
    "the of and to in a is that for it as was with be by on not he this are or his from at which but have an they "
    "you were her she there all one their we can been has more if will would so about who what when up out them some "
    "index vector graph search query embedding storage memory model recall latency batch shard replica document "
    "river mountain history science language music market city water energy system people government research"
).split()


def passage(rng: random.Random, words: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(words))


def embed(url: str, model: str, texts: list[str]) -> float:
    body = json.dumps({"model": model, "input": texts}).encode()
    req = urllib.request.Request(url.rstrip("/") + "/embeddings", data=body, headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = json.load(resp)
    elapsed = (time.perf_counter() - start) * 1000
    if len(data.get("data", [])) != len(texts):
        raise RuntimeError(f"expected {len(texts)} embeddings, got {len(data.get('data', []))}")
    return elapsed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8090/ai/v1")
    ap.add_argument("--model", default="BAAI/bge-small-en-v1.5")
    ap.add_argument("--batches", default="1,2,4,8,16,32,64,128")
    ap.add_argument("--words", type=int, default=190, help="words per passage (~256 tokens by default)")
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()

    rng = random.Random(7)
    sizes = [int(b) for b in args.batches.split(",")]
    embed(args.url, args.model, [passage(rng, args.words)])  # warm up / load the model
    print(f"# model={args.model} words/passage={args.words} repeats={args.repeats}")
    print("# batch_size median_ms per_item_ms")
    for b in sizes:
        times = [embed(args.url, args.model, [passage(rng, args.words) for _ in range(b)]) for _ in range(args.repeats)]
        med = statistics.median(times)
        print(f"{b} {med:.2f} {med / b:.2f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
