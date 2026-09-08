#!/usr/bin/env python3
"""Benchmark a resident OpenAI-compatible embedding endpoint."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18099/v1/embeddings")
    parser.add_argument("--model", default="bge-m3")
    parser.add_argument("--text", default="hello world")
    parser.add_argument("--batch-sizes", default="1,2,4,8")
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--reference-url")
    parser.add_argument("--reference-model")
    args = parser.parse_args()
    args.batch_sizes = [int(value) for value in args.batch_sizes.split(",")]
    if any(value < 1 for value in args.batch_sizes):
        parser.error("batch sizes must be positive")
    if args.warmups < 0 or args.repeats < 1:
        parser.error("warmups must be non-negative and repeats must be positive")
    return args


def request_embeddings(
    args: argparse.Namespace,
    batch: int,
    *,
    url: str | None = None,
    model: str | None = None,
) -> tuple[float, list[list[float]]]:
    body = json.dumps(
        {"model": model or args.model, "input": [args.text] * batch}
    ).encode()
    request = urllib.request.Request(
        url or args.url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.perf_counter()
    with urllib.request.urlopen(request, timeout=args.timeout) as response:
        payload_bytes = response.read()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    payload = json.loads(payload_bytes)
    data = payload.get("data")
    if not isinstance(data, list) or len(data) != batch:
        raise RuntimeError(
            f"expected {batch} embeddings, got {len(data) if isinstance(data, list) else 'none'}"
        )
    embeddings = [item.get("embedding") for item in data]
    if any(not isinstance(embedding, list) for embedding in embeddings):
        raise RuntimeError("response contains no embedding vector")
    dimensions = {len(embedding) for embedding in embeddings}
    if len(dimensions) != 1 or not dimensions or next(iter(dimensions)) == 0:
        raise RuntimeError(f"invalid embedding dimensions: {sorted(dimensions)}")
    return elapsed_ms, embeddings


def compare_embeddings(
    actual: list[list[float]], reference: list[list[float]]
) -> tuple[float, float, float, float]:
    if len(actual) != len(reference) or any(
        len(left) != len(right) for left, right in zip(actual, reference)
    ):
        raise RuntimeError("embedding response shapes differ")
    absolute_errors = [
        abs(left_value - right_value)
        for left, right in zip(actual, reference)
        for left_value, right_value in zip(left, right)
    ]
    cosines = []
    for left, right in zip(actual, reference):
        dot = sum(
            left_value * right_value for left_value, right_value in zip(left, right)
        )
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        cosines.append(dot / (left_norm * right_norm))
    return (
        max(absolute_errors),
        statistics.fmean(absolute_errors),
        min(cosines),
        statistics.fmean(cosines),
    )


def main() -> None:
    args = parse_args()
    parity_rows = []
    print(
        "batch,avg_ms,p50_ms,p95_ms,min_ms,max_ms,throughput_embeddings_s,dimensions,checksum_first32"
    )
    for batch in args.batch_sizes:
        embeddings = []
        for _ in range(args.warmups):
            _, embeddings = request_embeddings(args, batch)
        samples = []
        for _ in range(args.repeats):
            elapsed_ms, embeddings = request_embeddings(args, batch)
            samples.append(elapsed_ms)
        dimensions = len(embeddings[0])
        checksum = sum(sum(embedding[:32]) for embedding in embeddings)
        ordered = sorted(samples)
        p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
        average = statistics.fmean(samples)
        print(
            f"{batch},{average:.3f},{statistics.median(samples):.3f},{p95:.3f},"
            f"{ordered[0]:.3f},{ordered[-1]:.3f},{batch * 1000.0 / average:.2f},"
            f"{dimensions},{checksum:.6f}"
        )
        if args.reference_url:
            _, reference = request_embeddings(
                args,
                batch,
                url=args.reference_url,
                model=args.reference_model or args.model,
            )
            parity_rows.append((batch, *compare_embeddings(embeddings, reference)))

    if parity_rows:
        print("parity_batch,max_abs_error,mean_abs_error,min_cosine,mean_cosine")
        for batch, max_abs, mean_abs, min_cosine, mean_cosine in parity_rows:
            print(
                f"{batch},{max_abs:.9f},{mean_abs:.9f},"
                f"{min_cosine:.9f},{mean_cosine:.9f}"
            )


if __name__ == "__main__":
    main()
