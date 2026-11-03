#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Measure native/CUDA text queries and query-only cross-backend retrieval.

This is a diagnostic campaign. Similar vectors or unchanged rankings in this
small corpus do not override the runtime's distinct F32/BF16 model identities.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def vector(row):
    values = row["vector"]
    if len(values) != row["dimensions"] or not all(math.isfinite(v) for v in values):
        raise ValueError("invalid vector shape or nonfinite component")
    if abs(math.sqrt(sum(v * v for v in values)) - 1) > 1e-4:
        raise ValueError("unnormalized vector")
    return values


def cosine(a, b):
    if len(a) != len(b):
        raise ValueError("vector dimensions differ")
    return sum(x * y for x, y in zip(a, b)) / math.sqrt(
        sum(x * x for x in a) * sum(y * y for y in b)
    )


def assess(suite, native, cuda, image=None):
    if native["backend"] != "native" or cuda["backend"] != "cuda":
        raise ValueError("actual backends must be native and cuda")
    if native["suite_sha256"] != cuda["suite_sha256"]:
        raise ValueError("suite fingerprints differ")
    rows = {
        name: {row["name"]: row for row in raw["cases"]}
        for name, raw in (("native", native), ("cuda", cuda))
    }
    if not rows["native"] or rows["native"].keys() != rows["cuda"].keys():
        raise ValueError("case coverage differs")
    comparisons = {}
    for name, cpu in rows["native"].items():
        gpu = rows["cuda"][name]
        a, b = vector(cpu), vector(gpu)
        if cpu["input_tokens"] != gpu["input_tokens"]:
            raise ValueError("token counts differ")
        comparisons[name] = {
            "tokens": cpu["input_tokens"],
            "dimensions": len(a),
            "cpu_p50_ms": cpu["p50_seconds"] * 1000,
            "cpu_p95_ms": cpu["p95_seconds"] * 1000,
            "cuda_p50_ms": gpu["p50_seconds"] * 1000,
            "cuda_p95_ms": gpu["p95_seconds"] * 1000,
            "cosine": cosine(a, b),
            "max_abs": max(abs(x - y) for x, y in zip(a, b)),
        }
    spec = suite["retrieval"]
    # Documents remain CUDA-produced in BOTH comparisons: this models a CPU
    # query against an existing GPU-built index, without re-embedding that index.
    docs = {
        name: vector(rows["cuda"][name])
        for name in spec["documents"]
        if name in rows["cuda"]
    }
    if image is not None:
        if image.get("backend") != "cuda":
            raise ValueError("image receipt must use the CUDA backend")
        values = image["cases"][0]["output"]
        docs["earth_image"] = vector({"vector": values, "dimensions": 768})
    rankings = {}
    for name in spec["queries"]:
        expected = spec["expected"][name]
        if expected not in docs:
            continue
        scores = {
            backend: {
                doc: cosine(vector(rows[backend][name]), values)
                for doc, values in docs.items()
            }
            for backend in ("native", "cuda")
        }
        order = {
            backend: sorted(docs, key=lambda doc: (-scores[backend][doc], doc))
            for backend in scores
        }
        rankings[name] = {
            "expected": expected,
            "cpu_query_ranking": order["native"],
            "cuda_query_ranking": order["cuda"],
            "same_top1": order["native"][0] == order["cuda"][0],
            "same_top5_set": set(order["native"][:5]) == set(order["cuda"][:5]),
            "cpu_expected_top1": order["native"][0] == expected,
            "cuda_expected_top1": order["cuda"][0] == expected,
            "max_score_difference": max(
                abs(scores["native"][d] - scores["cuda"][d]) for d in docs
            ),
            "scores": scores,
        }
    return {
        "model_identity_equal": native["model_identity"] == cuda["model_identity"],
        "native_model_identity": native["model_identity"],
        "cuda_model_identity": cuda["model_identity"],
        "cases": comparisons,
        "retrieval": {
            "documents_backend": "cuda",
            "queries": len(rankings),
            "documents": len(docs),
            "same_top1": sum(r["same_top1"] for r in rankings.values()),
            "same_top5_set": sum(r["same_top5_set"] for r in rankings.values()),
            "cpu_expected_top1": sum(r["cpu_expected_top1"] for r in rankings.values()),
            "cuda_expected_top1": sum(
                r["cuda_expected_top1"] for r in rankings.values()
            ),
            "rankings": rankings,
        },
        "automatic_fallback_qualified": False,
        "qualification_note": "Diagnostic corpus only; the API's pinned model-identity contract remains authoritative.",
    }


def run(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    binary, model, suite_path = (
        args.binary.resolve(),
        args.model.resolve(),
        args.suite.resolve(),
    )
    suite = json.loads(suite_path.read_text())
    report = {
        "version": 1,
        "host": platform.platform(),
        "binary_sha256": sha256(binary),
        "suite_sha256": sha256(suite_path),
        "checkpoint_receipt_sha256": sha256(model / "embeddinggemma2_receipt.json"),
        "environment": {
            k: v
            for k, v in os.environ.items()
            if k.startswith("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_")
        },
        "runs": {},
    }

    def invoke(backend, role, first_only=False):
        receipt = output / f"{role}.json"
        command = [
            str(binary),
            "--model",
            str(model),
            "--suite",
            str(suite_path),
            "--backend",
            backend,
            "--output",
            str(receipt),
            "--warmups",
            "0" if first_only else str(args.warmups),
            "--iterations",
            "1" if first_only else str(args.iterations),
        ]
        if first_only:
            command += ["--case", suite["retrieval"]["queries"][0]]
        rss_path = output / f"{role}.maxrss-kib"
        if Path("/usr/bin/time").is_file() and platform.system() == "Linux":
            command = ["/usr/bin/time", "-f", "%M", "-o", str(rss_path), *command]
        with (output / f"{role}.log").open("w") as log:
            subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=args.timeout,
            )
        raw = json.loads(receipt.read_text())
        if rss_path.exists():
            raw["process_peak_rss_kib"] = int(rss_path.read_text().strip())
        if raw["backend"] != backend:
            raise ValueError("requested backend was not used")
        return raw

    for backend in ("native", "cuda"):
        report["runs"][backend] = invoke(backend, backend)
        (output / "campaign.json").write_text(json.dumps(report, indent=2) + "\n")
        print(
            backend,
            "time to first vector",
            report["runs"][backend]["first_vector_seconds"],
            flush=True,
        )
    image = (
        json.loads(args.cuda_image_receipt.read_text())
        if args.cuda_image_receipt
        else None
    )
    report["assessment"] = assess(
        suite, report["runs"]["native"], report["runs"]["cuda"], image
    )
    if args.startup_race:
        begun = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                backend: pool.submit(invoke, backend, "race-" + backend, True)
                for backend in ("native", "cuda")
            }
            report["startup_race"] = {
                backend: future.result() for backend, future in futures.items()
            }
        report["startup_race_wall_seconds"] = time.monotonic() - begun
        report["startup_race_scope"] = (
            "two fresh managed processes launched together; OS/file caches may already be warm; not automatic serving failover"
        )
    (output / "campaign.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in report["assessment"]["retrieval"].items()
                if k != "rankings"
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument(
        "--suite",
        type=Path,
        default=Path(__file__).parent / "testdata/cpu-query/suite.json",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cuda-image-receipt", type=Path)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--startup-race", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.iterations <= 100 or not 0 <= args.warmups <= 100:
        parser.error("require 1–100 measured iterations and 0–100 warmups")
    run(args)


if __name__ == "__main__":
    main()
