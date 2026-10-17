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

"""Paired synchronized Gemma4 image runs and independent HTTP concurrency load."""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import math
import mimetypes
import os
from pathlib import Path
import platform
import subprocess
import time
import urllib.error
import urllib.request


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parity(reference, actual):
    if len(reference) != len(actual) or not reference:
        raise ValueError("output shape mismatch or empty output")
    if not all(math.isfinite(x) for x in reference + actual):
        raise ValueError("nonfinite output")
    dot = sum(x * y for x, y in zip(reference, actual))
    aa = sum(x * x for x in reference)
    bb = sum(y * y for y in actual)
    if aa == 0 or bb == 0:
        raise ValueError("zero output")
    return {
        "cosine": dot / math.sqrt(aa * bb),
        "max_abs": max(abs(x - y) for x, y in zip(reference, actual)),
    }


def percentile(values, percent):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent) - 1)]


def load_http(args, images):
    """One image per request: clients are concurrency, never model batch size."""
    reports = []
    for image_index, image in enumerate(images):
        mime = mimetypes.guess_type(image.name)[0] or "image/png"
        data_url = (
            "data:" + mime + ";base64," + base64.b64encode(image.read_bytes()).decode()
        )
        body = json.dumps(
            {
                "model": args.http_model,
                "input": [{"type": "image_url", "image_url": {"url": data_url}}],
            }
        ).encode()

        def request(_):
            started = time.perf_counter()
            req = urllib.request.Request(
                args.url.rstrip("/") + "/embed",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=args.timeout) as response:
                    reply = json.load(response)
                vector = reply["data"][0]["embedding"]
                return {
                    "seconds": time.perf_counter() - started,
                    "vector": vector,
                    "status": 200,
                }
            except urllib.error.HTTPError as error:
                return {
                    "seconds": time.perf_counter() - started,
                    "status": error.code,
                    "error": error.read().decode(),
                }

        warm = [request(i) for i in range(args.warmups)]
        if any(x["status"] != 200 for x in warm):
            raise ValueError("HTTP warmup failed: " + repr(warm))
        reference = warm[-1]["vector"] if warm else request(0)["vector"]
        for clients in (1, 2, 4, 8):
            started = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
                results = list(pool.map(request, range(args.iterations)))
            elapsed = time.perf_counter() - started
            successes = [x for x in results if x["status"] == 200]
            checks = [parity(reference, x.pop("vector")) for x in successes]
            if any(x["cosine"] < args.minimum_cosine for x in checks):
                raise ValueError("HTTP concurrent output differs from serial reference")
            timings = [x["seconds"] for x in successes]
            reports.append(
                {
                    "image": str(image),
                    "clients": clients,
                    "requests": len(results),
                    "successes": len(successes),
                    "wall_seconds": elapsed,
                    "images_per_second": len(successes) / elapsed,
                    "p50_seconds": percentile(timings, 0.5) if timings else None,
                    "p95_seconds": percentile(timings, 0.95) if timings else None,
                    "minimum_cosine": min((x["cosine"] for x in checks), default=None),
                    "responses": results,
                }
            )
            (args.output_dir / f"http-{image_index}-{clients}.json").write_text(
                json.dumps(reports[-1], indent=2) + "\n"
            )
            print(
                f"completed HTTP {image.name} clients={clients} "
                f"successes={len(successes)}/{len(results)}",
                flush=True,
            )
    return reports


def run(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    images = [p.resolve() for p in args.image]
    report = {
        "version": 1,
        "host": platform.platform(),
        "images": [{"path": str(p), "sha256": sha(p)} for p in images],
        "warmups": args.warmups,
        "iterations": args.iterations,
        "cases": [],
    }
    if args.url:
        report["http_model"] = args.http_model
        report["url"] = args.url
        report["http"] = load_http(args, images)
    else:
        if args.baseline is None or args.candidate is None or args.model_dir is None:
            raise ValueError(
                "offline mode requires --baseline, --candidate and --model-dir"
            )
        report["binaries"] = {
            name: {"path": str(path.resolve()), "sha256": sha(path)}
            for name, path in (
                ("baseline", args.baseline),
                ("candidate", args.candidate),
            )
        }
        report["model"] = {
            "path": str(args.model_dir.resolve()),
            "weights_sha256": sha(args.model_dir / "model.safetensors"),
            "config_sha256": sha(args.model_dir / "config.json"),
        }
        for scope in args.scope:
            for batch in args.batch:
                for index, image in enumerate(images):
                    pair = {}
                    # Alternate A/B ordering. Processes never overlap on the GPU.
                    order = (
                        ("baseline", "candidate")
                        if index % 2 == 0
                        else ("candidate", "baseline")
                    )
                    for name in order:
                        path = output / f"{scope}-{batch}-{index}-{name}.json"
                        binary = args.baseline if name == "baseline" else args.candidate
                        command = [
                            str(binary.resolve()),
                            "--model",
                            str(args.model_dir.resolve()),
                            "--image",
                            str(image),
                            "--scope",
                            scope,
                            "--batch",
                            str(
                                1
                                if args.baseline_singletons and name == "baseline"
                                else batch
                            ),
                            "--backend",
                            args.backend,
                            "--warmups",
                            str(args.warmups),
                            "--iterations",
                            str(args.iterations),
                            "--output",
                            str(path),
                            "--batching",
                            args.baseline_batching if name == "baseline" else "on",
                        ]
                        env = os.environ.copy()
                        for key in (
                            "ANTFLY_EMBEDDINGGEMMA2_STAGES",
                            "TERMITE_METAL_STAGE_TIMING",
                            "ANTFLY_EMBEDDINGGEMMA2_PROFILE_ENCODER",
                        ):
                            env.pop(key, None)
                        with path.with_suffix(".log").open("w") as log:
                            subprocess.run(
                                command,
                                stdout=log,
                                stderr=log,
                                timeout=args.timeout,
                                check=True,
                                env=env,
                            )
                        pair[name] = json.loads(path.read_text())
                        print(
                            f"completed {scope} batch={batch} {image.name} {name}",
                            flush=True,
                        )
                    baseline = pair["baseline"]["cases"][0]
                    candidate = pair["candidate"]["cases"][0]
                    reference_output = (
                        baseline["output"] * batch
                        if args.baseline_singletons
                        else baseline["output"]
                    )
                    check = parity(reference_output, candidate["output"])
                    if len(reference_output) % batch:
                        raise ValueError("batch output cannot be split by image")
                    stride = len(reference_output) // batch
                    rows = [
                        parity(
                            reference_output[i * stride : (i + 1) * stride],
                            candidate["output"][i * stride : (i + 1) * stride],
                        )
                        for i in range(batch)
                    ]
                    check["minimum_image_cosine"] = min(x["cosine"] for x in rows)
                    if check["minimum_image_cosine"] < args.minimum_cosine:
                        raise ValueError(
                            f"parity failed: {scope} batch={batch} {image}: {check}"
                        )
                    report["cases"].append(
                        {
                            "scope": scope,
                            "batch": batch,
                            "image": str(image),
                            "patch_tokens": candidate["projected_tokens"] * 9,
                            "parity": check,
                            "per_image_p50_speedup": (
                                baseline["p50_seconds"] / baseline["batch"]
                            )
                            / (candidate["p50_seconds"] / candidate["batch"]),
                            "throughput_speedup": candidate["images_per_second"]
                            / baseline["images_per_second"],
                            "baseline": {
                                k: v for k, v in baseline.items() if k != "output"
                            },
                            "candidate": {
                                k: v for k, v in candidate.items() if k != "output"
                            },
                        }
                    )
                    (output / "summary.json").write_text(
                        json.dumps(report, indent=2) + "\n"
                    )
    (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument(
        "--baseline-batching",
        choices=("off", "on"),
        default="off",
        help="Use on to compare existing batched embedding encoders while isolating vision changes",
    )
    parser.add_argument(
        "--baseline-singletons",
        action="store_true",
        help="Compare candidate throughput against singleton forwards; latency speedup uses amortized per-image batch latency",
    )
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--image", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--scope", choices=("projector", "image", "embedding"), action="append"
    )
    parser.add_argument("--batch", type=int, action="append")
    parser.add_argument("--backend", choices=("native", "metal"), default="metal")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--minimum-cosine", type=float, default=0.9999)
    parser.add_argument(
        "--url",
        help="Existing dedicated /ai/v1 server; run concurrency load instead of offline pairs",
    )
    parser.add_argument("--http-model", default="embeddinggemma2")
    args = parser.parse_args()
    if args.iterations < 1 or args.warmups < 1 or args.timeout <= 0:
        parser.error("iterations, warmups and timeout must be positive")
    args.scope = args.scope or ["projector", "image", "embedding"]
    args.batch = args.batch or [1, 2, 4, 8]
    if any(x not in (1, 2, 4, 8) for x in args.batch):
        parser.error("batch sizes must be 1/2/4/8")
    run(args)


if __name__ == "__main__":
    main()
