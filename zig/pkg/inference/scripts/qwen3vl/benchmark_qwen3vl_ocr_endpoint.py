#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# Licensed under the Apache License, Version 2.0 (the "License").
"""Compare complete resident /read requests against frozen OCR outputs.

Both endpoints use Antfly's /read contract. HTTP errors (including truncated
OCR), changed outputs, usage mismatches, and artifact mismatches fail the run.
Wrap the server/client launcher with scripts/benchmark_resources.py so its
entire process group, including inference workers, is resource monitored.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys
import statistics
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "qwen3_embedding"))
from benchmark_qwen3_embedding_endpoint import (
    bootstrap_ratio_ci,
    latency_summary,
    portable_report,
    process_provenance,
    sha256_file,
)
from qualify_qwen3vl_metal import write_json_atomic

SCHEMA = "antfly.qwen3vl.ocr_endpoint_benchmark.v1"
FIXTURE_SCHEMA = "antfly.qwen3vl.ocr_endpoint_fixture.v1"
GOLDEN_SCHEMA = "antfly.qwen3vl.ocr_endpoint_golden.v1"


def model_artifacts(model_dir: Path) -> dict[str, str]:
    # Download receipts contain installation-specific metadata, not model data.
    artifacts = sorted(
        p
        for p in model_dir.iterdir()
        if not p.name.startswith(".") and p.suffix in (".gguf", ".json", ".jinja")
    )
    if not any(p.suffix == ".gguf" for p in artifacts):
        raise ValueError("OCR benchmark requires the split GGUF model bundle")
    return {p.name: sha256_file(p) for p in artifacts}


def load_cases(path: Path) -> list[dict]:
    fixture = json.loads(path.read_text())
    if fixture.get("schema") != FIXTURE_SCHEMA:
        raise ValueError("unsupported OCR fixture schema")
    cases = []
    seen = set()
    for raw in fixture["cases"]:
        name = raw["id"]
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError("invalid or duplicate case ID")
        seen.add(name)
        images = raw["images"]
        cap = raw["max_tokens"]
        if not 1 <= len(images) <= 64 or type(cap) is not int or not 1 <= cap <= 1024:
            raise ValueError("invalid image count or max_tokens")
        prompt = raw.get("prompt")
        if prompt is not None and not isinstance(prompt, str):
            raise ValueError("prompt must be a string or null")
        urls = []
        for image in images:
            image_path = (path.parent / image["path"]).resolve(strict=True)
            if image_path.stat().st_size > 20 * 1024 * 1024:
                raise ValueError("fixture image exceeds 20 MiB")
            content = image_path.read_bytes()
            if hashlib.sha256(content).hexdigest() != image["sha256"]:
                raise ValueError(f"image checksum mismatch: {name}")
            if content.startswith(b"\x89PNG\r\n\x1a\n"):
                mime = "image/png"
            elif content.startswith(b"\xff\xd8\xff"):
                mime = "image/jpeg"
            else:
                raise ValueError("fixture image must be PNG or JPEG")
            urls.append(
                {"url": f"data:{mime};base64," + base64.b64encode(content).decode()}
            )
        request = {"images": urls, "max_tokens": cap}
        if prompt is not None:
            request["prompt"] = prompt
        cases.append({"id": name, "request": request, "fixture": raw})
    if not cases:
        raise ValueError("empty OCR fixture")
    return cases


def canonical_response(response: dict, model: str, images: int) -> dict:
    if response.get("model") != model or response.get("object") != "list":
        raise ValueError("unexpected OCR response model or object")
    rows = response["data"]
    if len(rows) != images:
        raise ValueError("OCR result count mismatch")
    texts = []
    for index, row in enumerate(rows):
        if (
            row.get("index") != index
            or row.get("object") != "read"
            or not isinstance(row.get("text"), str)
            or not row["text"]
        ):
            raise ValueError("invalid or empty OCR result")
        texts.append(row["text"])
    usage = response["usage"]
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        if type(usage.get(key)) is not int or usage[key] <= 0:
            raise ValueError("OCR usage must contain positive token counts")
    if usage["total_tokens"] != usage["prompt_tokens"] + usage["completion_tokens"]:
        raise ValueError("OCR usage does not reconcile")
    return {"texts": texts, "usage": usage}


def request_read(
    url: str, model: str, request: dict, timeout: float
) -> tuple[float, dict]:
    started = time.perf_counter()
    body = json.dumps({**request, "model": model}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        raise ValueError(
            f"OCR HTTP {exc.code}: {exc.read(4096).decode(errors='replace')}"
        ) from exc
    elapsed = (time.perf_counter() - started) * 1000
    return elapsed, canonical_response(payload, model, len(request["images"]))


def measure_case(
    case: dict,
    targets: dict,
    expected: dict | None,
    *,
    warmup: int,
    iters: int,
    timeout: float,
) -> dict:
    samples = {label: [] for label in targets}
    pairs = []
    golden = expected
    for iteration in range(warmup + iters):
        pair = {"iteration": iteration, "warmup": iteration < warmup, "ms": {}}
        order = list(targets)
        if iteration % 2:
            order.reverse()
        for label in order:
            url, model = targets[label]
            elapsed, output = request_read(url, model, case["request"], timeout)
            pair["ms"][label] = elapsed
            if golden is None:
                golden = output
            if output != golden:
                raise ValueError(
                    f"OCR output or token-count mismatch: {case['id']} / {label}"
                )
            if iteration >= warmup:
                samples[label].append(elapsed)
        pairs.append(pair)
    result = {
        "id": case["id"],
        "fixture": case["fixture"],
        "golden": golden,
        "samples_ms": samples,
        "pairs": pairs,
        "latency": {k: latency_summary(v) for k, v in samples.items()},
        "median_ms": {k: statistics.median(v) for k, v in samples.items()},
    }
    if "reference" in samples:
        result["speedup_over_reference"] = bootstrap_ratio_ci(
            samples["candidate"], samples["reference"], 2000, 1729
        )
        result["median_speedup"] = bootstrap_ratio_ci(
            samples["candidate"], samples["reference"], 2000, 1729, statistic="median"
        )
        result["p95_speedup"] = bootstrap_ratio_ci(
            samples["candidate"], samples["reference"], 2000, 1729, statistic="p95"
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    gold = parser.add_mutually_exclusive_group(required=True)
    gold.add_argument("--golden", type=Path)
    gold.add_argument("--capture-golden", type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--reference-url")
    parser.add_argument("--reference-model")
    for label in ("candidate", "reference"):
        parser.add_argument(f"--{label}-bin", type=Path)
        parser.add_argument(f"--{label}-pid", type=int)
        parser.add_argument(f"--{label}-args")
        parser.add_argument(f"--{label}-build-id")
        parser.add_argument(f"--{label}-model-dir", type=Path)
    parser.add_argument("--case", action="append")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.warmup < 0 or args.iters < 1 or not 0 < args.timeout <= 3600:
        parser.error("invalid warmup, iterations, or timeout")
    if args.capture_golden and (args.capture_golden.exists() or args.reference_url):
        parser.error("capture requires a new golden file and one baseline endpoint")
    targets = {"candidate": (args.url, args.model)}
    if args.reference_url:
        targets["reference"] = (args.reference_url, args.reference_model or args.model)
    report = {
        "schema": SCHEMA,
        "pass": False,
        "cases": [],
        "servers": {},
        "protocol": {
            "warmup": args.warmup,
            "iters": args.iters,
            "order": "alternating AB/BA",
            "timing": "HTTP request including JSON encoding and response decoding",
        },
    }
    try:
        cases = load_cases(args.fixture)
        fixture_sha = sha256_file(args.fixture)
        report["fixture_sha256"] = fixture_sha
        if args.case:
            known = {case["id"] for case in cases}
            if not set(args.case) <= known:
                raise ValueError("unknown selected OCR case")
            cases = [case for case in cases if case["id"] in args.case]
        for label in targets:
            binary, pid, argv_text, build_id, model_dir = [
                getattr(args, f"{label}_{suffix}")
                for suffix in ("bin", "pid", "args", "build_id", "model_dir")
            ]
            if not all(
                value is not None
                for value in (binary, pid, argv_text, build_id, model_dir)
            ):
                raise ValueError(
                    f"{label} needs binary, PID, argv, build ID, and model directory"
                )
            report["servers"][label] = {
                **process_provenance(pid, binary, argv_text),
                "build_id": build_id,
                "artifacts": model_artifacts(model_dir),
            }
        if (
            "reference" in targets
            and report["servers"]["candidate"]["artifacts"]
            != report["servers"]["reference"]["artifacts"]
        ):
            raise ValueError("OCR model artifacts differ between endpoints")
        golden = json.loads(args.golden.read_text()) if args.golden else None
        if golden is not None:
            if (
                golden.get("schema") != GOLDEN_SCHEMA
                or golden.get("fixture_sha256") != fixture_sha
                or golden.get("artifacts")
                != report["servers"]["candidate"]["artifacts"]
            ):
                raise ValueError("golden fixture or artifact identity mismatch")
        for case in cases:
            expected = golden["outputs"][case["id"]] if golden is not None else None
            result = measure_case(
                case,
                targets,
                expected,
                warmup=args.warmup,
                iters=args.iters,
                timeout=args.timeout,
            )
            report["cases"].append(result)
            print(
                json.dumps({"id": case["id"], "latency": result["latency"]}), flush=True
            )
        report["pass"] = True
        if args.capture_golden:
            with args.capture_golden.open("x") as stream:
                json.dump(
                    {
                        "schema": GOLDEN_SCHEMA,
                        "fixture_sha256": fixture_sha,
                        "artifacts": report["servers"]["candidate"]["artifacts"],
                        "baseline": report["servers"]["candidate"],
                        "outputs": {
                            case["id"]: case["golden"] for case in report["cases"]
                        },
                    },
                    stream,
                    indent=2,
                )
    except Exception as exc:
        report["pass"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
    finally:
        write_json_atomic(
            args.output, portable_report(report, workdir=Path.cwd(), home=Path.home())
        )
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
