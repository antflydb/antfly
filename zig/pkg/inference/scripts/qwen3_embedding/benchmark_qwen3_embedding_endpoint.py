#!/usr/bin/env python3
"""Benchmark matched Qwen3-Embedding HTTP endpoints with parity evidence.

Strict comparisons require an exact-token fixture, identical GGUF bytes,
pinned build identities, and recorded server arguments. Samples run in
alternating AB/BA order so thermal drift does not favor either server.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
from pathlib import Path
import random
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from typing import Any
import urllib.request


DEFAULT_SEED = 20260901
COSINE_WARN_THRESHOLD = 0.98
FIXTURE_SCHEMA = "antfly.qwen3_embedding.benchmark_fixture.v1"
REPORT_SCHEMA = "antfly.qwen3_embedding.endpoint_benchmark.v2"

VOCABULARY = [
    "ant", "colony", "vector", "search", "index", "shard", "raft", "quorum",
    "metal", "kernel", "tensor", "batch", "token", "embed", "cosine", "norm",
    "query", "passage", "corpus", "latency", "throughput", "cache", "buffer",
    "graph", "layer", "attention", "pooling", "resident", "pipeline", "frame",
    "quantized", "matrix", "stream", "socket", "server", "client", "replica",
    "storage", "segment", "manifest", "schema", "field", "document", "score",
    "ranker", "encoder", "decoder", "hidden", "weight", "bias", "softmax",
    "gpu", "cpu", "memory", "bandwidth", "profile", "trace", "warmup", "bench",
    "distill", "sparse", "dense", "hybrid",
]

CORPUS_PROFILES = {
    "short": {"kind": "fixed", "target": 20},
    "passage": {"kind": "fixed", "target": 256},
    "long": {"kind": "range", "low": 1024, "high": 8192},
    "mixed": {"kind": "choice", "targets": [20, 64, 256, 1024, 2048, 4096]},
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18099/v1/embeddings")
    parser.add_argument("--reference-url", help="llama.cpp /v1/embeddings endpoint")
    parser.add_argument("--model", default="qwen3-embedding")
    parser.add_argument("--reference-model")
    parser.add_argument("--corpus", choices=sorted(CORPUS_PROFILES), default="mixed")
    parser.add_argument("--fixture", type=Path, help=f"exact-token {FIXTURE_SCHEMA} JSON")
    parser.add_argument("--fixture-cases", help="comma-separated fixture case IDs")
    parser.add_argument("--fixture-token-count", type=int, help="select exact-length fixture cases")
    parser.add_argument(
        "--antfly-reported-token-offset",
        type=int,
        default=0,
        help="per-input difference between Antfly usage.prompt_tokens and fixture model tokens",
    )
    parser.add_argument(
        "--reference-reported-token-offset",
        type=int,
        default=0,
        help="per-input difference between reference usage.prompt_tokens and fixture model tokens",
    )
    parser.add_argument("--batch-sizes", default="1,8,32,128")
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument(
        "--precondition-iters",
        type=int,
        default=0,
        help="unmeasured alternating iterations on two reserved fixture batches",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--cosine-threshold", type=float, default=COSINE_WARN_THRESHOLD)
    parser.add_argument("--fail-below-ratio", type=float)
    parser.add_argument("--require-comparable", action="store_true")
    parser.add_argument("--antfly-model-file", type=Path)
    parser.add_argument("--reference-model-file", type=Path)
    parser.add_argument("--antfly-build-id")
    parser.add_argument("--reference-build-id")
    parser.add_argument("--antfly-build-file", type=Path)
    parser.add_argument("--reference-build-file", type=Path)
    parser.add_argument("--antfly-server-pid", type=int)
    parser.add_argument("--reference-server-pid", type=int)
    parser.add_argument(
        "--antfly-server-args",
        help="shell-quoted live argv after the Antfly executable; strict mode matches it exactly",
    )
    parser.add_argument(
        "--reference-server-args",
        help="shell-quoted live argv after the reference executable; strict mode matches it exactly",
    )
    parser.add_argument("--output", type=Path, help="write results JSON to this path")
    args = parser.parse_args(argv)
    args.batch_sizes = [int(value) for value in args.batch_sizes.split(",") if value.strip()]
    if not args.batch_sizes or any(value < 1 for value in args.batch_sizes):
        parser.error("batch sizes must be positive")
    if args.warmup < 0 or args.precondition_iters < 0 or args.iters < 1:
        parser.error(
            "warmup and precondition-iters must be non-negative and iters must be positive"
        )
    if args.precondition_iters and args.fixture is None:
        parser.error("precondition-iters requires an exact-token fixture")
    if args.timeout <= 0 or args.bootstrap_samples < 1:
        parser.error("timeout and bootstrap samples must be positive")
    if args.fixture_token_count is not None and args.fixture_token_count < 1:
        parser.error("fixture token count must be positive")
    if not 0.0 <= args.cosine_threshold <= 1.0:
        parser.error("cosine threshold must be in [0, 1]")
    if args.fail_below_ratio is not None and args.fail_below_ratio <= 0.0:
        parser.error("fail-below-ratio must be positive")
    if args.require_comparable:
        missing = []
        for name in (
            "reference_url", "fixture", "antfly_model_file", "reference_model_file",
            "antfly_build_id", "reference_build_id", "antfly_build_file",
            "reference_build_file", "antfly_server_pid", "reference_server_pid",
            "antfly_server_args", "reference_server_args",
        ):
            if not getattr(args, name):
                missing.append("--" + name.replace("_", "-"))
        if missing:
            parser.error("--require-comparable needs " + ", ".join(missing))
    return args


def approx_token_count(text: str) -> int:
    return len(text.split())


def corpus_lengths(corpus: str, count: int, seed: int = DEFAULT_SEED) -> list[int]:
    profile = CORPUS_PROFILES[corpus]
    rng = random.Random(f"{seed}:{corpus}:{count}")
    lengths = []
    for _ in range(count):
        if profile["kind"] == "fixed":
            lengths.append(profile["target"])
        elif profile["kind"] == "range":
            lengths.append(rng.randint(profile["low"], profile["high"]))
        else:
            lengths.append(rng.choice(profile["targets"]))
    return lengths


def generate_text(target_tokens: int, rng: random.Random) -> str:
    return " ".join(rng.choice(VOCABULARY) for _ in range(max(1, target_tokens)))


def build_corpus(corpus: str, count: int, seed: int = DEFAULT_SEED) -> list[str]:
    return [
        generate_text(target, random.Random(f"{seed}:{corpus}:{count}:{index}"))
        for index, target in enumerate(corpus_lengths(corpus, count, seed))
    ]


def load_fixture(
    path: Path, expected_model_sha256: str | None = None
) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid fixture {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema") != FIXTURE_SCHEMA:
        raise ValueError(f"fixture must use schema {FIXTURE_SCHEMA}")
    if expected_model_sha256 is not None:
        fixture_sha256 = payload.get("model_sha256")
        if (
            not isinstance(fixture_sha256, str)
            or fixture_sha256.lower() != expected_model_sha256.lower()
        ):
            raise ValueError(
                f"fixture model SHA-256 {fixture_sha256!r} does not match "
                f"benchmark model {expected_model_sha256}"
            )
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("fixture has no cases")
    validated = []
    seen = set()
    for raw in cases:
        if not isinstance(raw, dict):
            raise ValueError("fixture case is not an object")
        case_id, text, token_ids = raw.get("id"), raw.get("text"), raw.get("token_ids")
        if not isinstance(case_id, str) or not case_id or case_id in seen:
            raise ValueError(f"invalid or duplicate fixture id: {case_id!r}")
        if not isinstance(text, str) or not text:
            raise ValueError(f"fixture case {case_id}: text is empty")
        if not isinstance(token_ids, list) or not token_ids or not all(
            isinstance(token, int) and not isinstance(token, bool) for token in token_ids
        ):
            raise ValueError(f"fixture case {case_id}: token_ids are invalid")
        seen.add(case_id)
        validated.append({"id": case_id, "text": text, "token_ids": token_ids})
    return validated


def fixture_batch(
    cases: list[dict[str, Any]], count: int, offset: int = 0
) -> tuple[list[str], list[int]]:
    selected = [cases[(offset + index) % len(cases)] for index in range(count)]
    return [case["text"] for case in selected], [len(case["token_ids"]) for case in selected]


def select_fixture_cases(
    cases: list[dict[str, Any]], raw_ids: str | None
) -> list[dict[str, Any]]:
    if raw_ids is None:
        return cases
    requested = [value.strip() for value in raw_ids.split(",") if value.strip()]
    if not requested:
        raise ValueError("fixture case selection is empty")
    by_id = {case["id"]: case for case in cases}
    missing = [case_id for case_id in requested if case_id not in by_id]
    if missing:
        raise ValueError(f"fixture lacks requested cases: {missing}")
    return [by_id[case_id] for case_id in requested]


def select_fixture_token_count(
    cases: list[dict[str, Any]], token_count: int | None
) -> list[dict[str, Any]]:
    if token_count is None:
        return cases
    selected = [case for case in cases if len(case["token_ids"]) == token_count]
    if not selected:
        raise ValueError(f"fixture has no {token_count}-token cases")
    return selected


def validate_cache_neutral_cases(
    cases: list[dict[str, Any]], needed_cases: int
) -> None:
    """Fail closed unless every consumed prompt defeats active-slot LCP reuse."""
    if len(cases) < needed_cases:
        raise ValueError(
            f"strict cache-neutral run needs {needed_cases} distinct fixture cases, "
            f"got {len(cases)}"
        )
    consumed = cases[:needed_cases]
    identities = {
        "case ids": [case["id"] for case in consumed],
        "texts": [case["text"] for case in consumed],
        "token sequences": [tuple(case["token_ids"]) for case in consumed],
        "first token ids": [case["token_ids"][0] for case in consumed],
    }
    duplicates = [
        label for label, values in identities.items() if len(set(values)) != len(values)
    ]
    if duplicates:
        raise ValueError(
            "strict cache-neutral fixture reuses " + ", ".join(duplicates)
        )


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        raise ValueError("no samples")
    ordered = sorted(samples)
    rank = math.ceil(fraction * len(ordered))
    return ordered[max(0, min(len(ordered), rank) - 1)]


def latency_summary(samples_ms: list[float]) -> dict[str, float]:
    return {
        "mean_ms": statistics.fmean(samples_ms),
        "p50_ms": percentile(samples_ms, 0.50),
        "p95_ms": percentile(samples_ms, 0.95),
        "p99_ms": percentile(samples_ms, 0.99),
        "min_ms": min(samples_ms),
        "max_ms": max(samples_ms),
    }


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if len(left) != len(right):
        raise ValueError(f"dimension mismatch: {len(left)} vs {len(right)}")
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    return 0.0 if left_norm == 0.0 or right_norm == 0.0 else dot / (left_norm * right_norm)


def cross_check(
    actual: list[list[float]], reference: list[list[float]], threshold: float = COSINE_WARN_THRESHOLD
) -> dict[str, Any]:
    if len(actual) != len(reference):
        raise ValueError("embedding counts differ between servers")
    cosines = [cosine_similarity(a, r) for a, r in zip(actual, reference)]
    max_abs = max(
        (abs(a_v - r_v) for a, r in zip(actual, reference) for a_v, r_v in zip(a, r)),
        default=0.0,
    )
    return {
        "min_cosine": min(cosines),
        "mean_cosine": statistics.fmean(cosines),
        "max_abs_error": max_abs,
        "threshold": threshold,
        "pass": min(cosines) >= threshold,
    }


def interleaved_schedule(iterations: int) -> list[str]:
    schedule = []
    for index in range(iterations):
        schedule.extend(("antfly", "reference") if index % 2 == 0 else ("reference", "antfly"))
    return schedule


def bootstrap_ratio_ci(
    antfly_ms: list[float], reference_ms: list[float], samples: int, seed: int
) -> dict[str, float]:
    if len(antfly_ms) != len(reference_ms) or not antfly_ms:
        raise ValueError("bootstrap inputs must be non-empty and paired")
    rng = random.Random(seed)
    ratios = []
    for _ in range(samples):
        indices = [rng.randrange(len(antfly_ms)) for _ in antfly_ms]
        antfly_mean = statistics.fmean(antfly_ms[index] for index in indices)
        reference_mean = statistics.fmean(reference_ms[index] for index in indices)
        ratios.append(reference_mean / antfly_mean)
    return {
        "estimate": statistics.fmean(reference_ms) / statistics.fmean(antfly_ms),
        "lower_95": percentile(ratios, 0.025),
        "upper_95": percentile(ratios, 0.975),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def model_provenance(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, path in (
        ("antfly", args.antfly_model_file), ("reference", args.reference_model_file)
    ):
        if path is not None:
            result[label] = {"path": str(path), "sha256": sha256_file(path)}
    hashes = [entry["sha256"] for label, entry in result.items() if label != "identical"]
    result["identical"] = len(hashes) == 2 and hashes[0] == hashes[1]
    if args.require_comparable and not result["identical"]:
        raise ValueError("strict comparison requires byte-identical model files")
    return result


def _resolve_executable(value: str) -> Path | None:
    candidate = Path(value).expanduser()
    if candidate.is_absolute() or "/" in value:
        return candidate.resolve()
    located = shutil.which(value)
    return Path(located).resolve() if located else None


def process_provenance(
    pid: int, build_file: Path, expected_args: str | None = None
) -> dict[str, Any]:
    if pid <= 0:
        raise ValueError(f"server PID must be positive, got {pid}")
    declared_executable = build_file.expanduser().resolve(strict=True)
    command_result = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "command="],
        check=False,
        capture_output=True,
        text=True,
    )
    if command_result.returncode != 0 or not command_result.stdout.strip():
        detail = command_result.stderr.strip() or "process not found"
        raise ValueError(f"cannot attest server PID {pid}: {detail}")
    command = command_result.stdout.strip()
    try:
        observed_argv = shlex.split(command)
        argv0 = observed_argv[0]
    except (ValueError, IndexError) as exc:
        raise ValueError(f"cannot parse command for server PID {pid}: {command!r}") from exc
    observed_executable = _resolve_executable(argv0)
    if observed_executable != declared_executable:
        comm_result = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "comm="],
            check=False,
            capture_output=True,
            text=True,
        )
        observed_executable = (
            _resolve_executable(comm_result.stdout.strip())
            if comm_result.returncode == 0 and comm_result.stdout.strip()
            else observed_executable
        )
    if observed_executable != declared_executable:
        raise ValueError(
            f"server PID {pid} executable {observed_executable} does not match "
            f"declared build file {declared_executable}"
        )
    declared_argv = shlex.split(expected_args) if expected_args is not None else None
    if declared_argv is not None and observed_argv[1:] != declared_argv:
        raise ValueError(
            f"server PID {pid} arguments do not match: "
            f"declared={declared_argv!r}, observed={observed_argv[1:]!r}"
        )
    return {
        "pid": pid,
        "command": command,
        "executable": str(declared_executable),
        "executable_sha256": sha256_file(declared_executable),
        "argv": observed_argv[1:],
    }


def request_embeddings(
    url: str, model: str, texts: list[str], timeout: float
) -> tuple[float, list[list[float]], str, int | None]:
    body = json.dumps({"model": model, "input": texts}).encode()
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    start = time.perf_counter()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or len(data) != len(texts):
        raise RuntimeError(
            f"expected {len(texts)} embeddings, got {len(data) if isinstance(data, list) else 'none'}"
        )
    embeddings = [item.get("embedding") if isinstance(item, dict) else None for item in data]
    if any(not isinstance(vector, list) or not vector for vector in embeddings):
        raise RuntimeError("response contains an empty embedding vector")
    dimensions = {len(vector) for vector in embeddings}
    if len(dimensions) != 1:
        raise RuntimeError(f"inconsistent embedding dimensions: {sorted(dimensions)}")
    usage = payload.get("usage") if isinstance(payload, dict) else None
    prompt_tokens = usage.get("prompt_tokens") if isinstance(usage, dict) else None
    if not isinstance(prompt_tokens, int) or isinstance(prompt_tokens, bool) or prompt_tokens < 0:
        prompt_tokens = None
    return elapsed_ms, embeddings, str(payload.get("model", "")), prompt_tokens


def result_for_target(
    label: str,
    corpus: str,
    samples_ms: list[float],
    embeddings: list[list[float]],
    token_counts: list[int],
    token_count_source: str,
    reported_prompt_tokens: int | None = None,
) -> dict[str, Any]:
    summary = latency_summary(samples_ms)
    total_tokens = sum(token_counts)
    return {
        "target": label,
        "corpus": corpus,
        "batch": len(embeddings),
        "iters": len(samples_ms),
        "latency": summary,
        "embeddings_per_second": len(embeddings) * 1000.0 / summary["mean_ms"],
        "input_tokens": total_tokens,
        "reported_prompt_tokens": reported_prompt_tokens,
        "tokens_per_second": total_tokens * 1000.0 / summary["mean_ms"],
        "token_count_source": token_count_source,
        "dimensions": len(embeddings[0]),
        "samples_ms": samples_ms,
    }


def run_cell(
    args: argparse.Namespace,
    request_batches: list[tuple[list[str], list[int]]],
    token_source: str,
    precondition_batches: list[tuple[list[str], list[int]]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, str]]:
    targets = {"antfly": (args.url, args.model)}
    if args.reference_url:
        targets["reference"] = (args.reference_url, args.reference_model or args.model)
    last_embeddings: dict[str, list[list[float]]] = {}
    last_prompt_tokens: dict[str, int | None] = {}
    observed_prompt_tokens: dict[str, list[int | None]] = {label: [] for label in targets}
    observed_models: dict[str, list[str]] = {label: [] for label in targets}
    reported: dict[str, str] = {}
    samples: dict[str, list[float]] = {label: [] for label in targets}
    for iteration, (texts, _) in enumerate(precondition_batches or []):
        order = ("antfly", "reference") if iteration % 2 == 0 else ("reference", "antfly")
        if not args.reference_url:
            order = ("antfly",)
        for label in order:
            (
                _, last_embeddings[label], reported[label], last_prompt_tokens[label]
            ) = request_embeddings(targets[label][0], targets[label][1], texts, args.timeout)
            observed_prompt_tokens[label].append(last_prompt_tokens[label])
            observed_models[label].append(reported[label])
    for iteration in range(args.warmup):
        texts, _ = request_batches[iteration]
        order = ("antfly", "reference") if iteration % 2 == 0 else ("reference", "antfly")
        if not args.reference_url:
            order = ("antfly",)
        for label in order:
            (
                _, last_embeddings[label], reported[label], last_prompt_tokens[label]
            ) = request_embeddings(targets[label][0], targets[label][1], texts, args.timeout)
            observed_prompt_tokens[label].append(last_prompt_tokens[label])
            observed_models[label].append(reported[label])
    measured_token_counts: list[list[int]] = []
    parity_checks: list[dict[str, Any]] = []
    for iteration in range(args.iters):
        texts, token_counts = request_batches[args.warmup + iteration]
        measured_token_counts.append(token_counts)
        order = ("antfly", "reference") if iteration % 2 == 0 else ("reference", "antfly")
        if not args.reference_url:
            order = ("antfly",)
        for label in order:
            (
                elapsed, last_embeddings[label], reported[label], last_prompt_tokens[label]
            ) = request_embeddings(targets[label][0], targets[label][1], texts, args.timeout)
            observed_prompt_tokens[label].append(last_prompt_tokens[label])
            observed_models[label].append(reported[label])
            samples[label].append(elapsed)
        if args.reference_url:
            parity_checks.append(
                cross_check(
                    last_embeddings["antfly"], last_embeddings["reference"],
                    args.cosine_threshold,
                )
            )
    expected_totals = {sum(counts) for counts in measured_token_counts}
    if len(expected_totals) != 1:
        raise ValueError("all measured fixture batches must have the same total token count")
    token_counts = measured_token_counts[0]
    if args.require_comparable:
        expected_tokens = sum(token_counts)
        reported_offsets = {
            "antfly": args.antfly_reported_token_offset,
            "reference": args.reference_reported_token_offset,
        }
        mismatches = {
            label: {
                "expected": expected_tokens + reported_offsets[label] * len(token_counts),
                "observed": sorted(
                    set(observed_prompt_tokens[label]),
                    key=lambda value: -1 if value is None else value,
                ),
            }
            for label in targets
            if any(
                value != expected_tokens + reported_offsets[label] * len(token_counts)
                for value in observed_prompt_tokens[label]
            )
        }
        if mismatches:
            raise ValueError(
                f"strict token-count mismatch: fixture model tokens={expected_tokens}, "
                f"reported={mismatches}"
            )
        model_mismatches = {
            label: {"expected": targets[label][1], "observed": sorted(set(values))}
            for label, values in observed_models.items()
            if any(value != targets[label][1] for value in values)
        }
        if model_mismatches:
            raise ValueError(f"strict live-model mismatch: {model_mismatches}")
    results = [
        result_for_target(
            label,
            args.corpus,
            samples[label],
            last_embeddings[label],
            token_counts,
            token_source,
            last_prompt_tokens.get(label),
        )
        for label in targets
    ]
    comparison = None
    if args.reference_url:
        if not parity_checks:
            raise ValueError("reference comparison has no measured parity checks")
        comparison = {
            "min_cosine": min(check["min_cosine"] for check in parity_checks),
            "mean_cosine": statistics.fmean(check["mean_cosine"] for check in parity_checks),
            "max_abs_error": max(check["max_abs_error"] for check in parity_checks),
            "threshold": args.cosine_threshold,
            "pass": all(check["pass"] for check in parity_checks),
            "parity_iterations": len(parity_checks),
            "throughput_ratio_antfly_over_reference": bootstrap_ratio_ci(
                samples["antfly"], samples["reference"], args.bootstrap_samples,
                args.seed + len(texts),
            ),
            "order": "alternating AB/BA",
        }
        threshold = args.fail_below_ratio
        comparison["ratio_gate"] = {
            "threshold": threshold,
            "pass": threshold is None
            or comparison["throughput_ratio_antfly_over_reference"]["lower_95"] >= threshold,
        }
    return results, comparison, reported


def print_row(result: dict[str, Any]) -> None:
    latency = result["latency"]
    print(
        f"{result['target']},{result['corpus']},{result['batch']},{result['input_tokens']},"
        f"{latency['mean_ms']:.3f},{latency['p50_ms']:.3f},{latency['p95_ms']:.3f},"
        f"{result['embeddings_per_second']:.2f},{result['tokens_per_second']:.1f},"
        f"{result['token_count_source']},{result['dimensions']}"
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        provenance = model_provenance(args)
        process_attestations = (
            {
                "antfly": process_provenance(
                    args.antfly_server_pid,
                    args.antfly_build_file,
                    args.antfly_server_args,
                ),
                "reference": process_provenance(
                    args.reference_server_pid,
                    args.reference_build_file,
                    args.reference_server_args,
                ),
            }
            if args.require_comparable
            else {}
        )
        fixture_model_sha256 = (
            provenance.get("antfly", {}).get("sha256") if args.require_comparable else None
        )
        fixture = (
            select_fixture_cases(
                load_fixture(args.fixture, fixture_model_sha256), args.fixture_cases
            )
            if args.fixture
            else None
        )
        if fixture is not None:
            fixture = select_fixture_token_count(fixture, args.fixture_token_count)
    except (OSError, ValueError) as exc:
        print(f"benchmark configuration failed: {exc}", file=sys.stderr)
        return 2
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "args": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "comparison_contract": {
            "strict": args.require_comparable,
            "sample_order": "alternating AB/BA" if args.reference_url else "single target",
            "thermal_preconditioning": {
                "iterations": args.precondition_iters,
                "measured": False,
                "fixture_batches_reserved": 2 if args.precondition_iters else 0,
            },
            "model_files": provenance,
            "build_ids": {"antfly": args.antfly_build_id, "reference": args.reference_build_id},
            "live_processes": process_attestations,
            "server_args": {
                "antfly": args.antfly_server_args,
                "reference": args.reference_server_args,
            },
        },
        "servers": {"antfly": {"url": args.url, "model_reported": ""}},
        "results": [],
        "comparisons": [],
    }
    if args.reference_url:
        report["servers"]["reference"] = {"url": args.reference_url, "model_reported": ""}
    print("target,corpus,batch,input_tokens,mean_ms,p50_ms,p95_ms,embeddings_s,tokens_s,token_source,dims")
    failed = False
    for batch in args.batch_sizes:
        if fixture:
            request_count = args.warmup + args.iters
            needed_cases = request_count * batch
            reserve_cases = 2 * batch if args.precondition_iters else 0
            if args.require_comparable:
                try:
                    validate_cache_neutral_cases(fixture, needed_cases + reserve_cases)
                except ValueError as exc:
                    print(f"benchmark configuration failed: {exc}", file=sys.stderr)
                    return 2
            request_batches = [
                fixture_batch(fixture, batch, offset=index * batch)
                for index in range(request_count)
            ]
            precondition_batches = [
                fixture_batch(
                    fixture,
                    batch,
                    offset=needed_cases + (index % 2) * batch,
                )
                for index in range(args.precondition_iters)
            ]
            token_source = f"exact:{args.fixture}"
        else:
            texts = build_corpus(args.corpus, batch, args.seed)
            token_counts = [approx_token_count(text) for text in texts]
            request_batches = [(texts, token_counts)] * (args.warmup + args.iters)
            precondition_batches = []
            token_source = "approximate:whitespace_words"
        try:
            results, comparison, reported = run_cell(
                args, request_batches, token_source, precondition_batches
            )
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"benchmark request failed: {exc}", file=sys.stderr)
            return 2
        for result in results:
            report["results"].append(result)
            report["servers"][result["target"]]["model_reported"] = reported[result["target"]]
            print_row(result)
        if comparison is not None:
            comparison["batch"] = batch
            report["comparisons"].append(comparison)
            if not comparison["pass"] or not comparison["ratio_gate"]["pass"]:
                failed = True
    report["pass"] = not failed
    if args.output:
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {args.output}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
