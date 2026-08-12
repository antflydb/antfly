#!/usr/bin/env python3
"""Gate CUDA continuous batching against the serialized server baseline."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import shlex
import socket
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
from typing import Any


VALID_CACHE_DTYPES = frozenset({"f16", "f32", "int8", "fp8", "int4", "polar4", "turbo3"})
SCHEDULER_COUNTERS = (
    "antfly_inference_native_scheduler_step_batches_total",
    "antfly_inference_native_scheduler_step_prefill_items_total",
    "antfly_inference_native_scheduler_step_decode_items_total",
    "antfly_inference_native_scheduler_step_query_tokens_total",
    "antfly_inference_native_scheduler_step_singleton_batches_total",
    "antfly_inference_native_scheduler_step_kv_block_skips_total",
    "antfly_inference_native_scheduler_decode_coalesce_waits_total",
    "antfly_inference_native_scheduler_decode_coalesce_wait_us_total",
    "antfly_inference_native_scheduler_step_batch_size_2_total",
    "antfly_inference_native_scheduler_step_batch_size_3_4_total",
    "antfly_inference_native_scheduler_step_batch_size_5_8_total",
    "antfly_inference_native_scheduler_step_batch_size_9_16_total",
    "antfly_inference_native_scheduler_turn_yields_total",
)
ROW_TWO_COUNTER = "antfly_inference_native_scheduler_step_batch_size_2_total"


def parse_prometheus_counters(text: str) -> dict[str, int | float]:
    """Parse unlabelled counter samples from Prometheus text exposition."""
    counter_names: set[str] = set()
    samples: dict[str, int | float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("# TYPE "):
            fields = line.split()
            if len(fields) == 4 and fields[3] == "counter":
                counter_names.add(fields[2])
            continue
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 2 or "{" in fields[0] or fields[0] not in counter_names:
            continue
        try:
            value = float(fields[1])
        except ValueError as exc:
            raise ValueError(f"invalid Prometheus counter value for {fields[0]}: {fields[1]}") from exc
        samples[fields[0]] = int(value) if value.is_integer() else value
    return samples


def require_scheduler_counters(text: str) -> dict[str, int | float]:
    counters = parse_prometheus_counters(text)
    missing = [name for name in SCHEDULER_COUNTERS if name not in counters]
    if missing:
        raise RuntimeError(f"scheduler metrics unavailable; missing counters: {', '.join(missing)}")
    return {name: counters[name] for name in SCHEDULER_COUNTERS}


def counter_delta(
    before: dict[str, int | float],
    after: dict[str, int | float],
) -> dict[str, int | float]:
    delta: dict[str, int | float] = {}
    for name in SCHEDULER_COUNTERS:
        value = after[name] - before[name]
        if value < 0:
            raise RuntimeError(f"scheduler counter decreased during benchmark: {name}")
        delta[name] = value
    return delta


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((pct / 100.0) * len(ordered) + 0.999999) - 1))
    return ordered[index]


def stats(values: list[float]) -> dict[str, float]:
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "p95": percentile(values, 95),
        "max": max(values),
    }


def choose_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def post_json(url: str, body: dict, timeout: float = 600.0) -> tuple[float, dict]:
    payload = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')}") from exc
    elapsed_ms = (time.monotonic() - started) * 1000.0
    return elapsed_ms, json.loads(raw)


def response_fingerprint(response: dict) -> str:
    encoded = json.dumps(response_summary(response), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def response_summary(response: dict) -> dict:
    choice = response["choices"][0]
    return {
        "prompt_tokens": int(response.get("usage", {}).get("prompt_tokens", 0)),
        "completion_tokens": int(response.get("usage", {}).get("completion_tokens", 0)),
        "finish_reason": choice.get("finish_reason"),
        "content": choice.get("message", {}).get("content", choice.get("text", "")),
    }


class Server:
    def __init__(self, args: argparse.Namespace, mode: str, output_dir: pathlib.Path):
        self.args = args
        self.mode = mode
        self.output_dir = output_dir
        self.port = choose_port()
        self.process: subprocess.Popen | None = None
        self.log = None

    def __enter__(self) -> "Server":
        config_path = self.output_dir / f"server-{self.mode}.json"
        config_path.write_text(
            json.dumps(
                {
                    "models_dir": str(self.args.models_dir),
                    "max_concurrent_requests": 32,
                    "generation_batching": {
                        "mode": self.mode,
                        "max_step_items": self.args.max_step_items,
                        "max_step_query_tokens": 512,
                        "max_decode_wait_us": self.args.decode_wait_us,
                    },
                },
                indent=2,
            )
            + "\n"
        )
        self.log = (self.output_dir / f"server-{self.mode}.log").open("wb")
        env = os.environ.copy()
        env.setdefault("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE", "0")
        env.setdefault("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK", "0")
        env.pop("ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING", None)
        command = [
            str(self.args.antfly_bin),
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--models-dir",
            str(self.args.models_dir),
            "--config",
            str(config_path),
        ]
        if self.args.server_prefix:
            command = shlex.split(self.args.server_prefix) + command
        self.process = subprocess.Popen(
            command,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        health = f"http://127.0.0.1:{self.port}/healthz"
        deadline = time.monotonic() + self.args.startup_timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server {self.mode} exited with {self.process.returncode}")
            try:
                with urllib.request.urlopen(health, timeout=1):
                    return self
            except Exception:
                time.sleep(0.25)
        raise RuntimeError(f"server {self.mode} did not become ready")

    def __exit__(self, *_: object) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()

    @property
    def generate_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/ai/v1/generate"

    @property
    def metrics_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/ml/v1/metrics"

    def scheduler_metrics(self) -> tuple[str, dict[str, int | float]]:
        try:
            with urllib.request.urlopen(self.metrics_url, timeout=5) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except Exception as exc:
            raise RuntimeError(f"failed to read scheduler metrics from {self.metrics_url}: {exc}") from exc
        if not raw.strip():
            raise RuntimeError(f"scheduler metrics endpoint returned an empty response: {self.metrics_url}")
        return raw, require_scheduler_counters(raw)


RequestCase = tuple[str, dict[str, Any]]


def run_wave(
    server: Server,
    cases: list[RequestCase],
    concurrency: int,
    *,
    offset: int = 0,
    stagger_ms: float = 0.0,
) -> dict:
    if not cases:
        raise ValueError("at least one request case is required")
    barrier = threading.Barrier(concurrency)

    def request(index: int, case: RequestCase) -> tuple[str, float, dict]:
        barrier.wait()
        if stagger_ms > 0:
            time.sleep(index * stagger_ms / 1000.0)
        elapsed_ms, response = post_json(server.generate_url, case[1])
        return case[0], elapsed_ms, response

    started = time.monotonic()
    selected = [cases[(offset + index) % len(cases)] for index in range(concurrency)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(request, index, case) for index, case in enumerate(selected)]
        results = [future.result() for future in futures]
    wall_ms = (time.monotonic() - started) * 1000.0
    case_ids = [case_id for case_id, _, _ in results]
    latencies = [elapsed for _, elapsed, _ in results]
    tokens = [int(response.get("usage", {}).get("completion_tokens", 0)) for _, _, response in results]
    return {
        "wall_ms": wall_ms,
        "latencies_ms": latencies,
        "completion_tokens": tokens,
        "aggregate_tok_s": sum(tokens) * 1000.0 / wall_ms,
        "case_ids": case_ids,
        "fingerprints": [response_fingerprint(response) for _, _, response in results],
        "responses": [response_summary(response) for _, _, response in results],
    }


def keyed_wave_values(waves: list[dict], field: str) -> dict[str, list[Any]]:
    values: dict[str, set[Any]] = {}
    for wave in waves:
        for case_id, value in zip(wave["case_ids"], wave[field], strict=True):
            values.setdefault(case_id, set()).add(value)
    return {case_id: sorted(items) for case_id, items in sorted(values.items())}


def measure_mode(server: Server, cases: list[RequestCase], concurrencies: list[int], warmups: int, repeats: int) -> dict:
    for warmup in range(warmups):
        for case_offset in range(len(cases)):
            run_wave(server, cases, 1, offset=warmup + case_offset)
    measured = {}
    final_metrics = ""
    final_counters: dict[str, int | float] = {}
    for concurrency in concurrencies:
        _, before = server.scheduler_metrics()
        wave_count = max(repeats, len(cases)) if concurrency == 1 else repeats
        waves = [run_wave(server, cases, concurrency, offset=repeat) for repeat in range(wave_count)]
        final_metrics, after = server.scheduler_metrics()
        measured[str(concurrency)] = {
            "aggregate_tok_s": stats([wave["aggregate_tok_s"] for wave in waves]),
            "wall_ms": stats([wave["wall_ms"] for wave in waves]),
            "request_latency_ms": stats([value for wave in waves for value in wave["latencies_ms"]]),
            "fingerprints": sorted({fingerprint for wave in waves for fingerprint in wave["fingerprints"]}),
            "fingerprints_by_case": keyed_wave_values(waves, "fingerprints"),
            "prompt_tokens_by_case": keyed_wave_values(
                [
                    {
                        "case_ids": wave["case_ids"],
                        "prompt_tokens": [response["prompt_tokens"] for response in wave["responses"]],
                    }
                    for wave in waves
                ],
                "prompt_tokens",
            ),
            "scheduler_counter_delta": counter_delta(before, after),
            "waves": waves,
        }
        final_counters = after
    return {
        "measurements": measured,
        "metrics": final_metrics,
        "scheduler_counters": final_counters,
    }


def evaluate_acceptance(
    baseline: dict,
    batched: dict,
    min_c2_speedup: float,
    max_c1_p95_ratio: float,
    isolation_probe: dict,
) -> dict:
    baseline_c1 = baseline["measurements"]["1"]
    batched_c1 = batched["measurements"]["1"]
    batched_c2 = batched["measurements"]["2"]
    baseline_tok_s = baseline_c1["aggregate_tok_s"]["median"]
    if baseline_tok_s <= 0:
        raise ValueError("off-mode C1 throughput must be positive")
    baseline_p95 = baseline_c1["request_latency_ms"]["p95"]
    if baseline_p95 <= 0:
        raise ValueError("off-mode C1 p95 latency must be positive")

    expected_fingerprints = baseline_c1["fingerprints_by_case"]

    def exact_by_case(measurement: dict) -> bool:
        observed = measurement["fingerprints_by_case"]
        return (
            bool(expected_fingerprints)
            and observed.keys() == expected_fingerprints.keys()
            and all(len(values) == 1 and observed[case_id] == values for case_id, values in expected_fingerprints.items())
        )

    expected_values = [values[0] for values in expected_fingerprints.values() if len(values) == 1]
    baseline_cases_distinct = len(expected_values) == len(expected_fingerprints) and len(set(expected_values)) == len(expected_values)
    prompt_token_values = baseline_c1["prompt_tokens_by_case"]
    prompt_token_lengths_equal = (
        len(prompt_token_values) == len(expected_fingerprints)
        and all(len(values) == 1 and values[0] > 0 for values in prompt_token_values.values())
        and len({values[0] for values in prompt_token_values.values()}) == 1
    )
    c1_exact = exact_by_case(batched_c1)
    c2_exact = exact_by_case(batched_c2)
    c2_speedup = batched_c2["aggregate_tok_s"]["median"] / baseline_tok_s
    c1_p95_ratio = batched_c1["request_latency_ms"]["p95"] / baseline_p95
    row_two_steps = batched_c2["scheduler_counter_delta"][ROW_TWO_COUNTER]

    diagnostics = {}
    for concurrency, measurement in batched["measurements"].items():
        if int(concurrency) < 4:
            continue
        diagnostics[concurrency] = {
            "aggregate_throughput_ratio_vs_off_c1": measurement["aggregate_tok_s"]["median"] / baseline_tok_s,
            "exact_response_fingerprints": exact_by_case(measurement),
            "scheduler_counter_delta": measurement["scheduler_counter_delta"],
        }

    passed = (
        c2_speedup >= min_c2_speedup
        and c1_p95_ratio <= max_c1_p95_ratio
        and c1_exact
        and c2_exact
        and baseline_cases_distinct
        and prompt_token_lengths_equal
        and isolation_probe["passed"]
        and row_two_steps > 0
    )
    return {
        "c2_aggregate_speedup": c2_speedup,
        "min_c2_aggregate_speedup": min_c2_speedup,
        "c1_p95_latency_ratio": c1_p95_ratio,
        "max_c1_p95_latency_ratio": max_c1_p95_ratio,
        "c1_exact_response_fingerprints": c1_exact,
        "c2_exact_response_fingerprints": c2_exact,
        "baseline_case_fingerprints_distinct": baseline_cases_distinct,
        "prompt_token_lengths_equal": prompt_token_lengths_equal,
        "isolation_probe": isolation_probe,
        "c2_step_batch_size_2_total": row_two_steps,
        "c2_row_two_steps_positive": row_two_steps > 0,
        "concurrency_4_plus_diagnostics": diagnostics,
        "passed": passed,
    }


def evaluate_isolation_probe(
    baseline_measurement: dict,
    waves: list[dict],
    scheduler_counter_delta: dict[str, int | float],
    cases: list[RequestCase],
    stagger_ms: float,
) -> dict:
    expected = baseline_measurement["fingerprints_by_case"]
    observed = keyed_wave_values(waves, "fingerprints")
    exact = (
        observed.keys() == expected.keys()
        and bool(expected)
        and all(len(values) == 1 and observed[case_id] == values for case_id, values in expected.items())
    )
    expected_values = [values[0] for values in expected.values() if len(values) == 1]
    distinct = len(expected_values) == len(expected) and len(set(expected_values)) == len(expected_values)
    requested_limits = sorted({int(body["max_tokens"]) for _, body in cases})
    mixed_output_limits = len(requested_limits) > 1
    repeated_kv_growth = len(waves) >= 2 and max(requested_limits, default=0) > 16
    row_two_steps = scheduler_counter_delta[ROW_TWO_COUNTER]
    passed = (
        exact
        and distinct
        and stagger_ms > 0
        and mixed_output_limits
        and repeated_kv_growth
    )
    return {
        "expected_fingerprints_by_case": expected,
        "observed_fingerprints_by_case": observed,
        "exact_response_fingerprints": exact,
        "baseline_case_fingerprints_distinct": distinct,
        "stagger_ms": stagger_ms,
        "staggered_arrivals": stagger_ms > 0,
        "requested_max_tokens": requested_limits,
        "mixed_output_limits": mixed_output_limits,
        "wave_count": len(waves),
        "repeated_kv_page_growth": repeated_kv_growth,
        "scheduler_counter_delta": scheduler_counter_delta,
        "row_two_steps_positive": row_two_steps > 0,
        "passed": passed,
    }


def parse_args() -> argparse.Namespace:
    repo = pathlib.Path(__file__).resolve().parents[4]
    tuning_wrapper = repo / "zig/pkg/inference/scripts/with_gemma4_qat_cuda_tuning.sh"
    default_server_prefix = shlex.join(
        [
            "env",
            "ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING=0",
            "ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off",
            str(tuning_wrapper),
        ]
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--antfly-bin", type=pathlib.Path, default=repo / "zig/pkg/inference/zig-out/bin/antfly-inference")
    parser.add_argument("--model", type=pathlib.Path, default=repo / ".models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf")
    parser.add_argument("--models-dir", type=pathlib.Path, default=repo / ".models")
    parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("/tmp/antfly-gemma4-cuda-batching"))
    parser.add_argument("--prompt", default="Write one sentence about ants.")
    parser.add_argument("--tokens", type=int, default=256)
    cache_group = parser.add_mutually_exclusive_group()
    cache_group.add_argument("--cache-dtypes", nargs="+", default=None)
    cache_group.add_argument("--cache-dtype", default=None, help="Run one cache dtype (compatibility alias)")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--stagger-ms", type=float, default=25.0)
    parser.add_argument("--decode-wait-us", type=int, default=1000)
    parser.add_argument("--max-step-items", type=int, default=2)
    parser.add_argument("--min-c2-speedup", type=float, default=1.5)
    parser.add_argument("--max-c1-p95-ratio", type=float, default=1.05)
    parser.add_argument("--startup-timeout", type=float, default=600.0)
    parser.add_argument(
        "--server-prefix",
        default=os.environ.get("ANTFLY_BATCH_SERVER_PREFIX", default_server_prefix),
    )
    args = parser.parse_args()
    args.cache_dtypes = args.cache_dtypes or ([args.cache_dtype] if args.cache_dtype else ["f32", "polar4"])
    return args


def validate_args(args: argparse.Namespace) -> None:
    if not args.antfly_bin.exists() or not args.model.exists():
        raise ValueError("antfly binary or model does not exist")
    if args.tokens < 32 or args.warmups < 0 or args.repeats < 1:
        raise ValueError("tokens must be at least 32, repeats positive, and warmups non-negative")
    if args.decode_wait_us < 0 or args.max_step_items < 2:
        raise ValueError("decode wait must be non-negative and max-step-items must be at least 2")
    if args.min_c2_speedup <= 0 or args.max_c1_p95_ratio <= 0 or args.startup_timeout <= 0 or args.stagger_ms <= 0:
        raise ValueError("acceptance thresholds and startup timeout must be positive")
    if any(value < 1 for value in args.concurrency):
        raise ValueError("concurrency values must be positive")
    if len(set(args.concurrency)) != len(args.concurrency):
        raise ValueError("concurrency values must be unique")
    if 1 not in args.concurrency or 2 not in args.concurrency:
        raise ValueError("--concurrency must include 1 and 2 for acceptance gates")
    if len(set(args.cache_dtypes)) != len(args.cache_dtypes):
        raise ValueError("cache dtype values must be unique")
    invalid_dtypes = [dtype for dtype in args.cache_dtypes if dtype not in VALID_CACHE_DTYPES]
    if invalid_dtypes:
        raise ValueError(f"unsupported cache dtype(s): {', '.join(invalid_dtypes)}")


def benchmark_dtype(args: argparse.Namespace, cache_dtype: str) -> dict:
    output_dir = args.output_dir / cache_dtype
    output_dir.mkdir(parents=True, exist_ok=True)
    base_body = {
        "model": str(args.model.resolve()),
        "backend": "cuda",
        "temperature": 0,
        "stream": False,
        "cache_dtype": cache_dtype,
    }

    def request_case(case_id: str, prompt: str, max_tokens: int) -> RequestCase:
        return (
            case_id,
            {
                **base_body,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            },
        )

    prompt_a = f"{args.prompt}\nBegin the answer with A:"
    prompt_b = f"{args.prompt}\nBegin the answer with B:"
    primary_cases = [
        request_case("primary_a", prompt_a, args.tokens),
        request_case("primary_b", prompt_b, args.tokens),
    ]
    short_tokens = args.tokens - max(1, min(16, args.tokens // 4))
    isolation_cases = [
        request_case("staggered_long_a", prompt_a, args.tokens),
        request_case("staggered_short_b", prompt_b, short_tokens),
    ]

    with Server(args, "off", output_dir) as server:
        baseline = measure_mode(server, primary_cases, [1], args.warmups, args.repeats)
        isolation_baseline = measure_mode(server, isolation_cases, [1], 0, max(2, args.repeats))
    with Server(args, "on", output_dir) as server:
        batched = measure_mode(server, primary_cases, args.concurrency, args.warmups, args.repeats)
        _, isolation_before = server.scheduler_metrics()
        isolation_waves = [
            run_wave(
                server,
                isolation_cases,
                2,
                offset=repeat,
                stagger_ms=args.stagger_ms,
            )
            for repeat in range(max(2, args.repeats))
        ]
        _, isolation_after = server.scheduler_metrics()

    isolation_probe = evaluate_isolation_probe(
        isolation_baseline["measurements"]["1"],
        isolation_waves,
        counter_delta(isolation_before, isolation_after),
        isolation_cases,
        args.stagger_ms,
    )

    acceptance = evaluate_acceptance(
        baseline,
        batched,
        args.min_c2_speedup,
        args.max_c1_p95_ratio,
        isolation_probe,
    )
    summary = {
        "config": {
            "model": str(args.model.resolve()),
            "tokens": args.tokens,
            "cache_dtype": cache_dtype,
            "prompts": [prompt_a, prompt_b],
            "concurrency": args.concurrency,
            "decode_wait_us": args.decode_wait_us,
            "max_step_items": args.max_step_items,
            "stagger_ms": args.stagger_ms,
        },
        "baseline": baseline,
        "batched": batched,
        "acceptance": acceptance,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main() -> None:
    args = parse_args()
    try:
        validate_args(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = {dtype: benchmark_dtype(args, dtype) for dtype in args.cache_dtypes}
    passed = all(result["acceptance"]["passed"] for result in results.values())
    matrix_summary = {
        "config": {
            "model": str(args.model.resolve()),
            "tokens": args.tokens,
            "cache_dtypes": args.cache_dtypes,
            "concurrency": args.concurrency,
            "decode_wait_us": args.decode_wait_us,
            "max_step_items": args.max_step_items,
            "stagger_ms": args.stagger_ms,
            "warmups": args.warmups,
            "repeats": args.repeats,
            "server_prefix": args.server_prefix,
        },
        "results": results,
        "passed": passed,
    }
    output = args.output_dir / "summary.json"
    output.write_text(json.dumps(matrix_summary, indent=2, sort_keys=True) + "\n")
    for dtype, result in results.items():
        acceptance = result["acceptance"]
        print(
            f"batching_gate cache_dtype={dtype} "
            f"c2_speedup={acceptance['c2_aggregate_speedup']:.3f} "
            f"c1_p95_ratio={acceptance['c1_p95_latency_ratio']:.3f} "
            f"row2_steps={acceptance['c2_step_batch_size_2_total']} "
            f"exact={str(acceptance['c1_exact_response_fingerprints'] and acceptance['c2_exact_response_fingerprints']).lower()}"
        )
    print(f"batching_matrix passed={str(passed).lower()} output={output}")
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
