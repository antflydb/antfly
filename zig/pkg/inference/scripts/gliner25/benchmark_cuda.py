#!/usr/bin/env python3
"""GLiNER2.5 CUDA comparison using the frozen CPU oracle helpers.

The CPU driver is itself hash-pinned by the trained-export contracts. Keep it
byte-for-byte unchanged; only device orchestration and candidate policy live
here. Model/schema/output semantics and paired statistics remain shared.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import platform
import sys
from typing import Any

import benchmark_cpu as cpu
import oracle
from metal_benchmark_supervisor import MAX_PROCESSES, ResourceGuard, Worker

HERE = Path(__file__).resolve().parent
CUDA_SCOPE = "gliner25_direct_core_cuda_comparison_v1"
TIMING_BOUNDARY = cpu.TIMING_BOUNDARY
BenchmarkError = cpu.BenchmarkError
PROFILES = ("eager_fp32", "compile_fp32", "eager_amp_bf16", "compile_amp_bf16", "eager_amp_fp16", "compile_amp_fp16")


def require_equal(expected: Any, actual: Any, path: str = "output", *, confidence_tolerance: float = 5e-4) -> None:
    if confidence_tolerance not in (5e-4, 5e-3):
        raise BenchmarkError("unknown CUDA confidence policy")
    if confidence_tolerance == 5e-3:
        def normalize(want, got, key=""):
            # Only snap already-acceptable confidence leaves. The frozen
            # comparator still checks every key, list order, decision, type,
            # offset and any out-of-tolerance/nonfinite confidence. Originals
            # remain intact in the evidence, including rejected candidates.
            if isinstance(want, dict) and isinstance(got, dict):
                return {k: normalize(want[k], v, k) if k in want else v for k, v in got.items()}
            if isinstance(want, list) and isinstance(got, list):
                return [normalize(want[i], v) if i < len(want) else v for i, v in enumerate(got)]
            if (key == "confidence" and type(got) in (int, float)
                    and math.isfinite(got) and abs(want - got) <= confidence_tolerance):
                return want
            return got
        actual = normalize(expected, actual)
    cpu.require_equal(expected, actual, path)


def checked_ready(arm, ready, bundle, cases_path, threads, *, profile="eager_fp32", flashdeberta=False, compile_static=False, batch_size=1):
    if ready.get("batch_size", 1) != batch_size:
        raise BenchmarkError("CUDA worker batch size differs")
    expected_arm = "fastino_cuda" if arm == "python" else "native"
    if ready.get("scope") != CUDA_SCOPE or ready.get("arm") != expected_arm:
        raise BenchmarkError("CUDA worker scope or arm differs")
    # Reuse all frozen artifact/build/thread checks after independently
    # validating the only two fields whose CUDA wire names differ.
    cpu.checked_ready(arm, {**ready, "scope": cpu.SCOPE, "arm": arm}, bundle, cases_path, threads)
    if arm == "native":
        if (ready.get("backend") != "cuda" or ready.get("synchronization_policy") != "cuda_stream_before_start_and_after_extract_v1"):
            raise BenchmarkError("native CUDA device or synchronization policy differs")
    elif (ready.get("device") != "cuda" or ready.get("parameter_device") != "cuda"
          or ready.get("profile") != profile
          or ready.get("compile_static") is not compile_static
          or ready.get("cuda_runtime", {}).get("triton_f32_default") != "ieee"
          or ready.get("encoder_backend") != ("flashdeberta" if flashdeberta else "transformers")
          or ready.get("math_policy") != (f"pytorch_cuda_amp_{profile.rsplit('_', 1)[-1]}_no_tf32_v1" if "_amp_" in profile else "pytorch_cuda_fp32_deterministic_no_tf32_v1")
          or ready.get("synchronization_policy") != "torch_cuda_synchronize_before_start_and_after_extract_v1"):
        raise BenchmarkError("Python CUDA device, encoder or math policy differs")


def checked_transfers(response):
    counts = response.get("cuda_transfers")
    fields = {"h2d_bytes", "d2h_bytes", "kernel_launches", "host_fallback_calls"}
    if (not isinstance(counts, dict) or set(counts) != fields
            or any(type(value) is not int or value < 0 for value in counts.values())
            or counts["kernel_launches"] == 0 or counts["host_fallback_calls"] != 0):
        raise BenchmarkError("native CUDA work/transfer evidence is missing or invalid")
    return counts


def checked_output(response, batch_size=1):
    if response.get("event") == "error":
        raise BenchmarkError(f"{response.get('arm')}: {response.get('error_type')}: {response.get('message')}")
    if "output" not in response:
        raise BenchmarkError("CUDA worker response is missing extraction output")
    if response.get("batch_size", 1) != batch_size:
        raise BenchmarkError("CUDA response batch size differs")
    if batch_size > 1:
        outputs = response.get("outputs")
        if not isinstance(outputs, list) or len(outputs) != batch_size:
            raise BenchmarkError("CUDA batch output count differs")
        return [cpu.canonical_result(item) for item in outputs]
    return cpu.canonical_result(response["output"])


def checked_batch_tokens(responses, batch_size):
    left, right = responses["python"]["input_ids"], responses["native"]["input_ids"]
    if (not isinstance(left, list) or not left or len(left) % batch_size
            or len(left) > batch_size * oracle.MAX_ENCODED_TOKENS
            or any(type(token) is not int for token in left) or left != right):
        raise BenchmarkError("encoder token identity differs")
    shape = [batch_size, len(left) // batch_size]
    for response in responses.values():
        if response.get("encoder_shape") != shape:
            raise BenchmarkError("actual encoder batch shape differs")
    return left, shape


def run_variant(args, variant, directory):
    directory.mkdir()
    model_dir = args.model_root / variant
    bundle = oracle.verify_model_dir(variant, model_dir)
    case_path = cpu.case_fixture(variant)
    fixture = oracle.read_json(case_path)
    regenerated = cpu.adaptation.generate(variant, oracle.FIXTURES / f"{variant}_reference", oracle.FIXTURES / "requests.json")
    if json.dumps(fixture, ensure_ascii=False) != json.dumps(regenerated, ensure_ascii=False):
        raise BenchmarkError("canonical fixture differs from verified oracle adaptation")
    cases = {case["id"]: case for case in fixture["cases"]}
    request_kinds = {item["id"]: item["kind"] for item in oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]}
    selected = args.cases or [name for name in cases if args.batch_size == 1 or request_kinds[name] == "extract"]
    if args.batch_size > 1 and any(request_kinds.get(name) != "extract" for name in selected):
        raise BenchmarkError("batch benchmark supports captured extract requests only")
    if len(selected) > 32 or len(set(selected)) != len(selected) or any(case not in cases for case in selected):
        raise BenchmarkError("case selection must be unique captured requests")
    command_count = len(selected) * (1 + args.warmup + args.pairs) + 1
    if command_count > 4096:
        raise BenchmarkError("requested benchmark exceeds worker command budget")
    expected = {name: cpu.canonical_result(cases[name]["expected"]) for name in selected}
    if args.batch_size > 1:
        expected = {name: [value] * args.batch_size for name, value in expected.items()}
    env = dict(os.environ)
    env.update({name: "1" for name in cpu.THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", TOKENIZERS_PARALLELISM="false", HF_HUB_OFFLINE="1",
               TRANSFORMERS_OFFLINE="1", TORCHINDUCTOR_COMPILE_THREADS="1")
    env.pop("USE_FLASHDEBERTA", None)
    commands = {
        "native": [str(args.native_bin), "--model-dir", str(model_dir), "--cases", str(case_path), "--threads", "1", "--batch-size", str(args.batch_size), "--timeout-ms", str(args.timeout_ms), "--max-commands", str(command_count)],
        "python": [sys.executable, str(HERE / "metal_python_worker.py"), "--device", "cuda", "--model", variant,
                   "--model-dir", str(model_dir), "--upstream", str(args.upstream), "--max-commands", str(command_count),
                   "--profile", args.python_profile, "--batch-size", str(args.batch_size)] + (["--flashdeberta"] if args.flashdeberta else []) + (["--compile-static"] if args.compile_static else []),
    }
    # A compiler can start hundreds of short-lived toolchain processes across
    # shapes. Bound the lifetime identity inventory independently of live RSS.
    guard = ResourceGuard(args.max_rss_mib * 1024 * 1024,
                          max_processes=1024 if args.python_profile.startswith("compile_") else MAX_PROCESSES)
    workers, readies, cleanup = {}, {}, {}
    def tolerance(arm):
        return 5e-3 if arm == "python" and "_amp_" in args.python_profile else 5e-4
    def check_result(name, arm, response):
        actual = checked_output(response, args.batch_size)
        if arm == "native":
            checked_transfers(response)
        require_equal(expected[name], actual, f"{variant}.{name}.{arm}", confidence_tolerance=tolerance(arm))
    def timeout(arm, warming=False):
        return args.compile_timeout if warming and arm == "python" and (args.python_profile.startswith("compile_") or args.flashdeberta) else args.timeout_ms / 1000 + 5
    try:
        for arm in ("python", "native"):
            workers[arm] = Worker("fastino_cuda" if arm == "python" else arm, commands[arm], env, directory, guard)
            ready = workers[arm].receive(args.startup_timeout)
            checked_ready(arm, ready, bundle, case_path, 1, profile=args.python_profile, flashdeberta=args.flashdeberta, compile_static=args.compile_static, batch_size=args.batch_size)
            readies[arm] = ready
            oracle.write_json(directory / "readiness.json", readies)
        validation = {}
        for name in selected:
            responses = {arm: workers[arm].request("validate", name, timeout(arm, True)) for arm in ("python", "native")}
            oracle.write_json(directory / f"validation-{name}.json", {"expected": expected[name], "responses": responses})
            for arm, response in responses.items():
                check_result(name, arm, response)
            left, shape = checked_batch_tokens(responses, args.batch_size)
            validation[name] = {"input_ids": left, "encoder_shape": shape, "outputs_match_oracle": True,
                                "confidence_absolute_tolerance": {arm: tolerance(arm) for arm in workers},
                                "expected": expected[name], **{arm: checked_output(response, args.batch_size) for arm, response in responses.items()}}
        for iteration in range(0 if args.validate_only else args.warmup):
            for name in selected:
                for arm in cpu.paired_benchmark.balanced_pair_order(iteration + 1, "native", "python"):
                    check_result(name, arm, workers[arm].request("run", name, timeout(arm, True)))
        rows = []
        for pair in range(1, 1 if args.validate_only else args.pairs + 1):
            for name in selected:
                row = {"pair": pair, "case_id": name, "order": cpu.paired_benchmark.balanced_pair_order(pair, "native", "python")}
                for arm in row["order"]:
                    response = workers[arm].request("run", name, timeout(arm))
                    check_result(name, arm, response)
                    row[arm] = {"duration_ns": response["duration_ns"], **({"cuda_transfers": response.get("cuda_transfers")} if arm == "native" else {})}
                rows.append(row)
        for worker in workers.values():
            worker.request("stop", timeout=args.startup_timeout)
            if worker.process.wait(timeout=5) != 0:
                raise BenchmarkError(f"{worker.arm}: nonzero worker exit after stop")
        if oracle.verify_model_dir(variant, model_dir) != bundle:
            raise BenchmarkError("bundle changed after measured calls")
        oracle.verify_upstream_checkout(args.upstream)
        comparisons = {}
        for name in (() if args.validate_only else selected):
            pairs = [(row["native"]["duration_ns"], row["python"]["duration_ns"]) for row in rows if row["case_id"] == name]
            comparisons[name] = {"batch_size": args.batch_size,
                                 "native_documents_per_second": cpu.paired_benchmark.distribution(args.batch_size * 1e9 / n for n, _ in pairs),
                                 "python_documents_per_second": cpu.paired_benchmark.distribution(args.batch_size * 1e9 / p for _, p in pairs),
                                 "native_ns": cpu.paired_benchmark.distribution(n for n, _ in pairs),
                                 "python_ns": cpu.paired_benchmark.distribution(p for _, p in pairs),
                                 "native_over_python_latency": cpu.paired_benchmark.paired_log_ratio_ci(pairs, samples=2000)}
        return {"model": variant, "model_bundle": bundle, "workers": readies, "validation": validation, "pairs": rows,
                "comparisons": comparisons, "combined_peak_observed_rss_bytes": guard.peak_rss_bytes,
                "rss_sampling_period_seconds": 0.1, "max_combined_rss_bytes": guard.max_rss_bytes,
                "max_tracked_processes_per_worker": guard.max_processes,
                "startup_commands": commands, "cleanup": cleanup}
    finally:
        for arm, worker in workers.items():
            worker.close()
            cleanup[arm] = worker.cleanup
        oracle.write_json(directory / "cleanup.json", cleanup)
        if any(not receipt or not receipt.get("complete") for receipt in cleanup.values()):
            raise BenchmarkError("CUDA benchmark worker tree cleanup incomplete")


def driver(args):
    if args.compile_static and not args.python_profile.startswith("compile_"):
        raise BenchmarkError("static shapes require a compiled profile")
    if (not 1 <= args.batch_size <= 64 or args.threads != 1 or not 1 <= args.compile_timeout <= 1800 or not 0 <= args.warmup <= 8
            or not 2 <= args.pairs <= 64 or not 1 <= args.timeout_ms <= 60000
            or not 1 <= args.startup_timeout <= 300 or not 256 <= args.max_rss_mib <= 12288):
        raise BenchmarkError("invalid CUDA resource or sampling limits")
    if args.output.exists() or not args.native_bin.is_file():
        raise BenchmarkError("requires a new output directory and an existing native binary")
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    oracle.verify_dependencies()
    provenance = oracle.verify_upstream_checkout(args.upstream)
    args.output.mkdir(parents=True)
    report = {"format_version": 1, "status": "running", "scope": CUDA_SCOPE, "backend": "cuda",
              "python_variant": args.python_profile, "flashdeberta": args.flashdeberta, "compile_static": args.compile_static, "timing_boundary": TIMING_BOUNDARY,
              "serving_qualified": False, "performance_release_qualified": False,
              "excluded": ["process_startup", "model_loading", "compilation", "HTTP", "admission", "model_resolution", "wire_JSON_serialization", "returned_result_destruction"],
              "validation_only": args.validate_only,
              "batch_size": args.batch_size, "batch_composition": "repeated_same_fixture_document_and_schema",
              "threads": 1, "warmup_per_case": 0 if args.validate_only else args.warmup,
              "pairs_per_case": 0 if args.validate_only else args.pairs,
              "native_binary": {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)},
              "driver_sha256": oracle.sha256_file(Path(__file__)),
              "helpers_sha256": {name: oracle.sha256_file(HERE / name) for name in ("benchmark_cpu.py", "metal_python_worker.py", "metal_benchmark_supervisor.py")},
              "source": provenance, "platform": {"system": platform.system(), "machine": platform.machine(), "processor": platform.processor()}, "models": []}
    try:
        for variant in (("small", "base", "multi") if args.model == "all" else (args.model,)):
            report["models"].append(run_variant(args, variant, args.output / variant))
        if oracle.sha256_file(args.native_bin) != report["native_binary"]["sha256"]:
            raise BenchmarkError("native executable changed during benchmark")
        if oracle.sha256_file(Path(__file__)) != report["driver_sha256"] or any(
                oracle.sha256_file(HERE / name) != digest for name, digest in report["helpers_sha256"].items()):
            raise BenchmarkError("benchmark driver or helpers changed during measurement")
        report.update(status="complete", parity_validated=True)
    except BaseException as error:
        report.update(status="failed", parity_validated=False, error=f"{type(error).__name__}: {error}")
        raise
    finally:
        oracle.write_json(args.output / "report.json", report)
        cpu.paired_benchmark.write_evidence_manifest(args.output)
    print(json.dumps({"status": report["status"], "scope": CUDA_SCOPE, "report": str(args.output / "report.json")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--backend", choices=("cuda",), default="cuda")
    run.add_argument("--python-profile", choices=PROFILES, default="eager_fp32")
    run.add_argument("--flashdeberta", action="store_true")
    run.add_argument("--compile-static", action="store_true", help="Compile fixed shapes instead of dynamic shapes; record as a separate candidate")
    run.add_argument("--validate-only", action="store_true", help="Check candidate parity and provenance without collecting timing samples")
    run.add_argument("--compile-timeout", type=float, default=600)
    run.add_argument("--native-bin", type=Path, required=True)
    run.add_argument("--model", choices=("small", "base", "multi", "all"), default="small")
    run.add_argument("--model-root", type=Path, required=True)
    run.add_argument("--upstream", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--cases", nargs="+")
    run.add_argument("--batch-size", type=int, default=1, help="True homogeneous GPU batch, 1..64; batch >1 supports extract fixtures")
    run.add_argument("--threads", type=int, default=1)
    run.add_argument("--warmup", type=int, default=5)
    run.add_argument("--pairs", type=int, default=30)
    run.add_argument("--timeout-ms", type=int, default=30000)
    run.add_argument("--startup-timeout", type=float, default=120)
    run.add_argument("--max-rss-mib", type=int, default=8192)
    args = parser.parse_args()
    for name in ("native_bin", "model_root", "upstream", "output"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    driver(args)


if __name__ == "__main__":
    main()
