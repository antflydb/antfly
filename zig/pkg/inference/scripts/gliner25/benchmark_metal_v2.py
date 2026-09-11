#!/usr/bin/env python3
"""Additive FP32 benchmark with explicit reference/optimized runtime policies.

Frozen v1 fixtures, reference functions and timing boundaries remain unchanged.
Diagnostic requests never enter latency acceptance statistics.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
from typing import Any
import benchmark_metal as v1
import metal_runtime_contract_v2 as runtime
from metal_benchmark_supervisor import BenchmarkError, ResourceGuard, Worker
cpu, oracle, paired_benchmark = v1.cpu, v1.oracle, v1.paired_benchmark
HERE, REPO, NATIVE, VARIANTS = v1.HERE, v1.REPO, v1.NATIVE, v1.VARIANTS
SCOPE, TIMING_BOUNDARY = runtime.SCOPE, v1.TIMING_BOUNDARY
BOOTSTRAP_SAMPLES, SEED = v1.BOOTSTRAP_SAMPLES, v1.SEED
write_json, utc_now, rotate = v1.write_json, v1.utc_now, v1.rotate
worker_environment, hardware_receipt = v1.worker_environment, v1.hardware_receipt
source_snapshot, load_contract, sample_receipt = v1.source_snapshot, v1.load_contract, v1.sample_receipt


def run_pair(args: argparse.Namespace, contract: dict[str, Any], baseline: str,
             directory: Path, *, repetition: int, phase: str, selected: list[str], native_backend: str = "metal") -> dict[str, Any]:
    if native_backend not in ("metal", "native") or (native_backend == "native" and baseline != "cpu"):
        raise BenchmarkError("invalid backend/reference pairing")
    cpu_mode = native_backend == "native"
    scaling = contract.get("workload") == "scaling"
    if scaling:
        import scaling_contract_v1
        import scaling_runtime_v1
        if cpu_mode or args.legacy_reference_bin:
            raise BenchmarkError("scaling requires the new Metal worker and separate source captures")
    active_scope = scaling_contract_v1.SCOPE if scaling else SCOPE
    directory.mkdir(parents=True)
    native_arm = "native" if cpu_mode else NATIVE
    variant, reference = contract["model"], "python" if cpu_mode else f"fastino_{baseline}"
    arms = (native_arm, reference)
    env, environment = worker_environment()
    guard = ResourceGuard(args.max_rss_mib * 1024**2)
    command_count = len(selected) * (1 + (args.warmup + args.pairs if phase == "measurement" else 1 if phase == "diagnostic" else 0)) + 1
    model_dir = args.model_root / variant
    commands = {
        native_arm: [str(args.native_bin), "--model-dir", str(model_dir), "--cases", str(contract["case_path"]),
                 "--threads", "1", "--timeout-ms", str(args.timeout_ms), "--max-commands", str(command_count)],
        reference: [sys.executable, str(HERE / "metal_python_worker_v2.py"), "--device", baseline,
                    "--model", variant, "--model-dir", str(model_dir), "--upstream", str(args.upstream),
                    "--max-commands", str(command_count)],
    }
    if cpu_mode:
        commands[reference] = [sys.executable, str(HERE / "benchmark_cpu.py"), "worker", "--model", variant,
            "--model-dir", str(model_dir), "--upstream", str(args.upstream), "--threads", "1",
            "--max-commands", str(command_count)]
    if not cpu_mode and not args.legacy_reference_bin:
        commands[native_arm].extend(["--execution-policy", args.execution_policy])
        if getattr(args, "purpose", None) == "diagnostic" or phase == "diagnostic":
            commands[native_arm].extend(["--diagnostics", "true"])
    if scaling:
        commands[native_arm].extend(["--workload", "scaling"])
        commands[reference][1] = str(HERE / "metal_scaling_python_worker.py")
        commands[reference].extend(["--prepared", str(contract["prepared"])])
    run = {"native_backend": native_backend, "execution_policy": "native_cpu_reference_v1" if cpu_mode else args.execution_policy, "model": variant, "baseline": baseline, "phase": phase, "repetition": repetition,
           "status": "running", "started_at": utc_now(), "workers": {}, "commands": commands,
           "hardware_start": hardware_receipt(),
           "environment": environment, "validation": {}, "pairs": [], "comparisons": {},
           "case_status": {name: {"status": "pending"} for name in selected},
           "failures": [], "directory": str(directory)}
    workers: dict[str, Worker] = {}
    native_receipt = None
    journal = (directory / "events.jsonl").open("w", encoding="utf-8")

    def record(value: dict[str, Any]) -> None:
        line = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
        if journal.tell() + len(line.encode()) > 64 * 1024**2:
            raise BenchmarkError("request evidence exceeded 64 MiB")
        journal.write(line)
        journal.flush()

    def call(arm: str, operation: str, name: str, stage: str, iteration: int = 0) -> dict[str, Any]:
        response = workers[arm].request(operation, name, args.timeout_ms / 1000 + 5)
        record({"stage": stage, "iteration": iteration, "arm": arm, "response": response})
        if response.get("event") == "error" and (response.get("recoverable") is not True or response.get("device_unsafe")):
            raise BenchmarkError(f"{arm}: unsafe worker error: {response}")
        return response

    def checked(arm, response, name, *, validation, stage):
        if scaling:
            return scaling_runtime_v1.result(arm, response, contract["source_cases"][name],
                contract["expected"][name], validation=validation, phase=stage,
                native_receipt=native_receipt)
        if cpu_mode:
            if response.get("event") == "error":
                raise cpu.BenchmarkError(f"CPU worker error: {response.get('message', '')}")
            if runtime.integer(response.get("duration_ns"), "duration_ns") == 0:
                raise BenchmarkError("CPU request has no completed duration")
            actual = cpu.canonical_result(response["output"])
            cpu.require_equal(contract["expected"][name], actual, arm)
            if validation:
                ids = response.get("input_ids")
                if (not isinstance(ids, list) or not 1 <= len(ids) <= oracle.MAX_ENCODED_TOKENS
                        or any(type(token) is not int or token < 0 for token in ids)):
                    raise cpu.BenchmarkError("invalid CPU encoder tokens")
            return actual
        if arm == native_arm:
            if native_receipt is None:
                raise BenchmarkError("result preceded readiness")
            return native_receipt.result(response, contract["expected"][name], validation=validation, phase=stage)
        if response.get("scope") != active_scope:
            raise BenchmarkError("Python result changed comparison scope")
        return v1.checked_result(response, contract["expected"][name], arm, validation=validation)

    def block(name: str, error: Exception, stage: str) -> None:
        failure = {"case_id": name, "stage": stage, "error": f"{type(error).__name__}: {error}"}
        run["failures"].append(failure)
        run["case_status"][name] = {"status": "blocked", **failure}

    try:
        for arm in paired_benchmark.balanced_pair_order(repetition + 1, reference, native_arm):
            workers[arm] = Worker(arm, commands[arm], env, directory, guard)
            ready = workers[arm].receive(args.startup_timeout)
            record({"stage": "startup", "arm": arm, "response": ready})
            if scaling:
                receipt = scaling_runtime_v1.ready(arm, ready, contract, args.execution_policy)
                if arm == native_arm:
                    native_receipt = receipt
            elif cpu_mode:
                cpu.checked_ready(arm, ready, contract["bundle"], contract["case_path"], 1)
                if type(ready.get("threads")) is not int or ready.get("qualification") is not False:
                    raise BenchmarkError("CPU ready counter/qualification types differ")
            elif arm == native_arm:
                native_receipt = runtime.native_ready(ready, contract["bundle"], variant, contract["case_path"], args.execution_policy)
            else:
                runtime.reference_ready(arm, ready, contract["bundle"], variant, contract["case_path"], args.execution_policy)
            run["workers"][arm] = ready
        for name in selected:
            responses = {arm: call(arm, "validate", name, "validation") for arm in arms}
            try:
                normalized = {arm: checked(arm, responses[arm], name, validation=True, stage="validation")
                              for arm in arms}
                if responses[native_arm]["input_ids"] != responses[reference]["input_ids"]:
                    raise cpu.BenchmarkError("encoder token identity differs between implementations")
                captured_ids = contract.get("reference_input_ids", {}).get(name)
                if scaling:
                    for response in responses.values():
                        scaling_contract_v1.check_packet(response, contract["source_cases"][name]["expected_input_ids"])
                    captured_ids = captured_ids["input_ids"]
                if captured_ids is not None and responses[native_arm]["input_ids"] != captured_ids:
                    raise cpu.BenchmarkError("encoder tokens differ from the frozen oracle capture")
                run["validation"][name] = {
                    "input_ids": responses[native_arm]["input_ids"], "outputs": normalized,
                    "expected": contract["expected"][name], "confidence_absolute_tolerance": 5e-4,
                    "outputs_match_oracle": True,
                    "tokens_match_frozen_capture": True if captured_ids is not None else None,
                }
                if scaling:
                    run["validation"][name].update(attention_mask=responses[native_arm]["attention_mask"],
                                                   input_shape=responses[native_arm]["input_shape"])
                run["case_status"][name] = {"status": "validated"}
            except (cpu.BenchmarkError, KeyError, TypeError) as error:
                block(name, error, "validation")
        eligible = [name for name in selected if run["case_status"][name]["status"] == "validated"]
        if phase in ("measurement", "diagnostic"):
            for iteration in range(args.warmup if phase == "measurement" else 0):
                for name in rotate(eligible.copy(), repetition + iteration):
                    try:
                        for arm in paired_benchmark.balanced_pair_order(iteration + repetition + 1, *arms):
                            response = call(arm, "run", name, "warmup", iteration + 1)
                            checked(arm, response, name, validation=False, stage="warmup")
                    except (cpu.BenchmarkError, KeyError, TypeError) as error:
                        block(name, error, "warmup")
                        eligible.remove(name)
            if phase == "measurement" and not cpu_mode:
                native_receipt.seal_workspace()
            for pair in range(1, (args.pairs if phase == "measurement" else 1) + 1):
                for name in rotate(eligible.copy(), repetition + pair - 1):
                    row = {"pair": pair, "case_id": name,
                           "order": paired_benchmark.balanced_pair_order(pair + repetition, *arms),
                           "valid": False}
                    run["pairs"].append(row)
                    try:
                        for arm in row["order"]:
                            response = call(arm, "run", name, phase, pair)
                            row[arm] = (scaling_runtime_v1.sample_receipt(response) if scaling else sample_receipt(response))
                            checked(arm, response, name, validation=False, stage=phase)
                        row["valid"] = True
                    except (cpu.BenchmarkError, KeyError, TypeError) as error:
                        block(name, error, "measurement")
                        eligible.remove(name)
        for arm, worker in workers.items():
            response = worker.request("stop", timeout=args.startup_timeout)
            record({"stage": "shutdown", "arm": arm, "response": response})
            if cpu_mode:
                pass  # Process cleanup is verified below; legacy CPU has no device owner receipt.
            elif arm == native_arm:
                run["final_owned_cleanup"] = native_receipt.stopped(response)
            elif response.get("scope") != active_scope:
                raise BenchmarkError("Python stop changed comparison scope")
            if worker.process.wait(timeout=5) != 0:
                raise BenchmarkError(f"{arm}: worker did not exit successfully after stop")
        if oracle.verify_model_dir(variant, model_dir) != contract["bundle"]:
            raise BenchmarkError("model identity changed during comparison")
        oracle.verify_upstream_checkout(args.upstream)
        if scaling and scaling_contract_v1.load(contract["prepared"], variant)["pins"] != contract["pins"]:
            raise BenchmarkError("scaling inputs changed during comparison")
        if phase == "measurement":
            for name in eligible:
                rows = [row for row in run["pairs"] if row["case_id"] == name and row["valid"]]
                if len(rows) != args.pairs:
                    raise BenchmarkError("incomplete pair count admitted for statistics")
                pairs = [(row[reference]["duration_ns"], row[native_arm]["duration_ns"]) for row in rows]
                run["comparisons"][name] = {
                    "sample_pairs": len(pairs),
                    "native_ns" if cpu_mode else "metal_ns": paired_benchmark.distribution(native for _, native in pairs),
                    "python_ns": paired_benchmark.distribution(python for python, _ in pairs),
                }
                if scaling:
                    run["comparisons"][name].update(
                        descriptive_python_over_metal_ratio=statistics.median(python / native for python, native in pairs),
                        confidence_interval=None, latency_acceptance_claim=False)
                else:
                    run["comparisons"][name]["native_over_python_latency" if cpu_mode else "python_over_metal_speedup"] = paired_benchmark.paired_log_ratio_ci(
                        [(native, python) for python, native in pairs] if cpu_mode else pairs,
                        samples=2000 if cpu_mode else BOOTSTRAP_SAMPLES, seed=SEED)
                run["case_status"][name] = {"status": "complete"}
        run["status"] = "partial" if run["failures"] else "complete"
    except (KeyboardInterrupt, SystemExit):
        run["status"] = "interrupted"
        raise
    except Exception as error:
        run["status"] = "failed"
        run["error"] = f"{type(error).__name__}: {error}"
        for name in selected:
            if run["case_status"][name]["status"] != "blocked":
                block(name, error, "worker_lifecycle")
        run["comparisons"] = {}
    finally:
        cleanup_errors = []
        for arm, worker in workers.items():
            try:
                worker.close()
            except Exception as error:
                cleanup_errors.append(f"{arm}: {type(error).__name__}: {error}")
        journal.close()
        run["resource_guard"] = guard.receipt()
        run["cleanup"] = {arm: getattr(worker, "cleanup", None) for arm, worker in workers.items()}
        cleanup_receipts = list(run["cleanup"].values()) + [
            entry.get("cleanup") for entry in run["resource_guard"].get("completed_workers", [])]
        if any(isinstance(receipt, dict) and receipt.get("complete") is not True
               for receipt in cleanup_receipts):
            cleanup_errors.append("an owned worker has an incomplete cleanup receipt")
        run["finished_at"] = utc_now()
        run["hardware_end"] = hardware_receipt()
        run["power_profile_stable"] = all(
            run["hardware_start"].get(key) == run["hardware_end"].get(key)
            for key in ("power_source", "low_power_mode"))
        if phase == "measurement" and not run["power_profile_stable"]:
            run["status"] = "failed"
            run["error"] = "power source or low-power mode changed within the repetition"
            run["comparisons"] = {}
            for name in selected:
                block(name, BenchmarkError(run["error"]), "power_profile")
        if cleanup_errors:
            run["status"] = "failed"
            run["cleanup_errors"] = cleanup_errors
            run["comparisons"] = {}
        write_json(directory / "run.json", run)
    return run


def aggregate(report: dict[str, Any]) -> list[dict[str, Any]]:
    count = report["repetitions"]
    if type(count) is not int or not 1 <= count <= 5:
        raise BenchmarkError("invalid saved repetition count")
    result = []
    for contract in report["contracts"]:
        for name in contract["cases"]:
            for baseline in report["baselines"]:
                runs = [run for run in report["runs"]
                        if run["phase"] == "measurement" and run["model"] == contract["model"]
                        and run["baseline"] == baseline]
                repetition_ids = [run.get("repetition") for run in runs]
                exact_repetitions = (all(type(value) is int for value in repetition_ids)
                                     and sorted(repetition_ids) == list(range(count)))
                observations = [
                    {**run["comparisons"][name], "repetition": run.get("repetition"),
                     "power_profile": {key: run.get("hardware_start", {}).get(key)
                                       for key in ("power_source", "low_power_mode")}}
                    for run in runs if name in run["comparisons"]
                ]
                row = {"model": contract["model"], "case_id": name, "baseline": baseline,
                       "status": "blocked", "repetitions": observations}
                same_power_profile = not observations or all(
                    obs["power_profile"] == observations[0]["power_profile"] for obs in observations)
                if not same_power_profile:
                    row["error"] = "power profile differs between repetitions"
                if not exact_repetitions:
                    row["error"] = "measurement repetitions must have exact unique IDs from zero through count minus one"
                if exact_repetitions and len(observations) == count and same_power_profile:
                    cis = [obs["python_over_metal_speedup"] for obs in observations]
                    row.update(
                        status="complete",
                        metal_median_ms=statistics.median(obs["metal_ns"]["median"] for obs in observations) / 1e6,
                        python_median_ms=statistics.median(obs["python_ns"]["median"] for obs in observations) / 1e6,
                        median_speedup=statistics.median(ci["median"] for ci in cis),
                        repeatability=("insufficient_repetitions" if len(cis) < 3 else
                                       "metal_faster" if all(ci["lower_95"] > 1 for ci in cis) else
                                       "python_faster" if all(ci["upper_95"] < 1 for ci in cis) else
                                       "inconclusive"),
                    )
                row["execution_policy"] = report["execution_policy"]
                if baseline == "mps":
                    row["milestone"] = runtime.competitiveness(
                        [obs["python_over_metal_speedup"] for obs in observations] if row["status"] == "complete" else [],
                        numerator="python_over_metal")
                result.append(row)
    return result


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# GLiNER2.5 Metal comparison v2", "",
        f"Execution policy: **{report['execution_policy']}**. Purpose: **{report['purpose']}**.",
        f"Status: **{report['status']}**. Batch 1, FP32, one CPU math thread per arm.",
        "Speedup is Python latency / Metal latency; values above 1 favor Metal.",
        "Latencies are medians of the fresh-process repetition medians. Intervals describe each repetition separately.",
        "Sample p95 is descriptive; this report does not qualify serving latency or release readiness.", "",
    ]
    if report.get("error"):
        lines.extend([f"Campaign error: {report['error']}", ""])
    for baseline in report["baselines"]:
        lines.extend([f"## Fastino {baseline.upper()} reference", "",
                      "| Model | Request | Metal ms | Python ms | Speedup | 95% intervals by repetition | Result |",
                      "| --- | --- | ---: | ---: | ---: | --- | --- |"])
        for row in report["summary"]:
            if row["baseline"] != baseline:
                continue
            if row["status"] != "complete":
                lines.append(f"| {row['model']} | {row['case_id']} | — | — | — | — | blocked |")
                continue
            intervals = "; ".join(
                f"{obs['repetition'] + 1}: [{obs['python_over_metal_speedup']['lower_95']:.3f}, "
                f"{obs['python_over_metal_speedup']['upper_95']:.3f}]" for obs in row["repetitions"])
            lines.append(
                f"| {row['model']} | {row['case_id']} | {row['metal_median_ms']:.3f} | "
                f"{row['python_median_ms']:.3f} | {row['median_speedup']:.3f}× | {intervals} | {row['repeatability']} |")
        lines.append("")
    failures = [run for run in report["runs"] if run["status"] != "complete"]
    if failures:
        lines.extend(["## Preserved failures", ""])
        for run in failures:
            description = run.get("error") or "; ".join(
                f"{entry['case_id']}: {entry['error']}" for entry in run["failures"])
            lines.append(f"- {run['phase']} / {run['model']} / {run['baseline']} / "
                         f"repetition {run['repetition'] + 1}: {description}")
        lines.append("")
    lines.extend(["Full outputs, timings, identities, memory observations and diagnostics accompany report.json.",
                  "Native Metal uses production default math, including eligible Apple MPSMatrix operations. "
                  "PyTorch MPS uses its pinned deterministic FP32 profile. Host decoding and native request uploads are timed.",
                  "The MPS and CPU comparisons use separate pairs; their Metal measurements must not be mixed.", ""])
    return "\n".join(lines)


def driver(args: argparse.Namespace) -> int:
    if args.execution_policy not in runtime.POLICIES:
        raise BenchmarkError("explicit execution policy is required")
    if args.legacy_reference_bin and args.execution_policy != "reference_v1":
        raise BenchmarkError("legacy binaries require reference_v1")
    if args.purpose not in ("benchmark", "diagnostic"):
        raise BenchmarkError("unknown evidence purpose")
    if (not 1 <= args.repetitions <= 5 or not 0 <= args.warmup <= 8
            or not 2 <= args.pairs <= 64 or args.pairs % 2
            or not 1 <= args.timeout_ms <= 60_000 or not 1 <= args.startup_timeout <= 300
            or not 256 <= args.max_rss_mib <= 8192):
        raise BenchmarkError("invalid resource or sampling limits; pair count must be even")
    if args.output.exists() or not args.native_bin.is_file():
        raise BenchmarkError("requires a new output directory and an existing Metal executable")
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    dependencies = oracle.verify_dependencies()
    upstream = oracle.verify_upstream_checkout(args.upstream)
    variants = list(VARIANTS) if args.model == "all" else [args.model]
    baselines = ["mps", "cpu"] if args.baseline == "both" else [args.baseline]
    contracts = [load_contract(variant, args.model_root, args.cases) for variant in variants]
    args.output.mkdir(parents=True)
    source = source_snapshot()
    write_json(args.output / "source_manifest.json", source)
    report = {
        "format_version": 2, "status": "running", "execution_policy": args.execution_policy,
        "purpose": args.purpose, "legacy_reference_binary": args.legacy_reference_bin, "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
        "started_at": utc_now(), "threads": 1, "batch_size": 1, "precision": "float32",
        "serving_qualified": False, "performance_release_qualified": False,
        "repetitions": args.repetitions, "warmup_per_case": args.warmup, "pairs_per_case": args.pairs,
        "bootstrap_samples": BOOTSTRAP_SAMPLES, "bootstrap_seed": SEED,
        "baselines": baselines, "hardware_start": hardware_receipt(),
        "upstream": upstream, "dependencies": dependencies,
        "source_head": source["head"], "source_files_sha256": source["source_files_sha256"],
        "native_binary": {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)},
        "excluded": ["process_startup", "model_loading", "HTTP", "model_resolution",
                     "wire_JSON_serialization", "returned_result_destruction", "validation_hooks", "result_comparison"],
        "contracts": [{key: value for key, value in contract.items() if key not in ("case_path", "expected")}
                      for contract in contracts],
        "runs": [], "summary": [],
    }
    write_json(args.output / "report.json", report)

    def execute(contract: dict[str, Any], baseline: str, *, repetition: int, phase: str,
                selected: list[str]) -> dict[str, Any]:
        name = f"{phase}-{repetition + 1}-{contract['model']}-{baseline}"
        print(json.dumps({"event": "comparison_start", "run": name, "cases": selected}), flush=True)
        run = run_pair(args, contract, baseline, args.output / name,
                       repetition=repetition, phase=phase, selected=selected)
        report["runs"].append(run)
        write_json(args.output / "report.json", report)
        print(json.dumps({"event": "comparison_end", "run": name, "status": run["status"],
                          "error": run.get("error"), "blocked_cases": [
                              key for key, value in run["case_status"].items() if value["status"] == "blocked"]}), flush=True)
        if run.get("cleanup_errors"):
            raise BenchmarkError("worker cleanup failed; no further model processes will be started")
        return run

    try:
        eligible = {}
        # Complete the entire requested preflight matrix before timing any row.
        for contract in contracts:
            for baseline in baselines:
                run = execute(contract, baseline, repetition=0, phase="preflight", selected=contract["cases"])
                eligible[(contract["model"], baseline)] = [
                    name for name, value in run["case_status"].items()
                    if value["status"] == "validated" and run["status"] in ("complete", "partial")]
        by_variant = {contract["model"]: contract for contract in contracts}
        for repetition in range(args.repetitions if args.purpose == "benchmark" else 1):
            for variant in rotate(variants, repetition):
                for baseline in baselines:
                    selected = eligible[(variant, baseline)]
                    if selected:
                        execute(by_variant[variant], baseline, repetition=repetition,
                                phase="measurement" if args.purpose == "benchmark" else "diagnostic", selected=selected)
        if oracle.sha256_file(args.native_bin) != report["native_binary"]["sha256"]:
            raise BenchmarkError("native executable changed during the campaign")
        if source_snapshot() != source:
            raise BenchmarkError("repository source identity changed during the campaign")
        oracle.verify_config_fixtures()
        oracle.verify_reference_fixtures()
        oracle.verify_upstream_checkout(args.upstream)
        if oracle.verify_dependencies() != dependencies:
            raise BenchmarkError("Python dependencies changed during the campaign")
        report["summary"] = aggregate(report) if args.purpose == "benchmark" else []
        finished = (all(row["status"] == "complete" for row in report["summary"]) if args.purpose == "benchmark"
                    else all(run["status"] == "complete" for run in report["runs"]))
        report["status"] = "complete" if finished else "partial"
        milestone_rows = [row for row in report["summary"] if row["baseline"] == "mps"]
        report["metal_mps_latency_gate_passed"] = (
            args.purpose == "benchmark" and args.execution_policy == "optimized_v2"
            and variants == list(VARIANTS) and len(milestone_rows) == 30
            and all(len(c["cases"]) == 10 for c in contracts)
            and args.repetitions == 3 and args.warmup == 5 and args.pairs == 30
            and all(row["milestone"]["passed"] for row in milestone_rows))
        report["cpu_preservation_requires_separate_report"] = True
        report["parity_validated"] = report["status"] == "complete"
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        report["parity_validated"] = False
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
    finally:
        report["hardware_end"] = hardware_receipt()
        report["finished_at"] = utc_now()
        write_json(args.output / "report.json", report)
        (args.output / "summary.md").write_text(markdown_report(report), encoding="utf-8")
        paired_benchmark.write_evidence_manifest(args.output)
    print(json.dumps({"status": report["status"], "report": str(args.output / "report.json")}), flush=True)
    return 0 if report["status"] == "complete" else 2


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execution-policy", choices=tuple(runtime.POLICIES), required=True)
    parser.add_argument("--legacy-reference-bin", action="store_true",
                        help="explicitly use an original v1 executable without the new CLI flag")
    parser.add_argument("--purpose", choices=("benchmark", "diagnostic"), default="benchmark")
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", choices=(*VARIANTS, "all"), default="all")
    parser.add_argument("--baseline", choices=("mps", "cpu", "both"), default="both")
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--max-rss-mib", type=int, default=8192)
    args = parser.parse_args(argv)
    for name in ("native_bin", "model_root", "upstream", "output"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    try:
        return driver(args)
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
