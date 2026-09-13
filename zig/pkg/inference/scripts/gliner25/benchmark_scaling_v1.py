#!/usr/bin/env python3
"""Bounded, separate true-batch scaling comparison; never the ten-case gate.

First capture new-text outputs through pinned Fastino CPU, then complete the
native Metal/Fastino MPS preflight before collecting optional measurements.
The immutable input manifest contains no expected model decisions.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import benchmark_metal_v2 as campaign
import scaling_contract_v1 as contract
import scaling_runtime_v1 as runtime
from metal_benchmark_supervisor import BenchmarkError, ResourceGuard, Worker

oracle, cpu = campaign.oracle, campaign.cpu


def capture_cpu(args, inputs, selected, directory):
    directory.mkdir(parents=True)
    env, environment = campaign.worker_environment()
    guard = ResourceGuard(args.max_rss_mib * 1024**2)
    command = [sys.executable, str(campaign.HERE / "metal_scaling_python_worker.py"),
        "--device", "cpu", "--model", inputs["model"], "--model-dir", str(args.model_root / inputs["model"]),
        "--upstream", str(args.upstream), "--prepared", str(args.prepared),
        "--max-commands", str(len(selected) + 1)]
    report = {"status": "running", "phase": "source_cpu_capture", "model": inputs["model"],
              "scope": contract.SCOPE, "command": command, "inputs": inputs["pins"],
              "environment": environment, "qualification": False, "cases": {},
              "started_at": campaign.utc_now()}
    worker = None
    with (directory / "events.jsonl").open("w", encoding="utf-8") as journal:
        def record(event):
            raw = json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
            if journal.tell() + len(raw.encode()) > 64 * 1024**2:
                raise BenchmarkError("source capture evidence exceeds 64 MiB")
            journal.write(raw)
            journal.flush()
        try:
            worker = Worker("fastino_cpu", command, env, directory, guard)
            ready = worker.receive(args.startup_timeout)
            record(ready)
            runtime.ready("fastino_cpu", ready, inputs, args.execution_policy)
            report["ready"] = ready
            for name in selected:
                response = worker.request("validate", name, args.timeout_ms / 1000 + 5)
                record(response)
                try:
                    output = runtime.result("fastino_cpu", response, inputs["source_cases"][name], None,
                                            validation=True, phase="validation")
                    report["cases"][name] = {"status": "complete", "outputs": output,
                        **contract.packet(inputs["source_cases"][name]["expected_input_ids"]),
                        "sample": runtime.sample_receipt(response)}
                except (cpu.BenchmarkError, KeyError, TypeError) as error:
                    report["cases"][name] = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
                    if response.get("recoverable") is not True or response.get("device_unsafe"):
                        raise BenchmarkError("unsafe CPU reference error prevents further cases") from error
            stopped = worker.request("stop", timeout=args.startup_timeout)
            record(stopped)
            if stopped.get("scope") != contract.SCOPE or stopped.get("requests_sha256") != inputs["requests_sha256"]:
                raise BenchmarkError("CPU capture stop changed input identity")
            if worker.process.wait(timeout=5) != 0:
                raise BenchmarkError("CPU capture failed to exit after stop")
            report["status"] = "complete" if all(row["status"] == "complete" for row in report["cases"].values()) else "partial"
        except BaseException as error:
            report.update(status="interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed",
                          error=f"{type(error).__name__}: {error}")
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
        finally:
            if worker is not None:
                try:
                    worker.close()
                except Exception as error:
                    report.update(status="failed", cleanup_error=f"{type(error).__name__}: {error}")
                report["cleanup"] = getattr(worker, "cleanup", None)
                if not isinstance(report["cleanup"], dict) or report["cleanup"].get("complete") is not True:
                    report.update(status="failed", cleanup_error="CPU reference cleanup is incomplete")
            report["resource_guard"] = guard.receipt()
            # A failed constructor is also owned by ResourceGuard. The strict
            # Worker constructor closes its Popen child before raising.
            for completed in report["resource_guard"].get("completed_workers", []):
                receipt = completed.get("cleanup")
                if not isinstance(receipt, dict) or receipt.get("complete") is not True:
                    report.update(status="failed", cleanup_error="partial-constructor worker cleanup is incomplete")
            report["finished_at"] = campaign.utc_now()
            for name in selected:
                report["cases"].setdefault(name, {"status": "unprocessed"})
            campaign.write_json(directory / "report.json", report)
    return report


def summary_rows(report, inputs):
    """Keep every selected row, including unsuccessful correctness-only B8s."""
    rows = []
    for value in inputs:
        for name in value["cases"]:
            evidence, observed = [], None
            for run in report["source_captures"] + report["runs"]:
                if run["model"] != value["model"]:
                    continue
                case = run.get("cases", run.get("case_status", {})).get(name)
                if case is None:
                    continue
                phase = run.get("phase", "source_cpu_capture")
                detail = {"phase": phase, "run_status": run["status"], "case_status": case["status"]}
                for key in ("stage", "error"):
                    if key in case:
                        detail[key] = case[key]
                for key in ("error", "cleanup_error", "cleanup_errors"):
                    if key in run:
                        detail[f"run_{key}"] = run[key]
                evidence.append(detail)
                if phase == "measurement" and run["status"] in ("complete", "partial"):
                    observed = run.get("comparisons", {}).get(name)
            def completed(phase, status):
                return any(item["phase"] == phase and item["case_status"] == status
                           and item["run_status"] in ("complete", "partial") for item in evidence)
            validated = completed("source_cpu_capture", "complete") and completed("preflight", "validated")
            smoke = value["source_cases"][name]["profile"]["mode"] == "smoke"
            status = "blocked"
            if any(item["case_status"] == "failed" or (
                    item["run_status"] in ("failed", "interrupted")
                    and item["case_status"] not in ("pending", "unprocessed")) for item in evidence):
                status = "failed"
            if validated and (smoke or observed is not None):
                status = "correctness_only" if smoke else "complete"
            row = {"model": value["model"], "case_id": name, "status": status,
                   "correctness_only": smoke, "evidence": evidence}
            if observed is not None and not smoke:
                row.update(observed)
            rows.append(row)
    return rows


def driver(args):
    if (args.output.exists() or not args.native_bin.is_file() or args.execution_policy not in campaign.runtime.POLICIES
            or not 256 <= args.max_rss_mib <= 8192 or not 1 <= args.timeout_ms <= 60_000
            or not 1 <= args.startup_timeout <= 300):
        raise BenchmarkError("requires fresh output, existing binary and bounded worker profile")
    args.repetitions, args.warmup, args.pairs, args.legacy_reference_bin = 1, 1, 3, False
    variants = list(contract.VARIANTS) if args.model == "all" else [args.model]
    dependencies = oracle.verify_dependencies()
    upstream = oracle.verify_upstream_checkout(args.upstream)
    oracle.verify_config_fixtures()
    inputs = []
    for variant in variants:
        value = contract.load(args.prepared, variant)
        value["workload"], value["bundle"] = "scaling", oracle.verify_model_dir(variant, args.model_root / variant)
        chosen = args.cases or value["cases"]
        if not chosen or len(set(chosen)) != len(chosen) or any(name not in value["cases"] for name in chosen):
            raise BenchmarkError("scaling selection must name unique manifest-approved cases")
        value["cases"] = list(chosen)
        inputs.append(value)
    args.output.mkdir(parents=True)
    snapshot = campaign.source_snapshot()
    campaign.write_json(args.output / "source_manifest.json", snapshot)
    native = {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)}
    report = {"format_version": 1, "scope": contract.SCOPE, "status": "running",
        "purpose": args.purpose, "execution_policy": args.execution_policy,
        "timing_boundary": cpu.TIMING_BOUNDARY, "threads": 1, "precision": "float32",
        "source": upstream, "dependencies": dependencies, "source_files_sha256": snapshot["source_files_sha256"],
        "native_binary": native, "policy": contract.POLICY,
        "repetitions": 1, "warmup_per_case": 1, "pairs_per_case": 3,
        "baselines": ["mps"], "bootstrap_samples": 0, "confidence_intervals": False,
        "contracts": [{"model": value["model"], "cases": value["cases"], "pins": value["pins"]} for value in inputs],
        "source_captures": [], "runs": [], "summary": [], "qualification": False,
        "serving_qualified": False, "performance_release_qualified": False,
        "original_30_case_gate": False, "started_at": campaign.utc_now()}
    campaign.write_json(args.output / "report.json", report)
    try:
        # Every requested source row remains in the denominator, including
        # source exceptions; no failed CPU row can acquire an invented oracle.
        for value in inputs:
            captured = capture_cpu(args, value, value["cases"], args.output / f"source-cpu-{value['model']}")
            report["source_captures"].append(captured)
            if captured.get("cleanup_error"):
                raise BenchmarkError("source process cleanup failed; refusing any further model")
            value["expected"] = {name: row["outputs"] for name, row in captured["cases"].items()
                                 if row["status"] == "complete"}
            campaign.write_json(args.output / "report.json", report)
        eligible = {}
        for value in inputs:
            selected = [name for name in value["cases"] if name in value["expected"]]
            if not selected:
                eligible[value["model"]] = []
                continue
            run = campaign.run_pair(args, value, "mps", args.output / f"preflight-{value['model']}",
                                    repetition=0, phase="preflight", selected=selected)
            report["runs"].append(run)
            if run.get("cleanup_errors"):
                raise BenchmarkError("paired process cleanup failed; refusing any further model")
            eligible[value["model"]] = [name for name in selected
                if run["status"] in ("complete", "partial") and run["case_status"][name]["status"] == "validated"]
            campaign.write_json(args.output / "report.json", report)
        if args.purpose != "validation":
            for repetition in range(1):
                for value in campaign.rotate(inputs, repetition):
                    selected = [name for name in eligible[value["model"]]
                                if value["source_cases"][name]["profile"]["mode"] != "smoke"]
                    if not selected:
                        continue
                    phase = "measurement" if args.purpose == "benchmark" else "diagnostic"
                    run = campaign.run_pair(args, value, "mps",
                        args.output / f"{phase}-{repetition + 1}-{value['model']}",
                        repetition=repetition, phase=phase, selected=selected)
                    report["runs"].append(run)
                    campaign.write_json(args.output / "report.json", report)
                    if run.get("cleanup_errors"):
                        raise BenchmarkError("paired cleanup failed; refusing any further model")
        for value in inputs:
            if contract.load(args.prepared, value["model"])["pins"] != value["pins"]:
                raise BenchmarkError("scaling inputs changed during campaign")
        if oracle.sha256_file(args.native_bin) != native["sha256"] or campaign.source_snapshot() != snapshot:
            raise BenchmarkError("scaling binary/source changed during campaign")
        if oracle.verify_dependencies() != dependencies:
            raise BenchmarkError("scaling dependency identity changed")
        oracle.verify_upstream_checkout(args.upstream)
        if args.purpose == "benchmark":
            report["summary"] = summary_rows(report, inputs)
        complete = (all(run["status"] == "complete" for run in report["source_captures"] + report["runs"])
                    and all(len(eligible[value["model"]]) == len(value["cases"]) for value in inputs)
                    and (args.purpose != "benchmark" or all(row["status"] in ("complete", "correctness_only")
                                                           for row in report["summary"])))
        report.update(status="complete" if complete else "partial", parity_validated=complete)
    except BaseException as error:
        report.update(status="interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed",
                      error=f"{type(error).__name__}: {error}", parity_validated=False)
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
    finally:
        if args.purpose == "benchmark":
            report["summary"] = summary_rows(report, inputs)
        report["finished_at"] = campaign.utc_now()
        campaign.write_json(args.output / "report.json", report)
        campaign.paired_benchmark.write_evidence_manifest(args.output)
    print(json.dumps({"status": report["status"], "report": str(args.output / "report.json")}), flush=True)
    return 0 if report["status"] == "complete" else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--model-root", type=Path, default=Path("/private/tmp/antfly-gliner25-models"))
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execution-policy", choices=tuple(campaign.runtime.POLICIES), required=True)
    parser.add_argument("--purpose", choices=("validation", "diagnostic", "benchmark"), default="benchmark")
    parser.add_argument("--model", choices=(*contract.VARIANTS, "all"), default="all")
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--max-rss-mib", type=int, default=8192)
    args = parser.parse_args(argv)
    for name in ("prepared", "native_bin", "model_root", "upstream", "output"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    try:
        return driver(args)
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
