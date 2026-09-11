#!/usr/bin/env python3
"""Three-repetition native CPU preservation campaign and saved-report audit.

Uses the original CPU worker/math/fixtures with the bounded v2 process owner.
Audit mode imports no ML runtime and rederives every paired statistic. Native
CPU observations are never labelled or combined with Metal/CPU comparisons.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import statistics
import sys

import benchmark_metal_v2 as campaign
import metal_runtime_contract_v2 as runtime

cpu, oracle, paired = campaign.cpu, campaign.oracle, campaign.paired_benchmark
SCOPE = "gliner25_native_cpu_preservation_fp32_v1"
MAX_REPORT_BYTES = 16 * 1024**2


def read_pinned(path):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REPORT_BYTES:
            raise cpu.BenchmarkError("CPU report must be a bounded regular file")
        data = source.read(MAX_REPORT_BYTES + 1)
        if len(data) != info.st_size:
            raise cpu.BenchmarkError("CPU report changed while reading")
    return cpu.strict_json(data), {"path": str(path), "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def bundle_for(variant):
    model = oracle.load_manifest()["models"][variant]
    return {"model_id": model["model_id"], "revision": model["revision"],
            "files": {name: {key: value[key] for key in ("sha256", "size_bytes")}
                      for name, value in model["files"].items()}}


def checked_model(model, *, check_statistics=True):
    variant = model.get("model")
    if variant not in campaign.VARIANTS:
        raise cpu.BenchmarkError("unknown CPU model variant")
    if "status" in model and model["status"] != "complete":
        raise cpu.BenchmarkError("CPU model run contains a preserved failure")
    fixture = oracle.read_json(cpu.case_fixture(variant))
    expected = {row["id"]: cpu.canonical_result(row["expected"]) for row in fixture["cases"]}
    bundle = bundle_for(variant)
    actual_bundle = model.get("model_bundle", {})
    if {key: actual_bundle.get(key) for key in bundle} != bundle:
        raise cpu.BenchmarkError("CPU report model identity differs from the pinned FP32 bundle")
    for arm in ("native", "python"):
        cpu.checked_ready(arm, model["workers"][arm], bundle, cpu.case_fixture(variant), 1)
    provenance = model["workers"]["python"].get("provenance", {})
    manifest = oracle.load_manifest()
    if (provenance.get("runtime") != manifest["runtime"]
            or provenance.get("commit") != manifest["upstream"]["commit"]
            or provenance.get("device") != "cpu" or provenance.get("dtype") != "float32"):
        raise cpu.BenchmarkError("CPU reference dependency/device/source provenance differs")
    if set(model.get("validation", {})) != set(expected) or set(model.get("comparisons", {})) != set(expected):
        raise cpu.BenchmarkError("CPU report omitted or added a fixed task case")
    rows = model.get("pairs")
    if not isinstance(rows, list) or len(rows) != 30 * len(expected):
        raise cpu.BenchmarkError("CPU report requires exactly thirty pairs per fixed case")
    seen, pairs = set(), {name: [] for name in expected}
    for row in rows:
        case, number = row.get("case_id"), row.get("pair")
        if (case not in expected or type(number) is not int or not 1 <= number <= 30
                or (case, number) in seen or row.get("valid", True) is not True
                or sorted(row.get("order", [])) != ["native", "python"]):
            raise cpu.BenchmarkError("CPU pair identity/order/validity is incomplete or duplicated")
        seen.add((case, number))
        values = [runtime.integer(row[arm].get("duration_ns"), f"{case}.{arm}.duration_ns") for arm in ("native", "python")]
        if not all(values):
            raise cpu.BenchmarkError("CPU paired durations must be positive")
        pairs[case].append((number, *values))
    result = {}
    for name in expected:
        validation = model["validation"][name]
        if validation.get("outputs_match_oracle") is not True or validation.get("confidence_absolute_tolerance") != 5e-4:
            raise cpu.BenchmarkError("CPU validation did not preserve the original confidence gate")
        cpu.require_equal(expected[name], validation["expected"])
        outputs = validation.get("outputs", validation)
        for arm in ("native", "python"):
            cpu.require_equal(expected[name], outputs[arm], f"{variant}.{name}.{arm}")
        ids = validation.get("input_ids")
        if (not isinstance(ids, list) or not 1 <= len(ids) <= oracle.MAX_ENCODED_TOKENS
                or any(type(token) is not int or token < 0 for token in ids)):
            raise cpu.BenchmarkError("CPU validation lacks encoder token evidence")
        selected = [(n, p) for _, n, p in sorted(pairs[name])]
        recorded = model["comparisons"][name]
        # Pair order must be balanced per task; either initial arm is allowed.
        orders = [row["order"][0] for row in rows if row["case_id"] == name]
        if orders.count("native") != 15 or orders.count("python") != 15:
            raise cpu.BenchmarkError("CPU arm order is not balanced per task")
        computed = {"native_ns": paired.distribution(n for n, _ in selected),
                    "python_ns": paired.distribution(p for _, p in selected),
                    "native_over_python_latency": paired.paired_log_ratio_ci(selected, samples=2000)}
        if check_statistics and any(recorded.get(key) != value for key, value in computed.items()):
            raise cpu.BenchmarkError("CPU report statistics differ from the retained raw pairs")
        result[name] = computed
    return result


def audit_reports(paths, *, power_receipt=None):
    if len(paths) != 3 or len({str(path.resolve()) for path in paths}) != 3:
        raise cpu.BenchmarkError("CPU preservation requires three distinct report files")
    reports, pins, checked = [], [], []
    for path in paths:
        report, pin = read_pinned(path)
        if (report.get("scope") not in (cpu.SCOPE, SCOPE) or report.get("status") != "complete"
                or report.get("parity_validated") is not True
                or report.get("serving_qualified") is not False
                or report.get("performance_release_qualified") is not False
                or report.get("timing_boundary") != cpu.TIMING_BOUNDARY
                or any(type(report.get(key)) is not int or report[key] != value for key, value in (
                    ("threads", 1), ("warmup_per_case", 5), ("pairs_per_case", 30)))):
            raise cpu.BenchmarkError("CPU report status/profile/scope is not eligible for preservation")
        if report.get("source", {}).get("commit") != oracle.load_manifest()["upstream"]["commit"]:
            raise cpu.BenchmarkError("CPU report upstream commit differs")
        models = report.get("models", [])
        if len(models) != 3 or {m.get("model") for m in models} != set(campaign.VARIANTS):
            raise cpu.BenchmarkError("CPU report must contain every variant exactly once")
        checked.append({model["model"]: checked_model(model) for model in models})
        reports.append(report)
        pins.append(pin)
    native = reports[0]["native_binary"]
    if any(report["native_binary"] != native for report in reports):
        raise cpu.BenchmarkError("CPU repetitions use different executable identities")
    if oracle.sha256_file(Path(native["path"])) != native["sha256"]:
        raise cpu.BenchmarkError("CPU executable bytes differ from the report")
    if any(report["driver_sha256"] != reports[0]["driver_sha256"] for report in reports):
        raise cpu.BenchmarkError("CPU repetitions use different driver identities")
    closure = {name: oracle.sha256_file(campaign.HERE / name) for name in (
        "benchmark_cpu.py", "oracle.py", "oracle_manifest.json", "generate_pipeline_cases.py")}
    closure["../paired_benchmark.py"] = oracle.sha256_file(Path(paired.__file__))
    expected_driver = closure["benchmark_cpu.py"] if reports[0]["scope"] == cpu.SCOPE else oracle.sha256_file(Path(__file__))
    if reports[0]["driver_sha256"] != expected_driver:
        raise cpu.BenchmarkError("CPU report driver differs from its reviewed helper")
    power = None
    if power_receipt is not None:
        observed, power = read_pinned(power_receipt)
        if (not isinstance(observed, list) or len(observed) != 3
                or {row.get("repetition") for row in observed} != {1, 2, 3}):
            raise cpu.BenchmarkError("CPU power receipt lacks the three observed repetitions")
        for row in observed:
            if (row.get("exit_code") != 0 or type(row.get("exit_code")) is not int
                    or Path(row.get("report", "")).resolve() != paths[row["repetition"] - 1].resolve()
                    or any("Now drawing from 'AC Power'" not in row.get(key, "") for key in ("power_before", "power_after"))):
                raise cpu.BenchmarkError("CPU repetition power/exit/report binding differs")
    else:
        profiles = [report.get("hardware_start") for report in reports]
        if not all(isinstance(profile, dict) for profile in profiles):
            raise cpu.BenchmarkError("historical CPU reports require their separately pinned power receipt")
        for report in reports:
            for model in report["models"]:
                if model.get("power_profile_stable") is not True or any(
                    model["hardware_start"].get(key) != reports[0]["hardware_start"].get(key)
                    for key in ("power_source", "low_power_mode")):
                    raise cpu.BenchmarkError("CPU power profile differs across the measured repetitions")
    summary = []
    for variant in campaign.VARIANTS:
        for name in checked[0][variant]:
            observations = [repetition[variant][name] for repetition in checked]
            intervals = [row["native_over_python_latency"] for row in observations]
            summary.append({"model": variant, "case_id": name,
                "native_median_ms": statistics.median(row["native_ns"]["median"] for row in observations) / 1e6,
                "python_median_ms": statistics.median(row["python_ns"]["median"] for row in observations) / 1e6,
                "repetitions": intervals, "acceptance": runtime.competitiveness(intervals, numerator="native_over_python")})
    return {"version": 1, "scope": SCOPE, "status": "audited", "reports": pins,
            "native_binary": native, "helpers": closure, "power_receipt": power,
            "models": {variant: bundle_for(variant) for variant in campaign.VARIANTS},
            "summary": summary, "all_30_cases_preserve_native_cpu_advantage": all(row["acceptance"]["passed"] for row in summary),
            "comparison": "native_cpu_vs_fastino_cpu", "source_update_identity": reports[0]["source"],
            "same_source_as_optimized_metal": False, "serving_qualified": False,
            "native_binary_source_binding_requires_separate_build_receipt": True,
            "performance_release_qualified": False, "audit_sha256": oracle.sha256_file(Path(__file__))}


def run(args):
    if args.output.exists() or not args.native_bin.is_file():
        raise cpu.BenchmarkError("requires a fresh directory and existing native CPU executable")
    if (not 256 <= args.max_rss_mib <= 8192 or not 1 <= args.timeout_ms <= 60_000
            or not 1 <= args.startup_timeout <= 300):
        raise cpu.BenchmarkError("CPU process RSS/deadline caps are outside the declared bounds")
    # No tuning surface: preserve the accepted baseline sampling exactly.
    args.warmup, args.pairs = 5, 30
    args.execution_policy, args.legacy_reference_bin = "reference_v1", True
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    dependencies = oracle.verify_dependencies()
    source = oracle.verify_upstream_checkout(args.upstream)
    native = {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)}
    contracts = {variant: campaign.load_contract(variant, args.model_root, None) for variant in campaign.VARIANTS}
    args.output.mkdir(parents=True)
    snapshot = campaign.source_snapshot()
    campaign.write_json(args.output / "source_manifest.json", snapshot)
    paths = []
    for repetition in range(3):
        directory = args.output / f"repetition-{repetition + 1}"
        directory.mkdir()
        report = {"format_version": 2, "scope": SCOPE, "status": "running", "parity_validated": False,
                  "threads": 1, "warmup_per_case": 5, "pairs_per_case": 30, "timing_boundary": cpu.TIMING_BOUNDARY,
                  "native_binary": native, "driver_sha256": oracle.sha256_file(Path(__file__)),
                  "source": source, "dependencies": dependencies, "hardware_start": campaign.hardware_receipt(),
                  "source_files_sha256": snapshot["source_files_sha256"], "models": [],
                  "serving_qualified": False, "performance_release_qualified": False}
        path = directory / "report.json"
        paths.append(path)
        try:
            for variant in campaign.rotate(list(campaign.VARIANTS), repetition):
                print(json.dumps({"event": "cpu_preservation_start", "model": variant, "repetition": repetition + 1}), flush=True)
                result = campaign.run_pair(args, contracts[variant], "cpu", directory / variant,
                    repetition=repetition, phase="measurement", selected=contracts[variant]["cases"], native_backend="native")
                result["model_bundle"] = contracts[variant]["bundle"]
                report["models"].append(result)
                campaign.write_json(path, report)
                if result.get("cleanup_errors"):
                    raise cpu.BenchmarkError("CPU worker cleanup incomplete; refusing another model process")
            if any(model["status"] != "complete" for model in report["models"]):
                raise cpu.BenchmarkError("CPU repetition contains a preserved failed case")
            if oracle.sha256_file(args.native_bin) != native["sha256"]:
                raise cpu.BenchmarkError("CPU executable changed during the campaign")
            report.update(status="complete", parity_validated=True)
        except BaseException as error:
            report.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            report["hardware_end"] = campaign.hardware_receipt()
            campaign.write_json(path, report)
            paired.write_evidence_manifest(directory)
    if campaign.source_snapshot() != snapshot or oracle.verify_dependencies() != dependencies:
        raise cpu.BenchmarkError("CPU campaign source or Python dependency identity changed")
    audit = audit_reports(paths)
    campaign.write_json(args.output / "audit.json", audit)
    paired.write_evidence_manifest(args.output)
    return audit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--reports", type=Path, nargs=3, required=True)
    audit.add_argument("--power-receipt", type=Path)
    audit.add_argument("--output", type=Path, required=True)
    execute = commands.add_parser("run")
    execute.add_argument("--native-bin", type=Path, required=True)
    execute.add_argument("--model-root", type=Path, required=True)
    execute.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--timeout-ms", type=int, default=30_000)
    execute.add_argument("--startup-timeout", type=float, default=120)
    execute.add_argument("--max-rss-mib", type=int, default=8192)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            for key in ("native_bin", "model_root", "upstream", "output"):
                setattr(args, key, getattr(args, key).expanduser().resolve())
            result = run(args)
        else:
            if args.output.exists():
                raise cpu.BenchmarkError("audit output already exists")
            result = audit_reports(args.reports, power_receipt=args.power_receipt)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            campaign.write_json(args.output, result)
        print(json.dumps({"status": result["status"], "all_30_cases_preserve_native_cpu_advantage":
                          result["all_30_cases_preserve_native_cpu_advantage"]}), flush=True)
        return 0 if result["all_30_cases_preserve_native_cpu_advantage"] else 2
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
