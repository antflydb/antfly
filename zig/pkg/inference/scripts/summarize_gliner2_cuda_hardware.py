#!/usr/bin/env python3
"""Bind passing L4, A100, and H100 CUDA qualification lanes into one report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from qualify_gliner2_cuda_hardware import ARCHITECTURES
from qualify_gliner2_cuda_hardware import CONTRACT as LANE_CONTRACT
from qualify_gliner2_cuda_hardware import FULL_PARITY_FILTERS
from qualify_gliner2_cuda_hardware import qualification_source_files
from qualify_gliner2_cuda_hardware import sha256_file
from qualify_gliner2_cuda_hardware import source_fingerprint


CONTRACT = "gliner2_cuda_hardware_qualification_matrix/v1"
REQUIRED = {"sm80": "A100", "sm89": "L4", "sm90": "H100"}
REQUIRED_CHECKS = {"embedded_artifacts", "full_parity", "memcheck", "initcheck", "racecheck"}
EXPECTED_CAPABILITIES = {
    architecture: capability for capability, (architecture, _family) in ARCHITECTURES.items()
}


def load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid CUDA qualification report {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"CUDA qualification report must be an object: {path}")
    return value


def is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        return False
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError:
        return False
    return True


def command_evidence_errors(
    label: str,
    check: Any,
    expected_log_name: str,
    report_path: Path | None,
) -> list[str]:
    if not isinstance(check, dict):
        return [f"{label} has no command evidence"]
    argv = check.get("argv")
    log_reference = check.get("log")
    errors: list[str] = []
    if (
        check.get("pass") is not True
        or check.get("returncode") != 0
        or check.get("timed_out") is not False
        or not isinstance(argv, list)
        or not argv
        or any(not isinstance(value, str) or not value for value in argv)
    ):
        errors.append(f"{label} did not record a passing command")
    if (
        not isinstance(log_reference, str)
        or Path(log_reference).is_absolute()
        or ".." in Path(log_reference).parts
        or Path(log_reference).name != expected_log_name
        or not is_sha256(check.get("log_sha256"))
    ):
        errors.append(f"{label} has invalid relocatable log evidence")
        return errors
    if report_path is not None:
        evidence_root = report_path.parent.resolve()
        log_path = (evidence_root / log_reference).resolve()
        try:
            log_path.relative_to(evidence_root)
        except ValueError:
            errors.append(f"{label} log escapes its evidence directory")
            return errors
        try:
            actual_sha256 = sha256_file(log_path)
        except OSError as exc:
            errors.append(f"{label} log is unavailable: {exc}")
        else:
            if actual_sha256 != check.get("log_sha256"):
                errors.append(f"{label} log hash does not match the lane report")
    return errors


def lane_report_errors(
    report: Any,
    architecture: str,
    family: str,
    current_fingerprint: str,
    package_dir: Path,
    report_path: Path | None,
) -> list[str]:
    if not isinstance(report, dict):
        return ["lane report is not an object"]
    gpu = report.get("gpu") if isinstance(report.get("gpu"), dict) else {}
    checks = report.get("checks") if isinstance(report.get("checks"), dict) else {}
    errors: list[str] = []
    if (
        report.get("contract") != LANE_CONTRACT
        or report.get("pass") is not True
        or report.get("failures") != []
        or report.get("training_precision") != "fp32"
        or report.get("optimizer_state_precision") != "fp32"
        or report.get("cuda_artifacts") != "fatbin"
        or report.get("source_fingerprint_sha256") != current_fingerprint
        or report.get("source_files") != list(qualification_source_files(package_dir))
    ):
        errors.append("lane contract, precision, artifacts, or source binding is invalid")
    if (
        gpu.get("architecture") != architecture
        or gpu.get("family") != family
        or gpu.get("compute_capability") != EXPECTED_CAPABILITIES.get(architecture)
        or family.lower() not in str(gpu.get("name", "")).lower()
        or not str(gpu.get("driver_version", ""))
        or not str(gpu.get("memory_mib", ""))
    ):
        errors.append("lane GPU identity is invalid")
    if set(checks) != REQUIRED_CHECKS:
        errors.append("lane does not contain the exact required check set")
        return errors

    errors.extend(command_evidence_errors(
        "embedded_artifacts",
        checks.get("embedded_artifacts"),
        "embedded-artifacts.log",
        report_path,
    ))
    for tool in ("memcheck", "initcheck", "racecheck"):
        errors.extend(command_evidence_errors(
            tool,
            checks.get(tool),
            f"{tool}.log",
            report_path,
        ))

    full_parity = checks.get("full_parity")
    if not isinstance(full_parity, dict):
        errors.append("full_parity has no suite evidence")
        return errors
    subchecks = full_parity.get("subchecks")
    if (
        full_parity.get("pass") is not True
        or full_parity.get("returncode") != 0
        or full_parity.get("timed_out") is not False
        or full_parity.get("failures") != []
        or not isinstance(full_parity.get("argv"), list)
        or not isinstance(subchecks, dict)
        or set(subchecks) != set(FULL_PARITY_FILTERS)
    ):
        errors.append("full_parity suite contract is invalid")
        return errors
    for test_filter in FULL_PARITY_FILTERS:
        errors.extend(command_evidence_errors(
            f"full_parity/{test_filter}",
            subchecks.get(test_filter),
            f"full-parity-{test_filter}.log",
            report_path,
        ))
    return errors


def summarize(paths: list[Path], package_dir: Path) -> dict[str, Any]:
    current_fingerprint = source_fingerprint(package_dir)
    lanes: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    evidence_logs_verified = True
    for path in paths:
        report = load(path)
        gpu = report.get("gpu") if isinstance(report.get("gpu"), dict) else {}
        architecture = str(gpu.get("architecture", ""))
        if architecture in lanes:
            failures.append(f"duplicate qualification lane for {architecture}")
            continue
        lanes[architecture] = {
            "path": str(path),
            "report": report,
        }
        expected_family = REQUIRED.get(architecture)
        lane_errors = (
            ["unsupported CUDA qualification architecture"]
            if expected_family is None
            else lane_report_errors(
                report,
                architecture,
                expected_family,
                current_fingerprint,
                package_dir,
                path,
            )
        )
        if lane_errors:
            evidence_logs_verified = False
            failures.extend(f"invalid qualification lane {path}: {error}" for error in lane_errors)
    missing = sorted(set(REQUIRED) - set(lanes))
    if missing:
        failures.append("missing required CUDA architectures: " + ", ".join(missing))
    return {
        "contract": CONTRACT,
        "pass": not failures,
        "failures": failures,
        "training_precision": "fp32",
        "optimizer_state_precision": "fp32",
        "cuda_artifacts": "fatbin",
        "evidence_logs_verified": evidence_logs_verified,
        "required_architectures": REQUIRED,
        "source_fingerprint_sha256": current_fingerprint,
        "lanes": lanes,
    }


def verify_summary(summary: dict[str, Any], package_dir: Path) -> list[str]:
    lanes = summary.get("lanes")
    if not isinstance(lanes, dict):
        return ["CUDA hardware qualification matrix has no lane evidence"]
    errors: list[str] = []
    try:
        current_fingerprint = source_fingerprint(package_dir)
    except ValueError as exc:
        return [str(exc)]
    if (
        summary.get("contract") != CONTRACT
        or summary.get("pass") is not True
        or summary.get("failures") != []
        or summary.get("training_precision") != "fp32"
        or summary.get("optimizer_state_precision") != "fp32"
        or summary.get("cuda_artifacts") != "fatbin"
        or summary.get("evidence_logs_verified") is not True
        or summary.get("required_architectures") != REQUIRED
        or summary.get("source_fingerprint_sha256") != current_fingerprint
        or set(lanes) != set(REQUIRED)
    ):
        errors.append("CUDA hardware qualification matrix is stale or has an invalid contract")
    for architecture, family in REQUIRED.items():
        lane = lanes.get(architecture)
        report = lane.get("report") if isinstance(lane, dict) else None
        if lane_report_errors(
            report,
            architecture,
            family,
            current_fingerprint,
            package_dir,
            None,
        ):
            errors.append(f"invalid embedded CUDA qualification lane: {architecture}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    script_dir = Path(__file__).resolve().parent
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        # A failed aggregation must not leave an older current-fingerprint
        # matrix available for a subsequent readiness command.
        output.unlink(missing_ok=True)
    except OSError as exc:
        parser.error(f"cannot invalidate previous CUDA matrix output: {exc}")
    try:
        result = summarize([path.expanduser().resolve() for path in args.report], script_dir.parent)
    except ValueError as exc:
        parser.error(str(exc))
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output)
    print(f"CUDA qualification matrix report: {output}")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
