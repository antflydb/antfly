#!/usr/bin/env python3
"""Compute GLiNER2 production readiness from current release evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from evaluate_gliner2_full_task import EVALUATION_CONTRACT, REQUIRED_MINIMA
from evaluate_gliner2_native_release_smoke import NATIVE_EVALUATION_CONTRACT
from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_NORMALIZATION,
    CANONICAL_ORACLE_PACKAGE_VERSIONS,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
)
from summarize_gliner2_convergence import (
    EVIDENCE_CONTRACT,
    OUTPUT_CONTRACT,
    STOCK_STOCHASTIC_TRAINING_POLICY,
    verify_summary_evidence,
)
from summarize_gliner2_cuda_hardware import CONTRACT as CUDA_HARDWARE_CONTRACT
from validate_gliner2_release_data import adapter_bundle_fingerprint


def is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(char in "0123456789abcdef" for char in value[7:])
    )


def object_field(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
    value = payload.get(key) if isinstance(payload, dict) else None
    return value if isinstance(value, dict) else {}


def build_summary(
    *,
    backend: str,
    default_rc: int,
    head_rc: int,
    quality_rc: int,
    native_rc: int,
    skip_head_opt_in: bool,
    require_head_opt_in: bool,
    default_summary: dict[str, Any] | None,
    head_summary: dict[str, Any] | None,
    quality_report: dict[str, Any] | None,
    native_report: dict[str, Any] | None,
    convergence_report: dict[str, Any] | None,
    convergence_evidence_errors: list[str],
    hardware_qualification: dict[str, Any] | None,
    hardware_qualification_errors: list[str],
    release_adapter_fingerprint: str | None,
    release_manifest: dict[str, Any] | None,
    paths: dict[str, str | None],
) -> dict[str, Any]:
    if backend not in {"metal", "cuda"}:
        raise ValueError(f"unsupported production backend: {backend}")
    integrity_errors: list[str] = []
    default_ready = bool(
        default_rc == 0
        and isinstance(default_summary, dict)
        and default_summary.get("pass") is True
        and default_summary.get("accelerator_backend") == backend
        and default_summary.get("training_precision") == "fp32"
        and default_summary.get("optimizer_state_precision") == "fp32"
        and default_summary.get("precision_consistent") is True
    )
    if default_rc == 0 and not default_ready:
        integrity_errors.append("deterministic parity/performance command returned success without a current passing summary")

    oracle = object_field(default_summary, "oracle")
    oracle_ready = bool(
        default_ready
        and default_summary.get("oracle_valid_in_every_run") is True
        and isinstance(oracle, dict)
        and oracle.get("commit") == UPSTREAM_COMMIT
        and oracle.get("gliner2_version") == CANONICAL_GLINER2_VERSION
        and oracle.get("package_versions") == CANONICAL_ORACLE_PACKAGE_VERSIONS
    )

    quality_ready = bool(
        quality_rc == 0
        and isinstance(quality_report, dict)
        and quality_report.get("contract") == EVALUATION_CONTRACT
        and quality_report.get("pass") is True
        and object_field(quality_report, "inference").get("normalization") == CANONICAL_NORMALIZATION
        and object_field(quality_report, "inference").get("unicode_version") == CANONICAL_UNICODE_VERSION
        and object_field(quality_report, "oracle").get("commit") == UPSTREAM_COMMIT
        and object_field(quality_report, "oracle").get("gliner2_version") == CANONICAL_GLINER2_VERSION
        and object_field(quality_report, "oracle").get("package_versions") == CANONICAL_ORACLE_PACKAGE_VERSIONS
    )
    if quality_rc == 0 and not quality_ready:
        integrity_errors.append("Python-oracle held-out command returned success without a pinned, normalized passing report")

    native_ready = bool(
        native_rc == 0
        and isinstance(native_report, dict)
        and native_report.get("contract") == NATIVE_EVALUATION_CONTRACT
        and native_report.get("pass") is True
        and native_report.get("normalization") == CANONICAL_NORMALIZATION
    )
    if native_rc == 0 and not native_ready:
        integrity_errors.append("Zig held-out command returned success without the canonical-normalization passing report")

    convergence_ready = bool(
        isinstance(convergence_report, dict)
        and convergence_report.get("contract") == OUTPUT_CONTRACT
        and convergence_report.get("evidence_contract") == EVIDENCE_CONTRACT
        and convergence_report.get("evidence_bound") is True
        and not convergence_evidence_errors
        and convergence_report.get("pass") is True
        and convergence_report.get("seed_count") == 5
        and object_field(convergence_report, "oracle").get("commit") == UPSTREAM_COMMIT
        and object_field(convergence_report, "oracle").get("checkout") == oracle.get("checkout")
        and object_field(convergence_report, "oracle").get("gliner2_version") == CANONICAL_GLINER2_VERSION
        and object_field(convergence_report, "oracle").get("package_versions") == CANONICAL_ORACLE_PACKAGE_VERSIONS
        and convergence_report.get("normalization") == CANONICAL_NORMALIZATION
        and convergence_report.get("unicode_version") == CANONICAL_UNICODE_VERSION
        and convergence_report.get("training_policy") == STOCK_STOCHASTIC_TRAINING_POLICY
        and convergence_report.get("accelerator_backend") == backend
        and convergence_report.get("training_precision") == "fp32"
        and convergence_report.get("thresholds") == {
            "max_mean_deficit": 0.02,
            "max_paired_deficit": 0.05,
        }
        and isinstance(convergence_report.get("runs"), list)
        and len(convergence_report["runs"]) == 5
        and all(isinstance(run, dict) and run.get("pass") is True for run in convergence_report["runs"])
        and isinstance(convergence_report.get("metrics"), dict)
        and set(convergence_report["metrics"]) == set(REQUIRED_MINIMA)
        and all(
            isinstance(metric, dict) and metric.get("pass") is True
            for metric in convergence_report["metrics"].values()
        )
    )

    perf_model = default_summary.get("model_fingerprint_sha256") if isinstance(default_summary, dict) else None
    perf_train = default_summary.get("training_data_fingerprint_sha256") if isinstance(default_summary, dict) else None
    quality_artifacts = object_field(quality_report, "artifacts")
    native_artifacts = object_field(native_report, "artifacts")
    convergence_fingerprints = object_field(convergence_report, "fingerprints")
    fingerprints_ready = bool(
        is_sha256(perf_model)
        and is_sha256(perf_train)
        and is_sha256(release_adapter_fingerprint)
        and quality_artifacts.get("base_model_fingerprint_sha256") == perf_model
        and quality_artifacts.get("adapter_bundle_fingerprint_sha256") == release_adapter_fingerprint
        and native_artifacts.get("base_model_fingerprint_sha256") == perf_model
        and native_artifacts.get("adapter_bundle_fingerprint_sha256") == release_adapter_fingerprint
        and native_artifacts.get("eval_data_fingerprint_sha256") == quality_artifacts.get("eval_data_fingerprint_sha256")
        and convergence_fingerprints.get("base_model") == perf_model
        and convergence_fingerprints.get("train_data") == perf_train
        and convergence_fingerprints.get("eval_data") == quality_artifacts.get("eval_data_fingerprint_sha256")
    )

    release_adapter_ready = bool(
        isinstance(release_manifest, dict)
        and str(release_manifest.get("backend", "")).lower() == backend
        and release_manifest.get("compiled_required") is True
        and release_manifest.get("training_precision") == "fp32"
        and release_manifest.get("optimizer_state_precision") == "fp32"
    )
    if not release_adapter_ready:
        integrity_errors.append(
            f"release adapter is not bound to compiled {backend.upper()} FP32 training and optimizer state"
        )

    hardware_ready = bool(
        backend != "cuda"
        or (
            isinstance(hardware_qualification, dict)
            and hardware_qualification.get("contract") == CUDA_HARDWARE_CONTRACT
            and hardware_qualification.get("pass") is True
            and hardware_qualification.get("training_precision") == "fp32"
            and hardware_qualification.get("optimizer_state_precision") == "fp32"
            and hardware_qualification.get("cuda_artifacts") == "fatbin"
            and not hardware_qualification_errors
        )
    )

    head_ready = (
        None
        if skip_head_opt_in
        else head_rc == 0 and isinstance(head_summary, dict) and head_summary.get("pass") is True
    )
    head_required_ready = not require_head_opt_in or head_ready is True
    if not skip_head_opt_in and head_rc == 0 and head_ready is not True:
        integrity_errors.append("head opt-in command returned success without a current passing summary")

    blockers: list[str] = []
    if not default_ready:
        blockers.append("deterministic cross-runtime parity/performance gate failed")
    if not oracle_ready:
        blockers.append("Python oracle checkout/import/runtime dependencies were not pinned in every comparison run")
    if not quality_ready:
        blockers.append("Python-oracle held-out quality gate failed or did not run")
    if not native_ready:
        blockers.append("Zig held-out full-task quality gate failed or used a different normalization contract")
    if not convergence_ready:
        blockers.append("five-seed Python/Zig held-out convergence gate failed or is missing")
    if not fingerprints_ready:
        blockers.append("model/train/eval/release-adapter fingerprints do not bind all readiness evidence to the same artifacts")
    if not hardware_ready:
        blockers.append("current FP32 CUDA sanitizer/parity qualification is missing for L4, A100, or H100")
    if not head_required_ready:
        blockers.append("required head opt-in gate failed")
    blockers.extend(integrity_errors)
    production_ready = not blockers

    return {
        "contract": "gliner2_zig_accelerator_production_readiness/v2",
        "accelerator_backend": backend,
        "training_precision": "fp32",
        "production_ready": production_ready,
        "production_readiness_blockers": blockers,
        "checks": {
            "deterministic_correctness_and_performance": default_ready,
            "pinned_python_oracle": oracle_ready,
            "python_oracle_heldout_quality": quality_ready,
            "zig_native_heldout_quality": native_ready,
            "five_seed_statistical_convergence": convergence_ready,
            "convergence_evidence_current": not convergence_evidence_errors,
            "artifact_fingerprints_consistent": fingerprints_ready,
            "release_adapter_backend_and_precision": release_adapter_ready,
            "required_hardware_matrix": hardware_ready,
            "required_head_opt_in": head_required_ready,
        },
        "policies": {
            "production_implementation": f"Zig GLiNER2 FP32 finetuning on the Zig {backend.upper()} kernels/runtime",
            "supported_training_precision": "FP32 only; BF16/FP16 require separate parity, quality, performance, and hardware-matrix qualification",
            "python_role": "correctness oracle and performance baseline only; not a deployed runtime dependency",
            "stochastic_training": "Fastino retains pinned upstream sampling, schema conditioning, model dropout, negative masking, and shuffle; Zig records its disabled sampling/model-dropout policy plus enabled epoch shuffle and negative masking; paired independent-seed held-out result parity is required and exact Python RNG-stream equality is not required",
            "unsupported_training_unicode": "fail closed with record/field/code-point context; this is a supported-input boundary, not a readiness blocker",
            "scoring_normalization": CANONICAL_NORMALIZATION,
            "independently_trained_tensor_equality": "diagnostic only; same-artifact round-trip remains strict",
        },
        "oracle": oracle,
        "normalization": CANONICAL_NORMALIZATION,
        "unicode_version": CANONICAL_UNICODE_VERSION,
        "release_adapter_fingerprint_sha256": release_adapter_fingerprint,
        "release_adapter_manifest": release_manifest,
        "convergence_evidence_errors": convergence_evidence_errors,
        "hardware_qualification_errors": hardware_qualification_errors,
        "hardware_qualification": hardware_qualification,
        "head_opt_in_ready": head_ready,
        "returncodes": {
            "default": default_rc,
            "head_opt_in": None if skip_head_opt_in else head_rc,
            "python_oracle_quality": quality_rc,
            "zig_native_quality": native_rc,
        },
        "paths": paths,
        "default_summary": default_summary,
        "head_opt_in_summary": head_summary,
        "python_oracle_heldout_quality": quality_report,
        "zig_native_heldout_quality": native_report,
        "convergence": convergence_report,
    }


def load(path: Path, *, summary: bool = False) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if summary:
        payload = payload.get("summary")
    return payload if isinstance(payload, dict) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--convergence-summary", type=Path, required=True)
    parser.add_argument("--release-adapter-dir", type=Path, required=True)
    parser.add_argument("--backend", choices=("metal", "cuda"), required=True)
    parser.add_argument("--hardware-qualification", type=Path)
    parser.add_argument("--default-rc", type=int, required=True)
    parser.add_argument("--head-rc", type=int, required=True)
    parser.add_argument("--quality-rc", type=int, required=True)
    parser.add_argument("--native-rc", type=int, required=True)
    parser.add_argument("--skip-head-opt-in", type=int, choices=(0, 1), required=True)
    parser.add_argument("--require-head-opt-in", type=int, choices=(0, 1), required=True)
    args = parser.parse_args()
    out_dir = args.out_dir.expanduser().resolve()
    default_path = out_dir / "default" / "perf_summary.json"
    head_path = out_dir / "head-mlp-opt-in" / "perf_summary.json"
    quality_path = out_dir / "heldout_quality.json"
    native_path = out_dir / "native_full_task_quality.json"
    convergence_path = args.convergence_summary.expanduser().resolve()
    release_adapter_dir = args.release_adapter_dir.expanduser().resolve()
    paths = {
        "default_summary": str(default_path),
        "head_opt_in_summary": None if args.skip_head_opt_in else str(head_path),
        "python_oracle_heldout_quality": str(quality_path),
        "zig_native_heldout_quality": str(native_path),
        "convergence_summary": str(convergence_path),
        "release_adapter_dir": str(release_adapter_dir),
        "cuda_hardware_qualification": (
            str(args.hardware_qualification.expanduser().resolve())
            if args.hardware_qualification is not None
            else None
        ),
    }
    convergence_report = load(convergence_path)
    convergence_evidence_errors = (
        verify_summary_evidence(convergence_report)
        if isinstance(convergence_report, dict)
        else ["convergence summary is missing or invalid"]
    )
    hardware_qualification = (
        load(args.hardware_qualification.expanduser().resolve())
        if args.hardware_qualification is not None
        else None
    )
    if args.backend == "cuda" and isinstance(hardware_qualification, dict):
        from summarize_gliner2_cuda_hardware import verify_summary

        hardware_qualification_errors = verify_summary(hardware_qualification, Path(__file__).resolve().parents[2])
    elif args.backend == "cuda":
        hardware_qualification_errors = ["CUDA hardware qualification matrix is missing"]
    else:
        hardware_qualification_errors = []
    try:
        release_adapter_fingerprint = adapter_bundle_fingerprint(release_adapter_dir)
    except (OSError, ValueError):
        release_adapter_fingerprint = None
    release_manifest = load(release_adapter_dir / "training_manifest.json")
    result = build_summary(
        backend=args.backend,
        default_rc=args.default_rc,
        head_rc=args.head_rc,
        quality_rc=args.quality_rc,
        native_rc=args.native_rc,
        skip_head_opt_in=bool(args.skip_head_opt_in),
        require_head_opt_in=bool(args.require_head_opt_in),
        default_summary=load(default_path, summary=True) if args.default_rc == 0 else None,
        head_summary=load(head_path, summary=True) if not args.skip_head_opt_in and args.head_rc == 0 else None,
        quality_report=load(quality_path) if args.quality_rc == 0 else None,
        native_report=load(native_path) if args.native_rc == 0 else None,
        convergence_report=convergence_report,
        convergence_evidence_errors=convergence_evidence_errors,
        hardware_qualification=hardware_qualification,
        hardware_qualification_errors=hardware_qualification_errors,
        release_adapter_fingerprint=release_adapter_fingerprint,
        release_manifest=release_manifest,
        paths=paths,
    )
    output = out_dir / "readiness_summary.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(f"readiness summary: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
