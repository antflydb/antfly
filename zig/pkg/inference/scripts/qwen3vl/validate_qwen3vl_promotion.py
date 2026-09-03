#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Validate the complete, immutable Qwen3-VL Metal promotion campaign.

The validator is intentionally independent from the model runners. It accepts
only hash-pinned evidence produced from one clean runtime revision, one binary,
one Metal device family, and exact managed receipts. Missing, duplicated,
dirty, unmeasured, unhashed, or failed lanes keep release_ready false.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time
from typing import Any


MANIFEST_SCHEMA = "antfly.qwen3vl.promotion_manifest.v1"
EVIDENCE_SCHEMA = "antfly.qwen3vl.production_evidence.v1"
REPORT_SCHEMA = "antfly.qwen3vl.promotion_report.v1"

GENERATION_SCENARIOS = {
    "artifact_loader": (
        "empty_cache_pull",
        "interrupted_resume",
        "digest_tamper_rejected",
        "partial_publication_hidden",
        "wrong_family_rejected",
        "receipt_identity",
    ),
    "transformers_parity": (
        "templates_exact",
        "token_ids_exact",
        "mrope_exact",
        "prefill_logits_within_tolerance",
        "greedy_tokens_exact",
        "incremental_kv_exact",
    ),
    "single_image_2mp": (
        "resize_geometry_exact",
        "pixels_within_tolerance",
        "projector_within_tolerance",
        "deepstack_within_tolerance",
        "greedy_tokens_exact",
        "fallbacks_zero",
    ),
    "multi_image": (
        "marker_expansion_exact",
        "mrope_exact",
        "projector_within_tolerance",
        "deepstack_within_tolerance",
        "greedy_tokens_exact",
        "fallbacks_zero",
    ),
    "long_decode": (
        "greedy_tokens_exact",
        "incremental_mrope_exact",
        "kv_reuse_exact",
        "memory_bounded",
        "fallbacks_zero",
    ),
    "concurrency": (
        "request_isolation",
        "deterministic_outputs",
        "memory_bounded",
        "fallbacks_zero",
    ),
    "cancellation": (
        "cancellation_bounded",
        "resources_reclaimed",
        "subsequent_request_passes",
    ),
    "cache_lifecycle": (
        "load_unload",
        "cache_eviction",
        "resources_reclaimed",
        "subsequent_request_passes",
    ),
    "performance": (
        "paired_protocol",
        "output_parity",
        "timing_boundary_exact",
        "route_counters_exact",
        "no_major_regression",
    ),
    "soak": (
        "mixed_workload",
        "malformed_inputs_rejected",
        "readiness_contract",
        "liveness_independent",
        "memory_bounded",
        "fallbacks_zero",
    ),
}

RERANKER_SCENARIOS = {
    "artifact_loader": GENERATION_SCENARIOS["artifact_loader"],
    "text_parity": (
        "prompt_ids_exact",
        "active_row_exact",
        "calibrated_q8",
        "raw_logit_within_tolerance",
        "score_within_tolerance",
        "ranking_exact",
        "fallbacks_zero",
    ),
    "multimodal_parity": (
        "typed_image_api",
        "prompt_ids_exact",
        "mrope_exact",
        "visual_mask_exact",
        "calibrated_q8",
        "raw_logit_within_tolerance",
        "score_within_tolerance",
        "ranking_exact",
        "fallbacks_zero",
    ),
    "quantized_conversion": (
        "reproducible_conversion",
        "source_digest_pinned",
        "output_digest_pinned",
        "tool_digests_pinned",
        "tensor_mapping_exact",
        "semantic_classifier_head_exact",
        "classifier_head_f16",
        "decoder_q8_0",
        "projector_q8_0",
    ),
    "retrieval_quality": (
        "frozen_corpus",
        "bf16_oracle_scores",
        "ranking_metrics_within_tolerance",
        "calibration_metrics_within_tolerance",
        "ranking_exact",
    ),
    "http_api": (
        "typed_image_api",
        "response_schema_exact",
        "limits_enforced",
        "malformed_inputs_rejected",
        "readiness_contract",
        "fallbacks_zero",
    ),
    "long_context": (
        "truncation_exact",
        "assistant_suffix_retained",
        "score_within_tolerance",
        "ranking_exact",
        "memory_bounded",
    ),
    "concurrency": GENERATION_SCENARIOS["concurrency"],
    "cancellation": GENERATION_SCENARIOS["cancellation"],
    "cache_lifecycle": GENERATION_SCENARIOS["cache_lifecycle"],
    "performance": GENERATION_SCENARIOS["performance"],
    "soak": GENERATION_SCENARIOS["soak"],
}

MODEL_CONTRACTS = {
    "qwen3-vl-2b": {
        "quantization": "decoder_q4_k_m_projector_q8_0",
        "scenarios": GENERATION_SCENARIOS,
    },
    "qwen3-vl-4b": {
        "quantization": "decoder_q4_k_m_projector_q8_0",
        "scenarios": GENERATION_SCENARIOS,
    },
    "qwen3-vl-8b": {
        "quantization": "decoder_q4_k_m_projector_q8_0",
        "scenarios": GENERATION_SCENARIOS,
    },
    "qwen3-vl-reranker-2b": {
        "quantization": "decoder_q8_0_projector_q8_0_classifier_f16",
        "scenarios": RERANKER_SCENARIOS,
    },
}

HEX_40 = re.compile(r"[0-9a-f]{40}")
HEX_64 = re.compile(r"[0-9a-f]{64}")


class PromotionError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_object(path: Path) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise PromotionError(f"duplicate JSON key {key!r} in {path}")
            value[key] = item
        return value

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionError(f"invalid JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PromotionError(f"JSON root is not an object: {path}")
    return value


def write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


def required_matrix() -> set[tuple[str, str]]:
    return {
        (model, scenario)
        for model, contract in MODEL_CONTRACTS.items()
        for scenario in contract["scenarios"]
    }


def safe_regular_file(root: Path, raw: object, label: str) -> tuple[str, Path]:
    if not isinstance(raw, str) or not raw or "\\" in raw or ":" in raw or "\x00" in raw:
        raise PromotionError(f"unsafe {label} path: {raw!r}")
    relative = PurePosixPath(raw)
    if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
        raise PromotionError(f"unsafe {label} path: {raw!r}")
    path = root / raw
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise PromotionError(f"missing {label} {raw}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise PromotionError(f"{label} is not a regular file: {raw}")
    canonical = path.resolve(strict=True)
    if not canonical.is_relative_to(root):
        raise PromotionError(f"{label} escapes campaign root: {raw}")
    return raw, path


def require_digest(raw: object, label: str) -> str:
    if not isinstance(raw, str) or HEX_64.fullmatch(raw) is None:
        raise PromotionError(f"invalid SHA-256 for {label}")
    return raw


def require_number(metrics: dict[str, Any], name: str, minimum: float) -> float:
    value = metrics.get(name)
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < minimum
    ):
        raise PromotionError(f"metric {name} must be >= {minimum}, got {value!r}")
    return float(value)


def require_maximum(metrics: dict[str, Any], name: str, maximum: float) -> float:
    value = metrics.get(name)
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value > maximum
    ):
        raise PromotionError(f"metric {name} must be <= {maximum}, got {value!r}")
    return float(value)


def validate_scenario_metrics(model: str, scenario: str, metrics: dict[str, Any]) -> None:
    if scenario == "transformers_parity":
        require_number(metrics, "fixture_count", 6)
    elif scenario == "single_image_2mp":
        require_number(metrics, "source_pixels", 2_000_000)
        require_number(metrics, "metal_repeat_count", 2)
    elif scenario == "multi_image":
        require_number(metrics, "image_count", 2)
    elif scenario == "long_decode":
        require_number(metrics, "generated_tokens", 256)
    elif scenario == "long_context":
        require_number(metrics, "active_tokens", 8_192)
        require_number(metrics, "fixture_count", 8)
    elif scenario == "concurrency":
        require_number(metrics, "max_concurrency", 4)
        require_number(metrics, "completed_requests", 20)
    elif scenario == "cancellation":
        require_number(metrics, "cancelled_requests", 10)
        require_number(metrics, "post_cancel_requests", 10)
    elif scenario == "cache_lifecycle":
        require_number(metrics, "cycles", 10)
    elif scenario == "performance":
        require_number(metrics, "warmups", 1)
        require_number(metrics, "paired_trials", 5)
        regression = metrics.get("performance_regression_percent")
        if (
            not isinstance(regression, (int, float))
            or isinstance(regression, bool)
            or not math.isfinite(regression)
            or regression > 5.0
        ):
            raise PromotionError(
                "metric performance_regression_percent must be <= 5.0, "
                f"got {regression!r}"
            )
    elif scenario == "soak":
        require_number(metrics, "duration_seconds", 3_600)
        require_number(metrics, "completed_requests", 100)
    elif scenario == "text_parity":
        require_number(metrics, "fixture_count", 32)
        require_maximum(metrics, "max_score_abs", 0.03)
        require_maximum(metrics, "max_logit_abs", 0.10)
        require_number(metrics, "ranking_match_rate", 1.0)
    elif scenario == "multimodal_parity":
        require_number(metrics, "fixture_count", 12)
        require_number(metrics, "max_source_pixels", 2_000_000)
        require_number(metrics, "metal_repeat_count", 2)
        require_maximum(metrics, "max_score_abs", 0.03)
        require_maximum(metrics, "max_logit_abs", 0.10)
    elif scenario == "quantized_conversion":
        if metrics.get("quantization") != MODEL_CONTRACTS[model]["quantization"]:
            raise PromotionError("reranker conversion evidence is not the calibrated Q8 artifact")
        require_number(metrics, "independent_conversion_passes", 2)
    elif scenario == "retrieval_quality":
        require_number(metrics, "query_count", 100)
        require_number(metrics, "candidate_count", 1_000)
        require_number(metrics, "mean_top_10_overlap", 0.95)
        require_maximum(metrics, "ndcg_at_10_abs_delta", 0.01)
        require_maximum(metrics, "mrr_abs_delta", 0.01)
        require_maximum(metrics, "brier_score_abs_delta", 0.02)
    elif scenario == "http_api":
        require_number(metrics, "successful_requests", 20)
        require_number(metrics, "invalid_requests", 20)


def validate_gates(raw: object, label: str) -> int:
    if isinstance(raw, dict) and raw:
        gates = list(raw.items())
        for name, gate in gates:
            if not isinstance(name, str) or not name or not isinstance(gate, dict) or gate.get("pass") is not True:
                raise PromotionError(f"failed or malformed gate in {label}: {name!r}")
        return len(gates)
    if isinstance(raw, list) and raw:
        for gate in raw:
            if not isinstance(gate, dict) or not isinstance(gate.get("name"), str) or gate.get("pass") is not True:
                raise PromotionError(f"failed or malformed gate in {label}")
        return len(raw)
    raise PromotionError(f"{label} has no gates")


def validate_resources(raw: object, label: str) -> None:
    if not isinstance(raw, dict):
        raise PromotionError(f"{label} has no resource measurements")
    require_number(raw, "sample_count", 1)
    swap_growth = raw.get("swapout_growth_mib")
    if not isinstance(swap_growth, (int, float)) or isinstance(swap_growth, bool) or swap_growth != 0:
        raise PromotionError(f"{label} observed non-zero or invalid swap growth")
    violations = raw.get("threshold_violations")
    if violations != []:
        raise PromotionError(f"{label} has resource threshold violations")


def validate_artifact_refs(
    campaign_root: Path,
    report_path: str,
    raw: object,
) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise PromotionError(f"evidence {report_path} has no immutable artifacts")
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise PromotionError(f"malformed artifact reference in {report_path}")
        relative, path = safe_regular_file(campaign_root, item.get("path"), "evidence artifact")
        if relative == report_path:
            raise PromotionError(f"evidence report cannot cite itself as its only proof: {report_path}")
        if relative in seen:
            raise PromotionError(f"duplicate artifact reference in {report_path}: {relative}")
        seen.add(relative)
        expected = require_digest(item.get("sha256"), relative)
        actual = sha256_file(path)
        if actual != expected:
            raise PromotionError(
                f"evidence artifact SHA-256 mismatch for {relative}: expected {expected}, got {actual}"
            )
        validated.append({"path": relative, "sha256": actual, "size": path.stat().st_size})
    return validated


def validate_evidence(
    campaign_root: Path,
    entry: dict[str, Any],
    target: dict[str, str],
    model_receipts: dict[str, str],
) -> dict[str, Any]:
    model = entry.get("model")
    scenario = entry.get("scenario")
    label = f"{model}/{scenario}"
    relative, path = safe_regular_file(campaign_root, entry.get("report"), "evidence report")
    expected_sha = require_digest(entry.get("sha256"), relative)
    actual_sha = sha256_file(path)
    if actual_sha != expected_sha:
        raise PromotionError(
            f"evidence report SHA-256 mismatch for {relative}: expected {expected_sha}, got {actual_sha}"
        )
    evidence = load_object(path)
    if evidence.get("schema") != EVIDENCE_SCHEMA or evidence.get("pass") is not True:
        raise PromotionError(f"evidence is not a passing {EVIDENCE_SCHEMA} report: {relative}")
    if evidence.get("release_ready") is not False:
        raise PromotionError(f"scenario evidence must not self-promote: {relative}")
    if evidence.get("model") != model or evidence.get("scenario") != scenario:
        raise PromotionError(f"evidence identity mismatch for {label}")

    runtime = evidence.get("runtime_build")
    if not isinstance(runtime, dict):
        raise PromotionError(f"missing runtime provenance in {label}")
    for field in ("git_head", "binary_sha256", "backend", "device_family"):
        if runtime.get(field) != target[field]:
            raise PromotionError(f"runtime {field} mismatch in {label}")
    if runtime.get("git_dirty") is not False:
        raise PromotionError(f"dirty or unknown runtime build in {label}")

    artifact = evidence.get("model_artifact")
    if not isinstance(artifact, dict):
        raise PromotionError(f"missing model artifact identity in {label}")
    if artifact.get("managed_receipt_sha256") != model_receipts[model]:
        raise PromotionError(f"managed receipt mismatch in {label}")
    if artifact.get("quantization") != MODEL_CONTRACTS[model]["quantization"]:
        raise PromotionError(f"quantization mismatch in {label}")

    checks = evidence.get("checks")
    if not isinstance(checks, dict):
        raise PromotionError(f"missing checks in {label}")
    for check in MODEL_CONTRACTS[model]["scenarios"][scenario]:
        if checks.get(check) is not True:
            raise PromotionError(f"required check {check} did not pass in {label}")

    metrics = evidence.get("metrics")
    if not isinstance(metrics, dict):
        raise PromotionError(f"missing metrics in {label}")
    validate_scenario_metrics(model, scenario, metrics)
    gate_count = validate_gates(evidence.get("gates"), label)
    validate_resources(evidence.get("resources"), label)
    artifacts = validate_artifact_refs(campaign_root, relative, evidence.get("artifacts"))
    return {
        "model": model,
        "scenario": scenario,
        "report": relative,
        "report_sha256": actual_sha,
        "report_size": path.stat().st_size,
        "gate_count": gate_count,
        "artifacts": artifacts,
    }


def validate_manifest(path: Path) -> dict[str, Any]:
    manifest_path = path.resolve(strict=True)
    campaign_root = manifest_path.parent
    manifest = load_object(manifest_path)
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise PromotionError(f"unexpected manifest schema: {manifest.get('schema')!r}")

    target_raw = manifest.get("target")
    if not isinstance(target_raw, dict):
        raise PromotionError("promotion manifest has no target")
    git_head = target_raw.get("git_head")
    if not isinstance(git_head, str) or HEX_40.fullmatch(git_head) is None:
        raise PromotionError("target git_head must be a full lowercase commit hash")
    target = {
        "git_head": git_head,
        "binary_sha256": require_digest(target_raw.get("binary_sha256"), "target binary"),
        "backend": target_raw.get("backend"),
        "device_family": target_raw.get("device_family"),
    }
    if target["backend"] != "metal":
        raise PromotionError("Qwen3-VL promotion currently requires backend=metal")
    if not isinstance(target["device_family"], str) or not target["device_family"].strip():
        raise PromotionError("target device_family must be non-empty")
    binary_relative, binary_path = safe_regular_file(
        campaign_root, target_raw.get("binary"), "target runtime binary"
    )
    if binary_path.stat().st_mode & stat.S_IXUSR == 0:
        raise PromotionError("target runtime binary is not owner-executable")
    actual_binary_sha = sha256_file(binary_path)
    if actual_binary_sha != target["binary_sha256"]:
        raise PromotionError(
            "target runtime binary SHA-256 mismatch: "
            f"expected {target['binary_sha256']}, got {actual_binary_sha}"
        )
    target["binary"] = binary_relative

    raw_models = manifest.get("models")
    if not isinstance(raw_models, dict) or set(raw_models) != set(MODEL_CONTRACTS):
        raise PromotionError(
            "promotion manifest must pin exactly the required model set: "
            f"{sorted(MODEL_CONTRACTS)}"
        )
    model_receipts: dict[str, str] = {}
    validated_receipts: dict[str, dict[str, Any]] = {}
    for model, contract in MODEL_CONTRACTS.items():
        item = raw_models[model]
        if not isinstance(item, dict):
            raise PromotionError(f"malformed model target: {model}")
        model_receipts[model] = require_digest(
            item.get("managed_receipt_sha256"), f"{model} managed receipt"
        )
        receipt_relative, receipt_path = safe_regular_file(
            campaign_root, item.get("managed_receipt"), f"{model} managed receipt"
        )
        actual_receipt_sha = sha256_file(receipt_path)
        if actual_receipt_sha != model_receipts[model]:
            raise PromotionError(
                f"managed receipt SHA-256 mismatch for {model}: "
                f"expected {model_receipts[model]}, got {actual_receipt_sha}"
            )
        receipt = load_object(receipt_path)
        if (
            receipt.get("version") != 2
            or not isinstance(receipt.get("source"), dict)
            or not isinstance(receipt.get("artifacts"), list)
            or not receipt["artifacts"]
        ):
            raise PromotionError(f"malformed version-2 managed receipt for {model}")
        validated_receipts[model] = {
            "path": receipt_relative,
            "sha256": actual_receipt_sha,
            "size": receipt_path.stat().st_size,
            "source": receipt["source"],
        }
        if item.get("quantization") != contract["quantization"]:
            raise PromotionError(f"manifest quantization mismatch for {model}")

    raw_entries = manifest.get("evidence")
    if not isinstance(raw_entries, list):
        raise PromotionError("promotion manifest evidence must be an array")
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in raw_entries:
        if not isinstance(entry, dict):
            raise PromotionError("malformed promotion evidence entry")
        key = (entry.get("model"), entry.get("scenario"))
        if key in entries:
            raise PromotionError(f"duplicate promotion evidence lane: {key[0]}/{key[1]}")
        entries[key] = entry
    required = required_matrix()
    actual = set(entries)
    if actual != required:
        missing = sorted(f"{model}/{scenario}" for model, scenario in required - actual)
        unexpected = sorted(f"{model}/{scenario}" for model, scenario in actual - required)
        raise PromotionError(f"incomplete promotion matrix: missing={missing} unexpected={unexpected}")

    validated = [
        validate_evidence(campaign_root, entries[key], target, model_receipts)
        for key in sorted(required)
    ]
    return {
        "schema": REPORT_SCHEMA,
        "pass": True,
        "release_ready": True,
        "created_unix_seconds": int(time.time()),
        "source_manifest": str(manifest_path),
        "source_manifest_sha256": sha256_file(manifest_path),
        "target": target,
        "models": raw_models,
        "validated_managed_receipts": validated_receipts,
        "required_lane_count": len(required),
        "validated_lane_count": len(validated),
        "evidence": validated,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        report = validate_manifest(args.manifest)
    except (OSError, PromotionError, ValueError) as exc:
        report = {
            "schema": REPORT_SCHEMA,
            "pass": False,
            "release_ready": False,
            "created_unix_seconds": int(time.time()),
            "source_manifest": str(args.manifest),
            "failure": str(exc),
        }
    write_json_atomic(args.output.resolve(), report)
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
