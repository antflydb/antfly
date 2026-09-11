#!/usr/bin/env python3
"""Bounded converted-bundle diagnostics and strict same-bundle backend parity.

This is a conversion/runtime check on ten curated requests, not a held-out
quality evaluation or backend promotion receipt. Changed decisions are saved
as evidence; they are not hidden by a wider confidence tolerance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any

import benchmark_cpu as bench
import oracle
import extract_token_evidence as token_contract

REPORT_VERSION = 2
REPORT_SCOPE = "converted_bundle_backend_diagnostic"
MATH_POLICY = "strict_f32_activations_v1"
CONFIDENCE_TOLERANCE = 5e-4


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def compare(expected: Any, actual: Any, path: str = "output") -> dict[str, Any]:
    """Report exact public decisions and confidence error only where aligned."""
    differences: list[str] = []
    errors: list[float] = []

    def validate(value: Any, at: str) -> None:
        # Validate unmatched branches too: a changed selection cannot conceal
        # a NaN/boolean confidence. Optional record confidence may be null.
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "confidence" and item is not None:
                    if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item):
                        raise bench.BenchmarkError(f"{at}.{key}: invalid confidence")
                validate(item, f"{at}.{key}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                validate(item, f"{at}[{index}]")

    def identity(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: identity(item) for key, item in value.items() if key != "confidence"}
        if isinstance(value, list):
            return [identity(item) for item in value]
        return value

    def walk(left: Any, right: Any, at: str, aligned: bool = True) -> None:
        if isinstance(left, dict):
            if not isinstance(right, dict) or left.keys() != right.keys():
                differences.append(at + ": keys")
                return
            if "confidence" in left:
                aligned = aligned and identity(left) == identity(right)
            for key in left:
                walk(left[key], right[key], f"{at}.{key}", aligned)
        elif isinstance(left, list):
            if not isinstance(right, list) or len(left) != len(right):
                differences.append(at + ": count")
                return
            for index, (a, b) in enumerate(zip(left, right)):
                walk(a, b, f"{at}[{index}]", aligned)
        elif at.endswith(".confidence") and left is not None:
            if isinstance(right, bool) or not isinstance(right, (int, float)) or not math.isfinite(right):
                raise bench.BenchmarkError(f"{at}: invalid confidence")
            if aligned:
                errors.append(abs(left - right))
        elif type(left) is not type(right) or left != right:
            differences.append(at + ": value")

    validate(expected, path)
    validate(actual, path)
    walk(expected, actual, path)
    return {"decisions_equal": not differences, "decision_differences": differences,
            "aligned_confidence_count": len(errors),
            "max_aligned_confidence_absolute_error": max(errors, default=0),
            "mean_aligned_confidence_absolute_error": sum(errors) / len(errors) if errors else 0,
            "fp32_reference_tolerance_pass": not differences and max(errors, default=0) <= CONFIDENCE_TOLERANCE}


def backend_result(output: dict[str, Any]) -> dict[str, Any]:
    """All presented decisions, including metadata absent from FP32 fixtures.

    Tensor routes, learned seed IDs and UTF-8 bookkeeping are internal. Solver
    work counters/utility are preserved separately, not compared as decisions.
    """
    result = bench.canonical_result(output)

    def value_metadata(target: dict[str, Any], original: dict[str, Any]) -> None:
        target["derived"] = original.get("derived", False)
        for target_group in target["attributes"]:
            matches = [group for group in original.get("attributes", []) if group["name"] == target_group["name"]]
            if len(matches) != 1:
                raise bench.BenchmarkError("duplicate attribute group")
            target_group["multi_label"] = matches[0]["multi_label"]

    for target, original in zip(result["entities"], output["entities"]):
        target["dtype"] = original["dtype"]
        for value, raw in zip(target["values"], original["values"]):
            value_metadata(value, raw)
    for target, original in zip(result["classifications"], output["classifications"]):
        target["multi_label"] = original["multi_label"]
    for target_group, original_group in zip(result["structures"], output["structures"]):
        for target, original in zip(target_group["instances"], original_group["instances"]):
            target["confidence"] = original["confidence"]
            anchor = original["anchor"]
            target["anchor"] = None if anchor is None else {key: anchor[key] for key in ("start", "end", "unit")}
            for field, raw_field in zip(target["fields"], original["fields"]):
                field["dtype"] = raw_field["dtype"]
                for value, raw in zip(field["values"], raw_field["values"]):
                    value_metadata(value, raw)
    for target, original in zip(result["relations"], output["relations"]):
        value_metadata(target["head"], original["head"])
        value_metadata(target["tail"], original["tail"])
    result["solver_status"] = {
        name: None if output.get(name) is None else {key: output[name][key] for key in ("status", "exhausted")}
        for name in ("classification_solver", "joint_solver", "record_solver")
    }
    result["long_document"] = output.get("long_document")
    return result


def compare_backends(expected: Any, actual: Any) -> dict[str, Any]:
    result = compare(expected, actual)
    result["parity_pass"] = result.pop("fp32_reference_tolerance_pass")
    result["confidence_absolute_tolerance"] = CONFIDENCE_TOLERANCE
    return result


def is_digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def validate_ready(ready: Any, backend: str, receipt: dict[str, Any], fixture_sha256: str) -> None:
    if (not isinstance(receipt, dict) or receipt.get("precision") not in ("fp32", "fp16_encoder", "q8_0", "q4_0", "q4_k") or
            not isinstance(ready, dict) or ready.get("event") != "ready" or
            ready.get("scope") != "converted_bundle_diagnostic" or ready.get("backend") != backend or
            ready.get("qualification") is not False or ready.get("receipt") != receipt or
            ready.get("fixture_sha256") != fixture_sha256 or ready.get("math_policy") != MATH_POLICY or
            ready.get("weight_precision") != receipt.get("precision") or
            any(ready.get(key) != "f32" for key in ("activation_precision", "accumulation_precision", "head_precision"))):
        raise bench.BenchmarkError("runner identity or strict arithmetic policy mismatch")


def validate_native_reference(report: Any, token_sha256: str, driver_sha256: str) -> None:
    """Old CPU reports cannot attest the corrected activation policy."""
    if (not isinstance(report, dict) or report.get("format_version") != REPORT_VERSION or
            report.get("scope") != REPORT_SCOPE or report.get("status") != "complete" or
            report.get("qualification") is not False or report.get("backend") != "native" or
            report.get("math_policy") != MATH_POLICY or report.get("source_commit") != oracle.UPSTREAM_COMMIT or
            report.get("token_reference_report_sha256") != token_sha256 or report.get("driver_sha256") != driver_sha256 or
            not is_digest(report.get("binary_sha256")) or not isinstance(report.get("bundles"), list) or
            not 1 <= len(report["bundles"]) <= 12):
        raise bench.BenchmarkError("native reference is not a completed matching strict backend report")
    identities = set()
    for entry in report["bundles"]:
        if (not isinstance(entry, dict) or entry.get("status") != "complete" or entry.get("backend") != "native" or
                entry.get("qualification") is not False or entry.get("math_policy") != MATH_POLICY or
                not is_digest(entry.get("receipt_sha256")) or not is_digest(entry.get("fixture_sha256")) or
                not isinstance(entry.get("cases"), list) or len(entry["cases"]) != 10):
            raise bench.BenchmarkError("native reference has an incomplete or unbound bundle")
        identity = (entry["receipt_sha256"], entry["fixture_sha256"])
        if identity in identities:
            raise bench.BenchmarkError("native reference has duplicate bundle identity")
        identities.add(identity)
        ready = entry.get("ready", {})
        receipt = ready.get("receipt", {})
        if entry.get("variant") != receipt.get("backbone") or entry.get("precision") != receipt.get("precision"):
            raise bench.BenchmarkError("native reference model/profile differs")
        validate_ready(ready, "native", receipt, entry["fixture_sha256"])


def matching_native_bundle(report: dict[str, Any], receipt: dict[str, Any], receipt_sha256: str,
                           fixture_sha256: str, fixture: dict[str, Any], tokens: dict[str, list[int]]) -> dict[str, Any]:
    matching = [entry for entry in report["bundles"] if entry["receipt_sha256"] == receipt_sha256 and entry["fixture_sha256"] == fixture_sha256]
    if len(matching) != 1:
        raise bench.BenchmarkError("no exact same-bundle native reference")
    native = matching[0]
    # Comparing the entire receipt binds both original source and converted
    # output tensor/sidecar hashes, architecture policy and precision.
    validate_ready(native["ready"], "native", receipt, fixture_sha256)
    if [row.get("case_id") for row in native["cases"]] != [case["id"] for case in fixture["cases"]]:
        raise bench.BenchmarkError("native reference cases are missing, duplicate or reordered")
    for case, row in zip(fixture["cases"], native["cases"]):
        expected = bench.canonical_result(case["expected"])
        if (row.get("token_ids_equal") is not True or row.get("input_ids") != tokens[case["id"]] or
                row.get("expected") != expected or "backend_output" not in row or
                row.get("source_fp32") != compare(expected, row.get("actual"))):
            raise bench.BenchmarkError("native reference fixture, tokens or quality evidence differs")
        if bench.canonical_result(row["backend_output"]) != row["actual"]:
            raise bench.BenchmarkError("native reference source and backend outputs differ")
        # Confidence validation also visits unmatched branches.
        compare_backends(row["backend_output"], row["backend_output"])
    return native


def run_one(binary: Path, directory: Path, output: Path, token_report: dict[str, Any], timeout: int, max_rss_mib: int,
            backend: str = "native", native_reference: dict[str, Any] | None = None) -> dict[str, Any]:
    if backend not in ("native", "metal") or (backend == "metal") != (native_reference is not None):
        raise bench.BenchmarkError("Metal requires an exact native reference; native execution does not consume one")
    receipt_path = directory / "antfly_inference_bundle.json"
    receipt = oracle.read_json(receipt_path)
    if receipt["family"] != "gliner_boundary_bundle/v1" or receipt["version"] != 1:
        raise bench.BenchmarkError("unsupported bundle receipt")
    variant, precision = receipt["backbone"], receipt["precision"]
    if variant not in ("small", "base", "multi"):
        raise bench.BenchmarkError("unsupported model variant")
    fixture_path = bench.case_fixture(variant)
    fixture = oracle.read_json(fixture_path)
    matching = [entry for entry in token_report["models"] if entry["model"] == variant]
    if len(matching) != 1:
        raise bench.BenchmarkError("missing token evidence model")
    compact = token_report.get("scope") == "completed_cpu_benchmark_encoder_token_evidence"
    files = matching[0]["model_files"] if compact else matching[0]["model_bundle"]["files"]
    if files != fixture["model_files"]:
        raise bench.BenchmarkError("token report and source artifacts differ")
    validation = matching[0]["validation"]
    tokens = {name: case["input_ids"] for name, case in validation.items() if compact or case["outputs_match_oracle"] is True}
    if set(tokens) != {case["id"] for case in fixture["cases"]} or len(fixture["cases"]) != 10:
        raise bench.BenchmarkError("token evidence case coverage differs")
    for name, ids in tokens.items():
        if ids and isinstance(ids[0], list):
            if len(ids) != 1:
                raise bench.BenchmarkError("fixture has an unexpected physical batch")
            ids = ids[0]
            tokens[name] = ids
        if (not isinstance(ids, list) or not 0 < len(ids) <= oracle.MAX_ENCODED_TOKENS or
                any(type(token) is not int or not 0 <= token < 2**32 for token in ids)):
            raise bench.BenchmarkError("invalid bounded token sequence")
    for case in fixture["cases"]:
        if compact:
            encoded = json.dumps({"text": case["text"], "schema": case["schema"]}, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
            captured = validation[case["id"]]
            token_bytes = b"".join(token.to_bytes(4, "little") for token in captured["input_ids"])
            if (sha256(fixture_path) != matching[0]["cases_sha256"] or
                    hashlib.sha256(encoded).hexdigest() != captured["canonical_request_sha256"] or
                    hashlib.sha256(token_bytes).hexdigest() != captured["input_ids_u32_le_sha256"]):
                raise bench.BenchmarkError("compact token evidence request differs")
        else:
            bench.require_equal(bench.canonical_result(case["expected"]), validation[case["id"]]["expected"])
    source_pins = {pin["path"]: {key: pin[key] for key in ("size_bytes", "sha256")} for pin in receipt["source_files"]}
    if len(receipt["source_files"]) != 5 or len(source_pins) != 5 or source_pins != fixture["model_files"]:
        raise bench.BenchmarkError("fixture and source artifacts differ")
    receipt_digest, fixture_digest = sha256(receipt_path), sha256(fixture_path)
    native = None if native_reference is None else matching_native_bundle(native_reference, receipt, receipt_digest, fixture_digest, fixture, tokens)
    run_dir = output / f"{variant}-{precision}"
    run_dir.mkdir()
    guard = bench.ResourceGuard(max_rss_mib * 1024 * 1024)
    env = os.environ.copy()
    env.update({name: "1" for name in bench.THREAD_ENV})
    worker = bench.Worker(backend, [str(binary), "--model-dir", str(directory), "--fixture", str(fixture_path), "--backend", backend], env, run_dir, guard)
    report: dict[str, Any] = {"variant": variant, "precision": precision, "bundle": str(directory),
                              "backend": backend, "math_policy": MATH_POLICY,
                              "receipt_sha256": receipt_digest, "fixture_sha256": fixture_digest,
                              "qualification": False, "cases": [], "status": "incomplete"}
    try:
        ready = worker.receive(timeout)
        validate_ready(ready, backend, receipt, report["fixture_sha256"])
        report["ready"] = ready
        for index, case in enumerate(fixture["cases"]):
            actual = worker.receive(timeout)
            if actual.get("event") != "result" or actual.get("case_id") != case["id"]:
                raise bench.BenchmarkError("missing, duplicate or reordered case")
            wanted_tokens = tokens[case["id"]]
            if actual["input_ids"] != wanted_tokens:
                raise bench.BenchmarkError("token IDs differ from pinned reference")
            expected = bench.canonical_result(case["expected"])
            result = bench.canonical_result(actual["output"])
            presented = backend_result(actual["output"])
            compare_backends(presented, presented)
            case_report = {"case_id": case["id"], "token_ids_equal": True, "input_ids": wanted_tokens,
                           "source_fp32": compare(expected, result), "expected": expected, "actual": result,
                           "backend_output": presented, "solver_diagnostics": {key: actual["output"].get(key) for key in ("classification_solver", "joint_solver", "record_solver")}}
            if native is not None:
                case_report["same_bundle_native"] = compare_backends(native["cases"][index]["backend_output"], presented)
            report["cases"].append(case_report)
        done = worker.receive(timeout)
        if done != {"event": "complete", "cases": len(fixture["cases"]), "qualification": False}:
            raise bench.BenchmarkError("runner did not verify final artifacts")
        deadline = time.monotonic() + 5
        while worker.process.poll() is None:
            guard.check()
            if time.monotonic() >= deadline:
                raise bench.BenchmarkError("runner failed to exit after completion")
            time.sleep(0.05)
        if worker.process.returncode != 0 or worker.buffer or worker.process.stdout.read(1):
            raise bench.BenchmarkError("runner failed or emitted unrecognized output")
        if sha256(receipt_path) != report["receipt_sha256"] or sha256(fixture_path) != report["fixture_sha256"]:
            raise bench.BenchmarkError("inputs changed during diagnostic")
        report["status"] = "complete"
        report["source_fp32"] = {
            "decisions_equal_cases": sum(case["source_fp32"]["decisions_equal"] for case in report["cases"]),
            "confidence_tolerance_pass_cases": sum(case["source_fp32"]["fp32_reference_tolerance_pass"] for case in report["cases"]),
            "confidence_absolute_tolerance": CONFIDENCE_TOLERANCE,
            "quality_qualified": False,
        }
        if native is not None:
            passing = sum(case["same_bundle_native"]["parity_pass"] for case in report["cases"])
            report["same_bundle_native"] = {"parity_pass_cases": passing, "cases": len(fixture["cases"]),
                                           "parity_pass": passing == len(fixture["cases"]),
                                           "confidence_absolute_tolerance": CONFIDENCE_TOLERANCE}
    finally:
        worker.close()
        report["peak_observed_rss_bytes"] = guard.peak_rss_bytes
        oracle.write_json(run_dir / "report.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--backend", choices=("native", "metal"), default="native")
    parser.add_argument("--bundle", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--token-reference-report", type=Path, default=oracle.FIXTURES / "token_evidence.json",
                        help="Pinned compact token evidence or its completed source benchmark report")
    parser.add_argument("--native-reference-report", type=Path,
                        help="Completed version-2 strict native report for the exact same bundles; required for Metal")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--max-rss-mib", type=int, default=6144)
    args = parser.parse_args()
    if not 1 <= len(args.bundle) <= 12 or not 10 <= args.timeout_seconds <= 300 or not 256 <= args.max_rss_mib <= 8192:
        parser.error("invalid resource limits")
    if (args.backend == "metal") != (args.native_reference_report is not None):
        parser.error("--native-reference-report is required only with --backend metal")
    binary = args.binary.resolve(strict=True)
    bundles = [path.resolve(strict=True) for path in args.bundle]
    if len(set(bundles)) != len(bundles):
        parser.error("duplicate bundle")
    token_report = oracle.read_json(args.token_reference_report)
    if token_report.get("scope") == "completed_cpu_benchmark_encoder_token_evidence":
        if (token_report.get("format_version") != 1 or token_report.get("source_commit") != oracle.UPSTREAM_COMMIT or
                token_report.get("report_sha256") != token_contract.REPORT_SHA256 or
                token_report.get("generator_sha256") != sha256(Path(token_contract.__file__))):
            raise bench.BenchmarkError("compact token evidence provenance differs")
    elif (sha256(args.token_reference_report) != token_contract.REPORT_SHA256 or token_report.get("status") != "complete" or
          token_report.get("parity_validated") is not True or token_report.get("source", {}).get("commit") != oracle.UPSTREAM_COMMIT):
        raise bench.BenchmarkError("token reference is not the completed pinned parity run")
    token_sha256 = sha256(args.token_reference_report)
    driver_sha256 = sha256(Path(__file__))
    native = None
    native_sha256 = None
    if args.native_reference_report is not None:
        if not 0 < args.native_reference_report.stat().st_size <= 64 * 1024 * 1024:
            raise bench.BenchmarkError("native reference exceeds byte limit")
        native_sha256 = sha256(args.native_reference_report)
        native = oracle.read_json(args.native_reference_report)
        validate_native_reference(native, token_sha256, driver_sha256)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    before = sha256(binary)
    report: dict[str, Any] = {"format_version": REPORT_VERSION, "scope": REPORT_SCOPE, "qualification": False,
                              "source_commit": oracle.UPSTREAM_COMMIT, "backend": args.backend, "math_policy": MATH_POLICY,
                              "driver_sha256": driver_sha256,
                              "binary": str(binary), "binary_sha256": before,
                              "token_reference_report_sha256": token_sha256,
                              "status": "incomplete", "bundles": []}
    if native is not None:
        report["native_reference"] = {"report_sha256": native_sha256, "binary_sha256": native["binary_sha256"],
                                      "driver_sha256": native["driver_sha256"]}
    try:
        for directory in bundles:
            result = run_one(binary, directory, args.output_dir, token_report, args.timeout_seconds, args.max_rss_mib,
                             args.backend, native)
            report["bundles"].append(result)
            quality = result["source_fp32"]
            message = f"{args.backend} {result['variant']}/{result['precision']}: {quality['decisions_equal_cases']}/10 source-FP32 decisions; {quality['confidence_tolerance_pass_cases']}/10 source-FP32 confidence tolerance"
            if native is not None:
                message += f"; {result['same_bundle_native']['parity_pass_cases']}/10 same-bundle native parity"
            print(message, flush=True)
        if sha256(binary) != before or sha256(Path(__file__)) != driver_sha256 or sha256(args.token_reference_report) != token_sha256:
            raise bench.BenchmarkError("runner or diagnostic inputs changed during execution")
        if native is not None and sha256(args.native_reference_report) != native_sha256:
            raise bench.BenchmarkError("native reference changed during execution")
        if native is not None:
            report["same_bundle_parity_pass"] = all(entry["same_bundle_native"]["parity_pass"] for entry in report["bundles"])
        report["status"] = "complete"
    finally:
        oracle.write_json(args.output_dir / "report.json", report)
    if native is not None and report["same_bundle_parity_pass"] is not True:
        raise bench.BenchmarkError("same-bundle backend parity failed; complete decision and confidence evidence was retained")


if __name__ == "__main__":
    main()
