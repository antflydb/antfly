#!/usr/bin/env python3
"""Extract compact encoder token evidence from the completed pinned CPU report.

No encoder or tokenizer runs here. This preserves the recorded native/Python
validation evidence, including the direct-classifier and JointIE encoder paths
that bypassed the original boundary-core tensor hook. It is deliberately
labeled as derived benchmark evidence, not a fresh pretrained tensor capture.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import benchmark_cpu as benchmark
import oracle

REPORT_SHA256 = "fd6aa011026bcd6cef008eb46205cc09ffe82c8a83a3c0c1051064951a250776"
DRIVER_SHA256 = "277fd7f16371ff055be07142a9d32e10f54c4e4fa90aff6011c21995044b4b9d"
NATIVE_SHA256 = "defa2bc50b4204792b3c563cd61fc43c0a9d714153fb58d0fda3928c118d7394"


def extract(report_path: Path) -> dict:
    if oracle.sha256_file(report_path) != REPORT_SHA256:
        raise oracle.ContractError("token evidence requires the exact completed benchmark report")
    if oracle.sha256_file(oracle.HERE / "benchmark_cpu.py") != DRIVER_SHA256:
        raise oracle.ContractError("benchmark validation contract has changed")
    report = oracle.read_json(report_path)
    manifest = oracle.load_manifest()
    if (report.get("format_version") != 1 or report.get("status") != "complete"
            or report.get("parity_validated") is not True
            or report.get("performance_release_qualified") is not False
            or report.get("serving_qualified") is not False
            or report.get("driver_sha256") != DRIVER_SHA256
            or report.get("native_binary", {}).get("sha256") != NATIVE_SHA256
            or report.get("source", {}).get("commit") != manifest["upstream"]["commit"]
            or report.get("scope") != benchmark.SCOPE
            or report.get("timing_boundary") != benchmark.TIMING_BOUNDARY
            or report.get("threads") != 1):
        raise oracle.ContractError("completed benchmark profile is not the declared source")
    reports = report["models"]
    if len(reports) != 3 or {row.get("model") for row in reports} != {"small", "base", "multi"}:
        raise oracle.ContractError("token evidence requires exactly all three pinned variants")
    models = []
    for row in reports:
        variant = row["model"]
        pinned = manifest["models"][variant]
        files = {name: {key: spec[key] for key in ("sha256", "size_bytes")} for name, spec in pinned["files"].items()}
        bundle = row["model_bundle"]
        if bundle.get("model_id") != pinned["model_id"] or bundle.get("revision") != pinned["revision"] or bundle.get("files") != files:
            raise oracle.ContractError("token evidence model files differ from pinned source")
        fixture_path = benchmark.case_fixture(variant)
        fixture = oracle.read_json(fixture_path)
        for arm in ("native", "python"):
            benchmark.checked_ready(arm, row["workers"][arm], bundle, fixture_path, 1)
        if row["workers"]["python"].get("provenance", {}).get("runtime") != manifest["runtime"]:
            raise oracle.ContractError("token evidence Python dependency profile differs")
        cases = fixture["cases"]
        if len(cases) != 10 or len({case["id"] for case in cases}) != 10 or set(row["validation"]) != {case["id"] for case in cases}:
            raise oracle.ContractError("token evidence must cover exactly every canonical request")
        validation = {}
        for case in cases:
            evidence = row["validation"][case["id"]]
            if evidence.get("outputs_match_oracle") is not True or evidence.get("confidence_absolute_tolerance") != 0.0005:
                raise oracle.ContractError("token evidence lacks the recorded output parity gate")
            expected = benchmark.canonical_result(case["expected"])
            for arm in ("expected", "native", "python"):
                benchmark.require_equal(expected, evidence[arm])
            tokens = evidence["input_ids"]
            if not isinstance(tokens, list) or not 0 < len(tokens) <= oracle.MAX_ENCODED_TOKENS or any(type(token) is not int or not 0 <= token < 2**32 for token in tokens):
                raise oracle.ContractError("token evidence must be a bounded single encoded sequence")
            request_bytes = json.dumps({"text": case["text"], "schema": case["schema"]}, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
            token_bytes = b"".join(token.to_bytes(4, "little") for token in tokens)
            validation[case["id"]] = {"input_ids": tokens,
                                      "input_ids_u32_le_sha256": hashlib.sha256(token_bytes).hexdigest(),
                                      "canonical_request_sha256": hashlib.sha256(request_bytes).hexdigest()}
        models.append({"model": variant, "model_id": pinned["model_id"], "revision": pinned["revision"],
                       "model_files": files, "cases_sha256": oracle.sha256_file(fixture_path),
                       "reference_sha256": fixture["reference_sha256"],
                       "requests_sha256": fixture["requests_sha256"], "validation": validation})
    return {"format_version": 1, "scope": "completed_cpu_benchmark_encoder_token_evidence",
            "fresh_model_execution": False, "native_runtime_qualified": False,
            "source_commit": manifest["upstream"]["commit"], "runtime": manifest["runtime"],
            "report_sha256": REPORT_SHA256, "benchmark_driver_sha256": DRIVER_SHA256,
            "native_binary_sha256": NATIVE_SHA256, "generator_sha256": oracle.sha256_file(Path(__file__)),
            "models": models}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "token_evidence.json")
    args = parser.parse_args()
    result = extract(args.report)
    oracle.write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "sha256": oracle.sha256_file(args.output),
                      "models": len(result["models"]), "cases": sum(len(row["validation"]) for row in result["models"])}))


if __name__ == "__main__":
    main()
