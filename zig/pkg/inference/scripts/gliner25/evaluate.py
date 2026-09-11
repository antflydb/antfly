#!/usr/bin/env python3
"""Bounded CrossNER AI held-out execution; qualification always remains false.

The driver audits corpus/gold separately. Disposable model workers receive
only a blinded fixture. One backend/model is loaded per invocation. No test
labels select schemas, thresholds, documents, or candidate types.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time
from typing import Any

import benchmark_cpu as bench
import check_bundles as comparison
import evaluation_contract as evaluation
import oracle
import prepare_crossner_ai as adapter

SCOPE = "gliner25_heldout_execution/v1"
WORKER_SCOPE = "gliner25_blinded_execution/v1"
POLICY = {"max_words": 128, "max_encoded_tokens": 512, "max_text_bytes": 1024 * 1024,
          "max_queries": 64, "timeout_ms": 120000}
MAX_CASES = 1024
MAX_FIXTURE_BYTES = 8 * 1024 * 1024
REQUEST_OPTIONS = {"threshold": 0.5, "overlap": "flat", "best_effort": False}


def contract_files() -> dict[str, str]:
    return {Path(module.__file__).name: oracle.sha256_file(Path(module.__file__))
            for module in (sys.modules[__name__], bench, comparison, evaluation, oracle, adapter)}


def source_identity(variant: str) -> dict[str, Any]:
    model = oracle.load_manifest()["models"][variant]
    return {"model_id": model["model_id"], "revision": model["revision"],
            "source_files": [{"path": name, "size_bytes": pin["size_bytes"], "sha256": pin["sha256"]}
                             for name, pin in sorted(model["files"].items())]}


def validate_fixture(value: Any) -> None:
    evaluation.checked(isinstance(value, dict) and set(value) == {
        "format_version", "scope", "qualification", "source_commit", "model", "source_files", "lock_sha256",
        "prepared_sha256", "requests_sha256", "adapter_sha256", "harness_sha256", "policy", "cases"}, "execution fixture fields differ")
    evaluation.checked(value["format_version"] == 1 and value["scope"] == WORKER_SCOPE and value["qualification"] is False and
                       value["source_commit"] == oracle.UPSTREAM_COMMIT and value["model"] in ("small", "base", "multi") and
                       value["source_files"] == source_identity(value["model"])["source_files"] and value["policy"] == POLICY,
                       "execution fixture identity or limits differ")
    for key in ("lock_sha256", "prepared_sha256", "requests_sha256", "adapter_sha256", "harness_sha256"):
        evaluation.checked(evaluation.is_digest(value[key]), "execution fixture digest is invalid")
    evaluation.checked(value["adapter_sha256"] == oracle.sha256_file(Path(adapter.__file__)) and
                       value["harness_sha256"] == oracle.sha256_file(Path(evaluation.__file__)), "fixture adaptation differs")
    cases = value["cases"]
    evaluation.checked(isinstance(cases, list) and 0 < len(cases) <= MAX_CASES, "execution case budget exceeded")
    ids = set()
    schema = {"entities": oracle.read_json(adapter.MANIFEST)["entity_types"]}
    for case in cases:
        evaluation.checked(isinstance(case, dict) and set(case) == {"id", "request_sha256", "text", "schema", "options", "offset_unit"}, "blinded case fields differ")
        evaluation.checked(evaluation.is_digest(case["id"]) and case["id"] not in ids and isinstance(case["text"], str) and
                           case["schema"] == schema and case["options"] == REQUEST_OPTIONS and case["offset_unit"] == "utf8_bytes", "case schema/options/identity differs")
        ids.add(case["id"])
        request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
        evaluation.checked(case["request_sha256"] == evaluation.digest(evaluation.encoded(request)), "blinded case content differs")


def prepare_fixture(lock_path: Path, prepared: Path, variant: str) -> dict[str, Any]:
    admitted = evaluation.audit(lock_path)
    lock = admitted["lock"]
    receipt = oracle.read_json(prepared / "prepared.json")
    evaluation.checked(lock["dataset"]["id"] == "crossner_ai" and lock["dataset"]["source_manifest_sha256"] == oracle.sha256_file(adapter.MANIFEST), "unsupported or changed corpus adapter")
    evaluation.checked(receipt.get("scope") == "gliner25_blinded_evaluation/v1" and receipt.get("status") == "complete" and receipt.get("qualification") is False and
                       receipt["lock_sha256"] == admitted["summary"]["lock_sha256"] and receipt["records"] == admitted["summary"]["split_counts"]["test"] and
                       receipt["adapter_sha256"] == lock["adapter_sha256"] and receipt["harness_sha256"] == lock["harness_sha256"] and
                       receipt["metric_contract_sha256"] == lock["metric_contract_sha256"] and receipt["metrics"] == lock["metrics"], "prepared receipt is incomplete or mismatched")
    for name, key in (("requests.jsonl", "requests_sha256"), ("gold.jsonl", "gold_sha256")):
        evaluation.checked(oracle.sha256_file(prepared / name) == receipt[key], "prepared evidence changed")
    cases = []
    request_iter = evaluation.rows(prepared / "requests.jsonl")
    gold_iter = evaluation.rows(prepared / "gold.jsonl")
    for spec, path in admitted["files"]:
        if spec["split"] != "test":
            continue
        for original in evaluation.rows(path):
            row, gold = next(request_iter, None), next(gold_iter, None)
            request = {"text": original["text"], "schema": admitted["schemas"][original["schema_id"]],
                       "options": lock["request_options"], "offset_unit": "utf8_bytes"}
            identity = {"request_id": evaluation.digest(evaluation.encoded({"lock": receipt["lock_sha256"], "id": original["id"]})),
                        "request_sha256": evaluation.digest(evaluation.encoded(request))}
            evaluation.checked(row == {**identity, "request": request} and gold == {**identity,
                "family_id": original["family_id"], "language": original["language"], "metrics": original["gold"]},
                "prepared requests/gold do not reproduce the locked test split")
            cases.append({"id": row["request_id"], "request_sha256": row["request_sha256"], **row["request"]})
            evaluation.checked(len(cases) <= MAX_CASES, "execution case budget exceeded")
    evaluation.checked(next(request_iter, None) is None and next(gold_iter, None) is None, "prepared evidence exceeds the locked denominator")
    evaluation.checked(len(cases) == receipt["records"], "request denominator differs")
    value = {"format_version": 1, "scope": WORKER_SCOPE, "qualification": False, "source_commit": oracle.UPSTREAM_COMMIT,
             "model": variant, "source_files": source_identity(variant)["source_files"], "lock_sha256": receipt["lock_sha256"],
             "prepared_sha256": oracle.sha256_file(prepared / "prepared.json"), "requests_sha256": receipt["requests_sha256"],
             "adapter_sha256": receipt["adapter_sha256"], "harness_sha256": receipt["harness_sha256"], "policy": POLICY, "cases": cases}
    validate_fixture(value)
    return value


def model_artifact(variant: str, directory: Path) -> dict[str, Any]:
    identity = source_identity(variant)
    receipt_path = directory / "antfly_inference_bundle.json"
    if not receipt_path.exists():
        bundle = oracle.verify_model_dir(variant, directory)
        evaluation.checked(bundle["files"] == {pin["path"]: {key: pin[key] for key in ("size_bytes", "sha256")} for pin in identity["source_files"]}, "source checkpoint differs")
        return {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None, **identity}
    evaluation.checked(0 < receipt_path.stat().st_size <= 64 * 1024, "bundle receipt byte budget exceeded")
    receipt = oracle.read_json(receipt_path)
    evaluation.checked(receipt.get("family") == "gliner_boundary_bundle/v1" and receipt.get("version") == 1 and
                       receipt.get("backbone") == variant and receipt.get("precision") in ("fp32", "fp16_encoder", "q8_0", "q4_0", "q4_k") and
                       sorted(receipt["source_files"], key=lambda pin: pin["path"]) == identity["source_files"], "bundle source identity differs")
    evaluation.checked(len(receipt["files"]) == 5 and {pin["path"] for pin in receipt["files"]} == {"model.gguf", "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json"}, "bundle file inventory differs")
    # The native worker additionally validates tensor inventory and precision
    # policy on the exact opened mapping before performing learned operations.
    for pin in receipt["files"]:
        evaluation.checked(type(pin["size_bytes"]) is int and 0 < pin["size_bytes"] <= 2 * 1024**3, "bundle file budget exceeded")
        oracle.verify_file(directory / pin["path"], pin)
    return {"kind": "bundle", "precision": receipt["precision"], "receipt": receipt,
            "receipt_sha256": oracle.sha256_file(receipt_path), **identity}


def canonical_output(request: dict[str, Any], output: dict[str, Any], backend: str) -> tuple[dict[str, Any], dict[str, Any]]:
    facts = adapter.prediction_facts(request, output, backend)
    groups = output["entities"] if backend == "python" else {group["name"]: group["values"] for group in output["entities"]}
    confidences = [value["confidence"] for name in request["schema"]["entities"] for value in groups[name]]
    evaluation.checked(len(confidences) == len(facts["entity_exact"]), "canonical confidence routing differs")
    return facts, {"entities": [{**fact, "confidence": confidence} for fact, confidence in zip(facts["entity_exact"], confidences)]}


def validate_tokens(value: Any) -> list[int]:
    evaluation.checked(isinstance(value, list) and 0 < len(value) <= POLICY["max_encoded_tokens"] and
                       all(type(token) is int and 0 <= token < 2**32 for token in value), "encoder token evidence is invalid or oversized")
    return value


def validate_ready(ready: Any, backend: str, artifact: dict[str, Any], fixture: dict[str, Any], fixture_sha: str) -> None:
    expected = {"event": "ready", "scope": WORKER_SCOPE, "qualification": False, "backend": backend,
                "artifact_kind": artifact["kind"], "receipt": artifact["receipt"], "source_files": fixture["source_files"],
                "model": fixture["model"], "source_commit": oracle.UPSTREAM_COMMIT, "fixture_sha256": fixture_sha,
                "lock_sha256": fixture["lock_sha256"], "weight_precision": artifact["precision"],
                "activation_precision": "f32", "accumulation_precision": "f32", "head_precision": "f32",
                "math_policy": "torch_f32_cpu_v1" if backend == "python" else comparison.MATH_POLICY}
    evaluation.checked(isinstance(ready, dict) and all(ready.get(key) == value for key, value in expected.items()), "worker artifact, fixture or arithmetic identity differs")
    if backend == "python":
        evaluation.checked(ready.get("threads") == 1 and ready.get("interop_threads") == 1 and ready.get("provenance", {}).get("commit") == oracle.UPSTREAM_COMMIT, "Python oracle profile differs")


def execute_python_case(model: Any, case: dict[str, Any], torch: Any) -> dict[str, Any]:
    """Preflight without truncation; then verify the actual encoder sees it."""
    request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
    text = request["text"]
    if len(text.encode()) > POLICY["max_text_bytes"] or len(list(model.processor.word_splitter(text, lower=False))) > POLICY["max_words"]:
        raise evaluation.EvaluationError("BoundaryTextLimitExceeded")
    schema = oracle.build_extract_schema(request["schema"])
    batch = model.processor.collate_fn_inference([(text, schema.build())], max_len=None, architecture="boundary",
        error_policy="raise", build_targets=False, on_capacity_exceeded="raise")
    if len(batch.text_tokens[0]) > POLICY["max_words"]:
        raise evaluation.EvaluationError("BoundaryTextLimitExceeded")
    if batch.input_ids.shape[-1] > POLICY["max_encoded_tokens"]:
        raise evaluation.EvaluationError("BoundarySequenceLimitExceeded")
    if batch.query_marker_mask.shape[-1] > POLICY["max_queries"]:
        raise evaluation.EvaluationError("BoundaryQueryLimitExceeded")
    expected_ids = validate_tokens(batch.input_ids[0].tolist())
    with bench.capture_encoder_input_ids(model) as captured, torch.inference_mode():
        output = model.extract(text, schema, threshold=0.5, overlap_policy="flat", include_confidence=True,
                               include_spans=True, max_len=None)
    evaluation.checked(captured == [expected_ids], "actual encoder differs from untruncated preparation")
    return {"event": "result", "case_id": case["id"], "request_sha256": case["request_sha256"], "input_ids": expected_ids, "output": output}


def python_worker(args: argparse.Namespace) -> None:
    evaluation.checked(0 < args.evaluation_fixture.stat().st_size <= MAX_FIXTURE_BYTES, "execution fixture byte budget exceeded")
    fixture = oracle.read_json(args.evaluation_fixture)
    validate_fixture(fixture)
    for name in bench.THREAD_ENV:
        evaluation.checked(os.environ.get(name) == "1", "worker thread budget must be explicit")
    provenance, torch = oracle.prepare_runtime(args.upstream)
    torch.set_num_interop_threads(1)
    artifact = model_artifact(fixture["model"], args.model_dir)
    evaluation.checked(artifact["kind"] == "source_fp32", "Python worker only supports original FP32 artifacts")
    from gliner2 import AutoExtractor
    model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True, map_location="cpu", use_flashdeberta=False).float().eval()
    evaluation.checked(model.architecture == "boundary", "wrong oracle architecture")
    fixture_sha = oracle.sha256_file(args.evaluation_fixture)
    bench.emit({"event": "ready", "scope": WORKER_SCOPE, "backend": "python", "qualification": False,
                "artifact_kind": "source_fp32", "receipt": None, "source_files": fixture["source_files"],
                "model": fixture["model"], "source_commit": oracle.UPSTREAM_COMMIT, "fixture_sha256": fixture_sha,
                "lock_sha256": fixture["lock_sha256"], "math_policy": "torch_f32_cpu_v1", "weight_precision": "fp32",
                "activation_precision": "f32", "accumulation_precision": "f32", "head_precision": "f32",
                "threads": torch.get_num_threads(), "interop_threads": torch.get_num_interop_threads(), "provenance": provenance})
    errors = 0
    for case in fixture["cases"]:
        try:
            response = execute_python_case(model, case, torch)
        except Exception as error:
            errors += 1
            code = str(error) if isinstance(error, evaluation.EvaluationError) and str(error).startswith("Boundary") else type(error).__name__
            response = {"event": "error", "case_id": case["id"], "request_sha256": case["request_sha256"], "error_code": code, "input_ids": None}
        bench.emit(response)
    evaluation.checked(model_artifact(fixture["model"], args.model_dir) == artifact and oracle.sha256_file(args.evaluation_fixture) == fixture_sha, "worker inputs changed during execution")
    oracle.verify_upstream_checkout(args.upstream)
    bench.emit({"event": "complete", "cases": len(fixture["cases"]), "errors": errors, "qualification": False})


def load_reference(path: Path, fixture: dict[str, Any], prepared: Path, expected_backend: str, artifact: dict[str, Any] | None = None) -> dict[str, Any]:
    evaluation.checked(0 < path.stat().st_size <= 32 * 1024 * 1024, "reference report byte budget exceeded")
    report = oracle.read_json(path)
    evaluation.checked(report.get("scope") == SCOPE and report.get("status") == "complete" and report.get("qualification") is False and
                       report.get("backend") == expected_backend and report.get("model") == fixture["model"] and
                       report.get("contract_files") == contract_files() and report.get("fixture_sha256") == evaluation.digest(evaluation.encoded(fixture) + b"\n") and
                       report.get("denominator") == len(fixture["cases"]) and report.get("errors") == 0, "reference report is incomplete or incompatible")
    if artifact is None:
        evaluation.checked(report["artifact"]["kind"] == "source_fp32" and report["artifact"]["precision"] == "fp32", "source reference must be original FP32")
    else:
        evaluation.checked(report["artifact"] == artifact, "backend reference must use the identical bundle")
    evaluation.checked(report["artifact"]["source_files"] == fixture["source_files"], "reference source artifacts differ")
    validate_ready(report["ready"], expected_backend, report["artifact"], fixture, report["fixture_sha256"])
    for pin in report["files"].values():
        evaluation.pinned_path(path.parent, pin)
    response_path = evaluation.pinned_path(path.parent, report["files"]["responses"])
    responses = list(evaluation.rows(response_path))
    evaluation.checked(len(responses) == len(fixture["cases"]), "reference response coverage differs")
    prediction_path = evaluation.pinned_path(path.parent, report["files"]["predictions"])
    predictions = list(evaluation.rows(prediction_path))
    evaluation.checked(len(predictions) == len(responses), "reference predictions and outputs differ")
    for case, response, prediction in zip(fixture["cases"], responses, predictions):
        evaluation.checked(response["event"] == "result" and response["case_id"] == case["id"] and response["request_sha256"] == case["request_sha256"], "reference response identity differs")
        validate_tokens(response["input_ids"])
        request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
        facts, _ = canonical_output(request, response["output"], expected_backend)
        evaluation.checked(prediction == {"request_id": case["id"], "request_sha256": case["request_sha256"], "metrics": facts}, "reference predictions do not match actual model outputs")
    metrics = evaluation.score(prepared, prediction_path)
    evaluation.checked(metrics == oracle.read_json(evaluation.pinned_path(path.parent, report["files"]["metrics"])), "reference metrics differ from predictions")
    return {"report": report, "responses": responses, "metrics": metrics, "sha256": oracle.sha256_file(path)}


def quality_delta(reference: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    evaluation.checked(reference["records"] == actual["records"] and reference["lock_sha256"] == actual["lock_sha256"] and
                       reference["metrics"].keys() == actual["metrics"].keys(), "quality denominators or contracts differ")
    return {name: {"fp32_micro_f1": ref["micro_f1"], "actual_micro_f1": actual["metrics"][name]["micro_f1"],
                   "micro_f1_delta": actual["metrics"][name]["micro_f1"] - ref["micro_f1"],
                   "micro_f1_loss": ref["micro_f1"] - actual["metrics"][name]["micro_f1"],
                   "support": ref["support"], "quality_floor": None, "quality_qualified": False}
            for name, ref in reference["metrics"].items()}


def run(args: argparse.Namespace) -> dict[str, Any]:
    evaluation.checked(args.backend in ("python", "native", "metal") and 1 <= args.max_rss_mib <= 12288 and
                       1 <= args.startup_timeout <= 600 and 1 <= args.response_timeout <= 180, "driver resource limits invalid")
    fixture = prepare_fixture(args.lock, args.prepared_dir, args.model)
    artifact = model_artifact(args.model, args.model_dir)
    evaluation.checked(args.backend != "python" or artifact["kind"] == "source_fp32", "Python does not load converted weights")
    evaluation.checked((args.backend == "python") == (args.source_reference_report is None), "native/Metal require a completed Python FP32 reference")
    evaluation.checked((args.backend == "metal") == (args.native_reference_report is not None), "Metal requires an exact matching native reference")
    source = None if args.source_reference_report is None else load_reference(args.source_reference_report, fixture, args.prepared_dir, "python")
    native = None if args.native_reference_report is None else load_reference(args.native_reference_report, fixture, args.prepared_dir, "native", artifact)
    output = args.output_dir
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    fixture_path = output / "requests.fixture.json"
    fixture_bytes = evaluation.encoded(fixture) + b"\n"
    evaluation.checked(len(fixture_bytes) <= MAX_FIXTURE_BYTES, "execution fixture byte budget exceeded")
    fixture_path.write_bytes(fixture_bytes)
    fixture_sha = oracle.sha256_file(fixture_path)
    env = os.environ.copy()
    env.update({name: "1" for name in bench.THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", TOKENIZERS_PARALLELISM="false")
    env.pop("USE_FLASHDEBERTA", None)
    if args.backend == "python":
        command = [sys.executable, str(Path(__file__).resolve()), "worker", "--evaluation-fixture", str(fixture_path), "--model-dir", str(args.model_dir), "--upstream", str(args.upstream)]
        binary_sha = oracle.sha256_file(Path(sys.executable).resolve())
    else:
        evaluation.checked(args.binary is not None and args.binary.is_file(), "native binary required")
        command = [str(args.binary.resolve()), "--model-dir", str(args.model_dir), "--evaluation-fixture", str(fixture_path), "--backend", args.backend]
        binary_sha = oracle.sha256_file(args.binary)
    report = {"scope": SCOPE, "status": "incomplete", "qualification": False, "backend": args.backend,
              "model": args.model, "artifact": artifact, "contract_files": contract_files(), "binary_sha256": binary_sha,
              "fixture_sha256": fixture_sha, "lock_sha256": fixture["lock_sha256"], "policy": POLICY,
              "denominator": len(fixture["cases"]), "errors": 0, "completed_results": 0, "unprocessed": 0,
              "source_reference_report_sha256": None if source is None else source["sha256"],
              "native_reference_report_sha256": None if native is None else native["sha256"], "comparisons": []}
    guard = bench.ResourceGuard(args.max_rss_mib * 1024 * 1024)
    worker = None
    processed = 0
    try:
        worker = bench.Worker(args.backend, command, env, output, guard)
        report["ready"] = worker.receive(args.startup_timeout)
        validate_ready(report["ready"], args.backend, artifact, fixture, fixture_sha)
        with (output / "responses.jsonl").open("xb") as evidence, (output / "predictions.jsonl").open("xb") as predictions:
            for index, case in enumerate(fixture["cases"]):
                response = worker.receive(args.response_timeout)
                evaluation.checked(response.get("event") in ("result", "error") and response.get("case_id") == case["id"] and response.get("request_sha256") == case["request_sha256"], "missing, duplicate or reordered worker result")
                evidence.write(evaluation.encoded(response) + b"\n")
                processed += 1
                if response["event"] == "error":
                    evaluation.checked(isinstance(response.get("error_code"), str) and bool(response["error_code"]), "untyped worker failure")
                    report["errors"] += 1
                    continue
                ids = validate_tokens(response["input_ids"])
                request = {key: case[key] for key in ("text", "schema", "options", "offset_unit")}
                facts, canonical = canonical_output(request, response["output"], args.backend)
                predictions.write(evaluation.encoded({"request_id": case["id"], "request_sha256": case["request_sha256"], "metrics": facts}) + b"\n")
                report["completed_results"] += 1
                row = {"case_id": case["id"], "input_ids_u32_le_sha256": evaluation.digest(b"".join(token.to_bytes(4, "little") for token in ids))}
                for name, reference in (("source_fp32", source), ("same_artifact_native", native)):
                    if reference is None:
                        continue
                    original = reference["responses"][index]
                    evaluation.checked(ids == original["input_ids"], "encoder token IDs differ from exact reference")
                    _, expected = canonical_output(request, original["output"], "python" if name == "source_fp32" else "native")
                    row[name] = comparison.compare_backends(expected, canonical)
                report["comparisons"].append(row)
        done = worker.receive(args.response_timeout)
        evaluation.checked(done == {"event": "complete", "cases": len(fixture["cases"]), "errors": report["errors"], "qualification": False}, "worker did not reverify final artifacts or denominator")
        deadline = time.monotonic() + 5
        while worker.process.poll() is None:
            guard.check()
            evaluation.checked(time.monotonic() < deadline, "worker failed to exit after completion")
            time.sleep(0.05)
        evaluation.checked(worker.process.returncode == 0 and not worker.buffer and not worker.process.stdout.read(1), "worker exited unsuccessfully or emitted extra output")
        evaluation.checked(model_artifact(args.model, args.model_dir) == artifact and prepare_fixture(args.lock, args.prepared_dir, args.model) == fixture and
                           contract_files() == report["contract_files"] and oracle.sha256_file(Path(sys.executable).resolve() if args.backend == "python" else args.binary) == binary_sha,
                           "inputs or execution code changed during execution")
        if not report["errors"]:
            metrics = evaluation.score(args.prepared_dir, output / "predictions.jsonl")
            oracle.write_json(output / "metrics.json", metrics)
            report["metrics"] = metrics["metrics"]
            per_type = [value for name, value in metrics["metrics"].items() if name.startswith("entity_type/")]
            report["task_summary"] = {"task": "typed_entity_exact_occurrence", "micro_f1": metrics["metrics"]["entity_exact"]["micro_f1"],
                "fixed_ontology_macro_f1": sum(value["micro_f1"] for value in per_type) / len(per_type),
                "ontology_types": len(per_type), "types_with_gold_support": sum(value["support"] > 0 for value in per_type),
                "quality_qualified": False}
            if source is not None:
                report["source_fp32_quality_delta"] = quality_delta(source["metrics"], metrics)
            report["status"] = "complete"
        else:
            report["status"] = "failed"
    except Exception as error:
        report["status"] = "failed"
        report["driver_error"] = {"type": type(error).__name__, "message": str(error)}
        report["unprocessed"] = len(fixture["cases"]) - processed
        report["errors"] = len(fixture["cases"]) - report["completed_results"]
    finally:
        if worker is not None:
            worker.close()
    report["peak_worker_rss_bytes"] = guard.peak_rss_bytes
    report["max_worker_rss_bytes"] = args.max_rss_mib * 1024 * 1024
    report["files"] = {key: {"path": name, "size_bytes": (output / name).stat().st_size, "sha256": oracle.sha256_file(output / name)}
                       for key, name in (("fixture", "requests.fixture.json"), ("responses", "responses.jsonl"), ("predictions", "predictions.jsonl"), ("metrics", "metrics.json")) if (output / name).exists()}
    required_parity = "same_artifact_native" if native is not None else "source_fp32" if source is not None and artifact["precision"] == "fp32" else None
    report["required_parity"] = None if required_parity is None else {"reference": required_parity,
        "pass": report["status"] == "complete" and all(row[required_parity]["parity_pass"] for row in report["comparisons"]),
        "confidence_absolute_tolerance": comparison.CONFIDENCE_TOLERANCE}
    oracle.write_json(output / "report.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    worker = commands.add_parser("worker")
    worker.add_argument("--evaluation-fixture", type=Path, required=True)
    worker.add_argument("--model-dir", type=Path, required=True)
    worker.add_argument("--upstream", type=Path, required=True)
    driver = commands.add_parser("run")
    driver.add_argument("--lock", type=Path, required=True)
    driver.add_argument("--prepared-dir", type=Path, required=True)
    driver.add_argument("--model", choices=("small", "base", "multi"), required=True)
    driver.add_argument("--model-dir", type=Path, required=True)
    driver.add_argument("--backend", choices=("python", "native", "metal"), required=True)
    driver.add_argument("--binary", type=Path)
    driver.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    driver.add_argument("--source-reference-report", type=Path)
    driver.add_argument("--native-reference-report", type=Path)
    driver.add_argument("--output-dir", type=Path, required=True)
    driver.add_argument("--max-rss-mib", type=int, default=6144)
    driver.add_argument("--startup-timeout", type=int, default=180)
    driver.add_argument("--response-timeout", type=int, default=125)
    args = parser.parse_args()
    if args.command == "worker":
        python_worker(args)
    else:
        report = run(args)
        print(evaluation.encoded({key: report[key] for key in ("status", "qualification", "denominator", "completed_results", "errors", "required_parity")}).decode())
        if report["status"] != "complete" or report["required_parity"] is not None and not report["required_parity"]["pass"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
