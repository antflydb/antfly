#!/usr/bin/env python3
"""Bounded, blinded MASSIVE shard execution and atomic full-denominator scoring.

Separate from the frozen CrossNER v1 driver. This tool never tunes a schema,
threshold, model or split and never turns incomplete execution into metrics.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import socket
import stat
import sys
import time

import benchmark_cpu as bench
import check_bundles as comparison
import evaluate
import evaluation_contract as contract
import massive_eval_contract as execution
import oracle


def python_runtime_identity():
    """Bind the invoked environment separately from the resolved executable bytes.

    Resolving a venv's bin/python symlink before exec loses pyvenv.cfg discovery.
    Keep that invocation spelling, while hashing the actual executable target.
    This helper imports no inference packages and runs before model admission.
    """
    invocation = Path(os.path.abspath(sys.executable))
    executable = invocation.resolve(strict=True)
    execution.check(executable.is_file(), "Python executable is not a regular file")
    prefix, base_prefix = os.path.abspath(sys.prefix), os.path.abspath(sys.base_prefix)
    configuration = Path(prefix) / "pyvenv.cfg"
    configuration_pin = None
    if configuration.exists():
        with configuration.open("rb") as source:
            before = os.fstat(source.fileno())
            execution.check(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= 65536,
                            "Python environment configuration exceeds its byte budget")
            data = source.read(65537)
            after = os.fstat(source.fileno())
        execution.check(len(data) == before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns,
                        "Python environment configuration changed while reading")
        configuration_pin = {"path": str(configuration), "size_bytes": len(data), "sha256": contract.digest(data)}
    execution.check(prefix == base_prefix or configuration_pin is not None,
                    "virtual environment is missing pyvenv.cfg")
    return {"scope": "gliner25_python_invocation/v1", "invocation": str(invocation),
        "executable": str(executable), "executable_sha256": oracle.sha256_file(executable),
        "prefix": prefix, "base_prefix": base_prefix, "pyvenv_cfg": configuration_pin,
        "environment": {name: os.environ.get(name) for name in
            ("PYTHONHOME", "PYTHONPATH", "PYTHONNOUSERSITE", "PYTHONSAFEPATH", "PYTHONUTF8", "VIRTUAL_ENV")},
        "flags": {name: getattr(sys.flags, name) for name in
            ("no_site", "no_user_site", "ignore_environment", "isolated", "safe_path", "utf8_mode")}}


def python_worker_command(fixture_path, model_dir, upstream, identity):
    return [identity["invocation"], str(Path(__file__).resolve()), "worker",
            "--evaluation-fixture", str(fixture_path), "--model-dir", str(model_dir.resolve()),
            "--upstream", str(upstream.resolve()),
            "--python-runtime-sha256", contract.digest(contract.encoded(identity))]


@contextmanager
def offline_network():
    """A local checkpoint must never fall back to network model resolution."""
    original_connect, original_connection = socket.socket.connect, socket.create_connection
    def denied(*_args, **_kwargs):
        raise RuntimeError("network access is disabled for the MASSIVE oracle")
    socket.socket.connect = denied
    socket.create_connection = denied
    try:
        yield
    finally:
        socket.socket.connect, socket.create_connection = original_connect, original_connection


def python_schema(specification):
    """Keep ordinary model temperature separate from the constraint facade."""
    if "classification_constraints" in specification:
        from gliner2.classification import ClassificationSchema
        from gliner2.classification.constraints import constraint_from_dict
        schema = ClassificationSchema()
        for task in specification["classifications"]:
            schema.single(task["name"], task["labels"], activation=task["activation"])
        for constraint in specification["classification_constraints"]:
            schema.constrain(constraint_from_dict(constraint))
        return schema
    if "classifications" in specification:
        from gliner2 import Schema
        schema = Schema()
        for task in specification["classifications"]:
            schema.classification(task=task["name"], labels=task["labels"], multi_label=False,
                                  cls_threshold=0.5, class_act=task["activation"])
        return schema
    return oracle.build_extract_schema(specification)


def execute_python_case(model, fixture, case, torch):
    text = case["text"]
    if len(text.encode("utf-8")) > execution.POLICY["max_text_bytes"]:
        raise contract.EvaluationError("BoundaryTextLimitExceeded")
    if len(list(model.processor.word_splitter(text, lower=False))) > execution.POLICY["max_words"]:
        raise contract.EvaluationError("BoundaryTextLimitExceeded")
    schema = python_schema(fixture["schema"])
    constrained = "classification_constraints" in fixture["schema"]
    if constrained:
        from gliner2.classification import Classifier, ClassificationConfig
        classifier = Classifier(model)
        schema = classifier.compile_schema(schema)
        config = ClassificationConfig(decoder="auto", on_infeasible="raise", batch_size=1, max_len=None,
            exact_node_budget=execution.LIMITS["exact_node_budget"], beam_size=execution.LIMITS["beam_width"],
            candidate_threshold=0.5, max_candidates_per_task=execution.LIMITS["max_candidates_per_task"],
            include_confidence=True)
    batch = model.processor.collate_fn_inference([(text, schema.build())], max_len=None,
        architecture="boundary", error_policy="raise", build_targets=False, on_capacity_exceeded="raise")
    if len(batch.text_tokens[0]) > execution.POLICY["max_words"]:
        raise contract.EvaluationError("BoundaryTextLimitExceeded")
    if batch.input_ids.shape[-1] > execution.POLICY["max_encoded_tokens"]:
        raise contract.EvaluationError("BoundarySequenceLimitExceeded")
    if batch.query_marker_mask.shape[-1] > execution.POLICY["max_queries"]:
        raise contract.EvaluationError("BoundaryQueryLimitExceeded")
    if batch.cls_marker_mask.shape[-1] > execution.LIMITS["max_classification_labels"]:
        raise contract.EvaluationError("BoundaryClassificationLimitExceeded")
    expected_ids = evaluate.validate_tokens(batch.input_ids[0].tolist())
    with bench.capture_encoder_input_ids(model) as captured, torch.inference_mode():
        if constrained:
            output = classifier.decode(classifier.score(text, schema, config=config), schema, config=config).to_dict()
        else:
            output = model.extract(text, schema, threshold=0.5, overlap_policy="flat",
                                   include_confidence=True, include_spans=True, max_len=None)
    execution.check(captured == [expected_ids], "actual encoder differs from untruncated preparation")
    return {"event": "result", "case_id": case["id"], "request_sha256": case["request_sha256"],
            "input_ids": expected_ids, "output": output}


def python_worker(args):
    python_runtime = python_runtime_identity()
    execution.check(contract.is_digest(args.python_runtime_sha256) and
                    contract.digest(contract.encoded(python_runtime)) == args.python_runtime_sha256,
                    "worker Python invocation or environment differs before model loading")
    fixture = execution.read_json(args.evaluation_fixture, execution.MAX_FIXTURE_BYTES)
    execution.validate_fixture(fixture)
    closure = execution.contract_files()
    for name in bench.THREAD_ENV:
        execution.check(os.environ.get(name) == "1", "worker thread budget must be explicit")
    provenance, torch = oracle.prepare_runtime(args.upstream)
    torch.set_num_interop_threads(1)
    artifact = evaluate.model_artifact(fixture["model"], args.model_dir)
    execution.check(artifact["kind"] == "source_fp32", "Python requires original FP32 checkpoint")
    fixture_sha = oracle.sha256_file(args.evaluation_fixture)
    with offline_network():
        from gliner2 import AutoExtractor
        model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True,
            map_location="cpu", use_flashdeberta=False).float().eval()
        execution.check(model.architecture == "boundary", "wrong oracle architecture")
        # One fresh model process per profile/shard: no splitter-dependent cache
        # from an earlier profile survives this explicit public setter.
        model.set_word_splitter(fixture["options"]["word_splitter"])
        bench.emit({"event": "ready", "scope": execution.WORKER_SCOPE, "backend": "python", "qualification": False,
            "artifact_kind": "source_fp32", "receipt": None, "source_files": fixture["source_files"],
            "model": fixture["model"], "source_commit": oracle.UPSTREAM_COMMIT, "fixture_sha256": fixture_sha,
            "lock_sha256": fixture["lock_sha256"], "profile": fixture["profile"], "transport": fixture["transport"],
            "registry_sha256": fixture["registry_sha256"], "word_splitter": fixture["options"]["word_splitter"],
            "math_policy": "torch_f32_cpu_v1", "weight_precision": "fp32", "activation_precision": "f32",
            "accumulation_precision": "f32", "head_precision": "f32", "threads": torch.get_num_threads(),
            "interop_threads": torch.get_num_interop_threads(), "provenance": provenance,
            "python_runtime": python_runtime})
        errors = 0
        for case in fixture["cases"]:
            try:
                response = execute_python_case(model, fixture, case, torch)
                execution.response_prediction(fixture, case, response, "python")
                if len(contract.encoded(response)) + 1 > execution.MAX_EVENT_BYTES:
                    raise contract.EvaluationError("BoundaryOutputLimitExceeded")
            except Exception as error:
                errors += 1
                code = str(error) if isinstance(error, contract.EvaluationError) and str(error).startswith("Boundary") else type(error).__name__
                response = {"event": "error", "case_id": case["id"], "request_sha256": case["request_sha256"],
                            "error_code": code, "input_ids": None}
            bench.emit(response)
    execution.check(evaluate.model_artifact(fixture["model"], args.model_dir) == artifact and
                    oracle.sha256_file(args.evaluation_fixture) == fixture_sha and execution.contract_files() == closure and
                    python_runtime_identity() == python_runtime,
                    "worker inputs changed during execution")
    oracle.verify_upstream_checkout(args.upstream)
    bench.emit({"event": "complete", "cases": len(fixture["cases"]), "errors": errors, "qualification": False})


def validate_ready(ready, backend, artifact, fixture, fixture_sha, python_runtime=None):
    expected = {"event": "ready", "scope": execution.WORKER_SCOPE, "qualification": False, "backend": backend,
        "artifact_kind": artifact["kind"], "receipt": artifact["receipt"], "source_files": fixture["source_files"],
        "model": fixture["model"], "source_commit": oracle.UPSTREAM_COMMIT, "fixture_sha256": fixture_sha,
        "lock_sha256": fixture["lock_sha256"], "profile": fixture["profile"], "transport": fixture["transport"],
        "registry_sha256": fixture["registry_sha256"], "word_splitter": fixture["options"]["word_splitter"],
        "weight_precision": artifact["precision"], "activation_precision": "f32", "accumulation_precision": "f32",
        "head_precision": "f32", "math_policy": "torch_f32_cpu_v1" if backend == "python" else comparison.MATH_POLICY}
    execution.check(isinstance(ready, dict) and all(ready.get(key) == value for key, value in expected.items()),
                    "worker artifact, shard, splitter or math identity differs")
    if backend == "python":
        execution.check(ready.get("threads") == ready.get("interop_threads") == 1 and
                        ready.get("provenance", {}).get("commit") == oracle.UPSTREAM_COMMIT, "oracle dependency/thread profile differs")
        execution.check(isinstance(python_runtime, dict) and ready.get("python_runtime") == python_runtime,
                        "worker Python invocation or environment differs")
    else:
        execution.check(ready.get("build_mode") == "ReleaseFast", "evaluation requires complete ReleaseFast build graph")


def write_line(target, value):
    data = contract.encoded(value) + b"\n"
    execution.check(len(data) <= execution.MAX_EVENT_BYTES and target.tell() + len(data) <= execution.MAX_SHARD_BYTES,
                    "shard output byte budget exceeded")
    target.write(data)


def durable_close(target):
    target.flush()
    os.fsync(target.fileno())


def wait_for_exit(worker, guard):
    deadline = time.monotonic() + 5
    while worker.process.poll() is None:
        guard.check()
        execution.check(time.monotonic() < deadline, "worker failed to exit after completion")
        time.sleep(0.05)
    execution.check(worker.process.returncode == 0 and not worker.buffer and not worker.process.stdout.read(1),
                    "worker exited unsuccessfully or emitted extra output")


def read_evidence(path: Path, pin, limit):
    result = contract.pinned_path(path.parent, pin)
    execution.check(result.stat().st_size <= limit, "evidence byte budget exceeded")
    return result


def evidence_rows(path: Path, expected, limit):
    """Hash the exact descriptor bytes that are parsed, with bounded lines."""
    source_path = read_evidence(path, expected, limit)
    rows, size, digest = [], 0, hashlib.sha256()
    with source_path.open("rb") as source:
        while line := source.readline(execution.MAX_EVENT_BYTES + 1):
            size += len(line)
            execution.check(len(line) <= execution.MAX_EVENT_BYTES and line.endswith(b"\n") and
                            size <= limit and len(rows) < execution.RECORDS, "evidence row/count budget exceeded")
            digest.update(line)
            value = bench.strict_json(line)
            execution.check(isinstance(value, dict), "evidence row must be an object")
            rows.append(value)
    execution.check(size == expected["size_bytes"] and digest.hexdigest() == expected["sha256"],
                    "consumed evidence differs from receipt")
    return rows


def reference_report(path, entry, root, expected_backend, artifact=None):
    """Recompute facts and all-denominator metrics from the pinned raw outputs."""
    report = execution.read_json(path)
    execution.check(report.get("scope") == execution.AGGREGATE_SCOPE and report.get("status") == "complete" and
        report.get("qualification") is False and report.get("backend") == expected_backend and
        report.get("profile") == entry["profile"]["id"] and report.get("lock_sha256") == entry["lock_sha256"] and
        report.get("contract_files") == execution.contract_files() and report.get("denominator") == execution.RECORDS and
        report.get("policy") == execution.POLICY and report.get("limits") == execution.LIMITS and
        report.get("registry_sha256") == oracle.sha256_file(execution.REGISTRY) and
        report.get("prepared_sha256") == entry["prepared_sha256"],
        "reference aggregate is incomplete or has different execution identity")
    execution.check(isinstance(report.get("shards"), list) and len(report["shards"]) == 3,
                    "reference does not contain all three shard identities")
    for index, shard in enumerate(report["shards"]):
        execution.check(set(shard) == {"index", "report_sha256", "transport"} and shard["index"] == index and
            contract.is_digest(shard["report_sha256"]) and shard["transport"] == {
                "global_records": execution.RECORDS, "transport_sha256": entry["transport_sha256"], **entry["shards"][index]},
            "reference shard identity or range differs")
    if artifact is not None:
        execution.check(report["artifact"] == artifact, "reference artifact differs")
    else:
        expected = evaluate.source_identity(report["model"])
        execution.check(report["artifact"] == {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None, **expected},
                        "source reference must use exact original FP32 files")
    responses_path = read_evidence(path, report["files"]["responses"], 3 * execution.MAX_SHARD_BYTES)
    predictions_path = read_evidence(path, report["files"]["predictions"], 3 * execution.MAX_SHARD_BYTES)
    responses = evidence_rows(path, report["files"]["responses"], 3 * execution.MAX_SHARD_BYTES)
    predictions = evidence_rows(path, report["files"]["predictions"], 3 * execution.MAX_SHARD_BYTES)
    requests = list(contract.rows(root / entry["profile"]["id"] / "prepared/requests.jsonl"))
    execution.check(len(responses) == len(predictions) == len(requests) == execution.RECORDS, "reference coverage differs")
    errors = 0
    for response, prediction, row in zip(responses, predictions, requests, strict=True):
        request = row["request"]
        fixture = {"profile": entry["profile"]["id"], **{key: request[key] for key in ("schema", "options", "offset_unit")}}
        case = {"id": row["request_id"], "request_sha256": row["request_sha256"], "text": request["text"]}
        expected, _ = execution.response_prediction(fixture, case, response, expected_backend)
        execution.check(prediction == expected, "reference predictions differ from raw outputs")
        errors += response["event"] == "error"
    execution.check(errors == report["errors"], "reference error denominator differs")
    metrics = contract.score(root / entry["profile"]["id"] / "prepared", predictions_path)
    execution.check(metrics["metrics"] == report["metrics"], "reference metrics differ from raw predictions")
    return {"sha256": oracle.sha256_file(path), "report": report, "responses": responses, "metrics": metrics}


def references(args, entry, artifact):
    source = native = None
    if args.backend == "python":
        execution.check(args.source_reference_report is None and args.native_reference_report is None,
                        "Python source capture cannot accept prediction references")
    else:
        execution.check(args.source_reference_report is not None, "native and Metal runs require completed FP32 Python reference")
        source = reference_report(args.source_reference_report, entry, args.prepared_root, "python")
        execution.check(source["report"]["model"] == args.model, "source reference model differs")
    if args.backend == "metal":
        execution.check(args.native_reference_report is not None, "Metal requires exact-artifact native aggregate")
        native = reference_report(args.native_reference_report, entry, args.prepared_root, "native", artifact)
        execution.check(native["report"]["model"] == args.model, "native reference model differs")
    else:
        execution.check(args.native_reference_report is None, "same-artifact native reference only applies to Metal")
    return source, native


def run_shard(args):
    execution.check(256 <= args.max_rss_mib <= 8192 and 1 <= args.startup_timeout <= 600 and
                    121 <= args.response_timeout <= 180, "invalid bounded process limits")
    entry, requests, cases = execution.admit_profile(args.prepared_root, args.profile)
    fixture = execution.make_fixture(entry, requests, cases, args.model, args.shard)
    artifact = evaluate.model_artifact(args.model, args.model_dir)
    execution.check(args.backend != "python" or artifact["kind"] == "source_fp32", "Python only supports original FP32")
    source, native = references(args, entry, artifact)
    python_runtime = python_runtime_identity() if args.backend == "python" else None
    binary = Path(python_runtime["executable"]) if python_runtime is not None else args.binary
    execution.check(binary is not None and binary.is_file(), "native evaluation binary is required")
    binary = binary.resolve()
    binary_sha = oracle.sha256_file(binary)
    if native is not None:
        execution.check(native["report"]["binary_sha256"] == binary_sha, "cross-backend comparison requires the identical binary")
    output = args.output_dir.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    fixture_path = output / "requests.fixture.json"
    fixture_path.write_bytes(contract.encoded(fixture) + b"\n")
    fixture_sha = oracle.sha256_file(fixture_path)
    closure = execution.contract_files()
    env = os.environ.copy()
    env.update({name: "1" for name in bench.THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", TOKENIZERS_PARALLELISM="false")
    env.pop("USE_FLASHDEBERTA", None)
    if args.backend == "python":
        command = python_worker_command(fixture_path, args.model_dir, args.upstream, python_runtime)
    else:
        command = [str(binary), "--model-dir", str(args.model_dir.resolve()), "--evaluation-fixture", str(fixture_path), "--backend", args.backend]
    report = {"scope": execution.SHARD_SCOPE, "status": "incomplete", "qualification": False,
        "backend": args.backend, "model": args.model, "artifact": artifact, "contract_files": closure,
        "binary_sha256": binary_sha, "python_runtime": python_runtime, "fixture_sha256": fixture_sha, "profile": args.profile,
        "registry_sha256": fixture["registry_sha256"], "lock_sha256": entry["lock_sha256"],
        "prepared_sha256": entry["prepared_sha256"], "policy": execution.POLICY, "limits": execution.LIMITS,
        "transport": fixture["transport"], "denominator": len(fixture["cases"]), "processed": 0, "errors": 0,
        "resource_policy": {"max_worker_rss_bytes": args.max_rss_mib * 1024**2, "startup_timeout_seconds": args.startup_timeout,
                            "response_timeout_seconds": args.response_timeout, "threads": 1},
        "source_reference_report_sha256": source["sha256"] if source else None,
        "native_reference_report_sha256": native["sha256"] if native else None, "comparisons": []}
    guard = bench.ResourceGuard(args.max_rss_mib * 1024**2)
    worker = None
    try:
        worker = bench.Worker(args.backend, command, env, output, guard)
        report["ready"] = worker.receive(args.startup_timeout)
        validate_ready(report["ready"], args.backend, artifact, fixture, fixture_sha, python_runtime)
        with (output / "responses.jsonl").open("xb") as responses, (output / "predictions.jsonl").open("xb") as predictions:
            for local_index, case in enumerate(fixture["cases"]):
                response = worker.receive(args.response_timeout)
                prediction, _ = execution.response_prediction(fixture, case, response, args.backend)
                write_line(responses, response)
                write_line(predictions, prediction)
                report["processed"] += 1
                report["errors"] += response["event"] == "error"
                row = {"case_id": case["id"], "input_ids_u32_le_sha256": None if response["event"] == "error" else
                    contract.digest(b"".join(token.to_bytes(4, "little") for token in response["input_ids"]))}
                global_index = fixture["transport"]["start"] + local_index
                for name, reference, backend in (("source_fp32", source, "python"), ("same_artifact_native", native, "native")):
                    if reference is not None:
                        row[name] = execution.compare_response(fixture, case, response, args.backend, reference["responses"][global_index], backend)
                report["comparisons"].append(row)
            durable_close(responses)
            durable_close(predictions)
        done = worker.receive(args.response_timeout)
        execution.check(done == {"event": "complete", "cases": len(fixture["cases"]), "errors": report["errors"], "qualification": False},
                        "worker omitted final artifact verification or denominator")
        wait_for_exit(worker, guard)
        execution.check(evaluate.model_artifact(args.model, args.model_dir) == artifact and oracle.sha256_file(binary) == binary_sha and
                        execution.contract_files() == closure and oracle.sha256_file(fixture_path) == fixture_sha,
                        "model, binary, fixture or helper changed during execution")
        execution.check(python_runtime is None or python_runtime_identity() == python_runtime,
                        "Python invocation or environment changed during execution")
        execution.admit_profile(args.prepared_root, args.profile)
        for path, reference in ((args.source_reference_report, source), (args.native_reference_report, native)):
            if reference is not None:
                execution.check(oracle.sha256_file(path) == reference["sha256"], "reference report changed during execution")
        report["status"] = "complete"
    except (Exception, KeyboardInterrupt) as error:
        report["driver_error"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        if worker is not None:
            worker.close()
    report["unprocessed"] = report["denominator"] - report["processed"]
    report["peak_worker_rss_bytes"] = guard.peak_rss_bytes
    report["files"] = {name: execution.pin(output / filename) for name, filename in
        (("fixture", "requests.fixture.json"), ("responses", "responses.jsonl"), ("predictions", "predictions.jsonl"))
        if (output / filename).exists() and (output / filename).stat().st_size > 0}
    execution.atomic_json(output / "report.json", report)
    return report


def shared_identity(report):
    keys = ("backend", "model", "artifact", "contract_files", "binary_sha256", "python_runtime", "profile", "registry_sha256", "lock_sha256",
            "prepared_sha256", "policy", "limits", "resource_policy", "source_reference_report_sha256", "native_reference_report_sha256")
    return {key: report[key] for key in keys}


def validate_shard_set(reports, entry):
    execution.check(len(reports) == len(execution.RANGES), "aggregate needs all three shards exactly once")
    identity = shared_identity(reports[0])
    for index, report in enumerate(reports):
        expected = {"global_records": execution.RECORDS, "transport_sha256": entry["transport_sha256"], **entry["shards"][index]}
        execution.check(report.get("scope") == execution.SHARD_SCOPE and report.get("status") == "complete" and
            report.get("qualification") is False and shared_identity(report) == identity and report.get("transport") == expected and
            report.get("processed") == report.get("denominator") == expected["end"] - expected["start"] and
            report.get("unprocessed") == 0 and type(report.get("errors")) is int and 0 <= report["errors"] <= report["denominator"],
            "incomplete, duplicated, reordered or differently configured shard")
    execution.check(identity["contract_files"] == execution.contract_files() and identity["profile"] == entry["profile"]["id"] and
        identity["registry_sha256"] == oracle.sha256_file(execution.REGISTRY) and identity["lock_sha256"] == entry["lock_sha256"] and
        identity["prepared_sha256"] == entry["prepared_sha256"] and identity["policy"] == execution.POLICY and identity["limits"] == execution.LIMITS,
        "aggregate execution contract differs")
    return identity


def aggregate(args):
    entry, requests, cases = execution.admit_profile(args.prepared_root, args.profile)
    reports = [execution.read_json(path) for path in args.shard_report]
    identity = validate_shard_set(reports, entry)
    # Explicit reference receipt paths are revalidated; their exact identities
    # must be the references used for each original shard execution.
    args.backend, args.model = identity["backend"], identity["model"]
    source, native = references(args, entry, identity["artifact"])
    for key, reference in (("source_reference_report_sha256", source), ("native_reference_report_sha256", native)):
        execution.check(identity[key] == (reference["sha256"] if reference else None), "aggregate reference differs from shard execution")
    output = args.output_dir.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    errors, error_codes, comparisons, shards = 0, Counter(), [], []
    try:
        with (output / "responses.jsonl").open("xb") as responses_target, (output / "predictions.jsonl").open("xb") as predictions_target:
            for index, (path, report) in enumerate(zip(args.shard_report, reports, strict=True)):
                report_sha = oracle.sha256_file(path)
                fixture = execution.read_json(read_evidence(path, report["files"]["fixture"], execution.MAX_FIXTURE_BYTES), execution.MAX_FIXTURE_BYTES)
                execution.check(fixture == execution.make_fixture(entry, requests, cases, identity["model"], index) and
                    report["fixture_sha256"] == report["files"]["fixture"]["sha256"], "shard fixture differs from approved global request range")
                validate_ready(report["ready"], identity["backend"], identity["artifact"], fixture, report["fixture_sha256"], identity["python_runtime"])
                response_path = read_evidence(path, report["files"]["responses"], execution.MAX_SHARD_BYTES)
                prediction_path = read_evidence(path, report["files"]["predictions"], execution.MAX_SHARD_BYTES)
                responses = evidence_rows(path, report["files"]["responses"], execution.MAX_SHARD_BYTES)
                predictions = evidence_rows(path, report["files"]["predictions"], execution.MAX_SHARD_BYTES)
                execution.check(len(responses) == len(predictions) == len(fixture["cases"]), "shard result coverage differs")
                count = 0
                for local_index, (case, response, prediction) in enumerate(zip(fixture["cases"], responses, predictions, strict=True)):
                    expected, _ = execution.response_prediction(fixture, case, response, identity["backend"])
                    execution.check(prediction == expected, "shard metrics were substituted after model execution")
                    count += response["event"] == "error"
                    if response["event"] == "error":
                        error_codes[response["error_code"]] += 1
                    row = {"case_id": case["id"], "input_ids_u32_le_sha256": None if response["event"] == "error" else
                        contract.digest(b"".join(token.to_bytes(4, "little") for token in response["input_ids"]))}
                    global_index = execution.RANGES[index][0] + local_index
                    for name, reference, backend in (("source_fp32", source, "python"), ("same_artifact_native", native, "native")):
                        if reference is not None:
                            row[name] = execution.compare_response(fixture, case, response, identity["backend"], reference["responses"][global_index], backend)
                    comparisons.append(row)
                    for target, value in ((responses_target, response), (predictions_target, prediction)):
                        data = contract.encoded(value) + b"\n"
                        execution.check(target.tell() + len(data) <= 3 * execution.MAX_SHARD_BYTES, "aggregate byte budget exceeded")
                        target.write(data)
                execution.check(count == report["errors"] and report["comparisons"] == comparisons[execution.RANGES[index][0]:execution.RANGES[index][1]],
                                "shard error accounting or parity evidence differs")
                errors += count
                execution.check(oracle.sha256_file(path) == report_sha and oracle.sha256_file(response_path) == report["files"]["responses"]["sha256"] and
                                oracle.sha256_file(prediction_path) == report["files"]["predictions"]["sha256"], "shard evidence changed while aggregating")
                shards.append({"index": index, "report_sha256": report_sha, "transport": report["transport"]})
            durable_close(responses_target); durable_close(predictions_target)
        execution.check(len(comparisons) == execution.RECORDS, "aggregate omitted request results")
        metrics = contract.score(args.prepared_root / args.profile / "prepared", output / "predictions.jsonl")
        required = "same_artifact_native" if native else "source_fp32" if source and identity["artifact"]["precision"] == "fp32" else None
        result = {"scope": execution.AGGREGATE_SCOPE, "status": "complete", "qualification": False, **identity,
            "denominator": execution.RECORDS, "successful_results": execution.RECORDS - errors, "errors": errors,
            "error_codes": dict(sorted(error_codes.items())), "shards": shards,
            "metrics_scope": "all_locked_rows_empty_facts_for_explicit_error_events" if errors else "all_locked_rows_successful_outputs",
            "metrics": metrics["metrics"], "comparisons": comparisons,
            "required_token_parity": None if source is None else {"pass": errors == 0 and all(
                row[name].get("token_ids_equal") is True for row in comparisons for name in
                (("source_fp32", "same_artifact_native") if native else ("source_fp32",))), "exact": True},
            "required_parity": None if required is None else {"reference": required, "pass": errors == 0 and all(row[required]["parity_pass"] for row in comparisons),
                "confidence_absolute_tolerance": comparison.CONFIDENCE_TOLERANCE},
            "files": {name: execution.pin(output / (name + ".jsonl")) for name in ("responses", "predictions")}}
        if source:
            result["source_fp32_quality_delta"] = evaluate.quality_delta(source["metrics"], metrics)
        names = ("intent_exact", "scenario_exact", "joint_intent_scenario", "constraints_satisfied")
        result["direct_document_rates"] = {name: metrics["metrics"][name]["document_exact_match"] for name in names if name in metrics["metrics"]}
        execution.admit_profile(args.prepared_root, args.profile)
        execution.check(execution.contract_files() == identity["contract_files"], "helpers changed while aggregating")
        # Receipt publication is the commit point. Before this there are no
        # published metrics, only incomplete raw staging files.
        execution.atomic_json(output / "report.json", result)
        return result
    except (Exception, KeyboardInterrupt) as error:
        execution.atomic_json(output / "failure.json", {"scope": execution.AGGREGATE_SCOPE, "status": "incomplete", "qualification": False,
            "denominator": execution.RECORDS, "driver_error": {"type": type(error).__name__, "message": str(error)}})
        raise


def prepare_fixtures(args):
    entry, requests, cases = execution.admit_profile(args.prepared_root, args.profile)
    output = args.output_dir.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    files = []
    for index in range(3):
        fixture = execution.make_fixture(entry, requests, cases, args.model, index)
        path = output / f"shard_{index:03}.fixture.json"
        path.write_bytes(contract.encoded(fixture) + b"\n")
        files.append(execution.pin(path))
    result = {"scope": "gliner25_massive11_execution_preflight/v1", "qualification": False, "no_model_execution": True,
              "profile": args.profile, "model": args.model, "denominator": execution.RECORDS,
              "registry_sha256": oracle.sha256_file(execution.REGISTRY), "contract_files": execution.contract_files(), "files": files}
    execution.atomic_json(output / "preflight.json", result)
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    worker = commands.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--evaluation-fixture", type=Path, required=True)
    worker.add_argument("--model-dir", type=Path, required=True)
    worker.add_argument("--upstream", type=Path, required=True)
    worker.add_argument("--python-runtime-sha256", required=True)
    for name in ("prepare", "run-shard", "aggregate"):
        command = commands.add_parser(name)
        command.add_argument("--prepared-root", type=Path, required=True)
        command.add_argument("--profile", required=True)
        command.add_argument("--output-dir", type=Path, required=True)
        if name != "aggregate":
            command.add_argument("--model", choices=("small", "base", "multi"), required=True)
        if name != "prepare":
            command.add_argument("--source-reference-report", type=Path)
            command.add_argument("--native-reference-report", type=Path)
        if name == "run-shard":
            command.add_argument("--model-dir", type=Path, required=True)
            command.add_argument("--backend", choices=("python", "native", "metal"), required=True)
            command.add_argument("--shard", type=int, choices=(0, 1, 2), required=True)
            command.add_argument("--binary", type=Path)
            command.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
            command.add_argument("--max-rss-mib", type=int, default=6144)
            command.add_argument("--startup-timeout", type=int, default=180)
            command.add_argument("--response-timeout", type=int, default=125)
        if name == "aggregate":
            command.add_argument("--shard-report", type=Path, action="append", required=True,
                                 help="repeat exactly three times, in shard order")
    return result


def main():
    args = parser().parse_args()
    if args.command == "worker":
        python_worker(args)
        return
    report = {"prepare": prepare_fixtures, "run-shard": run_shard, "aggregate": aggregate}[args.command](args)
    print(contract.encoded({key: report[key] for key in ("scope", "qualification", "denominator", "status", "errors", "required_parity") if key in report}).decode())
    if (report.get("status", "complete") != "complete" or report.get("errors", 0) or
            report.get("required_parity") and not report["required_parity"]["pass"] or
            report.get("required_token_parity") and not report["required_token_parity"]["pass"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
