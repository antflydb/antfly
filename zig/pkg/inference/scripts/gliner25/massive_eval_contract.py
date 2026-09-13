#!/usr/bin/env python3
"""Additive MASSIVE shard admission and output contracts. No inference imports."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any

import audit_massive11_preparation as preparation
import check_bundles as comparison
import evaluate
import evaluation_contract as contract
import oracle
import prepare_massive11 as massive

HERE = Path(__file__).resolve().parent
REGISTRY = HERE / "massive11_execution.json"
WORKER_SCOPE = "gliner25_massive11_blinded_execution/v2"
SHARD_SCOPE = "gliner25_massive11_execution_shard/v1"
AGGREGATE_SCOPE = "gliner25_massive11_execution_aggregate/v1"
RECORDS = 2974
RANGES = ((0, 1024), (1024, 2048), (2048, RECORDS))
MAX_FIXTURE_BYTES = 8 * 1024**2
MAX_EVENT_BYTES = 4 * 1024**2
MAX_SHARD_BYTES = 64 * 1024**2
MAX_REPORT_BYTES = 8 * 1024**2
POLICY = {"max_words": 128, "max_encoded_tokens": 512, "max_text_bytes": 1024**2,
          "max_queries": 64, "timeout_ms": 120000}
LIMITS = {"max_classification_labels": 78, "exact_node_budget": 200000,
          "beam_node_budget": 200000, "beam_width": 16, "max_candidates_per_task": 64,
          "max_local_assignments": 4096, "max_subset_visits": 65536,
          "max_output_values": 2048, "max_output_string_bytes": 1024**2,
          "max_request_host_bytes": 512 * 1024**2, "max_event_bytes": MAX_EVENT_BYTES,
          "max_shard_bytes": MAX_SHARD_BYTES}


def check(condition: bool, message: str):
    contract.checked(condition, message)


def read_json(path: Path, limit=MAX_REPORT_BYTES):
    check(0 < path.stat().st_size <= limit, "JSON file byte budget exceeded")
    return oracle.read_json(path)


def pin(path: Path, name: str | None = None):
    return {"path": name or path.name, "size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}


def atomic_json(path: Path, value):
    """Publish a complete receipt once; leave any existing receipt untouched."""
    data = contract.encoded(value) + b"\n"
    check(len(data) <= MAX_REPORT_BYTES, "receipt byte budget exceeded")
    descriptor, temporary = tempfile.mkstemp(prefix=".massive-receipt-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        # link is atomic and fails when the final receipt already exists.
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_registry():
    value = read_json(REGISTRY)
    check(value.get("scope") == "gliner25_massive11_execution_registry/v1" and
          value.get("qualification") is False and value.get("records") == RECORDS and
          value.get("source_commit") == oracle.UPSTREAM_COMMIT, "execution registry identity differs")
    for name, expected in value["frozen_helpers"].items():
        check(name in ("evaluate.py", "benchmark_cpu.py", "check_bundles.py", "evaluation_contract.py",
                       "oracle.py", "prepare_crossner_ai.py") and oracle.sha256_file(HERE / name) == expected,
              "frozen CrossNER helper changed")
    check(len(value["frozen_helpers"]) == 6, "frozen helper closure differs")
    for name, expected in (("massive11_preparation.json", value["preparation_receipt_sha256"]),
                           (massive.MANIFEST.name, value["corpus_manifest_sha256"]),
                           ("prepare_massive11.py", value["adapter_sha256"])):
        check(oracle.sha256_file(HERE / name) == expected, "approved preparation identity differs")
    manifest = read_json(massive.MANIFEST)
    check([row["profile"] for row in value["profiles"]] == massive.profile_definitions(manifest),
          "fixed profile inventory differs")
    check(value["models"] == {name: evaluate.source_identity(name) for name in ("small", "base", "multi")},
          "published source model identity differs")
    return value


def profile_entry(profile: str, registry=None):
    registry = registry or load_registry()
    rows = [entry for entry in registry["profiles"] if entry["profile"]["id"] == profile]
    check(len(rows) == 1, "unapproved MASSIVE profile")
    return rows[0]


def contract_files():
    registry = load_registry()
    names = (*registry["frozen_helpers"], "prepare_massive11.py", "audit_massive11_preparation.py", "capture_massive_execution_registry.py",
             "massive_eval_contract.py", "evaluate_massive11.py", "massive11_execution.json",
             "massive11_manifest.json", "massive11_preparation.json")
    return {name: oracle.sha256_file(HERE / name) for name in names}


def request_for(fixture, case):
    # The approved preparation reads options from its sorted-key lock JSON.
    # Preserve that canonical request order independently of envelope ordering.
    options = {key: fixture["options"][key] for key in ("best_effort", "overlap", "threshold", "word_splitter")}
    return {"text": case["text"], "schema": fixture["schema"], "options": options,
            "offset_unit": fixture["offset_unit"]}


def validate_fixture(value, registry=None):
    registry = registry or load_registry()
    check(isinstance(value, dict) and set(value) == {
        "format_version", "scope", "qualification", "source_commit", "model", "source_files",
        "registry_sha256", "profile", "lock_sha256", "prepared_sha256", "requests_sha256",
        "adapter_sha256", "harness_sha256", "policy", "limits", "schema", "options", "offset_unit",
        "transport", "cases"}, "MASSIVE envelope fields differ")
    check(value["format_version"] == 2 and value["scope"] == WORKER_SCOPE and value["qualification"] is False and
          value["source_commit"] == oracle.UPSTREAM_COMMIT and value["model"] in registry["models"] and
          value["source_files"] == registry["models"][value["model"]]["source_files"] and
          value["registry_sha256"] == oracle.sha256_file(REGISTRY) and value["policy"] == POLICY and value["limits"] == LIMITS,
          "MASSIVE envelope identity or resource policy differs")
    entry = profile_entry(value["profile"], registry)
    for key in ("lock_sha256", "prepared_sha256", "requests_sha256"):
        check(value[key] == entry[key], "MASSIVE prepared identity differs")
    check(value["adapter_sha256"] == registry["adapter_sha256"] and
          value["harness_sha256"] == registry["frozen_helpers"]["evaluation_contract.py"], "adaptation identity differs")
    schema, _ = massive.schema_and_metrics(entry["profile"], read_json(massive.MANIFEST))
    check(value["schema"] == schema and contract.digest(contract.encoded(value["schema"])) == entry["schema_sha256"] and
          value["options"] == {"threshold": 0.5, "overlap": "flat", "best_effort": False,
                               "word_splitter": entry["profile"]["word_splitter"]} and
          value["offset_unit"] == "utf8_bytes", "fixed schema, splitter or inference options differ")
    transport = value["transport"]
    check(isinstance(transport, dict) and type(transport.get("index")) is int and
          0 <= transport["index"] < len(RANGES), "unapproved shard index")
    expected = {"global_records": RECORDS, "transport_sha256": entry["transport_sha256"],
                **entry["shards"][transport["index"]]}
    check(transport == expected and (transport["start"], transport["end"]) == RANGES[transport["index"]],
          "shard identity, global range or denominator differs")
    cases = value["cases"]
    check(isinstance(cases, list) and len(cases) == transport["end"] - transport["start"], "incomplete shard")
    for case in cases:
        check(isinstance(case, dict) and set(case) == {"id", "source_id", "request_sha256", "text"},
              "blinded case contains missing, extra or gold fields")
        check(isinstance(case["text"], str) and isinstance(case["source_id"], str) and
              case["source_id"].startswith("massive/1.1/" + entry["profile"]["locale"] + "/test/"),
              "case text or original source identity differs")
        check(case["id"] == contract.digest(contract.encoded({"lock": value["lock_sha256"], "id": case["source_id"]})) and
              case["request_sha256"] == contract.digest(contract.encoded(request_for(value, case))),
              "case ID or blinded request digest differs")
    for key, field in (("request_ids_sha256", "id"), ("request_sha256s_sha256", "request_sha256")):
        check(contract.digest(contract.encoded([case[field] for case in cases])) == transport[key],
              "shard changed, duplicated or reordered a locked request")
    check(len({case["id"] for case in cases}) == len(cases), "duplicate shard request ID")
    return entry


def admit_profile(root: Path, profile: str):
    """Audit exact raw/normalized split pins and reconstruct blinded/gold rows separately."""
    registry = load_registry()
    entry = profile_entry(profile, registry)
    directory = root / profile
    for relative, key in (("lock.json", "lock_sha256"), ("prepared/prepared.json", "prepared_sha256"),
                          ("prepared/requests.jsonl", "requests_sha256"), ("prepared/gold.jsonl", "gold_sha256"),
                          ("transport.json", "transport_sha256")):
        check(oracle.sha256_file(directory / relative) == entry[key], "approved profile files changed")
    with tempfile.TemporaryDirectory(prefix="gliner25-massive-admit-") as temporary:
        reconstructed = Path(temporary) / "prepared"
        contract.prepare(directory / "lock.json", reconstructed)
        for name in ("prepared.json", "requests.jsonl", "gold.jsonl"):
            check((reconstructed / name).read_bytes() == (directory / "prepared" / name).read_bytes(),
                  "prepared requests or gold differ from immutable test rows")
    requests = list(contract.rows(directory / "prepared/requests.jsonl"))
    test_rows = list(contract.rows(directory / "test.jsonl"))
    check(len(requests) == len(test_rows) == RECORDS, "locked denominator differs")
    preparation.validate_transport(directory, requests, pin(directory / "transport.json"),
                                   entry["lock_sha256"], entry["prepared_sha256"])
    cases = [{"id": request["request_id"], "source_id": row["id"],
              "request_sha256": request["request_sha256"], "text": request["request"]["text"]}
             for request, row in zip(requests, test_rows, strict=True)]
    return entry, requests, cases


def make_fixture(entry, requests, cases, model: str, index: int):
    registry = load_registry()
    check(model in registry["models"] and type(index) is int and 0 <= index < len(RANGES), "invalid model or shard")
    start, end = RANGES[index]
    request = requests[0]["request"]
    result = {"format_version": 2, "scope": WORKER_SCOPE, "qualification": False,
        "source_commit": oracle.UPSTREAM_COMMIT, "model": model,
        "source_files": registry["models"][model]["source_files"], "registry_sha256": oracle.sha256_file(REGISTRY),
        "profile": entry["profile"]["id"], **{key: entry[key] for key in ("lock_sha256", "prepared_sha256", "requests_sha256")},
        "adapter_sha256": registry["adapter_sha256"], "harness_sha256": registry["frozen_helpers"]["evaluation_contract.py"],
        "policy": POLICY, "limits": LIMITS, **{key: request[key] for key in ("schema", "options", "offset_unit")},
        "transport": {"global_records": RECORDS, "transport_sha256": entry["transport_sha256"], **entry["shards"][index]},
        "cases": cases[start:end]}
    validate_fixture(result, registry)
    check(len(contract.encoded(result)) + 1 <= MAX_FIXTURE_BYTES, "shared-schema fixture exceeds byte budget")
    return result


def canonical_output(request, output, backend, profile):
    manifest = read_json(massive.MANIFEST)
    check(isinstance(output, dict), "model output is not an object")
    facts = massive.prediction_facts(request, output, backend, profile, manifest)
    if backend != "python":
        check(output.get("relations", []) == [] and output.get("structures", []) == [], "unrequested task output")
    if profile["task"] == "entities":
        check(set(output) == {"entities"} if backend == "python" else output.get("classifications", []) == [],
              "unrequested classification output")
        groups = output["entities"] if backend == "python" else {group["name"]: group["values"] for group in output["entities"]}
        confidences = [value["confidence"] for name in request["schema"]["entities"] for value in groups[name]]
        check(len(confidences) <= LIMITS["max_output_values"] and len(confidences) == len(facts["entity_exact"]),
              "canonical entity output cap or routing differs")
        canonical = {"entities": [{**fact, "confidence": confidence} for fact, confidence in zip(facts["entity_exact"], confidences)]}
    else:
        task_names = [task["name"] for task in request["schema"]["classifications"]]
        if backend == "python":
            check(set(output) <= set(task_names) | {"_meta"}, "undeclared classification output")
            groups = {name: {"label": output[name].get("label", output[name].get("value")),
                             "confidence": output[name]["confidence"]} for name in task_names}
        else:
            check(all(not group["multi_label"] for group in output["classifications"]), "single task emitted multi-label output")
            groups = {group["name"]: group["labels"][0] for group in output["classifications"]}
        if profile["task"] == "intent_scenario":
            check(facts["constraints_satisfied"] == [{"valid": True}], "strict classification emitted an invalid witness")
            diagnostic = output.get("_meta") if backend == "python" else output.get("classification_solver")
            check(isinstance(diagnostic, dict), "constrained classification omitted solver evidence")
            if backend == "python":
                check(diagnostic.get("feasible") is True and diagnostic.get("violations") == [] and
                      type(diagnostic.get("exact")) is bool, "source constrained result is not feasible")
            else:
                check(diagnostic.get("status") in ("optimal", "feasible") and diagnostic.get("exhausted") is False,
                      "native constrained result exhausted or infeasible")
        canonical = {"classifications": [{"name": name, "label": groups[name]["label"],
                                           "confidence": groups[name]["confidence"]} for name in task_names]}
    return facts, canonical


def response_prediction(fixture, case, response, backend):
    check(isinstance(response, dict) and response.get("event") in ("result", "error") and
          response.get("case_id") == case["id"] and response.get("request_sha256") == case["request_sha256"],
          "missing, duplicated, substituted or reordered result")
    entry = profile_entry(fixture["profile"])
    if response["event"] == "error":
        check(set(response) == {"event", "case_id", "request_sha256", "error_code", "input_ids"} and
              isinstance(response["error_code"], str) and 0 < len(response["error_code"]) <= 256 and
              response["input_ids"] is None, "untyped or malformed worker error")
        _, metrics = massive.schema_and_metrics(entry["profile"], read_json(massive.MANIFEST))
        # Explicit diagnostic failure; never relabel this event as an empty success.
        facts, canonical = {name: [] for name in metrics}, None
    else:
        check(set(response) == {"event", "case_id", "request_sha256", "input_ids", "output"}, "unexpected result fields")
        evaluate.validate_tokens(response["input_ids"])
        facts, canonical = canonical_output(request_for(fixture, case), response["output"], backend, entry["profile"])
    return {"request_id": case["id"], "request_sha256": case["request_sha256"], "metrics": facts}, canonical


def compare_response(fixture, case, response, backend, reference, reference_backend):
    _, actual = response_prediction(fixture, case, response, backend)
    _, expected = response_prediction(fixture, case, reference, reference_backend)
    if actual is None or expected is None:
        return {"parity_pass": False, "error_event": True,
                "both_error": actual is None and expected is None, "token_ids_equal": None}
    tokens_equal = response["input_ids"] == reference["input_ids"]
    result = comparison.compare_backends(expected, actual)
    result["token_ids_equal"] = tokens_equal
    result["parity_pass"] = tokens_equal and result["parity_pass"]
    return result
