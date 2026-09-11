"""Explicit scaling-only adaptation of the shared FP32 runtime receipts."""
from __future__ import annotations

import hashlib
import json

import metal_runtime_contract_v2 as runtime
import metal_python_worker as reference
import scaling_contract_v1 as contract

v1, cpu, oracle = runtime.v1, reference.bench, reference.oracle


def ready(arm, value, inputs, execution_policy):
    if (value.get("scope") != contract.SCOPE or value.get("workload") != "scaling"
            or value.get("requests_sha256") != inputs["requests_sha256"]
            or value.get("cases_sha256") != inputs["pins"]["native_inputs"]["sha256"]):
        raise cpu.BenchmarkError("scaling worker input/scope identity differs")
    # The original checker hard-codes the ten-request digest. Validate the new
    # raw source digest above, then reuse only its remaining device/math fields.
    adapted = {**value, "scope": runtime.SCOPE,
               "requests_sha256": oracle.sha256_file(oracle.FIXTURES / "requests.json")}
    if arm == v1.NATIVE:
        return runtime.native_ready(adapted, inputs["bundle"], inputs["model"],
                                    inputs["case_path"], execution_policy)
    if value.get("scaling_input_pins") != inputs["pins"]:
        raise cpu.BenchmarkError("source worker scaling manifest identity differs")
    runtime.reference_ready(arm, adapted, inputs["bundle"], inputs["model"],
                            inputs["case_path"], execution_policy)
    return None


def outputs(response, expected_count):
    raw = response.get("outputs")
    if not isinstance(raw, list) or len(raw) != expected_count:
        raise cpu.BenchmarkError("scaling response omitted or reordered batch outputs")
    actual = [cpu.canonical_result(value) for value in raw]
    reference.finite_output(actual)
    if "output" in response:
        cpu.require_equal(actual[0], cpu.canonical_result(response["output"]), "compatibility_first_output")
    return actual


def check_source_spans(value, text):
    if isinstance(value, dict):
        span = value.get("source")
        if span is not None:
            if (not isinstance(span, dict) or set(span) != {"start", "end"}
                    or type(span["start"]) is not int or type(span["end"]) is not int
                    or not 0 <= span["start"] < span["end"] <= len(text)
                    or value.get("text") != text[span["start"]:span["end"]]):
                raise cpu.BenchmarkError("batch output has invalid original source coordinates")
        for item in value.values():
            check_source_spans(item, text)
    elif isinstance(value, list):
        for item in value:
            check_source_spans(item, text)


def result(arm, response, case, expected, *, validation, phase, native_receipt=None):
    if response.get("event") == "error":
        raise cpu.BenchmarkError(f"{response.get('category', 'worker_error')}: {response.get('message', '')}")
    if runtime.integer(response.get("duration_ns"), "duration_ns") == 0:
        raise cpu.BenchmarkError("scaling response has no completed extraction duration")
    if arm == v1.NATIVE:
        if native_receipt is None or native_receipt.legacy:
            raise cpu.BenchmarkError("scaling requires explicit native v2 ownership")
        # Account the completed request before any recoverable batch parity
        # failure; the next independent case sees the actual workspace owner.
        native_receipt.observe_ownership(response, validation=False, phase=phase)
    actual = outputs(response, len(case["items"]))
    for output, item in zip(actual, case["items"]):
        check_source_spans(output, item["text"])
    if expected is not None:
        cpu.require_equal(expected, actual, f"{arm}.outputs")
    if validation:
        contract.check_packet(response, case["expected_input_ids"])
    elif response.get("input_shape") != contract.packet(case["expected_input_ids"])["input_shape"]:
        raise cpu.BenchmarkError("timed scaling request changed its batch geometry")
    if arm != v1.NATIVE:
        if response.get("scope") != contract.SCOPE:
            raise cpu.BenchmarkError("source scaling response changed scope")
        if validation and reference.normalized_device(response.get("input_device")) != arm.removeprefix("fastino_"):
            raise cpu.BenchmarkError("source scaling validation ran on another device")
    return actual


def sample_receipt(response):
    result = {key: value for key, value in response.items() if key not in (
        "output", "outputs", "input_ids", "attention_mask", "input_device", "encoder_input_devices",
        "event", "arm", "request_id", "case_id")}
    result["outputs_sha256"] = hashlib.sha256(json.dumps(response.get("outputs"), sort_keys=True,
        ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()).hexdigest()
    return result
