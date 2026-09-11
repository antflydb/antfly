#!/usr/bin/env python3
"""Bounded trained-adapter merge integrity and optional official PEFT parity.

Additive to the frozen export and evaluation tools. Tolerances are versioned,
never caller-tunable. Numerical runtime executes in one supervised process.
"""
from __future__ import annotations

import argparse
import array
import contextlib
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import sys
import tempfile
import time
import warnings
import weakref

import benchmark_cpu as bench
import check_bundles as comparison
import check_training_export as exports
import oracle

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "training_merge_contract.json"
SCOPE = "gliner25_training_merge_compatibility/v1"
RECEIPT = "antfly_gliner25_merge.json"
PHASES = ("unmerged", "peft_merged", "native_merged")
TOLERANCES = {"adapted_absolute": 1e-6, "adapted_relative": 1e-5, "confidence_absolute": 5e-4,
              "untouched": "exact_bytes", "bias": "exact_bytes", "sidecars": "exact_bytes",
              "token_ids": "exact", "decisions": "exact"}
HELPERS = ("oracle.py", "oracle_manifest.json", "benchmark_cpu.py", "check_bundles.py",
           "generate_pipeline_cases.py", "check_training_export.py", "training_export_contract.json",
           "training_export_runtime.py", "training_export_peft018.json", "../paired_benchmark.py")
MAX_JSON = 8 * 1024**2
CHUNK = 1024**2
NATIVE_RECEIPT = {"family": "gliner_boundary_merge/v1", "version": 1, "architecture_version": 1,
                  "config_version": 3, "tensor_policy_version": 1,
                  "math_policy": "f32_lora_delta_f64_dora_row_norm_v1"}
DEFAULT_LIMITS = {"max_rss_mib": 6144, "runtime_deadline_seconds": 600, "event_deadline_seconds": 180,
    "max_runtime_copy_bytes": 4 * 1024 * CHUNK, "max_output_bytes": 32 * CHUNK, "max_report_bytes": 8 * CHUNK,
    "max_stderr_bytes": 8 * CHUNK, "max_events": 128, "max_tensor_comparison_chunk_bytes": CHUNK}
REQUEST_PIN = {"size_bytes": 3467, "sha256": "030c979419cee5fd209ba6a6f23f2e0cc7744e396d4bf3357925651293f5fe5f"}


def checked(condition, message):
    exports.checked(condition, message)


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def object_digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def read_json(path, maximum=MAX_JSON):
    with exports.Opened(path, maximum) as source:
        return exports.decode(b"".join(source.chunks())), source.pin()


def pin(path, maximum):
    with exports.Opened(path, maximum) as source:
        return source.pin()


def atomic_json(path, value, maximum=MAX_JSON):
    """Publish once; an incomplete check keeps its own failure receipt."""
    data = encoded(value) + b"\n"
    checked(len(data) <= maximum, "merge report exceeds byte budget")
    descriptor, temporary = tempfile.mkstemp(prefix=".merge-report-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_contract():
    contract, contract_pin = read_json(CONTRACT, 65536)
    checked(contract.get("scope") == SCOPE and type(contract.get("version")) is int and contract["version"] == 1 and
            contract.get("qualification") is False and contract.get("upstream_commit") == oracle.UPSTREAM_COMMIT and
            contract.get("tolerances") == TOLERANCES and contract.get("tensor_count") == 334 and
            contract.get("comparison_phases") == list(PHASES) and contract.get("request_count") == 10 and
            contract.get("runtime_profile") == "peft-0.18.0-export-v1" and
            contract.get("native_receipt") == NATIVE_RECEIPT and contract.get("default_limits") == DEFAULT_LIMITS and
            contract.get("requests") == REQUEST_PIN and
            set(contract.get("frozen_helpers", {})) == set(HELPERS), "merge contract identity differs")
    checked(comparison.CONFIDENCE_TOLERANCE == TOLERANCES["confidence_absolute"], "frozen output tolerance differs")
    for name in HELPERS:
        exports.verify_pin(pin(HERE / name, 2 * CHUNK), contract["frozen_helpers"][name], name)
    return contract, contract_pin


def helper_identity():
    contract, contract_pin = load_contract()
    return {"checker": pin(Path(__file__), CHUNK), "contract": contract_pin,
            "frozen_helpers": contract["frozen_helpers"]}


def request_fixture(contract):
    path = oracle.FIXTURES / "requests.json"
    document, actual = read_json(path, CHUNK)
    exports.verify_pin(actual, contract["requests"], "fixed ten-request fixture")
    checked(document.get("format_version") == 1 and len(document.get("requests", [])) == contract["request_count"],
            "merge request inventory differs")
    checked(len({item["id"] for item in document["requests"]}) == contract["request_count"], "duplicate merge request")
    return document["requests"], actual


def normalized_identity(value):
    checked(isinstance(value, dict) and set(value) == {"backbone", "precision", "weight", "sidecars"} and
            value["backbone"] in ("small", "base", "multi") and value["precision"] == "fp32" and
            isinstance(value["sidecars"], list) and len(value["sidecars"]) == 4, "invalid merged/source identity")
    return {"backbone": value["backbone"], "precision": "fp32", "weight": exports.normalized_pin(value["weight"]),
            "sidecars": [exports.normalized_pin(item) for item in value["sidecars"]]}


def finite_file_tensor(source, descriptor):
    for block in source.chunks(descriptor["offset"], descriptor["size_bytes"]):
        values = array.array("f")
        values.frombytes(block)
        if sys.byteorder != "little":
            values.byteswap()
        checked(all(math.isfinite(value) for value in values), "nonfinite adapted merge tensor")


def validate_job(configuration, audit, receipt, merged_dir):
    """Inspect semantic identities; artifact relocation does not follow old paths."""
    required = {"version", "source_dir", "adapter_dir", "output_dir", "expected_source", "expected_adapter", "schema_sha256"}
    optional = {"source_limits", "merge_limits", "memory", "timeout_seconds", "disk_headroom_bytes"}
    checked(isinstance(configuration, dict) and required <= set(configuration) <= required | optional and
            type(configuration.get("version")) is int and configuration["version"] == 1,
            "invalid materialization job version")
    for name in ("source_dir", "adapter_dir", "output_dir"):
        path = configuration[name]
        checked(isinstance(path, str) and 1 < len(path.encode("utf-8")) <= 4096 and path.startswith("/") and
                all(part not in ("", ".", "..") for part in path[1:].split("/")) and
                all(ord(character) >= 32 and ord(character) != 127 for character in path), "invalid materialization path")
    checked(configuration["output_dir"] not in (configuration["source_dir"], configuration["adapter_dir"]),
            "materialization output aliases an input")
    def limit_object(value, specifications):
        checked(isinstance(value, dict) and set(value) <= set(specifications), "unrecognized materialization limit")
        for name, item in value.items():
            minimum, maximum = specifications[name]
            checked(type(item) is int and minimum <= item <= maximum, "invalid materialization limit: " + name)
    limit_object({key: configuration[key] for key in ("timeout_seconds", "disk_headroom_bytes") if key in configuration},
                 {"timeout_seconds": (1, 86400), "disk_headroom_bytes": (0, 16 * 1024 * CHUNK)})
    limit_object(configuration.get("source_limits", {}), {
        "max_source_bytes": (1, 4 * 1024 * CHUNK), "max_auxiliary_bytes": (1, 1024 * CHUNK),
        "max_weight_bytes": (1, 2 * 1024 * CHUNK), "max_tokenizer_bytes": (1, 64 * CHUNK),
        "max_config_bytes": (1, CHUNK), "max_header_bytes": (1, 64 * CHUNK), "max_json_depth": (1, 128)})
    limit_object(configuration.get("memory", {}), {
        "combined_bytes": (1, 16 * 1024 * CHUNK), "job_bytes": (CHUNK, 64 * CHUNK),
        "adapter_auxiliary_bytes": (1, 512 * CHUNK), "max_adapter_owner_bytes": (1, 4 * 1024 * CHUNK)})
    merge_limits = configuration.get("merge_limits", {})
    checked(isinstance(merge_limits, dict), "invalid materialization merge limits")
    limit_object({key: value for key, value in merge_limits.items() if key != "adapter"}, {
        "max_scratch_bytes": (512 * 1024 + 1, 2 * 1024 * CHUNK), "max_output_bytes": (1, 16 * 1024 * CHUNK),
        "max_header_bytes": (1, 64 * CHUNK), "max_receipt_bytes": (1, CHUNK)})
    limit_object(merge_limits.get("adapter", {}), {
        "max_config_bytes": (1, CHUNK), "max_receipt_bytes": (1, CHUNK), "max_tensor_bytes": (1, 2 * 1024 * CHUNK),
        "max_tensor_header_bytes": (1, 64 * CHUNK), "max_targets": (1, 4096), "max_rank": (1, 1024),
        "max_merge_tensor_bytes": (1, 1024 * CHUNK)})
    checked(normalized_identity(configuration["expected_source"]) == receipt["source"], "merge job source differs")
    checked(exports.hex_digest(configuration["schema_sha256"]) == exports.hex_digest(audit["provenance"]["schemas_sha256"]),
            "merge job schema differs")
    expected = configuration["expected_adapter"]
    checked(isinstance(expected, dict) and set(expected) == {"config", "weights", "receipt"}, "merge job adapter fields differ")
    for key, value in receipt["provenance"]["adapter_files"].items():
        checked((expected[key] is None and value is None) or
                (expected[key] is not None and value is not None and exports.normalized_pin(expected[key]) == exports.normalized_pin(value)),
                "merge job adapter pin differs: " + key)
    # The raw configuration digest is independently checked. Its recorded path
    # strings remain provenance only; no loader follows them during checking.
    del merged_dir


def audit_merge(variant, source_dir, adapter_dir, run_dir, merged_dir, job_config, contract):
    audit = exports.audit_export(variant, source_dir, adapter_dir, run_dir)
    checked(audit["mode"] in ("lora", "dora"), "merge check requires a trained standard adapter")
    checked(len(audit["source_tensors"]) == contract["tensor_count"], "merge requires the complete 334-tensor source")
    configuration, configuration_pin = read_json(job_config, 65536)
    raw, receipt_pin = read_json(Path(merged_dir) / RECEIPT, 65536)
    required = {"family", "version", "architecture_version", "config_version", "tensor_policy_version", "math_policy",
                "source", "provenance", "target_sha256", "parameter_sha256", "merged", "tensor_count", "merged_tensor_count"}
    checked(isinstance(raw, dict) and set(raw) == required and
            all(raw.get(key) == value and type(raw[key]) is type(value) for key, value in contract["native_receipt"].items()),
            "unsupported merge receipt")
    receipt = dict(raw, source=normalized_identity(raw["source"]), merged=normalized_identity(raw["merged"]))
    source_identity = exports.source_identity(variant, audit["source_files"])
    checked(receipt["source"] == source_identity, "materializer source identity differs")
    provenance = receipt["provenance"]
    checked(isinstance(provenance, dict) and set(provenance) == {"configuration", "adapter_files", "schema_sha256"},
            "invalid merge provenance")
    exports.verify_pin(configuration_pin, provenance["configuration"], "exact materialization job bytes")
    expected_files = {"config": audit["export_files"]["adapter_config.json"],
                      "weights": audit["export_files"]["adapter_model.safetensors"],
                      "receipt": audit["export_files"][exports.ADAPTER_RECEIPT]}
    checked(isinstance(provenance["adapter_files"], dict) and set(provenance["adapter_files"]) == set(expected_files),
            "merge adapter provenance fields differ")
    for key, expected in expected_files.items():
        exports.verify_pin(expected, provenance["adapter_files"][key], "consumed adapter " + key)
    adapter_receipt, _ = exports.read_json(Path(adapter_dir) / exports.ADAPTER_RECEIPT)
    for key in ("target_sha256", "parameter_sha256"):
        checked(exports.hex_digest(receipt[key]) == exports.hex_digest(adapter_receipt[key]), "merge adapter digest differs: " + key)
    checked(exports.hex_digest(provenance["schema_sha256"]) == exports.hex_digest(audit["provenance"]["schemas_sha256"]),
            "merge schema differs")
    validate_job(configuration, audit, receipt, merged_dir)
    adapted = {module["name"] + ".weight" for module in audit["adapter_modules"]}
    checked(type(receipt["tensor_count"]) is int and receipt["tensor_count"] == contract["tensor_count"] and
            type(receipt["merged_tensor_count"]) is int and receipt["merged_tensor_count"] == len(adapted) and bool(adapted),
            "merge tensor counts differ")
    files = {RECEIPT: receipt_pin}
    exports.validate_tree(Path(merged_dir), {RECEIPT, "model.safetensors", *exports.SIDECARS})
    for name in exports.SIDECARS:
        files[name] = pin(Path(merged_dir) / name, 32 * CHUNK)
        exports.verify_pin(files[name], audit["source_files"][name], "unchanged merge sidecar " + name)
    with exports.Opened(Path(merged_dir) / "model.safetensors", exports.MAX_WEIGHT) as file:
        header = exports.tensor_header(file)
        exports.validate_inventory(header, audit["source_tensors"])
        tensors = exports.tensor_inventory(file, header)
        files["model.safetensors"] = file.pin()
        for name in adapted:
            finite_file_tensor(file, header[name])
    checked(receipt["merged"] == exports.source_identity(variant, files), "merged file identity differs from receipt")
    untouched = sorted(set(tensors) - adapted)
    checked(all(tensors[name] == audit["source_tensors"][name] for name in untouched),
            "merged artifact changed an untouched tensor or bias")
    checked(all(not name.endswith(".bias") for name in adapted), "adapter unexpectedly targets bias")
    return {"variant": variant, "mode": audit["mode"], "training_export": audit,
            "configuration": configuration_pin, "merge_receipt": receipt, "merged_files": files,
            "merged_tensors": tensors, "adapted_names": sorted(adapted), "untouched_names": untouched,
            "untouched_and_bias_bytes_equal": True, "sidecars_equal": True,
            "static_scope": "complete_inventory_exact_untouched_bytes_finite_adapted_tensors_no_merge_math_claim"}


def numeric_comparison(expected, actual):
    """Streaming scalar reference used by pure tests; runtime vectorizes chunks."""
    count = violations = 0
    maximum_absolute = maximum_relative = 0.0
    first = []
    sentinel = object()
    from itertools import zip_longest
    for index, (left, right) in enumerate(zip_longest(expected, actual, fillvalue=sentinel)):
        checked(left is not sentinel and right is not sentinel, "merge tensor comparison lengths differ")
        checked(type(left) in (float, int) and type(right) in (float, int) and math.isfinite(left) and math.isfinite(right),
                "nonfinite merge tensor comparison")
        delta = abs(float(left) - float(right))
        denominator = abs(float(left))
        maximum_absolute = max(maximum_absolute, delta)
        maximum_relative = max(maximum_relative, delta / denominator if denominator else 0.0)
        if delta > TOLERANCES["adapted_absolute"] + TOLERANCES["adapted_relative"] * denominator:
            violations += 1
            if len(first) < 8:
                first.append(index)
        count += 1
    return {"elements": count, "violations": violations, "max_absolute_error": maximum_absolute,
            "max_relative_error_nonzero_reference": maximum_relative, "first_violation_indices": first}


def decision_view(value, path=()):
    """Preserve public fields; probabilities use the existing confidence bound."""
    if isinstance(value, dict):
        if path and path[-1] == "probabilities":
            return {name: {"confidence": item} for name, item in value.items()}
        return {name: decision_view(item, (*path, name)) for name, item in value.items()
                if not (path == ("_meta",) and name == "objective")}
    if isinstance(value, list):
        return [decision_view(item, (*path, index)) for index, item in enumerate(value)]
    return value


def compare_phases(captures, requests):
    checked(set(captures) == set(PHASES), "incomplete merge comparison phases")
    checked(all(len(captures[phase]) == len(requests) for phase in PHASES), "incomplete merge request denominator")
    comparisons = []
    for left_name, right_name in (("unmerged", "peft_merged"), ("peft_merged", "native_merged"), ("unmerged", "native_merged")):
        rows = []
        for request, left, right in zip(requests, captures[left_name], captures[right_name], strict=True):
            digest = object_digest(request)
            checked(left.get("id") == right.get("id") == request["id"] and
                    left.get("request_sha256") == right.get("request_sha256") == digest,
                    "merge output request identity differs")
            for record in (left, right):
                ids = record.get("input_ids")
                checked(isinstance(ids, list) and 0 < len(ids) <= oracle.MAX_ENCODED_TOKENS and
                        all(type(token) is int and 0 <= token < 2**32 for token in ids), "invalid merge input IDs")
            result = comparison.compare(decision_view(left["output"]), decision_view(right["output"]))
            result.update(id=request["id"], token_ids_equal=left["input_ids"] == right["input_ids"])
            result["pass"] = result["token_ids_equal"] and result["fp32_reference_tolerance_pass"]
            rows.append(result)
        comparisons.append({"left": left_name, "right": right_name, "requests": rows, "pass": all(row["pass"] for row in rows)})
    return comparisons


def python_identity():
    # Preserve the invoked venv path. Hash the resolved executable separately.
    invocation = Path(os.path.abspath(sys.executable))
    configuration = Path(sys.prefix) / "pyvenv.cfg"
    environment = pin(configuration, 65536) if configuration.exists() else None
    checked(sys.prefix == sys.base_prefix or environment is not None, "missing selected Python environment")
    return {"invocation": str(invocation), "executable": str(invocation.resolve(strict=True)),
            "executable_bytes": pin(invocation.resolve(strict=True), 256 * CHUNK),
            "prefix": sys.prefix, "base_prefix": sys.base_prefix, "pyvenv_cfg": environment,
            "environment": {name: os.environ.get(name) for name in
                ("PYTHONHOME", "PYTHONPATH", "PYTHONNOUSERSITE", "PYTHONSAFEPATH", "PYTHONUTF8", "VIRTUAL_ENV")},
            "flags": {name: getattr(sys.flags, name) for name in
                ("no_site", "no_user_site", "ignore_environment", "isolated", "safe_path", "utf8_mode")}}


def prepared_input_ids(model, request):
    if request["kind"] == "extract":
        schema = oracle.build_extract_schema(request["schema"])
    elif request["kind"] == "classification":
        from gliner2.classification import Classifier, ClassificationSchema
        schema = Classifier(model).compile_schema(ClassificationSchema.from_dict(request["schema"]))
    elif request["kind"] == "joint_ie":
        from gliner2.joint_ie import JointIE, JointSchema
        schema = JointIE(model).compile_schema(JointSchema.from_dict(request["schema"]))
    else:
        raise oracle.ContractError("unsupported merge request family")
    batch = oracle.bounded_batch(model, request["text"], schema)
    return batch.input_ids[0].tolist()


def capture_outputs(model, requests, phase, torch, emit):
    captures = []
    for request in requests:
        expected_ids = prepared_input_ids(model, request)
        with bench.capture_encoder_input_ids(model) as observed, torch.inference_mode():
            output = bench.execute_python(model, request, encoded(request["schema"]).decode())
        checked(observed == [expected_ids], "actual merge encoder differs from bounded preparation")
        # JSON roundtrip forbids tensor/owner references escaping this frame.
        captured = exports.decode(encoded({"id": request["id"], "request_sha256": object_digest(request),
                                          "input_ids": expected_ids, "output": output}))
        checked(len(encoded(captured)) <= bench.MAX_RESPONSE_BYTES - 1024, "merge case output exceeds budget")
        captures.append(captured)
        emit({"event": "case", "phase": phase, "case": captured})
    return captures


def load_unmerged(audit, stage, torch):
    from gliner2 import AutoExtractor
    from peft import PeftModel
    from peft.utils.save_and_load import get_peft_model_state_dict
    report = audit["training_export"]
    base = AutoExtractor.from_pretrained(str(stage / "source"), local_files_only=True,
        map_location="cpu", use_flashdeberta=False, quantize=False, compile=False)
    exports.verify_loaded_state(base.state_dict(), report["source_tensors"], torch)
    with warnings.catch_warnings():
        warnings.filterwarnings("error", message="Found missing adapter keys.*", category=UserWarning)
        wrapped = PeftModel.from_pretrained(base, str(stage / "adapter"), is_trainable=False,
            local_files_only=True, autocast_adapter_dtype=False)
    exports.verify_loaded_state(get_peft_model_state_dict(wrapped), report["export_tensors"], torch)
    state = base.state_dict()
    modules = {module["name"] for module in report["adapter_modules"]}
    restored = {}
    try:
        for name in report["source_tensors"]:
            module, leaf = name.rsplit(".", 1)
            restored[name] = state[module + ".base_layer." + leaf if module in modules else name]
        exports.verify_loaded_state(restored, report["source_tensors"], torch)
    finally:
        restored.clear()
        state.clear()
    return wrapped.eval()


def compare_merged_tensors(model, audit, merged_path, torch):
    """One bounded chunk at a time; all state_dict/tensor views die here."""
    state = model.state_dict()
    expected = audit["merged_tensors"]
    adapted = set(audit["adapted_names"])
    checked(set(state) == set(expected), "PEFT merged full tensor inventory differs")
    results = []
    try:
        with exports.Opened(merged_path, exports.MAX_WEIGHT) as source:
            descriptors = exports.tensor_header(source)
            exports.verify_pin(source.pin(), audit["merged_files"]["model.safetensors"], "consumed native merged weights")
            for name in sorted(expected):
                tensor = state[name].detach()
                checked(tensor.dtype == torch.float32 and tensor.device.type == "cpu" and
                        list(tensor.shape) == expected[name]["shape"], "PEFT merged tensor shape/dtype differs: " + name)
                if name not in adapted:
                    exports.verify_loaded_state({name: tensor}, {name: expected[name]}, torch)
                    results.append({"name": name, "kind": "exact", "elements": expected[name]["size_bytes"] // 4,
                                    "bytes_equal": True})
                    continue
                checked(tensor.is_contiguous(), "PEFT merged matrix must be contiguous")
                values = tensor.view(-1)
                elements = violations = 0
                maximum_absolute = maximum_relative = 0.0
                first = []
                for block in source.chunks(descriptors[name]["offset"], descriptors[name]["size_bytes"]):
                    right = torch.frombuffer(bytearray(block), dtype=torch.float32).to(torch.float64)
                    left = values[elements:elements + right.numel()].to(torch.float64)
                    checked(bool(torch.isfinite(left).all() and torch.isfinite(right).all()), "nonfinite merged matrix: " + name)
                    delta = (left - right).abs()
                    magnitude = left.abs()
                    failed = delta > TOLERANCES["adapted_absolute"] + TOLERANCES["adapted_relative"] * magnitude
                    maximum_absolute = max(maximum_absolute, float(delta.max().item()))
                    nonzero = magnitude > 0
                    if bool(nonzero.any()):
                        maximum_relative = max(maximum_relative, float((delta[nonzero] / magnitude[nonzero]).max().item()))
                    violations += int(failed.sum().item())
                    if len(first) < 8:
                        first.extend(elements + int(index) for index in failed.nonzero().view(-1)[:8 - len(first)].tolist())
                    elements += right.numel()
                checked(elements == expected[name]["size_bytes"] // 4, "merged matrix element count differs")
                results.append({"name": name, "kind": "numerical", "elements": elements, "violations": violations,
                    "max_absolute_error": maximum_absolute, "max_relative_error_nonzero_reference": maximum_relative,
                    "first_violation_indices": first})
    finally:
        state.clear()
    return results


def source_forms(audit, stage, requests, torch, emit):
    model = merged = None
    model_ref = merged_ref = None
    try:
        model = load_unmerged(audit, stage, torch)
        model_ref = weakref.ref(model)
        emit({"event": "phase", "phase": "unmerged_loaded"})
        unmerged = capture_outputs(model, requests, "unmerged", torch, emit)
        # PEFT's released implementation is the numerical reference. The
        # wrapper owns the same base model, not another full checkpoint.
        merged = model.merge_and_unload(safe_merge=True)
        merged_ref = weakref.ref(merged)
        model = None
        gc.collect()
        checked(model_ref() is None, "PEFT wrapper still retained before merged execution")
        checked(not any("lora_" in name for name in merged.state_dict()), "PEFT merge left adapter tensor keys")
        merged.eval()
        emit({"event": "phase", "phase": "peft_merged"})
        tensors = compare_merged_tensors(merged, audit, stage / "merged/model.safetensors", torch)
        outputs = capture_outputs(merged, requests, "peft_merged", torch, emit)
    finally:
        model = merged = None
        gc.collect()
    checked(model_ref is not None and merged_ref is not None and model_ref() is None and merged_ref() is None,
            "source model owner remains live before native-artifact load")
    emit({"event": "phase", "phase": "source_owner_released"})
    return {"unmerged": unmerged, "peft_merged": outputs}, tensors


def native_form(audit, stage, requests, torch, emit):
    from gliner2 import AutoExtractor
    model = None
    model_ref = None
    try:
        model = AutoExtractor.from_pretrained(str(stage / "merged"), local_files_only=True,
            map_location="cpu", use_flashdeberta=False, quantize=False, compile=False)
        model_ref = weakref.ref(model)
        exports.verify_loaded_state(model.state_dict(), audit["merged_tensors"], torch)
        model.eval()
        emit({"event": "phase", "phase": "native_merged_loaded"})
        outputs = capture_outputs(model, requests, "native_merged", torch, emit)
    finally:
        model = None
        gc.collect()
    checked(model_ref is not None and model_ref() is None, "native merged model owner remains live")
    emit({"event": "phase", "phase": "native_owner_released"})
    return outputs


def validate_tensor_results(results, audit):
    expected = audit["merged_tensors"]
    adapted = set(audit["adapted_names"])
    checked(isinstance(results, list) and len(results) == len(expected) and
            {row.get("name") for row in results} == set(expected), "incomplete or repeated tensor comparisons")
    for row in results:
        name = row["name"]
        checked(type(row.get("elements")) is int and row["elements"] == expected[name]["size_bytes"] // 4,
                "tensor comparison denominator differs")
        if name not in adapted:
            checked(set(row) == {"name", "kind", "elements", "bytes_equal"} and row["kind"] == "exact" and
                    row["bytes_equal"] is True, "untouched tensor comparison must be exact")
        else:
            checked(set(row) == {"name", "kind", "elements", "violations", "max_absolute_error",
                    "max_relative_error_nonzero_reference", "first_violation_indices"} and row["kind"] == "numerical" and
                    type(row["violations"]) is int and 0 <= row["violations"] <= row["elements"], "invalid adapted comparison")
            checked(all(type(row[key]) in (float, int) and math.isfinite(row[key]) and row[key] >= 0 for key in
                ("max_absolute_error", "max_relative_error_nonzero_reference")), "invalid tensor error summary")
            indices = row["first_violation_indices"]
            checked(isinstance(indices, list) and len(indices) == min(8, row["violations"]) and
                    all(type(index) is int and 0 <= index < row["elements"] for index in indices) and
                    indices == sorted(set(indices)), "invalid tensor violation evidence")
    return all(row.get("violations", 0) == 0 for row in results)


def runtime_check(audit, args, output, contract, emit):
    checked(sys.byteorder == "little", "runtime merge comparison requires a little-endian host")
    checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "merge runtime requires a fresh process")
    from training_export_runtime import load_profile
    profile = load_profile()
    copies = sum(item["size_bytes"] for item in profile["wheel_files"].values())
    destinations = (("source", args.source_dir, audit["training_export"]["source_files"]),
                    ("adapter", args.adapter_dir, audit["training_export"]["export_files"]),
                    ("merged", args.merged_dir, audit["merged_files"]))
    copies += sum(item["size_bytes"] for _, _, files in destinations for item in files.values())
    checked(copies <= args.max_runtime_copy_bytes, "aggregate merge runtime copies exceed admission")
    requests, request_pin = request_fixture(contract)
    with tempfile.TemporaryDirectory(prefix=".merge-runtime-", dir=output) as temporary, exports.deny_network():
        stage = Path(temporary)
        for name, source, files in destinations:
            exports.private_copy(source, stage / name, files, args.max_runtime_copy_bytes)
        with exports.runtime_context(args.upstream, output, contract["runtime_profile"], args.peft_wheel) as (provenance, torch):
            captures, tensors = source_forms(audit, stage, requests, torch, emit)
            captures["native_merged"] = native_form(audit, stage, requests, torch, emit)
            tensor_pass = validate_tensor_results(tensors, audit)
            comparisons = compare_phases(captures, requests)
            # Verify copies through exact descriptors after all model owners
            # and transient state_dict/hooks/captures have been released.
            for name, _, files in destinations:
                for filename, expected in files.items():
                    exports.verify_pin(pin(stage / name / filename, exports.MAX_WEIGHT), expected, "consumed runtime copy")
            result = {"status": "verified" if tensor_pass and all(row["pass"] for row in comparisons) else "comparison_failed",
                "qualification": False, "provenance": provenance, "loader_profile": contract["runtime_profile"],
                "tolerances": TOLERANCES, "requests": request_pin, "captures": captures, "comparisons": comparisons,
                "tensor_comparisons": tensors, "tensor_parity_pass": tensor_pass, "one_resident_model_owner": True,
                "owners_released_before_next_load": True, "private_copy_bytes": copies,
                "network_allowed": False, "missing_weight_fallback": False, "quality_evaluation": False}
        # The context updates source/PEFT import provenance after its yield.
        return result


def event_plan(requests):
    result = [("phase", "unmerged_loaded", None)]
    for phase in PHASES:
        if phase == "peft_merged":
            result.append(("phase", "peft_merged", None))
        if phase == "native_merged":
            result.extend((("phase", "source_owner_released", None), ("phase", "native_merged_loaded", None)))
        result.extend(("case", phase, request["id"]) for request in requests)
    result.append(("phase", "native_owner_released", None))
    return result


def validate_runtime_result(result, audit, requests, observed):
    checked(isinstance(result, dict) and result.get("qualification") is False and
            result.get("tolerances") == TOLERANCES and result.get("loader_profile") == "peft-0.18.0-export-v1" and
            result.get("one_resident_model_owner") is True and result.get("owners_released_before_next_load") is True and
            result.get("network_allowed") is False and result.get("missing_weight_fallback") is False and
            result.get("quality_evaluation") is False and result.get("captures") == observed and
            result.get("requests") == REQUEST_PIN and type(result.get("private_copy_bytes")) is int and
            result["private_copy_bytes"] > 0,
            "runtime merge report differs from consumed events")
    tensor_pass = validate_tensor_results(result.get("tensor_comparisons"), audit)
    comparisons = compare_phases(observed, requests)
    checked(result.get("comparisons") == comparisons and result.get("tensor_parity_pass") is tensor_pass,
            "runtime merge comparisons were substituted")
    success = tensor_pass and all(row["pass"] for row in comparisons)
    checked(result.get("status") == ("verified" if success else "comparison_failed"), "runtime merge status differs")
    return success


def serialized_args(args):
    keys = ("variant", "source_dir", "adapter_dir", "run_dir", "merged_dir", "job_config", "upstream",
            "peft_wheel", "max_runtime_copy_bytes")
    return {key: str(value.resolve()) if isinstance(value := getattr(args, key), Path) else value for key in keys}


class MergeGuard(bench.ResourceGuard):
    def __init__(self, maximum, output, deadline, limits):
        super().__init__(maximum)
        self.output, self.deadline, self.limits = output, deadline, limits

    def check(self):
        super().check()
        checked(time.monotonic() <= self.deadline, "merge runtime absolute deadline exceeded")
        for worker in self.workers:
            checked(worker.log_path.stat().st_size <= self.limits["max_stderr_bytes"], "merge stderr byte budget exceeded")


def supervise_runtime(audit, args, output, contract):
    limits = contract["default_limits"]
    requests, _ = request_fixture(contract)
    invocation = python_identity()
    helpers = helper_identity()
    scratch = output / ".runtime-scratch"
    scratch.mkdir(mode=0o700)
    envelope = {"version": 1, "scope": SCOPE, "args": serialized_args(args), "python": invocation,
                "helpers": helpers, "audit_sha256": object_digest(audit), "output_dir": str(output.resolve()),
                "scratch_dir": str(scratch.resolve())}
    input_path = output / "worker.json"
    atomic_json(input_path, envelope, CHUNK)
    input_pin = pin(input_path, CHUNK)
    command = [invocation["invocation"], str(Path(__file__).resolve()), "worker", str(input_path), input_pin["sha256"]]
    env = os.environ.copy()
    env.update({name: "1" for name in bench.THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", TOKENIZERS_PARALLELISM="false")
    env.pop("USE_FLASHDEBERTA", None)
    deadline = time.monotonic() + args.runtime_deadline_seconds
    guard = MergeGuard(args.max_rss_mib * CHUNK, output, deadline, limits)
    worker = None
    observed = {phase: [] for phase in PHASES}
    plan = event_plan(requests)
    count = 0
    result = None
    protocol_complete = False
    try:
        worker = bench.Worker("merge", command, env, output, guard)
        with (output / "runtime.events.jsonl").open("xb") as events:
            while True:
                remaining = min(args.event_deadline_seconds, deadline - time.monotonic())
                checked(remaining > 0, "merge runtime absolute deadline exceeded")
                event = worker.receive(remaining)
                count += 1
                data = encoded(event) + b"\n"
                checked(count <= limits["max_events"] and events.tell() + len(data) <= limits["max_output_bytes"],
                        "merge event count/output byte budget exceeded")
                events.write(data)
                events.flush()
                if event.get("event") == "error":
                    raise oracle.ContractError("merge worker failed: " + str(event.get("error")))
                if count == 1:
                    checked(event == {"event": "ready", "scope": SCOPE, "qualification": False,
                        "audit_sha256": envelope["audit_sha256"], "python": invocation, "helpers": helpers},
                        "merge worker input/runtime identity differs")
                    continue
                if event.get("event") == "complete":
                    checked(count == len(plan) + 2 and set(event) == {"event", "runtime"}, "incomplete merge runtime phases")
                    result = event["runtime"]
                    validate_runtime_result(result, audit, requests, observed)
                    checked(result["private_copy_bytes"] <= args.max_runtime_copy_bytes,
                            "runtime copy receipt exceeds admission")
                    events.flush()
                    os.fsync(events.fileno())
                    break
                checked(count - 2 < len(plan), "unexpected extra merge phase")
                kind, phase, identifier = plan[count - 2]
                checked(event.get("event") == kind and event.get("phase") == phase and
                        set(event) == ({"event", "phase"} if kind == "phase" else {"event", "phase", "case"}),
                        "merge owner/phase order differs")
                if kind == "case":
                    checked(event["case"].get("id") == identifier, "merge case order differs")
                    observed[phase].append(event["case"])
        exit_deadline = min(deadline, time.monotonic() + 5)
        while worker.process.poll() is None:
            guard.check()
            checked(time.monotonic() < exit_deadline, "merge worker failed to exit after complete result")
            time.sleep(.05)
        checked(worker.process.returncode == 0 and not worker.buffer and not worker.process.stdout.read(1),
                "merge worker exited unsuccessfully or emitted extra output")
        checked(helpers == helper_identity() and invocation == python_identity() and pin(input_path, CHUNK) == input_pin,
                "merge checker/interpreter/input changed during execution")
        protocol_complete = True
    finally:
        cleanup_error = None
        try:
            if worker is not None:
                worker.close()
            # Only this newly-created private directory is removed, after the
            # worker is reaped. SIGTERM/KILL cannot strand model-sized copies.
            shutil.rmtree(scratch)
        except Exception as error:
            cleanup_error = {"type": type(error).__name__, "message": str(error)[:1024]}
            raise
        finally:
            atomic_json(output / "runtime.process.json", {"qualification": False, "python": invocation,
                "worker_input": input_pin, "peak_worker_rss_bytes": guard.peak_rss_bytes,
                "max_worker_rss_bytes": args.max_rss_mib * CHUNK, "absolute_deadline_seconds": args.runtime_deadline_seconds,
                "event_deadline_seconds": args.event_deadline_seconds, "events": count,
                "complete_protocol": protocol_complete, "scratch_cleaned": not scratch.exists(),
                "cleanup_error": cleanup_error,
                "worker_exit_code": None if worker is None else worker.process.returncode})
    return result


def worker_entry(path, expected_sha):
    envelope, actual = read_json(path, CHUNK)
    checked(actual["sha256"] == expected_sha and set(envelope) ==
        {"version", "scope", "args", "python", "helpers", "audit_sha256", "output_dir", "scratch_dir"} and
        type(envelope["version"]) is int and envelope["version"] == 1 and envelope["scope"] == SCOPE,
        "invalid merge worker envelope")
    checked(envelope["python"] == python_identity() and envelope["helpers"] == helper_identity(),
            "merge worker Python/helper identity differs before runtime import")
    allowed = {"variant", "source_dir", "adapter_dir", "run_dir", "merged_dir", "job_config", "upstream",
               "peft_wheel", "max_runtime_copy_bytes"}
    checked(isinstance(envelope["args"], dict) and set(envelope["args"]) == allowed, "invalid merge worker arguments")
    arguments = dict(envelope["args"])
    for key in allowed - {"variant", "max_runtime_copy_bytes"}:
        checked(isinstance(arguments[key], str) and Path(arguments[key]).is_absolute(), "merge worker paths must be absolute")
        arguments[key] = Path(arguments[key])
    args = argparse.Namespace(**arguments)
    output = Path(envelope["output_dir"])
    scratch = Path(envelope["scratch_dir"])
    checked(output.is_absolute() and scratch == output / ".runtime-scratch" and scratch.is_dir() and not scratch.is_symlink(),
            "invalid merge worker scratch ownership")
    contract, _ = load_contract()
    checked(type(args.max_runtime_copy_bytes) is int and 0 < args.max_runtime_copy_bytes <= 8 * 1024 * CHUNK,
            "invalid merge worker copy admission")
    audit = audit_merge(args.variant, args.source_dir, args.adapter_dir, args.run_dir, args.merged_dir, args.job_config, contract)
    checked(object_digest(audit) == envelope["audit_sha256"], "merge worker consumed different source/artifact bytes")
    bench.emit({"event": "ready", "scope": SCOPE, "qualification": False, "audit_sha256": envelope["audit_sha256"],
                "python": envelope["python"], "helpers": envelope["helpers"]})
    result = runtime_check(audit, args, scratch, contract, bench.emit)
    checked(audit == audit_merge(args.variant, args.source_dir, args.adapter_dir, args.run_dir, args.merged_dir, args.job_config, contract) and
            envelope["helpers"] == helper_identity() and envelope["python"] == python_identity(),
            "merge source/artifact/runtime changed during execution")
    bench.emit({"event": "complete", "runtime": result})


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--variant", choices=("small", "base", "multi"), required=True)
    for name in ("source-dir", "adapter-dir", "run-dir", "merged-dir", "job-config", "output-dir"):
        result.add_argument("--" + name, type=Path, required=True)
    result.add_argument("--runtime", action="store_true", help="load models only in the shared serial compute lane")
    result.add_argument("--peft-wheel", type=Path, help="exact official wheel for the immutable PEFT0.18 export profile")
    result.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    result.add_argument("--max-runtime-copy-bytes", type=int, default=4 * 1024 * CHUNK)
    result.add_argument("--max-rss-mib", type=int, default=6144)
    result.add_argument("--runtime-deadline-seconds", type=int, default=600)
    result.add_argument("--event-deadline-seconds", type=int, default=180)
    return result


def run(args):
    contract, contract_pin = load_contract()
    checked(args.runtime is (args.peft_wheel is not None), "runtime requires exactly one pinned PEFT wheel")
    checked(0 < args.max_runtime_copy_bytes <= 8 * 1024 * CHUNK and 256 <= args.max_rss_mib <= 8192 and
            1 <= args.runtime_deadline_seconds <= 1800 and 1 <= args.event_deadline_seconds <= 600,
            "invalid merge runtime resource limits")
    output = args.output_dir.resolve()
    for directory in (args.source_dir, args.adapter_dir, args.run_dir, args.merged_dir):
        checked(not output.is_relative_to(directory.resolve()), "merge report must be outside input artifacts")
    checked(not args.output_dir.is_symlink(), "merge output path must not be a symlink")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    result = {"scope": SCOPE, "qualification": False, "status": "incomplete", "numerical_runtime_executed": False,
              "numerical_runtime_requested": args.runtime,
              "contract": contract_pin, "helpers": helper_identity(), "tolerances": TOLERANCES}
    try:
        audit = audit_merge(args.variant, args.source_dir, args.adapter_dir, args.run_dir, args.merged_dir, args.job_config, contract)
        result["audit"] = audit
        result["status"] = "static_verified"
        if args.runtime:
            # Incomplete transport cannot prove whether execution reached a
            # numerical phase. The preserved events provide the last evidence.
            result["numerical_runtime_executed"] = None
            runtime = supervise_runtime(audit, args, output, contract)
            result["runtime"] = runtime
            result["numerical_runtime_executed"] = True
            result["status"] = runtime["status"]
            checked(audit == audit_merge(args.variant, args.source_dir, args.adapter_dir, args.run_dir, args.merged_dir, args.job_config, contract),
                    "merge input artifacts changed before report publication")
        checked(result["helpers"] == helper_identity(), "merge helper closure changed before publication")
        atomic_json(output / "report.json", result, contract["default_limits"]["max_report_bytes"])
    except (Exception, KeyboardInterrupt) as error:
        result.update(status="incomplete", failure={"type": type(error).__name__, "message": str(error)[:8192]})
        atomic_json(output / "failure.json", result, contract["default_limits"]["max_report_bytes"])
        raise
    return result


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "worker":
        checked(len(argv) == 3, "invalid internal merge worker command")
        try:
            worker_entry(Path(argv[1]), argv[2])
        except (Exception, KeyboardInterrupt) as error:
            bench.emit({"event": "error", "error": {"type": type(error).__name__, "message": str(error)[:8192]}})
            raise SystemExit(1)
        return
    report = run(parser().parse_args(argv))
    print(encoded({"scope": SCOPE, "status": report["status"], "qualification": False,
                   "numerical_runtime_executed": report["numerical_runtime_executed"]}).decode())
    if report["status"] == "comparison_failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
