#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run a sealed, phased native-repeat attention-matching campaign.

The runner grants development training authority only through a plan frozen
before capture. It never upgrades a fit, candidate, or campaign to serving or
release authority.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import subprocess
import sys
import time

SCRIPTS = Path(__file__).resolve().parent.parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import benchmark_resources
from context_release_build_adapter import validate as validate_build
import native_attention_matching as native
import native_repeat_attention_matching as preparer
import native_repeat_campaign_resources as combined
import native_repeat_training_authority as authority
import release_attention_matching as release


SPEC_SCHEMA = "antfly.context.native.repeat-campaign-spec.v1"
PHASE_SCHEMA = "antfly.context.native.repeat-campaign-phase.v1"
RESULT_SCHEMA = "antfly.context.native.repeat-campaign-results.v1"
HEX = re.compile(r"[0-9a-f]{64}")
IDENTITY_LIMITS = {"timeout_seconds": 300, "max_rss_mib": 8192,
    "min_free_percent": 15, "max_swap_growth_mib": 0,
    "min_disk_free_mib": 4096}
CAPTURE_LIMITS = {**IDENTITY_LIMITS, "timeout_seconds": 900}
FIT_LIMITS = {**CAPTURE_LIMITS, "max_rss_mib": 1024}
HEADROOM_PROFILE = {"profile": "loader_64pct_two_sample_v1",
                    "required_free_percent": 64, "samples": 2,
                    "interval_seconds": 10}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hex_digest(value, label):
    require(isinstance(value, str) and HEX.fullmatch(value), "invalid " + label)
    return value


def write_new(path, value):
    path = Path(path)
    with path.open("xb") as stream:
        stream.write((json.dumps(value, indent=2, sort_keys=True) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    return value


def json_checked(path, digest, maximum=16 * 1024 * 1024):
    data = native.checked(Path(path), hex_digest(digest, "SHA-256"))
    require(len(data) <= maximum, "JSON input exceeds byte limit")
    value = json.loads(data)
    require(type(value) is dict, "JSON input must be an object")
    return value


def ref(value, label):
    require(type(value) is dict and set(value) == {"path", "sha256"} and
            isinstance(value["path"], str) and value["path"] and
            HEX.fullmatch(value["sha256"] or ""), "invalid " + label + " reference")
    path = Path(value["path"])
    require(path.is_absolute(), label + " path must be absolute")
    return path, value["sha256"]


def load_spec(path, digest):
    path = Path(path)
    spec = json_checked(path, digest)
    require(set(spec) == {"schema", "source_build_receipt", "model",
                         "coverage_plan", "training_document",
                         "training_question", "expected_owners",
                         "capture_stem"} and spec["schema"] == SPEC_SCHEMA,
            "invalid native-repeat campaign spec")
    for field in ("source_build_receipt", "coverage_plan",
                  "training_document", "training_question"):
        ref(spec[field], field.replace("_", " "))
    model = spec["model"]
    require(type(model) is dict and set(model) == {
        "directory", "artifact_sha256", "models_root", "model_id"} and
        all(isinstance(model[key], str) and model[key]
            for key in ("directory", "models_root", "model_id")) and
        HEX.fullmatch(model.get("artifact_sha256", "")),
        "invalid campaign model binding")
    directory, models_root = Path(model["directory"]), Path(model["models_root"])
    model_id = Path(model["model_id"])
    require(directory.is_absolute() and models_root.is_absolute() and
            not model_id.is_absolute() and model_id.parts and
            all(part not in ("", ".", "..") for part in model_id.parts) and
            directory.resolve(strict=True) == (models_root / model_id).resolve(strict=True) and
            directory.is_dir() and models_root.is_dir() and
            not directory.is_symlink() and not models_root.is_symlink(),
            "model directory is not the declared models-root entry")
    owners = spec["expected_owners"]
    require(type(owners) is list and owners, "missing expected owner geometry")
    previous = -1
    for owner in owners:
        require(type(owner) is dict and set(owner) == {
            "owner_layer", "source_heads", "head_dim"} and
            type(owner["owner_layer"]) is int and owner["owner_layer"] > previous and
            type(owner["source_heads"]) is int and 0 < owner["source_heads"] <= 64 and
            type(owner["head_dim"]) is int and 0 < owner["head_dim"] <= 512 and
            owner["head_dim"] % 2 == 0, "invalid expected owner geometry")
        previous = owner["owner_layer"]
    require(isinstance(spec["capture_stem"], str) and
            re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", spec["capture_stem"]),
            "invalid capture stem")
    return spec


def campaign_root(path):
    path = Path(path)
    require(path.is_absolute() and path.is_dir() and not path.is_symlink(),
            "output must be an existing absolute regular directory")
    return path.resolve(strict=True)


def build_for(spec):
    path, expected = ref(spec["source_build_receipt"], "source build receipt")
    require(sha(path) == expected, "source build receipt SHA-256 mismatch")
    build = validate_build(path)
    require(build["receipt_sha256"] == expected,
            "source build receipt changed during validation")
    return build


def _phase_ref(path):
    return {"path": str(Path(path)), "sha256": sha(path)}


def _phase(output, name, spec_sha256, sources, **values):
    return write_new(Path(output) / (name + "-phase.json"), {
        "schema": PHASE_SCHEMA, "phase": name, "completed": True,
        "diagnostic_only": True, "serving_qualified": False,
        "release_authority": False, "campaign_spec_sha256": spec_sha256,
        "runner_sha256": sha(__file__), "sources": sources, **values})


def _phase_contract(phase, name, spec_sha256):
    require(type(phase) is dict and phase.get("schema") == PHASE_SCHEMA and
            phase.get("phase") == name and phase.get("completed") is True and
            phase.get("diagnostic_only") is True and
            phase.get("serving_qualified") is False and
            phase.get("release_authority") is False and
            phase.get("campaign_spec_sha256") == spec_sha256 and
            phase.get("runner_sha256") == sha(__file__) and
            type(phase.get("sources")) is dict,
            "missing bound " + name + " phase")
    return phase


def _bound_source(phase, name, expected):
    value = phase["sources"].get(name)
    require(type(value) is dict and set(value) == {"path", "sha256"} and
            HEX.fullmatch(value.get("sha256", "")),
            "invalid phase source: " + name)
    path, expected = Path(value["path"]), Path(expected)
    require(path.is_absolute() and path.is_file() and not path.is_symlink() and
            path.resolve(strict=True) == expected.resolve(strict=True) and
            sha(path) == value["sha256"], "changed phase source: " + name)
    return path


def _resource_contract(receipt_path, expected_command, limits, numerical=False):
    receipt_path = Path(receipt_path)
    receipt = json_checked(receipt_path, sha(receipt_path))
    require(receipt.get("schema") == combined.SCHEMA and
            receipt.get("rss_scope") == combined.RSS_SCOPE and
            receipt.get("command") == expected_command and
            receipt.get("limits") == limits and receipt.get("violation") is None and
            "cleanup_error" not in receipt,
            "guarded command/resource binding failed")
    allowed = {0, 1} if numerical else {0}
    returncode = receipt.get("returncode")
    require(type(returncode) is int and returncode in allowed and
            receipt.get("pass") is (returncode == 0) and
            type(receipt.get("campaign_parent_pid")) is int and
            receipt["campaign_parent_pid"] > 0 and
            type(receipt.get("child_pgid")) is int and receipt["child_pgid"] > 0 and
            type(receipt.get("samples")) is int and receipt["samples"] > 0 and
            receipt.get("remaining_child_group_rss_mib") == 0 and
            receipt.get("remaining_child_group_live_processes") == 0,
            "guarded command did not terminate cleanly")
    for field in ("peak_parent_rss_mib", "peak_child_group_rss_mib",
                  "peak_combined_rss_mib", "swapout_growth_mib",
                  "min_disk_free_mib", "min_free_percent",
                  "initial_free_percent", "elapsed_seconds"):
        require(type(receipt.get(field)) in (int, float) and
                math.isfinite(receipt[field]), "invalid guarded measurement: " + field)
    require(0 < receipt["peak_parent_rss_mib"] <= receipt["peak_combined_rss_mib"] <= limits["max_rss_mib"] and
            0 <= receipt["peak_child_group_rss_mib"] <= receipt["peak_combined_rss_mib"] and
            receipt["swapout_growth_mib"] == 0 and
            receipt["min_disk_free_mib"] >= limits["min_disk_free_mib"] and
            limits["min_free_percent"] <= receipt["min_free_percent"] <= receipt["initial_free_percent"] <= 100 and
            0 <= receipt["elapsed_seconds"] <= limits["timeout_seconds"],
            "guarded command exceeded resource limits")
    return receipt


def _headroom(output, phase):
    receipt_path = Path(output) / (phase + "-headroom.json")
    rows = []
    try:
        for index in range(HEADROOM_PROFILE["samples"]):
            raw = subprocess.check_output(
                ["/usr/bin/memory_pressure", "-Q"], text=True, timeout=30)
            match = re.search(r"free percentage:\s*(\d+)%", raw)
            require(match is not None, "unparseable loader headroom probe")
            free = int(match.group(1))
            rows.append({"free_percent": free, "unix_seconds": time.time()})
            require(free >= HEADROOM_PROFILE["required_free_percent"],
                    "loader headroom below admitted 64% precheck")
            if index + 1 < HEADROOM_PROFILE["samples"]:
                time.sleep(HEADROOM_PROFILE["interval_seconds"])
    except Exception:
        write_new(receipt_path, {**HEADROOM_PROFILE, "pass": False, "checks": rows})
        raise
    return write_new(receipt_path, {**HEADROOM_PROFILE, "pass": True, "checks": rows})


def _run(command, resource, limits, numerical=False):
    result = combined.run_guarded_combined(command, resource,
        timeout=limits["timeout_seconds"], max_rss_mib=limits["max_rss_mib"],
        min_free_percent=limits["min_free_percent"],
        max_swap_growth_mib=limits["max_swap_growth_mib"],
        min_disk_free_mib=limits["min_disk_free_mib"])
    _resource_contract(resource, command, limits, numerical=numerical)
    return result


def _identity_contract(value, spec):
    require(type(value) is dict and value.get("schema") ==
            "antfly.context.native.identity-bootstrap.v1" and
            value.get("diagnostic_only") is True and
            value.get("training_source_authority") is False and
            value.get("release_authority") is False and
            value.get("serving_qualified") is False and
            type(value.get("load_generation")) is int and value["load_generation"] > 0 and
            value.get("model_forward_steps") == 0,
            "invalid identity bootstrap result")
    identity = value.get("identity")
    require(type(identity) is dict and set(identity) == {
        "artifact", "tokenizer", "template", "layout", "implementation",
        "profile", "dtype", "kernel_route"} and
        all(HEX.fullmatch(identity[key] or "") for key in
            ("artifact", "tokenizer", "template", "layout", "implementation", "profile")) and
        identity["artifact"] == spec["model"]["artifact_sha256"] and
        identity["profile"] == "0" * 64 and identity["dtype"] == "f16_kv_f32_bias" and
        identity["kernel_route"] == "metal_context_optimized",
        "identity bootstrap model binding mismatch")
    require(value.get("owners") == spec["expected_owners"],
            "identity bootstrap owner geometry mismatch")
    return value


def phase_identity(spec, spec_sha256, output):
    build = build_for(spec)
    _headroom(output, "identity")
    artifact = Path(output) / "identity-bootstrap.json"
    require(not artifact.exists() and not artifact.is_symlink(),
            "identity bootstrap output already exists")
    command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
        "ANTFLY_CONTEXT_MODEL_DIR=" + spec["model"]["directory"],
        "ANTFLY_CONTEXT_IDENTITY_BOOTSTRAP_OUTPUT=" + str(artifact),
        str(build["binary_path"]), "--test-filter",
        "context Metal installed Gemma compaction identity bootstrap"]
    resource = Path(output) / "identity-resource.json"
    _run(command, resource, IDENTITY_LIMITS)
    value = _identity_contract(json_checked(artifact, sha(artifact)), spec)
    stderr = resource.with_suffix(".stderr")
    require(b"context_native_identity_bootstrap:" in native.checked(stderr, sha(stderr)),
            "identity bootstrap completion evidence missing")
    return _phase(output, "identity", spec_sha256, {
        "build_receipt": _phase_ref(build["receipt_path"]),
        "bootstrap": _phase_ref(artifact), "resource": _phase_ref(resource),
        "stdout": _phase_ref(resource.with_suffix(".stdout")),
        "stderr": _phase_ref(stderr),
        "headroom": _phase_ref(Path(output) / "identity-headroom.json")},
        model_forward_steps=value["model_forward_steps"], owners=value["owners"])


def _load_identity(spec, spec_sha256, output):
    phase_path = Path(output) / "identity-phase.json"
    phase = _phase_contract(json_checked(phase_path, sha(phase_path)),
                            "identity", spec_sha256)
    bootstrap_path = _bound_source(
        phase, "bootstrap", Path(output) / "identity-bootstrap.json")
    bootstrap = json_checked(bootstrap_path, phase["sources"]["bootstrap"]["sha256"])
    return _identity_contract(bootstrap, spec), phase_path


def _copy(source, expected, destination):
    data = native.checked(source, expected)
    with Path(destination).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return data


def _local_coverage_plan(spec, output):
    plan_path, plan_sha = ref(spec["coverage_plan"], "coverage plan")
    document_path, document_sha = ref(spec["training_document"], "training document")
    question_path, question_sha = ref(spec["training_question"], "training question")
    plan = json_checked(plan_path, plan_sha)
    require(plan.get("schema_version") == 2 and type(plan.get("cases")) is list and
            len(plan["cases"]) == 1 and type(plan.get("policy")) is dict,
            "native-repeat coverage plan must contain exactly one schema-v2 case")
    case = plan["cases"][0]
    require(case.get("document_sha256") == document_sha and
            case.get("question_sha256") == question_sha,
            "explicit training inputs differ from coverage plan")
    root = Path(output) / "coverage-inputs"
    root.mkdir()
    document = root / "document.txt"
    question = root / "question.txt"
    _copy(document_path, document_sha, document)
    _copy(question_path, question_sha, question)
    normalized = json.loads(json.dumps(plan))
    normalized["cases"][0]["document_file"] = document.name
    normalized["cases"][0]["question_file"] = question.name
    normalized_path = root / "coverage-plan.json"
    write_new(normalized_path, normalized)
    return normalized, root, normalized_path, plan_path, document, question


@contextmanager
def _environment(values):
    saved = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def phase_coverage(spec, spec_sha256, output):
    build = build_for(spec)
    bootstrap, identity_phase = _load_identity(spec, spec_sha256, output)
    _headroom(output, "coverage")
    plan, plan_root, normalized_path, source_plan, document, question = \
        _local_coverage_plan(spec, output)
    device = subprocess.check_output(["/usr/sbin/system_profiler",
        "SPHardwareDataType", "SPDisplaysDataType", "-json"], timeout=30)
    os_info = json.dumps({"sw_vers": subprocess.check_output(
        ["/usr/bin/sw_vers"], text=True, timeout=30),
        "machine": platform.machine(), "platform": platform.platform()},
        sort_keys=True, separators=(",", ":")).encode()
    device_path, os_path = Path(output) / "device-origin.json", Path(output) / "os-origin.json"
    with device_path.open("xb") as stream: stream.write(device)
    with os_path.open("xb") as stream: stream.write(os_info)
    identity = bootstrap["identity"]
    origin = {"binary_sha256": build["binary_sha256"],
        "source_build_sha256": build["receipt_sha256"],
        "device_sha256": sha(device_path), "os_sha256": sha(os_path)}
    command = [str(build["binary_path"]), "--test-filter",
               "serving qualification installed persistent Metal driver"]
    coverage = Path(output) / "coverage"
    model = spec["model"]
    with _environment({"ANTFLY_CONTEXT_SERVING_DRIVER": "1",
        "ANTFLY_CONTEXT_MODELS_DIR": model["models_root"],
        "ANTFLY_CONTEXT_MODEL": model["model_id"]}):
        result = release.collect_coverage_v2(command, identity, origin, plan,
            plan_root, build["receipt_path"], coverage, timeout=300,
            max_rss_mib=8192)
    session = json_checked(coverage / "session.json", sha(coverage / "session.json"))
    require(result.get("completed") is True and session.get("completed") is True and
            session.get("driver_command") == command and
            session.get("semantic_identity") == identity and
            session.get("provenance") == origin,
            "coverage collection did not seal the requested identity")
    return _phase(output, "coverage", spec_sha256, {
        "identity_phase": _phase_ref(identity_phase),
        "source_plan": _phase_ref(source_plan),
        "normalized_plan": _phase_ref(normalized_path),
        "training_document": _phase_ref(document),
        "training_question": _phase_ref(question),
        "device_origin": _phase_ref(device_path), "os_origin": _phase_ref(os_path),
        "coverage_session": _phase_ref(coverage / "session.json"),
        "headroom": _phase_ref(Path(output) / "coverage-headroom.json")},
        coverage_directory=str(coverage))


def phase_plan(spec, spec_sha256, output):
    coverage_phase = Path(output) / "coverage-phase.json"
    phase = _phase_contract(json_checked(coverage_phase, sha(coverage_phase)),
                            "coverage", spec_sha256)
    coverage = Path(output) / "coverage"
    require(phase.get("coverage_directory") == str(coverage),
            "coverage phase directory is not canonical")
    _bound_source(phase, "identity_phase", Path(output) / "identity-phase.json")
    _bound_source(phase, "normalized_plan", Path(output) / "coverage-inputs/coverage-plan.json")
    _bound_source(phase, "training_document", Path(output) / "coverage-inputs/document.txt")
    _bound_source(phase, "training_question", Path(output) / "coverage-inputs/question.txt")
    _load_identity(spec, spec_sha256, output)
    session_path = _bound_source(phase, "coverage_session", coverage / "session.json")
    session = json_checked(session_path, phase["sources"]["coverage_session"]["sha256"])
    require(session.get("completed") is True and
            len(session.get("coverage_receipts", [])) == 1,
            "native-repeat plan requires one completed coverage case")
    inputs = coverage / "capture-inputs-000000.json"
    require(inputs.is_file() and not inputs.is_symlink(),
            "coverage did not materialize one capture input")
    plan_path = coverage / "training-plan.json"
    owners = [{"owner_layer": owner["owner_layer"],
               "source_heads": owner["source_heads"]}
              for owner in spec["expected_owners"]]
    authority.create_plan(source_build_receipt=coverage / "source-build-receipt.json",
        capture_inputs=inputs, capture_inputs_sha256=sha(inputs),
        output_stem=spec["capture_stem"], owners=owners, output=plan_path)
    return _phase(output, "plan", spec_sha256, {
        "coverage_phase": _phase_ref(coverage_phase),
        "capture_inputs": _phase_ref(inputs), "training_plan": _phase_ref(plan_path),
        "coverage_build_receipt": _phase_ref(coverage / "source-build-receipt.json")},
        coordinates=json_checked(plan_path, sha(plan_path))["coordinates"])


def _coverage_from_plan_phase(spec_sha256, output):
    path = Path(output) / "plan-phase.json"
    phase = _phase_contract(json_checked(path, sha(path)), "plan", spec_sha256)
    coverage = Path(output) / "coverage"
    _bound_source(phase, "coverage_phase", Path(output) / "coverage-phase.json")
    plan_path = _bound_source(phase, "training_plan", coverage / "training-plan.json")
    _bound_source(phase, "capture_inputs", coverage / "capture-inputs-000000.json")
    _bound_source(phase, "coverage_build_receipt", coverage / "source-build-receipt.json")
    return coverage, plan_path, path


def phase_capture(spec, spec_sha256, output):
    coverage, plan, plan_phase = _coverage_from_plan_phase(spec_sha256, output)
    _headroom(output, "capture")
    build = validate_build(coverage / "source-build-receipt.json")
    inputs = coverage / "capture-inputs-000000.json"
    stem = spec["capture_stem"]
    command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
        "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE=1",
        "ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS=1",
        "ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT=1",
        "ANTFLY_CONTEXT_NATIVE_REPEAT_TRAINING_PLAN_SHA256=" + sha(plan),
        "ANTFLY_CONTEXT_MODEL_DIR=" + spec["model"]["directory"],
        "ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR=" + str(coverage),
        "ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST=" + inputs.name,
        "ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256=" + sha(inputs),
        "ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT=source-build-receipt.json",
        "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM=" + stem,
        "ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES=8589934592",
        str(build["binary_path"]), "--test-filter",
        "context Metal installed Gemma production query campaign capture"]
    resource = coverage / "capture-resource.json"
    _run(command, resource, CAPTURE_LIMITS)
    source = coverage / (stem + "-native-head-index.json")
    repeat = coverage / (stem + "-native-repeat-index.json")
    auth = coverage / "training-authority.json"
    authority.seal_authority(plan=plan, plan_sha256=sha(plan),
        source_index=source, source_sha256=sha(source),
        repeat_source=repeat, repeat_sha256=sha(repeat),
        capture_resource=resource, resource_sha256=sha(resource), output=auth)
    authority.validate_authority(auth, sha(auth))
    return _phase(output, "capture", spec_sha256, {
        "plan_phase": _phase_ref(plan_phase), "resource": _phase_ref(resource),
        "source_index": _phase_ref(source), "repeat_source": _phase_ref(repeat),
        "training_authority": _phase_ref(auth),
        "headroom": _phase_ref(Path(output) / "capture-headroom.json")})


def _qualification_report(report_path, input_path, accepted_returncode,
                          owner, kv_head, expected):
    report = json_checked(report_path, sha(report_path))
    fields = {"schema", "recipe", "accepted_head", "outcome", "serving_qualified",
        "full_context_memory_qualified", "input_sha256", "source_rows", "head_dim",
        "training_rows", "validation_rows", "consumer_groups", "exact_prefix_rows",
        "exact_recent_rows", "continuation_rows", "retained_row_cap", "attempts",
        "derived_attention_readback_bytes", "validation_readback_bytes",
        "diagnostic_candidate_readback_bytes"}
    new_report = "terminal_failure" in report
    bound_report = "candidate_sha256" in report or "candidate_bytes" in report
    extra_fields = ({"terminal_failure"} if new_report else set()) | (
        {"candidate_sha256", "candidate_bytes"} if bound_report else set())
    require((not bound_report or new_report) and
            set(report) == fields | extra_fields and
            report.get("schema") == "antfly.context.native.head-qualification.v2" and
            report.get("recipe") == native.RECIPE and
            type(report.get("accepted_head")) is bool and
            report.get("outcome") == ("accepted_head" if report["accepted_head"] else "preserved_original") and
            report.get("serving_qualified") is False and
            report.get("full_context_memory_qualified") is False and
            report.get("input_sha256") == sha(input_path) and
            report.get("source_rows") == expected["source_rows"] and
            report.get("head_dim") == expected["head_dim"] and
            report.get("training_rows") == expected["training_rows"] and
            report.get("validation_rows") == expected["validation_rows"] and
            report.get("consumer_groups") == expected["consumer_groups"] and
            report.get("exact_prefix_rows") == expected["prefix_rows"] and
            report.get("exact_recent_rows") == expected["recent_rows"] and
            report.get("continuation_rows") == expected["continuation_rows"] and
            report.get("retained_row_cap") == expected["retained_row_cap"] and
            type(report.get("attempts")) is list and 0 < len(report["attempts"]) <= 3,
            f"invalid native head qualification report for owner {owner} head {kv_head}")
    if bound_report:
        require((report["candidate_sha256"] is None or
                 isinstance(report["candidate_sha256"], str) and
                 HEX.fullmatch(report["candidate_sha256"])) and
                type(report["candidate_bytes"]) is int and
                report["candidate_bytes"] >= 0,
                "invalid native head candidate artifact fields")
    for field in ("derived_attention_readback_bytes", "validation_readback_bytes",
                  "diagnostic_candidate_readback_bytes"):
        require(type(report[field]) is int and report[field] >= 0,
                "invalid native head readback evidence")
    expected_divisor = 4
    attempt_outcomes = {"retained_row_budget_exceeded", "MassFitDidNotConverge",
        "CompactionQualityRejected", "whole_head_quality_rejected", "accepted_head"}
    if new_report:
        attempt_outcomes |= {"SingularValueFit", "numerical_parity_rejected"}
    compacted_rows = (report["source_rows"] - report["exact_prefix_rows"] -
                      report["exact_recent_rows"])
    require(compacted_rows > 0 and compacted_rows % 512 == 0 and
            compacted_rows // 512 <= 256,
            "invalid native head compacted block geometry")
    chunks = compacted_rows // 512
    for attempt in report["attempts"]:
        retained = (512 + expected_divisor - 1) // expected_divisor
        expected_rows = (report["exact_prefix_rows"] + chunks * retained +
                         report["exact_recent_rows"])
        old_attempt = {"divisor", "retained_rows", "outcome", "metrics", "reference"}
        new_attempt = old_attempt | {"elapsed_ns", "blocks"}
        require(type(attempt) is dict and set(attempt) == (new_attempt if new_report else old_attempt) and
            attempt["divisor"] == expected_divisor and
            attempt["retained_rows"] == expected_rows and
            attempt["outcome"] in attempt_outcomes and
            (attempt["metrics"] is None) == (attempt["reference"] is None),
            "invalid native head attempt sequence")
        over_budget = expected_rows + report["continuation_rows"] > report["retained_row_cap"]
        require((attempt["outcome"] == "retained_row_budget_exceeded") is over_budget,
                "native head retained-row outcome differs from frozen budget")
        if new_report:
            require(type(attempt["elapsed_ns"]) is int and
                    0 < attempt["elapsed_ns"] <= FIT_LIMITS["timeout_seconds"] * 1_000_000_000 and
                    type(attempt["blocks"]) is list and len(attempt["blocks"]) <= chunks,
                    "invalid native head attempt diagnostics")
            block_elapsed = 0
            for block_index, block in enumerate(attempt["blocks"]):
                require(type(block) is dict and set(block) == {"block", "logical_start",
                    "retained_rows", "outcome", "elapsed_ns", "solver"} and
                    block["block"] == block_index and
                    block["logical_start"] == report["exact_prefix_rows"] + block_index * 512 and
                    block["retained_rows"] == retained and
                    block["outcome"] in {"fitted", "MassFitDidNotConverge",
                        "SingularValueFit", "CompactionQualityRejected"} and
                    type(block["elapsed_ns"]) is int and
                    0 < block["elapsed_ns"] <= attempt["elapsed_ns"],
                    "invalid native head block diagnostics")
                block_elapsed += block["elapsed_ns"]
                solver = block["solver"]
                if block["outcome"] == "fitted":
                    require(type(solver) is dict and set(solver) == {"quality_evaluated",
                        "nnls_sweeps", "selection_steps", "selection_active_solves"} and
                        solver["quality_evaluated"] is False and
                        all(type(solver[field]) is int and 0 <= solver[field] <= 2**32 for field in
                            ("nnls_sweeps", "selection_steps", "selection_active_solves")),
                        "invalid native head solver diagnostics")
                else:
                    require(solver is None and block_index + 1 == len(attempt["blocks"]),
                            "numerical fit rejection must terminate its block sequence")
            require(block_elapsed <= attempt["elapsed_ns"],
                    "native head block timings exceed attempt timing")
            if attempt["outcome"] in {"MassFitDidNotConverge", "SingularValueFit",
                                      "CompactionQualityRejected"}:
                require(attempt["blocks"] and
                        attempt["blocks"][-1]["outcome"] == attempt["outcome"] and
                        attempt["metrics"] is None,
                        "fit rejection lacks terminal raw block evidence")
            elif attempt["outcome"] != "retained_row_budget_exceeded":
                require(len(attempt["blocks"]) == chunks and
                        all(block["outcome"] == "fitted" for block in attempt["blocks"]),
                        "completed attempt lacks all fitted block records")
            else:
                require(not attempt["blocks"] and attempt["metrics"] is None,
                        "retained-row rejection unexpectedly ran blocks")
        expected_divisor -= 1
        for name in ("metrics", "reference"):
            metrics = attempt[name]
            if metrics is None:
                continue
            require(type(metrics) is dict and set(metrics) == {"version", "queries",
                "consumer_groups", "max_mass_error", "output_rmse",
                "max_consumer_output_rmse"} and metrics["version"] == 2 and
                metrics["queries"] == report["validation_rows"] and
                metrics["consumer_groups"] == report["consumer_groups"],
                "invalid native head numerical metrics")
            for field in ("max_mass_error", "output_rmse", "max_consumer_output_rmse"):
                require(type(metrics[field]) in (int, float) and
                        math.isfinite(metrics[field]) and metrics[field] >= 0,
                        "invalid native head numerical metric")
        if attempt["metrics"] is not None and attempt["reference"] is not None:
            differences = []
            for field in ("max_mass_error", "output_rmse", "max_consumer_output_rmse"):
                differences.append(abs(attempt["metrics"][field] -
                                       attempt["reference"][field]))
            if attempt["outcome"] == "numerical_parity_rejected":
                require(any(value > 2e-5 for value in differences),
                        "parity rejection lacks divergent raw numerics")
            else:
                require(all(value <= 2e-5 for value in differences),
                        "native head numerical parity differs")
            passing = (max(attempt["metrics"]["max_mass_error"],
                           attempt["reference"]["max_mass_error"]) <= .05 and
                       max(attempt["metrics"]["output_rmse"],
                           attempt["reference"]["output_rmse"]) <= .10 and
                       max(attempt["metrics"]["max_consumer_output_rmse"],
                           attempt["reference"]["max_consumer_output_rmse"]) <= .10)
            if attempt["outcome"] != "numerical_parity_rejected":
                require((attempt["outcome"] == "accepted_head") is passing,
                        "native head outcome differs from raw numerics")
    require(all(attempt["outcome"] not in {"accepted_head", "numerical_parity_rejected"}
                for attempt in report["attempts"][:-1]),
            "native head ladder continued after terminal attempt")
    if report["accepted_head"]:
        final = report["attempts"][-1]
        require(final["outcome"] == "accepted_head" and
                final["metrics"] is not None and final["reference"] is not None and
                max(final["metrics"]["max_mass_error"], final["reference"]["max_mass_error"]) <= .05 and
                max(final["metrics"]["output_rmse"], final["reference"]["output_rmse"]) <= .10 and
                max(final["metrics"]["max_consumer_output_rmse"],
                    final["reference"]["max_consumer_output_rmse"]) <= .10,
                "accepted native head lacks passing raw numerics")
    else:
        terminal = report.get("terminal_failure") if new_report else None
        parity_failure = terminal == "NativeHeadNumericalParityFailed"
        require((parity_failure or report["attempts"][-1]["divisor"] == 2) and
                all(attempt["outcome"] != "accepted_head" for attempt in report["attempts"]),
                "rejected native head contains an accepted attempt")
        if new_report:
            expected_terminal = None
            for attempt in report["attempts"]:
                if attempt["outcome"] == "numerical_parity_rejected":
                    expected_terminal = "NativeHeadNumericalParityFailed"
                elif attempt["outcome"] in {"MassFitDidNotConverge", "SingularValueFit",
                                            "CompactionQualityRejected",
                                            "whole_head_quality_rejected"}:
                    expected_terminal = attempt["outcome"]
            require(terminal == expected_terminal,
                    "terminal native head failure differs from attempt evidence")
    require(accepted_returncode == (0 if report["accepted_head"] else 1),
            "native head return code does not match numerical verdict")
    if new_report:
        require(not report["accepted_head"] or report["terminal_failure"] is None,
                "accepted native head has a terminal failure")
        if report["terminal_failure"] == "NativeHeadNumericalParityFailed":
            raise RuntimeError("native head numerical parity failure is operational, not recipe rejection")
    return report


def validate_head_artifacts(report, report_path, candidate_path, stderr_path):
    """Validate the latest report/candidate bytes and its unique child marker.

    Legacy report shapes remain readable by ``_qualification_report`` for
    diagnostic history. This validator deliberately requires the latest bound
    shape before a fit result may be sealed.
    """
    report_path, candidate_path, stderr_path = (Path(report_path),
                                                Path(candidate_path),
                                                Path(stderr_path))
    require(type(report) is dict and
            "candidate_sha256" in report and "candidate_bytes" in report,
            "native head report lacks bound candidate artifacts")
    report_sha256 = sha(report_path)
    require(json_checked(report_path, report_sha256) == report,
            "native head report object differs from bound report bytes")
    candidate_sha256 = report["candidate_sha256"]
    candidate_bytes = report["candidate_bytes"]
    require(type(candidate_bytes) is int and candidate_bytes >= 0 and
            (candidate_sha256 is None or
             isinstance(candidate_sha256, str) and HEX.fullmatch(candidate_sha256)),
            "invalid native head candidate artifact fields")
    if report.get("accepted_head") is True:
        require(candidate_sha256 is not None and candidate_bytes > 0 and
                candidate_path.is_file() and not candidate_path.is_symlink(),
                "accepted native head lacks a bound candidate")
        retained_rows = report["attempts"][-1]["retained_rows"]
        expected_bytes = retained_rows * (4 * report["head_dim"] + 4)
        require(candidate_bytes == expected_bytes and
                candidate_path.stat().st_size == expected_bytes and
                sha(candidate_path) == candidate_sha256,
                "native head candidate digest or size differs from report")
        marker_candidate = candidate_sha256
    else:
        require(candidate_sha256 is None and candidate_bytes == 0 and
                not candidate_path.exists(),
                "rejected native head unexpectedly bound a candidate")
        marker_candidate = "none"
    expected_marker = ("context_native_head_artifacts: report_sha256=" +
        report_sha256 + " candidate_sha256=" + marker_candidate +
        " candidate_bytes=" + str(candidate_bytes))
    marker_lines = [line for line in stderr_path.read_text().splitlines()
                    if line.startswith("context_native_head_artifacts:")]
    require(marker_lines == [expected_marker],
            "native head artifact marker differs from synced artifacts")
    return {"report_sha256": report_sha256,
            "candidate_sha256": candidate_sha256,
            "candidate_bytes": candidate_bytes}


def phase_fit_legacy_diagnostic(spec, spec_sha256, output):
    capture_phase = Path(output) / "capture-phase.json"
    phase = _phase_contract(json_checked(capture_phase, sha(capture_phase)),
                            "capture", spec_sha256)
    coverage = Path(output) / "coverage"
    _bound_source(phase, "plan_phase", Path(output) / "plan-phase.json")
    _bound_source(phase, "resource", coverage / "capture-resource.json")
    _bound_source(phase, "source_index",
                  coverage / (spec["capture_stem"] + "-native-head-index.json"))
    _bound_source(phase, "repeat_source",
                  coverage / (spec["capture_stem"] + "-native-repeat-index.json"))
    auth_ref = phase["sources"]["training_authority"]
    auth = _bound_source(phase, "training_authority",
                         coverage / "training-authority.json")
    binding = authority.validate_authority(auth, auth_ref["sha256"])
    expected_coordinates = [(owner["owner_layer"], head)
        for owner in spec["expected_owners"] for head in range(owner["source_heads"])]
    coordinates = [(row["owner_layer"], row["kv_head"])
                   for row in binding["plan"]["coordinates"]]
    require(coordinates == expected_coordinates,
            "training authority coordinates differ from campaign spec")
    bootstrap, _ = _load_identity(spec, spec_sha256, output)
    identity = bootstrap["identity"]
    expected_identity = {"artifact_sha256": identity["artifact"],
        "tokenizer_sha256": identity["tokenizer"],
        "template_sha256": identity["template"],
        "layout_sha256": identity["layout"],
        "implementation_sha256": identity["implementation"]}
    actual_identity = binding["authority"].get("loaded_identity")
    require(type(actual_identity) is dict and
            type(actual_identity.get("load_generation")) is int and
            actual_identity["load_generation"] > 0 and
            {key: actual_identity.get(key) for key in expected_identity} == expected_identity,
            "training authority identity differs from bootstrap")
    owner_geometry = {owner["owner_layer"]: owner for owner in spec["expected_owners"]}
    cells = binding["authority"].get("cells")
    require(type(cells) is list and
            [(cell.get("owner_layer"), cell.get("kv_head")) for cell in cells] == expected_coordinates and
            all(cell.get("source_heads") == owner_geometry[cell["owner_layer"]]["source_heads"] and
                cell.get("head_dim") == owner_geometry[cell["owner_layer"]]["head_dim"]
                for cell in cells),
            "training authority cell geometry differs from bootstrap/spec")
    spec_build, spec_build_sha = ref(spec["source_build_receipt"], "source build receipt")
    require(binding["digests"].get("source_build_receipt") == spec_build_sha,
            "training authority source build differs from campaign spec")
    build = validate_build(binding["paths"]["source_build_receipt"])
    require(auth.parent == coverage and build["receipt_sha256"] == spec_build_sha and
            build_for(spec)["binary_sha256"] == build["binary_sha256"],
            "training authority build/campaign path differs from spec")
    results = []
    for owner, kv_head in coordinates:
        name = f"owner{owner}-head{kv_head}"
        input_path = coverage / (name + "-input.bin")
        manifest = preparer.prepare(repeat_training_authority=auth,
            repeat_authority_sha256=auth_ref["sha256"], owner=owner,
            kv_head=kv_head, output=input_path)
        output_prefix = coverage / (name + "-result")
        command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
            "ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=" + str(input_path),
            "ANTFLY_CONTEXT_NATIVE_HEAD_SHA256=" + sha(input_path),
            "ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=" + str(output_prefix),
            str(build["binary_path"]), "--test-filter",
            "context captured native whole-head Gemma qualification"]
        resource = coverage / (name + "-resource.json")
        guarded = _run(command, resource, FIT_LIMITS, numerical=True)
        report_path = Path(str(output_prefix) + ".json")
        # A return code of one is numerical rejection only after a complete,
        # self-consistent raw numerical report is present. Otherwise it is an
        # operational failure and no campaign result is sealed.
        report = _qualification_report(report_path, input_path,
            guarded["returncode"], owner, kv_head, manifest)
        candidate = Path(str(output_prefix) + ".candidate")
        artifacts = validate_head_artifacts(report, report_path, candidate,
            resource.with_suffix(".stderr"))
        row = {"owner_layer": owner, "kv_head": kv_head,
            "accepted_head": report["accepted_head"],
            "input": _phase_ref(input_path),
            "input_manifest": _phase_ref(Path(str(input_path) + ".json")),
            "report": _phase_ref(report_path), "resource": _phase_ref(resource),
            "stdout": _phase_ref(resource.with_suffix(".stdout")),
            "stderr": _phase_ref(resource.with_suffix(".stderr")),
            "candidate": (_phase_ref(candidate) | {"bytes": artifacts["candidate_bytes"]}
                          if candidate.exists() else None)}
        seal = coverage / (name + "-seal.json")
        write_new(seal, {"schema": "antfly.context.native.head-fit-seal.v1",
            "diagnostic_only": True, "serving_qualified": False,
            "fresh_unseen_qualified": False, **row})
        row["seal"] = _phase_ref(seal)
        results.append(row)
    accepted = sum(row["accepted_head"] for row in results)
    value = {"schema": RESULT_SCHEMA, "completed": True,
        "outcome": "accepted" if accepted == len(results) else "numerical_rejection",
        "evaluation_role": "internal_development", "diagnostic_only": True,
        "serving_qualified": False, "release_authority": False,
        "full_context_memory_qualified": False, "fresh_unseen_qualified": False,
        "campaign_spec_sha256": spec_sha256,
        "capture_phase": _phase_ref(capture_phase),
        "training_authority": _phase_ref(auth),
        "accepted_head_count": accepted, "head_count": len(results),
        "all_heads_accepted": accepted == len(results), "results": results}
    write_new(coverage / "results.json", value)
    return value


def _default_attempt_operator_error(owner, kv_head):
    coordinate = f"owner{owner}-head{kv_head}/attempt-000000"
    return ("default immutable fit attempt " + coordinate +
        " is partial, rejected, or invalid and was preserved; use "
        "native_repeat_fit_resume.py run-head with this explicit coordinate and "
        "a new six-digit --attempt-id, then assemble an explicit canonical "
        "selection manifest (automatic latest-attempt selection is forbidden)")


def phase_fit(spec, spec_sha256, output):
    """Run/reuse only canonical attempt 000000, then explicitly assemble.

    A partial or rejected default attempt is immutable diagnostic evidence. It
    is never overwritten and no later attempt is auto-discovered; an operator
    must run and select a new attempt through ``native_repeat_fit_resume.py``.
    """
    # Avoid loading a second copy of this module when this file is the CLI.
    sys.modules.setdefault("native_repeat_campaign", sys.modules[__name__])
    import native_repeat_fit_resume as resume

    output = Path(output)
    coordinates = [(owner["owner_layer"], head)
        for owner in spec["expected_owners"]
        for head in range(owner["source_heads"])]
    require(len(coordinates) == 8 and len(set(coordinates)) == 8,
            "resumable fit requires exactly eight physical heads")
    selections = []
    for owner, kv_head in coordinates:
        attempt = (output / "coverage" / "fit-attempts" /
            f"owner{owner}-head{kv_head}" / "attempt-000000")
        seal = attempt / "seal.json"
        if attempt.exists() or attempt.is_symlink():
            try:
                require(attempt.is_dir() and not attempt.is_symlink() and
                        seal.is_file() and not seal.is_symlink(),
                        "default fit attempt lacks a real seal")
                seal_sha256 = sha(seal)
                resume.validate_head(spec, spec_sha256, output, owner, kv_head,
                                     "000000", seal_sha256)
            except Exception as exc:
                raise RuntimeError(_default_attempt_operator_error(
                    owner, kv_head)) from exc
        else:
            try:
                result = resume.run_head(spec, spec_sha256, output, owner,
                                         kv_head, "000000")
            except Exception as exc:
                raise RuntimeError(_default_attempt_operator_error(
                    owner, kv_head)) from exc
            if not result["accepted_head"]:
                raise RuntimeError(_default_attempt_operator_error(
                    owner, kv_head))
            seal_sha256 = result["seal"]["sha256"]
        selections.append({"owner_layer": owner, "kv_head": kv_head,
            "attempt_id": "000000", "seal_sha256": seal_sha256})
    return resume.assemble(spec, spec_sha256, output, selections)


def run_phase(phase, spec, spec_sha256, output):
    functions = {"identity": phase_identity, "coverage": phase_coverage,
                 "plan": phase_plan, "capture": phase_capture,
                 "fit": phase_fit}
    if phase == "all":
        result = None
        for name in ("identity", "coverage", "plan", "capture", "fit"):
            result = functions[name](spec, spec_sha256, output)
        return result
    return functions[phase](spec, spec_sha256, output)


@contextmanager
def controlled_termination():
    """Turn controlled shell termination into stack-unwinding interruption."""
    watched = (signal.SIGTERM, signal.SIGHUP)
    previous = {signum: signal.getsignal(signum) for signum in watched}
    def interrupted(signum, _frame):
        raise InterruptedError(f"native-repeat campaign interrupted by signal {signum}")
    for signum in watched:
        signal.signal(signum, interrupted)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("identity", "coverage", "plan", "capture", "fit", "all"))
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    with controlled_termination():
        try:
            output = campaign_root(args.output)
            spec = load_spec(args.spec, args.spec_sha256)
            result = run_phase(args.phase, spec, args.spec_sha256, output)
            print(json.dumps(result, sort_keys=True))
            if args.phase in ("fit", "all") and not result["all_heads_accepted"]:
                return 2
            return 0
        except Exception as exc:
            print(f"operational failure: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
