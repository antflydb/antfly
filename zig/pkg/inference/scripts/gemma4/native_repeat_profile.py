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

"""Freeze an accepted E4B native-repeat campaign for candidate qualification.

This artifact is intentionally not serving or release authority.  It bridges a
fully sealed, calibration-only native repeat campaign to the test-only serving
qualification driver without reusing the legacy fixed-36 stored-artifact lane.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

import attention_matching_calibrate as calibrate
import native_attention_matching as native
import native_repeat_campaign as campaign
import native_repeat_training_authority as authority


BOOTSTRAP_SCHEMA = "antfly.context.native.qualification-bootstrap.v1"
PROFILE_SCHEMA = "antfly.context.native.qualification-profile.v1"
PROFILE_STAGE = "native_v2_candidate_qualification"
RESULT_SCHEMA = "antfly.context.native.repeat-resumable-results.v1"
FIT_LAYOUT = "immutable_attempt_v1"
E4B_OWNERS = (
    {"layer": 5, "kv_heads": 2, "query_heads": 8, "head_dim": 512},
    {"layer": 11, "kv_heads": 2, "query_heads": 8, "head_dim": 512},
    {"layer": 17, "kv_heads": 2, "query_heads": 8, "head_dim": 512},
    {"layer": 23, "kv_heads": 2, "query_heads": 8, "head_dim": 512},
)
IMPLEMENTATION = {
    "engine": "native_whole_head_v2",
    "algorithm_version": 2,
    "packed_abi_version": 2,
    "kv_dtype": "f16",
    "kernel_route": "metal_context_optimized",
    "split_decode": True,
    "flash_prefill": True,
    "block_tokens": 512,
    "solver": "residual_omp_active_nnls_fp64_qr",
    "acceptance": "rounded_whole_head_v2",
}
BOOTSTRAP_TEST = "context Metal installed Gemma native v2 qualification bootstrap"
BOOTSTRAP_LIMITS = campaign.IDENTITY_LIMITS
HEX = re.compile(r"[0-9a-f]{64}")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def _json(path, digest):
    path = Path(path)
    raw = native.checked(path, digest)
    require(len(raw) <= 16 * 1024 * 1024, "JSON input exceeds byte limit")
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "duplicate JSON key: " + key)
            value[key] = item
        return value
    return json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(
        ValueError("nonfinite JSON number: " + value)), object_pairs_hook=pairs)


def _ref(root, value, label, *, absolute=True, nonempty=False, with_bytes=False):
    expected = {"path", "sha256", "bytes"} if with_bytes else {"path", "sha256"}
    require(type(value) is dict and set(value) == expected and
            HEX.fullmatch(value.get("sha256", "")), "invalid " + label + " reference")
    path = Path(value["path"])
    require(path.is_absolute() is absolute, "invalid " + label + " path form")
    if not absolute:
        require(path.name == value["path"], label + " must be a local filename")
        path = Path(root) / path
    require(path.is_file() and not path.is_symlink(), "missing " + label)
    data = native.checked(path, value["sha256"])
    require(not nonempty or data, "empty " + label)
    return path, data


def _protocol(protocol, owner):
    keys = {"schema_version", "layer", "sha256", "query_sampling", "accounting",
            "max_training_rows", "max_validation_rows", "sources", "owner",
            "strata", "fit", "gate"}
    require(type(protocol) is dict and set(protocol) == keys and
            protocol["schema_version"] == 2 and protocol["layer"] == owner["layer"] and
            HEX.fullmatch(protocol.get("sha256", "")) and
            protocol["query_sampling"] == "source_uniform_partition_v3" and
            protocol["accounting"] == "serial_physical_branch_upper_bound_v2" and
            protocol["max_training_rows"] == 3072 and
            protocol["max_validation_rows"] == 1024 and
            protocol["owner"] == {"kv_heads": owner["kv_heads"],
                                  "query_heads": owner["query_heads"],
                                  "head_dim": owner["head_dim"]},
            "invalid native runtime query protocol")
    sources = protocol["sources"]
    expected_sources = [
        {"kind": "request_question_replay", "partition": "training", "physical_branch": 0},
        {"kind": "fixed_study_prefill", "partition": "training", "physical_branch": 1},
        {"kind": "fixed_study_generated_response", "partition": "training", "physical_branch": 1},
        {"kind": "fixed_study_prefill", "partition": "validation", "physical_branch": 2},
        {"kind": "fixed_study_generated_response", "partition": "validation", "physical_branch": 2},
    ]
    require(sources == expected_sources, "native runtime source protocol changed")
    expected_strata = []
    # E4B's global owner is consumed by itself and, for owner 23, its three
    # shared local consumers.  The bootstrap is the authority for the concrete
    # consumer layers; this check enforces complete, ordered physical GQA rows.
    consumers = ({5: [5], 11: [11], 17: [17], 23: [23, 29, 35, 41]})[owner["layer"]]
    for layer in consumers:
        expected_strata.extend({"consumer_layer": layer, "query_head": head,
                                "kv_head": head // 4} for head in range(8))
    require(protocol["strata"] == expected_strata, "native runtime strata are incomplete or reordered")
    for name in ("fit", "gate"):
        study = protocol[name]
        require(type(study) is dict and set(study) == {
            "template_sha256", "generation_contract_sha256", "response_tokens",
            "generation_mode"} and
            HEX.fullmatch(study.get("template_sha256", "")) and
            HEX.fullmatch(study.get("generation_contract_sha256", "")) and
            study["response_tokens"] == 256 and
            study["generation_mode"] == "greedy_fixed_length_ignore_eos",
            "invalid compiled native runtime study")
    # This is the shared Python/Zig canonical runtime protocol digest.
    require(calibrate.runtime_protocol_identity({key: protocol[key] for key in
            keys - {"sha256"}}) == protocol["sha256"],
            "native runtime protocol digest mismatch")
    return protocol


def qualification_candidate_view(value):
    """Validate and project a standalone native candidate for collectors.

    This deliberately validates only the frozen artifact itself.  It grants no
    authority for its evidence graph, unseen quality, serving, or release.
    Call ``validate_freeze`` when the complete calibration graph is required.
    """
    fields = {"schema", "stage", "diagnostic_only", "candidate_qualification",
        "serving_qualified", "release_authority", "training_role",
        "evaluation_opened", "frozen_before_unseen_evaluation", "identity",
        "implementation", "allocation_page_bytes", "max_prompt_tokens",
        "max_output_tokens", "minimum_saved_bytes", "profile_semantic_sha256",
        "owners", "query_protocols", "evidence"}
    require(type(value) is dict and set(value) == fields and
            value.get("schema") == PROFILE_SCHEMA and value.get("stage") == PROFILE_STAGE and
            value.get("diagnostic_only") is True and
            value.get("candidate_qualification") is True and
            value.get("serving_qualified") is False and
            value.get("release_authority") is False and
            value.get("training_role") == "internal_development_calibration" and
            value.get("evaluation_opened") is False and
            value.get("frozen_before_unseen_evaluation") is True,
            "invalid native qualification candidate flags")
    identity = value["identity"]
    identity_fields = {name + "_sha256" for name in
                       ("artifact", "tokenizer", "template", "layout", "implementation")}
    require(type(identity) is dict and set(identity) == identity_fields and
            all(HEX.fullmatch(identity.get(name, "")) and identity[name] != "0" * 64
                for name in identity_fields), "invalid native qualification identity")
    require(value["implementation"] == IMPLEMENTATION,
            "invalid native qualification implementation")
    page = value["allocation_page_bytes"]
    prompt = value["max_prompt_tokens"]
    output = value["max_output_tokens"]
    savings = value["minimum_saved_bytes"]
    require(type(page) is int and 4096 <= page <= 1024 * 1024 and page & (page - 1) == 0 and
            type(prompt) is int and prompt > 0 and type(output) is int and output > 0 and
            prompt + output <= 131072 and type(savings) is int and savings > 0 and
            savings % page == 0, "invalid native qualification runtime bounds")
    owners = value["owners"]
    require(type(owners) is list and len(owners) == len(E4B_OWNERS),
            "invalid native qualification owners")
    for row, expected in zip(owners, E4B_OWNERS):
        require(type(row) is dict and set(row) == {"layer", "retained"} and
                row["layer"] == expected["layer"] and type(row["retained"]) is list and
                len(row["retained"]) == expected["kv_heads"] and
                all(type(retained) is int and 0 < retained < IMPLEMENTATION["block_tokens"]
                    for retained in row["retained"]),
                "invalid native qualification owners")
    protocols = value["query_protocols"]
    require(type(protocols) is list and len(protocols) == len(E4B_OWNERS),
            "invalid native qualification protocols")
    for protocol, owner in zip(protocols, E4B_OWNERS):
        _protocol(protocol, owner)
    semantic = value["profile_semantic_sha256"]
    require(HEX.fullmatch(semantic or "") and semantic != "0" * 64 and
            semantic == calibrate.profile_semantic_identity(
                owners, protocols, prompt, output, savings,
                engine=IMPLEMENTATION["engine"]),
            "native qualification profile semantic digest mismatch")
    evidence_fields = {"campaign_spec_sha256", "repeat_results_sha256",
        "training_authority_sha256", "target_bootstrap_sha256",
        "target_bootstrap_resource_sha256", "source_build_sha256",
        "binary_sha256", "freezer_sha256"}
    evidence = value["evidence"]
    require(type(evidence) is dict and set(evidence) == evidence_fields and
            all(HEX.fullmatch(evidence.get(name, "")) and evidence[name] != "0" * 64
                for name in evidence_fields), "invalid native qualification evidence")
    return {"schema_version": 2, "identity": identity,
        "query_protocols": protocols, "implementation": value["implementation"],
        "max_prompt_tokens": prompt, "max_output_tokens": output,
        "allocation_page_bytes": page, "minimum_saved_bytes": savings,
        "profile_semantic_sha256": semantic, "owners": owners}


def validate_bootstrap(path, digest):
    value = _json(path, digest)
    require(type(value) is dict and set(value) == {
        "schema", "purpose", "diagnostic_only", "candidate_qualification",
        "serving_qualified", "release_authority", "model_forward_steps",
        "loaded_identity", "implementation", "allocation_page_bytes", "owners",
        "query_protocols"} and value["schema"] == BOOTSTRAP_SCHEMA and
        value["purpose"] == PROFILE_STAGE and value["diagnostic_only"] is True and
        value["candidate_qualification"] is True and
        value["serving_qualified"] is False and value["release_authority"] is False and
        value["model_forward_steps"] == 0 and value["implementation"] == IMPLEMENTATION and
        value["owners"] == list(E4B_OWNERS), "invalid native qualification bootstrap")
    identity = value["loaded_identity"]
    require(type(identity) is dict and set(identity) == {
        "load_generation", "artifact_sha256", "tokenizer_sha256", "template_sha256",
        "layout_sha256", "implementation_sha256"} and
        type(identity["load_generation"]) is int and identity["load_generation"] > 0 and
        all(HEX.fullmatch(identity[name] or "") and identity[name] != "0" * 64
            for name in identity if name != "load_generation"),
        "invalid target-native loaded identity")
    page = value["allocation_page_bytes"]
    require(type(page) is int and 4096 <= page <= 1024 * 1024 and page & (page - 1) == 0,
            "invalid native allocation page")
    protocols = value["query_protocols"]
    require(type(protocols) is list and len(protocols) == len(E4B_OWNERS),
            "incomplete native runtime protocols")
    for protocol, owner in zip(protocols, E4B_OWNERS):
        _protocol(protocol, owner)
    return value


def _campaign_spec(path, digest):
    spec = campaign.load_spec(path, digest)
    expected = [{"owner_layer": owner["layer"], "source_heads": owner["kv_heads"],
                 "head_dim": owner["head_dim"]} for owner in E4B_OWNERS]
    require(spec["expected_owners"] == expected, "campaign is not complete E4B geometry")
    return spec


def _manifest(path, digest, input_path, owner, head, binding):
    manifest = _json(path, digest)
    fields = {"schema", "recipe", "diagnostic_only", "evaluation_role", "query_sampling",
        "fresh_unseen_qualified", "serving_qualified", "full_context_memory_qualified",
        "old_validation_queries_used_for_fit", "held_numerics_used_for_selection",
        "training_authority_mode", "preparer_sha256", "source_preparer_sha256",
        "authority_sealer_sha256", "source_tensor_order", "source_kv_heads", "kv_head",
        "input", "owner_layer", "consumer_groups", "head_dim", "source_rows",
        "training_rows", "validation_rows", "prefix_rows", "recent_rows",
        "minimum_recent_rows", "continuation_rows", "retained_row_cap", "staging_mib", "retention_divisors",
        "limits", "sources"}
    require(type(manifest) is dict and set(manifest) == fields and
            manifest.get("schema") == native.SCHEMA and
            manifest.get("recipe") == native.RECIPE and manifest.get("diagnostic_only") is True and
            manifest.get("evaluation_role") == "internal_development" and
            manifest.get("fresh_unseen_qualified") is False and
            manifest.get("serving_qualified") is False and
            manifest.get("full_context_memory_qualified") is False and
            manifest.get("old_validation_queries_used_for_fit") is False and
            manifest.get("held_numerics_used_for_selection") is False and
            manifest.get("training_authority_mode") == "native_repeat_v1" and
            manifest.get("preparer_sha256") == campaign.sha(campaign.preparer.__file__) and
            manifest.get("source_preparer_sha256") == campaign.sha(native.__file__) and
            manifest.get("authority_sealer_sha256") == binding["authority"]["sealer_sha256"] and
            manifest.get("query_sampling") == binding["repeat"]["query_sampling"] and
            manifest.get("source_tensor_order") == "logical_row_kv_head_head_dim" and
            manifest.get("source_kv_heads") == 2 and manifest.get("kv_head") == head and
            manifest.get("owner_layer") == owner and manifest.get("head_dim") == 512 and
            type(manifest.get("prefix_rows")) is int and manifest["prefix_rows"] >= 0 and
            type(manifest.get("recent_rows")) is int and manifest["recent_rows"] >= 2048 and
            manifest.get("minimum_recent_rows") == 2048 and
            manifest.get("continuation_rows") == 32 and
            manifest.get("retained_row_cap") == 6144 and manifest.get("staging_mib") == 512 and
            manifest.get("retention_divisors") == [4, 3, 2] and
            manifest.get("limits") == {"max_mass_error": .05, "max_output_rmse": .10,
                                       "max_consumer_output_rmse": .10},
            "invalid frozen native head input manifest")
    source = binding["descriptors"][(owner, head)]
    captured = source["repeat_owner"]
    descriptor = source["source_descriptor"]
    require(manifest.get("source_rows") == descriptor["rows"] and
            manifest.get("training_rows") == captured["training_rows"] and
            manifest.get("validation_rows") == captured["validation_rows"] and
            manifest.get("consumer_groups") == len(captured["strata"]),
            "native head manifest differs from sealed authority")
    entry = manifest.get("input")
    require(type(entry) is dict and set(entry) == {"path", "bytes", "sha256"} and
            entry["path"] == str(input_path) and entry["sha256"] == campaign.sha(input_path) and
            entry["bytes"] == input_path.stat().st_size,
            "native head input binding mismatch")
    expected_sources = {name: {"path": str(source_path), "sha256": binding["digests"][name]}
                        for name, source_path in binding["paths"].items()
                        if name in binding["digests"]}
    expected_sources["source_descriptor"] = {
        "path": str(source["source_descriptor_path"]),
        "sha256": source["source_descriptor_ref"]["sha256"]}
    require(manifest.get("sources") == expected_sources,
            "native head manifest source graph mismatch")
    _input_contract(input_path, entry["sha256"], manifest, source,
                    binding["paths"]["source_index"].parent, head)
    return manifest


def _input_contract(path, digest, manifest, source, source_root, head):
    """Reconstruct AMHEAD02 bytes from the sealed authority without decoding floats."""
    descriptor, captured = source["source_descriptor"], source["repeat_owner"]
    rows, heads, dim = descriptor["rows"], descriptor["heads"], descriptor["dim"]
    all_keys = native.checked(native.local(source_root, descriptor["keys"]["path"]),
                              descriptor["keys"]["sha256"], rows * heads * dim * 2)
    all_values = native.checked(native.local(source_root, descriptor["values"]["path"]),
                                descriptor["values"]["sha256"], rows * heads * dim * 2)
    keys = native.select_head_rows(all_keys, rows, heads, dim, head, 2)
    values = native.select_head_rows(all_values, rows, heads, dim, head, 2)
    training = native.checked(native.local(source_root, captured["training"]["path"]),
                              captured["training"]["sha256"],
                              manifest["training_rows"] * dim * 4)
    validation = native.checked(native.local(source_root, captured["validation"]["path"]),
                                captured["validation"]["sha256"],
                                manifest["validation_rows"] * dim * 4)
    header = native.HEADER.pack(b"AMHEAD02", rows, dim, manifest["training_rows"],
        manifest["validation_rows"], captured["validation_group_rows"],
        manifest["prefix_rows"], manifest["recent_rows"], 4, 2,
        manifest["continuation_rows"], manifest["retained_row_cap"],
        manifest["staging_mib"], descriptor["scale"], 0)
    expected = header + keys + values + training + validation
    actual = native.checked(path, digest, len(expected))
    require(hashlib.sha256(expected).hexdigest() == digest and actual == expected,
            "native head input differs from sealed authority bytes")


def _fit_rows(results, root, binding, build, campaign_spec_sha256, capture_phase_ref,
              authority_ref):
    rows = results.get("results")
    expected_coordinates = [(owner["layer"], head) for owner in E4B_OWNERS for head in range(2)]
    require(type(rows) is list and len(rows) == 8 and
            [(row.get("owner_layer"), row.get("kv_head")) for row in rows] == expected_coordinates,
            "missing, extra, or reordered E4B head results")
    retained, common, head_evidence = {}, None, []
    for row, (owner, head) in zip(rows, expected_coordinates):
        require(type(row) is dict and set(row) == {"owner_layer", "kv_head", "accepted_head",
                "input", "input_manifest", "report", "resource", "stdout", "stderr",
                "candidate", "seal", "attempt_id"} and row["accepted_head"] is True and
                isinstance(row["attempt_id"], str) and re.fullmatch(r"[0-9]{6}", row["attempt_id"]),
                "native profile requires every E4B head accepted")
        paths = {}
        for name in ("input", "input_manifest", "report", "resource", "stdout", "stderr",
                     "candidate", "seal"):
            require(row[name] is not None, "accepted native head lacks " + name)
            paths[name], _ = _ref(root, row[name], "head " + name,
                                  nonempty=name != "stdout",
                                  with_bytes=name == "candidate")
        attempt_root = (root / "fit-attempts" / f"owner{owner}-head{head}" /
                        ("attempt-" + row["attempt_id"]))
        require(paths["input"] == attempt_root / "input.bin" and
                paths["input_manifest"] == attempt_root / "input.bin.json" and
                paths["report"] == attempt_root / "result.json" and
                paths["candidate"] == attempt_root / "result.candidate" and
                paths["resource"] == attempt_root / "resource.json" and
                paths["stdout"] == attempt_root / "resource.stdout" and
                paths["stderr"] == attempt_root / "resource.stderr" and
                paths["seal"] == attempt_root / "seal.json",
                "noncanonical native head result path")
        require(row["candidate"].get("bytes") == paths["candidate"].stat().st_size,
                "native candidate size mismatch")
        manifest = _manifest(paths["input_manifest"], row["input_manifest"]["sha256"],
                             paths["input"], owner, head, binding)
        expected_command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
            "ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=" + str(paths["input"]),
            "ANTFLY_CONTEXT_NATIVE_HEAD_SHA256=" + row["input"]["sha256"],
            "ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=" + str(attempt_root / "result"),
            str(build["binary_path"]), "--test-filter",
            "context captured native whole-head Gemma qualification"]
        resource = campaign._resource_contract(paths["resource"], expected_command,
                                               campaign.FIT_LIMITS, numerical=True)
        report = campaign._qualification_report(paths["report"], paths["input"],
                                                resource["returncode"], owner, head, manifest)
        artifact = campaign.validate_head_artifacts(
            report, paths["report"], paths["candidate"], paths["stderr"])
        require(report["accepted_head"] is True and report["attempts"][-1]["outcome"] == "accepted_head",
                "head result is not an accepted terminal fit")
        divisor = report["attempts"][-1]["divisor"]
        block_rows = (512 + divisor - 1) // divisor
        require(divisor in (4, 3, 2) and report["attempts"][-1]["retained_rows"] +
                report["continuation_rows"] <= report["retained_row_cap"],
                "accepted native head exceeds frozen row budget")
        blocks = (report["source_rows"] - report["exact_prefix_rows"] -
                  report["exact_recent_rows"]) // 512
        require(blocks > 0 and report["attempts"][-1]["retained_rows"] ==
                report["exact_prefix_rows"] + blocks * block_rows + report["exact_recent_rows"],
                "native head retained rows do not match selected divisor")
        geometry = (report["source_rows"], report["exact_prefix_rows"],
                    report["exact_recent_rows"], blocks,
                    report["continuation_rows"], report["retained_row_cap"])
        common = geometry if common is None else common
        require(geometry == common, "E4B head eligible source bounds differ")
        seal = _json(paths["seal"], row["seal"]["sha256"])
        sealed_row = dict(row); sealed_row.pop("seal")
        require(seal == {"schema": "antfly.context.native.resumable-head-fit-seal.v1",
                         "fit_layout": FIT_LAYOUT, "attempt_id": row["attempt_id"],
                         "diagnostic_only": True, "serving_qualified": False,
                         "fresh_unseen_qualified": False,
                         "campaign_spec_sha256": campaign_spec_sha256,
                         "capture_phase": capture_phase_ref,
                         "training_authority": authority_ref, **sealed_row},
                "native head seal differs from result graph")
        retained[(owner, head)] = block_rows
        final = report["attempts"][-1]
        head_evidence.append({"owner_layer": owner, "kv_head": head,
            "attempt_id": row["attempt_id"], "selected_divisor": divisor,
            "metal_metrics": final["metrics"], "reference_metrics": final["reference"],
            "report": row["report"], "candidate": row["candidate"],
            "resource": row["resource"], "artifact_validation": artifact})
    source = binding["source_index"]
    starts = source.get("eligible_block_starts")
    require(type(starts) is list and starts == list(range(common[1],
            common[0] - common[2], 512)) and len(starts) == common[3],
            "accepted profile differs from fresh eligible-block source bounds")
    return retained, common, head_evidence


def freeze(*, campaign_spec, campaign_spec_sha256, results, results_sha256,
           target_bootstrap, target_bootstrap_sha256, target_bootstrap_resource,
           target_bootstrap_resource_sha256, max_prompt_tokens, max_output_tokens,
           minimum_saved_bytes, output=None):
    spec = _campaign_spec(campaign_spec, campaign_spec_sha256)
    results_path = Path(results)
    value = _json(results_path, results_sha256)
    root = results_path.parent
    result_fields = {"schema", "completed", "outcome", "evaluation_role", "diagnostic_only",
        "serving_qualified", "release_authority", "full_context_memory_qualified",
        "fresh_unseen_qualified", "campaign_spec_sha256", "capture_phase",
        "training_authority", "accepted_head_count", "head_count", "all_heads_accepted",
        "results", "fit_layout"}
    require(type(value) is dict and set(value) == result_fields and
            results_path.name == "results.json" and root.name == "coverage" and
            value.get("schema") == RESULT_SCHEMA and value.get("fit_layout") == FIT_LAYOUT and
            value.get("completed") is True and
            value.get("outcome") == "accepted" and value.get("evaluation_role") == "internal_development" and
            value.get("diagnostic_only") is True and value.get("serving_qualified") is False and
            value.get("release_authority") is False and value.get("full_context_memory_qualified") is False and
            value.get("fresh_unseen_qualified") is False and value.get("campaign_spec_sha256") == campaign_spec_sha256 and
            value.get("accepted_head_count") == 8 and value.get("head_count") == 8 and
            value.get("all_heads_accepted") is True,
            "invalid or incomplete native repeat campaign results")
    capture_phase_path, _ = _ref(root, value.get("capture_phase"), "capture phase")
    require(capture_phase_path == root.parent / "capture-phase.json", "noncanonical capture phase")
    capture_phase = campaign._phase_contract(_json(capture_phase_path,
        value["capture_phase"]["sha256"]), "capture", campaign_spec_sha256)
    authority_path, _ = _ref(root, value.get("training_authority"), "training authority")
    require(authority_path == root / "training-authority.json" and
            capture_phase["sources"].get("training_authority") == value["training_authority"],
            "results training authority differs from capture phase")
    binding = authority.validate_authority(authority_path, value["training_authority"]["sha256"])
    coordinates = [(owner["layer"], head) for owner in E4B_OWNERS for head in range(2)]
    require([(row["owner_layer"], row["kv_head"]) for row in binding["plan"]["coordinates"]] == coordinates and
            [(row["owner_layer"], row["kv_head"]) for row in binding["authority"]["cells"]] == coordinates,
            "training authority does not cover exactly eight E4B heads")
    native.checked(binding["paths"]["source_build_receipt"],
                   binding["digests"]["source_build_receipt"])
    build = campaign.validate_build(binding["paths"]["source_build_receipt"])
    require(build["receipt_sha256"] == binding["digests"]["source_build_receipt"] ==
            spec["source_build_receipt"]["sha256"] and
            campaign.sha(build["binary_path"]) == build["binary_sha256"],
            "native profile source build mismatch")
    retained, bounds, head_evidence = _fit_rows(
        value, root, binding, build, campaign_spec_sha256,
        value["capture_phase"], value["training_authority"])

    bootstrap_path = Path(target_bootstrap)
    bootstrap = validate_bootstrap(bootstrap_path, target_bootstrap_sha256)
    require(bootstrap_path.parent == root.parent and
            bootstrap_path.name == "native-qualification-bootstrap.json",
            "noncanonical target-native bootstrap path")
    resource_path = Path(target_bootstrap_resource)
    require(resource_path.parent == root.parent and
            resource_path.name == "native-qualification-bootstrap-resource.json" and
            campaign.sha(resource_path) == target_bootstrap_resource_sha256,
            "noncanonical target-native bootstrap resource")
    command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
        "ANTFLY_CONTEXT_MODEL_DIR=" + spec["model"]["directory"],
        "ANTFLY_CONTEXT_NATIVE_QUALIFICATION_BOOTSTRAP_OUTPUT=" + str(bootstrap_path),
        str(build["binary_path"]), "--test-filter", BOOTSTRAP_TEST]
    campaign._resource_contract(resource_path, command, BOOTSTRAP_LIMITS)
    bootstrap_stdout = resource_path.with_suffix(".stdout")
    bootstrap_stderr = resource_path.with_suffix(".stderr")
    require(bootstrap_stdout.is_file() and not bootstrap_stdout.is_symlink() and
            bootstrap_stderr.is_file() and not bootstrap_stderr.is_symlink(),
            "missing native qualification bootstrap logs")
    bootstrap_stdout_sha256 = campaign.sha(bootstrap_stdout)
    native.checked(bootstrap_stdout, bootstrap_stdout_sha256)
    lines = bootstrap_stderr.read_text().splitlines()
    artifact_marker = "context_native_qualification_bootstrap_artifact: sha256=" + target_bootstrap_sha256
    completion_marker = ("context_native_qualification_bootstrap: owners=4 model_forward_steps=0 "
        "candidate_qualification=true serving_qualified=false release_authority=false")
    require([line for line in lines if line.startswith(
            "context_native_qualification_bootstrap_artifact:")] == [artifact_marker] and
            [line for line in lines if line.startswith(
            "context_native_qualification_bootstrap:")] == [completion_marker],
            "native qualification bootstrap completion evidence differs")
    capture_identity = binding["authority"]["loaded_identity"]
    target_identity = bootstrap["loaded_identity"]
    for name in ("artifact", "tokenizer", "template", "layout"):
        require(capture_identity[name + "_sha256"] == target_identity[name + "_sha256"],
                "capture/target loaded identity mismatch: " + name)
    require(target_identity["artifact_sha256"] == spec["model"]["artifact_sha256"] and
            target_identity["implementation_sha256"] != capture_identity["implementation_sha256"],
            "target native implementation was not separated from capture implementation")
    require(type(max_prompt_tokens) is int and max_prompt_tokens == bounds[0] and
            type(max_output_tokens) is int and max_output_tokens == bounds[4] and
            max_prompt_tokens + max_output_tokens <= 131072,
            "invalid native qualification envelope")
    page = bootstrap["allocation_page_bytes"]
    # This is only an admission floor, not a savings result.  Deduct the full
    # dense bias allocation and four pages of rounding/metadata slack per head;
    # later installed performance evidence must still prove >=25% physically.
    conservative_saved = 0
    for owner in E4B_OWNERS:
        for head in range(2):
            block_rows = retained[(owner["layer"], head)]
            removed_kv = bounds[3] * (512 - block_rows) * owner["head_dim"] * 4
            candidate_rows = bounds[1] + bounds[3] * block_rows + bounds[2] + bounds[4]
            head_floor = removed_kv - candidate_rows * 4 - 4 * page
            conservative_saved += max(0, head_floor // page * page)
    require(type(minimum_saved_bytes) is int and minimum_saved_bytes > 0 and
            minimum_saved_bytes % page == 0 and minimum_saved_bytes <= conservative_saved,
            "invalid or optimistic page-granular minimum savings")
    owners = [{"layer": owner["layer"],
               "retained": [retained[(owner["layer"], head)] for head in range(2)]}
              for owner in E4B_OWNERS]
    protocols = bootstrap["query_protocols"]
    identity = {name + "_sha256": target_identity[name + "_sha256"]
                for name in ("artifact", "tokenizer", "template", "layout", "implementation")}
    semantic = calibrate.profile_semantic_identity(
        owners, protocols, max_prompt_tokens, max_output_tokens, minimum_saved_bytes,
        engine="native_whole_head_v2")
    profile = {
        "schema": PROFILE_SCHEMA, "stage": PROFILE_STAGE,
        "diagnostic_only": True, "candidate_qualification": True,
        "serving_qualified": False, "release_authority": False,
        "training_role": "internal_development_calibration", "evaluation_opened": False,
        "frozen_before_unseen_evaluation": True, "identity": identity,
        "implementation": IMPLEMENTATION, "allocation_page_bytes": page,
        "max_prompt_tokens": max_prompt_tokens, "max_output_tokens": max_output_tokens,
        "minimum_saved_bytes": minimum_saved_bytes,
        "profile_semantic_sha256": semantic, "owners": owners,
        "query_protocols": protocols,
        "evidence": {
            "campaign_spec_sha256": campaign_spec_sha256,
            "repeat_results_sha256": results_sha256,
            "training_authority_sha256": value["training_authority"]["sha256"],
            "target_bootstrap_sha256": target_bootstrap_sha256,
            "target_bootstrap_resource_sha256": target_bootstrap_resource_sha256,
            "source_build_sha256": build["receipt_sha256"],
            "binary_sha256": build["binary_sha256"],
            "freezer_sha256": campaign.sha(__file__),
        },
    }
    if output is not None:
        output = Path(output)
        require(output.parent == root.parent and output.name == "native-qualification-profile.json",
                "noncanonical native qualification profile output")
        campaign.write_new(output, profile)
    return profile


def validate_freeze(profile_path, profile_sha256, *, campaign_spec, campaign_spec_sha256,
                    results, results_sha256, target_bootstrap, target_bootstrap_sha256,
                    target_bootstrap_resource, target_bootstrap_resource_sha256):
    profile = _json(profile_path, profile_sha256)
    regenerated = freeze(campaign_spec=campaign_spec, campaign_spec_sha256=campaign_spec_sha256,
        results=results, results_sha256=results_sha256, target_bootstrap=target_bootstrap,
        target_bootstrap_sha256=target_bootstrap_sha256,
        target_bootstrap_resource=target_bootstrap_resource,
        target_bootstrap_resource_sha256=target_bootstrap_resource_sha256,
        max_prompt_tokens=profile.get("max_prompt_tokens"),
        max_output_tokens=profile.get("max_output_tokens"),
        minimum_saved_bytes=profile.get("minimum_saved_bytes"))
    require(profile == regenerated, "native qualification profile does not match source graph")
    results_value = _json(results, results_sha256)
    authority_ref = results_value["training_authority"]
    binding = authority.validate_authority(authority_ref["path"], authority_ref["sha256"])
    strata = {}
    for coordinate, descriptor in binding["descriptors"].items():
        for row in descriptor["source_descriptor"]["strata"]:
            key = (coordinate[0], row["kv_head"], row["consumer_layer"], row["query_head"])
            strata[key] = {"owner_layer": key[0], "kv_head": key[1],
                           "consumer_layer": key[2], "query_head": key[3]}
    per_head = []
    for row in results_value["results"]:
        report = _json(row["report"]["path"], row["report"]["sha256"])
        final = report["attempts"][-1]
        per_head.append({"owner_layer": row["owner_layer"], "kv_head": row["kv_head"],
            "attempt_id": row["attempt_id"], "selected_divisor": final["divisor"],
            "metal_metrics": final["metrics"], "reference_metrics": final["reference"],
            "report": row["report"], "candidate": row["candidate"],
            "resource": row["resource"]})
    return {"profile": profile,
        "evidence_scope": "native_repeat_same_replay_calibration",
        "calibration_only": True, "fresh_unseen_numerical_qualified": False,
        "serving_qualified": False, "release_authority": False,
        "semantic_identity": profile["identity"],
        "provenance": {"source_build_sha256": profile["evidence"]["source_build_sha256"],
                       "binary_sha256": profile["evidence"]["binary_sha256"]},
        "calibration_documents": [binding["authority"]["document"]["sha256"]],
        "calibration_context_tokens": [profile["max_prompt_tokens"] + profile["max_output_tokens"]],
        "owner_heads": [{"owner_layer": row["layer"], "kv_head": head,
                         "retained_rows_per_block": retained}
                        for row in profile["owners"]
                        for head, retained in enumerate(row["retained"])],
        "consumer_strata": [strata[key] for key in sorted(strata)],
        "selected_head_count": results_value["accepted_head_count"],
        "per_head_numerics": per_head,
        "sources": {"profile": {"path": str(Path(profile_path)), "sha256": profile_sha256},
                    "campaign_spec": {"path": str(Path(campaign_spec)), "sha256": campaign_spec_sha256},
                    "results": {"path": str(Path(results)), "sha256": results_sha256},
                    "training_authority": authority_ref,
                    "target_bootstrap": {"path": str(Path(target_bootstrap)),
                                         "sha256": target_bootstrap_sha256},
                    "target_bootstrap_resource": {"path": str(Path(target_bootstrap_resource)),
                         "sha256": target_bootstrap_resource_sha256},
                    "target_bootstrap_stdout": {
                         "path": str(Path(target_bootstrap_resource).with_suffix(".stdout")),
                         "sha256": campaign.sha(Path(target_bootstrap_resource).with_suffix(".stdout"))}}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-spec", type=Path, required=True)
    parser.add_argument("--campaign-spec-sha256", required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--results-sha256", required=True)
    parser.add_argument("--target-bootstrap", type=Path, required=True)
    parser.add_argument("--target-bootstrap-sha256", required=True)
    parser.add_argument("--target-bootstrap-resource", type=Path, required=True)
    parser.add_argument("--target-bootstrap-resource-sha256", required=True)
    parser.add_argument("--max-prompt-tokens", type=int, required=True)
    parser.add_argument("--max-output-tokens", type=int, required=True)
    parser.add_argument("--minimum-saved-bytes", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args())
    value = freeze(**args)
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
