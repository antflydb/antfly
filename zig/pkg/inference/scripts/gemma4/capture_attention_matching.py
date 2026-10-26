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

"""Capture one private installed-model replay and seal its numerical evidence.

Requires a bounded-build receipt binding the test binary to capture-time source
hashes. This driver is local-only and never modifies serving capabilities.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tempfile

import attention_matching_calibrate as calibration
import context_release_build_adapter as release_build

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_resources import run_guarded

ROOT = Path(__file__).resolve().parents[5]
BLOCK_TOKENS = 512
MAX_CAPTURE_INPUT_BYTES = 16 * 1024 * 1024


def _plan_path(plan_path, value):
    path = (Path(plan_path).resolve().parent / value).resolve()
    calibration.require(path.is_file(), f"missing capture input: {value}")
    return path


def _capture_artifact(root, reference, label, max_bytes=MAX_CAPTURE_INPUT_BYTES):
    require = calibration.require
    require(isinstance(reference, dict) and set(reference) == {"path", "sha256"} and
            isinstance(reference["path"], str) and reference["path"] and
            isinstance(reference["sha256"], str), "invalid " + label + " reference")
    calibration.identity(reference["sha256"], label)
    relative = Path(reference["path"])
    require(not relative.is_absolute() and ".." not in relative.parts, label + " path escapes campaign")
    unresolved = Path(root).resolve() / relative
    require(not unresolved.is_symlink(), label + " must not be a symlink")
    path = unresolved.resolve()
    require(path.is_relative_to(Path(root).resolve()) and path.is_file() and
            path.stat().st_size <= max_bytes and calibration.digest(path) == reference["sha256"],
            label + " identity mismatch")
    return path


def validate_capture_inputs(manifest, root, coverage_receipt, prompt_tokens, prompt_token_offsets,
                            question_tokens,
                            loaded_identity, provenance, request_binding, diagnostic_only=False,
                            rendered_prompt=None):
    """Bind renderer-produced study inputs to one genuine coverage payload."""
    require = calibration.require
    exact = {"schema_version", "stage", "request", "coverage_receipt", "rendered_prompt", "prompt",
             "prompt_token_offsets",
             "question", "fit", "gate", "loaded_identity", "provenance", "completed",
             "diagnostic_only", "admitted_prefill_chunk_rows"}
    require(isinstance(manifest, dict) and set(manifest) == exact and manifest["schema_version"] == 1 and
            manifest["stage"] == "context_numerical_capture_inputs" and manifest["completed"] is True and
            type(manifest["diagnostic_only"]) is bool and manifest["diagnostic_only"] == diagnostic_only and
            manifest["admitted_prefill_chunk_rows"] == 64,
            "invalid numerical capture input manifest")
    require(manifest["coverage_receipt"] == coverage_receipt and
            manifest["loaded_identity"] == loaded_identity and manifest["provenance"] == provenance,
            "numerical capture input provenance mismatch")
    request = manifest["request"]
    require(isinstance(request, dict) and set(request) == {"session_id", "request_index",
            "question_sha256", "request_content_sha256"} and request == request_binding,
            "invalid numerical capture request")
    prompt = manifest["prompt"]
    require(isinstance(prompt, dict) and set(prompt) == {"path", "sha256", "count"} and
            {key: prompt[key] for key in ("path", "sha256")} == prompt_tokens and
            type(prompt["count"]) is int and 0 < prompt["count"] <= 131072,
            "numerical capture prompt mismatch")
    prompt_path = _capture_artifact(root, prompt_tokens, "numerical capture prompt", 131072 * 8)
    require(prompt_path.stat().st_size == prompt["count"] * 8, "numerical capture prompt count mismatch")
    offsets = manifest["prompt_token_offsets"]
    require(isinstance(offsets, dict) and set(offsets) == {"path", "sha256", "count"} and
            {key: offsets[key] for key in ("path", "sha256")} == prompt_token_offsets and
            offsets["count"] == prompt["count"], "numerical capture prompt offsets mismatch")
    offsets_path = _capture_artifact(root, prompt_token_offsets,
                                     "numerical capture prompt offsets", 131072 * 8)
    require(offsets_path.stat().st_size == prompt["count"] * 8,
            "numerical capture prompt offset count mismatch")
    if rendered_prompt is not None:
        require(manifest["rendered_prompt"] == rendered_prompt,
                "numerical capture rendered prompt mismatch")
    rendered_path = _capture_artifact(root, manifest["rendered_prompt"],
                                      "numerical capture rendered prompt")
    require(rendered_path.stat().st_size > 0, "empty numerical capture rendered prompt")
    previous_start = previous_end = 0
    saw_mapped = False
    saw_nonempty = False
    offset_bytes = offsets_path.read_bytes()
    for index in range(prompt["count"]):
        start, end = struct.unpack_from("<II", offset_bytes, index * 8)
        require(start <= end <= rendered_path.stat().st_size,
                "numerical capture token offset out of bounds")
        if (start, end) != (0, 0):
            require(not saw_mapped or (start >= previous_start and end >= previous_end),
                    "numerical capture token offsets move backward")
            saw_mapped = True
            previous_start, previous_end = start, end
        if start != end:
            saw_nonempty = True
    require(saw_nonempty, "numerical capture has no text-bearing token offsets")
    question = manifest["question"]
    require(isinstance(question, dict) and set(question) == {"tokens", "source_start", "source_end"} and
            isinstance(question["tokens"], dict) and set(question["tokens"]) == {"path", "sha256", "count"} and
            {key: question["tokens"][key] for key in ("path", "sha256")} == question_tokens and
            type(question["tokens"]["count"]) is int and
            type(question["source_start"]) is int and type(question["source_end"]) is int and
            0 <= question["source_start"] < question["source_end"] <= prompt["count"] and
            question["tokens"]["count"] == question["source_end"] - question["source_start"],
            "numerical capture question mismatch")
    question_path = _capture_artifact(root, question_tokens, "numerical capture question", 8192 * 8)
    prompt_bytes, question_bytes = prompt_path.read_bytes(), question_path.read_bytes()
    require(len(question_bytes) == question["tokens"]["count"] * 8 and
            question_bytes == prompt_bytes[question["source_start"] * 8:question["source_end"] * 8],
            "numerical capture question is not the prompt slice")
    studies = []
    for name in ("fit", "gate"):
        study = manifest[name]
        require(isinstance(study, dict) and set(study) == {"tokens", "prompt_tokens_sha256",
                "rendered_suffix_sha256", "template", "generation_contract", "response_tokens",
                "generation_mode"} and study["prompt_tokens_sha256"] == prompt["sha256"] and
                isinstance(study["tokens"], dict) and set(study["tokens"]) == {"path", "sha256", "count"} and
                type(study["tokens"]["count"]) is int and 8 <= study["tokens"]["count"] <= 8192 and
                type(study["response_tokens"]) is int and 8 <= study["response_tokens"] <= 8192 and
                study["generation_mode"] == "greedy_fixed_length_ignore_eos",
                "invalid numerical " + name + " study")
        calibration.identity(study["rendered_suffix_sha256"], "numerical " + name + " rendered suffix")
        tokens_path = _capture_artifact(root, {key: study["tokens"][key] for key in ("path", "sha256")},
                                        "numerical " + name + " tokens", 8192 * 8)
        require(tokens_path.stat().st_size == study["tokens"]["count"] * 8,
                "numerical " + name + " token count mismatch")
        template = _capture_artifact(root, study["template"], "numerical " + name + " template")
        contract = _capture_artifact(root, study["generation_contract"],
                                     "numerical " + name + " generation contract")
        require(template.stat().st_size > 0 and contract.stat().st_size > 0,
                "empty numerical study authority")
        studies.append(study)
    require(studies[0]["template"]["sha256"] != studies[1]["template"]["sha256"] and
            studies[0]["generation_contract"]["sha256"] == studies[1]["generation_contract"]["sha256"],
            "invalid numerical study separation")
    return manifest


def validate_block_coverage(receipt, prompt_tokens_sha256=None, loaded_identity=None, provenance=None,
                            request_binding=None):
    """Recompute the complete eligible block set from runtime ranges.

    ``eligible_ranges`` are the runtime's contiguous post-tokenization ranges:
    cross-part tokens, specials, prefix and recent-tail positions have already
    been removed. Blocks are anchored at the beginning of each range and may
    never cross a payload-part boundary.
    """
    require = calibration.require
    exact = {"schema_version", "stage", "request", "document", "question", "qualified_context_tokens", "prompt_tokens",
             "reserved_output_tokens", "admitted_prefill_chunk_rows", "prompt_tokens_sha256",
             "prompt_token_offsets_sha256",
             "rendered_prompt_sha256", "policy", "payload_parts",
             "eligible_token_labels",
             "eligible_ranges", "eligible_blocks", "loaded_identity", "provenance", "completed",
             "cancelled", "elapsed_ns", "status_readback_bytes", "kv_readback_bytes"}
    require(isinstance(receipt, dict) and set(receipt) == exact and receipt["schema_version"] == 2 and
            receipt["stage"] == "context_payload_block_coverage", "invalid block coverage receipt")
    request = receipt["request"]
    require(isinstance(request, dict) and set(request) == {"session_id", "request_index",
            "question_sha256", "request_content_sha256"} and
            isinstance(request["session_id"], str) and
            re.fullmatch(r"[0-9a-f]{64}", request["session_id"]) is not None and
            request["session_id"] != "0" * 64 and type(request["request_index"]) is int and
            0 <= request["request_index"] < 2**32, "invalid block coverage request binding")
    calibration.identity(request["question_sha256"], "block coverage question")
    calibration.identity(request["request_content_sha256"], "block coverage request content")
    if request_binding is not None:
        require(request == request_binding, "block coverage request binding mismatch")
    document = receipt["document"]
    require(isinstance(document, dict) and set(document) == {"id", "sha256", "bucket"} and
            isinstance(document["id"], str) and document["id"] and
            document["bucket"] in calibration.BUCKETS, "invalid block coverage document")
    calibration.identity(document["sha256"], "block coverage document")
    for field in ("qualified_context_tokens", "prompt_tokens", "reserved_output_tokens"):
        require(type(receipt[field]) is int and receipt[field] > 0, "invalid block coverage " + field)
    require(receipt["admitted_prefill_chunk_rows"] == 64,
            "invalid block coverage admitted prefill chunk")
    calibration.identity(receipt["rendered_prompt_sha256"], "block coverage rendered prompt")
    question = receipt["question"]
    require(isinstance(question, dict) and set(question) == {"message_index", "part_index", "source_start",
            "source_end", "token_count", "tokens_sha256"} and question["message_index"] == 0 and
            question["part_index"] == 1 and type(question["source_start"]) is int and
            type(question["source_end"]) is int and type(question["token_count"]) is int and
            0 <= question["source_start"] < question["source_end"] <= receipt["prompt_tokens"] and
            question["token_count"] == question["source_end"] - question["source_start"],
            "invalid block coverage question attribution")
    calibration.identity(question["tokens_sha256"], "block coverage question tokens")
    require(receipt["prompt_tokens"] + receipt["reserved_output_tokens"] == receipt["qualified_context_tokens"],
            "block coverage context length mismatch")
    calibration.identity(receipt["prompt_tokens_sha256"], "block coverage prompt tokens")
    calibration.identity(receipt["prompt_token_offsets_sha256"], "block coverage prompt token offsets")
    if prompt_tokens_sha256 is not None:
        require(receipt["prompt_tokens_sha256"] == prompt_tokens_sha256,
                "block coverage prompt token identity mismatch")
    policy = receipt["policy"]
    require(isinstance(policy, dict) and set(policy) == {"keep_prefix_tokens", "keep_recent_tokens",
                                                           "block_tokens", "compaction_targets"} and
            type(policy["keep_prefix_tokens"]) is int and policy["keep_prefix_tokens"] >= 0 and
            type(policy["keep_recent_tokens"]) is int and policy["keep_recent_tokens"] > 0 and
            policy["block_tokens"] == BLOCK_TOKENS, "invalid block coverage policy")
    targets = policy["compaction_targets"]
    require(isinstance(targets, list) and targets and len(targets) <= 64, "invalid block coverage targets")
    target_keys = []
    for target in targets:
        require(isinstance(target, dict) and set(target) == {"message_index", "part_index"} and
                type(target["message_index"]) is int and target["message_index"] >= 0 and
                type(target["part_index"]) is int and target["part_index"] >= 0,
                "invalid block coverage target")
        target_keys.append((target["message_index"], target["part_index"]))
    require(len(target_keys) == len(set(target_keys)), "duplicate block coverage target")
    parts, part_by_label = receipt["payload_parts"], {}
    require(isinstance(parts, list) and len(parts) == len(targets), "incomplete block coverage payload parts")
    previous_end = 0
    for part in parts:
        require(isinstance(part, dict) and set(part) == {"message_index", "part_index", "label",
                                                        "token_start", "token_end"},
                "invalid block coverage payload part")
        key = (part["message_index"], part["part_index"])
        require(key in target_keys and type(part["label"]) is int and part["label"] > 0 and
                part["label"] not in part_by_label and type(part["token_start"]) is int and
                type(part["token_end"]) is int and previous_end <= part["token_start"] < part["token_end"] <= receipt["prompt_tokens"],
                "invalid or overlapping block coverage payload part")
        previous_end = part["token_end"]
        part_by_label[part["label"]] = part
    require({(part["message_index"], part["part_index"]) for part in parts} == set(target_keys),
            "block coverage target/part mismatch")
    token_labels = receipt["eligible_token_labels"]
    require(isinstance(token_labels, list) and len(token_labels) == receipt["prompt_tokens"] and
            all(type(label) is int and 0 <= label <= len(targets) for label in token_labels),
            "invalid block coverage token labels")
    require(all(label == 0 for label in token_labels[question["source_start"]:question["source_end"]]),
            "selected block coverage question attribution")
    protected_end = receipt["prompt_tokens"] - policy["keep_recent_tokens"]
    for position, label in enumerate(token_labels):
        if position < policy["keep_prefix_tokens"] or position >= protected_end:
            require(label == 0, "unprotected prefix or recent token")
        if label:
            part = part_by_label.get(label)
            require(part is not None and part["token_start"] <= position < part["token_end"],
                    "eligible token outside payload part")
    expected_ranges, position = [], 0
    while position < len(token_labels):
        label = token_labels[position]
        if label == 0:
            position += 1
            continue
        start = position
        while position < len(token_labels) and token_labels[position] == label:
            position += 1
        part = part_by_label[label]
        expected_ranges.append({"message_index": part["message_index"], "part_index": part["part_index"],
                                "label": label, "start": start, "end": position})
    ranges = receipt["eligible_ranges"]
    require(ranges == expected_ranges, "incomplete or extra runtime eligible ranges")
    expected = []
    for eligible in expected_ranges:
        require(isinstance(eligible, dict) and set(eligible) == {"message_index", "part_index", "label",
                                                                "start", "end"},
                "invalid block coverage eligible range")
        part = part_by_label.get(eligible["label"])
        require(part is not None and (eligible["message_index"], eligible["part_index"]) ==
                (part["message_index"], part["part_index"]) and
                type(eligible["start"]) is int and type(eligible["end"]) is int and
                eligible["start"] < eligible["end"] and
                part["token_start"] <= eligible["start"] and eligible["end"] <= part["token_end"] and
                eligible["start"] >= policy["keep_prefix_tokens"] and
                eligible["end"] <= receipt["prompt_tokens"] - policy["keep_recent_tokens"],
                "invalid or unprotected block coverage eligible range")
        for start in range(eligible["start"], eligible["end"] - BLOCK_TOKENS + 1, BLOCK_TOKENS):
            expected.append({"message_index": eligible["message_index"], "part_index": eligible["part_index"],
                             "label": eligible["label"], "start": start, "end": start + BLOCK_TOKENS})
    require(receipt["eligible_blocks"] == expected, "incomplete or extra runtime eligible block coverage")
    loaded = receipt["loaded_identity"]
    require(isinstance(loaded, dict) and set(loaded) == {"load_generation", "artifact_sha256",
            "tokenizer_sha256", "template_sha256", "layout_sha256", "implementation_sha256"} and
            type(loaded["load_generation"]) is int and loaded["load_generation"] > 0,
            "invalid block coverage loaded identity")
    for field in ("artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256",
                  "implementation_sha256"):
        calibration.identity(loaded[field], "block coverage loaded " + field)
    if loaded_identity is not None:
        require(loaded == loaded_identity, "block coverage loaded identity mismatch")
    bound = receipt["provenance"]
    require(isinstance(bound, dict) and set(bound) == {"binary_sha256", "source_build_sha256"},
            "invalid block coverage provenance")
    for field in bound:
        calibration.identity(bound[field], "block coverage " + field)
    if provenance is not None:
        require(bound == provenance, "block coverage provenance mismatch")
    require(receipt["completed"] is True and receipt["cancelled"] is False, "incomplete block coverage receipt")
    for field in ("elapsed_ns", "status_readback_bytes", "kv_readback_bytes"):
        require(type(receipt[field]) is int and receipt[field] >= 0, "invalid block coverage " + field)
    require(receipt["elapsed_ns"] > 0 and receipt["status_readback_bytes"] > 0 and
            receipt["kv_readback_bytes"] == 0, "invalid block coverage runtime evidence")
    return {"document_sha256": document["sha256"],
            "qualified_context_tokens": receipt["qualified_context_tokens"],
            "prompt_tokens": receipt["prompt_tokens"],
            "reserved_output_tokens": receipt["reserved_output_tokens"],
            "question": question,
            "block_starts": [block["start"] for block in expected], "loaded_identity": loaded,
            "provenance": bound}


def _validate_private_study_observation(value, schema_version=None):
    require = calibration.require
    timing_fields = {"prefill_hidden_ns", "initial_logits_ns", "generated_loop_ns"}
    provider_fields = {"frame_begins", "frame_submits", "frame_wait_ns", "frame_gpu_ns",
        "compute_encoders", "blit_encoders", "precomputed_tail_blocks", "precomputed_tail_failures",
        "paged_attention_1x_calls", "decode_gqa_split_calls", "generated_attention_decode_1x_calls",
        "generated_attention_flash_prefill_calls", "attention_prefill_direct_kv_calls",
        "attention_prefill_paged_kv_calls", "to_host_calls", "to_host_device_calls",
        "host_download_bytes"}
    provider_fields_v7 = provider_fields | {"context_session_paged_decode_framed_dispatches"}
    require(isinstance(value, dict) and set(value) == {"fit", "gate"},
            "invalid private study observation branches")
    for branch, physical_id in (("fit", "fit-study"), ("gate", "gate-study")):
        row = value[branch]
        base_fields = {"physical_branch_id", "timing", "provider_delta"}
        count_fields = {"singleton_paged_decode_forwards",
                        "singleton_paged_decode_expected_layer_dispatches"}
        require(isinstance(row, dict) and frozenset(row) in {frozenset(base_fields),
                frozenset(base_fields | count_fields)} and
                row["physical_branch_id"] == physical_id,
                "invalid private study branch observation")
        if count_fields <= set(row):
            require(type(row["singleton_paged_decode_forwards"]) is int and
                    row["singleton_paged_decode_forwards"] >= 0 and
                    type(row["singleton_paged_decode_expected_layer_dispatches"]) is int and
                    row["singleton_paged_decode_expected_layer_dispatches"] >= 0,
                    "invalid singleton paged-decode expected counts")
        timing = row["timing"]
        provider = row["provider_delta"]
        require(timing is None or (isinstance(timing, dict) and set(timing) == timing_fields and
                all(type(item) is int and item >= 0 for item in timing.values())),
                "invalid private study timing")
        expected_provider_fields = (provider_fields_v7,) if schema_version == 7 else (
            (provider_fields,) if schema_version is not None else (provider_fields, provider_fields_v7))
        require(provider is None or (isinstance(provider, dict) and
                set(provider) in expected_provider_fields and
                all(type(item) is int and item >= 0 for item in provider.values()) and
                provider["frame_submits"] <= provider["frame_begins"] and
                provider["to_host_device_calls"] <= provider["to_host_calls"]),
                "invalid private study provider delta")
    return value


def _validate_private_question_observation(value, schema_version=None):
    provider_fields = {"frame_begins", "frame_submits", "frame_wait_ns", "frame_gpu_ns",
        "compute_encoders", "blit_encoders", "precomputed_tail_blocks", "precomputed_tail_failures",
        "paged_attention_1x_calls", "decode_gqa_split_calls", "generated_attention_decode_1x_calls",
        "generated_attention_flash_prefill_calls", "attention_prefill_direct_kv_calls",
        "attention_prefill_paged_kv_calls", "to_host_calls", "to_host_device_calls",
        "host_download_bytes"}
    calibration.require(isinstance(value, dict) and set(value) == {
            "physical_branch_id", "elapsed_ns", "provider_delta"} and
            value["physical_branch_id"] == "question" and
            type(value["elapsed_ns"]) is int and value["elapsed_ns"] > 0,
            "invalid private question observation")
    provider = value["provider_delta"]
    provider_fields_v7 = provider_fields | {"context_session_paged_decode_framed_dispatches"}
    expected_provider_fields = (provider_fields_v7,) if schema_version == 7 else (
        (provider_fields,) if schema_version is not None else (provider_fields, provider_fields_v7))
    calibration.require(isinstance(provider, dict) and
            set(provider) in expected_provider_fields and
            all(type(item) is int and item >= 0 for item in provider.values()) and
            provider["frame_submits"] <= provider["frame_begins"] and
            provider["to_host_device_calls"] <= provider["to_host_calls"],
            "invalid private question provider delta")
    return value


def prepare_campaign_plan(campaign_root, summary_ref, capture_inputs_ref, source_receipt_ref,
                          model_id, expected_global_owner_shapes, output_path, purpose="calibration"):
    """Build an assembler plan only from one sealed production campaign.

    The exporter index is authoritative for file discovery. Every referenced
    descriptor and tensor is reopened and hashed; filenames or directory
    listings alone never create a base/source cell.
    """
    require = calibration.require
    root = Path(campaign_root).resolve()
    output_path = Path(output_path).resolve()
    require(root.is_dir() and output_path.parent == root and not output_path.exists() and
            purpose == "calibration" and isinstance(model_id, str) and model_id,
            "invalid campaign plan destination")
    require(isinstance(expected_global_owner_shapes, list) and expected_global_owner_shapes,
            "missing predeclared global owner shapes")
    expected_shapes, previous_layer = [], -1
    for shape in expected_global_owner_shapes:
        require(isinstance(shape, dict) and set(shape) == {"layer", "heads", "dim"} and
                type(shape["layer"]) is int and shape["layer"] > previous_layer and
                type(shape["heads"]) is int and 0 < shape["heads"] <= 64 and
                type(shape["dim"]) is int and 0 < shape["dim"] <= 512,
                "invalid predeclared global owner shapes")
        expected_shapes.append(dict(shape))
        previous_layer = shape["layer"]
    summary_path = _capture_artifact(root, summary_ref, "campaign summary")
    summary = calibration.read_json(summary_path)
    legacy_summary_fields = {"schema_version", "stage", "completed", "publication_requested", "branch_order",
        "prefix_storage", "prefix_kernel_route", "admitted_prefill_chunk_rows",
        "prefix_flash_prefill_dispatches", "private_kernel_route", "fit_template_sha256",
        "gate_template_sha256", "generation_contract_sha256", "capture_inputs_manifest_sha256",
        "coverage_receipt_sha256", "request_content_sha256", "prompt_tokens",
        "prompt_tokens_sha256", "question", "document", "eligible_block_starts", "load_generation",
        "live_epoch_before", "live_epoch_after", "live_bytes_before", "live_bytes_after",
        "query_export_bytes", "campaign_query_bytes", "campaign_backend_bytes", "campaign_host_bytes",
        "campaign_physical_bytes", "observer_output_admitted_bytes", "base_index", "artifact_index",
        "kv_export_readback_bytes", "loaded_identity", "provenance"}
    version = summary.get("schema_version") if isinstance(summary, dict) else None
    schema6_fields = {"private_question_observation", "live_session_identity_before",
        "live_session_identity_after", "live_fingerprint_publication_epoch_before",
        "live_fingerprint_publication_epoch_after", "live_fingerprint_logical_tokens_before",
        "live_fingerprint_logical_tokens_after", "live_payload_sha256_before",
        "live_payload_sha256_after", "live_descriptor_sha256_before",
        "live_descriptor_sha256_after", "live_fingerprint_diagnostic_readback_bytes_before",
        "live_fingerprint_diagnostic_readback_bytes_after"}
    expected_fields = (legacy_summary_fields if version == 1 else
        legacy_summary_fields | {"private_replay_attention"} if version == 2 else
        legacy_summary_fields | {"private_replay_attention", "private_study_observation"} if version == 3 else
        legacy_summary_fields | {"private_replay_attention", "private_study_observation",
                                 "study_singleton_paged_decode_requested"} if version in {4, 5} else
        legacy_summary_fields | {"private_replay_attention", "private_study_observation",
                                 "study_singleton_paged_decode_requested"} | schema6_fields)
    require(version in {1, 2, 3, 4, 5, 6, 7} and set(summary) == expected_fields and
            summary["stage"] == "context_query_campaign_capture" and
            summary["completed"] is True and summary["publication_requested"] is False and
            summary["branch_order"] == ["question", "fit", "gate"] and
            summary["prefix_storage"] == "packed_exact" and
            summary["prefix_kernel_route"] == "metal_context_flash_prefill_abi2" and
            summary["private_kernel_route"] in {"fixed_serial_private_replay", "bounded_split_global_replay_v1"},
            "invalid production campaign summary")
    replay_attention = summary.get("private_replay_attention")
    if version == 1:
        require(summary["private_kernel_route"] == "fixed_serial_private_replay" and replay_attention is None,
                "legacy campaign summary cannot claim measured split replay")
    else:
        require(isinstance(replay_attention, dict) and set(replay_attention) == {
            "serial_dispatches", "split_dispatches", "split_scratch_bytes",
            "split_max_query_rows", "split_count"} and
            all(type(replay_attention[field]) is int and replay_attention[field] >= 0
                for field in ("serial_dispatches", "split_dispatches", "split_scratch_bytes")) and
            replay_attention["serial_dispatches"] + replay_attention["split_dispatches"] > 0,
            "invalid private replay attention receipt")
        if summary["private_kernel_route"] == "fixed_serial_private_replay":
            require(replay_attention["split_dispatches"] == 0 and
                    replay_attention["split_scratch_bytes"] == 0 and
                    replay_attention["split_max_query_rows"] is None and
                    replay_attention["split_count"] is None,
                    "serial replay receipt contains split execution")
        else:
            require(replay_attention["split_dispatches"] > 0 and
                    replay_attention["split_scratch_bytes"] > 0 and
                    type(replay_attention["split_max_query_rows"]) is int and
                    0 < replay_attention["split_max_query_rows"] <= 8 and
                    type(replay_attention["split_count"]) is int and
                    2 <= replay_attention["split_count"] <= 32,
                    "split replay receipt lacks bounded route proof")
    study_observation = summary.get("private_study_observation")
    if version in {3, 4, 5, 6, 7}:
        _validate_private_study_observation(study_observation, version)
    else:
        require(study_observation is None, "legacy campaign cannot claim private study observations")
    singleton_paged_decode = summary.get("study_singleton_paged_decode_requested")
    if version in {4, 5, 6, 7}:
        require(type(singleton_paged_decode) is bool,
                "invalid singleton paged-decode qualification request")
        if version in {5, 6, 7}:
            for branch in ("fit", "gate"):
                row = study_observation[branch]
                require({"singleton_paged_decode_forwards",
                         "singleton_paged_decode_expected_layer_dispatches"} <= set(row),
                        "schema5 singleton paged-decode counts missing")
                forwards = row["singleton_paged_decode_forwards"]
                expected = row["singleton_paged_decode_expected_layer_dispatches"]
                provider = row["provider_delta"]
                if version == 7:
                    require(provider is not None and
                            "context_session_paged_decode_framed_dispatches" in provider,
                            "schema7 contextual route counter missing")
                if version == 7 and singleton_paged_decode:
                    require(forwards > 0 and expected > 0 and provider is not None and
                            provider["frame_begins"] >= forwards and
                            provider["frame_submits"] >= forwards and
                            provider["context_session_paged_decode_framed_dispatches"] == expected and
                            provider["precomputed_tail_blocks"] == expected and
                            provider["precomputed_tail_failures"] == 0,
                            "singleton paged-decode request lacks exact contextual route proof")
                elif version == 7:
                    require(forwards == 0 and expected == 0 and provider is not None and
                            provider["context_session_paged_decode_framed_dispatches"] == 0 and
                            provider["precomputed_tail_blocks"] == 0 and
                            provider["precomputed_tail_failures"] == 0,
                            "disabled singleton paged-decode has contextual route counters")
                elif singleton_paged_decode:
                    require(forwards > 0 and expected > 0 and provider is not None and
                            provider["frame_begins"] >= forwards and
                            provider["frame_submits"] >= forwards and
                            provider["generated_attention_decode_1x_calls"] == expected and
                            provider["precomputed_tail_blocks"] == expected and
                            provider["precomputed_tail_failures"] == 0,
                            "singleton paged-decode request lacks exact backend-owned route proof")
                else:
                    require(forwards == 0 and expected == 0,
                            "disabled singleton paged-decode has nonzero expected counts")
        elif singleton_paged_decode:
            for branch in ("fit", "gate"):
                provider = study_observation[branch]["provider_delta"]
                require(provider is not None and provider["frame_begins"] > 0 and
                        provider["frame_submits"] > 0 and
                        provider["generated_attention_decode_1x_calls"] > 0 and
                        provider["precomputed_tail_blocks"] > 0,
                        "singleton paged-decode request lacks backend-owned route proof")
    else:
        require(singleton_paged_decode is None,
                "legacy campaign cannot claim singleton paged-decode evidence")
    question_observation = summary.get("private_question_observation")
    live_fingerprint = None
    if version in {6, 7}:
        _validate_private_question_observation(question_observation, version)
        if version == 7:
            require("context_session_paged_decode_framed_dispatches" in
                    question_observation["provider_delta"] and
                    question_observation["provider_delta"]
                    ["context_session_paged_decode_framed_dispatches"] == 0,
                    "question replay has singleton paged-decode dispatches")
        counter_fields = ("live_session_identity_before", "live_session_identity_after",
            "live_fingerprint_publication_epoch_before", "live_fingerprint_publication_epoch_after",
            "live_fingerprint_logical_tokens_before", "live_fingerprint_logical_tokens_after",
            "live_fingerprint_diagnostic_readback_bytes_before",
            "live_fingerprint_diagnostic_readback_bytes_after")
        require(all(type(summary[field]) is int and summary[field] > 0 for field in counter_fields) and
                summary["live_session_identity_before"] == summary["live_session_identity_after"] and
                summary["live_fingerprint_publication_epoch_before"] == summary["live_epoch_before"] and
                summary["live_fingerprint_publication_epoch_after"] == summary["live_epoch_after"] and
                summary["live_fingerprint_logical_tokens_before"] == summary["prompt_tokens"] and
                summary["live_fingerprint_logical_tokens_after"] == summary["prompt_tokens"] and
                summary["live_fingerprint_diagnostic_readback_bytes_before"] ==
                    summary["live_fingerprint_diagnostic_readback_bytes_after"],
                "invalid live campaign fingerprint counters")
        for stem in ("payload", "descriptor"):
            calibration.identity(summary[f"live_{stem}_sha256_before"], f"live {stem} before")
            calibration.identity(summary[f"live_{stem}_sha256_after"], f"live {stem} after")
            require(summary[f"live_{stem}_sha256_before"] == summary[f"live_{stem}_sha256_after"],
                    f"live {stem} changed during campaign")
        live_fingerprint = {field: summary[field] for field in schema6_fields
                            if field != "private_question_observation"}
        live_fingerprint.update(prompt_tokens=summary["prompt_tokens"],
                                live_epoch_before=summary["live_epoch_before"],
                                live_epoch_after=summary["live_epoch_after"])
    else:
        require(question_observation is None, "legacy campaign cannot claim question provider evidence")
    for field in ("admitted_prefill_chunk_rows", "prefix_flash_prefill_dispatches", "prompt_tokens",
                  "load_generation", "live_epoch_before", "live_epoch_after", "live_bytes_before",
                  "live_bytes_after", "query_export_bytes", "campaign_query_bytes", "campaign_backend_bytes",
                  "campaign_host_bytes", "campaign_physical_bytes", "observer_output_admitted_bytes",
                  "kv_export_readback_bytes"):
        require(type(summary[field]) is int and summary[field] >= 0,
                "invalid campaign summary counter")
    require(summary["admitted_prefill_chunk_rows"] == 64 and
            summary["prefix_flash_prefill_dispatches"] > 0 and
            summary["live_epoch_before"] == summary["live_epoch_after"] and
            summary["live_bytes_before"] == summary["live_bytes_after"] and
            summary["query_export_bytes"] == summary["campaign_query_bytes"] and
            summary["campaign_physical_bytes"] == summary["campaign_backend_bytes"] + summary["campaign_host_bytes"] and
            summary["kv_export_readback_bytes"] > 0,
            "invalid production campaign accounting")
    capture_path = _capture_artifact(root, capture_inputs_ref, "campaign capture inputs")
    require(capture_inputs_ref["sha256"] == summary["capture_inputs_manifest_sha256"],
            "campaign capture-input identity mismatch")
    manifest = calibration.read_json(capture_path)
    source_path = _capture_artifact(root, source_receipt_ref, "campaign source receipt")
    require(source_receipt_ref["sha256"] == summary["provenance"]["source_build_sha256"],
            "campaign source receipt binding mismatch")
    build = release_build.validate(source_path, require_campaign_root=root)
    source_receipt = build["receipt"]
    require(build["receipt_sha256"] == source_receipt_ref["sha256"] and
            build["binary_sha256"] == summary["provenance"]["binary_sha256"],
            "campaign full-release binary/source receipt mismatch")
    coverage_ref, prompt_ref = manifest["coverage_receipt"], {
        key: manifest["prompt"][key] for key in ("path", "sha256")}
    offsets_ref = {key: manifest["prompt_token_offsets"][key] for key in ("path", "sha256")}
    question_ref = {key: manifest["question"]["tokens"][key] for key in ("path", "sha256")}
    validate_capture_inputs(manifest, root, coverage_ref, prompt_ref, offsets_ref, question_ref,
                            summary["loaded_identity"], summary["provenance"], manifest["request"], False,
                            manifest["rendered_prompt"])
    coverage_path = _capture_artifact(root, coverage_ref, "campaign coverage receipt")
    coverage_record = calibration.read_json(coverage_path)
    require(coverage_record.get("prompt_token_offsets_sha256") == offsets_ref["sha256"],
            "campaign prompt token offsets do not match runtime coverage")
    coverage = validate_block_coverage(coverage_record, prompt_ref["sha256"],
                                       summary["loaded_identity"], summary["provenance"], manifest["request"])
    manifest_question = {"source_start": manifest["question"]["source_start"],
                         "source_end": manifest["question"]["source_end"],
                         "token_count": manifest["question"]["tokens"]["count"],
                         "tokens_sha256": manifest["question"]["tokens"]["sha256"]}
    coverage_question = dict(coverage["question"])
    require(coverage_question.pop("message_index", None) == 0 and
            coverage_question.pop("part_index", None) == 1 and
            coverage_ref["sha256"] == summary["coverage_receipt_sha256"] and
            manifest["request"]["request_content_sha256"] == summary["request_content_sha256"] and
            manifest["prompt"]["count"] == summary["prompt_tokens"] and
            prompt_ref["sha256"] == summary["prompt_tokens_sha256"] and
            summary["question"] == manifest_question == coverage_question and
            summary["load_generation"] == summary["loaded_identity"]["load_generation"] and
            coverage["block_starts"] == summary["eligible_block_starts"] and
            calibration.digest(_capture_artifact(root, manifest["fit"]["template"], "fit template")) == summary["fit_template_sha256"] and
            calibration.digest(_capture_artifact(root, manifest["gate"]["template"], "gate template")) == summary["gate_template_sha256"] and
            manifest["fit"]["generation_contract"]["sha256"] == summary["generation_contract_sha256"],
            "campaign summary/input mismatch")

    index_path = _capture_artifact(root, summary["artifact_index"], "campaign artifact index")
    index = calibration.read_json(index_path)
    require(isinstance(index, dict) and set(index) == {"schema_version", "stage", "completed", "artifacts"} and
            index["schema_version"] == 1 and index["stage"] == "context_query_campaign_artifact_index" and
            index["completed"] is True and isinstance(index["artifacts"], list),
            "invalid campaign artifact index")
    artifacts = {}
    for row in index["artifacts"]:
        require(isinstance(row, dict) and set(row) == {"kind", "source_id", "owner_layer", "path", "sha256"},
                "invalid campaign artifact row")
        key = (row["kind"], row["source_id"], row["owner_layer"])
        require(key not in artifacts, "duplicate campaign artifact row")
        _capture_artifact(root, {"path": row["path"], "sha256": row["sha256"]}, "campaign " + row["kind"])
        artifacts[key] = row
    base_row = artifacts.get(("packed_base_index", None, None))
    require(base_row is not None and {key: base_row[key] for key in ("path", "sha256")} == summary["base_index"],
            "campaign base index is not artifact-bound")
    base_index = calibration.read_json(_capture_artifact(root, summary["base_index"], "campaign base index"))
    require(isinstance(base_index, dict) and set(base_index) == {"schema_version", "stage", "completed",
            "prompt", "document", "eligible_block_starts", "descriptors"} and
            base_index["schema_version"] == 1 and base_index["stage"] == "context_packed_exact_block_index" and
            base_index["completed"] is True and base_index["prompt"] == {
                "tokens": summary["prompt_tokens"], "sha256": summary["prompt_tokens_sha256"]} and
            base_index["document"] == summary["document"] and
            base_index["eligible_block_starts"] == summary["eligible_block_starts"],
            "invalid packed base index")
    descriptors, shapes = [], {}
    for ref in base_index["descriptors"]:
        require(isinstance(ref, dict) and set(ref) == {"owner_layer", "block_start", "path", "sha256"},
                "invalid packed base descriptor reference")
        descriptor = calibration.read_json(_capture_artifact(root, {"path": ref["path"], "sha256": ref["sha256"]},
                                                             "packed base descriptor"))
        descriptor_fields = {"schema_version", "stage", "completed", "prefix_storage", "prefix_kernel_route",
            "owner_layer", "block_start", "rows", "heads", "dim", "scale", "consumers", "strata",
            "tensor_order", "tensor_dtype", "keys", "values", "prompt", "document", "loaded_identity",
            "provenance", "kv_export_readback_bytes"}
        require(set(descriptor) == descriptor_fields and
                descriptor.get("schema_version") == 1 and descriptor.get("stage") == "context_packed_exact_block_capture" and
                descriptor.get("completed") is True and descriptor.get("owner_layer") == ref["owner_layer"] and
                descriptor.get("block_start") == ref["block_start"] and descriptor.get("rows") == BLOCK_TOKENS and
                type(descriptor.get("heads")) is int and 0 < descriptor["heads"] <= 64 and
                type(descriptor.get("dim")) is int and 0 < descriptor["dim"] <= 512 and
                type(descriptor.get("scale")) in (int, float) and descriptor["scale"] > 0 and
                descriptor.get("tensor_order") == "logical_row_kv_head_head_dim" and
                descriptor.get("tensor_dtype") == "f16_le" and descriptor.get("prompt") == base_index["prompt"] and
                descriptor.get("document") == summary["document"] and descriptor.get("loaded_identity") == summary["loaded_identity"] and
                descriptor.get("provenance") == summary["provenance"] and descriptor.get("kv_export_readback_bytes") ==
                2 * BLOCK_TOKENS * descriptor.get("heads", 0) * descriptor.get("dim", 0) * 2,
                "invalid packed base descriptor")
        expected_tensor_bytes = BLOCK_TOKENS * descriptor["heads"] * descriptor["dim"] * 2
        for tensor in ("keys", "values"):
            tensor_path = _capture_artifact(root, descriptor[tensor], "packed base " + tensor,
                                            expected_tensor_bytes)
            require(tensor_path.stat().st_size == expected_tensor_bytes,
                    "packed base tensor size mismatch")
        key = (ref["owner_layer"], ref["block_start"])
        require(key not in {(x[0], x[1]) for x in descriptors}, "duplicate packed owner block")
        descriptors.append((ref["owner_layer"], ref["block_start"], descriptor))
        shape = {"layer": ref["owner_layer"], "heads": descriptor["heads"], "dim": descriptor["dim"]}
        require(ref["owner_layer"] not in shapes or shapes[ref["owner_layer"]] == shape,
                "packed owner shape drift")
        shapes[ref["owner_layer"]] = shape
    shape_rows = [shapes[layer] for layer in sorted(shapes)]
    require(shape_rows == expected_shapes, "packed global owner shapes differ from predeclared layout")
    expected_blocks = {(shape["layer"], block) for shape in expected_shapes
                       for block in summary["eligible_block_starts"]}
    require({(layer, block) for layer, block, _ in descriptors} == expected_blocks,
            "incomplete packed owner/block capture")

    source_ids = ("request-question-replay", "fit-study-prefill", "fit-study-response",
                  "gate-study-prefill", "gate-study-response")
    source_kinds = ("request_question_replay", "fixed_study_prefill", "fixed_study_generated_response",
                    "fixed_study_prefill", "fixed_study_generated_response")
    partitions = ("training", "training", "training", "validation", "validation")
    branches = ("question", "fit-study", "fit-study", "gate-study", "gate-study")
    conditions = ("completed_prompt_private_replay", "completed_prompt_private_replay",
                  "completed_prompt_study_generated_response", "completed_prompt_private_replay",
                  "completed_prompt_study_generated_response")
    templates = (manifest["rendered_prompt"], manifest["fit"]["template"], manifest["fit"]["template"],
                 manifest["gate"]["template"], manifest["gate"]["template"])
    sources = []
    branch_attention = {}
    for source_id, kind, partition, branch, conditioning, template in zip(
            source_ids, source_kinds, partitions, branches, conditions, templates):
        token = artifacts.get(("source_tokens", source_id, None))
        require(token is not None, "missing campaign source tokens")
        owners = []
        for layer in sorted(shapes):
            descriptor = artifacts.get(("source_descriptor", source_id, layer))
            queries = artifacts.get(("source_queries", source_id, layer))
            positions = artifacts.get(("source_positions", source_id, layer))
            require(descriptor and queries and positions, "incomplete campaign source owner artifacts")
            descriptor_record = calibration.read_json(_capture_artifact(
                root, {"path": descriptor["path"], "sha256": descriptor["sha256"]},
                "campaign source descriptor"))
            attention_fields = ("replay_split_scratch_bytes", "serial_attention_dispatches",
                                "split_attention_dispatches")
            if version in {2, 3, 4, 5, 6, 7}:
                require(all(type(descriptor_record.get(field)) is int and descriptor_record[field] >= 0
                            for field in attention_fields),
                        "schema2 source descriptor lacks replay attention receipt")
                observed = tuple(descriptor_record[field] for field in attention_fields)
                prior = branch_attention.setdefault(branch, observed)
                require(prior == observed, "physical branch replay attention receipts disagree")
            else:
                require(all(field not in descriptor_record for field in attention_fields),
                        "legacy source descriptor cannot claim measured replay attention")
            owners.append({"layer": layer, "descriptor": descriptor["path"], "descriptor_sha256": descriptor["sha256"],
                           "queries": queries["path"], "queries_sha256": queries["sha256"],
                           "positions": positions["path"], "positions_sha256": positions["sha256"]})
        source = {"id": source_id, "kind": kind, "partition": partition,
                  "physical_branch_id": branch, "capture_route": "private_model_replay",
                  "conditioning": conditioning, "template": template["path"],
                  "template_sha256": template["sha256"], "tokens": token["path"],
                  "tokens_sha256": token["sha256"], "owners": owners}
        if kind.startswith("fixed_study"):
            source["study_pair_id"] = branch
        if kind == "fixed_study_generated_response":
            source.update(generation_contract=manifest["fit" if partition == "training" else "gate"]["generation_contract"]["path"],
                          generation_contract_sha256=summary["generation_contract_sha256"],
                          generation_mode="greedy_fixed_length_ignore_eos")
        sources.append(source)
    if version in {2, 3, 4, 5, 6, 7}:
        require(set(branch_attention) == {"question", "fit-study", "gate-study"},
                "incomplete replay attention branch receipts")
        observed_scratch = {value[0] for value in branch_attention.values()}
        require(sum(value[1] for value in branch_attention.values()) == replay_attention["serial_dispatches"] and
                sum(value[2] for value in branch_attention.values()) == replay_attention["split_dispatches"] and
                observed_scratch == {replay_attention["split_scratch_bytes"]},
                "campaign replay attention summary does not match physical branches")
    require(len(artifacts) == 1 + len(source_ids) + len(source_ids) * len(shapes) * 3,
            "extra campaign artifacts are not plan-bound")

    source = source_receipt.get("source")
    git_head = source_receipt.get("head")
    require(isinstance(source, dict) and re.fullmatch(r"[0-9a-f]{40}", git_head or "") and
            isinstance(source.get("files"), dict) and source["files"], "invalid source receipt manifest")
    base_dir = root / (output_path.stem + "-base-specs")
    require(not base_dir.exists(), "campaign base-spec directory already exists")
    base_dir.mkdir()
    try:
        base_specs = []
        for layer, block, descriptor in descriptors:
            strata = [{**row, "training_start": 0, "training_count": 1,
                       "validation_start": 0, "validation_count": 1} for row in descriptor["strata"]]
            spec = {"schema_version": 1, "purpose": purpose,
                    "model": {"id": model_id, "sha256": summary["loaded_identity"]["artifact_sha256"],
                              "tokenizer_sha256": summary["loaded_identity"]["tokenizer_sha256"],
                              "template_sha256": summary["loaded_identity"]["template_sha256"],
                              "layout_sha256": summary["loaded_identity"]["layout_sha256"],
                              "implementation_sha256": summary["loaded_identity"]["implementation_sha256"],
                              "global_owner_shapes": shape_rows},
                    "document": summary["document"],
                    "source": {"git_head": git_head, "files": source["files"],
                               "binary_sha256": summary["provenance"]["binary_sha256"],
                               "build_receipt_sha256": source_receipt_ref["sha256"]},
                    "capture": {"rows": BLOCK_TOKENS, "heads": descriptor["heads"], "dim": descriptor["dim"],
                                "training_rows": len(strata), "validation_rows": len(strata),
                                "block_start": block, "owner_layer": layer, "scale": descriptor["scale"],
                                "query_origin": "multi_branch_v2", "query_sampling": "source_uniform_partition_v3",
                                "consumers": descriptor["consumers"], "strata": strata},
                    "tensors": {name: os.path.relpath(root / descriptor[name]["path"], base_dir)
                                for name in ("keys", "values")}}
            name = f"layer{layer}-block{block}.json"
            calibration.write_json(base_dir / name, spec)
            base_specs.append(str((base_dir / name).relative_to(root)))
        plan = {"schema_version": 2, "purpose": purpose, "global_owners": sorted(shapes),
                "global_owner_shapes": expected_shapes,
                "coverage_receipts": [{"receipt": coverage_ref["path"], "sha256": coverage_ref["sha256"],
                                       "prompt_tokens": prompt_ref["path"],
                                       "prompt_tokens_sha256": prompt_ref["sha256"]}],
                "private_kernel_route": summary["private_kernel_route"] if version in {2, 3, 4, 5, 6, 7} else None,
                "private_replay_attention": replay_attention,
                "private_question_observation": question_observation,
                "live_context_fingerprint": live_fingerprint,
                "private_study_observation": study_observation,
                "study_singleton_paged_decode_requested": singleton_paged_decode,
                "base_specs": base_specs, "sources": sources}
        calibration.write_json(output_path, plan)
    except Exception:
        shutil.rmtree(base_dir)
        raise
    return plan


def assemble_v2(plan_path, output_dir):
    """Assemble raw per-source observations into sealed schema-2 captures.

    The plan supplies only immutable provenance and file locations. Tensor
    dimensions, logical positions, timings, and allocation totals are derived
    from the runtime source observations and revalidated by `seal`.
    """
    require, np = calibration.require, calibration.np
    plan_path, output_dir = Path(plan_path).resolve(), Path(output_dir).resolve()
    plan = calibration.read_json(plan_path)
    require(plan.get("schema_version") == 2 and plan.get("purpose") in {"calibration", "evaluation"},
            "invalid schema2 assembly plan")
    base_specs = plan.get("base_specs")
    sources = plan.get("sources")
    coverage_entries = plan.get("coverage_receipts")
    diagnostic_without_coverage = plan.get("diagnostic_without_block_coverage") is True
    private_kernel_route = plan.get("private_kernel_route")
    private_replay_attention = plan.get("private_replay_attention")
    private_study_observation = plan.get("private_study_observation")
    private_question_observation = plan.get("private_question_observation")
    live_context_fingerprint = plan.get("live_context_fingerprint")
    study_singleton_paged_decode_requested = plan.get("study_singleton_paged_decode_requested")
    if private_study_observation is not None:
        _validate_private_study_observation(private_study_observation)
    if private_question_observation is not None:
        _validate_private_question_observation(private_question_observation)
    if live_context_fingerprint is not None:
        expected_fingerprint_fields = {"live_session_identity_before", "live_session_identity_after",
            "live_fingerprint_publication_epoch_before", "live_fingerprint_publication_epoch_after",
            "live_fingerprint_logical_tokens_before", "live_fingerprint_logical_tokens_after",
            "live_payload_sha256_before", "live_payload_sha256_after",
            "live_descriptor_sha256_before", "live_descriptor_sha256_after",
            "live_fingerprint_diagnostic_readback_bytes_before",
            "live_fingerprint_diagnostic_readback_bytes_after", "prompt_tokens",
            "live_epoch_before", "live_epoch_after"}
        require(isinstance(live_context_fingerprint, dict) and
                set(live_context_fingerprint) == expected_fingerprint_fields and
                all(type(live_context_fingerprint[field]) is int and
                    live_context_fingerprint[field] > 0 for field in (
                        "live_session_identity_before", "live_session_identity_after",
                        "live_fingerprint_publication_epoch_before",
                        "live_fingerprint_publication_epoch_after",
                        "live_fingerprint_logical_tokens_before",
                        "live_fingerprint_logical_tokens_after",
                        "live_fingerprint_diagnostic_readback_bytes_before",
                        "live_fingerprint_diagnostic_readback_bytes_after", "prompt_tokens",
                        "live_epoch_before", "live_epoch_after")) and
                live_context_fingerprint["live_session_identity_before"] ==
                    live_context_fingerprint["live_session_identity_after"] and
                live_context_fingerprint["live_fingerprint_publication_epoch_before"] ==
                    live_context_fingerprint["live_epoch_before"] and
                live_context_fingerprint["live_fingerprint_publication_epoch_after"] ==
                    live_context_fingerprint["live_epoch_after"] and
                live_context_fingerprint["live_epoch_before"] ==
                    live_context_fingerprint["live_epoch_after"] and
                live_context_fingerprint["live_fingerprint_logical_tokens_before"] ==
                    live_context_fingerprint["prompt_tokens"] and
                live_context_fingerprint["live_fingerprint_logical_tokens_after"] ==
                    live_context_fingerprint["prompt_tokens"] and
                live_context_fingerprint["live_fingerprint_diagnostic_readback_bytes_before"] ==
                    live_context_fingerprint["live_fingerprint_diagnostic_readback_bytes_after"],
                "invalid live context fingerprint plan evidence")
        for stem in ("payload", "descriptor"):
            before = live_context_fingerprint[f"live_{stem}_sha256_before"]
            after = live_context_fingerprint[f"live_{stem}_sha256_after"]
            calibration.identity(before, f"live {stem} before")
            calibration.identity(after, f"live {stem} after")
            require(before == after, f"live {stem} changed during campaign")
    require((private_question_observation is None) == (live_context_fingerprint is None),
            "question provider and live fingerprint evidence must be paired")
    require(study_singleton_paged_decode_requested is None or
            type(study_singleton_paged_decode_requested) is bool,
            "invalid singleton paged-decode plan evidence")
    context_witness = (private_study_observation is not None and all(
        private_study_observation[branch]["provider_delta"] is not None and
        "context_session_paged_decode_framed_dispatches" in
            private_study_observation[branch]["provider_delta"]
        for branch in ("fit", "gate")))
    if study_singleton_paged_decode_requested:
        require(private_study_observation is not None and all(
                private_study_observation[branch]["provider_delta"] is not None and
                private_study_observation[branch]["singleton_paged_decode_forwards"] > 0 and
                private_study_observation[branch]["singleton_paged_decode_expected_layer_dispatches"] > 0 and
                private_study_observation[branch]["provider_delta"]["frame_begins"] >=
                    private_study_observation[branch]["singleton_paged_decode_forwards"] and
                private_study_observation[branch]["provider_delta"]["frame_submits"] >=
                    private_study_observation[branch]["singleton_paged_decode_forwards"] and
                (("context_session_paged_decode_framed_dispatches" in
                  private_study_observation[branch]["provider_delta"] and
                  private_study_observation[branch]["provider_delta"]
                    ["context_session_paged_decode_framed_dispatches"] ==
                    private_study_observation[branch]["singleton_paged_decode_expected_layer_dispatches"]) or
                 ("context_session_paged_decode_framed_dispatches" not in
                  private_study_observation[branch]["provider_delta"] and
                  private_study_observation[branch]["provider_delta"]["generated_attention_decode_1x_calls"] ==
                    private_study_observation[branch]["singleton_paged_decode_expected_layer_dispatches"])) and
                private_study_observation[branch]["provider_delta"]["precomputed_tail_blocks"] ==
                    private_study_observation[branch]["singleton_paged_decode_expected_layer_dispatches"] and
                private_study_observation[branch]["provider_delta"]["precomputed_tail_failures"] == 0
                for branch in ("fit", "gate")),
                "singleton paged-decode plan lacks exact backend-owned route proof")
    elif context_witness:
        require(all(
                private_study_observation[branch]
                    ["singleton_paged_decode_forwards"] == 0 and
                private_study_observation[branch]
                    ["singleton_paged_decode_expected_layer_dispatches"] == 0 and
                private_study_observation[branch]["provider_delta"]
                    ["context_session_paged_decode_framed_dispatches"] == 0 and
                private_study_observation[branch]["provider_delta"]
                    ["precomputed_tail_blocks"] == 0 and
                private_study_observation[branch]["provider_delta"]
                    ["precomputed_tail_failures"] == 0
                for branch in ("fit", "gate")),
                "disabled singleton paged-decode plan has contextual route counters")
    require((private_kernel_route is None and private_replay_attention is None) or
            (private_kernel_route in {"fixed_serial_private_replay", "bounded_split_global_replay_v1"} and
             isinstance(private_replay_attention, dict) and set(private_replay_attention) == {
                "serial_dispatches", "split_dispatches", "split_scratch_bytes",
                "split_max_query_rows", "split_count"}),
            "invalid private replay route receipt")
    if private_replay_attention is not None:
        require(all(type(private_replay_attention[field]) is int and private_replay_attention[field] >= 0
                    for field in ("serial_dispatches", "split_dispatches", "split_scratch_bytes")) and
                private_replay_attention["serial_dispatches"] +
                    private_replay_attention["split_dispatches"] > 0,
                "invalid private replay dispatch counters")
    if private_kernel_route == "fixed_serial_private_replay":
        require(private_replay_attention["split_dispatches"] == 0 and
                private_replay_attention["split_scratch_bytes"] == 0 and
                private_replay_attention["split_max_query_rows"] is None and
                private_replay_attention["split_count"] is None,
                "serial replay plan contains split execution")
    elif private_kernel_route == "bounded_split_global_replay_v1":
        require(type(private_replay_attention["split_dispatches"]) is int and
                private_replay_attention["split_dispatches"] > 0 and
                type(private_replay_attention["split_scratch_bytes"]) is int and
                private_replay_attention["split_scratch_bytes"] > 0 and
                type(private_replay_attention["split_max_query_rows"]) is int and
                0 < private_replay_attention["split_max_query_rows"] <= 8 and
                type(private_replay_attention["split_count"]) is int and
                2 <= private_replay_attention["split_count"] <= 32,
                "split replay plan lacks bounded route proof")
    require(isinstance(base_specs, list) and base_specs and isinstance(sources, list) and sources,
            "empty schema2 assembly plan")
    require((isinstance(coverage_entries, list) and bool(coverage_entries)) != diagnostic_without_coverage,
            "schema2 assembly requires sealed coverage or explicit diagnostic mode")
    require(not output_dir.exists(), "capture output already exists")
    source_ids = [x.get("id") for x in sources]
    require(len(source_ids) == len(set(source_ids)) and all(isinstance(x, str) and re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", x)
                                                           for x in source_ids), "invalid/duplicate source ids")
    partitions = {name: [x for x in sources if x.get("partition") == name]
                  for name in ("training", "validation", "evaluation")}
    require(partitions["training"] and partitions["validation"], "missing fit/gate query sources")
    require((plan["purpose"] == "evaluation") == bool(partitions["evaluation"]),
            "teacher sources must exactly match evaluation purpose")
    for source in sources:
        require(source.get("kind") in {"request_question_prefill", "request_question_replay", "fixed_study_prefill",
                                       "fixed_study_generated_response",
                                       "exact_teacher_continuation"}, "invalid query source kind")
        calibration.identity(source.get("template_sha256"), "query source template")
        template_path = _plan_path(plan_path, source.get("template"))
        require(calibration.digest(template_path) == source["template_sha256"], "query source template identity mismatch")
        if source["kind"] in {"fixed_study_prefill", "fixed_study_generated_response"}:
            require(isinstance(source.get("study_pair_id"), str), "missing study pair identity")
        if source["kind"] == "fixed_study_generated_response":
            calibration.identity(source.get("generation_contract_sha256"), "study generation contract")
            require(source.get("generation_mode") == "greedy_fixed_length_ignore_eos",
                    "unsupported study generation mode")
            contract_path = _plan_path(plan_path, source.get("generation_contract"))
            require(calibration.digest(contract_path) == source["generation_contract_sha256"],
                    "study generation contract identity mismatch")
        source["_tokens_path"] = _plan_path(plan_path, source.get("tokens"))
        actual_tokens_sha256 = calibration.digest(source["_tokens_path"])
        if diagnostic_without_coverage:
            source["tokens_sha256"] = actual_tokens_sha256
        else:
            calibration.identity(source.get("tokens_sha256"), "query source token artifact")
            require(source["tokens_sha256"] == actual_tokens_sha256,
                    "query source token artifact identity mismatch")
        owners = source.get("owners")
        require(isinstance(owners, list) and owners, "missing query source owners")
        branch_id = source.get("physical_branch_id")
        if branch_id is None and diagnostic_without_coverage:
            branch_id = source["id"]
            source["physical_branch_id"] = branch_id
        require(isinstance(branch_id, str) and re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", branch_id),
                "missing/invalid physical query branch identity")
    canonical_runs = {}
    for source in sources:
        for cell in source["owners"]:
            descriptor_path = _plan_path(plan_path, cell.get("descriptor"))
            if not diagnostic_without_coverage:
                calibration.identity(cell.get("descriptor_sha256"), "source descriptor artifact")
                require(calibration.digest(descriptor_path) == cell["descriptor_sha256"],
                        "source descriptor artifact identity mismatch")
            descriptor = calibration.read_json(descriptor_path)
            require(descriptor.get("source_id") == source["id"], "source descriptor identity mismatch")
            if not diagnostic_without_coverage:
                require(descriptor.get("capture_route") == "private_model_replay",
                        "production source did not use private model replay")
                for field in ("tokens_sha256", "queries_sha256", "positions_sha256"):
                    calibration.identity(descriptor.get(field), "source descriptor " + field)
                require(descriptor["tokens_sha256"] == source["tokens_sha256"],
                        "source descriptor token identity mismatch")
            if diagnostic_without_coverage:
                descriptor.setdefault("physical_branch_id", source["physical_branch_id"])
                descriptor.setdefault("branch_logical_start", descriptor.get("logical_start"))
                descriptor.setdefault("branch_logical_count", descriptor.get("logical_count"))
                descriptor.setdefault("branch_rows_per_stratum", descriptor.get("rows_per_stratum"))
            observation = {key: descriptor.get(key) for key in
                           ("source_id", "kind", "partition", "capture_route", "conditioning", "logical_start",
                            "logical_count", "branch_logical_start", "branch_logical_count",
                            "study_pair_id", "generation_contract_sha256",
                            "generation_mode", "physical_branch_id",
                            "loaded_identity",
                            "completed", "cancelled", "elapsed_ns", "physical_bytes", "query_bytes",
                            "status_readback_bytes", "kv_readback_bytes",
                            "replay_split_scratch_bytes", "serial_attention_dispatches",
                            "split_attention_dispatches")}
            if observation["physical_branch_id"] is None and diagnostic_without_coverage:
                observation["physical_branch_id"] = source["physical_branch_id"]
            require(observation["physical_branch_id"] == source["physical_branch_id"],
                    "source descriptor physical branch mismatch")
            prior = canonical_runs.setdefault(source["id"], observation)
            require(prior == observation, "source runtime observation differs across owners")
    identity_fields = ("artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256",
                       "implementation_sha256")
    for row in canonical_runs.values():
        loaded = row.get("loaded_identity")
        require(isinstance(loaded, dict) and type(loaded.get("load_generation")) is int and
                loaded["load_generation"] > 0, "invalid runtime-loaded identity")
        for field in identity_fields:
            calibration.identity(loaded.get(field), "runtime-loaded " + field)
    loaded_identities = {tuple(row["loaded_identity"].get(field) for field in identity_fields)
                         for row in canonical_runs.values()}
    require(len(loaded_identities) == 1, "query sources disagree on loaded identity")
    source_loaded = next(iter(canonical_runs.values()))["loaded_identity"]
    coverage_by_document = {}
    for entry in coverage_entries or []:
        require(isinstance(entry, dict) and set(entry) == {"receipt", "sha256", "prompt_tokens",
                                                           "prompt_tokens_sha256"},
                "invalid block coverage plan entry")
        receipt_path = _plan_path(plan_path, entry["receipt"])
        calibration.identity(entry["sha256"], "block coverage receipt")
        require(calibration.digest(receipt_path) == entry["sha256"], "block coverage receipt digest mismatch")
        prompt_path = _plan_path(plan_path, entry["prompt_tokens"])
        calibration.identity(entry["prompt_tokens_sha256"], "block coverage prompt tokens")
        require(calibration.digest(prompt_path) == entry["prompt_tokens_sha256"],
                "block coverage prompt token digest mismatch")
        receipt = calibration.read_json(receipt_path)
        require(prompt_path.stat().st_size == receipt.get("prompt_tokens", -1) * 8,
                "block coverage prompt token count mismatch")
        coverage = validate_block_coverage(receipt, entry["prompt_tokens_sha256"], source_loaded)
        document = coverage["document_sha256"]
        require(document not in coverage_by_document, "duplicate block coverage document")
        coverage_by_document[document] = (receipt, coverage, entry)
    temporary = Path(tempfile.mkdtemp(prefix=".attention-v2-", dir=output_dir.parent))
    try:
        manifests, owner_layers, owner_blocks = [], set(), set()
        for base_name in base_specs:
            base_path = _plan_path(plan_path, base_name)
            base = calibration.read_json(base_path)
            require(base.get("schema_version") == 1 and base.get("purpose") == plan["purpose"],
                    "invalid base capture spec")
            c = base["capture"]
            layer, heads, dim = c["owner_layer"], c["heads"], c["dim"]
            document_sha256 = base["document"]["sha256"]
            coverage_binding = None
            if diagnostic_without_coverage:
                require(base.get("purpose") != "evaluation", "evaluation requires runtime block coverage")
            else:
                require(document_sha256 in coverage_by_document and
                        base["document"] == coverage_by_document[document_sha256][0]["document"],
                        "base capture lacks matching block coverage document")
                expected_provenance = {"binary_sha256": base["source"].get("binary_sha256"),
                                       "source_build_sha256": base["source"].get("build_receipt_sha256")}
                validate_block_coverage(coverage_by_document[document_sha256][0],
                                        loaded_identity=source_loaded, provenance=expected_provenance)
                require(c["block_start"] in coverage_by_document[document_sha256][1]["block_starts"],
                        "extra runtime block coverage")
                coverage_entry = coverage_by_document[document_sha256][2]
                coverage_binding = {"receipt_sha256": coverage_entry["sha256"],
                                    "prompt_tokens_sha256": coverage_entry["prompt_tokens_sha256"],
                                    "prompt_token_offsets_sha256": coverage_by_document[document_sha256][0]["prompt_token_offsets_sha256"],
                                    "rendered_prompt_sha256": coverage_by_document[document_sha256][0]["rendered_prompt_sha256"],
                                    "admitted_prefill_chunk_rows": coverage_by_document[document_sha256][0]["admitted_prefill_chunk_rows"],
                                    "qualified_context_tokens": coverage_by_document[document_sha256][1]["qualified_context_tokens"],
                                    "prompt_tokens": coverage_by_document[document_sha256][1]["prompt_tokens"],
                                    "reserved_output_tokens": coverage_by_document[document_sha256][1]["reserved_output_tokens"]}
            block_key = (document_sha256, layer, c["block_start"])
            require(block_key not in owner_blocks, "duplicate base owner block")
            owner_blocks.add(block_key)
            owner_layers.add(layer)
            strata_count = len(c["strata"])
            require(strata_count > 0, "base capture lacks strata")
            # Query pools are laid out independently per KV head. The runtime
            # protocol lists every consumer query head, so using the global
            # stratum count here overstates rows/head whenever kv_heads > 1.
            strata_by_head = {head: [row for row in c["strata"] if row["kv_head"] == head]
                              for head in range(heads)}
            per_head_counts = {len(rows) for rows in strata_by_head.values()}
            require(len(per_head_counts) == 1 and next(iter(per_head_counts)) > 0 and
                    sum(per_head_counts) * heads == strata_count,
                    "base capture strata do not partition equally by KV head")
            strata_per_head = next(iter(per_head_counts))
            source_rows = {}
            for source in sources:
                cells = [x for x in source["owners"] if x.get("layer") == layer]
                require(len(cells) == 1, "missing/duplicate source owner cell")
                descriptor_path = _plan_path(plan_path, cells[0].get("descriptor"))
                descriptor = calibration.read_json(descriptor_path)
                if diagnostic_without_coverage:
                    descriptor.setdefault("physical_branch_id", source["physical_branch_id"])
                    descriptor.setdefault("branch_logical_start", descriptor.get("logical_start"))
                    descriptor.setdefault("branch_logical_count", descriptor.get("logical_count"))
                    descriptor.setdefault("branch_rows_per_stratum", descriptor.get("rows_per_stratum"))
                for key in ("source_id", "kind", "partition", "capture_route", "conditioning",
                            "physical_branch_id"):
                    expected = source["id"] if key == "source_id" else source.get(key)
                    if key == "physical_branch_id" and descriptor.get(key) is None and diagnostic_without_coverage:
                        descriptor[key] = expected
                    require(descriptor.get(key) == expected, f"source descriptor {key} mismatch")
                if source["kind"] in {"fixed_study_prefill", "fixed_study_generated_response"}:
                    require(descriptor.get("study_pair_id") == source["study_pair_id"], "source descriptor study pair mismatch")
                if source["kind"] == "fixed_study_generated_response":
                    require(descriptor.get("generation_contract_sha256") == source["generation_contract_sha256"],
                            "source descriptor generation contract mismatch")
                    require(descriptor.get("generation_mode") == source["generation_mode"],
                            "source descriptor generation mode mismatch")
                require(descriptor.get("owner_layer") == layer and descriptor.get("heads") == heads and
                        descriptor.get("dim") == dim and type(descriptor.get("rows_per_head")) is int and
                        type(descriptor.get("rows_per_stratum")) is int and
                        descriptor["rows_per_head"] == descriptor["rows_per_stratum"] * strata_per_head,
                        "source descriptor geometry mismatch")
                for field in ("branch_logical_start", "branch_logical_count", "branch_rows_per_stratum"):
                    require(type(descriptor.get(field)) is int and descriptor[field] > 0,
                            "source descriptor branch geometry mismatch")
                require(descriptor["branch_logical_start"] <= descriptor["logical_start"] and
                        descriptor["logical_start"] + descriptor["logical_count"] <=
                        descriptor["branch_logical_start"] + descriptor["branch_logical_count"] and
                        descriptor["rows_per_stratum"] <= descriptor["branch_rows_per_stratum"],
                        "source outside physical query branch")
                query_path = _plan_path(plan_path, cells[0].get("queries"))
                position_path = _plan_path(plan_path, cells[0].get("positions"))
                if not diagnostic_without_coverage:
                    calibration.identity(cells[0].get("queries_sha256"), "source query artifact")
                    calibration.identity(cells[0].get("positions_sha256"), "source position artifact")
                    require(cells[0]["queries_sha256"] == descriptor["queries_sha256"] and
                            calibration.digest(query_path) == descriptor["queries_sha256"],
                            "source query artifact identity mismatch")
                    require(cells[0]["positions_sha256"] == descriptor["positions_sha256"] and
                            calibration.digest(position_path) == descriptor["positions_sha256"],
                            "source position artifact identity mismatch")
                shape = (heads, descriptor["rows_per_head"], dim)
                queries = calibration.raw_array(query_path, "<f4", shape)
                positions = calibration.raw_array(position_path, "<u4", shape[:2])
                expected_positions = calibration.replay_positions(
                    descriptor["logical_start"], descriptor["logical_count"],
                    descriptor["rows_per_stratum"], descriptor["partition"] != "training")
                require(len(expected_positions) == descriptor["rows_per_stratum"],
                        "source rows do not partition physical query branch")
                for head in range(heads):
                    for stratum in range(strata_per_head):
                        rows = positions[head, stratum * descriptor["rows_per_stratum"]:(stratum + 1) * descriptor["rows_per_stratum"]]
                        require(np.array_equal(rows, expected_positions), "source positions do not match declared sampling")
                source_rows[source["id"]] = (descriptor, queries, positions)
                token_bytes = source["_tokens_path"].stat().st_size
                require(token_bytes == descriptor["logical_count"] * 8 and token_bytes <= 8192 * 8,
                        "query source token count mismatch")
                token_ids = np.fromfile(source["_tokens_path"], dtype="<i8")
                require(np.all(token_ids >= 0), "invalid query source token ids")
            combined_queries, combined_positions = [], []
            partition_totals = {name: sum(source_rows[x["id"]][0]["rows_per_head"] for x in rows)
                                for name, rows in partitions.items()}
            total_rows = sum(partition_totals.values())
            require(0 < total_rows <= calibration.MAX_QUERY_ROWS_V2, "assembled queries exceed schema2 limit")
            for head in range(heads):
                q_parts, p_parts = [], []
                for partition in ("training", "validation", "evaluation"):
                    for stratum in range(strata_per_head):
                        for source in partitions[partition]:
                            descriptor, query, position = source_rows[source["id"]]
                            count = descriptor["rows_per_stratum"]
                            q_parts.append(query[head, stratum * count:(stratum + 1) * count])
                            p_parts.append(position[head, stratum * count:(stratum + 1) * count])
                combined_queries.append(np.concatenate(q_parts))
                combined_positions.append(np.concatenate(p_parts))
            queries = np.stack(combined_queries).astype("<f4", copy=False)
            positions = np.stack(combined_positions).astype("<u4", copy=False)
            require(queries.shape == (heads, total_rows, dim) and positions.shape == (heads, total_rows),
                    "assembled query shape mismatch")
            layer_dir = temporary / f"layer{layer}-block{c['block_start']}-raw"
            layer_dir.mkdir()
            queries.tofile(layer_dir / "queries.raw")
            positions.tofile(layer_dir / "positions.raw")
            assembled_sources = []
            for source in sources:
                descriptor = source_rows[source["id"]][0]
                assembled_sources.append({
                    "id": source["id"], "partition": source["partition"], "kind": source["kind"],
                    "capture_route": source["capture_route"], "conditioning": source["conditioning"],
                    "physical_branch_id": source["physical_branch_id"],
                    "loaded_identity": descriptor["loaded_identity"],
                    "logical_start": descriptor["logical_start"], "logical_count": descriptor["logical_count"],
                    "branch_logical_start": descriptor["branch_logical_start"],
                    "branch_logical_count": descriptor["branch_logical_count"],
                    "branch_rows_per_stratum": descriptor["branch_rows_per_stratum"],
                    "tokens_sha256": source["tokens_sha256"], "template_sha256": source["template_sha256"],
                    "sampling_partition": "training" if source["partition"] == "training" else "validation",
                    "selection_visible": source["partition"] == "training",
                    "private": source["kind"] != "request_question_prefill", "usage_visible": False,
                    **({"predeclared": True} if source["kind"] in
                       {"fixed_study_prefill", "fixed_study_generated_response"} else {}),
                    **({"study_pair_id": source["study_pair_id"]} if source["kind"] in
                       {"fixed_study_prefill", "fixed_study_generated_response"} else {}),
                    **({"generation_contract_sha256": source["generation_contract_sha256"]}
                       if source["kind"] == "fixed_study_generated_response" else {}),
                    **({"generation_mode": source["generation_mode"]}
                       if source["kind"] == "fixed_study_generated_response" else {}),
                })
            new_strata = []
            per_head_stratum_index = [0] * heads
            for old in c["strata"]:
                head = old["kv_head"]
                local_index = per_head_stratum_index[head]
                per_head_stratum_index[head] += 1
                row = {key: old[key] for key in ("consumer_layer", "query_head", "kv_head")}
                for partition in ("training", "validation", "evaluation"):
                    offset, ranges = 0, []
                    for source in partitions[partition]:
                        count = source_rows[source["id"]][0]["rows_per_stratum"]
                        ranges.append({"source_id": source["id"], "start": offset, "count": count})
                        offset += count
                    if offset:
                        row[partition + "_start"] = local_index * offset
                        row[partition + "_count"] = offset
                row["query_source_ranges"] = {name: ([{"source_id": x["id"], "start": sum(source_rows[y["id"]][0]["rows_per_stratum"] for y in partitions[name][:i]),
                                                               "count": source_rows[x["id"]][0]["rows_per_stratum"]}
                                                              for i, x in enumerate(partitions[name])])
                                                     for name in ("training", "validation", "evaluation")}
                new_strata.append(row)
            runs = []
            for branch_id in dict.fromkeys(source["physical_branch_id"] for source in sources):
                branch_sources = [source for source in sources if source["physical_branch_id"] == branch_id]
                # Sampling quotas depend on this owner's shared consumers and
                # GQA strata. Run identity/timing is common across owners, but
                # branch row geometry must be checked within the current owner.
                observations = [source_rows[source["id"]][0] for source in branch_sources]
                branch_geometry = {key: observations[0][key] for key in
                                   ("branch_logical_start", "branch_logical_count", "branch_rows_per_stratum")}
                physical_keys = ["completed", "cancelled", "elapsed_ns", "physical_bytes", "query_bytes",
                                 "status_readback_bytes", "kv_readback_bytes"]
                if private_replay_attention is not None:
                    physical_keys += ["replay_split_scratch_bytes", "serial_attention_dispatches",
                                      "split_attention_dispatches"]
                    require(all(type(observations[0][key]) is int and observations[0][key] >= 0
                                for key in physical_keys[-3:]),
                            "schema2 descriptor lacks replay attention receipt")
                physical = {key: observations[0][key] for key in physical_keys}
                require(all(all(row[key] == value for key, value in physical.items()) for row in observations),
                        "physical query branch observations disagree")
                require(all(all(row[key] == value for key, value in branch_geometry.items()) for row in observations),
                        "physical query branch geometry disagrees")
                ordered = sorted(observations, key=lambda row: row["logical_start"])
                cursor = branch_geometry["branch_logical_start"]
                for row in ordered:
                    require(row["logical_start"] == cursor, "physical query branch has a gap or overlap")
                    cursor += row["logical_count"]
                require(cursor == branch_geometry["branch_logical_start"] + branch_geometry["branch_logical_count"] and
                        sum(source_rows[row["source_id"]][0]["rows_per_stratum"] for row in observations) ==
                        branch_geometry["branch_rows_per_stratum"],
                        "physical query branch source coverage mismatch")
                runs.append({"physical_branch_id": branch_id,
                             "source_ids": [source["id"] for source in branch_sources], **physical})
            if private_replay_attention is not None:
                require(sum(run["serial_attention_dispatches"] for run in runs) ==
                        private_replay_attention["serial_dispatches"] and
                        sum(run["split_attention_dispatches"] for run in runs) ==
                        private_replay_attention["split_dispatches"] and
                        {run["replay_split_scratch_bytes"] for run in runs} ==
                        {private_replay_attention["split_scratch_bytes"]},
                        "assembled replay attention receipt does not match physical branches")
            serving = [run for run in runs if all(next(source for source in sources if source["id"] == source_id)["partition"] != "evaluation"
                                                   for source_id in run["source_ids"])]
            evaluation = [x for x in runs if x not in serving]
            campaign = {"accounting": "serial_physical_branch_upper_bound_v2", "serial_branches": True,
                        "private_kernel_route": private_kernel_route,
                        "private_replay_attention": private_replay_attention,
                        "private_question_observation": private_question_observation,
                        "live_context_fingerprint": live_context_fingerprint,
                        "private_study_observation": private_study_observation,
                        "study_singleton_paged_decode_requested": study_singleton_paged_decode_requested,
                        "branch_runs": runs,
                        "serving_elapsed_ns": sum(x["elapsed_ns"] for x in serving),
                        "evaluation_elapsed_ns": sum(x["elapsed_ns"] for x in evaluation),
                        "serving_query_bytes": sum(x["query_bytes"] for x in serving),
                        "projected_serving_peak_bytes": max(x["physical_bytes"] - x["query_bytes"] for x in serving) + sum(x["query_bytes"] for x in serving),
                        "observed_peak_bytes": max(x["physical_bytes"] for x in runs),
                        "evaluation_peak_bytes": max((x["physical_bytes"] for x in evaluation), default=0)}
            capture = {**c, "training_rows": partition_totals["training"], "validation_rows": partition_totals["validation"],
                       "evaluation_rows": partition_totals["evaluation"], "query_origin": "multi_branch_v2",
                       "query_sampling": "source_uniform_partition_v3", "query_sources": assembled_sources,
                       "query_campaign": campaign, "strata": new_strata}
            capture["runtime_block_coverage"] = coverage_binding or {
                "status": "diagnostic_unavailable", "missing_gate": "runtime_block_coverage"}
            capture["runtime_query_pool"] = calibration.runtime_query_pool_observation(capture)
            capture.pop("replay_start", None)
            capture.pop("replay_tokens", None)
            capture["query_protocol_sha256"] = calibration.query_protocol_identity(capture)
            spec = {**base, "schema_version": 2,
                    "purpose": "diagnostic" if diagnostic_without_coverage else base["purpose"], "capture": capture,
                    "tensors": {"keys": str((_plan_path(base_path, base["tensors"]["keys"]))),
                                "values": str((_plan_path(base_path, base["tensors"]["values"]))),
                                "queries": str(layer_dir / "queries.raw"), "positions": str(layer_dir / "positions.raw")}}
            spec_path = layer_dir / "spec.json"
            calibration.write_json(spec_path, spec)
            destination = temporary / f"layer{layer}-block{c['block_start']}"
            calibration.seal(spec_path, destination)
            manifests.append(str(Path(destination.name) / "manifest.json"))
        expected_layers = set(plan.get("global_owners", owner_layers))
        if not diagnostic_without_coverage:
            require(expected_layers and owner_blocks == {
                        (document, layer, block)
                        for document, (_, coverage, _) in coverage_by_document.items()
                        for layer in expected_layers for block in coverage["block_starts"]},
                    "incomplete runtime block coverage")
        require(owner_layers == expected_layers and all({x.get("layer") for x in source["owners"]} == expected_layers
                                                        for source in sources), "incomplete global owner assembly")
        calibration.write_json(temporary / "receipt.json", {"schema_version": 2, "stage": "offline_query_source_assembly",
                               "serving_qualified": False, "diagnostic_only": diagnostic_without_coverage,
                               "missing_gates": (["runtime_block_coverage"] if diagnostic_without_coverage else []),
                               "plan_sha256": calibration.digest(plan_path), "captures": manifests})
        temporary.rename(output_dir)
        return calibration.read_json(output_dir / "receipt.json")
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def is_projector(path):
    # Same filename classification as models/manifest.zig.
    stem = path.stem
    return (stem == "mmproj" or stem.startswith(("mmproj-", "mmproj_")) or
            stem.endswith(("-mmproj", "_mmproj")))


def validate_replay_sequences(prefix, source_tokens, replay_tokens):
    require, np = calibration.require, calibration.np
    require(0 < replay_tokens <= 8192, "invalid captured replay length")
    prompt = calibration.raw_array(str(prefix) + "-prompt.i64", "<i8", (source_tokens,))
    replay = calibration.raw_array(str(prefix) + "-replay.i64", "<i8", (replay_tokens,))
    positions = calibration.raw_array(str(prefix) + "-replay-source-positions.u32", "<u4", (replay_tokens,))
    instruction = positions == 0xffffffff
    count = int(instruction.sum())
    require(0 < count < replay_tokens and np.all(instruction[:count]) and not np.any(instruction[count:]),
            "invalid replay instruction provenance")
    selected = positions[count:]
    require(prompt[0] == 2 and np.all(prompt >= 0) and np.all(replay >= 0), "invalid replay token ids")
    require(np.all(selected > 0) and np.all(selected < source_tokens) and
            np.all(selected[1:] > selected[:-1]), "invalid replay payload positions")
    require(np.array_equal(replay[count:], prompt[selected]), "replay payload token identity mismatch")
    if replay_tokens == count + source_tokens - 1:
        require(np.array_equal(selected, np.arange(1, source_tokens)), "incomplete whole-payload replay")


def capture(args):
    require = calibration.require
    build = release_build.validate(args.build_receipt, args.binary)
    require(build["binary_path"] == args.binary.resolve(strict=True),
            "binary does not match full release build receipt")
    source = {
        "git_head": build["head"],
        "files": build["source"]["files"],
        "binary_sha256": build["binary_sha256"],
        "build_receipt_sha256": build["receipt_sha256"],
    }
    require(not any(name.startswith("TERMITE_METAL_") for name in os.environ), "clear Metal tuning overrides before capture")
    require(1025 <= args.source_tokens <= 16384, "source token count outside capture bounds")
    with args.document.open("rb") as stream:
        document_bytes = stream.read(1024 * 1024 + 1)
    require(len(document_bytes) <= 1024 * 1024, "document exceeds capture bounds")
    document_hash = hashlib.sha256(document_bytes).hexdigest()
    calibration.identity(args.model_sha256, "model")
    require(calibration.digest(args.artifact) == args.model_sha256, "artifact identity mismatch")
    # The test model loader takes a directory. Bind the selected core GGUF,
    # rather than accepting a receipt for another file in an ambiguous folder.
    cores = {path.resolve() for path in args.artifact.parent.glob("*.gguf") if not is_projector(path)}
    require(cores == {args.artifact.resolve()}, "capture requires one unambiguous core GGUF")
    require(not any((args.artifact.parent / name).exists() for name in
                    ("tokenizer.json", "tokenizer.model", "tokenizer_config.json", "chat_template.jinja")),
            "external tokenizer/template requires a separate identity contract")
    require(not args.output_dir.exists(), "capture output already exists")
    args.output_dir.mkdir(parents=True)
    snapshot = args.output_dir / "document.utf8"
    with snapshot.open("xb") as stream:
        stream.write(document_bytes)
    prefix = args.output_dir / "raw"
    command = ["/usr/bin/env"]
    for name in os.environ:
        if name.startswith("ANTFLY_CONTEXT_"):
            command.extend(["-u", name])
    command.extend([
        "MTL_DEBUG_LAYER=1", "ANTFLY_CONTEXT_REPLAY_CAPTURE=1",
        "ANTFLY_CONTEXT_MODEL_DIR=" + str(args.artifact.parent),
        "ANTFLY_CONTEXT_PROBE_TEXT_FILE=" + str(snapshot),
        "ANTFLY_CONTEXT_PROBE_DUMP_PREFIX=" + str(prefix),
        "ANTFLY_CONTEXT_REPLAY_SOURCE_TOKENS=" + str(args.source_tokens),
        str(args.binary), "--test-filter", "context Metal installed Gemma post ingestion replay capture",
    ])
    result = run_guarded(command, args.output_dir / "resources.json", timeout=args.timeout,
                         max_rss_mib=args.max_rss_mib, min_free_percent=10,
                         max_swap_growth_mib=0, min_disk_free_mib=4096)
    require(result["pass"], f"capture failed: {result['violation'] or result['returncode']}; see resource logs")
    log = (args.output_dir / "resources.stderr").read_text()
    require("source_unchanged=true published=false" in log and "1 selected; 1 passed; 0 skipped." in log,
            "capture test did not prove an unchanged original source")
    require(calibration.digest(snapshot) == document_hash, "document snapshot changed during capture")
    require(calibration.digest(args.artifact) == args.model_sha256, "artifact changed during capture")
    descriptors = sorted(args.output_dir.glob("raw-layer*-capture.json"))
    captures = [calibration.read_json(path) for path in descriptors]
    require({item["owner_layer"] for item in captures} == set(args.global_owner) and len(captures) == len(args.global_owner),
            "incomplete/duplicate global owner capture")
    require(captures, "capture has no global owners")
    validate_replay_sequences(prefix, args.source_tokens, captures[0]["replay_tokens"])
    shapes = [{"layer": item["owner_layer"], "heads": item["heads"], "dim": item["dim"]} for item in captures]
    sequences = {suffix: {"path": str(prefix) + "-" + suffix, "sha256": calibration.digest(str(prefix) + "-" + suffix)}
                 for suffix in ("prompt.i64", "replay.i64", "replay-source-positions.u32")}
    require(Path(sequences["prompt.i64"]["path"]).stat().st_size == args.source_tokens * 8, "prompt length mismatch")
    manifests = []
    for path, item in zip(descriptors, captures):
        stem = str(path)[:-len("capture.json")]
        require(item["replay_start"] == args.source_tokens, "replay start mismatch")
        require(item["replay_tokens"] * 8 == Path(sequences["replay.i64"]["path"]).stat().st_size, "replay length mismatch")
        spec = {
            "schema_version": 1, "purpose": args.purpose,
            "model": {"id": args.model_id, "sha256": args.model_sha256,
                      "tokenizer_sha256": args.model_sha256,
                      "template_sha256": hashlib.sha256(b"raw encode; BOS id 2; no chat template; private replay v1").hexdigest(),
                      "global_owner_shapes": shapes},
            "document": {"id": args.document_id, "sha256": document_hash, "bucket": args.bucket},
            "source": source, "capture": item,
            "tensors": {name: stem + suffix for name, suffix in (("keys", "keys.f16"), ("values", "values.f16"),
                        ("queries", "queries.f32"), ("positions", "positions.u32"))},
            "token_sequences": sequences,
            "provenance_notes": ["Private offline session; no serving sequence, sampler, grammar or usage object.",
                                 "Tokenizer identity is the containing GGUF. Raw text and BOS 2; no chat template.",
                                 "Does not qualify serving branch sharing, task accuracy, or committed device-memory reduction."],
        }
        spec_path = args.output_dir / f"layer{item['owner_layer']}-spec.json"
        calibration.write_json(spec_path, spec)
        manifests.append(str(calibration.seal(spec_path, args.output_dir / f"layer{item['owner_layer']}")))
    receipt = {"schema_version": 1, "stage": "offline_replay_capture", "serving_qualified": False,
               "build_receipt_sha256": build["receipt_sha256"], "binary_sha256": build["binary_sha256"],
               "driver_sha256": calibration.digest(__file__), "calibrator_sha256": calibration.script_identity(),
               "resource_receipt_sha256": calibration.digest(args.output_dir / "resources.json"), "captures": manifests}
    calibration.write_json(args.output_dir / "receipt.json", receipt)
    return receipt


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "prepare-campaign-plan":
        planner = argparse.ArgumentParser(description="bind a genuine campaign capture into a schema2 assembly plan")
        planner.add_argument("--campaign-dir", type=Path, required=True)
        planner.add_argument("--summary", required=True)
        planner.add_argument("--summary-sha256", required=True)
        planner.add_argument("--capture-inputs", required=True)
        planner.add_argument("--capture-inputs-sha256", required=True)
        planner.add_argument("--source-build-receipt", required=True)
        planner.add_argument("--source-build-receipt-sha256", required=True)
        planner.add_argument("--model-id", required=True)
        planner.add_argument("--expected-global-owner-shape", action="append", required=True,
                            metavar="LAYER:HEADS:DIM")
        planner.add_argument("--output", type=Path, required=True)
        args = planner.parse_args(sys.argv[2:])
        shapes = []
        for raw in args.expected_global_owner_shape:
            try:
                layer, heads, dim = (int(value, 10) for value in raw.split(":"))
            except (TypeError, ValueError):
                planner.error("--expected-global-owner-shape must be LAYER:HEADS:DIM")
            shapes.append({"layer": layer, "heads": heads, "dim": dim})
        print(json.dumps(prepare_campaign_plan(
            args.campaign_dir,
            {"path": args.summary, "sha256": args.summary_sha256},
            {"path": args.capture_inputs, "sha256": args.capture_inputs_sha256},
            {"path": args.source_build_receipt, "sha256": args.source_build_receipt_sha256},
            args.model_id, shapes, args.output), indent=2))
        return
    if len(sys.argv) > 1 and sys.argv[1] == "assemble-v2":
        assembler = argparse.ArgumentParser(description="assemble and seal schema2 query-source captures")
        assembler.add_argument("--plan", type=Path, required=True)
        assembler.add_argument("--output-dir", type=Path, required=True)
        args = assembler.parse_args(sys.argv[2:])
        print(json.dumps(assemble_v2(args.plan, args.output_dir), indent=2))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-receipt", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--model-sha256", required=True)
    parser.add_argument("--global-owner", type=int, action="append", required=True)
    parser.add_argument("--document", type=Path, required=True)
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--bucket", choices=sorted(calibration.BUCKETS), required=True)
    parser.add_argument("--purpose", choices=("calibration", "evaluation"), required=True)
    parser.add_argument("--source-tokens", type=int, default=2817)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--max-rss-mib", type=float, default=10240)
    args = parser.parse_args()
    for name in ("binary", "build_receipt", "artifact", "document", "output_dir"):
        setattr(args, name, getattr(args, name).resolve())
    print(json.dumps(capture(args), indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as exc:
        print(f"attention capture: {exc}", file=sys.stderr)
        sys.exit(2)
