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

"""Fail-closed raw paired quality contracts for Gemma context compaction.

The persistent driver emits one exact and one attention-matching observation
for every predeclared case while retaining one loaded model generation. This
module opens and hashes the sealed inputs and reconstructs accuracy and NLL
from raw outputs. It never grants serving authority by itself.
"""
import hashlib
import json
import math
from pathlib import Path
import re
import struct
import unicodedata

import release_attention_matching as release
import quality_ordered_messages as ordered_messages

VARIANTS = ("exact", "attention_matching")
# Quality requires structured prompt and decoded-text observations from the
# actual route. CLI and mixed-batch still have separate serving release gates.
QUALITY_ROUTES = frozenset(("native", "http", "stream"))
SCORER = "utf8_nfc_trim_exact_v1"
MAX_TEXT_BYTES = 1024 * 1024
MAX_REFERENCE_TOKENS = 4096


def digest(value, label):
    release.require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid " + label)
    return value


def _path(root, value, label):
    release.require(isinstance(value, str) and value, "missing " + label)
    relative = Path(value)
    release.require(not relative.is_absolute() and ".." not in relative.parts and "." not in relative.parts,
                    "absolute or traversing " + label)
    base = Path(root).resolve()
    path = base
    for part in relative.parts:
        path = path / part
        release.require(not path.is_symlink(), "symlinked " + label)
    release.require(path.is_file(), "missing " + label)
    return path


def _bytes(path, limit, label):
    release.require(path.stat().st_size <= limit, label + " exceeds byte limit")
    return path.read_bytes()


def reference_tokens(path):
    raw = _bytes(path, MAX_TEXT_BYTES, "reference tokens")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid reference token file") from exc
    # Post-ingestion compaction reuses the exact final-prefill logits for the
    # first token. A single-token reference never scores a compacted-context
    # continuation and therefore cannot qualify perplexity preservation.
    release.require(type(value) is list and 2 <= len(value) <= MAX_REFERENCE_TOKENS and
                    all(type(token) is int and 0 <= token <= 2**31 - 1 for token in value),
                    "invalid reference token ids")
    return value


def normalized(text):
    return unicodedata.normalize("NFC", text).strip()


def plan_contract(plan, root):
    require = release.require
    require(type(plan) is dict and set(plan) == {"schema_version", "qualified_context_tokens", "cases"} and
            plan["schema_version"] in (3, 4), "invalid raw quality plan")
    lengths, cases = plan["qualified_context_tokens"], plan["cases"]
    require(type(lengths) is list and lengths == sorted(set(lengths)) and lengths and
            all(type(value) is int and 1025 <= value <= 2**32 - 1 for value in lengths),
            "invalid quality context lengths")
    require(type(cases) is list and cases and len(cases) <= 32768, "empty or oversized quality plan")
    ids, documents, prompts, cells = set(), set(), set(), {}
    common_fields = {"id", "bucket", "request_content_sha256", "prompt_file", "prompt_sha256",
                    "prompt_tokens", "reserved_output_tokens",
                    "reference_file", "reference_sha256",
                    "reference_tokens_file", "reference_tokens_sha256", "scorer", "route",
                    "settings_sha256", "variant_settings_sha256"}
    exact_fields = (common_fields | {"document_file", "document_sha256", "question_file", "question_sha256"}
                    if plan["schema_version"] == 3 else
                    common_fields | {"messages_file", "messages_sha256", "source_sha256",
                                     "selected_part_sha256s"})
    for case in cases:
        require(type(case) is dict and set(case) == exact_fields, "invalid quality case fields")
        require(isinstance(case["id"], str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", case["id"]) and
                case["id"] not in ids, "invalid or duplicate quality case id")
        ids.add(case["id"])
        require(case["bucket"] in release.BUCKETS and case["scorer"] == SCORER and case["route"] in QUALITY_ROUTES,
                "invalid quality bucket/scorer/route")
        prompt, reserved = case["prompt_tokens"], case["reserved_output_tokens"]
        require(type(prompt) is int and type(reserved) is int and prompt > 0 and 0 < reserved <= 4096 and
                prompt + reserved in lengths, "quality case outside context cells")
        input_digest = case["document_sha256"] if plan["schema_version"] == 3 else case["messages_sha256"]
        digest(input_digest, "quality input")
        if plan["schema_version"] == 3:
            require(case["document_sha256"] not in documents,
                    "quality selected sources are not independent")
            documents.add(case["document_sha256"])
        if plan["schema_version"] == 4:
            digest(case["source_sha256"], "quality selected source")
        digest(case["request_content_sha256"], "quality structured request")
        digest(case["prompt_sha256"], "quality prompt")
        require(case["prompt_sha256"] not in prompts, "quality prompts are not independent")
        prompts.add(case["prompt_sha256"])
        input_files = (("document_file", "document_sha256", MAX_TEXT_BYTES),
                       ("question_file", "question_sha256", MAX_TEXT_BYTES)) if plan["schema_version"] == 3 else (
                       ("messages_file", "messages_sha256", ordered_messages.MAX_TOTAL_TEXT_BYTES * 2),)
        for file_field, hash_field, limit in input_files + (("prompt_file", "prompt_sha256", MAX_TEXT_BYTES),
                                               ("reference_file", "reference_sha256", MAX_TEXT_BYTES),
                                               ("reference_tokens_file", "reference_tokens_sha256", MAX_TEXT_BYTES)):
            path = _path(root, case[file_field], file_field)
            digest(case[hash_field], hash_field)
            require(hashlib.sha256(_bytes(path, limit, file_field)).hexdigest() == case[hash_field],
                    file_field + " digest mismatch")
        for field in (("document_file", "question_file") if plan["schema_version"] == 3 else ()) + (
                "prompt_file", "reference_file"):
            try:
                text = _path(root, case[field], field).read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(field + " is not UTF-8") from exc
            require(bool(text), "empty " + field)
        if plan["schema_version"] == 3:
            document_bytes = _bytes(_path(root, case["document_file"], "document_file"),
                                    MAX_TEXT_BYTES, "document_file")
            question_bytes = _bytes(_path(root, case["question_file"], "question_file"),
                                    MAX_TEXT_BYTES, "question_file")
            actual_request = release._coverage_structured_request_identity(document_bytes, question_bytes)
            selected_parts = [case["document_sha256"]]
        else:
            messages_bytes = _bytes(_path(root, case["messages_file"], "messages_file"),
                                    ordered_messages.MAX_TOTAL_TEXT_BYTES * 2, "messages_file")
            messages = ordered_messages.loads(messages_bytes)
            actual_request = ordered_messages.semantic_digest(messages)
            require(ordered_messages.selected_source_digest(messages) == case["source_sha256"],
                    "quality selected source identity mismatch")
            selected_parts = ordered_messages.selected_part_digests(messages)
            require(case["selected_part_sha256s"] == selected_parts,
                    "quality selected part identities mismatch")
        require(selected_parts and len(set(selected_parts)) == len(selected_parts) and
                all(re.fullmatch(r"[0-9a-f]{64}", value) for value in selected_parts),
                "invalid quality selected part identities")
        if plan["schema_version"] == 4:
            require(not documents.intersection(selected_parts), "quality selected sources are not independent")
            documents.update(selected_parts)
        require(actual_request == case["request_content_sha256"],
                "quality structured request identity mismatch")
        tokens = reference_tokens(_path(root, case["reference_tokens_file"], "reference_tokens_file"))
        require(len(tokens) <= reserved, "reference continuation exceeds reserved output")
        digest(case["settings_sha256"], "quality settings")
        variant_settings = case["variant_settings_sha256"]
        require(type(variant_settings) is dict and set(variant_settings) == set(VARIANTS),
                "invalid quality variant settings")
        for value in variant_settings.values():
            digest(value, "quality variant settings")
        cells[(prompt + reserved, case["bucket"])] = cells.get((prompt + reserved, case["bucket"]), 0) + 1
    expected = {(length, bucket) for length in lengths for bucket in release.BUCKETS}
    require(set(cells) == expected and len(set(cells.values())) == 1 and
            min(cells.values()) >= release.MIN_QUALITY_CASES,
            "quality plan cells are missing, imbalanced, or underpowered")
    return plan


def requests(plan):
    """Produce the exact persistent-driver command order from a validated plan."""
    release.require(type(plan) is dict and type(plan.get("cases")) is list, "invalid raw quality plan")
    requests = []
    for case in plan["cases"]:
        for variant in VARIANTS:
            request = {"command": "quality", "case_id": case["id"], "variant": variant,
                             "bucket": case["bucket"], "route": case["route"],
                             "request_content_sha256": case["request_content_sha256"],
                             "prompt_file": case["prompt_file"],
                             "prompt_sha256": case["prompt_sha256"],
                             "prompt_tokens": case["prompt_tokens"],
                             "reserved_output_tokens": case["reserved_output_tokens"],
                             "reference_file": case["reference_file"],
                             "reference_sha256": case["reference_sha256"],
                             "reference_tokens_file": case["reference_tokens_file"],
                             "reference_tokens_sha256": case["reference_tokens_sha256"],
                             "scorer": case["scorer"], "settings_sha256": case["settings_sha256"],
                             "variant_settings_sha256": case["variant_settings_sha256"][variant]}
            if "messages_file" in case:
                request.update(messages_file=case["messages_file"], messages_sha256=case["messages_sha256"],
                               source_sha256=case["source_sha256"],
                               selected_part_sha256s=case["selected_part_sha256s"])
            else:
                request.update(document_file=case["document_file"], document_sha256=case["document_sha256"],
                               question_file=case["question_file"], question_sha256=case["question_sha256"])
            requests.append(request)
    return requests


def flatten_plan(plan, root):
    """Validate sealed inputs before producing persistent-driver commands."""
    plan_contract(plan, root)
    return requests(plan)


def token_digest(tokens):
    return hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()


def report_contract(report, case, variant, identity, origin, root):
    require = release.require
    require(type(report) is dict and report.get("schema_version") == 2 and
            report.get("semantic_identity") == identity and report.get("provenance") == origin and
            report.get("completed") is True, "incomplete or mismatched raw quality report")
    generation = release.loaded_serving_identity(report, identity)
    require(report.get("case_id") == case["id"] and report.get("variant") == variant and
            report.get("bucket") == case["bucket"] and report.get("route") == case["route"],
            "raw quality report request mismatch")
    request_fields = (("messages_sha256",) if "messages_sha256" in case else
                      ("document_sha256", "question_sha256"))
    for field in request_fields + ("request_content_sha256", "prompt_sha256",
                  "reference_sha256", "reference_tokens_sha256", "settings_sha256"):
        require(report.get(field) == case[field], "raw quality report digest mismatch: " + field)
    require((report.get("document_sha256") is None and report.get("question_sha256") is None)
            if "messages_sha256" in case else report.get("messages_sha256") is None,
            "raw quality report mixed ordered and legacy bindings")
    expected_source = case.get("source_sha256", case.get("document_sha256"))
    if "messages_sha256" in case or report.get("source_sha256") is not None:
        require(report.get("source_sha256") == expected_source, "raw quality selected source mismatch")
    expected_parts = case.get("selected_part_sha256s")
    if expected_parts is not None or report.get("selected_part_sha256s") is not None:
        require(report.get("selected_part_sha256s") == (expected_parts or [case["document_sha256"]]),
                "raw quality selected part identities mismatch")
    require(report.get("variant_settings_sha256") == case["variant_settings_sha256"][variant] and
            report.get("prompt_tokens") == case["prompt_tokens"] and
            report.get("reserved_output_tokens") == case["reserved_output_tokens"] and
            report.get("scorer") == SCORER, "raw quality report settings mismatch")
    require(report.get("exact_fallback") is False and type(report.get("published")) is bool and
            report["published"] == (variant == "attention_matching"),
            "raw quality report did not execute requested variant")
    for field in ("elapsed_ns", "committed_before_bytes", "committed_after_bytes", "context_peak_bytes",
                  "live_bytes_before", "live_bytes_after_release", "backend_kv_readback_bytes"):
        require(type(report.get(field)) is int and not isinstance(report[field], bool) and report[field] >= 0,
                "invalid raw quality counter: " + field)
    require(report["elapsed_ns"] > 0 and report["committed_before_bytes"] > 0 and
            report["committed_after_bytes"] > 0 and
            report["context_peak_bytes"] >= max(report["committed_before_bytes"], report["committed_after_bytes"]) and
            report["backend_kv_readback_bytes"] == 0 and
            report["live_bytes_before"] == report["live_bytes_after_release"],
            "raw quality allocation invariant failed")
    require((variant == "exact" and report["committed_after_bytes"] == report["committed_before_bytes"]) or
            (variant == "attention_matching" and report["committed_after_bytes"] < report["committed_before_bytes"]),
            "raw quality publication allocation mismatch")
    release.context_allocation_scope_contract(report, "raw quality")
    require(report["committed_context_bytes"] == report["committed_after_bytes"],
            "raw quality committed context scope mismatch")
    tokens = report.get("output_token_ids")
    require(type(tokens) is list and 0 < len(tokens) <= case["reserved_output_tokens"] and
            all(type(token) is int and 0 <= token <= 2**31 - 1 for token in tokens),
            "invalid raw quality output tokens")
    digest(report.get("output_sha256"), "raw quality output")
    require(token_digest(tokens) == report["output_sha256"], "raw quality output token digest mismatch")
    output = report.get("output_utf8")
    require(isinstance(output, str) and len(output.encode("utf-8")) <= MAX_TEXT_BYTES,
            "invalid raw quality decoded output")
    digest(report.get("output_utf8_sha256"), "raw quality decoded output")
    require(hashlib.sha256(output.encode("utf-8")).hexdigest() == report["output_utf8_sha256"],
            "raw quality decoded output digest mismatch")
    expected_tokens = reference_tokens(_path(root, case["reference_tokens_file"], "reference_tokens_file"))
    require(report.get("reference_sequence_sha256") == token_digest(expected_tokens),
            "raw quality reference sequence digest mismatch")
    continuation_steps = len(expected_tokens) - 1
    for field, expected_count in (
            ("continuation_forward_steps", continuation_steps),
            ("published_compaction_forward_steps", continuation_steps if variant == "attention_matching" else 0)):
        require(type(report.get(field)) is int and report[field] == expected_count,
                "raw quality teacher continuation step mismatch: " + field)
    teacher = report.get("teacher_forced")
    require(type(teacher) is list and len(teacher) == len(expected_tokens),
            "raw quality teacher continuation length mismatch")
    nll = 0.0
    for index, (expected, row) in enumerate(zip(expected_tokens, teacher)):
        require(type(row) is dict and set(row) == {"sequence_position", "token_id", "conditioned_on_token_id",
                                                 "source", "log_probability"} and
                type(row["token_id"]) is int and row["token_id"] == expected and
                type(row["log_probability"]) in (int, float) and
                not isinstance(row["log_probability"], bool) and math.isfinite(row["log_probability"]) and
                -1e6 <= row["log_probability"] <= 0, "invalid raw quality teacher token probability")
        previous = row["conditioned_on_token_id"]
        expected_source = ("prefill_final_row" if index == 0 else
                           "published_compacted_context" if variant == "attention_matching" else
                           "exact_context_continuation")
        require(type(row["sequence_position"]) is int and row["sequence_position"] == index and
                ((index == 0 and previous is None) or
                 (index > 0 and type(previous) is int and previous == expected_tokens[index - 1])) and
                row["source"] == expected_source, "invalid raw quality teacher conditional chain")
        nll -= row["log_probability"]
    reference = _path(root, case["reference_file"], "reference_file").read_text(encoding="utf-8")
    return {"generation": generation, "correct": normalized(output) == normalized(reference), "nll": nll,
            "scored_tokens": len(expected_tokens)}


def driver_report_contract(report, identity, origin, request, root):
    """Validate one child reply for the persistent parent collector.

    The driver must decode ``output_token_ids`` with the tokenizer in its bound
    ``loaded_identity`` to produce ``output_utf8``. It must never echo or use the
    sealed reference text as generated output.
    """
    release.require(type(request) is dict and request.get("command") == "quality" and
                    request.get("variant") in VARIANTS, "invalid quality driver request")
    variant = request["variant"]
    case = {key: value for key, value in request.items() if key not in {"command", "variant"}}
    case["id"] = case.pop("case_id")
    case["variant_settings_sha256"] = {variant: case["variant_settings_sha256"]}
    return report_contract(report, case, variant, identity, origin, root)


def reconstruct(plan, reports, identity, origin, root):
    """Recompute paired correctness and token-level NLL from raw observations."""
    plan_contract(plan, root)
    release.require(type(reports) is list and len(reports) == len(plan["cases"]) * len(VARIANTS),
                    "raw quality request/report count mismatch")
    by_key = {}
    for report in reports:
        key = (report.get("case_id"), report.get("variant"))
        release.require(key not in by_key, "duplicate raw quality report")
        by_key[key] = report
    cases, generations = [], set()
    for case in plan["cases"]:
        observations = {}
        for variant in VARIANTS:
            key = (case["id"], variant)
            release.require(key in by_key, "missing raw quality variant")
            observations[variant] = report_contract(by_key[key], case, variant, identity, origin, root)
            generations.add(observations[variant]["generation"])
        release.require(observations["exact"]["generation"] == observations["attention_matching"]["generation"],
                        "quality pair changed loaded generation")
        candidate = by_key[(case["id"], "attention_matching")]
        case_input = ({"messages_sha256": case["messages_sha256"], "source_sha256": case["source_sha256"],
                       "selected_part_sha256s": case["selected_part_sha256s"]}
                      if "messages_sha256" in case else
                      {"document_sha256": case["document_sha256"],
                       "question_sha256": case["question_sha256"]})
        cases.append({"schema_version": 2, "semantic_identity": identity, "provenance": origin,
                      "loaded_identity": candidate["loaded_identity"], "published": True,
                      "exact_fallback": False, "route": case["route"],
                      "committed_before_bytes": candidate["committed_before_bytes"],
                      "committed_after_bytes": candidate["committed_after_bytes"],
                      "live_bytes_before": candidate["live_bytes_before"],
                      "live_bytes_after_release": candidate["live_bytes_after_release"],
                      "backend_kv_readback_bytes": candidate["backend_kv_readback_bytes"],
                      "id": case["id"], "bucket": case["bucket"], **case_input,
                      "request_content_sha256": case["request_content_sha256"],
                      "prompt_sha256": case["prompt_sha256"], "prompt_tokens": case["prompt_tokens"],
                      "reserved_output_tokens": case["reserved_output_tokens"],
                      "exact_correct": observations["exact"]["correct"],
                      "candidate_correct": observations["attention_matching"]["correct"],
                      "scored_tokens": observations["exact"]["scored_tokens"],
                      "exact_nll_sum": observations["exact"]["nll"],
                      "candidate_nll_sum": observations["attention_matching"]["nll"]})
    release.require(len(generations) == 1, "quality campaign changed loaded generation")
    return {"schema_version": 2, "semantic_identity": identity, "provenance": origin,
            "diagnostic_only": True, "serving_qualified": False, "cases": cases}
