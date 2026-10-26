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

"""Contracts for raw, matched Gemma context performance observations.

The persistent driver uses the same loaded generation for ordinary exact,
packed exact, and attention matching. These contracts reconstruct the existing
release performance inputs; they do not grant serving authority.
"""
import hashlib
from pathlib import Path
import re
import struct

import release_attention_matching as release
import native_repeat_profile as native_profile


def digest(value, label):
    release.require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid " + label)
    return value


def plan_contract(plan, root=None, expected_identity=None):
    require = release.require
    require(type(plan) is dict and set(plan) == {"schema_version", "runtime", "cells", "requests"} and
            plan["schema_version"] == 4, "invalid paired performance plan")
    runtime = plan["runtime"]
    require(type(runtime) is dict and set(runtime) == {"policy", "kernel_flags", "implementation_sha256",
            "profile_file", "profile_sha256", "profile_semantic_sha256"},
            "invalid paired runtime authority")
    policy, flags = runtime["policy"], runtime["kernel_flags"]
    require(type(policy) is dict and set(policy) == {"max_context_tokens", "max_kv_tokens",
            "keep_prefix_tokens", "keep_recent_tokens"}, "invalid paired runtime policy")
    for field in policy:
        require(type(policy[field]) is int and 0 <= policy[field] < 2**32,
                "invalid paired runtime policy " + field)
    require(0 < policy["max_kv_tokens"] < policy["max_context_tokens"] <= 131072 and
            policy["keep_prefix_tokens"] + policy["keep_recent_tokens"] < policy["max_kv_tokens"],
            "unsupported paired runtime policy")
    require(type(flags) is dict and set(flags) == {"split_decode", "flash_prefill"} and
            all(type(value) is bool for value in flags.values()), "invalid paired kernel flags")
    for field in ("implementation_sha256", "profile_sha256", "profile_semantic_sha256"):
        digest(runtime[field], "paired runtime " + field)
        require(runtime[field] != "0" * 64, "zero paired runtime " + field)
    require(isinstance(runtime["profile_file"], str) and runtime["profile_file"],
            "missing paired frozen profile")
    if expected_identity is not None:
        require(runtime["implementation_sha256"] == expected_identity["implementation"] and
                runtime["profile_sha256"] == expected_identity["profile"],
                "paired runtime semantic identity mismatch")
    candidate = None
    if root is not None:
        profile_path = release.campaign_file(Path(root).resolve(),
            {"path": runtime["profile_file"], "sha256": runtime["profile_sha256"]},
            "paired frozen profile", release.MAX_REPORT)
        profile = release.read_json(profile_path, runtime["profile_sha256"])
        candidate = (native_profile.qualification_candidate_view(profile)
                     if profile.get("schema") == native_profile.PROFILE_SCHEMA else
                     profile.get("runtime_candidate"))
        require(type(candidate) is dict and candidate.get("profile_semantic_sha256") ==
                runtime["profile_semantic_sha256"] and
                candidate.get("identity", {}).get("implementation_sha256") == runtime["implementation_sha256"] and
                candidate.get("implementation", {}).get("split_decode") == flags["split_decode"] and
                candidate.get("implementation", {}).get("flash_prefill") == flags["flash_prefill"],
                "paired frozen profile runtime authority mismatch")
        require(type(candidate.get("max_prompt_tokens")) is int and candidate["max_prompt_tokens"] > 0 and
                type(candidate.get("max_output_tokens")) is int and candidate["max_output_tokens"] > 0 and
                policy["max_context_tokens"] <= candidate["max_prompt_tokens"] + candidate["max_output_tokens"],
                "paired policy exceeds frozen runtime envelope")
    cells, requests = plan["cells"], plan["requests"]
    require(type(cells) is list and cells and type(requests) is list and 0 < len(requests) <= 4096,
            "empty or oversized paired performance plan")
    indexed = {}
    for cell in cells:
        require(type(cell) is dict and set(cell) == {"context_tokens", "reserved_output_tokens",
                                                   "generation_settings_sha256", "variant_settings_sha256"},
                "invalid paired performance cell")
        context, reserved = cell["context_tokens"], cell["reserved_output_tokens"]
        require(type(context) is int and type(reserved) is int and 2 <= reserved < context <= 2**32 - 1,
                "invalid paired performance context")
        require(context <= policy["max_context_tokens"] and
                (candidate is None or context - reserved <= candidate.get("max_prompt_tokens", -1) and
                 reserved <= candidate.get("max_output_tokens", -1)),
                "paired performance cell exceeds frozen runtime envelope")
        digest(cell["generation_settings_sha256"], "paired generation settings")
        require(cell["generation_settings_sha256"] == release._coverage_generation_settings(reserved),
                "paired generation settings are not derived from request contract")
        variants = cell["variant_settings_sha256"]
        require(type(variants) is dict and set(variants) == set(release.PERFORMANCE_VARIANTS),
                "invalid paired variant settings")
        for variant, value in variants.items():
            digest(value, "paired variant settings")
            require(value == release._variant_settings_v2(variant, policy,
                    runtime["implementation_sha256"], runtime["profile_sha256"],
                    runtime["profile_semantic_sha256"], flags["split_decode"], flags["flash_prefill"]),
                    "paired variant settings are not derived from runtime authority")
        require((context, reserved) not in indexed, "duplicate paired performance cell")
        indexed[(context, reserved)] = cell
    pairs, prompts = {}, {}
    for request in requests:
        require(type(request) is dict and set(request) == {"pair_id", "warmup", "variant", "route",
                    "document_file", "document_sha256", "question_file", "question_sha256",
                    "request_content_sha256", "prompt_file", "prompt_sha256", "prompt_tokens", "reserved_output_tokens",
                    "settings_sha256", "variant_settings_sha256"}, "invalid paired request fields")
        pair_id = request["pair_id"]
        require(isinstance(pair_id, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", pair_id) and
                type(request["warmup"]) is bool and request["route"] in release.ROUTES and
                request["variant"] in release.PERFORMANCE_VARIANTS, "invalid paired request identity")
        prompt, reserved = request["prompt_tokens"], request["reserved_output_tokens"]
        require(type(prompt) is int and type(reserved) is int and prompt > 0 and
                (prompt + reserved, reserved) in indexed, "paired request outside context cells")
        cell = indexed[(prompt + reserved, reserved)]
        require(request["settings_sha256"] == cell["generation_settings_sha256"] and
                request["variant_settings_sha256"] == cell["variant_settings_sha256"][request["variant"]],
                "paired request settings mismatch")
        digest(request["prompt_sha256"], "paired prompt digest")
        for field in ("document", "question", "prompt"):
            require(isinstance(request[field + "_file"], str) and request[field + "_file"],
                    "missing paired " + field + " file")
            digest(request[field + "_sha256"], "paired " + field + " digest")
        digest(request["request_content_sha256"], "paired structured request")
        if root is not None:
            document = release.campaign_file(Path(root).resolve(),
                {"path": request["document_file"], "sha256": request["document_sha256"]},
                "paired document", release.MAX_REPORT).read_bytes()
            question = release.campaign_file(Path(root).resolve(),
                {"path": request["question_file"], "sha256": request["question_sha256"]},
                "paired question", release.MAX_REPORT).read_bytes()
            require(release._coverage_structured_request_identity(document, question) ==
                    request["request_content_sha256"], "paired structured request identity mismatch")
        contract = {key: value for key, value in request.items() if key not in {"variant", "variant_settings_sha256"}}
        pair = pairs.setdefault(pair_id, {"contract": contract, "variants": set()})
        require(pair["contract"] == contract and request["variant"] not in pair["variants"],
                "paired prompt changed or variant repeated")
        pair["variants"].add(request["variant"])
        require(prompts.setdefault(request["prompt_sha256"], pair_id) == pair_id,
                "paired document reused across prompts or warmups")
    require(all(pair["variants"] == set(release.PERFORMANCE_VARIANTS) for pair in pairs.values()),
            "incomplete planned performance pair")
    return plan


def requests(plan):
    """Return the sealed driver wire contract without the expected prompt artifact.

    The child must render document and question as distinct message parts. The
    parent keeps `prompt_file` only to validate the expected renderer output;
    it is never an alternate input that can authorize generated prompt tokens.
    """
    plan_contract(plan)
    result = []
    for request in plan["requests"]:
        wire = dict(request)
        del wire["prompt_file"]
        result.append(wire)
    return result


def report_contract(report, identity, origin, request=None):
    require = release.require
    require(type(report) is dict and report.get("schema_version") == 2 and
            report.get("semantic_identity") == identity and report.get("provenance") == origin and
            report.get("completed") is True, "incomplete or mismatched raw performance report")
    release.loaded_serving_identity(report, identity)
    variant = report.get("variant")
    require(variant in release.PERFORMANCE_VARIANTS and report.get("route") in release.ROUTES,
            "invalid raw performance route/variant")
    require(type(report.get("warmup")) is bool and isinstance(report.get("pair_id"), str),
            "invalid raw performance pair")
    for field in ("prompt_sha256", "settings_sha256", "variant_settings_sha256"):
        digest(report.get(field), "raw performance " + field)
    release.request_context_tokens(report)
    require(report["reserved_output_tokens"] >= 2, "raw performance needs decode continuation")
    if request is not None:
        for key, value in request.items():
            if key != "prompt_file":
                require(report.get(key) == value, "raw performance reply/request mismatch: " + key)
    for field in ("ttft_ns", "decode_ns", "elapsed_ns", "committed_context_bytes"):
        require(type(report.get(field)) is int and report[field] > 0, "invalid raw performance " + field)
    require(report["ttft_ns"] <= report["elapsed_ns"] and report["decode_ns"] <= report["elapsed_ns"],
            "raw performance phase exceeds generation duration")
    for field in ("backend_kv_readback_bytes", "live_bytes_before", "live_bytes_after_release"):
        require(type(report.get(field)) is int and report[field] >= 0, "invalid raw performance " + field)
    require(report["backend_kv_readback_bytes"] == 0 and
            report["live_bytes_before"] == report["live_bytes_after_release"],
            "raw performance read back KV or leaked allocations")
    release.context_allocation_scope_contract(report, "raw performance")
    require(type(report.get("published")) is bool and report["published"] == (variant == "attention_matching") and
            report.get("exact_fallback") is False, "raw performance did not execute requested variant")
    tokens = report.get("output_token_ids")
    require(type(tokens) is list and len(tokens) == report["reserved_output_tokens"] and len(tokens) <= 4096 and
            all(type(token) is int and 0 <= token <= 2**31 - 1 for token in tokens),
            "raw performance lacks full output tokens")
    require(hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest() == report.get("output_sha256"),
            "raw performance output digest mismatch")
    return report


def reconstruct(plan, reports, identity, origin, root=None):
    """Derive pair order, timings, savings and output identity from raw reports."""
    plan_contract(plan, root, identity if root is not None else None)
    release.require(type(reports) is list and len(reports) == len(plan["requests"]),
                    "raw performance request/report count mismatch")
    pairs = {}
    for index, (request, report) in enumerate(zip(plan["requests"], reports)):
        report_contract(report, identity, origin, request)
        pair = pairs.setdefault(report["pair_id"], {
            "pair_id": report["pair_id"], "prompt_sha256": report["prompt_sha256"],
            "settings_sha256": report["settings_sha256"], "prompt_tokens": report["prompt_tokens"],
            "reserved_output_tokens": report["reserved_output_tokens"],
            "context_tokens": release.request_context_tokens(report),
            "output_tokens": len(report["output_token_ids"]), "warmup": report["warmup"], "order": []})
        variant = report["variant"]
        pair["order"].append(variant)
        pair[variant] = {"completed": True, "attempt_id": str(index),
                         "settings_sha256": report["variant_settings_sha256"],
                         "ttft_ms": report["ttft_ns"] / 1e6,
                         "decode_tokens_per_second": (len(report["output_token_ids"]) - 1) * 1e9 / report["decode_ns"],
                         "committed_context_bytes": report["committed_context_bytes"],
                         "swapout_growth_bytes": 0, "output_sha256": report["output_sha256"],
                         "published": report["published"], "exact_fallback": report["exact_fallback"]}
    result = {"schema_version": 2, "semantic_identity": identity, "provenance": origin,
              "cells": plan["cells"], "warmups": [], "samples": []}
    for pair in pairs.values():
        warmup = pair.pop("warmup")
        result["warmups" if warmup else "samples"].append(pair)
    return result
