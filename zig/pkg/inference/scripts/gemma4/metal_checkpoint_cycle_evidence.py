#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate native whole-head owner-cycle evidence without granting release authority."""
import math
import re


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, label, minimum=0):
    require(type(value) is int and value >= minimum, "invalid " + label)
    return value


def number(value, label, minimum=0):
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum,
            "invalid " + label)
    return value


def digest(value, label):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) and
            value != "0" * 64, "invalid " + label)
    return value


def byte_digest(value, label):
    require(type(value) is list and len(value) == 32 and
            all(type(x) is int and 0 <= x <= 255 for x in value) and any(value),
            "invalid " + label)
    return bytes(value).hex()


def validate_report(value, frozen_input, *, input_sha256, model_directory, loaded_identity):
    """All numerical bounds are the frozen runtime contract, never caller overrides."""
    require(type(value) is dict and value.get("schema") ==
            "antfly.metal-checkpoint-cycles.report.v3", "unsupported cycle report")
    request = frozen_input["request"]
    count = integer(request["cycles"], "requested cycles", 1)
    require(count <= 20, "too many cycles")
    append = integer(request["append_tokens"], "append tokens", 1)
    answers = integer(request["answer_tokens"], "answer tokens", 1)
    require(append <= 2048 and answers <= 32, "invalid continuation request")
    initial = integer(frozen_input["expected_prompt_tokens"], "initial tokens", 2)
    require(initial + count * (append + answers) <= 32768 - 512,
            "logical capacity exceeds query-tail reserve")
    encoding = request["encoding"]
    require(encoding in ("f16", "fp8", "rotated4"), "invalid encoding")
    require(value.get("model") == frozen_input["model"] and
            value.get("encoding") == encoding and
            value.get("model_directory") == str(model_directory) and
            value.get("input_sha256") == digest(input_sha256, "input hash"),
            "cycle input/model binding mismatch")
    identity = value.get("model_identity")
    require(type(identity) is dict and set(identity) == {"weights", "tokenizer", "template", "layout"},
            "invalid loaded identity fields")
    require({key: byte_digest(raw, key) for key, raw in identity.items()} == loaded_identity,
            "loaded model identity mismatch")
    integer(value.get("load_generation"), "load generation", 1)
    for key in ("rendered_prompt_sha256", "prompt_tokens_sha256", "implementation_sha256"):
        digest(value.get(key), key)
    report = value.get("report")
    require(type(report) is dict and report.get("passed") is True and
            report.get("diagnostic_only") is True and report.get("production_ready") is False,
            "invalid cycle evidence authority")
    reference = request.get("reference", [])
    require(type(reference) is list and all(type(token) is int and token >= 0 for token in reference) and
            (not reference or len(reference) == count * answers),
            "invalid frozen reference")
    require(report.get("frozen_reference") is bool(reference), "reference provenance mismatch")
    require(type(report.get("completed")) is int and report["completed"] == count and
            report.get("initial_tokens") == initial and
            report.get("final_tokens") == initial + (count - 1) * append + count * answers,
            "incomplete cycle trajectory")
    rows = report.get("rows")
    require(type(rows) is list and len(rows) == 20, "invalid bounded row array")
    active = rows[:count]
    owner = integer(active[0].get("owner_id"), "installed owner", 1)
    previous_revision = 0
    previous_peak = 0
    exact_nll = compacted_nll = 0.0
    greedy = comparisons = 0
    checkpoint_hashes = []
    for index, row in enumerate(active):
        require(type(row) is dict and type(row.get("cycle")) is int and row["cycle"] == index + 1 and
                row.get("owner_id") == owner and type(row.get("owner_id")) is int and
                row.get("logical_tokens") == initial + index * append + (index + 1) * answers,
                "invalid persistent owner trajectory")
        source_revision = integer(row.get("source_revision"), "source revision", 1)
        require(source_revision > previous_revision and
                type(row.get("installed_revision")) is int and
                row["installed_revision"] == source_revision + 1, "invalid install revision")
        previous_revision = row["installed_revision"]
        require(row.get("query_scope_released") is True and
                type(row.get("query_scope_delta_bytes")) is int and row["query_scope_delta_bytes"] == 0 and
                row.get("query_uses_source_owner") is True and
                type(row.get("query_owner_id")) is int and row["query_owner_id"] == owner,
                "query clone escaped parent allocation scope")
        peak = integer(row.get("query_owner_peak_bytes"), "query peak", 1)
        require(previous_peak <= peak <= 8 * 1024**3, "invalid cumulative query peak")
        previous_peak = peak
        for key in ("completed_chunks", "validated_heads", "attention_calls", "donor_appends", "fp64_accelerated_value_solves"):
            integer(row.get(key), key, 1)
        require(integer(row.get("admitted_peak_bytes"), "admitted bytes", 1) <= 8 * 1024**3,
                "aggregate admission exceeded")
        require(number(row.get("max_head_mass_error"), "head mass error") <= .05 and
                number(row.get("max_head_output_rmse"), "head output error") <= .10,
                "head numerical acceptance exceeded")
        if index:
            integer(row.get("ragged_repeat_calls"), "installed query attention", 1)
        source_bytes = integer(row.get("source_global_bytes"), "source global bytes", 1)
        candidate_bytes = integer(row.get("candidate_global_bytes"), "candidate global bytes", 1)
        threshold = request["initial_minimum_savings_ratio"] if index == 0 else request["incremental_minimum_savings_ratio"]
        require(number(threshold, "savings threshold", 1) >= (1.5 if index == 0 else 1) and
                candidate_bytes < source_bytes and source_bytes / candidate_bytes + 1e-12 >= threshold,
                "cycle did not meet frozen global savings")
        fp8 = integer(row.get("fp8_heads"), "FP8 heads")
        rot4 = integer(row.get("rotated4_heads"), "rotated4 heads")
        require((encoding == "f16" and fp8 == rot4 == 0) or
                (encoding == "fp8" and fp8 > 0 and rot4 == 0) or
                (encoding == "rotated4" and rot4 > 0 and fp8 == 0),
                "requested resident codec was not exercised")
        byte_digest(row.get("immutable_rows_sha256"), "historical rows hash")
        checkpoint_hashes.append(byte_digest(row.get("checkpoint_sha256"), "checkpoint hash"))
        compared = integer(row.get("comparisons"), "comparisons", 1)
        matched = integer(row.get("greedy_matches"), "greedy matches")
        require(compared == answers and matched <= compared, "invalid continuation comparison count")
        exact_nll += number(row.get("exact_nll"), "exact NLL")
        compacted_nll += number(row.get("compacted_nll"), "compacted NLL")
        number(row.get("max_logit_relative_l2"), "logit drift")
        comparisons += compared
        greedy += matched
    require(len(set(checkpoint_hashes)) == count, "checkpoint trajectory replayed")
    delta = (compacted_nll - exact_nll) / comparisons
    require(delta <= math.log(1.05), "continuation NLL acceptance exceeded")
    for label in ("exact_release", "compacted_release"):
        receipt = report.get(label)
        require(type(receipt) is dict and receipt.get("released") is True and
                type(receipt.get("post_release_live_bytes")) is int and receipt["post_release_live_bytes"] == 0,
                "cycle owner did not release")
    return {"completed_cycles": count, "owner_id": owner, "comparisons": comparisons,
            "greedy_matches": greedy, "nll_delta_per_token": delta,
            "frozen_reference": bool(reference), "checkpoint_sha256": checkpoint_hashes,
            "diagnostic_only": True, "production_ready": False}
