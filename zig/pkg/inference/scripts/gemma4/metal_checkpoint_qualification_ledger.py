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

"""Strict parser for test-only Metal checkpoint owner-ledger evidence."""
from __future__ import annotations

import json
from pathlib import Path
import time

LEDGER_MARKER = "metal_checkpoint_qualification "
PHASE_SCHEMA = "antfly.metal-checkpoint-phase.v1"
RELEASE_SCHEMA = "antfly.metal-checkpoint-release.v1"
# The private test ingress pins Gemma-4 E4B. Zig derives and attests these from
# the loaded model layout; the collector pins the qualified cell independently.
E4B_ATTENTION_CALLS_PER_MATERIALIZED_TOKEN = 42
E4B_DONOR_APPENDS_PER_MATERIALIZED_TOKEN = 24
KV_BOUNDARY_FIELDS = (
    "kv_materialized_tokens", "pending_sampled_tokens", "kv_boundary_proven",
    "binding_attention_calls_per_materialized_token",
    "binding_donor_appends_per_materialized_token",
)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def ledger_rows(path, offset):
    rows = []
    with Path(path).open("rb") as stream:
        stream.seek(offset)
        for raw in stream:
            text = raw.decode("utf-8", errors="strict")
            marker = text.find(LEDGER_MARKER)
            if marker < 0:
                continue
            value = json.loads(text[marker + len(LEDGER_MARKER):])
            require(type(value) is dict, "invalid Metal owner ledger row")
            rows.append(value)
    return rows


def wait_ledger(path, offset, phases=("generation_committed", "final_release"),
                timeout=5):
    deadline = time.monotonic() + timeout
    rows = []
    while time.monotonic() < deadline:
        rows = ledger_rows(path, offset)
        if all(any(row.get("phase") == phase for row in rows) for phase in phases):
            return rows
        time.sleep(.01)
    return rows


def _binding_boundary_contract(ready, current):
    """Prove a sampled final token that has not undergone a subsequent forward."""
    integer_fields = (
        "kv_materialized_tokens", "pending_sampled_tokens",
        "binding_attention_calls_per_materialized_token",
        "binding_donor_appends_per_materialized_token",
    )
    require(
        all(type(row.get(field)) is int and row[field] >= 0
            for row in (ready, current) for field in integer_fields) and
        all(type(row.get(field)) is int and row[field] >= 0
            for row in (ready, current)
            for field in ("logical_tokens", "binding_logical_tokens",
                          "attention_calls", "donor_appends")) and
        ready.get("kv_boundary_proven") is True and
        current.get("kv_boundary_proven") is True,
        "checkpoint restore lacks typed KV-boundary proof",
    )
    require(
        ready["pending_sampled_tokens"] == 0 and
        ready["kv_materialized_tokens"] == ready.get("logical_tokens") and
        ready.get("binding_logical_tokens") == ready["kv_materialized_tokens"] and
        current["pending_sampled_tokens"] in (0, 1) and
        current.get("logical_tokens") ==
            current["kv_materialized_tokens"] + current["pending_sampled_tokens"] and
        current.get("binding_logical_tokens") == current["kv_materialized_tokens"],
        "checkpoint restore KV boundary does not reconcile",
    )
    materialized_delta = (
        current["kv_materialized_tokens"] - ready["kv_materialized_tokens"]
    )
    attention_delta = current.get("attention_calls", -1) - ready.get(
        "attention_calls", -1
    )
    append_delta = current.get("donor_appends", -1) - ready.get(
        "donor_appends", -1
    )
    attention_rate = current["binding_attention_calls_per_materialized_token"]
    append_rate = current["binding_donor_appends_per_materialized_token"]
    require(
        ready["binding_attention_calls_per_materialized_token"] ==
            attention_rate == E4B_ATTENTION_CALLS_PER_MATERIALIZED_TOKEN and
        ready["binding_donor_appends_per_materialized_token"] ==
            append_rate == E4B_DONOR_APPENDS_PER_MATERIALIZED_TOKEN,
        "checkpoint restore KV rates differ from qualified E4B geometry",
    )
    require(
        materialized_delta > 0 and
        attention_delta == materialized_delta * attention_rate and
        append_delta == materialized_delta * append_rate,
        "checkpoint restore KV activity does not prove its materialized boundary",
    )


def generation_contract(rows, expected_route, require_binding=True,
                        expected_representation=None):
    committed = [row for row in rows if row.get("schema") == PHASE_SCHEMA and
                 row.get("phase") == "generation_committed"]
    released = [row for row in rows if row.get("schema") == RELEASE_SCHEMA and
                row.get("phase") == "final_release"]
    require(len(committed) == len(released) == 1,
            "request lacks unique committed/release owner ledger")
    current, final = committed[0], released[0]
    identity = (current.get("request"), current.get("route"), current.get("owner_id"))
    require(identity == (final.get("request"), final.get("route"),
                         final.get("owner_id")), "owner ledger attribution changed")
    require(current.get("route") == expected_route,
            "owner ledger route differs from requested workload")
    require(all(current.get(field) is True for field in
                ("owner_available", "physical_available", "combined_available")) and
            current.get("invalid") is False and
            type(current.get("logical_tokens")) is int and
            all(type(current.get(field)) is int and current[field] >= 0 for field in
                ("validation_ns", "upload_ns", "install_ns", "suffix_prefill_ns",
                 "attention_calls", "donor_appends")) and
            type(current.get("tracked_combined_live_bytes")) is int and
            current["tracked_combined_live_bytes"] > 0 and
            type(current.get("tracked_combined_peak_bytes")) is int and
            current["tracked_combined_peak_bytes"] >= current["tracked_combined_live_bytes"] and
            final.get("released") is True and final.get("scope_complete") is True and
            final.get("full_owner_physical_available") is True and
            final.get("measured_available") is True and
            type(final.get("initial_live_bytes")) is int and
            final["initial_live_bytes"] >= 0 and
            type(final.get("measured_committed_bytes")) is int and
            final["measured_committed_bytes"] > 0 and
            final.get("post_release_live_bytes") == 0,
            "incomplete physical owner ledger")
    require(current["tracked_combined_live_bytes"] >= final["initial_live_bytes"] and
            current["tracked_combined_live_bytes"] - final["initial_live_bytes"] ==
            final["measured_committed_bytes"],
            "phase and release committed-byte measurements do not reconcile")
    if require_binding:
        ready = [row for row in rows if row.get("schema") == PHASE_SCHEMA and
                 row.get("phase") == "resumed_ready"]
        require(len(ready) == 1 and
                (ready[0].get("request"), ready[0].get("route"),
                 ready[0].get("owner_id")) == identity,
                "checkpoint restore lacks unique resumed-ready ledger")
        require(current.get("binding") is True,
                "checkpoint restore binding is incomplete")
        proof_fields_present = any(
            field in row for row in (ready[0], current) for field in KV_BOUNDARY_FIELDS
        )
        if proof_fields_present:
            _binding_boundary_contract(ready[0], current)
        else:
            require(current.get("binding_logical_tokens") == current["logical_tokens"],
                    "checkpoint restore binding is incomplete")
        require(expected_representation in ("f16", "fp8") and
                current.get("binding_representation") == expected_representation,
                "checkpoint resident representation differs")
        for field in ("binding_fp8_heads", "binding_fp8_rows",
                      "binding_exact_resident_bytes", "binding_encoded_resident_bytes",
                      "binding_metadata_resident_bytes", "binding_allocated_bytes"):
            require(type(current.get(field)) is int and current[field] >= 0,
                    "invalid checkpoint resident accounting: " + field)
        require(current["binding_exact_resident_bytes"] +
                current["binding_encoded_resident_bytes"] +
                current["binding_metadata_resident_bytes"] ==
                current["binding_allocated_bytes"],
                "checkpoint resident byte accounting does not reconcile")
        require(current["binding_allocated_bytes"] > 0 and
                current["attention_calls"] > 0,
                "checkpoint restore lacks resident execution proof")
        if expected_representation == "fp8":
            require(current["binding_fp8_heads"] > 0 and
                    current["binding_fp8_rows"] > 0 and
                    current["binding_encoded_resident_bytes"] > 0,
                    "FP8 restore lacks resident encoded storage proof")
        else:
            require(current["binding_exact_resident_bytes"] > 0 and
                    current["binding_fp8_heads"] == 0 and
                    current["binding_fp8_rows"] == 0 and
                    current["binding_encoded_resident_bytes"] == 0,
                    "f16 restore unexpectedly claims encoded storage")
    else:
        require(current.get("binding") is False and
                current.get("binding_representation") == "none" and
                all(current.get(field) == 0 for field in (
                    "binding_logical_tokens", "binding_allocated_bytes",
                    "binding_fp8_heads", "binding_fp8_rows",
                    "binding_exact_resident_bytes",
                    "binding_encoded_resident_bytes",
                    "binding_metadata_resident_bytes",
                )), "exact Metal request unexpectedly installed checkpoint state")
    return {"committed": current, "release": final,
            "committed_context_bytes": final["measured_committed_bytes"],
            "tracked_combined_live_bytes": current["tracked_combined_live_bytes"],
            "release_measured_committed_bytes": final["measured_committed_bytes"],
            "initial_baseline_bytes": final["initial_live_bytes"],
            "measurement_reconciliation_delta_bytes":
                current["tracked_combined_live_bytes"] - final["initial_live_bytes"] -
                final["measured_committed_bytes"]}
