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

"""Offline, bounded attention-matching calibration; never enables serving.

Commands: seal a capture spec, sweep calibration/diagnostic captures, freeze a
profile from calibration results, evaluate that profile on unseen documents.
The independent NumPy/SciPy FP64 implementation is a diagnostic oracle, not a
bitwise reproduction of the Zig/Metal fitter or the authors' FP32 implementation.
See docs/design/inference/context-windowing.md for the manifest contract.
"""
from __future__ import annotations

import os
# Keep these diagnostics serial and bounded on model-development hosts.
for _name in ("OPENBLAS_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "OMP_NUM_THREADS"):
    os.environ[_name] = "1"

import argparse
import hashlib
import json
import platform
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import scipy
from scipy.linalg import lstsq, qr, svdvals
from scipy.optimize import linprog

SCHEMA = 1
CAPTURE_SCHEMAS = (1, 2)
AUTHOR_REVISION = "2020f6d37940fe013e7e7f1ebf409e5d107c5670"
DEFAULT_RECIPES = ("relative_cd_ridge_rms", "relative_cd_ridge_pursuit", "relative_cd_ridge_cpqr", "relative_qr",
                   "max_shift_qr", "max_shift_omp_lstsq")
RECIPES = DEFAULT_RECIPES + ("relative_minimax_ridge_peak",)
BUDGETS = (128, 256, 384, 448, 512)
MASS_LIMIT, OUTPUT_LIMIT, RCOND = .05, .10, 1e-10
BUCKETS = {"document_qa", "exact_retrieval", "code", "structured_tool", "multiturn"}
MAX_TENSOR_BYTES = 512 * 1024 * 1024
MAX_CAPTURE_BYTES = 256 * 1024 * 1024
MAX_QUERY_ROWS_V2 = 16384
RUNTIME_MAX_TRAINING_ROWS = 3072
RUNTIME_MAX_VALIDATION_ROWS = 1024
RUNTIME_PROFILE_RECIPE = "relative_cd_ridge_rms"
RUNTIME_PROFILE_IMPLEMENTATION = {
    "algorithm_version": 1,
    "packed_abi_version": 2,
    "kv_dtype": "f16",
    "kernel_route": "metal_context_optimized",
    "block_tokens": 512,
    "solver": "bounded_coordinate_descent",
}
DEFAULT_MAX_FITS = 4096
DEFAULT_MAX_SECONDS = 30 * 60
DEFAULT_MAX_WORKING_SET_BYTES = 4 * 1024 * 1024 * 1024
MINIMAX_TAU = .90
MINIMAX_PEAK_FRACTION = .50
MINIMAX_BATCH = 8
MINIMAX_MAX_ROWS = MAX_QUERY_ROWS_V2
MINIMAX_MAX_COLUMNS = 512
MINIMAX_MAX_GROUPS = 4096
MINIMAX_MAX_LP_SOLVES = 128
MINIMAX_WEIGHT_MIN = 1e-6
MINIMAX_WEIGHT_MAX = 1e6
MINIMAX_PRIMAL_TOLERANCE = 1e-9

# Recipe names are part of frozen profiles. Keep their numerical choices
# explicit so an "OMP" result cannot be confused with the batched coordinate
# pursuit used by the current Zig fitter.
RECIPE_IDENTITIES = {
    "relative_cd_ridge_rms": {"selection": "rms_probability", "mass_solver": "positive_coordinate_descent_128",
                               "value_solver": "ridge_normal_equations", "precision": "numpy_float64"},
    "relative_cd_ridge_pursuit": {"selection": "batched_matching_pursuit_8", "mass_solver": "positive_coordinate_descent_128",
                                   "value_solver": "ridge_normal_equations", "precision": "numpy_float64"},
    "relative_cd_ridge_cpqr": {"selection": "training_probability_column_pivoted_qr_low_index_ties",
                                "mass_solver": "positive_coordinate_descent_128",
                                "value_solver": "ridge_normal_equations", "precision": "numpy_float64_oracle"},
    "relative_qr": {"selection": "rms_probability", "mass_solver": "bounded_lstsq_projected_2",
                    "value_solver": "scipy_gelsy_unregularized", "precision": "numpy_float64"},
    "max_shift_qr": {"selection": "rms_probability", "mass_solver": "bounded_lstsq_projected_2",
                     "value_solver": "scipy_gelsy_unregularized", "precision": "numpy_float64"},
    "max_shift_omp_lstsq": {"selection": "upstream_simple_omp_single_key_raw_correlation",
                            "mass_solver": "scipy_gelsy_then_clamp_min_1e-12",
                            "value_solver": "scipy_gelsy_unregularized", "precision": "numpy_float64_oracle",
                            "upstream_revision": AUTHOR_REVISION},
    "relative_minimax_ridge_peak": {
        "selection": "training_stratum_equal_peak_coverage_tau_0.90_fraction_0.50_batch_8",
        "mass_solver": "scipy_highs_ds_bounded_chebyshev_1e-9",
        "value_solver": "ridge_normal_equations",
        "precision": "numpy_float64_offline_diagnostic",
        "solver_library": "scipy.optimize.linprog",
        "solver_method": "highs-ds",
        "validation_visible_to_selection": False,
    },
}


class NumericalFailure(ValueError):
    """A rejected candidate, as distinct from invalid capture provenance."""


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def identity(value, label):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), f"invalid {label} SHA256")


def read_json(path):
    require(Path(path).stat().st_size <= 16 * 1024 * 1024, "manifest exceeds limit")
    return json.loads(Path(path).read_text())


def write_json(path, data):
    # Create once; never truncate a previous receipt/profile after a failed run.
    encoded = json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n"
    with Path(path).open("x") as stream:
        stream.write(encoded)


def script_identity():
    return digest(__file__)


def model_key(meta):
    return ":".join(meta["model"][key] for key in ("sha256", "tokenizer_sha256", "template_sha256"))


def shape_contract(meta):
    capture_schema = meta.get("schema_version")
    require(capture_schema in CAPTURE_SCHEMAS, "unsupported capture version")
    require(meta.get("purpose") in {"diagnostic", "calibration", "evaluation"}, "invalid capture purpose")
    for key in ("sha256", "tokenizer_sha256", "template_sha256"):
        identity(meta["model"][key], key)
    require(bool(meta["model"]["id"]), "missing model id")
    identity(meta["document"]["sha256"], "document")
    require(bool(meta["document"]["id"]) and meta["document"]["bucket"] in BUCKETS, "invalid document identity/bucket")
    source = meta["source"]
    require(re.fullmatch(r"[0-9a-f]{40}", source["git_head"]), "invalid source head")
    require(bool(source["files"]), "missing source file hashes")
    for value in source["files"].values():
        identity(value, "source file")
    c = meta["capture"]
    for key in ("rows", "heads", "dim", "training_rows", "validation_rows", "block_start", "owner_layer"):
        require(type(c[key]) is int, f"{key} must be an integer")
    n, h, d, t, v = (c[k] for k in ("rows", "heads", "dim", "training_rows", "validation_rows"))
    e = c.get("evaluation_rows", 0)
    require(type(e) is int and e >= 0, "evaluation_rows must be a nonnegative integer")
    require(2 <= n <= 512 and n % 2 == 0 and 1 <= h <= 64 and 1 <= d <= 512, "invalid KV dimensions")
    query_limit = 4096 if capture_schema == 1 else MAX_QUERY_ROWS_V2
    require(t > 0 and v > 0 and t + v + e <= query_limit and c["block_start"] >= 0 and 0 <= c["owner_layer"] < 256, "invalid query dimensions")
    require(np.isfinite(c["scale"]) and c["scale"] > 0, "invalid attention scale")
    require(c["query_origin"] in {"request_prefill", "repeat_prefill", "generated", "multi_branch_v2"}, "invalid query origin")
    if c["query_origin"] == "repeat_prefill":
        require(type(c["replay_tokens"]) is int and 0 < c["replay_tokens"] <= 8192 and
                type(c["replay_start"]) is int and c["replay_start"] >= c["block_start"] + n and
                c["replay_start"] + c["replay_tokens"] <= 0xffffffff, "invalid replay position budget")
    owners = meta["model"]["global_owner_shapes"]
    require(owners and len({x["layer"] for x in owners}) == len(owners), "missing/duplicate global owner shapes")
    for owner in owners:
        require(all(type(owner[k]) is int for k in ("layer", "heads", "dim")) and 0 <= owner["layer"] < 256 and
                0 < owner["heads"] <= 64 and 0 < owner["dim"] <= 512, "invalid global owner shape")
    require({"layer": c["owner_layer"], "heads": h, "dim": d} in owners, "capture owner not in model layout")
    if capture_schema == 1 and meta["purpose"] != "diagnostic":
        require(c["query_origin"] == "repeat_prefill", "qualification requires isolated replay captures")
        require(c.get("query_sampling") == "uniform_partition_v1", "qualification requires whole-replay sampling")
    if capture_schema == 2:
        require(c["query_origin"] == "multi_branch_v2" and c.get("query_sampling") == "source_uniform_partition_v3",
                "v2 qualification requires branch-uniform query capture")
        validate_query_source_contract(meta)
        validate_query_campaign(c)
    q = t + v + e
    shapes = {"keys": ("<f2", (n, h, d)), "values": ("<f2", (n, h, d)),
              "queries": ("<f4", (h, q, d)), "positions": ("<u4", (h, q))}
    total = sum(int(np.prod(shape)) * np.dtype(dtype).itemsize for dtype, shape in shapes.values())
    total += h * q * (d + 1) * 8  # exact output/partition references
    require(total <= MAX_CAPTURE_BYTES, "capture exceeds memory envelope")
    expected_consumers = c["consumers"]
    require(expected_consumers and len({x["layer"] for x in expected_consumers}) == len(expected_consumers), "duplicate or absent consumers")
    expected = set()
    for consumer in expected_consumers:
        layer, query_heads = consumer["layer"], consumer["query_heads"]
        require(type(layer) is int and 0 <= layer < 256 and type(query_heads) is int and 0 < query_heads <= 256 and query_heads % h == 0, "invalid consumer")
        expected.update((layer, q) for q in range(query_heads))
    require(c["owner_layer"] in {x["layer"] for x in expected_consumers}, "owner absent from consumers")
    seen, coverage = set(), np.zeros((h, q), dtype=np.uint32)
    for s in c["strata"]:
        layer, q, head = s["consumer_layer"], s["query_head"], s["kv_head"]
        require((layer, q) in expected and (layer, q) not in seen, "missing/duplicate query consumer")
        seen.add((layer, q))
        count = next(x["query_heads"] for x in expected_consumers if x["layer"] == layer)
        require(head == q // (count // h), "incorrect GQA head mapping")
        split_bounds = [("training", t, 0), ("validation", v, t)]
        if capture_schema == 2 and e:
            split_bounds.append(("evaluation", e, t + v))
        for split, bound, base in split_bounds:
            start, size = s[split + "_start"], s[split + "_count"]
            require(type(start) is int and type(size) is int and start >= 0 and size > 0 and start + size <= bound, "invalid stratum range")
            coverage[head, base + start:base + start + size] += 1
    require(seen == expected and np.all(coverage == 1), "queries must cover every consumer exactly once")
    return shapes


def validate_query_source_contract(meta):
    """Validate source separation before any tensor bytes are trusted.

    Fit sources are visible before publication. Gate sources are private fixed
    studies. Exact-teacher continuation is offline evaluation evidence and may
    never affect selection, fitting, adaptive retry, or the runtime gate.
    """
    c = meta["capture"]
    sources = c.get("query_sources")
    require(isinstance(sources, list) and sources, "missing v2 query sources")
    ids, loaded_identities = set(), set()
    by_partition = {name: [] for name in ("training", "validation", "evaluation")}
    for source in sources:
        require(isinstance(source, dict), "invalid query source")
        source_id = source.get("id")
        require(isinstance(source_id, str) and re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", source_id) and source_id not in ids,
                "invalid/duplicate query source id")
        ids.add(source_id)
        require(isinstance(source.get("physical_branch_id"), str) and
                re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", source["physical_branch_id"]),
                "invalid physical query branch identity")
        partition, kind = source.get("partition"), source.get("kind")
        require(partition in by_partition, "invalid query source partition")
        study_kinds = {"fixed_study_prefill", "fixed_study_generated_response"}
        allowed = {"training": {"request_question_prefill", "request_question_replay", *study_kinds},
                   "validation": study_kinds,
                   "evaluation": {"exact_teacher_continuation"}}
        require(kind in allowed[partition], "query source kind/partition mismatch")
        expected_route = "native_prefill" if kind == "request_question_prefill" else "private_model_replay"
        require(source.get("capture_route") == expected_route, "query source capture route mismatch")
        expected_conditioning = ("original_prompt_prefix" if kind == "request_question_prefill" else
                                 "completed_prompt_study_generated_response" if kind == "fixed_study_generated_response" else
                                 "completed_prompt_private_replay")
        require(source.get("conditioning") == expected_conditioning, "query source conditioning mismatch")
        expected_sampling_partition = "training" if partition == "training" else "validation"
        require(source.get("sampling_partition") == expected_sampling_partition,
                "query source sampling partition mismatch")
        require(type(source.get("logical_start")) is int and source["logical_start"] >= c["block_start"] + c["rows"] and
                type(source.get("logical_count")) is int and source["logical_count"] > 0 and
                source["logical_start"] + source["logical_count"] <= 0xffffffff,
                "invalid query source positions")
        require(type(source.get("branch_logical_start")) is int and source["branch_logical_start"] >= 0 and
                type(source.get("branch_logical_count")) is int and source["branch_logical_count"] > 0 and
                type(source.get("branch_rows_per_stratum")) is int and source["branch_rows_per_stratum"] > 0 and
                source["branch_logical_start"] <= source["logical_start"] and
                source["logical_start"] + source["logical_count"] <=
                source["branch_logical_start"] + source["branch_logical_count"],
                "invalid physical query branch bounds")
        identity(source.get("tokens_sha256"), "query source tokens")
        identity(source.get("template_sha256"), "query source template")
        loaded = source.get("loaded_identity")
        require(isinstance(loaded, dict) and type(loaded.get("load_generation")) is int and
                loaded["load_generation"] > 0, "invalid query source loaded generation")
        for field in ("artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256",
                      "implementation_sha256"):
            identity(loaded.get(field), "query source loaded " + field)
        loaded_identities.add(tuple(loaded[field] for field in
                              ("artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256",
                               "implementation_sha256")))
        require(source.get("selection_visible") is (partition == "training"), "query source selection visibility mismatch")
        require(source.get("private") is (kind != "request_question_prefill"), "query source privacy mismatch")
        require(source.get("usage_visible") is False, "query source must not affect usage")
        if kind in study_kinds:
            require(source.get("predeclared") is True, "study query source must be predeclared")
            require(isinstance(source.get("study_pair_id"), str) and
                    re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", source["study_pair_id"]),
                    "invalid study pair identity")
            if kind == "fixed_study_generated_response":
                identity(source.get("generation_contract_sha256"), "study generation contract")
                require(source.get("generation_mode") == "greedy_fixed_length_ignore_eos",
                        "unsupported study generation mode")
        by_partition[partition].append(source)
    require(len(loaded_identities) == 1, "query sources disagree on loaded identity")
    loaded = sources[0]["loaded_identity"]
    require(loaded["artifact_sha256"] == meta["model"]["sha256"] and
            loaded["tokenizer_sha256"] == meta["model"]["tokenizer_sha256"] and
            loaded["template_sha256"] == meta["model"]["template_sha256"] and
            loaded["layout_sha256"] == meta["model"].get("layout_sha256") and
            loaded["implementation_sha256"] == meta["model"].get("implementation_sha256"),
            "loaded query identity does not match capture model")
    require(any(x["kind"] in {"request_question_prefill", "request_question_replay"} for x in by_partition["training"]) and
            {x["kind"] for x in by_partition["training"]} >= study_kinds,
            "training requires request question, prefill study, and generated-response study sources")
    require({x["kind"] for x in by_partition["validation"]} == study_kinds,
            "validation requires prefill and generated-response study sources")
    for partition in ("training", "validation"):
        prefill_pairs = {x["study_pair_id"] for x in by_partition[partition] if x["kind"] == "fixed_study_prefill"}
        response_pairs = {x["study_pair_id"] for x in by_partition[partition] if x["kind"] == "fixed_study_generated_response"}
        require(prefill_pairs == response_pairs and prefill_pairs, "unpaired fixed study sources")
        if meta["purpose"] != "diagnostic":
            for pair in prefill_pairs:
                paired = {x["physical_branch_id"] for x in by_partition[partition]
                          if x.get("study_pair_id") == pair}
                require(len(paired) == 1, "study pair must share one physical branch")
    purpose = meta["purpose"]
    if purpose == "evaluation":
        require(c.get("evaluation_rows", 0) > 0 and by_partition["evaluation"],
                "evaluation capture requires exact teacher continuation")
    elif purpose == "calibration":
        require(c.get("evaluation_rows", 0) == 0 and not by_partition["evaluation"],
                "calibration cannot consume teacher continuation")
    require(not ({x["tokens_sha256"] for x in by_partition["training"]} &
                 {x["tokens_sha256"] for x in by_partition["validation"] + by_partition["evaluation"]}),
            "fit query tokens overlap gate/evaluation")
    identity(c.get("query_protocol_sha256"), "query protocol")
    require(c["query_protocol_sha256"] == query_protocol_identity(c), "query protocol digest mismatch")
    validate_runtime_query_pool(c, by_partition)
    source_ids = {name: {x["id"] for x in values} for name, values in by_partition.items()}
    for stratum in c["strata"]:
        ranges = stratum.get("query_source_ranges")
        require(isinstance(ranges, dict), "missing query source ranges")
        for partition, suffix in (("training", "training_count"), ("validation", "validation_count"),
                                  ("evaluation", "evaluation_count")):
            expected_count = stratum.get(suffix, 0)
            entries = ranges.get(partition, [])
            if expected_count == 0:
                require(not entries, "unexpected empty-partition query sources")
                continue
            require(isinstance(entries, list) and entries, "missing stratum query sources")
            cursor, seen_sources = 0, set()
            for entry in entries:
                require(isinstance(entry, dict) and entry.get("source_id") in source_ids[partition] and
                        entry["source_id"] not in seen_sources and type(entry.get("start")) is int and
                        type(entry.get("count")) is int and entry["start"] == cursor and entry["count"] > 0,
                        "invalid stratum query source range")
                seen_sources.add(entry["source_id"])
                cursor += entry["count"]
            require(cursor == expected_count and seen_sources == source_ids[partition],
                    "incomplete stratum query source coverage")


def runtime_query_pool_description(capture):
    sources = capture.get("query_sources", [])
    return {"version": 2, "allocation": "equal_source_per_stratum_population_cap_v2",
            "max_training_rows": RUNTIME_MAX_TRAINING_ROWS,
            "max_validation_rows": RUNTIME_MAX_VALIDATION_ROWS,
            "training_source_order": [x.get("id") for x in sources if x.get("partition") == "training"],
            "validation_source_order": [x.get("id") for x in sources if x.get("partition") == "validation"]}


def runtime_query_pool_observation(capture):
    description = runtime_query_pool_description(capture)
    strata = capture.get("strata", [])
    per_head = {}
    for stratum in strata:
        per_head[stratum.get("kv_head")] = per_head.get(stratum.get("kv_head"), 0) + 1
    strata_per_head = max(per_head.values(), default=0)
    source_counts = {}
    for partition in ("training", "validation"):
        sources = description[partition + "_source_order"]
        limit = description["max_" + partition + "_rows"]
        source_rows = [x for x in capture.get("query_sources", []) if x.get("partition") == partition]
        populations = [replay_population(x["logical_start"], x["logical_count"], partition == "validation")
                       for x in source_rows]
        quota = min([limit // max(1, strata_per_head * len(sources)), *populations], default=0)
        for source_id in sources:
            counts = [next((entry["count"] for entry in stratum["query_source_ranges"][partition]
                            if entry["source_id"] == source_id), 0) for stratum in capture.get("strata", [])]
            source_counts[source_id] = {"partition": partition, "max_rows_per_stratum": quota,
                                        "observed_rows_per_stratum": counts[0] if counts and len(set(counts)) == 1 else None}
    supported = bool(strata_per_head and source_counts and all(
        row["observed_rows_per_stratum"] is not None and
        0 < row["observed_rows_per_stratum"] <= row["max_rows_per_stratum"]
        for row in source_counts.values()))
    return {"description": description, "strata_per_kv_head": strata_per_head,
            "source_counts": source_counts, "runtime_supported": supported}


def validate_runtime_query_pool(capture, by_partition=None):
    observed = capture.get("runtime_query_pool")
    require(isinstance(observed, dict) and observed == runtime_query_pool_observation(capture),
            "runtime query pool contract mismatch")
    if capture.get("runtime_block_coverage", {}).get("status") != "diagnostic_unavailable":
        require(observed["runtime_supported"] is True,
                "captured queries exceed runtime shared query pool")


def validate_query_campaign(capture):
    """Recompute serial replay cost from runtime-emitted per-source records.

    Separate private replay sessions may be freed after their selected queries
    are copied into the campaign pool.  The projected peak conservatively adds
    every serving source's complete query allocation to the largest non-query
    branch footprint.  Teacher evaluation is reported separately and is never
    charged to, or hidden inside, the serving projection.
    """
    campaign = capture.get("query_campaign")
    require(isinstance(campaign, dict) and campaign.get("accounting") == "serial_physical_branch_upper_bound_v2" and
            campaign.get("serial_branches") is True, "invalid query campaign accounting")
    runs = campaign.get("branch_runs")
    require(isinstance(runs, list) and runs, "missing query campaign branch runs")
    sources = {source["id"]: source for source in capture["query_sources"]}
    require(len(sources) == len(capture["query_sources"]), "duplicate query campaign source")
    seen, seen_branches = set(), set()
    for run in runs:
        source_ids = run.get("source_ids")
        require(isinstance(run, dict) and isinstance(source_ids, list) and source_ids and
                run.get("physical_branch_id") not in seen_branches and
                all(source_id in sources and source_id not in seen for source_id in source_ids) and
                {sources[x]["physical_branch_id"] for x in source_ids} == {run["physical_branch_id"]},
                "invalid query campaign branch run")
        seen.update(source_ids)
        seen_branches.add(run["physical_branch_id"])
        require(run.get("completed") is True and run.get("cancelled") is False,
                "incomplete query campaign source run")
        for key in ("elapsed_ns", "physical_bytes", "query_bytes", "status_readback_bytes", "kv_readback_bytes"):
            require(type(run.get(key)) is int and run[key] >= 0, f"invalid query campaign {key}")
        require(run["elapsed_ns"] > 0 and run["physical_bytes"] > 0 and run["query_bytes"] > 0 and
                run["physical_bytes"] >= run["query_bytes"] and run["status_readback_bytes"] > 0 and
                run["kv_readback_bytes"] == 0, "invalid query campaign resource observation")
    require(seen == set(sources), "incomplete query campaign source coverage")
    serving = [run for run in runs if all(sources[x]["partition"] != "evaluation" for x in run["source_ids"])]
    evaluation = [run for run in runs if all(sources[x]["partition"] == "evaluation" for x in run["source_ids"])]
    require(len(serving) + len(evaluation) == len(runs), "physical branch mixes serving and evaluation")
    require(serving, "query campaign lacks serving sources")
    serving_elapsed = sum(run["elapsed_ns"] for run in serving)
    evaluation_elapsed = sum(run["elapsed_ns"] for run in evaluation)
    serving_query = sum(run["query_bytes"] for run in serving)
    projected_peak = max(run["physical_bytes"] - run["query_bytes"] for run in serving) + serving_query
    observed_peak = max(run["physical_bytes"] for run in runs)
    evaluation_peak = max((run["physical_bytes"] for run in evaluation), default=0)
    expected = {"serving_elapsed_ns": serving_elapsed, "evaluation_elapsed_ns": evaluation_elapsed,
                "serving_query_bytes": serving_query, "projected_serving_peak_bytes": projected_peak,
                "observed_peak_bytes": observed_peak, "evaluation_peak_bytes": evaluation_peak}
    require(all(type(campaign.get(key)) is int and campaign[key] == value for key, value in expected.items()),
            "query campaign aggregate mismatch")


def query_protocol_description(capture):
    """Return the reusable online protocol, excluding request observations.

    Generated response tokens, request question lengths and sampled row counts
    are observations. The protocol binds the predeclared source order, compiled
    study templates, response-generation contract, sampling algorithm/limits,
    and runtime implementation that produce those observations.
    """
    fixed_sources = []
    for source in capture.get("query_sources", []):
        if source.get("partition") == "evaluation":
            continue
        row = {key: source.get(key) for key in
               ("id", "partition", "kind", "sampling_partition", "capture_route", "conditioning",
                "physical_branch_id")}
        loaded = source.get("loaded_identity") or {}
        row["loaded_identity"] = {key: loaded.get(key) for key in
                                  ("artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256",
                                   "implementation_sha256")}
        if source.get("kind") in {"fixed_study_prefill", "fixed_study_generated_response"}:
            row.update(template_sha256=source.get("template_sha256"), predeclared=source.get("predeclared"),
                       study_pair_id=source.get("study_pair_id"),
                       generation_contract_sha256=source.get("generation_contract_sha256"),
                       generation_mode=source.get("generation_mode"))
        fixed_sources.append(row)
    strata = []
    for source_stratum in capture.get("strata", []):
        row = {key: source_stratum.get(key) for key in ("consumer_layer", "query_head", "kv_head")}
        ranges = source_stratum.get("query_source_ranges", {})
        row["query_source_order"] = {name: [entry.get("source_id") for entry in ranges.get(name, [])]
                                      for name in ("training", "validation")}
        strata.append(row)
    return {"version": 1, "query_sampling": capture.get("query_sampling"),
            "query_campaign_accounting": (capture.get("query_campaign") or {}).get("accounting"),
            "runtime_query_pool": runtime_query_pool_description(capture),
            "sources": fixed_sources, "strata": strata}


def query_protocol_identity(capture):
    """Hash the reusable protocol description for profile/runtime binding."""
    contract = query_protocol_description(capture)
    encoded = json.dumps(contract, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def raw_array(path, dtype, shape):
    size = int(np.prod(shape)) * np.dtype(dtype).itemsize
    require(size <= MAX_TENSOR_BYTES and Path(path).stat().st_size == size, f"invalid tensor size: {path}")
    values = np.fromfile(path, dtype=dtype).reshape(shape)
    require(np.isfinite(values).all(), f"nonfinite tensor: {path}")
    return values


def validate_queries(meta, tensors):
    c, positions = meta["capture"], tensors["positions"]
    t = c["training_rows"]
    if meta["schema_version"] == 2:
        return validate_branch_queries(meta, positions)
    require(np.all(positions != 0xffffffff) and np.all(positions >= c["block_start"] + c["rows"]), "query is absent or precedes complete source block")
    if c["query_origin"] == "repeat_prefill":
        require(np.all(positions >= c["replay_start"]) and np.all(positions < c["replay_start"] + c["replay_tokens"]), "query outside isolated replay")
    require(not np.intersect1d(positions[:, :t], positions[:, t:]).size, "training and validation positions overlap")
    for s in c["strata"]:
        for split, base in (("training", 0), ("validation", t)):
            start, count = base + s[split + "_start"], s[split + "_count"]
            p = positions[s["kv_head"], start:start + count]
            require(len(np.unique(p)) == count, "duplicate query positions within consumer")
            if c.get("query_sampling") == "uniform_partition_v1":
                require(c["query_origin"] == "repeat_prefill", "uniform sampling requires replay bounds")
                expected = replay_positions(c["replay_start"], c["replay_tokens"], count, split == "validation")
                require(np.array_equal(p, expected), "queries do not uniformly cover the full replay partition")


def uniform_branch_positions(start, tokens, count):
    require(type(start) is int and start >= 0 and type(tokens) is int and tokens > 0 and
            type(count) is int and 0 < count <= tokens, "invalid branch query population")
    if count == 1:
        return np.asarray([start + (tokens - 1) // 2], dtype="<u4")
    return np.asarray([start + i * (tokens - 1) // (count - 1) for i in range(count)], dtype="<u4")


def validate_branch_queries(meta, positions):
    c = meta["capture"]
    t, v, e = c["training_rows"], c["validation_rows"], c.get("evaluation_rows", 0)
    sources = {x["id"]: x for x in c["query_sources"]}
    partition_base = {"training": 0, "validation": t, "evaluation": t + v}
    require(np.all(positions != 0xffffffff), "query position is absent")
    for stratum in c["strata"]:
        head = stratum["kv_head"]
        for partition in ("training", "validation", "evaluation"):
            count = stratum.get(partition + "_count", 0)
            if count == 0:
                continue
            split_start = partition_base[partition] + stratum[partition + "_start"]
            observed_count = 0
            for item in stratum["query_source_ranges"][partition]:
                source = sources[item["source_id"]]
                begin = split_start + item["start"]
                observed = positions[head, begin:begin + item["count"]]
                # Production quotas sample each logical source independently.
                # This guarantees equal source weight even when a paired
                # prefill/response branch has very unequal token lengths.
                expected = replay_positions(source["logical_start"], source["logical_count"],
                                            item["count"], source["sampling_partition"] == "validation")
                require(len(expected) == item["count"], "query source does not partition physical branch")
                require(np.array_equal(observed, expected), "queries do not uniformly cover declared branch")
                require(len(np.unique(observed)) == item["count"], "duplicate query positions within source branch")
                observed_count += len(observed)
            require(observed_count == count, "incomplete query source partition")


def replay_positions(start, tokens, count, held):
    """Independent receipt check of the versioned replay sampling contract."""
    candidates = []
    for position in range(start, start + tokens):
        group = position // 4
        hashed = ((group ^ (group >> 16)) * 0x7feb352d) & 0xffffffff
        hashed = ((hashed ^ (hashed >> 15)) * 0x846ca68b) & 0xffffffff
        hashed ^= hashed >> 16
        if (position % 4 == hashed % 4) == held:
            candidates.append(position)
    require(0 < count <= len(candidates), "insufficient replay query population")
    if count == 1:
        return np.asarray([candidates[(len(candidates) - 1) // 2]], dtype="<u4")
    return np.asarray([candidates[i * (len(candidates) - 1) // (count - 1)] for i in range(count)], dtype="<u4")


def replay_population(start, tokens, held):
    """Count one source's stable train or held partition without allocating."""
    require(type(start) is int and start >= 0 and type(tokens) is int and tokens > 0,
            "invalid replay query population")
    count = 0
    for position in range(start, start + tokens):
        group = position // 4
        hashed = ((group ^ (group >> 16)) * 0x7feb352d) & 0xffffffff
        hashed = ((hashed ^ (hashed >> 15)) * 0x846ca68b) & 0xffffffff
        hashed ^= hashed >> 16
        count += (position % 4 == hashed % 4) == held
    return count


def exact_attention(keys, values, queries, scale):
    scores = queries.astype(np.float64) @ keys.astype(np.float64).T * scale
    maxima = scores.max(axis=1)
    shifted = np.exp(scores - maxima[:, None])
    mass = shifted.sum(axis=1)
    probs = shifted / mass[:, None]
    return probs, shifted, probs @ values.astype(np.float64), maxima + np.log(mass)


def seal(spec_path, destination):
    """Seal explicit capture metadata and bytes, including exact FP64 outputs.

    Spec tensors map names to raw files, relative to the spec. Model/tokenizer/
    template and source identities must come from capture-time provenance; this
    tool does not claim a legacy capture was made by the current source tree.
    """
    spec_path, destination = Path(spec_path).resolve(), Path(destination).resolve()
    meta = read_json(spec_path)
    shapes = shape_contract(meta)
    require(not destination.exists(), "capture destination already exists")
    require(set(meta["tensors"]) == set(shapes), "capture requires keys, values, queries, positions")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".attention-capture-", dir=destination.parent))
    try:
        arrays, files = {}, {}
        for name, (dtype, shape) in shapes.items():
            source = spec_path.parent / meta["tensors"][name]
            arrays[name] = raw_array(source, dtype, shape)
            # Write the bytes actually validated, avoiding a copy/hash race.
            path = temporary / (name + ".bin")
            arrays[name].tofile(path)
            files[name] = {"path": path.name, "sha256": digest(path), "bytes": path.stat().st_size}
        validate_queries(meta, arrays)
        c = meta["capture"]
        outputs, partitions = [], []
        for head in range(c["heads"]):
            _, _, y, z = exact_attention(arrays["keys"][:, head], arrays["values"][:, head], arrays["queries"][head], c["scale"])
            outputs.append(y)
            partitions.append(z)
        for name, data in (("exact_outputs", outputs), ("exact_log_partition", partitions)):
            path = temporary / (name + ".bin")
            np.asarray(data, dtype="<f8").tofile(path)
            files[name] = {"path": path.name, "sha256": digest(path), "bytes": path.stat().st_size}
        meta["tensors"] = files
        meta["sealed_by"] = {"script_sha256": script_identity(), "numpy": np.__version__, "scipy": scipy.__version__}
        write_json(temporary / "manifest.json", meta)
        temporary.rename(destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return destination / "manifest.json"


def load_capture(path):
    path = Path(path).resolve()
    meta = read_json(path)
    shapes = shape_contract(meta)
    c = meta["capture"]
    q = c["training_rows"] + c["validation_rows"] + c.get("evaluation_rows", 0)
    shapes.update(exact_outputs=("<f8", (c["heads"], q, c["dim"])),
                  exact_log_partition=("<f8", (c["heads"], q)))
    require(set(meta["tensors"]) == set(shapes), "incomplete sealed capture")
    arrays = {}
    for name, (dtype, shape) in shapes.items():
        entry = meta["tensors"][name]
        require(Path(entry["path"]).name == entry["path"], "capture tensor must be local")
        file = path.parent / entry["path"]
        identity(entry["sha256"], name)
        arrays[name] = raw_array(file, dtype, shape)
        # Hash the data used, rather than reopening a potentially changing file.
        require(arrays[name].nbytes == entry["bytes"] and hashlib.sha256(arrays[name].tobytes()).hexdigest() == entry["sha256"], f"tensor integrity failed: {name}")
    validate_queries(meta, arrays)
    return meta, arrays


def least_squares(matrix, target):
    return lstsq(matrix, target, cond=RCOND, lapack_driver="gelsy", check_finite=True)[0]


def bounded_mass(matrix, target):
    lo, hi = np.exp(-3), np.exp(3)
    weights = np.clip(least_squares(matrix, target), lo, hi)
    direction = np.random.default_rng(0).normal(size=matrix.shape[1])
    direction /= np.linalg.norm(direction)
    for _ in range(3):
        vector = matrix @ direction
        vector /= max(np.linalg.norm(vector), 1e-12)
        direction = matrix.T @ vector
        direction /= max(np.linalg.norm(direction), 1e-12)
    lipschitz = max(float(direction @ (matrix.T @ (matrix @ direction))), 1e-6)
    for _ in range(2):
        weights = np.clip(weights - matrix.T @ (matrix @ weights - target) / lipschitz, lo, hi)
    return weights


def coordinate_fit(matrix, weights, iterations):
    residual = matrix @ weights - 1
    for _ in range(iterations):
        for column in range(matrix.shape[1]):
            feature = matrix[:, column]
            norm = feature @ feature
            if norm < 1e-20:
                continue
            value = max(1e-6, weights[column] - (feature @ residual) / norm)
            if not np.isfinite(value) or value > 1e6:
                raise NumericalFailure("coordinate fit diverged")
            residual += feature * (value - weights[column])
            weights[column] = value
    return weights


def pursuit(probs, retained):
    """Current Zig selection recipe in FP64; no dependence on held-out Q."""
    chosen, weights = [], np.zeros(retained)
    residual = np.ones(len(probs))
    batch = min(8, max(1, retained // 4))
    while len(chosen) < retained:
        scores = probs.T @ residual
        scores[chosen] = -np.inf
        chosen.extend(np.argsort(-scores, kind="stable")[:min(batch, retained - len(chosen))].tolist())
        selected = probs[:, chosen]
        weights[:len(chosen)] = coordinate_fit(selected, weights[:len(chosen)], 8)
        residual = 1 - selected @ weights[:len(chosen)]
    order = np.argsort(chosen)
    return np.asarray(chosen)[order], np.maximum(weights[order], 1e-6)


def upstream_omp(features, target, retained):
    """FP64 port of pinned SimpleOMPCompaction selection and mass fitting.

    The upstream implementation calls its solve NNLS, but its default is an
    unconstrained least-squares solve followed by a lower clamp. Preserve that
    distinction in both the recipe name and receipt instead of relabeling it as
    mathematical NNLS. np.argmax provides deterministic lowest-index tie
    breaking, and the final source order is canonicalized for storage.
    """
    require(features.ndim == 2 and target.shape == (features.shape[0],), "invalid OMP features")
    require(0 < retained <= features.shape[1], "invalid OMP retention")
    selected = []
    current = np.zeros_like(target, dtype=np.float64)
    weights = np.empty(0, dtype=np.float64)
    for _ in range(retained):
        residual = target - current
        correlation = features.T @ residual
        if selected:
            correlation[np.asarray(selected)] = -np.inf
        index = int(np.argmax(correlation))
        if not np.isfinite(correlation[index]):
            raise NumericalFailure("OMP exhausted finite source columns")
        selected.append(index)
        matrix = features[:, selected]
        weights = np.maximum(least_squares(matrix, target), 1e-12)
        if not np.isfinite(weights).all():
            raise NumericalFailure("OMP mass solve produced nonfinite weights")
        current = matrix @ weights
    order = np.argsort(selected, kind="stable")
    return np.asarray(selected, dtype=np.int64)[order], weights[order]


def cpqr_selection(features, retained):
    """Offline training-only selection that preserves diverse query directions."""
    require(features.ndim == 2 and 0 < retained <= features.shape[1], "invalid CPQR features")
    require(np.isfinite(features).all(), "nonfinite CPQR features")
    tie_scale = 1.0 - np.arange(features.shape[1], dtype=np.float64) * np.finfo(np.float64).eps * 8
    _, pivots = qr(features * tie_scale, mode="r", pivoting=True, overwrite_a=True, check_finite=True)
    return np.sort(np.asarray(pivots[:retained], dtype=np.int64))


def training_strata_groups(capture, head):
    """Return the predeclared training partition for one KV head.

    This function deliberately receives capture metadata rather than query
    tensors.  Its result indexes only the training prefix.  Validation and
    evaluation ranges are neither inspected nor returned.
    """
    rows = capture["training_rows"]
    groups, coverage = [], np.zeros(rows, dtype=np.uint16)
    for stratum in capture["strata"]:
        if stratum["kv_head"] != head:
            continue
        start, count = stratum["training_start"], stratum["training_count"]
        require(type(start) is int and type(count) is int and start >= 0 and count > 0 and
                start + count <= rows, "invalid minimax training stratum")
        indices = np.arange(start, start + count, dtype=np.int64)
        coverage[indices] += 1
        groups.append({"consumer_layer": stratum["consumer_layer"],
                       "query_head": stratum["query_head"], "rows": indices})
    require(groups and len(groups) <= MINIMAX_MAX_GROUPS and np.all(coverage == 1),
            "minimax groups must partition every training row exactly once")
    return groups


def validate_minimax_training(probabilities, groups, retained):
    require(isinstance(probabilities, np.ndarray) and probabilities.ndim == 2,
            "invalid minimax probability matrix")
    rows, columns = probabilities.shape
    require(0 < rows <= MINIMAX_MAX_ROWS and 1 < columns <= MINIMAX_MAX_COLUMNS and
            0 < retained < columns, "minimax dimensions exceed bounded recipe")
    require(np.isfinite(probabilities).all() and np.all(probabilities >= 0),
            "minimax probabilities must be finite and nonnegative")
    require(np.allclose(probabilities.sum(axis=1), 1., rtol=0., atol=1e-10),
            "minimax probability rows must sum to one")
    require(isinstance(groups, list) and 0 < len(groups) <= MINIMAX_MAX_GROUPS,
            "invalid minimax training groups")
    coverage = np.zeros(rows, dtype=np.uint16)
    for group in groups:
        indices = np.asarray(group["rows"], dtype=np.int64)
        require(indices.ndim == 1 and len(indices) and np.all(indices >= 0) and np.all(indices < rows) and
                len(np.unique(indices)) == len(indices), "invalid minimax group rows")
        coverage[indices] += 1
    require(np.all(coverage == 1), "minimax groups must be complete and nonoverlapping")

    # Exact duplicate or zero-support columns cannot add coverage.  Excluding
    # them before selection makes an exhausted retention budget a hard error,
    # rather than padding a receipt with rows that cannot affect the objective.
    canonical, candidates = set(), []
    for column in range(columns):
        values = np.ascontiguousarray(probabilities[:, column], dtype=np.float64)
        if float(np.max(values)) <= 1e-15:
            continue
        fingerprint = hashlib.sha256(values.tobytes()).digest()
        if fingerprint in canonical:
            continue
        canonical.add(fingerprint)
        candidates.append(column)
    if len(candidates) < retained:
        raise NumericalFailure("minimax finite unique columns exhausted")
    return np.asarray(candidates, dtype=np.int64)


def chebyshev_mass_fit(matrix):
    """Solve the pinned positive L-infinity mass problem in FP64."""
    require(matrix.ndim == 2 and matrix.shape[0] and matrix.shape[1] and
            np.isfinite(matrix).all() and np.all(matrix >= 0), "invalid minimax mass matrix")
    rows, columns = matrix.shape
    objective = np.zeros(columns + 1, dtype=np.float64)
    objective[-1] = 1.
    negative_t = -np.ones((rows, 1), dtype=np.float64)
    inequalities = np.vstack((np.hstack((matrix, negative_t)),
                              np.hstack((-matrix, negative_t))))
    bounds = [(MINIMAX_WEIGHT_MIN, MINIMAX_WEIGHT_MAX)] * columns + [(0., None)]
    solved = linprog(objective, A_ub=inequalities,
                     b_ub=np.concatenate((np.ones(rows), -np.ones(rows))),
                     bounds=bounds, method="highs-ds",
                     options={"presolve": True,
                              "dual_feasibility_tolerance": MINIMAX_PRIMAL_TOLERANCE,
                              "primal_feasibility_tolerance": MINIMAX_PRIMAL_TOLERANCE})
    if not solved.success or solved.status != 0 or solved.x is None or not np.isfinite(solved.x).all():
        raise NumericalFailure(f"minimax LP failed: status={solved.status}")
    weights, optimum = solved.x[:-1], float(solved.x[-1])
    residual = matrix @ weights - 1.
    measured = float(np.max(np.abs(residual)))
    if measured > optimum + 16 * MINIMAX_PRIMAL_TOLERANCE:
        raise NumericalFailure("minimax LP violated its reported primal bound")
    diagnostics = {
        "objective_max_abs": measured,
        "reported_objective": optimum,
        "iterations": int(solved.nit),
        "status": int(solved.status),
        "message": str(solved.message),
        "lower_bound_count": int(np.count_nonzero(weights <= MINIMAX_WEIGHT_MIN * (1 + 1e-8))),
        "upper_bound_count": int(np.count_nonzero(weights >= MINIMAX_WEIGHT_MAX * (1 - 1e-8))),
        "primal_tolerance": MINIMAX_PRIMAL_TOLERANCE,
    }
    return weights, diagnostics


def minimax_peak_selection(probabilities, groups, retained):
    """Bounded training-only peak coverage followed by worst-stratum minimax.

    `probabilities` must contain the training partition only.  Selection never
    accepts a validation/evaluation matrix, making held-out leakage impossible
    through this interface.
    """
    candidates = validate_minimax_training(probabilities, groups, retained)
    worst_case_solves = 2 + int(np.ceil((retained - 1) / MINIMAX_BATCH))
    require(worst_case_solves <= MINIMAX_MAX_LP_SOLVES,
            "minimax requested retention exceeds LP solve budget")
    rows, columns = probabilities.shape
    candidate_mask = np.zeros(columns, dtype=bool)
    candidate_mask[candidates] = True
    selected, coverage = [], np.zeros(rows, dtype=np.float64)
    row_influence = np.empty(rows, dtype=np.float64)
    for group in groups:
        indices = np.asarray(group["rows"], dtype=np.int64)
        row_influence[indices] = 1. / (len(groups) * len(indices))

    peak_slots = max(1, int(np.floor(retained * MINIMAX_PEAK_FRACTION)))
    while len(selected) < peak_slots and float(np.min(coverage)) < MINIMAX_TAU:
        deficit = np.maximum(MINIMAX_TAU - coverage, 0.)
        scores = np.full(columns, -np.inf, dtype=np.float64)
        available = np.flatnonzero(candidate_mask)
        for column in available:
            scores[column] = float(row_influence @ np.minimum(probabilities[:, column], deficit))
        chosen = int(np.argmax(scores))
        if not np.isfinite(scores[chosen]) or scores[chosen] <= 0:
            raise NumericalFailure("minimax peak coverage exhausted positive candidates")
        selected.append(chosen)
        candidate_mask[chosen] = False
        coverage += probabilities[:, chosen]

    if not selected:
        raise NumericalFailure("minimax peak coverage selected no source rows")
    peak_selected = len(selected)
    weights, solve = chebyshev_mass_fit(probabilities[:, selected])
    solve_history = [solve]
    while len(selected) < retained:
        residual = probabilities[:, selected] @ weights - 1.
        group_errors = np.asarray([np.max(np.abs(residual[np.asarray(group["rows"], dtype=np.int64)]))
                                   for group in groups])
        worst_order = np.argsort(-group_errors, kind="stable")
        additions = []
        batch_target = min(MINIMAX_BATCH, retained - len(selected))
        if float(group_errors[worst_order[0]]) <= 8 * MINIMAX_PRIMAL_TOLERANCE:
            # The objective is already numerically zero but the storage ABI
            # requires an exact row count. Fill by training energy only; this
            # cannot consult held-out rows and preserves stable low-index ties.
            energy = np.full(columns, -np.inf, dtype=np.float64)
            available = np.flatnonzero(candidate_mask)
            energy[available] = np.sum(probabilities[:, available] ** 2, axis=0)
            fill = np.argsort(-energy, kind="stable")[:batch_target]
            additions = [int(x) for x in fill if np.isfinite(energy[x]) and energy[x] > 0]
            for chosen in additions:
                candidate_mask[chosen] = False
        else:
            for group_index in worst_order:
                indices = np.asarray(groups[int(group_index)]["rows"], dtype=np.int64)
                active_error = float(np.max(np.abs(residual[indices])))
                active = indices[np.abs(residual[indices]) >= active_error - 8 * MINIMAX_PRIMAL_TOLERANCE]
                scores = np.full(columns, -np.inf, dtype=np.float64)
                available = np.flatnonzero(candidate_mask)
                if not len(available):
                    break
                scores[available] = (np.abs(residual[active])[:, None] *
                                     probabilities[np.ix_(active, available)]).sum(axis=0)
                for chosen in np.argsort(-scores, kind="stable"):
                    if len(additions) == batch_target:
                        break
                    if not np.isfinite(scores[chosen]) or scores[chosen] <= 0:
                        break
                    additions.append(int(chosen))
                    candidate_mask[chosen] = False
                if len(additions) == batch_target:
                    break
        if not additions:
            raise NumericalFailure("minimax worst-stratum selection exhausted positive candidates")
        selected.extend(additions)
        if len(solve_history) + 2 > MINIMAX_MAX_LP_SOLVES:
            raise NumericalFailure("minimax LP solve budget exceeded")
        weights, solve = chebyshev_mass_fit(probabilities[:, selected])
        previous = solve_history[-1]["objective_max_abs"]
        if solve["objective_max_abs"] > previous + 32 * MINIMAX_PRIMAL_TOLERANCE:
            raise NumericalFailure("minimax objective increased after adding source rows")
        solve_history.append(solve)

    order = np.argsort(selected, kind="stable")
    selected = np.asarray(selected, dtype=np.int64)[order]
    weights, final_solve = chebyshev_mass_fit(probabilities[:, selected])
    solve_count = len(solve_history) + 1
    if solve_count > MINIMAX_MAX_LP_SOLVES:
        raise NumericalFailure("minimax LP solve budget exceeded")
    residual = probabilities[:, selected] @ weights - 1.
    if final_solve["objective_max_abs"] > solve_history[-1]["objective_max_abs"] + 32 * MINIMAX_PRIMAL_TOLERANCE:
        raise NumericalFailure("canonical minimax refit increased the objective")
    per_group = []
    for group, group_residual in zip(groups, (residual[np.asarray(x["rows"], dtype=np.int64)] for x in groups)):
        per_group.append({"consumer_layer": group.get("consumer_layer"),
                          "query_head": group.get("query_head"),
                          "rows": len(group_residual),
                          "mass_max": float(np.max(np.abs(group_residual))),
                          "mass_rms": float(np.sqrt(np.mean(group_residual ** 2)))})
    diagnostics = {
        "diagnostic_only": True,
        "information_boundary": "training_rows_and_predeclared_training_strata_only",
        "tau": MINIMAX_TAU,
        "peak_budget_fraction": MINIMAX_PEAK_FRACTION,
        "peak_rows_selected": peak_selected,
        "batch_limit": MINIMAX_BATCH,
        "lp_solve_limit": MINIMAX_MAX_LP_SOLVES,
        "lp_solves": solve_count,
        "rows": rows,
        "source_columns": columns,
        "groups": len(groups),
        "estimated_peak_array_scratch_bytes": int(
            8 * (2 * rows * (retained + 1) + 2 * rows + retained + 1)),
        "solver_internal_allocation_included": False,
        "objective_history": [x["objective_max_abs"] for x in solve_history] +
                             [final_solve["objective_max_abs"]],
        "final_solver": final_solve,
        "training_group_errors": per_group,
        "scipy_version": scipy.__version__,
    }
    return selected, weights, diagnostics


def conditioning(matrix):
    singular = svdvals(matrix)
    rank = int(np.sum(singular > singular[0] * RCOND)) if singular[0] else 0
    condition = float(singular[0] / singular[-1]) if singular[-1] > 0 else None
    if condition is not None and not np.isfinite(condition):
        condition = None
    return {"rank": rank, "columns": matrix.shape[1], "condition_2": condition, "rcond": RCOND}


def residual_diagnostics(matrix, target, solution):
    residual = matrix @ solution - target
    squared = float(np.sum(residual ** 2))
    target_squared = float(np.sum(target ** 2))
    return {"squared_l2_objective": squared,
            "residual_l2": float(np.sqrt(squared)),
            "residual_rms": float(np.sqrt(squared / max(residual.size, 1))),
            "residual_max_abs": float(np.max(np.abs(residual))),
            "residual_relative_l2": float(np.sqrt(squared / max(target_squared, 1e-24)))}


def estimated_fit_bytes(meta, retained):
    """Conservative in-process FP64 scratch estimate, excluding capture bytes."""
    c = meta["capture"]
    q = c["training_rows"] + c["validation_rows"] + c.get("evaluation_rows", 0)
    n, d = c["rows"], c["dim"]
    train = c["training_rows"]
    elements = (3 * q * n + 4 * q * d + 3 * train * retained +
                2 * retained * retained + 4 * retained * d + 8 * q)
    return elements * np.dtype(np.float64).itemsize


def errors(mass, actual, target):
    return {"mass_max": float(np.max(np.abs(mass - 1))),
            "mass_rms": float(np.sqrt(np.mean((mass - 1) ** 2))),
            "output_relative_rmse": float(np.sqrt(np.sum((actual - target) ** 2) / max(float(np.sum(target ** 2)), 1e-12)))}


def passes(report):
    return report["mass_max"] <= MASS_LIMIT and report["output_relative_rmse"] <= OUTPUT_LIMIT


def query_coverage(probs, selected, training_rows, validation_rows=None):
    """Diagnostic only: never feed held-out visibility back into selection."""
    omitted = np.ones(probs.shape[1], dtype=bool)
    omitted[selected] = False
    omitted_mass = probs[:, omitted].sum(axis=1)
    train_peak = probs[:training_rows].max(axis=0)
    held_end = training_rows + validation_rows if validation_rows is not None else len(probs)
    held_peak = probs[training_rows:held_end].max(axis=0)
    # A rare key can be nearly invisible in training and dominate a future
    # query. Average fit error cannot diagnose that missing coverage.
    unseen = (train_peak <= MASS_LIMIT) & (held_peak > MASS_LIMIT)
    report = {"diagnostic_only": True,
            "omitted_mass_training_max": float(omitted_mass[:training_rows].max()),
            "omitted_mass_validation_max": float(omitted_mass[training_rows:training_rows + validation_rows].max()) if validation_rows else float(omitted_mass[training_rows:].max()),
            "held_dominant_training_rare_keys": np.flatnonzero(unseen).tolist(),
            "omitted_held_dominant_training_rare_keys": np.flatnonzero(unseen & omitted).tolist()}
    if validation_rows is not None and training_rows + validation_rows < len(probs):
        report["omitted_mass_evaluation_max"] = float(omitted_mass[training_rows + validation_rows:].max())
    return report


def fit_head(meta, arrays, head, retained, recipe):
    c, started = meta["capture"], time.perf_counter()
    n, d, train = c["rows"], c["dim"], c["training_rows"]
    require(1 <= retained <= n and recipe in RECIPES, "invalid fit profile")
    keys, values, queries = arrays["keys"][:, head], arrays["values"][:, head], arrays["queries"][head]
    probs, shifted, target, z = exact_attention(keys, values, queries, c["scale"])
    require(np.allclose(target, arrays["exact_outputs"][head], atol=1e-12, rtol=1e-12) and
            np.allclose(z, arrays["exact_log_partition"][head], atol=1e-12, rtol=1e-12), "exact reference mismatch")
    result = {"head": head, "owner_layer": c["owner_layer"], "retained_rows": retained, "source_rows": n,
              "recipe": recipe, "exact": retained == n, "source_kv_bytes": n * d * 4,
              "retained_kv_bias_bytes": retained * (d * 4 + (0 if retained == n else 4)),
              "solver_identity": RECIPE_IDENTITIES[recipe]}
    result["kv_payload_savings"] = {
        "scope": "per_head_key_value_and_mass_bias_only",
        "includes_session_or_fitter_overhead": False,
        "source_bytes": result["source_kv_bytes"],
        "retained_bytes": result["retained_kv_bias_bytes"],
        "saved_bytes": result["source_kv_bytes"] - result["retained_kv_bias_bytes"],
        "saved_fraction": ((result["source_kv_bytes"] - result["retained_kv_bias_bytes"]) /
                           result["source_kv_bytes"]),
    }
    if retained == n:
        selected, weights, fitted = np.arange(n), np.ones(n), values.astype(float)
        features, mass_target, a = shifted[:train], shifted[:train].sum(axis=1), probs[:train]
        result["mass_conditioning"] = result["value_conditioning"] = None
    else:
        if recipe == "max_shift_omp_lstsq":
            features, mass_target = shifted[:train], shifted[:train].sum(axis=1)
            selected, weights = upstream_omp(features, mass_target, retained)
        elif recipe == "relative_minimax_ridge_peak":
            groups = training_strata_groups(c, head)
            selected, weights, selection_diagnostics = minimax_peak_selection(
                np.ascontiguousarray(probs[:train], dtype=np.float64), groups, retained)
            result["selection_diagnostics"] = selection_diagnostics
        elif recipe.endswith("pursuit"):
            selected, weights = pursuit(probs[:train], retained)
        elif recipe.endswith("cpqr"):
            selected = cpqr_selection(probs[:train], retained)
            weights = np.full(retained, n / retained)
        else:
            selected = np.sort(np.argsort(-(probs[:train] ** 2).sum(axis=0), kind="stable")[:retained])
            weights = np.full(retained, n / retained)
        if recipe != "max_shift_omp_lstsq":
            features = shifted[:train, selected] if recipe == "max_shift_qr" else probs[:train, selected]
            mass_target = shifted[:train].sum(axis=1) if recipe == "max_shift_qr" else np.ones(train)
            if recipe == "relative_minimax_ridge_peak":
                # Selection already performed the final bounded Chebyshev fit.
                pass
            else:
                weights = coordinate_fit(features, weights, 128) if recipe.startswith("relative_cd") else bounded_mass(features, mass_target)
        else:
            features = features[:, selected]
        represented = probs[:, selected] * weights
        if not np.isfinite(represented).all() or np.any(represented.sum(axis=1) <= 0):
            raise NumericalFailure("selected keys have zero or nonfinite query mass")
        attention = represented / represented.sum(axis=1)[:, None]
        a = attention[:train]
        if recipe.startswith("relative_cd") or recipe == "relative_minimax_ridge_peak":
            prior = values[selected].astype(float)
            gram = a.T @ a
            ridge = 1e-4 * max(float(np.trace(gram)) / retained, 1e-6)
            fitted = prior + np.linalg.solve(gram + np.eye(retained) * ridge, a.T @ (target[:train] - a @ prior))
        else:
            fitted = least_squares(a, target[:train])
        result["mass_conditioning"], result["value_conditioning"] = conditioning(features), conditioning(a)
    result["fit_diagnostics"] = {
        "mass": residual_diagnostics(features, mass_target, weights),
        "value": residual_diagnostics(a, target[:train], fitted),
    }
    # Evaluate exactly the cache storage types: original half keys, half V,
    # float mass bias. Re-expanding a rounded bias matters at tight gates.
    with np.errstate(over="ignore", invalid="ignore"):
        stored = fitted.astype(np.float16).astype(float)
        bias = np.log(weights).astype(np.float32).astype(float)
    result["finite_storage"] = bool(np.isfinite(stored).all() and np.isfinite(bias).all())
    result["selected_source_rows"] = selected.tolist()
    result["query_coverage"] = query_coverage(probs, selected, train, c["validation_rows"] if meta["schema_version"] == 2 else None)
    result["fit_seconds"] = time.perf_counter() - started
    if not result["finite_storage"]:
        result.update(accepted=False, reason="nonfinite_stored_candidate")
        return result
    represented = probs[:, selected] * np.exp(bias)
    mass = represented.sum(axis=1)
    attention = represented / mass[:, None]
    actual = attention @ stored
    result["f16_value_relative_rmse"] = float(np.linalg.norm(stored - fitted) / max(np.linalg.norm(fitted), 1e-12))
    result["f16_output_relative_rmse"] = float(np.linalg.norm(attention @ (stored - fitted)) / max(np.linalg.norm(target), 1e-12))
    stored_weights = np.exp(bias)
    weight_delta = stored_weights - weights
    value_delta = stored - fitted
    result["fit_diagnostics"]["storage_conversion"] = {
        "bias_dtype": "float32", "value_dtype": "float16",
        "mass_weight_max_abs": float(np.max(np.abs(weight_delta))),
        "mass_weight_relative_l2": float(np.linalg.norm(weight_delta) / max(np.linalg.norm(weights), 1e-12)),
        "value_max_abs": float(np.max(np.abs(value_delta))),
        "value_relative_l2": result["f16_value_relative_rmse"],
        "output_relative_l2": result["f16_output_relative_rmse"],
    }
    result["training"] = errors(mass[:train], actual[:train], target[:train])
    gate_end = train + c["validation_rows"]
    result["validation"] = errors(mass[train:gate_end], actual[train:gate_end], target[train:gate_end])
    evaluation_rows = c.get("evaluation_rows", 0)
    if evaluation_rows:
        result["evaluation"] = errors(mass[gate_end:], actual[gate_end:], target[gate_end:])
    per_consumer = []
    for s in c["strata"]:
        if s["kv_head"] != head:
            continue
        report = {"consumer_layer": s["consumer_layer"], "query_head": s["query_head"]}
        split_bases = [("training", 0), ("validation", train)]
        if evaluation_rows:
            split_bases.append(("evaluation", gate_end))
        for split, base in split_bases:
            start = base + s[split + "_start"]
            sl = slice(start, start + s[split + "_count"])
            report[split] = errors(mass[sl], actual[sl], target[sl])
        per_consumer.append(report)
    result["consumers"] = per_consumer
    # Training output is diagnostic; the fixed runtime contract gates training
    # mass and independent held-out mass/output, including every consumer.
    result["runtime_accepted"] = bool(result["training"]["mass_max"] <= MASS_LIMIT and passes(result["validation"]) and all(
        x["training"]["mass_max"] <= MASS_LIMIT and passes(x["validation"]) for x in per_consumer))
    result["independent_evaluation_passed"] = bool(not evaluation_rows or
        (passes(result["evaluation"]) and all(passes(x["evaluation"]) for x in per_consumer)))
    result["accepted"] = result["runtime_accepted"] and result["independent_evaluation_passed"]
    return result


def capture_contract(meta):
    c = meta["capture"]
    strata = c["strata"]
    if meta.get("schema_version") == 2:
        # Starts, counts and source ranges vary with question length and the
        # bounded sampler. The profile binds only stable consumer geometry and
        # the separately hashed protocol description.
        strata = [{key: s[key] for key in ("consumer_layer", "query_head", "kv_head")} for s in strata]
    result = {"source": meta["source"], "global_owner_shapes": meta["model"]["global_owner_shapes"],
            "source_rows": c["rows"], "head_dim": c["dim"], "scale": c["scale"],
            "consumers": c["consumers"], "strata": strata, "query_sampling": c.get("query_sampling")}
    if meta.get("schema_version") == 2:
        # The protocol binds fixed study templates and limits, while per-request
        # question/teacher token digests remain capture evidence rather than a
        # frozen-profile identity.
        result["query_protocol_sha256"] = c["query_protocol_sha256"]
    else:
        result.update(training_rows=c["training_rows"], validation_rows=c["validation_rows"])
    return result


def campaign_limits(max_fits=DEFAULT_MAX_FITS, max_seconds=DEFAULT_MAX_SECONDS,
                    max_working_set_bytes=DEFAULT_MAX_WORKING_SET_BYTES):
    require(type(max_fits) is int and 0 < max_fits <= 100000, "invalid campaign fit limit")
    require(isinstance(max_seconds, (int, float)) and np.isfinite(max_seconds) and 0 < max_seconds <= 24 * 60 * 60,
            "invalid campaign time limit")
    require(type(max_working_set_bytes) is int and MAX_CAPTURE_BYTES <= max_working_set_bytes <= 64 * 1024 ** 3,
            "invalid campaign working-set limit")
    return {"max_fits": max_fits, "max_seconds": float(max_seconds),
            "max_estimated_working_set_bytes": max_working_set_bytes}


def run_captures(paths, budgets, recipes, profile=None, limits=None):
    require(paths and len(set(map(str, paths))) == len(paths), "empty or duplicate captures")
    if profile is None:
        require(budgets and len(set(budgets)) == len(budgets) and recipes and len(set(recipes)) == len(recipes), "empty or duplicate fit profiles")
    limits = campaign_limits() if limits is None else campaign_limits(
        limits["max_fits"], limits["max_seconds"], limits["max_estimated_working_set_bytes"])
    planned_fits, planned_peak = 0, 0
    for path in paths:
        planned_meta = read_json(path)
        shape_contract(planned_meta)
        require(all(type(entry.get("bytes")) is int and 0 <= entry["bytes"] <= MAX_TENSOR_BYTES
                    for entry in planned_meta["tensors"].values()), "invalid planned tensor bytes")
        tensor_bytes = sum(entry["bytes"] for entry in planned_meta["tensors"].values())
        for head in range(planned_meta["capture"]["heads"]):
            planned_choices = [(m, r) for m in budgets for r in
                               (recipes[:1] if m == planned_meta["capture"]["rows"] else recipes)]
            if profile is not None:
                key = f"{model_key(planned_meta)}:{planned_meta['capture']['owner_layer']}:{head}"
                require(key in profile["profiles"], "frozen profile lacks model/owner/head")
                chosen = profile["profiles"][key]
                planned_choices = [(m, chosen["recipe"]) for m in
                                   chosen.get("retention_schedule", [chosen["retained_rows"]])]
            for retained, _ in planned_choices:
                require(type(retained) is int and 0 < retained <= planned_meta["capture"]["rows"],
                        "invalid planned retention")
                planned_fits += 1
                planned_peak = max(planned_peak, tensor_bytes + estimated_fit_bytes(planned_meta, retained))
    require(planned_fits <= limits["max_fits"], "campaign fit limit exceeded")
    require(planned_peak <= limits["max_estimated_working_set_bytes"],
            "estimated fit working set exceeds campaign limit")
    campaign_started, fits_started, peak_estimate = time.monotonic(), 0, 0
    captures, results = [], []
    for path in paths:
        meta, arrays = load_capture(path)
        require(meta["purpose"] != "evaluation" if profile is None else meta["purpose"] == "evaluation", "wrong corpus split for this command")
        if profile is not None:
            require(meta["document"]["sha256"] not in profile["calibration_document_hashes"] and meta["document"]["id"] not in profile["calibration_document_ids"], "evaluation document overlaps calibration")
            require(meta["tensors"]["queries"]["sha256"] not in profile["calibration_query_hashes"], "evaluation queries overlap calibration")
        capture_id = digest(path)
        require(capture_id not in {x["sha256"] for x in captures}, "duplicate capture bytes")
        captures.append({"sha256": capture_id, "path": str(Path(path).resolve()), "query_sha256": meta["tensors"]["queries"]["sha256"],
                         "metadata": {k: meta[k] for k in ("schema_version", "purpose", "model", "document", "source", "capture")}})
        for head in range(meta["capture"]["heads"]):
            choices = [(m, r) for m in budgets for r in (recipes[:1] if m == meta["capture"]["rows"] else recipes)]
            if profile is not None:
                key = f"{model_key(meta)}:{meta['capture']['owner_layer']}:{head}"
                require(key in profile["profiles"], "frozen profile lacks model/owner/head")
                chosen = profile["profiles"][key]
                contract = capture_contract(meta)
                require(all(chosen.get(k) == value for k, value in contract.items()), "frozen source/geometry contract mismatch")
                schedule = chosen.get("retention_schedule", [chosen["retained_rows"]])
                require(schedule and schedule[0] == chosen["retained_rows"] and
                        all(type(m) is int and 0 < m <= chosen["source_rows"] for m in schedule) and
                        schedule == sorted(set(schedule)), "invalid frozen retention schedule")
                choices = [(m, chosen["recipe"]) for m in schedule]
            for m, recipe in choices:
                require(m <= meta["capture"]["rows"], "budget exceeds capture block")
                require(fits_started < limits["max_fits"], "campaign fit limit exceeded")
                require(time.monotonic() - campaign_started < limits["max_seconds"], "campaign time limit exceeded")
                estimate = arrays["keys"].nbytes + arrays["values"].nbytes + arrays["queries"].nbytes + arrays["positions"].nbytes
                estimate += arrays["exact_outputs"].nbytes + arrays["exact_log_partition"].nbytes + estimated_fit_bytes(meta, m)
                require(estimate <= limits["max_estimated_working_set_bytes"], "estimated fit working set exceeds campaign limit")
                fits_started += 1
                peak_estimate = max(peak_estimate, estimate)
                try:
                    result = fit_head(meta, arrays, head, m, recipe)
                except (NumericalFailure, np.linalg.LinAlgError) as exc:
                    result = {"head": head, "owner_layer": meta["capture"]["owner_layer"], "retained_rows": m,
                              "source_rows": meta["capture"]["rows"], "recipe": recipe, "exact": m == meta["capture"]["rows"],
                              "source_kv_bytes": meta["capture"]["rows"] * meta["capture"]["dim"] * 4,
                              "retained_kv_bias_bytes": m * (meta["capture"]["dim"] * 4 + (0 if m == meta["capture"]["rows"] else 4)),
                              "solver_identity": RECIPE_IDENTITIES[recipe], "finite_storage": False,
                              "accepted": False, "reason": str(exc)}
                result["capture_sha256"] = capture_id
                runtime_accepted = result.get("runtime_accepted", result["accepted"])
                result["selected_attempt"] = profile is None or runtime_accepted or m == choices[-1][0]
                results.append(result)
                print(json.dumps({k: result[k] for k in ("capture_sha256", "owner_layer", "head", "retained_rows", "recipe", "accepted")}), file=sys.stderr, flush=True)
                # Adaptive retry is an online decision and therefore may use
                # only fit/runtime-gate evidence. Independent exact-teacher
                # continuation can reject qualification, never choose a retry.
                if profile is not None and runtime_accepted:
                    break
    if profile is not None:
        validate_coverage(captures)
    totals = {}
    selected = [x for x in results if x["selected_attempt"]]
    for result in selected:
        meta = next(x["metadata"] for x in captures if x["sha256"] == result["capture_sha256"])
        # A compressible document must not hide another document's exact-only
        # fallback. These are raw block bytes, never device allocation claims.
        key = model_key(meta) + ":" + meta["document"]["sha256"]
        total = totals.setdefault(key, [0, 0])
        total[0] += result["source_kv_bytes"]
        total[1] += result["retained_kv_bias_bytes"]
    return {"schema_version": SCHEMA, "stage": "offline_numerical", "serving_qualified": False,
            "script_sha256": script_identity(), "author_revision": AUTHOR_REVISION,
            "numerics": {"dtype": "float64", "storage": "f16_kv_f32_bias", "driver": "gelsy", "rcond": RCOND,
                         "mass_limit": MASS_LIMIT, "output_limit": OUTPUT_LIMIT, "coordinate_sweeps": 128,
                         "coordinate_weight_bounds": [1e-6, 1e6], "ridge_scale": 1e-4,
                         "pursuit_batch_max": 8, "pursuit_refit_sweeps": 8,
                         "qr_log_weight_bounds": [-3, 3], "qr_projected_steps": 2,
                         "power_iterations": 3, "power_seed": 0,
                         "minimax": {"diagnostic_only": True, "tau": MINIMAX_TAU,
                                     "peak_budget_fraction": MINIMAX_PEAK_FRACTION,
                                     "batch_limit": MINIMAX_BATCH,
                                     "max_rows": MINIMAX_MAX_ROWS,
                                     "max_columns": MINIMAX_MAX_COLUMNS,
                                     "max_groups": MINIMAX_MAX_GROUPS,
                                     "max_lp_solves": MINIMAX_MAX_LP_SOLVES,
                                     "weight_bounds": [MINIMAX_WEIGHT_MIN, MINIMAX_WEIGHT_MAX],
                                     "solver_method": "highs-ds",
                                     "primal_tolerance": MINIMAX_PRIMAL_TOLERANCE,
                                     "training_only": True},
                         "recipe_identities": RECIPE_IDENTITIES},
            "environment": {"python": platform.python_version(), "numpy": np.__version__, "scipy": scipy.__version__, "platform": platform.platform()},
            "campaign_limits": limits,
            "campaign_usage": {"fits_planned": planned_fits, "fits_started": fits_started,
                               "peak_estimated_working_set_bytes": peak_estimate,
                               "elapsed_seconds": time.monotonic() - campaign_started},
            "captures": captures, "results": results,
            "all_passed": all(x["accepted"] for x in selected),
            "all_candidates_passed": all(x["accepted"] for x in results),
            "all_nonidentity": all(not x["exact"] for x in selected),
            "raw_bytes_by_model_document": {key: {"source": value[0], "retained": value[1]} for key, value in totals.items()},
            "all_models_reduce_kv_bytes": all(saved < source for source, saved in totals.values())}


def validate_coverage(captures):
    """Every document must include every global owner, with stable geometry."""
    layouts, documents = {}, {}
    for capture in captures:
        meta = capture["metadata"]
        key = model_key(meta)
        layout = meta["model"]["global_owner_shapes"]
        require(key not in layouts or layouts[key] == layout, "inconsistent model layout")
        layouts[key] = layout
        doc_key = (key, meta["document"]["sha256"])
        owners = documents.setdefault(doc_key, set())
        # Multiple blocks from one owner/document are allowed; all must pass.
        owners.add(meta["capture"]["owner_layer"])
    for (model, _), owners in documents.items():
        require(owners == {x["layer"] for x in layouts[model]}, "incomplete global owner coverage")


RUNTIME_PROTOCOL_DOMAIN = b"antfly.context-query-runtime-protocol.v2\x00"
RUNTIME_PROTOCOL_VERSION = 2
RUNTIME_PROTOCOL_SOURCES = (
    (0, "request_question_replay", "training", 0),
    (1, "fixed_study_prefill", "training", 1),
    (2, "fixed_study_generated_response", "training", 1),
    (3, "fixed_study_prefill", "validation", 2),
    (4, "fixed_study_generated_response", "validation", 2),
)
RUNTIME_KIND_TAG = {"request_question_replay": 0, "fixed_study_prefill": 1,
                    "fixed_study_generated_response": 2}
RUNTIME_PARTITION_TAG = {"training": 0, "validation": 1}
RUNTIME_GENERATION_MODE_TAG = {"greedy_fixed_length_ignore_eos": 1}


def _runtime_int(value, size):
    require(type(value) is int and 0 <= value < 1 << (size * 8), "invalid runtime protocol integer")
    return value.to_bytes(size, "little")


def runtime_protocol_identity(authority):
    """Match context_query_protocol.runtimeIdentity binary-domain v2."""
    require(isinstance(authority, dict) and set(authority) ==
            {"schema_version", "layer", "query_sampling", "accounting", "max_training_rows",
             "max_validation_rows", "sources", "owner", "strata", "fit", "gate"},
            "invalid runtime protocol authority")
    require(authority["schema_version"] == RUNTIME_PROTOCOL_VERSION and
            authority["query_sampling"] == "source_uniform_partition_v3" and
            authority["accounting"] == "serial_physical_branch_upper_bound_v2" and
            authority["max_training_rows"] == 3072 and authority["max_validation_rows"] == 1024,
            "invalid runtime protocol constants")
    expected_sources = [{"kind": kind, "partition": partition, "physical_branch": branch}
                        for _, kind, partition, branch in RUNTIME_PROTOCOL_SOURCES]
    require(authority["sources"] == expected_sources, "invalid runtime protocol source rules")
    owner = authority["owner"]
    require(isinstance(owner, dict) and set(owner) == {"kv_heads", "query_heads", "head_dim"},
            "invalid runtime protocol owner")
    layer = authority["layer"]
    require(type(layer) is int and 0 <= layer < 256 and
            all(type(owner[key]) is int and owner[key] > 0 for key in owner),
            "invalid runtime protocol geometry")
    require(owner["query_heads"] % owner["kv_heads"] == 0, "invalid runtime owner GQA geometry")
    strata = authority["strata"]
    require(isinstance(strata, list) and 0 < len(strata) <= 128, "invalid runtime strata")
    expected_strata = []
    for row in strata:
        require(isinstance(row, dict) and set(row) == {"consumer_layer", "query_head", "kv_head"} and
                all(type(row[key]) is int and row[key] >= 0 for key in row), "invalid runtime stratum")
        expected_strata.append((row["consumer_layer"], row["query_head"], row["kv_head"]))
    require(len(set(expected_strata)) == len(expected_strata), "duplicate runtime stratum")
    require(expected_strata == sorted(expected_strata), "runtime strata must be canonical")
    by_consumer = {}
    for consumer_layer, query_head, kv_head in expected_strata:
        by_consumer.setdefault(consumer_layer, []).append((query_head, kv_head))
    require(layer in by_consumer and len(by_consumer[layer]) == owner["query_heads"],
            "runtime owner GQA strata mismatch")
    for rows in by_consumer.values():
        query_heads = len(rows)
        require(query_heads % owner["kv_heads"] == 0 and
                rows == [(query_head, query_head // (query_heads // owner["kv_heads"]))
                         for query_head in range(query_heads)], "runtime consumer GQA strata mismatch")

    hashed = hashlib.sha256()
    hashed.update(RUNTIME_PROTOCOL_DOMAIN)
    hashed.update(_runtime_int(authority["schema_version"], 4))
    hashed.update((authority["query_sampling"] + "\x00").encode())
    hashed.update((authority["accounting"] + "\x00").encode())
    hashed.update(_runtime_int(authority["max_training_rows"], 4))
    hashed.update(_runtime_int(authority["max_validation_rows"], 4))
    hashed.update(_runtime_int(len(authority["sources"]), 1))
    for index, source in enumerate(authority["sources"]):
        hashed.update(bytes((index, RUNTIME_KIND_TAG[source["kind"]],
                             RUNTIME_PARTITION_TAG[source["partition"]], source["physical_branch"])))
    hashed.update(_runtime_int(layer, 4))
    hashed.update(_runtime_int(owner["kv_heads"], 4))
    hashed.update(_runtime_int(owner["query_heads"], 4))
    hashed.update(_runtime_int(owner["head_dim"], 4))
    for consumer_layer, query_head, kv_head in expected_strata:
        hashed.update(_runtime_int(consumer_layer, 4))
        hashed.update(_runtime_int(query_head, 4))
        hashed.update(_runtime_int(kv_head, 4))
    hashed.update(_runtime_int(len(expected_strata), 4))
    for name in ("fit", "gate"):
        study = authority[name]
        require(isinstance(study, dict) and set(study) ==
                {"template_sha256", "generation_contract_sha256", "response_tokens", "generation_mode"},
                "invalid runtime study authority")
        identity(study["template_sha256"], "runtime study template")
        identity(study["generation_contract_sha256"], "runtime generation contract")
        require(type(study["response_tokens"]) is int and study["response_tokens"] > 0 and
                study["generation_mode"] in RUNTIME_GENERATION_MODE_TAG, "invalid runtime generation authority")
        hashed.update(bytes.fromhex(study["template_sha256"]))
        hashed.update(bytes.fromhex(study["generation_contract_sha256"]))
        hashed.update(_runtime_int(study["response_tokens"], 4))
        hashed.update(_runtime_int(RUNTIME_GENERATION_MODE_TAG[study["generation_mode"]], 1))
    return hashed.hexdigest()


def runtime_protocol_authority(meta):
    """Reconstruct one production authority row from a sealed schema2 capture."""
    require(meta.get("schema_version") == 2 and meta.get("purpose") == "calibration",
            "runtime protocol requires schema2 calibration capture")
    c = meta["capture"]
    sources = [row for row in c.get("query_sources", []) if row.get("partition") != "evaluation"]
    require(len(sources) == len(RUNTIME_PROTOCOL_SOURCES), "runtime protocol requires exactly five sources")
    branch_ids = []
    for source, (_, kind, partition, branch) in zip(sources, RUNTIME_PROTOCOL_SOURCES):
        require(source.get("kind") == kind and source.get("partition") == partition,
                "runtime source order mismatch")
        require(source.get("private") is True and source.get("usage_visible") is False and
                source.get("selection_visible") is (partition == "training"),
                "runtime source visibility mismatch")
        require(source.get("capture_route") == "private_model_replay" and
                source.get("conditioning") in {"completed_prompt_private_replay",
                                                "completed_prompt_study_generated_response"},
                "runtime source execution mismatch")
        if kind != "request_question_replay":
            require(source.get("predeclared") is True, "runtime study source must be predeclared")
        branch_ids.append((branch, source.get("physical_branch_id")))
    grouped = {branch: {value for actual, value in branch_ids if actual == branch} for branch in range(3)}
    require(all(len(grouped[branch]) == 1 for branch in range(3)) and
            len({next(iter(grouped[branch])) for branch in range(3)}) == 3,
            "runtime physical branch grouping mismatch")

    question, fit_prefill, fit_response, gate_prefill, gate_response = sources
    require(question["logical_start"] == fit_prefill["logical_start"] == gate_prefill["logical_start"] and
            fit_prefill["logical_start"] + fit_prefill["logical_count"] == fit_response["logical_start"] and
            gate_prefill["logical_start"] + gate_prefill["logical_count"] == gate_response["logical_start"],
            "runtime study source adjacency mismatch")
    expected_branches = ((question,), (fit_prefill, fit_response), (gate_prefill, gate_response))
    for rows in expected_branches:
        start = rows[0]["logical_start"]
        end = rows[-1]["logical_start"] + rows[-1]["logical_count"]
        require(all(row.get("branch_logical_start") == start and
                    row.get("branch_logical_count") == end - start and
                    row.get("branch_rows_per_stratum") == rows[0].get("branch_rows_per_stratum")
                    for row in rows), "runtime physical branch bounds mismatch")

    def study(prefill, response):
        require(prefill.get("template_sha256") == response.get("template_sha256"),
                "runtime study template drift")
        require(prefill.get("study_pair_id") == response.get("study_pair_id") and
                response.get("generation_mode") == "greedy_fixed_length_ignore_eos",
                "runtime study pair mismatch")
        return {"template_sha256": prefill["template_sha256"],
                "generation_contract_sha256": response.get("generation_contract_sha256"),
                "response_tokens": response["logical_count"],
                "generation_mode": response.get("generation_mode")}

    layer = c["owner_layer"]
    consumers = sorted(c["consumers"], key=lambda row: row["layer"])
    owner_consumer = [row for row in consumers if row["layer"] == layer]
    require(len(owner_consumer) == 1, "runtime owner consumer missing")
    canonical_strata = []
    for consumer in consumers:
        query_heads = consumer["query_heads"]
        require(query_heads % c["heads"] == 0, "runtime consumer GQA mismatch")
        group = query_heads // c["heads"]
        canonical_strata.extend({"consumer_layer": consumer["layer"], "query_head": query_head,
                                 "kv_head": query_head // group} for query_head in range(query_heads))
    observed = [{key: row[key] for key in ("consumer_layer", "query_head", "kv_head")} for row in c["strata"]]
    key = lambda row: (row["consumer_layer"], row["query_head"], row["kv_head"])
    require({key(row) for row in observed} == {key(row) for row in canonical_strata} and
            len(observed) == len(canonical_strata), "runtime owner strata mismatch")
    authority = {"schema_version": RUNTIME_PROTOCOL_VERSION,
                 "layer": layer,
                 "query_sampling": "source_uniform_partition_v3",
                 "accounting": "serial_physical_branch_upper_bound_v2",
                 "max_training_rows": 3072,
                 "max_validation_rows": 1024,
                 "sources": [{"kind": kind, "partition": partition, "physical_branch": branch}
                             for _, kind, partition, branch in RUNTIME_PROTOCOL_SOURCES],
                 "owner": {"kv_heads": c["heads"], "query_heads": owner_consumer[0]["query_heads"],
                           "head_dim": c["dim"]},
                 "strata": canonical_strata,
                 "fit": study(fit_prefill, fit_response),
                 "gate": study(gate_prefill, gate_response)}
    return {**authority, "sha256": runtime_protocol_identity(authority)}


def profile_semantic_identity(owners, query_protocols, max_prompt_tokens, max_output_tokens, minimum_saved_bytes, engine="block_v1"):
    """Match compaction_profile.Profile.semanticDigest exactly."""
    def integer(value, size):
        require(type(value) is int and 0 <= value < 1 << (size * 8), "invalid profile semantic integer")
        return value.to_bytes(size, "little")
    hashed = hashlib.sha256()
    require(engine in {"block_v1", "native_whole_head_v2"}, "unknown attention matching engine")
    hashed.update(b"antfly.context-compaction.profile.v3\x00")
    hashed.update(integer(1, 4))
    hashed.update(integer({"block_v1": 0, "native_whole_head_v2": 1}[engine], 1))
    hashed.update(integer(max_prompt_tokens, 4))
    hashed.update(integer(max_output_tokens, 4))
    hashed.update(integer(minimum_saved_bytes, 8))
    hashed.update(integer(len(owners), 4))
    for owner in owners:
        hashed.update(integer(owner["layer"], 4))
        hashed.update(integer(len(owner["retained"]), 4))
        for rows in owner["retained"]:
            hashed.update(integer(rows, 4))
    hashed.update(integer(len(query_protocols), 4))
    for protocol in query_protocols:
        hashed.update(integer(protocol["schema_version"], 4))
        hashed.update(integer(protocol["layer"], 4))
        hashed.update(bytes.fromhex(protocol["sha256"]))
        hashed.update(integer(1, 1))
        hashed.update(integer(1, 1))
        hashed.update(integer(protocol["max_training_rows"], 4))
        hashed.update(integer(protocol["max_validation_rows"], 4))
        hashed.update(integer(len(protocol["sources"]), 1))
        for index, source in enumerate(protocol["sources"]):
            hashed.update(integer(index, 1))
            hashed.update(integer(RUNTIME_KIND_TAG[source["kind"]], 1))
            hashed.update(integer(RUNTIME_PARTITION_TAG[source["partition"]], 1))
            hashed.update(integer(source["physical_branch"], 1))
        hashed.update(integer(protocol["owner"]["kv_heads"], 4))
        hashed.update(integer(protocol["owner"]["query_heads"], 4))
        hashed.update(integer(protocol["owner"]["head_dim"], 4))
        hashed.update(integer(len(protocol["strata"]), 4))
        for stratum in protocol["strata"]:
            hashed.update(integer(stratum["consumer_layer"], 4))
            hashed.update(integer(stratum["query_head"], 4))
            hashed.update(integer(stratum["kv_head"], 4))
        for name in ("fit", "gate"):
            study = protocol[name]
            hashed.update(bytes.fromhex(study["template_sha256"]))
            hashed.update(bytes.fromhex(study["generation_contract_sha256"]))
            hashed.update(integer(study["response_tokens"], 4))
            hashed.update(integer(RUNTIME_GENERATION_MODE_TAG[study["generation_mode"]], 1))
    return hashed.hexdigest()


def derive_runtime_candidate(captures, profiles, allocation_page_bytes, minimum_saved_bytes,
                             split_decode, flash_prefill):
    """Derive the runtime view embedded in the exact frozen artifact.

    This is deliberately a projection of sealed calibration captures and the
    selected per-head profiles.  It is not a second profile file and does not
    grant serving authority; runtime and release both hash the complete outer
    artifact before consuming this section.
    """
    require(captures and profiles, "empty runtime candidate")
    require(type(allocation_page_bytes) is int and allocation_page_bytes >= 4096 and
            allocation_page_bytes <= 1024 * 1024 and allocation_page_bytes & (allocation_page_bytes - 1) == 0,
            "invalid runtime allocation page")
    require(type(minimum_saved_bytes) is int and minimum_saved_bytes > 0 and
            minimum_saved_bytes % allocation_page_bytes == 0,
            "runtime minimum saving must be positive and page granular")
    require(type(split_decode) is bool and type(flash_prefill) is bool and
            (split_decode or flash_prefill), "runtime candidate requires an optimized Metal route")
    metadata = list(captures.values())
    require(all(meta.get("schema_version") == 2 for meta in metadata),
            "runtime candidate requires schema2 captures")
    require(all(meta["capture"].get("runtime_block_coverage", {}).get("status") != "diagnostic_unavailable"
                for meta in metadata), "runtime candidate requires block coverage")
    model_ids = {(meta["model"]["sha256"], meta["model"]["tokenizer_sha256"],
                  meta["model"]["template_sha256"], meta["model"].get("layout_sha256"),
                  meta["model"].get("implementation_sha256")) for meta in metadata}
    require(len(model_ids) == 1, "runtime candidate requires one semantic identity")
    artifact, tokenizer, template, layout, implementation = next(iter(model_ids))
    for value, label in ((artifact, "artifact"), (tokenizer, "tokenizer"), (template, "template"),
                         (layout, "layout"), (implementation, "implementation")):
        identity(value, "runtime candidate " + label)
    protocols = {}
    for meta in metadata:
        layer = meta["capture"]["owner_layer"]
        authority = runtime_protocol_authority(meta)
        require(layer not in protocols or protocols[layer] == authority,
                "runtime candidate owner query protocol drift")
        protocols[layer] = authority
    coverage = [meta["capture"]["runtime_block_coverage"] for meta in metadata]
    for row in coverage:
        require(isinstance(row, dict) and set(row) == {"receipt_sha256", "prompt_tokens_sha256",
                "qualified_context_tokens", "prompt_tokens", "reserved_output_tokens"} and
                type(row["prompt_tokens"]) is int and row["prompt_tokens"] > 0 and
                type(row["reserved_output_tokens"]) is int and row["reserved_output_tokens"] > 0 and
                row["prompt_tokens"] + row["reserved_output_tokens"] == row["qualified_context_tokens"],
                "invalid runtime block coverage binding")
        identity(row["receipt_sha256"], "runtime block coverage receipt")
        identity(row["prompt_tokens_sha256"], "runtime prompt tokens")
    shapes = metadata[0]["model"]["global_owner_shapes"]
    require(all(meta["model"]["global_owner_shapes"] == shapes for meta in metadata),
            "runtime candidate layout drift")
    require(set(protocols) == {shape["layer"] for shape in shapes},
            "runtime candidate query protocols do not cover every owner")
    model_prefix = ":".join((artifact, tokenizer, template))
    owners = []
    previous_layer = -1
    for shape in shapes:
        layer, heads, dim = shape["layer"], shape["heads"], shape["dim"]
        require(type(layer) is int and layer > previous_layer and type(heads) is int and heads > 0 and
                type(dim) is int and dim > 0, "invalid runtime owner layout")
        retained = []
        for head in range(heads):
            key = f"{model_prefix}:{layer}:{head}"
            require(key in profiles, "runtime candidate lacks owner/head profile")
            chosen = profiles[key]
            require(chosen.get("recipe") == RUNTIME_PROFILE_RECIPE and
                    type(chosen.get("retained_rows")) is int and
                    0 < chosen["retained_rows"] < chosen.get("source_rows", 0) and
                    chosen.get("source_rows") == RUNTIME_PROFILE_IMPLEMENTATION["block_tokens"] and
                    chosen.get("head_dim") == dim and "retention_schedule" not in chosen,
                    "runtime candidate requires fixed supported RMS retention")
            retained.append(chosen["retained_rows"])
        owners.append({"layer": layer, "retained": retained})
        previous_layer = layer
    require(len(profiles) == sum(x["heads"] for x in shapes),
            "runtime candidate contains extra owner/head profile")
    max_prompt = max(row["prompt_tokens"] for row in coverage)
    max_output = max(row["reserved_output_tokens"] for row in coverage)
    return {
        "schema_version": 2,
        "identity": {"artifact_sha256": artifact, "tokenizer_sha256": tokenizer,
                     "template_sha256": template, "layout_sha256": layout,
                     "implementation_sha256": implementation},
        "query_protocols": [protocols[layer] for layer in sorted(protocols)],
        "implementation": {**RUNTIME_PROFILE_IMPLEMENTATION,
                           "split_decode": split_decode, "flash_prefill": flash_prefill,
                           "recipe": RUNTIME_PROFILE_RECIPE,
                           "solver_identity": RECIPE_IDENTITIES[RUNTIME_PROFILE_RECIPE]},
        "max_prompt_tokens": max_prompt,
        "max_output_tokens": max_output,
        "allocation_page_bytes": allocation_page_bytes,
        "minimum_saved_bytes": minimum_saved_bytes,
        "profile_semantic_sha256": profile_semantic_identity(
            owners, [protocols[layer] for layer in sorted(protocols)],
            max_prompt, max_output, minimum_saved_bytes),
        "owners": owners,
    }


def freeze(receipt, receipt_hash, adaptive=False, runtime_constraints=None):
    require(receipt["schema_version"] == SCHEMA and receipt["script_sha256"] == script_identity(), "calibrator/source mismatch")
    require(receipt["stage"] == "offline_numerical" and receipt["serving_qualified"] is False, "invalid numerical receipt")
    captures = {x["sha256"]: x["metadata"] for x in receipt["captures"]}
    require(captures and all(x["purpose"] == "calibration" for x in captures.values()), "only calibration documents may freeze a profile")
    for meta in captures.values():
        shape_contract(dict(meta, schema_version=meta.get("schema_version", SCHEMA)))
    validate_coverage(receipt["captures"])
    groups = {}
    for capture_id, meta in captures.items():
        for head in range(meta["capture"]["heads"]):
            key = f"{model_key(meta)}:{meta['capture']['owner_layer']}:{head}"
            groups.setdefault(key, set()).add(capture_id)
    profiles = {}
    for key, required in groups.items():
        metadata = [captures[x] for x in sorted(required)]
        contract = capture_contract(metadata[0])
        require(all(capture_contract(x) == contract for x in metadata), "mixed source/geometry contracts in calibration")
        rows = [x for x in receipt["results"] if x["capture_sha256"] in required and
                f"{model_key(captures[x['capture_sha256']])}:{x['owner_layer']}:{x['head']}" == key]
        candidates = {}
        for row in rows:
            candidates.setdefault((row["retained_rows"], row["recipe"]), []).append(row)
        passing = []
        for (retained, recipe), values in candidates.items():
            if len(values) == len(required) and {x["capture_sha256"] for x in values} == required and all(x["accepted"] for x in values):
                passing.append((max(x["retained_kv_bias_bytes"] for x in values), float(np.median([x["fit_seconds"] for x in values])), retained, recipe))
        require(passing, f"no candidate passes every calibration document for {key}")
        _, cost, retained, recipe = min(passing)
        profiles[key] = {"retained_rows": retained, "recipe": recipe, "calibration_median_fit_seconds": cost, **contract}
        if adaptive:
            # Freeze the retry rule before exposing evaluation documents. Only
            # increase retention, keep the selected solver, and preserve all
            # rejected attempts. Exact retention is an explicit final outcome.
            schedule = sorted({m for (m, r), values in candidates.items() if m >= retained and
                               (r == recipe or m == contract["source_rows"]) and
                               len(values) == len(required) and {x["capture_sha256"] for x in values} == required})
            require(schedule and schedule[-1] == contract["source_rows"], "adaptive profile requires exact calibration reference")
            profiles[key]["retention_schedule"] = schedule
    totals = {}
    for key, chosen in profiles.items():
        total = totals.setdefault(key.rsplit(":", 2)[0], [0, 0])
        total[0] += chosen["source_rows"] * chosen["head_dim"] * 4
        total[1] += chosen["retained_rows"] * (chosen["head_dim"] * 4 + (0 if chosen["retained_rows"] == chosen["source_rows"] else 4))
    require(all(saved < source for source, saved in totals.values()), "no nonidentity memory reduction for at least one model")
    frozen = {"schema_version": SCHEMA, "stage": "offline_numerical", "serving_qualified": False,
            "retention_policy": "increase_then_exact" if adaptive else "fixed",
            "script_sha256": script_identity(), "calibration_receipt_sha256": receipt_hash,
            "calibration_document_hashes": sorted({x["document"]["sha256"] for x in captures.values()}),
            "calibration_document_ids": sorted({x["document"]["id"] for x in captures.values()}),
            "calibration_query_hashes": sorted({x["query_sha256"] for x in receipt["captures"]}), "profiles": profiles}
    # Offline oracle/adaptive profiles remain useful diagnostics. Only the
    # fixed schema2 recipe implemented by the online Metal path gets a runtime
    # projection, and both consumers bind this same outer artifact's SHA256.
    if runtime_constraints is not None:
        require(isinstance(runtime_constraints, dict) and set(runtime_constraints) ==
                {"allocation_page_bytes", "minimum_saved_bytes", "split_decode", "flash_prefill"},
                "invalid runtime constraints")
        require(not adaptive and all(meta.get("schema_version") == 2 for meta in captures.values()) and
                {chosen["recipe"] for chosen in profiles.values()} == {RUNTIME_PROFILE_RECIPE},
                "runtime candidate requires fixed schema2 supported RMS profiles")
        frozen["runtime_candidate"] = derive_runtime_candidate(
            captures, profiles, runtime_constraints["allocation_page_bytes"],
            runtime_constraints["minimum_saved_bytes"], runtime_constraints["split_decode"],
            runtime_constraints["flash_prefill"])
    return frozen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pack = commands.add_parser("seal")
    pack.add_argument("--spec", type=Path, required=True)
    pack.add_argument("--output-dir", type=Path, required=True)
    sweep = commands.add_parser("sweep")
    sweep.add_argument("--capture", type=Path, action="append", required=True)
    sweep.add_argument("--retained-rows", type=int, nargs="+", default=BUDGETS)
    sweep.add_argument("--recipe", choices=RECIPES, action="append")
    sweep.add_argument("--max-fits", type=int, default=DEFAULT_MAX_FITS)
    sweep.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    sweep.add_argument("--max-working-set-mib", type=int, default=DEFAULT_MAX_WORKING_SET_BYTES // (1024 * 1024))
    sweep.add_argument("--output", type=Path, required=True)
    lock = commands.add_parser("freeze")
    lock.add_argument("--receipt", type=Path, required=True)
    lock.add_argument("--output", type=Path, required=True)
    lock.add_argument("--adaptive", action="store_true", help="freeze larger-budget retries and explicit exact fallback")
    lock.add_argument("--runtime-allocation-page-bytes", type=int,
                      help="destination allocation page size for an embedded runtime candidate")
    lock.add_argument("--runtime-minimum-saved-bytes", type=int,
                      help="positive page-granular transaction saving floor for an embedded runtime candidate")
    lock.add_argument("--runtime-split-decode", choices=(0, 1), type=int,
                      help="bind whether the qualified runtime candidate uses split decode")
    lock.add_argument("--runtime-flash-prefill", choices=(0, 1), type=int,
                      help="bind whether the qualified runtime candidate uses flash prefill")
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("--profile", type=Path, required=True)
    evaluate.add_argument("--capture", type=Path, action="append", required=True)
    evaluate.add_argument("--max-fits", type=int, default=DEFAULT_MAX_FITS)
    evaluate.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    evaluate.add_argument("--max-working-set-mib", type=int, default=DEFAULT_MAX_WORKING_SET_BYTES // (1024 * 1024))
    evaluate.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "seal":
        print(seal(args.spec, args.output_dir))
        return 0
    require(not args.output.exists(), "output already exists")
    if args.command == "freeze":
        supplied = tuple(value is not None for value in
                         (args.runtime_allocation_page_bytes, args.runtime_minimum_saved_bytes,
                          args.runtime_split_decode, args.runtime_flash_prefill))
        require(all(supplied) or not any(supplied), "runtime candidate constraints must be supplied together")
        constraints = ({"allocation_page_bytes": args.runtime_allocation_page_bytes,
                        "minimum_saved_bytes": args.runtime_minimum_saved_bytes,
                        "split_decode": bool(args.runtime_split_decode),
                        "flash_prefill": bool(args.runtime_flash_prefill)} if supplied[0] else None)
        write_json(args.output, freeze(read_json(args.receipt), digest(args.receipt), args.adaptive, constraints))
        return 0
    profile = None
    if args.command == "evaluate":
        profile = read_json(args.profile)
        require(profile["schema_version"] == SCHEMA and profile["script_sha256"] == script_identity() and profile["serving_qualified"] is False, "profile/source mismatch")
    limits = campaign_limits(args.max_fits, args.max_seconds, args.max_working_set_mib * 1024 * 1024)
    receipt = run_captures(args.capture, getattr(args, "retained_rows", ()), getattr(args, "recipe", None) or DEFAULT_RECIPES,
                           profile, limits)
    if profile is not None:
        receipt["profile_sha256"] = digest(args.profile)
    write_json(args.output, receipt)
    # Sweeps intentionally record rejected fits; a final frozen evaluation
    # must fail the process as well as the receipt if any consumer fails.
    return 2 if profile is not None and not (receipt["all_passed"] and receipt["all_models_reduce_kv_bytes"]) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f"attention calibration: {exc}", file=sys.stderr)
        sys.exit(2)
