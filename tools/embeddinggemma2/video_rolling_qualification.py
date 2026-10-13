#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify real rolling/sparse live outputs and projection reuse across eviction.

Full rolling oracles cover every interval. Historical three-window oracles
remain supported with their explicit matching-subset scope.
"""

import argparse
import copy
import json
from pathlib import Path

import video_live_qualification
import compare
import video_compare


def validate(report, oracle, cache_policy="static_idr"):
    if cache_policy not in ("static_idr", "prefix"):
        raise ValueError("unknown projection cache policy")
    if report.get("status") != "pass" or len(report.get("cases", [])) != 8:
        raise ValueError("eight completed rolling/sparse outputs required")
    rolling = report["cases"][:6]
    sparse = report["cases"][6:]
    for rows, name, advance in (
        (rolling, "sintel-6s", 1_000_000_000),
        (sparse, "sintel-6s-sparse", 3_000_000_000),
    ):
        for ordinal, row in enumerate(rows):
            start = 200_000_000 + ordinal * advance
            end = min(start + 2_000_000_000, 6_200_000_000)
            tokens = 124 if end - start == 1_000_000_000 else 246
            if (
                row["case"] != name
                or row["ordinal"] != ordinal
                or row["epoch"] != 0
                or row["start_ns"] != start
                or row["end_ns"] != end
                or row["input_tokens"] != tokens
            ):
                raise ValueError("source interval, identity or token count changed")
            if row["vector"] != row["baseline_vector"] or row["max_abs"] != 0:
                raise ValueError("cached live/complete-clip vector drift")
            expected_hits = (
                ordinal if name == "sintel-6s" and cache_policy == "static_idr" else 0
            )
            expected_stored = (
                (
                    min(ordinal + 2, 6)
                    if cache_policy == "static_idr"
                    else min(2 * (ordinal + 1), 11)
                )
                if name == "sintel-6s"
                else 2 * (ordinal + 1)
            )
            if (
                row["live_cache_hits"] != expected_hits
                or row["live_cache_stored"] != expected_stored
            ):
                raise ValueError("rolling reuse/sparse cache isolation changed")
    if "documents" in oracle:
        result = full_reference(report, oracle)
    else:
        # Explicit verification view: preserve source times and vectors, renumber
        # only the three matched intervals to the original oracle's ordinals.
        subset = copy.deepcopy(report)
        subset["cases"] = [copy.deepcopy(rolling[i]) for i in (0, 2, 4)]
        for ordinal, row in enumerate(subset["cases"]):
            row["ordinal"] = ordinal
        result = video_live_qualification.validate(subset, oracle)
        result["scope"] = (
            "Pinned PyTorch gates cover rolling windows 0, 2, 4 matching the original "
            "non-overlapping intervals. All eight rolling/sparse outputs match "
            "complete-clip native baselines exactly. Five rolling cache hits survive "
            "preroll eviction; sparse intervals have no shared selected pictures."
        )
    if cache_policy == "prefix" and "documents" not in oracle:
        result["scope"] = (
            "Pinned PyTorch gates cover matching rolling windows 0, 2, 4 only. "
            "All eight native outputs match complete-clip baselines exactly. "
            "Conservative dynamic prefix keys record zero rolling cache hits."
        )
    result["cache_policy"] = cache_policy
    result["rolling_cache_hits"] = rolling[-1]["live_cache_hits"]
    result["rolling_cache_stored"] = rolling[-1]["live_cache_stored"]
    return result


def full_reference(report, oracle):
    if oracle.get("status") != "pass":
        raise ValueError("completed rolling oracle required")
    expected = {row["case"] + "/" + str(row["ordinal"]) for row in report["cases"]}
    documents = oracle["documents"]
    queries = report["queries"]
    query_ids = {row["id"] for row in queries}
    if (
        len(documents) != 8
        or {row["id"] for row in documents} != expected
        or len(queries) != 2
        or query_ids != {"dragon", "landscape"}
        or set(oracle["cases"]) != expected | query_ids
    ):
        raise ValueError("all rolling oracle identities required")
    policies = {row["id"]: row["sampling"] for row in documents}
    vectors = {}
    maximum = 0.0
    for row in report["cases"]:
        name = row["case"] + "/" + str(row["ordinal"])
        policy = {
            "policy": "timestamps",
            "start_ns": row["start_ns"] - 200_000_000,
            "end_ns": row["end_ns"] - 200_000_000,
        }
        if (
            policies[name] != policy
            or row["input_tokens"] != oracle["cases"][name]["encoder_tokens"]
        ):
            raise ValueError("rolling oracle sampling/token identity changed")
        metric = compare.assert_agreement(
            row["vector"], oracle["cases"][name]["vector"]
        )
        maximum = max(maximum, metric["max_abs"])
        vectors[name] = row["vector"]
    for row in queries:
        compare.assert_agreement(row["vector"], oracle["cases"][row["id"]]["vector"])
        vectors[row["id"]] = row["vector"]
    data = {"documents": documents, "queries": queries}
    actual = video_compare.retrieval(data, vectors)
    reference = video_compare.retrieval(
        data, {name: row["vector"] for name, row in oracle["cases"].items()}
    )
    agreement = video_compare.rank_agreement(actual, reference, (1, 3, 5, 8), 1e-5)
    if not agreement["pass"]:
        raise ValueError("full rolling reference ranking/score drift")
    return {
        "max_reference_abs": maximum,
        "retrieval": actual,
        "reference_retrieval": reference,
        "stream_baseline_exact": True,
        "retrieval_agreement": agreement,
        "scope": "Pinned PyTorch gates cover all six rolling and two sparse intervals, including the clipped tail. All native outputs also match complete-clip baselines exactly.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--cache-policy", choices=("static_idr", "prefix"), default="static_idr"
    )
    args = parser.parse_args()
    result = validate(
        json.loads(args.report.read_text()),
        json.loads(args.reference.read_text()),
        args.cache_policy,
    )
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
