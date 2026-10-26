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

"""Fail-closed preparation checks for the Gemma4 long-context quality corpus.

This tool validates frozen, source-backed construction evidence. It does not
select cases from model outputs and does not invent an accuracy threshold.
"""
import hashlib
import argparse
import json
import re
import struct
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import capture_attention_matching as capture
import release_attention_matching as release

MODELS = ("gemma4-e2b", "gemma4-e4b")
BUCKETS = ("code", "document_qa", "exact_retrieval", "multiturn", "structured_tool")
ROUTES = ("http", "native", "stream")
CASES_PER_CELL = 100
MAX_FILE_BYTES = 8 * 1024 * 1024


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(value, label):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid " + label)
    return value


def decode_json(raw, label):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "duplicate key in " + label)
            result[key] = value
        return result
    def reject_constant(value):
        raise ValueError("non-finite JSON number " + value)
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("invalid " + label) from exc


def campaign_file(root, ref, label, limit=MAX_FILE_BYTES):
    require(type(ref) is dict and set(ref) == {"path", "sha256"}, "invalid " + label + " reference")
    digest(ref["sha256"], label)
    relative = Path(ref["path"])
    require(not relative.is_absolute() and relative.parts and
            all(part not in ("", ".", "..") for part in relative.parts), "unsafe " + label + " path")
    path = Path(root).resolve()
    for part in relative.parts:
        path = path / part
        require(not path.is_symlink(), "symlinked " + label)
    require(path.is_file() and path.stat().st_size <= limit, "missing or oversized " + label)
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == ref["sha256"], label + " digest mismatch")
    return path, raw


def normalized(raw):
    return unicodedata.normalize("NFC", raw.decode("utf-8")).strip()


def u32_pairs(raw, count, label):
    require(len(raw) == count * 8, label + " count mismatch")
    result = [struct.unpack_from("<II", raw, index * 8) for index in range(count)]
    previous_start = previous_end = 0
    saw_mapped = False
    for start, end in result:
        require(start <= end, "invalid or non-monotonic " + label)
        if (start, end) != (0, 0):
            require(not saw_mapped or (start >= previous_start and end >= previous_end),
                    "invalid or non-monotonic " + label)
            saw_mapped = True
            previous_start, previous_end = start, end
    return result


def i64_tokens(raw, count, label):
    require(len(raw) == count * 8, label + " count mismatch")
    values = [struct.unpack_from("<q", raw, index * 8)[0] for index in range(count)]
    require(all(0 <= value <= 2**31 - 1 for value in values), "invalid " + label)
    return values


def validate_source_manifest(model, ref, root):
    _, raw = campaign_file(root, ref, model + " source manifest")
    value = decode_json(raw, model + " source manifest")
    exact = {"schema_version", "stage", "model", "selection_seed", "snapshot_sha256",
             "selection_source_families", "fitting_source_families", "calibration_source_families", "sources"}
    require(type(value) is dict and set(value) == exact and value["schema_version"] == 1 and
            value["stage"] == "quality_source_manifest" and value["model"] == model,
            "invalid quality source manifest")
    digest(value["selection_seed"], "selection seed")
    digest(value["snapshot_sha256"], "source snapshot")
    forbidden = set()
    for field in ("selection_source_families", "fitting_source_families", "calibration_source_families"):
        rows = value[field]
        require(type(rows) is list and all(isinstance(item, str) and item for item in rows) and
                len(rows) == len(set(rows)), "invalid " + field)
        forbidden.update(rows)
    sources = {}
    family_digests = set()
    normalized_digests = set()
    clone_clusters = set()
    exact_source = {"family_id", "family_sha256", "repository", "revision", "path", "sha256",
                    "license", "content_type", "authorship", "normalized_sha256",
                    "clone_cluster_id", "partition"}
    for source in value["sources"]:
        require(type(source) is dict and set(source) == exact_source, "invalid source row")
        family = source["family_id"]
        require(isinstance(family, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", family) and
                family not in sources and family not in forbidden, "invalid or overlapping source family")
        require(source["partition"] == "quality_evaluation" and
                source["authorship"] in {"human", "generated", "mixed"} and
                isinstance(source["clone_cluster_id"], str) and
                re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", source["clone_cluster_id"]) and
                source["clone_cluster_id"] not in clone_clusters,
                "invalid or duplicate declared source clone cluster")
        require(isinstance(source["repository"], str) and source["repository"] and
                isinstance(source["revision"], str) and re.fullmatch(r"[0-9a-f]{40,64}", source["revision"]) and
                isinstance(source["path"], str) and source["path"] and
                isinstance(source["license"], str) and source["license"] and
                source["content_type"] in {"code", "config", "markdown", "text"},
                "invalid source provenance")
        for field in ("family_sha256", "sha256", "normalized_sha256"):
            digest(source[field], "source " + field)
        _, source_raw = campaign_file(root, {"path": source["path"], "sha256": source["sha256"]},
                                      "source snapshot")
        try:
            normalized_sha256 = hashlib.sha256(normalized(source_raw).encode("utf-8")).hexdigest()
        except UnicodeDecodeError as exc:
            raise ValueError("source snapshot is not UTF-8") from exc
        require(source["normalized_sha256"] == normalized_sha256,
                "source normalized digest mismatch")
        require(source["family_sha256"] not in family_digests, "duplicate source family digest")
        require(source["normalized_sha256"] not in normalized_digests, "duplicate normalized source content")
        family_digests.add(source["family_sha256"])
        normalized_digests.add(source["normalized_sha256"])
        clone_clusters.add(source["clone_cluster_id"])
        sources[family] = source
    require(sources, "empty source manifest")
    return value, sources, hashlib.sha256(raw).hexdigest()


def validate_exclusion_manifests(model_row, source_manifest, root):
    refs = model_row["exclusion_manifests"]
    require(type(refs) is dict and set(refs) == {"selection", "fitting", "calibration"},
            "invalid exclusion manifests")
    excluded = set()
    for partition in ("selection", "fitting", "calibration"):
        _, raw = campaign_file(root, refs[partition], partition + " source-family manifest")
        value = decode_json(raw, partition + " source-family manifest")
        require(type(value) is dict and set(value) == {"schema_version", "stage", "model", "partition",
                "source_family_ids"} and value["schema_version"] == 1 and
                value["stage"] == "context_source_families" and value["model"] == model_row["model"] and
                value["partition"] == partition and type(value["source_family_ids"]) is list and
                value["source_family_ids"] == sorted(set(value["source_family_ids"])),
                "invalid " + partition + " source-family manifest")
        require(value["source_family_ids"] == source_manifest[partition + "_source_families"],
                partition + " source-family binding mismatch")
        require(not excluded.intersection(value["source_family_ids"]), "overlapping non-evaluation partitions")
        excluded.update(value["source_family_ids"])
    return excluded


def validate_construction(case, model, model_row, sources, manifest_sha, root):
    _, raw = campaign_file(root, case["construction_receipt"], "construction receipt")
    receipt = decode_json(raw, "construction receipt")
    exact = {"schema_version", "stage", "completed", "diagnostic_only", "case_id", "model",
             "source_manifest_sha256", "request_content_sha256", "tokenizer_sha256", "template_sha256",
             "rendered_prompt", "prompt_tokens", "token_offsets", "coverage_receipt", "reference_roundtrip",
             "evidence"}
    require(type(receipt) is dict and set(receipt) == exact and receipt["schema_version"] == 1 and
            receipt["stage"] == "quality_case_construction" and receipt["completed"] is True and
            receipt["diagnostic_only"] is False and receipt["case_id"] == case["id"] and
            receipt["model"] == model and receipt["source_manifest_sha256"] == manifest_sha and
            receipt["request_content_sha256"] == case["request_content_sha256"] and
            receipt["tokenizer_sha256"] == model_row["tokenizer_sha256"] and
            receipt["template_sha256"] == model_row["template_sha256"], "invalid construction receipt")
    _, rendered = campaign_file(root, receipt["rendered_prompt"], "rendered prompt")
    _, prompt_raw = campaign_file(root, receipt["prompt_tokens"], "prompt tokens")
    prompt_count = case["prompt_tokens"]
    prompt_ids = i64_tokens(prompt_raw, prompt_count, "prompt tokens")
    require(prompt_ids, "empty prompt tokens")
    _, offsets_raw = campaign_file(root, receipt["token_offsets"], "token offsets")
    offsets = u32_pairs(offsets_raw, prompt_count, "token offsets")
    require(all(end <= len(rendered) for _, end in offsets), "token offset exceeds rendered prompt")
    _, coverage_raw = campaign_file(root, receipt["coverage_receipt"], "coverage receipt")
    coverage = decode_json(coverage_raw, "coverage receipt")
    _, document = campaign_file(root, case["document"], "case document")
    _, question = campaign_file(root, case["question"], "case question")
    request_binding = {
        "session_id": coverage.get("request", {}).get("session_id"),
        "request_index": coverage.get("request", {}).get("request_index"),
        "question_sha256": hashlib.sha256(question).hexdigest(),
        "request_content_sha256": release._coverage_structured_request_identity(document, question),
    }
    require(case["request_content_sha256"] == request_binding["request_content_sha256"],
            "case structured request identity mismatch")
    derived_coverage = capture.validate_block_coverage(
        coverage,
        hashlib.sha256(prompt_raw).hexdigest(),
        request_binding=request_binding,
    )
    require(coverage.get("prompt_tokens") == prompt_count and
            coverage.get("rendered_prompt_sha256") == hashlib.sha256(rendered).hexdigest() and
            coverage.get("prompt_tokens_sha256") == hashlib.sha256(prompt_raw).hexdigest(),
            "construction does not bind actual renderer/tokenizer coverage")
    require(coverage.get("prompt_token_offsets_sha256") == hashlib.sha256(offsets_raw).hexdigest(),
            "construction does not bind runtime token offsets")
    require(coverage.get("document", {}).get("sha256") == case["document"]["sha256"] and
            coverage.get("loaded_identity", {}).get("tokenizer_sha256") == model_row["tokenizer_sha256"] and
            coverage.get("loaded_identity", {}).get("template_sha256") == model_row["template_sha256"] and
            derived_coverage["prompt_tokens"] == prompt_count,
            "coverage loaded/request identity mismatch")
    labels = coverage.get("eligible_token_labels")
    require(type(labels) is list and len(labels) == prompt_count and
            all(type(label) is int and 0 <= label <= 2**32 - 1 for label in labels),
            "invalid eligible token labels")
    policy = coverage.get("policy")
    require(type(policy) is dict and set(policy) == {"keep_prefix_tokens", "keep_recent_tokens",
            "block_tokens", "compaction_targets"} and policy["block_tokens"] == 512 and
            type(policy["compaction_targets"]) is list and policy["compaction_targets"],
            "invalid construction policy")
    prefix, recent = policy["keep_prefix_tokens"], policy["keep_recent_tokens"]
    require(type(prefix) is int and type(recent) is int and prefix >= 0 and recent >= 0 and
            prefix + recent < prompt_count, "invalid compactable interval")
    compact_start, compact_end = prefix, prompt_count - recent
    evidence = receipt["evidence"]
    require(type(evidence) is list and len(evidence) == 1, "quality case requires one answer-bearing evidence span")
    quartiles = []
    for item in evidence:
        exact_evidence = {"source_family_id", "source_byte_start", "source_byte_end", "rendered_byte_start",
                          "rendered_byte_end", "token_start", "token_end", "fact_sha256"}
        require(type(item) is dict and set(item) == exact_evidence, "invalid evidence row")
        family = item["source_family_id"]
        require(family == case["primary_source_family"] and family in sources, "evidence is not from primary source")
        source_path, source_raw = campaign_file(root, {"path": sources[family]["path"],
                                                       "sha256": sources[family]["sha256"]}, "evidence source")
        _ = source_path
        ss, se = item["source_byte_start"], item["source_byte_end"]
        rs, re_ = item["rendered_byte_start"], item["rendered_byte_end"]
        ts, te = item["token_start"], item["token_end"]
        require(all(type(value) is int for value in (ss, se, rs, re_, ts, te)) and
                0 <= ss < se <= len(source_raw) and 0 <= rs < re_ <= len(rendered) and
                compact_start <= ts < te <= compact_end, "invalid evidence range")
        fact = source_raw[ss:se]
        require(fact == rendered[rs:re_] and hashlib.sha256(fact).hexdigest() == item["fact_sha256"] and
                rendered.count(fact) == 1, "evidence is not uniquely source-backed in rendered prompt")
        intersecting = [index for index, (start, end) in enumerate(offsets)
                        if (start, end) != (0, 0) and start < re_ and end > rs]
        require(intersecting and (ts, te) == (intersecting[0], intersecting[-1] + 1),
                "evidence token interval is not the minimal runtime-offset enclosure")
        require(offsets[ts][0] <= rs and offsets[te - 1][1] >= re_ and
                all(labels[index] > 0 for index in range(ts, te)),
                "evidence tokens are not wholly compactable")
        midpoint = ((ts + te) / 2 - compact_start) / (compact_end - compact_start)
        quartiles.append(min(3, int(midpoint * 4)))
    roundtrip = receipt["reference_roundtrip"]
    require(type(roundtrip) is dict and set(roundtrip) == {"reference", "tokens", "decoded",
            "token_count", "tokenizer_sha256"} and roundtrip["tokenizer_sha256"] == model_row["tokenizer_sha256"],
            "invalid reference round-trip receipt")
    _, reference = campaign_file(root, roundtrip["reference"], "reference text")
    _, reference_tokens = campaign_file(root, roundtrip["tokens"], "reference tokens")
    _, decoded = campaign_file(root, roundtrip["decoded"], "decoded reference")
    count = roundtrip["token_count"]
    require(type(count) is int and 2 <= count <= case["reserved_output_tokens"] and
            len(reference_tokens) == count * 4 and reference == decoded,
            "reference text/token round-trip failed")
    ids = [struct.unpack_from("<I", reference_tokens, index * 4)[0] for index in range(count)]
    require(all(value <= 2**31 - 1 for value in ids), "invalid reference token id")
    return {"quartiles": quartiles, "rendered_sha256": hashlib.sha256(rendered).hexdigest(),
            "reference_sha256": roundtrip["reference"]["sha256"],
            "loaded_identity": coverage["loaded_identity"], "provenance": coverage["provenance"]}


def validate_campaign(manifest, root):
    exact = {"schema_version", "stage", "frozen_before_execution", "models"}
    require(type(manifest) is dict and set(manifest) == exact and manifest["schema_version"] == 1 and
            manifest["stage"] == "quality_corpus_campaign" and manifest["frozen_before_execution"] is True and
            type(manifest["models"]) is list and len(manifest["models"]) == 2,
            "invalid quality corpus campaign")
    require(tuple(sorted(row.get("model") for row in manifest["models"])) == MODELS,
            "campaign requires independent E2B and E4B plans")
    result = {}
    for row in manifest["models"]:
        exact_model = {"model", "tokenizer_sha256", "template_sha256", "qualified_context_tokens",
                       "source_manifest", "exclusion_manifests", "cases", "baseline_reporting"}
        require(type(row) is dict and set(row) == exact_model and
                row["baseline_reporting"] == "report_only_predeclared_no_filtering_v1",
                "invalid model corpus plan")
        model = row["model"]
        digest(row["tokenizer_sha256"], "tokenizer identity")
        digest(row["template_sha256"], "template identity")
        lengths = row["qualified_context_tokens"]
        require(type(lengths) is list and lengths == sorted(set(lengths)) and lengths and
                all(type(value) is int and value >= 1025 for value in lengths), "invalid qualified lengths")
        source_manifest, sources, manifest_sha = validate_source_manifest(model, row["source_manifest"], root)
        excluded = validate_exclusion_manifests(row, source_manifest, root)
        cases = row["cases"]
        require(type(cases) is list and len(cases) == len(lengths) * len(BUCKETS) * CASES_PER_CELL,
                "wrong quality case count")
        ids, families = set(), set()
        cell_families, cell_documents, cell_prompts = defaultdict(set), defaultdict(set), defaultdict(set)
        cells, routes, quartiles = Counter(), defaultdict(Counter), defaultdict(Counter)
        loaded_identities, provenances = set(), set()
        exact_case = {"id", "bucket", "route", "context_tokens", "prompt_tokens", "reserved_output_tokens",
                      "primary_source_family", "distractor_source_families", "document", "question",
                      "request_content_sha256", "construction_receipt"}
        for case in cases:
            require(type(case) is dict and set(case) == exact_case and case["id"] not in ids and
                    isinstance(case["id"], str) and case["bucket"] in BUCKETS and case["route"] in ROUTES,
                    "invalid or duplicate quality case")
            ids.add(case["id"])
            require(case["context_tokens"] in lengths and type(case["prompt_tokens"]) is int and
                    type(case["reserved_output_tokens"]) is int and case["reserved_output_tokens"] > 0 and
                    case["prompt_tokens"] + case["reserved_output_tokens"] == case["context_tokens"],
                    "quality case length mismatch")
            used = [case["primary_source_family"], *case["distractor_source_families"]]
            cell = (case["context_tokens"], case["bucket"])
            require(len(used) == len(set(used)) and all(family in sources for family in used) and
                    not cell_families[cell].intersection(used), "source family reused within quality cell")
            cell_families[cell].update(used)
            families.update(used)
            _, document = campaign_file(root, case["document"], "case document")
            _question_path, _question = campaign_file(root, case["question"], "case question")
            require(hashlib.sha256(document).hexdigest() not in cell_documents[cell],
                    "document reused within quality cell")
            cell_documents[cell].add(hashlib.sha256(document).hexdigest())
            require(hashlib.sha256(document).hexdigest() == case["document"]["sha256"], "document digest mismatch")
            digest(case["request_content_sha256"], "structured request")
            cells[cell] += 1
            routes[cell][case["route"]] += 1
            construction = validate_construction(case, model, row, sources, manifest_sha, root)
            loaded_identities.add(json.dumps(construction["loaded_identity"], sort_keys=True,
                                             separators=(",", ":")))
            provenances.add(json.dumps(construction["provenance"], sort_keys=True,
                                       separators=(",", ":")))
            require(construction["rendered_sha256"] not in cell_prompts[cell],
                    "rendered prompt reused within quality cell")
            cell_prompts[cell].add(construction["rendered_sha256"])
            for quartile in construction["quartiles"]:
                quartiles[cell][quartile] += 1
            # Repeated answers are legitimate (for example booleans, enum
            # values, or the same exact nonce in unrelated tasks). Source,
            # question, rendered prompt, and family bindings provide case
            # independence; reference uniqueness would reject valid cases.
        expected = {(length, bucket) for length in lengths for bucket in BUCKETS}
        require(set(cells) == expected and all(cells[cell] == CASES_PER_CELL for cell in expected),
                "quality cells are not exactly balanced")
        for cell in expected:
            expected_routes = sorted(CASES_PER_CELL // 3 + int(index < CASES_PER_CELL % 3)
                                     for index in range(3))
            require(set(routes[cell]) == set(ROUTES) and sorted(routes[cell].values()) == expected_routes,
                    "quality routes are not balanced")
            require(CASES_PER_CELL % 4 == 0 and
                    quartiles[cell] == Counter({index: CASES_PER_CELL // 4 for index in range(4)}),
                    "answer evidence is not quartile-balanced")
        require(not families.intersection(excluded), "quality evaluation overlaps selection/fitting/calibration")
        require(len(loaded_identities) == 1 and len(provenances) == 1,
                "quality model cases mix loaded artifact or build provenance")
        result[model] = {"cases": len(cases), "source_families": len(families), "cells": len(cells)}
    return result


def baseline_accuracy_report(cases, reports):
    """Report the frozen exact baseline without inventing a pass threshold."""
    by_id = {row.get("case_id"): row for row in reports}
    require(len(by_id) == len(reports) == len(cases), "baseline report set mismatch")
    cells = defaultdict(list)
    for case in cases:
        row = by_id.get(case["id"])
        require(type(row) is dict and row.get("variant") == "exact" and
                type(row.get("correct")) is bool, "invalid exact baseline report")
        cells[(case["context_tokens"], case["bucket"])].append(row)
    return {"schema_version": 1, "stage": "quality_exact_baseline_report",
            "acceptance_threshold": None, "case_filtering_permitted": False,
            "cells": [{"context_tokens": cell[0], "bucket": cell[1], "cases": len(rows),
                       "correct": sum(row["correct"] for row in rows),
                       "accuracy": sum(row["correct"] for row in rows) / len(rows),
                       "failed_case_ids": [row["case_id"] for row in rows if not row["correct"]]}
                      for cell, rows in sorted(cells.items())]}


def read_cli_json(path, label):
    value = Path(path)
    require(value.is_file() and not value.is_symlink() and value.stat().st_size <= MAX_FILE_BYTES,
            "missing or oversized " + label)
    return decode_json(value.read_bytes(), label)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("--campaign", required=True)
    validate.add_argument("--root", required=True)
    baseline = sub.add_parser("baseline-report")
    baseline.add_argument("--campaign", required=True)
    baseline.add_argument("--root", required=True)
    baseline.add_argument("--model", choices=MODELS, required=True)
    baseline.add_argument("--reports", required=True)
    args = parser.parse_args(argv)
    campaign = read_cli_json(args.campaign, "quality campaign")
    validated = validate_campaign(campaign, Path(args.root))
    if args.command == "validate":
        result = {"schema_version": 1, "stage": "quality_corpus_validation", "completed": True,
                  "models": validated}
    else:
        reports = read_cli_json(args.reports, "baseline reports")
        require(type(reports) is list, "baseline reports must be a list")
        model_row = next(row for row in campaign["models"] if row["model"] == args.model)
        result = baseline_accuracy_report(model_row["cases"], reports)
        result["model"] = args.model
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
