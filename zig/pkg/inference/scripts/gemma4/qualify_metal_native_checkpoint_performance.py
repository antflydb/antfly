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

"""Matched Metal full-replay versus portable-checkpoint observations.

This collector is test-ingress evidence.  Its paired comparison restores an
unchanged exact f16 checkpoint and a fitted f16/FP8 checkpoint at the same
logical prefix. Full replay is recorded separately and is never used as the
decode denominator.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import http.client
import json
import math
from pathlib import Path
import random
import statistics
import sys
import time
import itertools

BASE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(BASE))

from native_checkpoint_transport import send_resume
from metal_checkpoint_qualification_ledger import (
    generation_contract as ledger_contract, wait_ledger,
)
from qualify_context_management import git_provenance, sha256_file
from qualify_native_checkpoint_http import (
    normalize_request_body, prepare, response_headers, server, validate_backend_configuration,
    validate_metal_memory,
)

SCHEMA = "antfly.metal-native-checkpoint.matched-performance.v1"
MIN_WARMUPS = 5
MIN_PAIRS = 20
BOOTSTRAP_SAMPLES = 10_000
PORTABLE_IDENTITY_FIELDS = {"weights", "tokenizer", "template", "layout"}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def _portable_identity(value):
    return (type(value) is dict and set(value) == PORTABLE_IDENTITY_FIELDS and
            all(isinstance(item, str) and len(item) == 64 and
                all(character in "0123456789abcdef" for character in item)
                for item in value.values()))


def _prepared_result(result, expected_path, expected_identity, label):
    require(type(result) is dict and type(result.get("metadata")) is dict and
            set(("bytes", "file_sha256", "identity", "metadata")) <= set(result),
            "incomplete producer " + label)
    path = Path(expected_path)
    require(path.is_absolute() and path.is_file() and not path.is_symlink(),
            "invalid producer " + label + " path")
    size = path.stat().st_size
    require(type(result["bytes"]) is int and result["bytes"] == size and
            result["metadata"].get("checkpoint_bytes") == size and
            result["file_sha256"] == sha256_file(path) and
            result["identity"] == expected_identity,
            "producer " + label + " artifact changed")
    return {
        "path": str(path),
        "sha256": result["file_sha256"],
        "bytes": size,
        "identity": result["identity"],
        "metadata": result["metadata"],
    }


def load_prepared_evidence(args, request_path, normalized_request_sha256):
    """Validate immutable smoke-only producer evidence and its two checkpoints."""
    path = args.prepared_evidence.resolve(strict=True)
    require(path.is_file() and not path.is_symlink(), "invalid prepared evidence path")
    producer = json.loads(path.read_bytes())
    require(type(producer) is dict and
            producer.get("schema") == SCHEMA and
            producer.get("backend") == "metal" and
            producer.get("server_kind") == "metal-test" and
            producer.get("qualification_profile") == "metal_test_native_checkpoint_v1" and
            producer.get("production_ready") is False and
            producer.get("passed") is False and
            producer.get("qualification_sized") is False and
            producer.get("performance_qualified") is False and
            producer.get("encoding") == args.encoding and
            producer.get("declared_context_tokens") == args.declared_context_tokens,
            "prepared evidence is not a compatible diagnostic producer")
    require(producer.get("request") == {
                "path": str(request_path), "sha256": sha256_file(request_path)},
            "producer request identity changed")
    require(producer.get("normalized_request_sha256") == normalized_request_sha256,
            "producer normalized request changed")

    worker = producer.get("worker")
    ready = worker.get("ready") if type(worker) is dict else None
    identity = ready.get("loaded_identity") if type(ready) is dict else None
    require(type(ready) is dict and ready.get("backend") == "metal" and
            ready.get("metal_enabled") is True and ready.get("cuda_enabled") is False and
            ready.get("model") == args.model and _portable_identity(identity),
            "invalid producer loaded-model identity")
    ready_path = path.with_suffix(".worker.ready.json")
    require(ready_path.is_file() and not ready_path.is_symlink() and
            sha256_file(ready_path) == worker.get("ready_sha256") and
            json.loads(ready_path.read_bytes()) == ready,
            "producer ready attestation changed")

    command = worker.get("command")
    require(isinstance(command, list) and len(command) == 3 and
            command[1:] == ["--test-filter",
                            "context Metal native checkpoint qualification server"],
            "producer did not use the Metal test server")
    candidate_path = args.checkpoint.resolve(strict=True)
    exact_path = candidate_path.with_name(candidate_path.name + ".exact.kv")

    producer_server = Path(command[0]).resolve(strict=True)
    require(producer_server.is_file() and not producer_server.is_symlink() and
            sha256_file(producer_server) == producer.get("binary_sha256"),
            "producer binary identity changed")
    source = producer.get("source")
    provenance_digests = ("source_snapshot_sha256", "status_sha256",
                          "tracked_diff_sha256", "untracked_content_sha256")
    require(type(source) is dict and type(source.get("dirty")) is bool and
            isinstance(source.get("head"), str) and len(source["head"]) in (40, 64) and
            all(character in "0123456789abcdef" for character in source["head"]) and
            all(isinstance(source.get(field), str) and len(source[field]) == 64 and
                all(character in "0123456789abcdef" for character in source[field])
                for field in provenance_digests) and
            isinstance(producer.get("runner_sha256"), str) and
            len(producer["runner_sha256"]) == 64 and
            all(character in "0123456789abcdef" for character in producer["runner_sha256"]),
            "invalid producer source identity")

    requests = worker.get("requests")
    prepares = [request for request in requests if request.get("kind") == "prepare"] \
        if isinstance(requests, list) else []
    require(len(prepares) == 2, "producer must contain exactly two completed prepares")
    exact = _prepared_result(prepares[0].get("result"), exact_path, identity,
                             "exact checkpoint")
    candidate = _prepared_result(prepares[1].get("result"), candidate_path, identity,
                                 "compacted checkpoint")
    exact_meta, metadata = exact["metadata"], candidate["metadata"]
    require(exact_meta.get("model") == metadata.get("model") == args.model and
            exact_meta.get("encoding") == "f16" and
            exact_meta.get("outcome") == "unchanged_insufficient_savings" and
            exact_meta.get("global_savings_ratio") == 1 and
            exact_meta.get("validated_heads") == 0 and
            metadata.get("encoding") == args.encoding and
            metadata.get("outcome") == "compacted" and
            type(metadata.get("global_savings_ratio")) in (int, float) and
            math.isfinite(metadata["global_savings_ratio"]) and
            metadata["global_savings_ratio"] >= args.minimum_savings and
            exact_meta.get("logical_tokens") == metadata.get("logical_tokens") and
            metadata.get("logical_tokens") + args.reserved_output_tokens ==
                args.declared_context_tokens,
            "producer checkpoint metadata does not match the release cell")
    for field in ("validated_heads", "preserved_heads", "fp8_heads",
                  "fp8_fallback_heads"):
        require(type(metadata.get(field)) is int and metadata[field] >= 0,
                "producer checkpoint omitted " + field)
    require(args.encoding != "fp8" or metadata["fp8_heads"] > 0,
            "producer FP8 checkpoint has no encoded heads")

    exact_summary = {key: exact_meta.get(key) for key in (
        "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
        "global_savings_ratio", "validated_heads")}
    candidate_summary = {key: metadata.get(key) for key in (
        "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
        "global_savings_ratio", "validated_heads", "preserved_heads",
        "fp8_heads", "fp8_fallback_heads")}
    require(producer.get("exact_control") == exact_summary and
            producer.get("physical_ledger") == candidate_summary,
            "producer checkpoint summary differs from prepare receipts")
    return {
        "producer_evidence": {"path": str(path), "sha256": sha256_file(path)},
        "producer_passed": producer.get("passed") is True,
        "producer_error": producer.get("error"),
        "producer_binary": {"path": str(producer_server),
                            "sha256": producer["binary_sha256"]},
        "producer_runner_sha256": producer["runner_sha256"],
        "producer_source": source,
        "producer_ready": ready,
        "exact": exact,
        "candidate": candidate,
        "preparation_seconds": producer.get("preparation_seconds"),
    }


def paired_schedule(pairs):
    require(type(pairs) is int and pairs > 0, "positive pair count required")
    orders = tuple(itertools.permutations(("ordinary", "exact", "compacted")))
    return [orders[index % len(orders)] for index in range(pairs)]


def percentile(values, quantile):
    ordered = sorted(values)
    require(ordered and 0 <= quantile <= 1 and
            all(math.isfinite(value) and value > 0 for value in ordered),
            "invalid positive sample")
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def finite_percentile(values, quantile):
    ordered = sorted(values)
    require(ordered and 0 <= quantile <= 1 and
            all(math.isfinite(value) for value in ordered), "invalid finite sample")
    position = (len(ordered) - 1) * quantile
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def paired_ratio_interval(pairs, field, seed, samples=BOOTSTRAP_SAMPLES):
    """Deterministic paired bootstrap of compacted/exact restore ratios."""
    ratios = []
    for pair in pairs:
        exact, compacted = pair["exact"][field], pair["compacted"][field]
        require(all(type(value) in (int, float) and math.isfinite(value) and value > 0
                    for value in (exact, compacted)), "invalid paired timing")
        ratios.append(compacted / exact)
    require(ratios, "empty paired timing")
    rng = random.Random(seed)
    medians = []
    for _ in range(samples):
        medians.append(statistics.median(ratios[rng.randrange(len(ratios))]
                                         for _ in ratios))
    return {"median": statistics.median(ratios),
            "lower_95": percentile(medians, .025),
            "upper_95": percentile(medians, .975),
            "pairs": len(ratios), "bootstrap_samples": samples,
            "seed": seed}


def paired_transform_interval(pairs, transform, seed, samples=BOOTSTRAP_SAMPLES):
    values = [transform(pair) for pair in pairs]
    require(values and all(math.isfinite(value) for value in values),
            "invalid paired transform")
    rng = random.Random(seed)
    medians = [statistics.median(values[rng.randrange(len(values))]
                                 for _ in values) for _ in range(samples)]
    return {"median": statistics.median(values),
            "lower_95": finite_percentile(medians, .025),
            "upper_95": finite_percentile(medians, .975),
            "pairs": len(values), "bootstrap_samples": samples, "seed": seed}


def matched_output_identity(observations):
    """Require every timed route to produce the same greedy public output."""
    return bool(observations) and all(
        pair.get("same_greedy_output") is True and
        pair.get("ordinary_same_greedy_output") is True
        for pair in observations
    )


def _event_delta(event):
    choices = event.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return "", None
    choice = choices[0]
    delta = choice.get("delta")
    content = delta.get("content") if isinstance(delta, dict) else None
    return (content if isinstance(content, str) else ""), choice.get("finish_reason")


def timed_stream(response, started_ns, upload_complete_ns=None):
    first_ns = last_content_ns = terminal_usage_ns = None
    fragments, events = [], []
    finish_reason = None
    for raw in response:
        line = raw.decode("utf-8", errors="strict").strip()
        if not line or line.startswith(":") or line.startswith("event:"):
            continue
        require(line.startswith("data:"), "invalid SSE line")
        payload = line[5:].strip()
        if payload == "[DONE]":
            break
        event = json.loads(payload)
        require(isinstance(event, dict) and not event.get("error"), "SSE error")
        now = time.monotonic_ns()
        events.append(event)
        content, ending = _event_delta(event)
        if content:
            if first_ns is None:
                first_ns = now
            last_content_ns = now
            fragments.append(content)
        if isinstance(event.get("usage"), dict) and \
                event["usage"].get("completion_tokens") is not None:
            terminal_usage_ns = now
        if ending is not None:
            finish_reason = ending
    finished_ns = time.monotonic_ns()
    require(first_ns is not None and last_content_ns is not None,
            "stream emitted no visible content")
    usage = next((event["usage"] for event in reversed(events)
                  if isinstance(event.get("usage"), dict) and
                  event["usage"].get("completion_tokens") is not None), None)
    require(isinstance(usage, dict) and type(usage.get("prompt_tokens")) is int and
            type(usage.get("completion_tokens")) is int and
            usage["completion_tokens"] > 1, "missing terminal token accounting")
    decode_end = terminal_usage_ns or last_content_ns
    content = "".join(fragments).encode()
    if upload_complete_ns is not None:
        require(started_ns <= upload_complete_ns <= first_ns,
                "checkpoint upload timing is outside request TTFT")
    return {"ttft_ms": (first_ns - started_ns) / 1e6,
            "wire_send_ms": ((upload_complete_ns - started_ns) / 1e6
                             if upload_complete_ns is not None else None),
            "post_upload_to_first_token_ms":
                ((first_ns - upload_complete_ns) / 1e6
                 if upload_complete_ns is not None else None),
            "decode_ms": (decode_end - first_ns) / 1e6,
            "e2e_ms": (finished_ns - started_ns) / 1e6,
            "completion_tokens": usage["completion_tokens"],
            "prompt_tokens": usage["prompt_tokens"],
            "cached_prompt_tokens": usage.get("cached_prompt_tokens", 0),
            "finish_reason": finish_reason,
            "content_sha256": hashlib.sha256(content).hexdigest()}


def observe(args, body, record, mode, checkpoint=None):
    log_path = args.output.with_suffix(".worker.log")
    log_offset = log_path.stat().st_size
    connection = http.client.HTTPConnection("127.0.0.1", args.port,
                                            timeout=args.timeout)
    upload_complete_ns = None
    def upload_complete():
        nonlocal upload_complete_ns
        require(upload_complete_ns is None, "checkpoint upload callback repeated")
        upload_complete_ns = time.monotonic_ns()
    try:
        started = time.monotonic_ns()
        if checkpoint is not None:
            response = send_resume(connection, "/ai/v1/generate/context/resume",
                                   body, checkpoint,
                                   on_upload_complete=upload_complete)
        else:
            connection.request("POST", "/ai/v1/generate", json.dumps(body).encode(),
                               {"Content-Type": "application/json",
                                "Accept": "text/event-stream"})
            response = connection.getresponse()
        require(response.status == 200, f"{mode} HTTP {response.status}")
        response_headers(response, record, args)
        require(response.getheader("Content-Type", "").split(";")[0] ==
                "text/event-stream", "missing SSE response")
        result = timed_stream(response, started, upload_complete_ns)
    finally:
        connection.close()
    route = "checkpoint_resume" if checkpoint is not None else "exact"
    attempt = {
        "attempt_index": len(record.setdefault("performance_observations", [])),
        "mode": mode,
        "route": route,
        "checkpoint": (str(Path(checkpoint).resolve())
                       if checkpoint is not None else None),
        "log_offset": log_offset,
        "observation": copy.deepcopy(result),
        "validated": False,
    }
    record["performance_observations"].append(attempt)
    try:
        rows = wait_ledger(log_path, log_offset)
        attempt["ledger_rows"] = rows
        result["owner_ledger"] = ledger_contract(
            rows, route, require_binding=checkpoint is not None,
            expected_representation=(None if checkpoint is None else
                                     "f16" if mode == "exact" else args.encoding))
    except Exception as exc:
        attempt["validation_error"] = f"{type(exc).__name__}: {exc}"
        raise
    attempt["owner_ledger"] = result["owner_ledger"]
    attempt["validated"] = True
    return result


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".part")
    with temporary.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["metal"], required=True)
    parser.add_argument("--server-kind", choices=["metal-test"], required=True)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--declared-context-tokens", type=int,
                        choices=[8192, 32768], required=True)
    parser.add_argument("--encoding", choices=["f16", "fp8"], required=True)
    parser.add_argument("--warmups", type=int, default=MIN_WARMUPS)
    parser.add_argument("--pairs", type=int, default=MIN_PAIRS)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--minimum-savings", type=float, default=1.5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument(
        "--prepared-evidence", type=Path,
        help="Smoke-only failed producer receipt whose completed checkpoints are reused",
    )
    args = parser.parse_args(argv)
    require(args.prepared_evidence is None or args.smoke,
            "--prepared-evidence is diagnostic-only and requires --smoke")
    require(args.smoke or args.warmups >= MIN_WARMUPS and args.pairs >= MIN_PAIRS,
            "qualification requires at least five warmups and twenty pairs")
    require(args.output.is_absolute() and args.checkpoint.is_absolute(),
            "outputs must be absolute")
    require(not args.output.exists(), "exclusive output already exists")
    require((args.prepared_evidence is not None) == args.checkpoint.exists(),
            "checkpoint must be absent for fresh preparation and present for reuse")
    args.server = args.server.resolve(strict=True)
    args.models_dir = args.models_dir.resolve(strict=True)
    args.fixture = "fact"
    args.expected_outcome = "compacted"
    args.minimum_fp8_heads = 1 if args.encoding == "fp8" else 0
    args.experimental_rotated4 = False
    args.generation_timing = False
    validate_backend_configuration(args)
    source = git_provenance(REPO)
    result = {"schema": SCHEMA, "passed": False, "production_ready": False,
              "backend": "metal", "server_kind": "metal-test",
              "encoding": args.encoding,
              "declared_context_tokens": args.declared_context_tokens,
              "warmups": args.warmups, "pairs_requested": args.pairs,
              "qualification_sized": not args.smoke,
              "performance_qualified": False,
              "qualification_profile": "metal_test_native_checkpoint_v1",
              "timing_scope": (
                  "matched exact-checkpoint versus compacted-checkpoint restore, plus "
                  "ordinary full-KV decode; ordinary TTFT/E2E use different ingestion"),
              "binary_sha256": sha256_file(args.server), "source": source,
              "runner_sha256": sha256_file(Path(__file__)), "observations": []}
    try:
        request_path = args.request.resolve(strict=True)
        body = json.loads(request_path.read_bytes())
        require(type(body) is dict and body.get("model") == args.model and
                body.get("temperature") == 0 and
                body.get("prompt_cache") in (None, False) and
                body.get("cache_dtype") in (None, "f16") and
                body.get("stream") is True and
                "context_management" not in body,
                "invalid frozen public performance request")
        body = normalize_request_body(args, body)
        result["request"] = {"path": str(request_path),
                             "sha256": sha256_file(request_path)}
        result["execution_mode"] = body.get("mode")
        result["normalized_request_sha256"] = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":"),
                       allow_nan=False).encode()).hexdigest()
        stream_body = copy.deepcopy(body)
        stream_body["stream"] = True
        reserved_output_tokens = stream_body.get("max_tokens")
        require(type(reserved_output_tokens) is int and reserved_output_tokens > 1,
                "performance workload lacks fixed output reservation")
        args.reserved_output_tokens = reserved_output_tokens
        declared_context_tokens = args.declared_context_tokens
        result["release_cell"] = {
            "declared_context_tokens": declared_context_tokens,
            "reserved_output_tokens": reserved_output_tokens,
            "rendered_prompt_tokens": None,
            "context_cell_matched": False,
            "output_reservation_filled": False,
        }
        exact_checkpoint = args.checkpoint.with_name(args.checkpoint.name + ".exact.kv")
        require((args.prepared_evidence is not None) == exact_checkpoint.exists(),
                "exact checkpoint must be absent for fresh preparation and present for reuse")
        reused = (load_prepared_evidence(
            args, request_path, result["normalized_request_sha256"]
        ) if args.prepared_evidence is not None else None)
        if reused is not None:
            result["prepared_reuse"] = {
                **{key: reused[key] for key in (
                    "producer_evidence", "producer_passed", "producer_error",
                    "producer_binary", "producer_runner_sha256", "producer_source",
                    "producer_ready", "preparation_seconds")},
                "exact": reused["exact"],
                "candidate": reused["candidate"],
                "diagnostic_only": True,
                "formal_performance_qualified": False,
            }
        with server(args, "worker", result) as record:
            candidate_checkpoint = args.checkpoint
            if reused is None:
                # Four is the public API maximum. On this workload it must preserve
                # the original rather than fitting; anything else aborts the pair.
                args.checkpoint = exact_checkpoint
                args.encoding = "f16"
                args.expected_outcome = "unchanged_insufficient_savings"
                args.minimum_savings = 4.0
                args.qualification_minimum_savings = 4.0
                args.minimum_fp8_heads = 0
                exact_prepared = prepare(args, body, record)
                args.checkpoint = candidate_checkpoint
                args.encoding = result["encoding"]
                args.expected_outcome = "compacted"
                args.minimum_savings = 1.5
                args.qualification_minimum_savings = 1.5
                args.minimum_fp8_heads = 1 if args.encoding == "fp8" else 0
                prepared = prepare(args, body, record)
                preparation_seconds = {
                    "exact": record["requests"][-2]["seconds"],
                    "compacted": record["requests"][-1]["seconds"],
                }
            else:
                exact_prepared = reused["exact"]
                prepared = reused["candidate"]
                preparation_seconds = reused["preparation_seconds"]
                require(record["ready"]["loaded_identity"] ==
                        reused["producer_ready"]["loaded_identity"],
                        "consumer loaded-model identity differs from producer")
            exact_meta = exact_prepared["metadata"]
            metadata = prepared["metadata"]
            require(exact_meta.get("outcome") == "unchanged_insufficient_savings" and
                    exact_meta.get("global_savings_ratio") == 1 and
                    exact_meta.get("validated_heads") == 0,
                    "exact checkpoint control unexpectedly fitted heads")
            require(metadata.get("outcome") == "compacted" and
                    metadata.get("global_savings_ratio", 0) >= args.minimum_savings and
                    exact_meta.get("logical_tokens") == metadata.get("logical_tokens"),
                    "checkpoint lacks matched physical savings proof")
            result["exact_control"] = {key: exact_meta.get(key) for key in (
                "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
                "global_savings_ratio", "validated_heads")}
            result["physical_ledger"] = {
                key: metadata.get(key) for key in (
                    "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
                    "global_savings_ratio", "validated_heads", "preserved_heads",
                    "fp8_heads", "fp8_fallback_heads")}
            result["physical_ledger_source"] = (
                "public checkpoint metadata computed from backend allocation accounting")
            result["total_committed_context_qualified"] = False
            result["minimum_savings_controls"] = {"exact": 4.0, "compacted": 1.5}
            result["preparation_seconds"] = preparation_seconds
            result["preparation_reused"] = reused is not None
            # Retain a cold ordinary replay separately from all warm/measured rows.
            result["full_replay_cold"] = observe(args, stream_body, record,
                                                  "full_replay")
            cold = result["full_replay_cold"]
            result["release_cell"]["rendered_prompt_tokens"] = cold["prompt_tokens"]
            result["release_cell"]["context_cell_matched"] = (
                cold["prompt_tokens"] + reserved_output_tokens == declared_context_tokens)
            result["release_cell"]["output_reservation_filled"] = (
                cold["completion_tokens"] == reserved_output_tokens and
                cold["finish_reason"] == "length")
            require(result["release_cell"]["context_cell_matched"],
                    "rendered prompt plus reservation misses declared release cell")
            require(result["release_cell"]["output_reservation_filled"],
                    "workload stopped before its output reservation; calibrate fixture")
            # Every route receives the same count of untimed warmups.
            for _ in range(args.warmups):
                observe(args, stream_body, record, "ordinary")
                observe(args, stream_body, record, "exact", exact_checkpoint)
                observe(args, stream_body, record, "compacted", candidate_checkpoint)
            for index, order in enumerate(paired_schedule(args.pairs)):
                pair = {"index": index, "order": list(order)}
                for mode in order:
                    selected = (exact_checkpoint if mode == "exact" else
                                candidate_checkpoint if mode == "compacted" else None)
                    pair[mode] = observe(args, stream_body, record, mode, selected)
                ordinary, exact, compacted = pair["ordinary"], pair["exact"], pair["compacted"]
                require(exact["completion_tokens"] == compacted["completion_tokens"] and
                        ordinary["completion_tokens"] == exact["completion_tokens"] and
                        exact["completion_tokens"] == reserved_output_tokens and
                        ordinary["finish_reason"] == exact["finish_reason"] ==
                            compacted["finish_reason"] == "length" and
                        ordinary["prompt_tokens"] == exact["prompt_tokens"] ==
                            compacted["prompt_tokens"] and
                        ordinary["cached_prompt_tokens"] == 0 and
                        exact["cached_prompt_tokens"] == exact_meta["logical_tokens"] and
                        compacted["cached_prompt_tokens"] == metadata["logical_tokens"],
                        "matched token accounting differs")
                pair["same_greedy_output"] = exact["content_sha256"] == compacted["content_sha256"]
                pair["ordinary_same_greedy_output"] = (
                    ordinary["content_sha256"] == compacted["content_sha256"])
                result["observations"].append(pair)
        validate_metal_memory(result["worker"]["memory"], 8 * 1024**3)
        if reused is not None:
            require(
                sha256_file(Path(reused["producer_evidence"]["path"])) ==
                    reused["producer_evidence"]["sha256"] and
                sha256_file(Path(reused["producer_binary"]["path"])) ==
                    reused["producer_binary"]["sha256"] and
                all(Path(reused[key]["path"]).stat().st_size == reused[key]["bytes"] and
                    sha256_file(Path(reused[key]["path"])) == reused[key]["sha256"]
                    for key in ("exact", "candidate")),
                "prepared producer evidence changed during consumer run",
            )
            result["prepared_reuse"]["consumer_reverified"] = True
        require(git_provenance(REPO) == source and
                sha256_file(args.server) == result["binary_sha256"] and
                sha256_file(request_path) == result["request"]["sha256"],
                "source, binary, or request changed during measurements")
        result["confidence"] = {
            metric: paired_ratio_interval(result["observations"], metric,
                                           f"{args.encoding}:{declared_context_tokens}:{metric}")
            for metric in ("ttft_ms", "wire_send_ms",
                           "post_upload_to_first_token_ms", "decode_ms", "e2e_ms")}
        result["confidence"]["decode_throughput_ratio"] = paired_transform_interval(
            result["observations"],
            lambda pair: pair["exact"]["decode_ms"] / pair["compacted"]["decode_ms"],
            f"{args.encoding}:{declared_context_tokens}:decode-throughput")
        result["confidence"]["ordinary_decode_throughput_ratio"] = paired_transform_interval(
            result["observations"],
            lambda pair: pair["ordinary"]["decode_ms"] /
            pair["compacted"]["decode_ms"],
            f"{args.encoding}:{declared_context_tokens}:ordinary-decode-throughput")
        result["diagnostic_different_ingestion"] = {
            metric: paired_transform_interval(
                result["observations"],
                lambda pair, field=metric: pair["compacted"][field] /
                pair["ordinary"][field],
                f"{args.encoding}:{declared_context_tokens}:ordinary-{metric}")
            for metric in ("ttft_ms", "e2e_ms")}
        result["confidence"]["committed_context_savings"] = paired_transform_interval(
            result["observations"],
            lambda pair: 1 - pair["compacted"]["owner_ledger"]["committed_context_bytes"] /
            pair["exact"]["owner_ledger"]["committed_context_bytes"],
            f"{args.encoding}:{declared_context_tokens}:committed-savings")
        result["comparison"] = "compacted_checkpoint_restore / exact_checkpoint_restore"
        result["ordinary_decode_comparison"] = (
            "compacted checkpoint decode throughput / ordinary full-KV decode throughput")
        result["ordinary_ttft_e2e_comparison_qualified"] = False
        result["packed_exact_ingestion_ttft_qualified"] = False
        result["performance_qualified_scope"] = (
            "checkpoint-restore TTFT/decode plus ordinary full-KV decode; excludes "
            "ordinary or packed-exact ingestion TTFT/E2E")
        result["all_greedy_outputs_equal"] = matched_output_identity(
            result["observations"])
        result["separate_gates"] = {
            "semantic_quality": False, "nll": False, "cancellation": False,
            "soak": False, "production_serving": False}
        result["performance_qualified"] = (
            reused is None and
            result["qualification_sized"] and
            result["all_greedy_outputs_equal"] and
            result["confidence"]["ttft_ms"]["upper_95"] <= 1.25 and
            result["confidence"]["decode_throughput_ratio"]["lower_95"] >= .90 and
            result["confidence"]["ordinary_decode_throughput_ratio"]["lower_95"] >= .90 and
            result["confidence"]["committed_context_savings"]["lower_95"] >= .25)
        result["passed"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        atomic_json(args.output, result)
    print(json.dumps({"passed": result["passed"], "output": str(args.output),
                      "error": result.get("error")}), flush=True)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
