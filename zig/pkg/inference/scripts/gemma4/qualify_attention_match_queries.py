#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check isolated repeat-prefill query capture and unchanged live continuation.

Checks query equality, profiles phases, or compares repeat-collection latency.
Runs variants serially on one GPU. Optional fitting diagnostics exercise snapshot,
rounded fitting, bounded encoding and upload. Explicit --fit-install additionally
checks private live replacement and continuation; this is not release qualification.
"""

import argparse
import copy
import statistics
import json
import math
import os
import re
import signal
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from qualify_context_management import CudaProcessSampler, git_provenance, sha256_file


def read_response(response, stream):
    if not stream:
        return json.load(response)
    message = {"role": "assistant", "content": ""}
    finish_reason = None
    usage = None
    error_event = False
    for raw in response:
        line = raw.decode().strip()
        if line.startswith("event:") and line[6:].strip() == "error":
            error_event = True
            continue
        if not line.startswith("data: "):
            continue
        if error_event:
            raise RuntimeError(f"SSE error: {line[6:4096]}")
        if line[6:] == "[DONE]":
            if finish_reason is None or usage is None:
                raise AssertionError("incomplete SSE completion")
            return {"choices": [{"index": 0, "message": message, "finish_reason": finish_reason}], "usage": usage}
        event = json.loads(line[6:])
        if event.get("error"):
            raise RuntimeError(f"SSE error: {event['error']}")
        if event.get("usage") is not None:
            usage = event["usage"]
        for choice in event.get("choices", []):
            if choice.get("index", 0) != 0:
                raise AssertionError("unexpected extra choice")
            finish_reason = choice.get("finish_reason") or finish_reason
            for key, value in choice.get("delta", {}).items():
                if value is None:
                    continue
                if key == "role":
                    message[key] = value
                elif isinstance(value, str):
                    message[key] = message.get(key, "") + value
                else:
                    raise AssertionError(f"unexpected structured delta: {key}")
    if error_event:
        raise RuntimeError("SSE error event without data")
    raise AssertionError("SSE ended without DONE")


def active_group_members(group_id):
    members = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) != group_id:
                continue
            active = fields[0] not in ("Z", "X")
            if not active:
                # A thread-group leader can already be a zombie while other
                # threads are still exiting and retaining shared descriptors.
                # Wait for those threads too before declaring the port free.
                for task in (entry / "task").iterdir():
                    try:
                        state = (task / "stat").read_text().rsplit(")", 1)[1].split()[0]
                        if state not in ("Z", "X"):
                            active = True
                            break
                    except (FileNotFoundError, ProcessLookupError):
                        pass
            if active:
                members.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return members


def wait_server_ready(process, log_path, port, timeout=120):
    # The controller prints "listening" before the worker binds its socket.
    # Require an actual health response and surface worker startup failures.
    deadline = time.monotonic() + timeout
    while True:
        message = log_path.read_text()
        if process.poll() is not None or "AddressInUse" in message or "InferenceWorkerExited" in message:
            raise RuntimeError(f"server startup failed; see {log_path}")
        if f"listening on 127.0.0.1:{port}" in message:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"server health check timeout; see {log_path}")
        time.sleep(0.1)


def discover_worker_pid(controller_pid, server, proc_root=Path("/proc")):
    """Resolve the private controller's worker before the first model request.

    The response header is still checked against this PID. Ambiguous process
    topology fails closed instead of measuring the controller as the worker.
    """
    children = (proc_root / str(controller_pid) / "task" / str(controller_pid) / "children").read_text().split()
    expected = server.resolve()
    matches = []
    for child in children:
        pid = int(child)
        try:
            if (proc_root / str(pid) / "exe").resolve(strict=True) == expected:
                matches.append(pid)
        except FileNotFoundError:
            continue
    if len(matches) != 1:
        raise RuntimeError(f"expected one private inference worker, found {len(matches)}")
    return matches[0]


def requests(model, dtype, padding_lines, stream=False, fixture="fact"):
    prefixes = {
        "fact": "The access code is violet-729.\n",
        "fact_middle": "The access code is violet-729.\n",
        "tool_call": "The access code is violet-729.\n",
        "code": 'Read this configuration without executing it:\nCONFIG = {"access_code": "violet-729"}\n',
        "tool": 'Tool result (JSON): {"tool": "lookup_access_code", "result": {"access_code": "violet-729"}}\n',
    }
    for lines in padding_lines:
        filler = "This log entry records ordinary maintenance with no changes.\n"
        before = lines // 2 if fixture in ("fact_middle", "tool_call") else 0
        prompt = filler * before + prefixes[fixture] + filler * (lines - before)
        prompt += ("Call lookup_access_record with the access_code from this log. Do not answer in text."
                   if fixture == "tool_call" else "Reply with only the access code.")
        body = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 16,
            "temperature": 0,
            "cache_dtype": dtype,
            "stream": stream,
            "stream_options": {"include_usage": True},
            "chat_template_kwargs": {"enable_thinking": False},
            "context_management": {"mode": "reject"},
        }
        if fixture == "tool_call":
            if stream:
                raise ValueError("tool-call fixture currently requires non-streaming responses")
            body["max_tokens"] = 128
            body["tools"] = [{"type": "function", "function": {
                "name": "lookup_access_record", "description": "Look up a record by its exact access code.",
                "parameters": {"type": "object", "properties": {"access_code": {"type": "string"}},
                               "required": ["access_code"], "additionalProperties": False}}}]
            body["tool_choice"] = {"type": "function", "function": {"name": "lookup_access_record"}}
        yield body


def canonical_choices(choices):
    result = copy.deepcopy(choices)
    for choice in result:
        for call in choice.get("message", {}).get("tool_calls", []):
            # Call IDs are server-generated, not model answer content.
            call.pop("id", None)
    return result


def validate_fixture_choice(choice, fixture):
    message = choice.get("message", {})
    if fixture != "tool_call":
        if choice.get("finish_reason") != "stop" or message.get("content", "").strip() != "violet-729":
            raise AssertionError(f"continuation did not preserve the fixture fact: {choice}")
        return
    calls = message.get("tool_calls", [])
    if choice.get("finish_reason") != "tool_calls" or len(calls) != 1 or message.get("content") not in (None, ""):
        raise AssertionError("fixture did not produce exactly one structured tool call")
    call = calls[0]
    function = call.get("function", {})
    if call.get("type") != "function" or not call.get("id") or function.get("name") != "lookup_access_record":
        raise AssertionError("fixture called the wrong function or omitted call identity")
    def unique_object(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate argument key")
            out[key] = value
        return out
    arguments = json.loads(function.get("arguments", ""), object_pairs_hook=unique_object)
    if arguments != {"access_code": "violet-729"}:
        raise AssertionError("tool call lost or changed the retained fact")


def parse_capture(message):
    # Structured logs wrap msg in JSON quotes; the final value must not absorb
    # the closing quote/brace. This also accepts scientific float notation.
    fields = dict(re.findall(r"(\w+)=([A-Za-z0-9_.+-]+)", message))
    capture = {}
    for key, value in fields.items():
        if key in ("prefix_mode", "repeat_mode", "query_sha256", "export_sha256", "weights_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256", "phase", "error"):
            capture[key] = value
        elif key in ("query_max_absolute", "query_max_relative_l2", "mass_error", "output_rmse", "rounded_mass_error", "rounded_output_rmse", "max_head_mass_error", "max_head_output_rmse", "actual_global_savings_ratio", "incremental_savings_ratio", "max_logit_relative_l2"):
            capture[key] = float(value)
            if not math.isfinite(capture[key]):
                raise AssertionError("non-finite query agreement evidence")
        else:
            capture[key] = int(value)
    return capture


def validate_identity_evidence(rows, request_count):
    if len(rows) != request_count or request_count <= 0:
        raise AssertionError("missing loader-bound native KV identity")
    previous = None
    for row in rows:
        digests = tuple(row.get(key) for key in ("weights_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256"))
        if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) or value == "0" * 64 for value in digests):
            raise AssertionError("invalid or sentinel native KV identity")
        if row.get("cache_hit") not in (0, 1) or row.get("elapsed_ns", 0) <= 0:
            raise AssertionError("missing identity hashing timing/cache evidence")
        if previous is not None and digests != previous:
            raise AssertionError("loaded model identity changed between requests")
        previous = digests


def validate_fit_evidence(rows, requests, minimum_savings=None, cuda_required=False, install_required=False, cooperative_required=False, workers_required=1, host_workspace_required=None, export_version_required=None):
    if len(rows) != len(requests):
        raise AssertionError("missing fitting evidence")
    for row, request in zip(rows, requests):
        if row.get("source_tokens") != request["response"].get("usage", {}).get("prompt_tokens"):
            raise AssertionError("fit length does not match the live prefix")
        if row.get("diagnostic_only") != 1 or row.get("installed") != int(install_required):
            raise AssertionError("unexpected live-cache installation evidence")
        if install_required and row.get("install_ns", 0) <= 0:
            raise AssertionError("missing live-cache installation timing")
        if not install_required and row.get("install_ns", 0) != 0:
            raise AssertionError("unexpected live-cache installation timing")
        if cuda_required and row.get("cuda_fit") != 1:
            raise AssertionError("missing native CUDA fitting evidence")
        if cooperative_required:
            if row.get("cooperative_fit") != 1 or row.get("cuda_fit") != 1:
                raise AssertionError("missing cooperative CUDA fitting evidence")
            launches, restarts = row.get("cooperative_launches"), row.get("cooperative_restarts")
            if (not isinstance(launches, int) or not isinstance(restarts, int) or
                    not 0 <= restarts <= launches or
                    (row.get("completed_chunks", 0) > 0 and launches <= 0)):
                raise AssertionError("invalid cooperative execution counters")
        if host_workspace_required is not None and row.get("host_workspace_limit_bytes") != host_workspace_required:
            raise AssertionError("missing or mismatched aggregate host workspace limit")
        if workers_required > 1 and (row.get("fit_workers") != workers_required or row.get("workspace_reuses", 0) <= 0):
            raise AssertionError("missing parallel worker and scratch reuse evidence")
        phases = [row.get(key, 0) for key in ("snapshot_ns", "fit_ns", "export_ns", "upload_ns")]
        if any(value <= 0 for value in phases) or row.get("total_ns", 0) < sum(phases) + row.get("install_ns", 0):
            raise AssertionError("invalid fitting phase timing")
        if row["source_tokens"] > 2049 and row.get("completed_chunks", 0) == 0:
            raise AssertionError("long fitting probe did not compact any chunks")
        if row.get("residual_fit") and row.get("completed_chunks", 0):
            if row.get("validated_heads", 0) <= 0:
                raise AssertionError("missing whole-head validation evidence")
            for key in ("max_head_mass_error", "max_head_output_rmse"):
                value = row.get(key)
                if value is None or not math.isfinite(value) or not 0 <= value <= 0.05:
                    raise AssertionError("whole-head fit exceeds unchanged quality gate")
        if "source_global_bytes" in row:
            source_bytes, candidate_bytes = row["source_global_bytes"], row.get("candidate_global_bytes", 0)
            ratio = row.get("actual_global_savings_ratio", 0)
            if source_bytes <= 0 or candidate_bytes <= 0 or not math.isclose(ratio, source_bytes / candidate_bytes, rel_tol=1e-9):
                raise AssertionError("actual savings do not match global byte counts")
            if row["source_tokens"] > 2049 and candidate_bytes >= source_bytes:
                raise AssertionError("long fitting probe did not reduce global cache bytes")
            if minimum_savings is not None and ratio < minimum_savings:
                raise AssertionError("actual savings fall below requested minimum")
        elif minimum_savings is not None or row.get("preserved_heads", 0):
            raise AssertionError("missing actual savings evidence")
        if export_version_required is not None and row.get("export_version") != export_version_required:
            raise AssertionError("missing or mismatched checkpoint wire version")
        if not re.fullmatch(r"[0-9a-f]{64}", row.get("export_sha256", "")):
            raise AssertionError("missing encoded checkpoint digest")
        if any(row.get(key, 0) <= 0 for key in ("source_host_bytes", "candidate_host_bytes", "candidate_device_bytes", "export_bytes")):
            raise AssertionError("missing fitting allocation evidence")


def validate_cycle_evidence(rows, fits, summaries, requests, cycles, append_tokens, answer_tokens, minimum_savings, first_minimum_savings=None, workers=4, host_workspace_bytes=2 * 1024 ** 3):
    if len(rows) != cycles * len(requests) or len(fits) != len(rows) or len(summaries) != len(requests):
        raise AssertionError("missing real-model cycle evidence")
    for request_index, request in enumerate(requests):
        initial = request["response"]["usage"]["prompt_tokens"]
        previous_length = initial
        previous_revision = None
        total_matches = total_comparisons = 0
        for index in range(cycles):
            row = rows[request_index * cycles + index]
            fit = fits[request_index * cycles + index]
            source_length = previous_length + (append_tokens if index else 0)
            if row.get("cycle") != index + 1 or row.get("source_unchanged") != 1 or row.get("invalid") != 0 or row.get("checkpoint_roundtrip") != 1:
                raise AssertionError("cycle did not preserve original source and checkpoint round-trip")
            if index and row.get("ragged_repeat_calls", 0) <= 0:
                raise AssertionError("successive cycle did not collect queries from fitted state")
            appends, comparisons, matches = (row.get(key) for key in ("answer_appends", "greedy_comparisons", "greedy_matches"))
            if (not all(isinstance(x, int) for x in (appends, comparisons, matches)) or
                    not 0 <= appends <= answer_tokens or not 1 <= comparisons <= answer_tokens or
                    comparisons not in (appends, appends + 1) or matches != comparisons):
                raise AssertionError("cycle continuation diverged from exact reference")
            if row.get("logical_tokens") != source_length + appends or row["logical_tokens"] > 32768:
                raise AssertionError("cycle positions do not match identical continuation tokens")
            revision = row.get("installed_revision")
            if not isinstance(revision, int) or revision <= 0 or (previous_revision is not None and revision <= previous_revision):
                raise AssertionError("cycle did not publish a new cache revision")
            metric = row.get("max_logit_relative_l2")
            if not isinstance(metric, (int, float)) or not math.isfinite(metric) or metric < 0:
                raise AssertionError("invalid exact-reference logit comparison")
            ratio = row.get("incremental_savings_ratio")
            required = first_minimum_savings if index == 0 else minimum_savings
            if (not isinstance(ratio, (int, float)) or not math.isfinite(ratio) or ratio <= 1 or
                    (required is not None and ratio < required) or row.get("completed_chunks", 0) <= 0):
                raise AssertionError("cycle did not compact newly eligible rows")
            if row.get("completed_chunks") != fit.get("completed_chunks") or not math.isclose(ratio, fit.get("actual_global_savings_ratio", 0), rel_tol=1e-9):
                raise AssertionError("cycle savings disagree with fitted candidate")
            validate_fit_evidence([fit], [{"response": {"usage": {"prompt_tokens": source_length}}}],
                                  minimum_savings=required, cuda_required=True, install_required=True,
                                  cooperative_required=True, workers_required=workers,
                                  host_workspace_required=host_workspace_bytes, export_version_required=2)
            previous_length = row["logical_tokens"]
            previous_revision = revision
            total_matches += matches
            total_comparisons += comparisons
        expected = dict(cycles=cycles, initial_tokens=initial, final_tokens=previous_length,
                        greedy_matches=total_matches, greedy_comparisons=total_comparisons, source_unchanged=1)
        if any(summaries[request_index].get(key) != value for key, value in expected.items()):
            raise AssertionError("incomplete cycle chain summary")


def validate_restore_evidence(rows, requests):
    if len(rows) != len(requests) or not rows:
        raise AssertionError("missing portable fresh-owner restore evidence")
    for row, request in zip(rows, requests):
        source = request["response"]["usage"]["prompt_tokens"]
        if row.get("source_tokens") != source:
            raise AssertionError("restored prefix length mismatch")
        flags = dict(checkpoint_roundtrip=1, fresh_owner=1, replayed_tokens=0,
                     bitwise_logits=1, source_unchanged=1, invalid=0)
        if any(row.get(key) != value for key, value in flags.items()):
            raise AssertionError("restore did not preserve checkpoint/source or replayed the prefix")
        count, appended = row.get("greedy_comparisons"), row.get("appended_tokens")
        if (type(count) is not int or type(appended) is not int or
                not 1 <= appended <= 16 or count not in (appended, appended + 1) or count > 16 or
                row.get("logical_tokens") != source + appended or source + appended > 32768 or
                row.get("attention_calls", 0) <= 0 or row.get("donor_appends", 0) < appended):
            raise AssertionError("restored owner did not execute a valid continuation")


def validate_serving_evidence(rows, requests):
    if len(rows) != len(requests):
        raise AssertionError("missing installed-cache serving evidence")
    for row, request in zip(rows, requests):
        usage = request["response"].get("usage", {})
        source = usage.get("prompt_tokens", 0)
        appended = row.get("logical_tokens", 0) - source
        if (row.get("source_tokens") != source or row.get("invalid") != 0 or
                not 0 < appended <= usage.get("completion_tokens", 0) or
                row.get("attention_calls", 0) <= 0 or row.get("donor_appends", 0) < appended or
                row.get("allocated_device_bytes", 0) <= 0):
            raise AssertionError("installed cache did not serve a valid continuation")


def fitting_fixture_evidence(path, capture, full_head):
    with path.open("rb") as file:
        header = file.read(24)
    if len(header) != 24:
        raise AssertionError("truncated fitting fixture header")
    version, rows, dim, train, heldout, scale = struct.unpack("<6f", header)
    if not all(math.isfinite(value) for value in (version, rows, dim, train, heldout, scale)):
        raise AssertionError("non-finite fitting fixture header")
    if version != 1 or any(value != int(value) or value <= 0 for value in (rows, dim, train, heldout)):
        raise AssertionError("invalid fitting fixture header")
    rows, dim, train, heldout = map(int, (rows, dim, train, heldout))
    source_tokens = capture["source_tokens"]
    expected_rows = source_tokens if full_head else min(512, source_tokens - 2048)
    if source_tokens <= 2049 or rows != expected_rows or dim > 4096 or scale <= 0:
        raise AssertionError("fitting fixture does not match captured source")
    if (not capture["train"] or not capture["heldout"] or
        train % capture["train"] or heldout % capture["heldout"] or
        train // capture["train"] != heldout // capture["heldout"]):
        raise AssertionError("fitting fixture query pool mismatch")
    size = path.stat().st_size
    if size > 160 * 1024 * 1024 or size != 24 + 4 * dim * (2 * rows + train + heldout):
        raise AssertionError("fitting fixture payload size mismatch")
    return {"path": str(path.resolve()), "sha256": sha256_file(path), "bytes": size,
            "rows": rows, "dim": dim, "train_rows": train, "heldout_rows": heldout,
            "source_tokens": source_tokens, "query_sha256": capture["query_sha256"],
            "full_head": full_head, "diagnostic_only": True}


def fitting_fixtures_evidence(path, capture, log):
    events = [(match.start(), parse_capture(match[1])) for match in
              re.finditer(r"attention_match_fit_fixture ([^\n]+)", log)]
    completed = list(re.finditer(r"attention_match_fit_fixture_complete heads=(\d+)", log))
    if not completed:
        raise AssertionError("incomplete all-head fixture capture")
    last = completed[-1]
    count = int(last[1])
    if count <= 0:
        raise AssertionError("empty all-head fixture capture")
    lower = completed[-2].end() if len(completed) > 1 else 0
    rows = [row for position, row in events if lower <= position < last.start()]
    identities = [(row["donor_layer"], row["head"]) for row in rows]
    if len(rows) != count or len(set(identities)) != count:
        raise AssertionError("missing or duplicate global head fixture")
    evidence = []
    for index, row in enumerate(rows):
        file = path if index == 0 else Path(f"{path}.layer{row['donor_layer']}.head{row['head']}.f32")
        fixture = fitting_fixture_evidence(file, capture, True)
        if any(fixture[key] != row[key] for key in ("rows", "train_rows", "heldout_rows")):
            raise AssertionError("fixture does not match recorded donor/head")
        evidence.append({**fixture, "donor_layer": row["donor_layer"], "head": row["head"]})
    return evidence


def run_server(args, probe, evidence, label=None):
    mode = label or ("probe" if probe else "baseline")
    port = args.port + int(probe)
    log_path = args.output.with_suffix(f".{mode}.log")
    environment = os.environ.copy()
    overrides = {
        "ANTFLY_INFERENCE_PREFERRED_BACKEND": "cuda",
        "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda",
        "ANTFLY_SERVER_DECODE_GRAPH_REPLAY": "off",
        "ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS": "0",
        "ANTFLY_INFERENCE_ATTENTION_MATCH_REFERENCE_PROBE": str(int(probe)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_EXACT_PREFILL_PROBE": str(int(probe and getattr(args, "exact_prefill_probe", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_CYCLES": str(getattr(args, "fit_cycles", 0) if probe else 0),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_CYCLE_APPEND": str(getattr(args, "cycle_append_tokens", 1024)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_CYCLE_ANSWER": str(getattr(args, "cycle_answer_tokens", 16)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_CYCLE_MIN_SAVINGS": str(getattr(args, "cycle_min_savings_ratio", 1.000001)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_WORKERS": str(getattr(args, "fit_workers", 1) if probe else 1),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_VALIDATION_WORKERS": str(getattr(args, "validation_workers", 2) if probe else 2),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_HOST_BYTES": str((getattr(args, "fit_host_mib", 2048) if probe else 2048) * 1024 * 1024),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_COOPERATIVE": str(int(probe and getattr(args, "fit_cooperative", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_CUDA": str(int(probe and getattr(args, "fit_cuda", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_INSTALL": str(int(probe and getattr(args, "fit_install", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_PRESERVE_REJECTED": str(int(probe and getattr(args, "fit_preserve_rejected", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_MIN_SAVINGS": str(args.fit_min_savings_ratio) if probe and getattr(args, "fit_min_savings_ratio", None) is not None else "",
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_RESIDUAL": str(int(probe and getattr(args, "fit_residual", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_PROBE": str(int(probe and getattr(args, "fit_probe", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_FIXTURE": str(args.fit_fixture.resolve()) if probe and getattr(args, "fit_fixture", None) else "",
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_FIXTURE_ALL_HEADS": str(int(probe and getattr(args, "fit_fixture_all_heads", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_FIXTURE_FULL_HEAD": str(int(probe and getattr(args, "fit_fixture_full_head", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_PREFIX_MODE": args.prefix_mode,
        "ANTFLY_INFERENCE_ATTENTION_MATCH_REPEAT_MODE": args.repeat_mode,
        "ANTFLY_INFERENCE_ATTENTION_MATCH_QUERY_SAMPLES": str(getattr(args, "query_samples", 80)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_REPEAT_CHUNK_SIZE": str(getattr(args, "repeat_chunk_size", 256)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_REFERENCE_SUFFIX_TOKENS": str(getattr(args, "reference_suffix_tokens", None) or 0),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_VERIFY_REPEAT": str(int(probe and args.verify_repeat)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_VERIFY_ATTENTION": str(int(probe and args.verify_attention)),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_VERIFY_RESTORE": str(int(probe and getattr(args, "verify_restore", False))),
        "ANTFLY_INFERENCE_ATTENTION_MATCH_VERIFY_CLONE": str(int(probe and args.verify_clone)),
        "ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING": "1",
        "ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS": str(int(args.profile_prefill)),
    }
    environment.update(overrides)
    command = [
        "bash", str(Path(__file__).with_name("with_gemma4_qat_cuda_tuning.sh")),
        str(args.server.resolve()), "run", "--host", "127.0.0.1", "--port", str(port),
        "--models-dir", str(args.models_dir.resolve()), "--max-concurrent-requests", "1",
        "--host-budget-mb", "8192", "--backend-budget-mb", "18432", "--combined-budget-mb", "26624",
        "--kv-budget-mb", "4096",
    ]
    record = {"command": command, "environment_overrides": overrides, "requests": []}
    evidence[mode] = record
    sampler = None
    with log_path.open("w") as log:
        process = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            wait_server_ready(process, log_path, port)
            worker_pid = discover_worker_pid(process.pid, args.server)
            sampler = CudaProcessSampler(worker_pid)
            sampler.start()
            record["sampling_started_before_requests"] = True
            workloads = [(fixture, body) for fixture in getattr(args, "fixtures", ["fact"])
                         for body in requests(args.model, args.dtype, args.padding_lines, args.profile_prefill, fixture)]
            for iteration in range(args.warmups + args.repetitions):
                for fixture, body in workloads:
                    request = urllib.request.Request(
                        f"http://127.0.0.1:{port}/ai/v1/generate",
                        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"},
                    )
                    started = time.monotonic()
                    try:
                        with urllib.request.urlopen(request, timeout=args.request_timeout) as response:
                            result = read_response(response, args.profile_prefill)
                            headers = {k: v for k, v in response.headers.items() if k.lower().startswith("x-antfly-inference")}
                    except urllib.error.HTTPError as error:
                        raise RuntimeError(f"HTTP {error.code}: {error.read(65536).decode(errors='replace')}") from error
                    record["requests"].append({"body": body, "fixture": fixture, "response": result, "headers": headers, "elapsed_s": time.monotonic() - started, "warmup": iteration < args.warmups})
                    if result.get("error") or not result.get("choices"):
                        raise AssertionError(f"generation failed: {result}")
                    choice = result["choices"][0]
                    validate_fixture_choice(choice, fixture)
                    if not any(k.lower() == "x-antfly-inference-backend" and v == "cuda" for k, v in headers.items()):
                        raise AssertionError("generation did not confirm CUDA backend")
                    worker_pid = int(next(v for k, v in headers.items() if k.lower() == "x-antfly-inference-pid"))
                    if sampler.pid != worker_pid:
                        raise AssertionError("worker changed during qualification")
                    print(f"{mode}: request {len(record['requests'])} completed", flush=True)
        finally:
            if sampler is not None:
                record["memory"] = sampler.finish()
                # Compatibility name for existing performance evidence readers.
                record["memory_after_first_request"] = record["memory"]
            # The server controller owns a worker process. Stop the entire
            # private group so an old worker cannot serve the next variant.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            deadline = time.monotonic() + 30
            while active_group_members(process.pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            if active_group_members(process.pid):
                os.killpg(process.pid, signal.SIGKILL)
                raise RuntimeError("server process group failed to stop")
            record["log"] = {"path": str(log_path), "sha256": sha256_file(log_path)}
            record["restores"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_restore ([^\n]+)", log_path.read_text())]
            record["identities"] = [parse_capture(match[1]) for match in
                re.finditer(r"native_kv_identity ([^\n]+)", log_path.read_text())]
            record["exact_prefill"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_exact_prefill ([^\n]+)", log_path.read_text())]
            record["captures"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_queries ([^\n]+)", log_path.read_text())]
            record["fit_probes"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_fit ([^\n]+)", log_path.read_text())]
            record["fit_rejections"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_fit_rejected ([^\n]+)", log_path.read_text())]
            record["fit_errors"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_fit_error ([^\n]+)", log_path.read_text())]
            record["serving"] = [parse_capture(match[1]) for match in
                re.finditer(r"attention_match_serving ([^\n]+)", log_path.read_text())]
            for key, tag in (("cycles", "attention_match_cycle"), ("cycle_fits", "attention_match_cycle_fit"), ("cycle_summaries", "attention_match_cycles_complete")):
                record[key] = [parse_capture(match[1]) for match in re.finditer(tag + r" ([^\n]+)", log_path.read_text())]
            # A completed fit or entry into fitting proves the preceding dump
            # completed. Do not attribute a stale file to a failed snapshot.
            if (probe and getattr(args, "fit_fixture", None) and record["captures"] and
                record["captures"][-1]["source_tokens"] > 2049 and
                (record["fit_rejections"][-1]["phase"] == "fit" if record["fit_rejections"] else
                 len(record["fit_probes"]) == len(record["captures"]))):
                record["fit_fixture"] = fitting_fixture_evidence(args.fit_fixture, record["captures"][-1], args.fit_fixture_full_head)
                if getattr(args, "fit_fixture_all_heads", False):
                    record["fit_fixtures"] = fitting_fixtures_evidence(args.fit_fixture, record["captures"][-1], log_path.read_text())
    if probe and getattr(args, "fit_probe", False):
        if record["fit_rejections"]:
            raise AssertionError("fitting candidate rejected")
        validate_fit_evidence(record["fit_probes"], record["requests"], getattr(args, "fit_min_savings_ratio", None), getattr(args, "fit_cuda", False), getattr(args, "fit_install", False), getattr(args, "fit_cooperative", False), getattr(args, "fit_workers", 1), getattr(args, "fit_host_mib", 2048) * 1024 * 1024, export_version_required=2)
        if getattr(args, "fit_install", False):
            validate_serving_evidence(record["serving"], record["requests"])
    if probe and getattr(args, "verify_restore", False):
        validate_restore_evidence(record["restores"], record["requests"])
    if probe and args.fit_probe:
        validate_identity_evidence(record["identities"], len(record["requests"]))
    if probe and getattr(args, "exact_prefill_probe", False):
        if len(record["exact_prefill"]) != len(record["requests"]):
            raise AssertionError("missing exact-prefill candidate evidence")
        for row, request in zip(record["exact_prefill"], record["requests"]):
            if row.get("source_tokens") != request["response"]["usage"]["prompt_tokens"] or row.get("split_launches", 0) <= 0 or row.get("diagnostic_only") != 1:
                raise AssertionError("exact-prefill candidate did not execute")
    if probe and getattr(args, "fit_cycles", 0):
        validate_cycle_evidence(record["cycles"], record["cycle_fits"], record["cycle_summaries"], record["requests"],
                                args.fit_cycles, args.cycle_append_tokens, args.cycle_answer_tokens,
                                args.cycle_min_savings_ratio, args.fit_min_savings_ratio,
                                args.fit_workers, args.fit_host_mib * 1024 * 1024)
    captures = record["captures"]
    record["prefill_profiles_us"] = [
        {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", match[1])}
        for match in re.finditer(r"cuda_prefill_profile_us: ([^\n]+)", log_path.read_text())
    ]
    if args.profile_prefill and (len(record["prefill_profiles_us"]) != len(record["requests"]) or
                                 any(not row.get("events") for row in record["prefill_profiles_us"])):
        raise AssertionError("missing per-request CUDA operator profiles")
    record["captures"] = captures
    record["phase_profiles"] = [parse_capture(match[1]) for match in
        re.finditer(r"attention_match_phase ([^\n]+)", log_path.read_text())]
    if probe and args.profile_prefill:
        expected = len(record["requests"]) * (1 + int(args.verify_repeat) + int(args.verify_clone))
        repeats = [row for row in record["phase_profiles"] if row.get("phase") == "repeat"]
        if len(repeats) != expected or any(not row.get("events") for row in repeats):
            raise AssertionError("missing isolated repeat phase profiles")
    if probe:
        if len(captures) != len(record["requests"]):
            raise AssertionError("missing query capture evidence")
        for capture, request in zip(captures, record["requests"]):
            if capture.get("chunk_size", 256) != getattr(args, "repeat_chunk_size", 256):
                raise AssertionError("repeat chunk size mismatch")
            reference_tokens = min(capture["source_tokens"], getattr(args, "reference_suffix_tokens", None) or capture["source_tokens"])
            if capture.get("reference_tokens", capture["source_tokens"]) != reference_tokens:
                raise AssertionError("reference token count mismatch")
            if capture.get("train", 0) + capture.get("heldout", 0) != min(reference_tokens, getattr(args, "query_samples", 80)):
                raise AssertionError("query sample count mismatch")
            if capture.get("consumers") != 7 or capture.get("heads") != 56:
                raise AssertionError(f"incomplete Gemma 4 E4B consumer coverage: {capture}")
            if not capture.get("train") or not capture.get("heldout") or not capture.get("elapsed_ns"):
                raise AssertionError(f"missing held-out queries or timing: {capture}")
            if capture.get("source_tokens") != request["response"].get("usage", {}).get("prompt_tokens"):
                raise AssertionError("query capture length does not match the live prefix")
            if capture.get("repeat_mode") != args.repeat_mode or capture.get("verified_repeat") != int(args.verify_repeat):
                raise RuntimeError("repeat mode or verification mismatch")
            if args.repeat_mode != "reference" and capture.get("repeat_kernel_launches", 0) <= 0:
                raise RuntimeError("repeat candidate never launched")
            if args.repeat_mode == "exact_split" and (capture.get("split_kernel_launches", 0) <= 0 or not 0 < capture.get("scratch_bytes", 0) <= 160 * 1024 * 1024):
                raise RuntimeError("split route missing launches or bounded scratch")
            if args.verify_repeat and not (capture.get("query_max_absolute") == 0 and capture.get("query_max_relative_l2") == 0):
                raise RuntimeError("repeat query numerical agreement failed")
            if capture.get("prefix_mode") != args.prefix_mode or capture.get("verified_clone") != int(args.verify_clone):
                raise AssertionError("incorrect prefix copy/verification route")
            if args.prefix_mode == "clone" and not capture.get("cloned_bytes"):
                raise AssertionError("prefix copy did not copy device bytes")
            if not capture.get("prefix_ns") or not capture.get("repeat_ns") or not re.fullmatch(r"[0-9a-f]{64}", capture.get("query_sha256", "")):
                raise AssertionError("missing query digest or phase timing")
    elif captures:
        raise AssertionError("baseline unexpectedly captured queries")


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty latency samples")
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def summarize_performance(records, metric="collection"):
    """Keep the isolated query stage separate from the complete HTTP request."""
    if metric not in ("collection", "request"):
        raise ValueError("unknown latency metric")
    groups = {}
    for record in records:
        if len(record["requests"]) != len(record["captures"]):
            raise ValueError("request/capture count mismatch")
        for request, capture in zip(record["requests"], record["captures"]):
            if request["warmup"]:
                continue
            key = (record.get("variant", capture["repeat_mode"]), capture["repeat_mode"],
                   capture["source_tokens"], request.get("fixture", "fact"))
            collection = capture["elapsed_ns"] / 1e9
            value = collection if metric == "collection" else request["elapsed_s"]
            if not math.isfinite(value) or value <= 0 or not math.isfinite(collection) or collection <= 0:
                raise ValueError("invalid latency observation")
            if metric == "request" and value < collection:
                raise ValueError("request latency shorter than query collection")
            groups.setdefault(key, []).append(value)
    return [{"metric": metric, "variant": variant, "repeat_mode": mode, "source_tokens": tokens, "fixture": fixture, "samples": len(values),
             "p50_s": statistics.median(values), "p95_s": percentile(values, .95),
             "min_s": min(values), "max_s": max(values),
             "cv": statistics.pstdev(values) / statistics.mean(values)}
            for (variant, mode, tokens, fixture), values in sorted(groups.items())]


def summarize_memory(records):
    """Describe actual sampler coverage; missing evidence is never a pass."""
    reports = [record.get("memory", record.get("memory_after_first_request", {})) for record in records]
    includes_cold_start = bool(records) and all(record.get("sampling_started_before_requests") for record in records)
    complete = bool(reports) and all(
        row.get("samples", 0) >= 2 and not row.get("errors") and
        row.get("peak_host_rss_bytes") is not None and
        row.get("peak_gpu_memory_bytes") is not None for row in reports)
    host_peak = max((row.get("peak_host_rss_bytes") or 0 for row in reports), default=0)
    gpu_peak = max((row.get("peak_gpu_memory_bytes") or 0 for row in reports), default=0)
    return {"scope": ("from before first request through block completion; includes cold-start peak" if includes_cold_start
                      else "coverage differs or begins after first response; cold-start peak not guaranteed"),
            "includes_cold_start": includes_cold_start,
            "sampling_complete": complete,
            "peak_host_rss_bytes": host_peak, "peak_gpu_memory_bytes": gpu_peak,
            "host_limit_bytes": 8 * 1024**3, "gpu_limit_bytes": 18 * 1024**3,
            "memory_gate_met": complete and 0 < host_peak <= 8 * 1024**3 and 0 < gpu_peak <= 18 * 1024**3}


def compare_performance(rows, candidate, reference, warmups=3):
    """Report the development speed gate; this never changes runtime defaults."""
    indexed = {(row.get("variant", row["repeat_mode"]), row["source_tokens"], row["fixture"]): row for row in rows}
    comparisons = []
    for (route, tokens, fixture), current in indexed.items():
        if route != candidate:
            continue
        baseline = indexed[(reference, tokens, fixture)]
        p50_reduction = 1 - current["p50_s"] / baseline["p50_s"]
        p95_reduction = 1 - current["p95_s"] / baseline["p95_s"]
        enough_samples = min(current["samples"], baseline["samples"]) >= 10
        comparisons.append({
            "source_tokens": tokens, "fixture": fixture,
            "candidate": candidate, "reference": reference,
            "p50_reduction_fraction": p50_reduction,
            "p95_reduction_fraction": p95_reduction,
            "development_sample_count_met": enough_samples,
            "development_warmup_count_met": warmups >= 3,
            "speed_gate_met": enough_samples and warmups >= 3 and min(p50_reduction, p95_reduction) >= .15,
        })
    return comparisons


def performance_schedule(candidate, reference, repetitions):
    # AB then BA. Each block has its own unmeasured warmups; startup and model
    # loading never enter collection timings. Total measured runs per route
    # and fixture equals repetitions, including odd counts.
    if repetitions < 2 or candidate == reference:
        raise ValueError("comparison requires distinct routes and at least two repetitions")
    return [(candidate, repetitions // 2), (reference, repetitions // 2),
            (reference, repetitions - repetitions // 2), (candidate, repetitions - repetitions // 2)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--compare-server", type=Path, help="performance only: compare a saved baseline binary, including the same repeat route")
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument("--port", type=int, default=8091, help="baseline port; probe uses the next port")
    parser.add_argument("--mode", choices=("correctness", "profile", "performance"), default="correctness")
    parser.add_argument("--compare-repeat-mode", choices=("reference", "exact", "exact_split"), default="exact")
    parser.add_argument("--warmups", type=int, default=None)
    parser.add_argument("--request-timeout", type=float, default=3600)
    parser.add_argument("--profile-prefill", action="store_true", help="collect CUDA operator timings via the streaming route")
    parser.add_argument("--repetitions", type=int, default=None)
    parser.add_argument("--dtype", choices=("f16", "f32"), default="f16")
    parser.add_argument("--prefix-mode", choices=("clone", "replay"), default="clone")
    parser.add_argument("--repeat-mode", choices=("reference", "exact", "exact_split"), default="reference")
    parser.add_argument("--query-samples", type=int, default=80, help="total training/held-out sample positions per query head (2..320)")
    parser.add_argument("--repeat-chunk-size", type=int, choices=(256, 512), default=256)
    parser.add_argument("--reference-suffix-tokens", type=int, help="experimental bounded suffix repeat over the complete original prefix")
    parser.add_argument("--verify-attention", action="store_true", help="bounded short-prompt per-layer attention diagnostics; excludes latency qualification")
    parser.add_argument("--verify-repeat", action="store_true", help="compare every captured query against reference repeat")
    parser.add_argument("--verify-restore", action="store_true", help="Compare a fresh imported cache with an independent fitted clone, without prefix replay")
    parser.add_argument("--verify-clone", action="store_true", help="also collect replay queries in the same request and require bitwise equality")
    parser.add_argument("--fit-probe", action="store_true", help="diagnostic snapshot, f16-rounded fitting, bounded export and upload")
    parser.add_argument("--fit-workers", type=int, choices=(1, 2, 3, 4), default=1, help="independent native fitting streams with an aggregate memory limit")
    parser.add_argument("--validation-workers", type=int, choices=(1, 2, 3, 4), default=2, help="bound concurrent whole-head validation independently of fit streams")
    parser.add_argument("--fit-host-mib", type=int, default=2048, help="Aggregate fitting host-workspace limit in MiB (maximum 4096); sampled process RSS is qualified separately")
    parser.add_argument("--fit-cooperative", action="store_true", help="qualify native cooperative CUDA selection with measured execution counters")
    parser.add_argument("--fit-cuda", action="store_true", help="qualify context-owned native CUDA fitting inside the residual preparation probe")
    parser.add_argument("--fit-install", action="store_true", help="explicit private live replacement and continuation probe; requires native CUDA fitting")
    parser.add_argument("--fit-preserve-rejected", action="store_true", help="keep numerically rejected heads unchanged; actual savings are reported separately")
    parser.add_argument("--fit-min-savings-ratio", type=float, help="reject a prepared candidate below this actual global device-byte saving")
    parser.add_argument("--fit-cycles", type=int, choices=range(0, 21), default=0, help="private successive real-model compaction/round-trip diagnostic against an independent exact cache")
    parser.add_argument("--cycle-append-tokens", type=int, default=1024)
    parser.add_argument("--cycle-answer-tokens", type=int, default=16)
    parser.add_argument("--cycle-min-savings-ratio", type=float, default=1.000001, help="strict incremental saving required after the first cycle")
    parser.add_argument("--fit-residual", action="store_true", help="experimental native residual selection/active NNLS, exact prefix128, chunk1024, complete rounded head gate")
    parser.add_argument("--fit-fixture", type=Path, help="write the first fitted head's numerical inputs for offline diagnosis; requires --fit-probe")
    parser.add_argument("--fit-fixture-all-heads", action="store_true", help="dump every global donor/head; requires full-head fixtures, diagnostic only")
    parser.add_argument("--fit-fixture-full-head", action="store_true", help="include all rows of the first global KV head, including the exact recent suffix")
    parser.add_argument("--exact-prefill-probe", action="store_true", help="Private exact-split prompt-ingestion candidate; requires fitting diagnostics")
    parser.add_argument("--fixtures", nargs="+", choices=("fact", "fact_middle", "code", "tool", "tool_call"), default=["fact"])
    parser.add_argument("--padding-lines", type=int, nargs="+", default=[0, 20, 80, 114],
                        help="fact fixture: 114 has a one-token tail, 742 is 8189 tokens, 2976 is 32763; use 2950 for all fixtures near 32K")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 2 <= args.query_samples <= 320:
        parser.error("--query-samples must be between 2 and 320")
    if args.reference_suffix_tokens is not None and not 2 <= args.reference_suffix_tokens <= 32768:
        parser.error("--reference-suffix-tokens must be between 2 and 32768")
    if args.fit_probe and (args.mode == "performance" or args.dtype != "f16"):
        parser.error("--fit-probe requires f16 and correctness/profile mode; it does not qualify repeat-only performance")
    if args.fit_cuda and not (args.fit_probe and args.fit_residual):
        parser.error("--fit-cuda requires --fit-probe and --fit-residual")
    if args.fit_workers > 1 and not args.fit_cooperative:
        parser.error("--fit-workers > 1 requires --fit-cooperative")
    if args.fit_cooperative and not args.fit_cuda:
        parser.error("--fit-cooperative requires --fit-cuda")
    if args.verify_restore and (not args.fit_install or args.mode != "correctness"):
        parser.error("--verify-restore requires correctness mode with --fit-install")
    if args.fit_install and not args.fit_cuda:
        parser.error("--fit-install requires --fit-cuda")
    if (args.fit_preserve_rejected or args.fit_min_savings_ratio is not None) and not args.fit_probe:
        parser.error("fitting fallback/savings options require --fit-probe")
    if args.fit_min_savings_ratio is not None and (not math.isfinite(args.fit_min_savings_ratio) or args.fit_min_savings_ratio < 1):
        parser.error("--fit-min-savings-ratio must be finite and at least one")
    if args.exact_prefill_probe and (not args.fit_probe or args.mode != "correctness"):
        parser.error("exact prefill probe requires correctness mode with --fit-probe")
    if args.fit_cycles and not (args.fit_install and args.fit_cooperative):
        parser.error("--fit-cycles requires --fit-install and --fit-cooperative")
    if not 512 <= args.cycle_append_tokens <= 2048 or not 1 <= args.cycle_answer_tokens <= 32:
        parser.error("cycle appends must be 512..2048 and answers 1..32 tokens")
    if not math.isfinite(args.cycle_min_savings_ratio) or args.cycle_min_savings_ratio <= 1:
        parser.error("cycle incremental savings must be finite and greater than one")
    if args.fit_residual and not args.fit_probe:
        parser.error("--fit-residual requires --fit-probe")
    if args.fit_fixture and not args.fit_probe:
        parser.error("--fit-fixture requires --fit-probe")
    if args.fit_fixture_all_heads and not args.fit_fixture_full_head:
        parser.error("--fit-fixture-all-heads requires --fit-fixture-full-head")
    if args.fit_fixture_full_head and not args.fit_fixture:
        parser.error("--fit-fixture-full-head requires --fit-fixture")
    if args.repetitions is None:
        args.repetitions = 10 if args.mode == "performance" else 2
    if args.warmups is None:
        args.warmups = 3 if args.mode == "performance" else 0
    if not 1 <= args.port <= (65528 if args.mode == "performance" else 65534):
        parser.error("--port must leave room for all server variants")
    if args.fit_host_mib < 1 or args.fit_host_mib > 4096:
        parser.error("--fit-host-mib must be between 1 and 4096")
    if args.warmups < 0 or not math.isfinite(args.request_timeout) or args.request_timeout <= 0:
        parser.error("warmups must be nonnegative and timeout finite and positive")
    if args.mode == "profile":
        args.profile_prefill = True
    if args.mode == "performance":
        if args.profile_prefill or args.verify_repeat or args.verify_clone or args.verify_attention:
            parser.error("performance mode excludes profiling and verification passes")
        if args.repetitions < 2 or (args.compare_server is None and args.compare_repeat_mode == args.repeat_mode):
            parser.error("performance mode requires distinct routes or --compare-server, and at least two repetitions")
        if args.dtype != "f16":
            parser.error("performance comparison requires f16 KV")
    elif args.mode == "correctness" and args.repeat_mode != "reference":
        args.verify_repeat = True
    if args.compare_server is not None and args.mode != "performance":
        parser.error("--compare-server requires performance mode")
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    if args.repeat_mode != "reference" and args.dtype != "f16":
        parser.error("exact repeat requires f16 KV")
    if args.verify_attention and (args.repeat_mode == "reference" or args.padding_lines != [0]):
        parser.error("--verify-attention requires exact and --padding-lines 0")
    if args.verify_repeat and args.repeat_mode == "reference":
        parser.error("--verify-repeat requires --repeat-mode exact")
    if args.verify_clone and args.prefix_mode != "clone":
        parser.error("--verify-clone requires --prefix-mode clone")
    if any(lines < 0 or lines > 10000 for lines in args.padding_lines):
        parser.error("--padding-lines must be between 0 and 10000")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[5]
    evidence = {"passed": False, "scope": "private fitted-cache installation and visible continuation parity; not release qualification" if args.fit_install
                else "query capture and visible continuation parity; no cache replacement"}
    try:
        evidence["source"] = git_provenance(repo)
        evidence["binary"] = {"path": str(args.server.resolve()), "sha256": sha256_file(args.server)}
        if args.compare_server is not None:
            evidence["comparison_binary"] = {"path": str(args.compare_server.resolve()), "sha256": sha256_file(args.compare_server)}
            if evidence["comparison_binary"]["sha256"] == evidence["binary"]["sha256"]:
                raise ValueError("comparison binary must differ; use route comparison for the same binary")
        evidence["runner_sha256"] = sha256_file(Path(__file__))
        evidence["model_files"] = {
            str(path.relative_to(args.models_dir)): sha256_file(path)
            for path in sorted((args.models_dir / args.model).iterdir()) if path.is_file()
        }
        if not evidence["model_files"]:
            raise AssertionError("no model artifacts to fingerprint")
        evidence["gpu"] = subprocess.check_output([
            "nvidia-smi", "--query-gpu=name,uuid,driver_version,memory.total", "--format=csv,noheader",
        ], text=True).strip()
        if args.mode == "performance":
            evidence["scope"] = "repeat-query collection latency; excludes fitting/export; no cache replacement"
            evidence["configuration"] = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
            candidate_label, reference_label = ("candidate", "baseline") if args.compare_server else (args.repeat_mode, args.compare_repeat_mode)
            records = []
            for index, (label, repetitions) in enumerate(performance_schedule(candidate_label, reference_label, args.repetitions)):
                variant = copy.copy(args)
                # Each block owns a new listener; immediate port reuse after a
                # worker exits can fail even though its process group is gone.
                variant.port = args.port + index * 2
                variant.repeat_mode = args.repeat_mode if label == candidate_label else args.compare_repeat_mode
                if args.compare_server is not None and label == reference_label:
                    variant.server = args.compare_server
                variant.repetitions = repetitions
                record_label = f"round{index}-{label}"
                run_server(variant, True, evidence, record_label)
                evidence[record_label]["variant"] = label
                evidence[record_label]["binary_sha256"] = sha256_file(variant.server)
                records.append(evidence[record_label])
            evidence["latency"] = summarize_performance(records)
            evidence["request_latency"] = summarize_performance(records, metric="request")
            evidence["timing_scopes"] = {
                "collection": "query collection only; excludes original ingestion, fitting and export",
                "request": "HTTP request including ingestion, query collection and continuation; not full compaction",
            }
            evidence["warm_memory"] = summarize_memory(records)
            evidence["comparison"] = compare_performance(evidence["latency"], candidate_label, reference_label, args.warmups)
            evidence["release_sample_count_met"] = args.warmups >= 5 and args.repetitions >= 100
            # Check continuation, usage and query digests across all routes and
            # repetitions, including warmups. Correctness mode separately
            # compares every query byte against the reference.
            signatures = {}
            for record in records:
                for request, capture in zip(record["requests"], record["captures"]):
                    key = json.dumps(request["body"], sort_keys=True)
                    signature = (canonical_choices(request["response"]["choices"]), {field: request["response"].get("usage", {}).get(field) for field in ("prompt_tokens", "completion_tokens", "total_tokens")}, capture["query_sha256"])
                    if signatures.setdefault(key, signature) != signature:
                        raise AssertionError("performance routes changed queries or continuation")
        else:
            run_server(args, False, evidence)
            run_server(args, True, evidence)
            baseline = evidence["baseline"]["requests"]
            probe = evidence["probe"]["requests"]
            if len(baseline) != len(probe):
                raise AssertionError("request count mismatch")
            for before, after in zip(baseline, probe):
                if before["body"] != after["body"] or canonical_choices(before["response"]["choices"]) != canonical_choices(after["response"]["choices"]):
                    raise AssertionError("query collection changed visible continuation")
                for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
                    if before["response"].get("usage", {}).get(field) != after["response"].get("usage", {}).get(field):
                        raise AssertionError(f"query collection changed {field}")
        if git_provenance(repo)["source_snapshot_sha256"] != evidence["source"]["source_snapshot_sha256"]:
            raise AssertionError("source changed during qualification")
        for key in ("binary", "comparison_binary"):
            if key in evidence and sha256_file(Path(evidence[key]["path"])) != evidence[key]["sha256"]:
                raise AssertionError(f"{key} changed during qualification")
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = str(error)
    finally:
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"passed": evidence["passed"], "error": evidence.get("error"), "output": str(args.output)}))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
