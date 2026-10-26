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

"""Bounded persistent loaded-tokenizer search for one frozen 100-case cell."""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import stat
import subprocess
import sys
import time

import quality_corpus_pack_v8_tool_history as pack
import quality_ordered_messages as ordered
import quality_tokenizer_materialize as materialize

MAX_ITERATIONS = 19
MAX_CELL_CASES = 100
MAX_CAMPAIGN_BYTES = 2 * 1024 * 1024 * 1024
GUARD_FD_ENV = "ANTFLY_BENCHMARK_GUARD_FD"
GUARD_TOKEN_ENV = "ANTFLY_BENCHMARK_GUARD_TOKEN"
GUARD_OWNER_PID_ENV = "ANTFLY_BENCHMARK_GUARD_OWNER_PID"
GUARD_WITNESS_BYTES = 32
BUCKET_CONTRACTS = {
    "document_qa":"source_cited_scalar_question",
    "exact_retrieval":"source_cited_exact_span",
    "code":"source_cited_python_integer_expression",
    "structured_tool":"historical_assistant_call_selected_tool_response_canonical_json",
    "multiturn":"ordered_user_assistant_user_source_update",
}


def require(value, message):
    if not value:
        raise ValueError(message)


def _json_bytes(value):
    return (json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _append_sync(stream, value):
    stream.write(_json_bytes(value).decode())
    stream.flush()
    os.fsync(stream.fileno())


def _campaign_bytes(root):
    total = 0
    for path in Path(root).rglob("*"):
        if path.is_file() and not path.is_symlink():
            total += path.stat().st_size
    return total


def _initial_control(item, target):
    families = len(item["distractor_families"])
    insertion = min(families, ((2 * item["case"]["intended_compactable_quartile"] + 1) *
                               (families + 1)) // 8)
    maximum = item["available_source_bytes"]
    return {"target_prompt_tokens": target,
            "intended_quartile": item["case"]["intended_compactable_quartile"],
            "byte_budget": min(maximum, max(8192, target * 3)),
            "maximum_byte_budget": maximum, "insertion_index": insertion,
            "maximum_insertion_index": families, "phase": "source",
            "padding_units": 0, "samples": []}


def _write_candidate(root, work, item, authority, request_index, control):
    candidate = pack.compose_candidate(item, work["source_rows"], authority["source_root"],
                                       control["byte_budget"], control["insertion_index"],
                                       control["padding_units"])
    stem = f"candidate-{request_index:04d}"
    messages_name, reference_name = stem + ".messages.json", stem + ".reference.txt"
    messages_raw = _json_bytes(candidate["messages"])
    reference_raw = candidate["reference_utf8"].encode()
    pack._write_exclusive(Path(root) / messages_name, messages_raw)
    pack._write_exclusive(Path(root) / reference_name, reference_raw)
    messages = ordered.loads(messages_raw)
    case = item["case"]
    request_case = {"case_id": case["id"], "messages_file": messages_name,
        "messages_sha256": pack.sha(messages_raw),
        "request_content_sha256": ordered.semantic_digest(messages),
        "source_sha256": ordered.selected_source_digest(messages),
        "selected_part_sha256s": ordered.selected_part_digests(messages),
        "reference_file": reference_name, "reference_sha256": pack.sha(reference_raw),
        "qualified_context_tokens": authority["preparation_context_tokens"],
        "reserved_output_tokens": authority["reserved_output_tokens"],
        "settings_sha256": authority["settings_sha256"],
        "variant_settings_sha256": authority["variant_settings_sha256"]}
    return candidate, request_case


def _observe(root, report, candidate, target_context, reserved):
    rendered = materialize._artifact(root, report["rendered_prompt"], "rendered prompt",
                                     32 * 1024 * 1024, 32 * 1024 * 1024)
    prompt_raw = materialize._artifact(root, report["prompt_tokens"], "prompt tokens",
                                      materialize.MAX_PROMPT_TOKENS,
                                      materialize.MAX_PROMPT_TOKENS * 8)
    prompt = materialize._i64(prompt_raw, report["prompt_tokens"]["count"], "prompt")
    offsets_raw = materialize._artifact(root, report["prompt_token_offsets"], "prompt token offsets",
                                       materialize.MAX_PROMPT_TOKENS,
                                       materialize.MAX_PROMPT_TOKENS * 8)
    offsets = materialize._offsets(offsets_raw, len(prompt), len(rendered))
    rendered_raw = materialize._artifact(root, report["rendered_labels"], "rendered labels",
                                        32 * 1024 * 1024, 32 * 1024 * 1024 * 4)
    rendered_labels = materialize._u32(rendered_raw, report["rendered_labels"]["count"],
                                       "rendered labels")
    token_raw = materialize._artifact(root, report["prompt_token_labels"], "prompt token labels",
                                     materialize.MAX_PROMPT_TOKENS,
                                     materialize.MAX_PROMPT_TOKENS * 4)
    token_labels = materialize._u32(token_raw, report["prompt_token_labels"]["count"],
                                    "prompt token labels")
    part = candidate["evidence_part_utf8"]
    start = rendered.find(part)
    require(start >= 0 and rendered.find(part, start + 1) < 0,
            "selected evidence part is not unique in rendered prompt")
    evidence_start = start + candidate["evidence_part_byte_start"]
    evidence_end = start + candidate["evidence_part_byte_end"]
    label = candidate["primary_label"]
    require(len(rendered_labels) == len(rendered) and
            all(value == label for value in rendered_labels[evidence_start:evidence_end]),
            "evidence bytes lack selected provenance")
    token_start, token_end = pack.minimal_evidence_interval(offsets, evidence_start, evidence_end)
    require(len(token_labels) == len(prompt) and
            all(value == label for value in token_labels[token_start:token_end]),
            "evidence tokens lack selected eligibility")
    quartile = min(3, (2 * (token_start + token_end)) // len(prompt))
    return {"prompt_tokens": len(prompt), "quartile": quartile,
            "evidence_token_start": token_start, "evidence_token_end": token_end,
            "exact": len(prompt) + reserved == target_context}


def _validate_authority(authority, model, length):
    required = {"schema_version", "model", "session_id", "loaded_identity",
                "expected_artifact_sha256", "expected_implementation_sha256",
                "preparation_context_tokens", "reserved_output_tokens", "settings_sha256",
                "variant_settings_sha256", "source_root"}
    require(type(authority) is dict and set(authority) == required and authority["schema_version"] == 2,
            "invalid packing authority")
    require(isinstance(model, str) and model and authority["model"] == model,
            "packing authority model mismatch")
    materialize.digest(authority["session_id"], "packing session")
    materialize.digest(authority["expected_artifact_sha256"], "expected artifact")
    materialize.digest(authority["expected_implementation_sha256"], "expected implementation")
    require(authority["loaded_identity"] is None or type(authority["loaded_identity"]) is dict,
            "invalid observed loaded identity")
    require(type(authority["preparation_context_tokens"]) is int and
            authority["preparation_context_tokens"] == 2 * length and
            authority["preparation_context_tokens"] <= materialize.MAX_PROMPT_TOKENS,
            "preparation context must be exactly twice the target context")


def _observed_identity(report, expected_artifact, expected_implementation):
    identity = report.get("loaded_identity")
    require(type(identity) is dict and set(identity) == {"load_generation", "artifact_sha256",
            "tokenizer_sha256", "template_sha256", "layout_sha256", "implementation_sha256"} and
            type(identity["load_generation"]) is int and identity["load_generation"] > 0,
            "invalid observed loaded identity")
    for field in set(identity) - {"load_generation"}:
        materialize.digest(identity[field], "observed loaded " + field)
    require(identity["artifact_sha256"] == expected_artifact and
            identity["implementation_sha256"] == expected_implementation,
            "observed loaded core identity mismatch")
    return dict(identity)


class MaterializationDriverError(ValueError):
    pass


def _driver_error_report(report, request_index):
    if type(report) is not dict:
        return None
    if "completed" in report:
        require(type(report["completed"]) is bool,
                "invalid materialization driver completion")
    if report.get("completed") is not False:
        return None
    required = {"schema_version", "completed", "request_index", "error"}
    require(set(report) == required and type(report["schema_version"]) is int and
            report["schema_version"] == 2 and
            type(report["request_index"]) is int and report["request_index"] == request_index and
            type(report["error"]) is str and
            re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", report["error"]) is not None,
            "invalid materialization driver error report")
    return dict(report)


def _reject_driver_error(journal_stream, report, request_index, case_index, case_id, iteration):
    driver_error = _driver_error_report(report, request_index)
    if driver_error is None:
        return
    _append_sync(journal_stream, {
        "schema_version": 1,
        "stage": "quality_loaded_tokenizer_driver_failure",
        "completed": False,
        "case_index": case_index,
        "case_id": case_id,
        "iteration": iteration,
        "request_index": request_index,
        "driver_report": driver_error,
    })
    raise MaterializationDriverError("materialization driver error: " + driver_error["error"])


def _consume_outer_guard_witness():
    values = {name: os.environ.pop(name, None) for name in
              (GUARD_FD_ENV, GUARD_TOKEN_ENV, GUARD_OWNER_PID_ENV)}
    require(all(isinstance(value, str) and value for value in values.values()),
            "quality packer lacks outer resource-guard witness")
    try:
        fd = int(values[GUARD_FD_ENV], 10)
        owner_pid = int(values[GUARD_OWNER_PID_ENV], 10)
        token = bytes.fromhex(values[GUARD_TOKEN_ENV])
    except (ValueError, OverflowError) as exc:
        raise ValueError("invalid outer resource-guard witness") from exc
    close_fd = fd if fd > 2 else None
    try:
        require(close_fd is not None and owner_pid > 0 and owner_pid == os.getppid() and
                len(token) == GUARD_WITNESS_BYTES,
                "invalid outer resource-guard ownership")
        require(stat.S_ISFIFO(os.fstat(fd).st_mode),
                "outer resource-guard witness is not a pipe")
        os.set_blocking(fd, False)
        payload = bytearray()
        try:
            while len(payload) <= GUARD_WITNESS_BYTES:
                chunk = os.read(fd, GUARD_WITNESS_BYTES + 1 - len(payload))
                if not chunk:
                    break
                payload.extend(chunk)
        except BlockingIOError as exc:
            raise ValueError("outer resource-guard witness is not sealed") from exc
        require(bytes(payload) == token,
                "outer resource-guard witness mismatch")
    finally:
        if close_fd is not None:
            try:
                os.close(close_fd)
            except OSError:
                pass
    require(os.getpgrp() == os.getpid(),
            "quality packer must be the outer guard process-group leader")


def _require_driver_in_guard_group(process):
    require(os.getpgid(process.pid) == os.getpgrp(),
            "materialization driver escaped the outer resource guard")


def _terminate_direct_child(process):
    # The enclosing resource guard owns the group. Killing the group here would
    # signal this launcher too, so local cleanup is limited to the direct child.
    if process.poll() is not None:
        return
    for action in (process.terminate, process.kill):
        if process.poll() is not None:
            break
        try:
            action()
        except ProcessLookupError:
            break
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def _validate_restart_failed_journal(path):
    rows = [materialize._strict_json(line, "prior packing journal")
            for line in Path(path).read_bytes().splitlines()]
    require(rows and rows[-1].get("stage") == "quality_loaded_tokenizer_pack_failure" and
            rows[-1].get("completed") is False,
            "restart requires a terminal failed prior journal")
    return rows[-1]


def run_cell(work, authority, campaign_root, length, bucket, driver, journal_path, result_path,
             request_timeout, total_timeout, max_campaign_bytes=MAX_CAMPAIGN_BYTES,
             restart_failed_journal=None, driver_env=None, campaign_precreated=False):
    _validate_authority(authority, work.get("model"), length)
    require(length in pack.LENGTHS and bucket in pack.BUCKETS, "invalid packing cell")
    cell = next((row for row in work["cells"] if row["context_tokens"] == length and
                 row["bucket"] == bucket), None)
    require(cell is not None and len(cell["cases"]) == pack.CASES_PER_CELL == MAX_CELL_CASES,
            "packing cell must contain every frozen case")
    require(type(driver) is list and driver and all(isinstance(v, str) and v for v in driver),
            "invalid materialization driver")
    executable = (Path(driver[0]).resolve(strict=True) if Path(driver[0]).is_absolute() or
                  len(Path(driver[0]).parts) > 1 else
                  Path(shutil.which(driver[0]) or "").resolve(strict=True))
    require(executable.is_file() and os.access(executable, os.X_OK),
            "missing materialization driver executable")
    driver = [str(executable), *driver[1:]]
    require(type(request_timeout) in (int, float) and 0 < request_timeout <=
            materialize.MAX_REQUEST_TIMEOUT_SECONDS and type(total_timeout) in (int, float) and
            0 < total_timeout <= materialize.MAX_TOTAL_TIMEOUT_SECONDS,
            "invalid packing timeout")
    require(type(max_campaign_bytes) is int and 0 < max_campaign_bytes <= MAX_CAMPAIGN_BYTES,
            "invalid campaign byte bound")
    if restart_failed_journal is not None:
        _validate_restart_failed_journal(restart_failed_journal)
    root = Path(campaign_root)
    if campaign_precreated:
        require(root.is_dir() and not root.is_symlink(), "invalid precreated campaign root")
    else:
        root.mkdir(parents=True, exist_ok=False)
    journal = Path(journal_path)
    require(journal.parent.resolve() == root.resolve() and Path(result_path).parent.resolve() == root.resolve(),
            "journal and result must be in campaign root")
    stack = contextlib.ExitStack()
    journal_stream = stack.enter_context(journal.open("x", encoding="utf-8", newline="\n"))
    stderr_stream = stack.enter_context(Path(str(journal) + ".stderr").open("xb"))
    process = selector = None
    completed = False
    accepted = 0
    results = []
    active_case = active_control = None
    total_deadline = time.monotonic() + total_timeout
    try:
        # Consume the one-use witness inside the failure lifecycle, but before
        # the model process exists. Rejection is journaled and closes streams.
        _consume_outer_guard_witness()
        process = subprocess.Popen(driver, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, cwd=root, start_new_session=False, bufsize=0,
                                   env=driver_env)
        _require_driver_in_guard_group(process)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout.fileno(), selectors.EVENT_READ, "stdout")
        selector.register(process.stderr.fileno(), selectors.EVENT_READ, "stderr")
        stderr_count = 0
        pinned_identity = (dict(authority["loaded_identity"])
                           if authority["loaded_identity"] is not None else None)
        collector = (materialize.PersistentCollector(root, authority["session_id"], pinned_identity)
                     if pinned_identity is not None else None)
        target = length - authority["reserved_output_tokens"]
        require(target > 0, "reserved output exceeds cell context")
        for case_index, item in enumerate(cell["cases"]):
            control = _initial_control(item, target)
            active_case = {"case_index": case_index, "case_id": item["case"]["id"]}
            active_control = control
            solved = None
            for iteration in range(MAX_ITERATIONS):
                request_index = accepted
                candidate, request_case = _write_candidate(root, work, item, authority,
                                                            request_index, control)
                deadline = min(total_deadline, time.monotonic() + request_timeout)
                command = materialize.command(request_case, authority["session_id"], request_index)
                payload = _json_bytes(command)
                require(len(payload) <= materialize.MAX_DRIVER_COMMAND_BYTES,
                        "materialization command exceeds byte bound")
                stderr_count = materialize._write_command(process, selector, payload, stderr_stream,
                                                           stderr_count, deadline)
                raw, stderr_count = materialize._read_report(process, selector, stderr_stream,
                                                              stderr_count, deadline)
                report = materialize._strict_json(raw, "materialization driver")
                _reject_driver_error(journal_stream, report, request_index, case_index,
                                     item["case"]["id"], iteration)
                observed_identity = _observed_identity(
                    report, authority["expected_artifact_sha256"],
                    authority["expected_implementation_sha256"])
                if pinned_identity is None:
                    pinned_identity = observed_identity
                    collector = materialize.PersistentCollector(
                        root, authority["session_id"], pinned_identity)
                require(observed_identity == pinned_identity,
                        "loaded identity changed within persistent packing cell")
                expected = dict(request_case, session_id=authority["session_id"],
                                request_index=request_index, loaded_identity=pinned_identity)
                report = materialize.report_contract(report, root, expected)
                require(report["schema_version"] == 2,
                        "packing requires labeled materialization schema 2")
                collector.accept(report, request_case)
                observed = _observe(root, report, candidate, length,
                                    authority["reserved_output_tokens"])
                event = {"schema_version": 1, "stage": "quality_loaded_tokenizer_pack_attempt",
                         "completed": observed["exact"] and observed["quartile"] ==
                                      item["case"]["intended_compactable_quartile"],
                         "case_index": case_index, "case_id": item["case"]["id"],
                         "iteration": iteration, "request_index": request_index,
                         "messages_sha256": request_case["messages_sha256"],
                         "byte_budget": control["byte_budget"],
                         "insertion_index": control["insertion_index"],
                         "padding_units": control["padding_units"],
                         "search_phase": control["phase"],
                         "candidate": {"messages_file": request_case["messages_file"],
                             "reference_file": request_case["reference_file"],
                             "segments": candidate["segments"],
                             "primary_label": candidate["primary_label"],
                             "evidence_part_byte_start": candidate["evidence_part_byte_start"],
                             "evidence_part_byte_end": candidate["evidence_part_byte_end"]},
                         "materialization_report": report, **observed}
                _append_sync(journal_stream, event)
                accepted += 1
                require(_campaign_bytes(root) <= max_campaign_bytes,
                        "packing campaign exceeds disk byte bound")
                if event["completed"]:
                    solved = {**event, "reference_sha256": request_case["reference_sha256"],
                              "report": report, "search_samples": control["samples"] +
                                  [{"bytes": control["byte_budget"],
                                    "tokens": observed["prompt_tokens"],
                                    "insertion_index": control["insertion_index"],
                                    "padding_units": control["padding_units"]}]}
                    break
                control = pack.next_candidate_control(control, observed["prompt_tokens"],
                                                      observed["quartile"])
                active_control = control
            require(solved is not None, "packing case exhausted bounded tokenizer search")
            results.append(solved)
        stop = _json_bytes({"command": "stop", "session_id": authority["session_id"]})
        stderr_count = materialize._write_command(
            process, selector, stop, stderr_stream, stderr_count,
            min(total_deadline, time.monotonic() + request_timeout))
        process.stdin.close()
        returncode, stderr_count = materialize._wait_for_exit(
            process, selector, stderr_stream, stderr_count,
            min(total_deadline, time.monotonic() + 10))
        require(returncode == 0, "materialization driver failed")
        result = {"schema_version": 1, "stage": "quality_loaded_tokenizer_packed_cell",
                  "completed": True, "model_outputs_observed": False, "model": work["model"],
                  "context_tokens": length, "bucket": bucket,
                  "bucket_contract": BUCKET_CONTRACTS[bucket],
                  "session_id": authority["session_id"],
                  "target_context_tokens": length,
                  "preparation_context_tokens": authority["preparation_context_tokens"],
                  "loaded_identity": pinned_identity, "cases": results}
        pack._write_exclusive(result_path, _json_bytes(result))
        require(_campaign_bytes(root) <= max_campaign_bytes,
                "packing campaign exceeds disk byte bound")
        _append_sync(journal_stream, {"schema_version": 1,
            "stage": "quality_loaded_tokenizer_pack_complete", "completed": True,
            "accepted_reports": accepted, "case_count": len(results),
            "result_sha256": hashlib.sha256(Path(result_path).read_bytes()).hexdigest()})
        completed = True
        return result
    except Exception as exc:
        failure = {"schema_version": 1,
            "stage": "quality_loaded_tokenizer_pack_failure", "completed": False,
            "accepted_reports": accepted, "campaign_bytes": _campaign_bytes(root),
            "error": type(exc).__name__, "message": str(exc)[:1024]}
        if active_case is not None:
            failure["active_case"] = active_case
        if active_control is not None:
            failure["search_control"] = active_control
        _append_sync(journal_stream, failure)
        raise
    finally:
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            _terminate_direct_child(process)
            if process.stdout is not None: process.stdout.close()
            if process.stderr is not None: process.stderr.close()
        if selector is not None: selector.close()
        stack.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True)
    parser.add_argument("--authority", required=True)
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--length", required=True, type=int, choices=pack.LENGTHS)
    parser.add_argument("--bucket", required=True, choices=pack.BUCKETS)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--restart-failed-journal")
    parser.add_argument("--request-timeout", type=float, default=materialize.DEFAULT_REQUEST_TIMEOUT_SECONDS)
    parser.add_argument("--total-timeout", type=float, default=materialize.DEFAULT_TOTAL_TIMEOUT_SECONDS)
    parser.add_argument("--max-campaign-bytes", type=int, default=MAX_CAMPAIGN_BYTES)
    parser.add_argument("driver", nargs="+")
    args = parser.parse_args(argv)
    run_cell(pack.strict_json(args.work), pack.strict_json(args.authority), args.campaign_root,
             args.length, args.bucket, args.driver, args.journal, args.result,
             args.request_timeout, args.total_timeout, args.max_campaign_bytes,
             args.restart_failed_journal)


if __name__ == "__main__":
    main()
