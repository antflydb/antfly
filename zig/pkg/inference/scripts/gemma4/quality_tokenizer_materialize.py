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

"""Strict parent contract for loaded-tokenizer quality materialization evidence."""
import hashlib
import contextlib
from array import array
import json
import os
import re
import selectors
import signal
import stat
import struct
import subprocess
import sys
import time
from pathlib import Path

MAX_PROMPT_TOKENS = 131072
MAX_REFERENCE_TOKENS = 4096
MAX_DRIVER_LINE_BYTES = 65536
MAX_DRIVER_COMMAND_BYTES = 65536
MAX_MATERIALIZATION_CASES = 2000
MAX_DRIVER_STDERR_BYTES = 16 * 1024 * 1024
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_TOTAL_TIMEOUT_SECONDS = 1800
MAX_REQUEST_TIMEOUT_SECONDS = 600
MAX_TOTAL_TIMEOUT_SECONDS = 4 * 60 * 60
CASE_FIELDS = {"case_id", "messages_file", "messages_sha256", "request_content_sha256",
               "source_sha256", "selected_part_sha256s", "reference_file", "reference_sha256",
               "qualified_context_tokens", "reserved_output_tokens", "settings_sha256",
               "variant_settings_sha256"}


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(value, name):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid " + name)


def _artifact(root, ref, name, maximum_count, maximum_bytes):
    require(type(ref) is dict and set(ref) == {"path", "sha256", "count"}, "invalid " + name)
    digest(ref["sha256"], name + " digest")
    require(type(ref["count"]) is int and 0 < ref["count"] <= maximum_count,
            "invalid " + name + " count")
    require(isinstance(ref["path"], str) and ref["path"], "invalid " + name + " path")
    relative = Path(ref["path"])
    require(not relative.is_absolute() and ".." not in relative.parts, name + " path escapes campaign")
    root = Path(root).resolve(strict=True)
    current = root
    for component in relative.parts[:-1]:
        current = current / component
        require(not current.is_symlink(), name + " parent must not be a symlink")
        require(current.is_dir(), "invalid " + name + " parent")
    path = root / relative
    flags = (os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) |
             getattr(os, "O_NONBLOCK", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError("unverifiable " + name + " file") from exc
    try:
        metadata = os.fstat(descriptor)
        require(stat.S_ISREG(metadata.st_mode), name + " file must be regular")
        require(0 < metadata.st_size <= maximum_bytes, name + " exceeds byte limit")
        raw = bytearray()
        while len(raw) <= maximum_bytes:
            part = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
        require(len(raw) == metadata.st_size and len(raw) <= maximum_bytes,
                name + " changed or exceeds byte limit")
    finally:
        os.close(descriptor)
    result = bytes(raw)
    require(hashlib.sha256(result).hexdigest() == ref["sha256"], name + " file digest mismatch")
    return result


def _i64(raw, count, name):
    require(len(raw) == count * 8, "invalid " + name + " byte length")
    values = list(struct.unpack("<" + "q" * count, raw))
    require(all(0 <= value <= 2**31 - 1 for value in values), "invalid " + name + " token")
    return values


def _offsets(raw, count, rendered_bytes):
    require(len(raw) == count * 8, "invalid prompt offset byte length")
    rows = [struct.unpack_from("<II", raw, index * 8) for index in range(count)]
    previous_start = previous_end = 0
    for start, end in rows:
        require((start == 0 and end == 0) or start <= end <= rendered_bytes,
                "invalid prompt token offset")
        if start or end:
            require(start >= previous_start and end >= previous_end, "backward prompt token offset")
            previous_start, previous_end = start, end
    return rows


def _u32(raw, count, name):
    require(len(raw) == count * 4, "invalid " + name + " byte length")
    values = array("I")
    require(values.itemsize == 4, "unsupported label word size")
    values.frombytes(raw)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def reference_digest(tokens):
    require(2 <= len(tokens) <= MAX_REFERENCE_TOKENS, "invalid reference token count")
    return hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()


def command(case, session_id, request_index):
    require(type(case) is dict and set(case) == CASE_FIELDS, "invalid materialization case fields")
    digest(session_id, "materialization session")
    require(type(request_index) is int and 0 <= request_index < MAX_MATERIALIZATION_CASES and isinstance(case["case_id"], str) and
            re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", case["case_id"]), "invalid materialization request")
    for field in ("messages_sha256", "request_content_sha256", "source_sha256", "reference_sha256",
                  "settings_sha256", "variant_settings_sha256"):
        digest(case[field], field)
    for field in ("messages_file", "reference_file"):
        require(isinstance(case[field], str) and 0 < len(case[field].encode()) <= 1024,
                "invalid " + field)
        path = Path(case[field])
        require(not path.is_absolute() and ".." not in path.parts, field + " escapes campaign")
    parts = case["selected_part_sha256s"]
    require(type(parts) is list and 0 < len(parts) <= 64 and len(parts) == len(set(parts)),
            "invalid selected part identities")
    for value in parts:
        digest(value, "selected part")
    require(type(case["qualified_context_tokens"]) is int and
            0 < case["qualified_context_tokens"] <= MAX_PROMPT_TOKENS and
            type(case["reserved_output_tokens"]) is int and
            2 <= case["reserved_output_tokens"] <= MAX_REFERENCE_TOKENS and
            case["reserved_output_tokens"] < case["qualified_context_tokens"],
            "invalid materialization context limits")
    return {"command": "materialize_quality_v1", "session_id": session_id,
            "request_index": request_index, "variant": "attention_matching",
            "case_id": case["case_id"], "messages_file": case["messages_file"],
            "messages_sha256": case["messages_sha256"],
            "request_content_sha256": case["request_content_sha256"],
            "source_sha256": case["source_sha256"],
            "selected_part_sha256s": case["selected_part_sha256s"],
            "reference_file": case["reference_file"], "reference_sha256": case["reference_sha256"],
            "reserved_output_tokens": case["reserved_output_tokens"],
            "settings_sha256": case["settings_sha256"],
            "variant_settings_sha256": case["variant_settings_sha256"]}


def report_contract(report, root, expected):
    base_fields = {
        "schema_version", "stage", "completed", "model_outputs_observed", "model_forward_steps",
        "session_id", "request_index", "case_id", "messages_sha256", "request_content_sha256",
        "source_sha256", "selected_part_sha256s", "reference_sha256", "qualified_context_tokens",
        "reserved_output_tokens", "settings_sha256", "variant_settings_sha256", "loaded_identity",
        "rendered_prompt", "prompt_tokens", "prompt_token_offsets", "combined_tokens",
        "reference_tokens", "reference_sequence_sha256", "stable_prefix_verified",
        "reference_roundtrip_verified",
    }
    require(type(report) is dict and type(report.get("schema_version")) is int and
            report["schema_version"] in (1, 2), "invalid tokenizer materialization schema")
    label_fields = {"rendered_labels", "prompt_token_labels"} if report["schema_version"] == 2 else set()
    require(set(report) == base_fields | label_fields,
            "invalid tokenizer materialization report fields")
    require(report["stage"] == "quality_tokenizer_materialization" and
            report["completed"] is True and report["model_outputs_observed"] is False and
            type(report["model_forward_steps"]) is int and report["model_forward_steps"] == 0,
            "invalid tokenizer materialization completion")
    for field in ("session_id", "case_id", "messages_sha256", "request_content_sha256", "source_sha256",
                  "reference_sha256", "qualified_context_tokens", "reserved_output_tokens", "settings_sha256",
                  "variant_settings_sha256"):
        require(report[field] == expected[field], "materialization binding mismatch: " + field)
    require(type(report["request_index"]) is int and report["request_index"] == expected["request_index"] and
            report["selected_part_sha256s"] == expected["selected_part_sha256s"],
            "materialization request binding mismatch")
    identity = report["loaded_identity"]
    require(type(identity) is dict and set(identity) == {"load_generation", "artifact_sha256", "tokenizer_sha256",
            "template_sha256", "layout_sha256", "implementation_sha256"} and
            type(identity["load_generation"]) is int and identity["load_generation"] > 0,
            "invalid loaded materialization identity")
    for field in set(identity) - {"load_generation"}:
        digest(identity[field], "loaded " + field)
    require(identity == expected["loaded_identity"], "loaded materialization identity mismatch")
    rendered = _artifact(root, report["rendered_prompt"], "rendered prompt",
                         32 * 1024 * 1024, 32 * 1024 * 1024)
    require(report["rendered_prompt"]["count"] == len(rendered), "invalid rendered prompt byte count")
    if report["schema_version"] == 2:
        rendered_labels_raw = _artifact(root, report["rendered_labels"], "rendered labels",
                                        32 * 1024 * 1024, 32 * 1024 * 1024 * 4)
        rendered_labels = _u32(rendered_labels_raw, report["rendered_labels"]["count"], "rendered labels")
        require(report["rendered_labels"]["count"] == len(rendered), "rendered label count mismatch")
        require(all(label <= len(expected["selected_part_sha256s"]) for label in rendered_labels),
                "rendered label exceeds selected part count")
    prompt_raw = _artifact(root, report["prompt_tokens"], "prompt tokens",
                           MAX_PROMPT_TOKENS, MAX_PROMPT_TOKENS * 8)
    prompt = _i64(prompt_raw, report["prompt_tokens"]["count"], "prompt")
    offsets_raw = _artifact(root, report["prompt_token_offsets"], "prompt token offsets",
                            MAX_PROMPT_TOKENS, MAX_PROMPT_TOKENS * 8)
    require(report["prompt_token_offsets"]["count"] == len(prompt),
            "prompt token offset count mismatch")
    _offsets(offsets_raw, len(prompt), len(rendered))
    if report["schema_version"] == 2:
        prompt_labels_raw = _artifact(root, report["prompt_token_labels"], "prompt token labels",
                                      MAX_PROMPT_TOKENS, MAX_PROMPT_TOKENS * 4)
        prompt_labels = _u32(prompt_labels_raw, report["prompt_token_labels"]["count"], "prompt token labels")
        require(report["prompt_token_labels"]["count"] == len(prompt),
                "prompt token label count mismatch")
        require(all(label <= len(expected["selected_part_sha256s"]) for label in prompt_labels),
                "prompt label exceeds selected part count")
    combined_raw = _artifact(root, report["combined_tokens"], "combined tokens",
                             MAX_PROMPT_TOKENS + MAX_REFERENCE_TOKENS,
                             (MAX_PROMPT_TOKENS + MAX_REFERENCE_TOKENS) * 8)
    combined = _i64(combined_raw, report["combined_tokens"]["count"], "combined")
    reference_raw = _artifact(root, report["reference_tokens"], "reference tokens",
                              MAX_REFERENCE_TOKENS, MAX_REFERENCE_TOKENS * 12 + 2)
    try:
        reference = json.loads(reference_raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid reference token JSON") from exc
    require(type(reference) is list and all(type(value) is int and 0 <= value <= 2**31 - 1 for value in reference),
            "invalid reference token JSON")
    require(report["reference_tokens"]["count"] == len(reference) and 2 <= len(reference) <=
            min(report["reserved_output_tokens"], MAX_REFERENCE_TOKENS), "invalid reference continuation length")
    require(len(prompt) + report["reserved_output_tokens"] <= report["qualified_context_tokens"],
            "materialized prompt exceeds qualified context")
    require(combined == prompt + reference, "one-shot encoding does not preserve the prompt prefix")
    digest(report["reference_sequence_sha256"], "reference sequence")
    require(reference_digest(reference) == report["reference_sequence_sha256"],
            "reference sequence digest mismatch")
    require(report["stable_prefix_verified"] is True and report["reference_roundtrip_verified"] is True,
            "missing loaded-tokenizer verification")
    return report


class PersistentCollector:
    """Validates one ordered stream from a single pinned loaded generation."""

    def __init__(self, root, session_id, loaded_identity):
        digest(session_id, "materialization session")
        require(type(loaded_identity) is dict, "missing expected loaded identity")
        self.root = Path(root)
        self.session_id = session_id
        self.loaded_identity = dict(loaded_identity)
        self.next_index = 0

    def accept(self, report, case):
        expected = dict(case)
        expected.update(session_id=self.session_id, request_index=self.next_index,
                        loaded_identity=self.loaded_identity)
        result = report_contract(report, self.root, expected)
        self.next_index += 1
        return result


def _strict_json(raw, label):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, "duplicate " + label + " JSON field: " + key)
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid " + label + " JSON") from exc


def _terminate_group(process, force=False):
    if process.poll() is not None and not force:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        if process.poll() is None:
            process.wait(timeout=2)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass
    deadline = time.monotonic() + 2
    while force and time.monotonic() < deadline:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.02)
    if process.poll() is None or force:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        process.wait(timeout=2)


def _read_report(process, selector, stderr_stream, stderr_count, deadline):
    output = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        require(remaining > 0, "materialization driver request timed out")
        events = selector.select(remaining)
        require(events, "materialization driver request timed out")
        for key, _ in events:
            chunk = os.read(key.fd, 65536)
            if key.data == "stderr":
                if not chunk:
                    selector.unregister(key.fd)
                    continue
                stderr_count += len(chunk)
                require(stderr_count <= MAX_DRIVER_STDERR_BYTES, "materialization driver stderr exceeds byte limit")
                stderr_stream.write(chunk)
                stderr_stream.flush()
                continue
            require(chunk, "materialization driver closed stdout")
            output.extend(chunk)
            require(len(output) <= MAX_DRIVER_LINE_BYTES, "materialization driver report exceeds byte limit")
            if b"\n" in output:
                line, tail = output.split(b"\n", 1)
                require(not tail, "materialization driver sent an unsolicited report")
                return bytes(line), stderr_count


def _write_command(process, selector, payload, stderr_stream, stderr_count, deadline):
    descriptor = process.stdin.fileno()
    os.set_blocking(descriptor, False)
    selector.register(descriptor, selectors.EVENT_WRITE, "stdin")
    written = 0
    try:
        while written < len(payload):
            remaining = deadline - time.monotonic()
            require(remaining > 0, "materialization driver request timed out")
            events = selector.select(remaining)
            require(events, "materialization driver request timed out")
            for key, _ in events:
                if key.data == "stdin":
                    try:
                        count = os.write(descriptor, payload[written:])
                    except BlockingIOError:
                        continue
                    require(count > 0, "materialization driver closed stdin")
                    written += count
                    continue
                chunk = os.read(key.fd, 65536)
                if key.data == "stdout":
                    require(not chunk, "materialization driver sent an unsolicited report")
                    raise ValueError("materialization driver closed stdout")
                if not chunk:
                    selector.unregister(key.fd)
                    continue
                stderr_count += len(chunk)
                require(stderr_count <= MAX_DRIVER_STDERR_BYTES,
                        "materialization driver stderr exceeds byte limit")
                stderr_stream.write(chunk)
                stderr_stream.flush()
        return stderr_count
    finally:
        try:
            selector.unregister(descriptor)
        except KeyError:
            pass


def _wait_for_exit(process, selector, stderr_stream, stderr_count, deadline):
    while process.poll() is None:
        remaining = deadline - time.monotonic()
        require(remaining > 0, "materialization driver total timeout")
        events = selector.select(min(remaining, 0.1))
        for key, _ in events:
            chunk = os.read(key.fd, 65536)
            if not chunk:
                selector.unregister(key.fd)
                continue
            if key.data == "stdout":
                raise ValueError("materialization driver sent an unsolicited report")
            stderr_count += len(chunk)
            require(stderr_count <= MAX_DRIVER_STDERR_BYTES,
                    "materialization driver stderr exceeds byte limit")
            stderr_stream.write(chunk)
            stderr_stream.flush()
    while True:
        events = selector.select(0)
        if not events:
            break
        for key, _ in events:
            chunk = os.read(key.fd, 65536)
            if not chunk:
                selector.unregister(key.fd)
                continue
            if key.data == "stdout":
                raise ValueError("materialization driver sent an unsolicited report")
            stderr_count += len(chunk)
            require(stderr_count <= MAX_DRIVER_STDERR_BYTES,
                    "materialization driver stderr exceeds byte limit")
            stderr_stream.write(chunk)
    stderr_stream.flush()
    os.fsync(stderr_stream.fileno())
    return process.returncode, stderr_count


def collect(root, cases, driver_command, journal_path, session_id, loaded_identity,
            request_timeout_seconds=DEFAULT_REQUEST_TIMEOUT_SECONDS,
            total_timeout_seconds=DEFAULT_TOTAL_TIMEOUT_SECONDS):
    """Run a bounded persistent child and retain its exact validated JSONL reports."""
    require(type(cases) is list and 0 < len(cases) <= MAX_MATERIALIZATION_CASES,
            "invalid materialization case count")
    require(type(driver_command) is list and driver_command and
            all(isinstance(value, str) and value for value in driver_command), "invalid driver command")
    require(type(request_timeout_seconds) in (int, float) and
            0 < request_timeout_seconds <= MAX_REQUEST_TIMEOUT_SECONDS and
            type(total_timeout_seconds) in (int, float) and
            0 < total_timeout_seconds <= MAX_TOTAL_TIMEOUT_SECONDS,
            "invalid materialization timeout")
    collector = PersistentCollector(root, session_id, loaded_identity)
    journal = Path(journal_path)
    stack = contextlib.ExitStack()
    try:
        journal_stream = stack.enter_context(journal.open("x", encoding="utf-8", newline="\n"))
        stderr_stream = stack.enter_context(Path(str(journal_path) + ".stderr").open("xb"))
    except Exception:
        stack.close()
        raise
    process = None
    selector = None
    reports = []
    accepted = 0
    completed = False
    total_deadline = time.monotonic() + total_timeout_seconds
    try:
        process = subprocess.Popen(driver_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, cwd=root, start_new_session=True, bufsize=0)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout.fileno(), selectors.EVENT_READ, "stdout")
        selector.register(process.stderr.fileno(), selectors.EVENT_READ, "stderr")
        stderr_count = 0
        for index, case in enumerate(cases):
            request = command(case, session_id, index)
            deadline = min(total_deadline, time.monotonic() + request_timeout_seconds)
            payload = (json.dumps(request, allow_nan=False, sort_keys=True) + "\n").encode()
            require(len(payload) <= MAX_DRIVER_COMMAND_BYTES,
                    "materialization driver command exceeds byte limit")
            stderr_count = _write_command(
                process, selector, payload, stderr_stream, stderr_count, deadline)
            raw, stderr_count = _read_report(process, selector, stderr_stream, stderr_count, deadline)
            report = collector.accept(_strict_json(raw, "materialization driver"), case)
            reports.append(report)
            journal_stream.write(json.dumps(report, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n")
            journal_stream.flush()
            os.fsync(journal_stream.fileno())
            accepted += 1
        stop = (json.dumps({"command": "stop", "session_id": session_id}, sort_keys=True) + "\n").encode()
        stderr_count = _write_command(
            process, selector, stop, stderr_stream, stderr_count,
            min(total_deadline, time.monotonic() + request_timeout_seconds))
        process.stdin.close()
        returncode, stderr_count = _wait_for_exit(
            process, selector, stderr_stream, stderr_count,
            min(total_deadline, time.monotonic() + 10))
        require(returncode == 0, "materialization driver failed")
        completed = True
        return reports
    except Exception as exc:
        failure = {"schema_version": 1, "stage": "quality_tokenizer_materialization_failure",
                   "completed": False, "accepted_reports": accepted,
                   "error": type(exc).__name__, "message": str(exc)[:1024]}
        journal_stream.write(json.dumps(failure, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n")
        journal_stream.flush()
        os.fsync(journal_stream.fileno())
        raise
    finally:
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            _terminate_group(process, force=not completed)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        if selector is not None:
            selector.close()
        stack.close()


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("driver", nargs="+")
    args = parser.parse_args(argv)
    plan = _strict_json(Path(args.plan).read_bytes(), "materialization plan")
    require(type(plan) is dict and set(plan) == {"schema_version", "session_id", "loaded_identity", "cases"} and
            type(plan["schema_version"]) is int and plan["schema_version"] == 1,
            "invalid materialization plan")
    collect(args.campaign_root, plan["cases"], args.driver, args.journal,
            plan["session_id"], plan["loaded_identity"])


if __name__ == "__main__":
    main()
