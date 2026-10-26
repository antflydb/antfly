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

"""Run and audit guarded Gemma4 exact-context observations.

The native test probe emits `context_exact_observation: {json}`. Collection
remains diagnostic even after the runtime supplies loaded-byte identity. The
audit separately requires complete allocation counters and a qualified corpus.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import time

import context_release_build_adapter as release_build

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_resources import group_rss_mib, memory_free_percent, swapout_bytes

from release_attention_matching import (MAX_REPORT, MIN_PERFORMANCE_PAIRS,
    MAX_JOURNAL_BYTES, bootstrap_bound, campaign_file, decode_json, journal_records,
    number, provenance, read_json, require, semantic_identity, sha, write_json)

PREFIX = "context_exact_observation: "
STORAGES = {"automatic", "packed_exact"}
ENV_PREFIX_ALLOWLIST = ("ANTFLY_", "TERMITE_", "ZIG_")
ENV_NAME_ALLOWLIST = {"NO_COLOR"}
LOADED_DIGESTS = {
    "artifact_sha256": "artifact",
    "tokenizer_sha256": "tokenizer",
    "template_sha256": "template",
    "layout_sha256": "layout",
    "implementation_sha256": "implementation",
}
LOADED_IDENTITY_FIELDS = {"load_generation"} | set(LOADED_DIGESTS)


def parse_observations(output):
    require(isinstance(output, str), "observation output is not text")
    rows = []
    for line in output.splitlines():
        if line.startswith(PREFIX):
            raw = line[len(PREFIX):]
            require(len(raw.encode()) <= 65536, "observation line exceeds byte limit")
            rows.append(decode_json(raw))
    require(rows, "driver emitted no context observation")
    return rows


def observation_contract(row):
    require(type(row) is dict, "context observation is not an object")
    require(row.get("schema_version") == 2, "unsupported context observation")
    require(row.get("route") == "native_pipeline", "unexpected diagnostic route")
    require(row.get("storage") in STORAGES, "unexpected context storage")
    for key in ("prompt_tokens", "reserved_output_tokens", "produced_output_tokens",
                "total_positions", "prefill_chunk_rows", "elapsed_ns", "callback_count"):
        require(type(row.get(key)) is int and row[key] >= 0, "invalid observation field: " + key)
    require(row["prompt_tokens"] + row["produced_output_tokens"] == row["total_positions"], "inconsistent total positions")
    require(row["prompt_tokens"] + row["reserved_output_tokens"] >= row["total_positions"], "output exceeded reservation")
    require(row["prefill_chunk_rows"] > 0 and row["elapsed_ns"] > 0, "invalid timing geometry")
    require(isinstance(row.get("output_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", row["output_sha256"]), "invalid output digest")
    first = row.get("first_callback_ns")
    require(first is None or type(first) is int and 0 < first <= row["elapsed_ns"], "invalid first callback timing")
    phases = row.get("phases_ms")
    require(type(phases) is dict and all(isinstance(key, str) and
                                         ((value is None and key == "first_token") or
                                          (type(value) in (int, float) and math.isfinite(value) and value >= 0))
                                         for key, value in phases.items()), "invalid phase timing")
    for key in ("context_live_bytes", "context_peak_bytes", "backend_kv_readback_bytes"):
        require(row.get(key) is None or type(row[key]) is int and row[key] >= 0, "invalid allocation counter: " + key)
    loaded_identity_contract(row.get("loaded_identity"))
    return row


def loaded_identity_contract(value):
    """Validate the identity emitted from the same loaded model generation."""
    require(type(value) is dict and set(value) == LOADED_IDENTITY_FIELDS,
            "invalid loaded identity fields")
    generation = value["load_generation"]
    require(type(generation) is int and 0 < generation <= 0xffffffffffffffff,
            "invalid loaded identity generation")
    for field in LOADED_DIGESTS:
        require(isinstance(value[field], str) and re.fullmatch(r"[0-9a-f]{64}", value[field]),
                "invalid loaded identity digest: " + field)
    return value


def bind_loaded_identities(entries, expected_artifact_sha256=None, identity=None):
    """Derive the semantic binding from raw observations; never trust a receipt flag."""
    require(type(entries) is list and entries, "missing exact-context observations")
    expected = None
    for entry in entries:
        require(type(entry) is dict, "invalid exact-context observation entry")
        row = observation_contract(entry.get("observation"))
        loaded = row["loaded_identity"]
        current = {semantic: loaded[field] for field, semantic in LOADED_DIGESTS.items()}
        if expected is None:
            expected = current
        require(current == expected, "loaded identity changed across observations")
        if expected_artifact_sha256 is not None:
            require(loaded["artifact_sha256"] == expected_artifact_sha256,
                    "loaded artifact does not match requested artifact")
        if identity is not None:
            for field, semantic in LOADED_DIGESTS.items():
                require(loaded[field] == identity[semantic],
                        "loaded identity does not match semantic identity: " + semantic)
    return expected


def paired_variants(entries, label):
    """Return complete pairs without silently overwriting or dropping a sample."""
    pairs = {}
    for entry in entries:
        pair_id = entry.get("pair_id")
        require(isinstance(pair_id, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", pair_id),
                "invalid " + label + " pair id")
        storage = entry["observation"]["storage"]
        pair = pairs.setdefault(pair_id, {})
        require(storage not in pair, "duplicate " + label + " pair variant")
        pair[storage] = entry["observation"]
    require(all(set(pair) == STORAGES for pair in pairs.values()),
            "incomplete " + label + " pair variants")
    require(len(entries) == len(pairs) * len(STORAGES),
            label + " raw/sample count mismatch")
    return list(pairs.values())


def _terminate(process):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=2)
            return
        except subprocess.TimeoutExpired:
            pass


def _run_plan(plan, output, timeout=900, max_rss_mib=8192):
    number(timeout, "exact-context timeout", .001)
    number(max_rss_mib, "exact-context RSS cap", 1)
    require(plan.get("schema_version") == 1, "unsupported exact-context run plan")
    binary, artifact, source = (Path(plan[key]).resolve() for key in ("binary", "artifact", "source_build_receipt"))
    require(binary.is_file() and os.access(binary, os.X_OK), "missing executable binary")
    require(artifact.is_file() and source.is_file(), "missing artifact or source receipt")
    binary_sha256 = sha(binary)
    build = release_build.validate(source, binary)
    require(build["binary_sha256"] == binary_sha256,
            "binary does not match full release build receipt")
    runs = plan.get("runs", [])
    require(runs and len(runs) <= 256, "invalid exact-context run count")
    require(len({run.get("id") for run in runs}) == len(runs), "duplicate exact-context run id")
    receipt = {
        "schema_version": 1,
        "stage": "exact_context_diagnostic",
        "completed": False,
        "diagnostic_only": True,
        "evidence_passed": False,
        "serving_qualified": False,
        "loaded_identity_verified": False,
        "identity_note": "loaded identity has not yet been observed",
        "file_provenance": {"artifact_sha256": sha(artifact), "binary_sha256": binary_sha256,
                            "source_build_receipt_sha256": sha(source)},
        "observations": [],
    }
    baseline_swap = swapout_bytes()
    journal_path = Path(output).with_suffix(Path(output).suffix + ".observations.jsonl")
    events_path = Path(output).with_suffix(Path(output).suffix + ".events.jsonl")
    plan_path = Path(output).with_suffix(Path(output).suffix + ".plan.json")
    with plan_path.open("x") as stream:
        json.dump(plan, stream, allow_nan=False, sort_keys=True)
        stream.write("\n")
    receipt["collector_sha256"] = sha(__file__)
    receipt["limits"] = {"timeout_seconds": timeout, "max_rss_mib": max_rss_mib,
                         "min_free_percent": 15, "min_disk_free_mib": 4096,
                         "max_swap_growth_bytes": 0}
    epoch = time.monotonic_ns()
    def limit_files():
        import resource
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_REPORT, MAX_REPORT))

    with journal_path.open("x") as journal, events_path.open("x") as events:
        def emit(kind, **fields):
            require(events.tell() <= MAX_JOURNAL_BYTES - 65536, "exact-context journal exceeds byte limit")
            events.write(json.dumps({"event": kind, "elapsed_ns": time.monotonic_ns() - epoch,
                                     **fields}, allow_nan=False, sort_keys=True) + "\n")
            events.flush()

        emit("start", binary_sha256=binary_sha256, baseline_swapout_bytes=baseline_swap)
        for spec in runs:
            argv = spec.get("argv")
            require(isinstance(argv, list) and argv and all(isinstance(x, str) and x for x in argv), "run argv must be a nonempty string array")
            require(Path(argv[0]).resolve() == binary, "run argv does not use bound binary")
            require(isinstance(spec.get("id"), str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", spec["id"]), "invalid run id")
            overlay = spec.get("env", {})
            require(type(overlay) is dict and all(isinstance(key, str) and isinstance(value, str) and len(value) <= 4096 and
                                                  (key in ENV_NAME_ALLOWLIST or key.startswith(ENV_PREFIX_ALLOWLIST))
                                                  for key, value in overlay.items()), "run environment is not allowlisted")
            run_env = os.environ.copy()
            run_env.update(overlay)
            before_free = memory_free_percent()
            min_free = before_free
            min_disk = shutil.disk_usage(Path(output).parent).free / 1048576
            require(before_free >= 15, "free-memory guard refused launch")
            require(min_disk >= 4096, "free-disk guard refused launch")
            stdout_path = Path(output).with_name(Path(output).name + "." + spec["id"] + ".stdout.log")
            stderr_path = Path(output).with_name(Path(output).name + "." + spec["id"] + ".stderr.log")
            with stdout_path.open("xb") as stdout_stream, stderr_path.open("xb") as stderr_stream:
                process = subprocess.Popen(argv, stdout=stdout_stream, stderr=stderr_stream,
                                           start_new_session=True, env=run_env, preexec_fn=limit_files)
                started = time.monotonic()
                peak_rss = 0.0
                emit("command", id=spec["id"], pid=process.pid,
                     initial_free_percent=before_free, initial_disk_free_mib=min_disk)
                try:
                    while True:
                        require(time.monotonic() - started <= timeout, "exact-context run timed out")
                        rss = group_rss_mib(process.pid)
                        free = memory_free_percent()
                        disk = shutil.disk_usage(Path(output).parent).free / 1048576
                        swap = swapout_bytes()
                        peak_rss = max(peak_rss, rss)
                        min_free = min(min_free, free)
                        min_disk = min(min_disk, disk)
                        emit("resource", id=spec["id"], group_rss_mib=rss, free_percent=free,
                             disk_free_mib=disk, swapout_bytes=swap)
                        require(peak_rss <= max_rss_mib, "exact-context RSS guard failed")
                        require(min_free >= 15 and min_disk >= 4096 and swap == baseline_swap,
                                "exact-context resource guard failed")
                        require(stdout_path.stat().st_size <= MAX_REPORT and stderr_path.stat().st_size <= MAX_REPORT,
                                "driver output exceeds byte limit")
                        if process.poll() is not None:
                            break
                        time.sleep(.25)
                    require(process.returncode == 0, "exact-context driver failed")
                    require(group_rss_mib(process.pid) == 0, "exact-context driver left live descendants")
                    emit("exit", id=spec["id"], returncode=process.returncode, remaining_group_rss_mib=0)
                except BaseException:
                    _terminate(process)
                    raise
            stdout = stdout_path.read_text(errors="replace")
            stderr = stderr_path.read_text(errors="replace")
            require(len(stdout.encode()) <= MAX_REPORT and len(stderr.encode()) <= MAX_REPORT, "driver output exceeds byte limit")
            rows = parse_observations(stdout + "\n" + stderr)
            require(len(rows) == 1, "run must emit exactly one context observation")
            row = observation_contract(rows[0])
            require(row["loaded_identity"]["artifact_sha256"] == receipt["file_provenance"]["artifact_sha256"],
                    "loaded artifact does not match requested artifact")
            expected = spec.get("expected", {})
            for key in ("storage", "prompt_tokens", "reserved_output_tokens"):
                require(row.get(key) == expected.get(key), "observation does not match expected " + key)
            observation = {"id": spec["id"], "pair_id": spec.get("pair_id"), "warmup": spec.get("warmup") is True,
                           "observation": row, "elapsed_wall_seconds": time.monotonic() - started,
                           "peak_group_rss_mib": peak_rss, "min_free_percent": min_free,
                           "min_disk_free_mib": min_disk,
                           "swapout_growth_bytes": max(swapout_bytes() - baseline_swap, 0),
                           "stdout_sha256": sha(stdout_path), "stderr_sha256": sha(stderr_path),
                           "stdout": {"path": stdout_path.name, "sha256": sha(stdout_path)},
                           "stderr": {"path": stderr_path.name, "sha256": sha(stderr_path)}}
            receipt["observations"].append(observation)
            emit("observation", id=spec["id"], stdout=observation["stdout"], stderr=observation["stderr"])
            journal.write(json.dumps(observation, allow_nan=False, sort_keys=True) + "\n")
            journal.flush()
            os.fsync(journal.fileno())
        require(sha(binary) == binary_sha256 and sha(source) == receipt["file_provenance"]["source_build_receipt_sha256"],
                "exact-context executable provenance changed during collection")
        emit("complete", count=len(runs))
        os.fsync(events.fileno())
    receipt["raw_files"] = {"plan": {"path": plan_path.name, "sha256": sha(plan_path)},
                            "events": {"path": events_path.name, "sha256": sha(events_path)}}
    receipt["observation_journal_sha256"] = sha(journal_path)
    receipt["loaded_semantic_identity"] = bind_loaded_identities(
        receipt["observations"], receipt["file_provenance"]["artifact_sha256"])
    receipt["loaded_identity_verified"] = True
    receipt["identity_note"] = "raw observations agree on loaded semantic identity and requested artifact bytes"
    receipt["completed"] = True
    write_json(output, receipt)
    return receipt


def raw_contract(raw, root, identity, origin):
    """Reconstruct exact observations from executable output and parent samples.

    Historical receipts without this journal remain useful diagnostics. Summary
    pass bits, timing values, output hashes and resource minima are not authority.
    """
    root = Path(root).resolve()
    require(type(raw) is dict and set(raw) == {"session"}, "invalid raw exact evidence")
    path = campaign_file(root, raw["session"], "exact session", MAX_REPORT)
    session = read_json(path)
    require(session.get("completed") is True and session.get("collector_sha256") == sha(__file__),
            "raw exact collector incomplete or stale")
    provenance(origin)
    semantic_identity(identity)
    files = session.get("raw_files")
    require(type(files) is dict and set(files) == {"plan", "events"}, "missing raw exact files")
    plan_path = campaign_file(path.parent, files["plan"], "exact plan", MAX_REPORT)
    events_path = campaign_file(path.parent, files["events"], "exact events", MAX_JOURNAL_BYTES)
    plan = read_json(plan_path)
    runs = plan.get("runs")
    require(plan.get("schema_version") == 1 and type(runs) is list and 0 < len(runs) <= 256,
            "invalid raw exact plan")
    require(len({run.get("id") for run in runs}) == len(runs), "duplicate raw exact planned run")
    source = session.get("file_provenance", {})
    require(source.get("binary_sha256") == origin["binary_sha256"] and
            source.get("source_build_receipt_sha256") == origin["source_build_sha256"] and
            source.get("artifact_sha256") == identity["artifact"], "raw exact provenance mismatch")
    limits = session.get("limits")
    require(type(limits) is dict and set(limits) == {"timeout_seconds", "max_rss_mib", "min_free_percent",
                                                   "min_disk_free_mib", "max_swap_growth_bytes"},
            "invalid raw exact resource limits")
    timeout = number(limits["timeout_seconds"], "raw exact timeout", .001)
    rss_cap = number(limits["max_rss_mib"], "raw exact RSS limit", 1)
    require(15 <= number(limits["min_free_percent"], "raw exact free limit") <= 100 and
            number(limits["min_disk_free_mib"], "raw exact disk limit") >= 4096 and
            type(limits["max_swap_growth_bytes"]) is int and limits["max_swap_growth_bytes"] == 0,
            "insufficient raw exact resource limits")
    rows = []
    previous, sample_ns, started_ns = -1, None, None
    active, exited, complete, baseline = None, False, False, None
    peak, free_min, disk_min = 0, 100, math.inf
    for event in journal_records(events_path):
        require(type(event) is dict and type(event.get("elapsed_ns")) is int and
                previous <= event["elapsed_ns"] and event["elapsed_ns"] >= 0 and not complete,
                "invalid raw exact event clock")
        now = event["elapsed_ns"]
        previous = now
        kind = event.get("event")
        if kind == "start":
            require(baseline is None and active is None and not rows and
                    type(event.get("baseline_swapout_bytes")) is int and event["baseline_swapout_bytes"] >= 0 and
                    event.get("binary_sha256") == origin["binary_sha256"], "invalid raw exact start")
            baseline = event["baseline_swapout_bytes"]
        elif kind == "command":
            require(baseline is not None and active is None and len(rows) < len(runs), "invalid raw exact command")
            active = runs[len(rows)]
            require(event.get("id") == active.get("id") and type(event.get("pid")) is int and event["pid"] > 0,
                    "raw exact command/plan mismatch")
            argv = active.get("argv")
            require(type(argv) is list and argv and argv[0] == plan.get("binary") and
                    all(isinstance(arg, str) and arg for arg in argv), "invalid raw exact executable")
            free_min = number(event.get("initial_free_percent"), "raw exact initial memory")
            disk_min = number(event.get("initial_disk_free_mib"), "raw exact initial disk")
            require(limits["min_free_percent"] <= free_min <= 100 and disk_min >= limits["min_disk_free_mib"],
                    "raw exact launch resource guard failed")
            peak, sample_ns, started_ns, exited = 0, None, now, False
        elif kind == "resource":
            require(active is not None and not exited and event.get("id") == active["id"], "unexpected raw exact resource")
            require(now - (sample_ns if sample_ns is not None else started_ns) <= 2_000_000_000,
                    "raw exact resource sampling gap")
            sample_ns = now
            peak = max(peak, number(event.get("group_rss_mib"), "raw exact measured RSS"))
            free_min = min(free_min, number(event.get("free_percent"), "raw exact measured memory"))
            disk_min = min(disk_min, number(event.get("disk_free_mib"), "raw exact measured disk"))
            require(type(event.get("swapout_bytes")) is int and event["swapout_bytes"] == baseline and
                    peak <= rss_cap and limits["min_free_percent"] <= free_min <= 100 and
                    disk_min >= limits["min_disk_free_mib"], "raw exact resource guard failed")
        elif kind == "exit":
            require(active is not None and not exited and event.get("id") == active["id"] and
                    type(event.get("returncode")) is int and event["returncode"] == 0 and
                    type(event.get("remaining_group_rss_mib")) is int and event["remaining_group_rss_mib"] == 0 and
                    sample_ns is not None and now - sample_ns <= 2_000_000_000 and
                    now - started_ns <= timeout * 1e9, "raw exact process completion failed")
            exited = True
        elif kind == "observation":
            require(active is not None and exited and event.get("id") == active["id"], "unexpected raw exact observation")
            outputs = [campaign_file(path.parent, event.get(key), "exact " + key, MAX_REPORT)
                       for key in ("stdout", "stderr")]
            observations = parse_observations("\n".join(output.read_text(errors="strict") for output in outputs))
            require(len(observations) == 1, "raw exact run did not emit one observation")
            observation = observation_contract(observations[0])
            for key in ("storage", "prompt_tokens", "reserved_output_tokens"):
                require(observation[key] == active.get("expected", {}).get(key), "raw exact expected observation mismatch")
            require(observation["elapsed_ns"] <= now - started_ns, "raw exact child timing exceeds parent duration")
            rows.append({"id": active["id"], "pair_id": active.get("pair_id"), "warmup": active.get("warmup") is True,
                         "observation": observation, "min_free_percent": free_min, "min_disk_free_mib": disk_min,
                         "peak_group_rss_mib": peak, "swapout_growth_bytes": 0})
            active = None
        elif kind == "complete":
            require(active is None and len(rows) == len(runs) and type(event.get("count")) is int and
                    event["count"] == len(rows), "incomplete raw exact campaign")
            complete = True
        else:
            raise ValueError("unknown raw exact event")
    require(complete and active is None, "incomplete raw exact journal")
    summaries = session.get("observations")
    require(type(summaries) is list and len(summaries) == len(rows) and
            all(all(summary.get(key) == value for key, value in row.items()) for summary, row in zip(summaries, rows)),
            "raw exact summary does not match journal")
    bind_loaded_identities(rows, identity["artifact"], identity)
    return {"schema_version": 1, "completed": True, "file_provenance": source, "observations": rows}


def run_plan(plan, output, timeout=900, max_rss_mib=8192):
    """Collect diagnostics and always leave a terminal receipt after planning succeeds."""
    try:
        return _run_plan(plan, output, timeout, max_rss_mib)
    except BaseException as error:
        if not Path(output).exists():
            journal_path = Path(output).with_suffix(Path(output).suffix + ".observations.jsonl")
            partial = []
            if journal_path.exists():
                with journal_path.open() as journal:
                    partial = [json.loads(line) for line in journal if line.strip()]
            write_json(output, {"schema_version": 1, "stage": "exact_context_diagnostic",
                                "completed": False, "diagnostic_only": True,
                                "evidence_passed": False, "serving_qualified": False,
                                "failure": str(error), "observations": partial,
                                "observation_journal_sha256": sha(journal_path) if journal_path.exists() else None})
        raise


def audit(receipt, identity, origin, qualified_context_tokens):
    semantic_identity(identity)
    provenance(origin)
    require(receipt.get("schema_version") == 1, "unsupported exact-context receipt")
    require(receipt.get("completed") is True, "incomplete exact-context collection")
    require(type(qualified_context_tokens) is list and qualified_context_tokens and
            len(set(qualified_context_tokens)) == len(qualified_context_tokens) and
            all(type(value) is int and value >= 8192 for value in qualified_context_tokens),
            "invalid qualified context lengths")
    rows = receipt.get("observations", [])
    require(rows, "missing exact-context observations")
    file_provenance = receipt.get("file_provenance")
    require(type(file_provenance) is dict and
            isinstance(file_provenance.get("artifact_sha256"), str) and
            re.fullmatch(r"[0-9a-f]{64}", file_provenance["artifact_sha256"]),
            "missing requested artifact provenance")
    require(file_provenance.get("binary_sha256") == origin["binary_sha256"] and
            file_provenance.get("source_build_receipt_sha256") == origin["source_build_sha256"],
            "exact-context executable provenance mismatch")
    loaded_semantic_identity = bind_loaded_identities(
        rows, file_provenance["artifact_sha256"], identity)
    ids = [entry.get("id") for entry in rows]
    require(all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", value) for value in ids),
            "invalid exact-context observation id")
    require(len(set(ids)) == len(ids), "duplicate exact-context observation id")
    for entry in rows:
        require(type(entry.get("warmup")) is bool, "invalid exact-context warmup classification")
        observation_contract(entry.get("observation", {}))
        require(entry.get("swapout_growth_bytes") == 0 and number(entry.get("min_free_percent"), "free memory") >= 15,
                "unclean exact-context run")
        row = entry["observation"]
        require(row["prompt_tokens"] > 0 and row["reserved_output_tokens"] > 0 and
                row["produced_output_tokens"] == row["reserved_output_tokens"],
                "exact-context run did not exercise the full output reservation")
        tokens = row.get("output_token_ids")
        require(type(tokens) is list and len(tokens) == row["produced_output_tokens"] and
                all(type(token) is int and 0 <= token <= 0x7fffffff for token in tokens),
                "missing or invalid exact raw output tokens")
        require(hashlib.sha256(b"".join(token.to_bytes(4, "little") for token in tokens)).hexdigest() == row["output_sha256"],
                "exact output mismatch: raw token digest")
        require(number(row["phases_ms"].get("decode"), "exact decode timing") > 0,
                "missing exact decode timing")
        require(all(type(row.get(key)) is int for key in ("context_live_bytes", "context_peak_bytes", "backend_kv_readback_bytes")),
                "missing exact-context allocation counters")
        require(row["backend_kv_readback_bytes"] == 0 and row["context_peak_bytes"] >= row["context_live_bytes"],
                "invalid exact-context allocation evidence")
    measured = [x for x in rows if not x.get("warmup")]
    warmups = [x for x in rows if x.get("warmup")]
    require(not ({x.get("pair_id") for x in measured} & {x.get("pair_id") for x in warmups}),
            "exact-context pair reused across warmup and measurement")
    results = []
    consumed_measured = 0
    consumed_warmups = 0
    for length in qualified_context_tokens:
        relevant = [x for x in measured if x["observation"]["prompt_tokens"] + x["observation"]["reserved_output_tokens"] == length]
        warm = [x for x in warmups if x["observation"]["prompt_tokens"] + x["observation"]["reserved_output_tokens"] == length]
        complete_warmups = paired_variants(warm, "warmup")
        complete = paired_variants(relevant, "measured")
        require(len(complete_warmups) >= 5, "too few exact-context warmups")
        require(len(complete) >= MIN_PERFORMANCE_PAIRS, "too few paired exact-context samples")
        consumed_measured += len(relevant)
        consumed_warmups += len(warm)
        for pair in complete_warmups + complete:
            require(pair["automatic"]["output_sha256"] == pair["packed_exact"]["output_sha256"], "exact output mismatch")
            require(pair["automatic"]["produced_output_tokens"] == pair["packed_exact"]["produced_output_tokens"], "exact output length mismatch")
            require(all(row.get("workload") == "independent_document" and
                        isinstance(row.get("prompt_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", row["prompt_sha256"])
                        for row in pair.values()), "exact-context capacity probes are not an independent corpus")
            require(pair["automatic"]["prompt_sha256"] == pair["packed_exact"]["prompt_sha256"] and
                    pair["automatic"]["prompt_tokens"] == pair["packed_exact"]["prompt_tokens"] and
                    pair["automatic"]["reserved_output_tokens"] == pair["packed_exact"]["reserved_output_tokens"],
                    "exact-context pair has different prompts or reservations")
        prompts = {pair["automatic"]["prompt_sha256"] for pair in complete}
        warm_prompts = {pair["automatic"]["prompt_sha256"] for pair in complete_warmups}
        require(len(prompts) == len(complete) and len(warm_prompts) == len(complete_warmups) and
                not prompts & warm_prompts, "exact-context corpus is reused across pairs or warmups")
        orders = {storage: 0 for storage in STORAGES}
        seen = set()
        for entry in relevant:
            if entry["pair_id"] not in seen:
                orders[entry["observation"]["storage"]] += 1
                seen.add(entry["pair_id"])
        require(max(orders.values()) - min(orders.values()) <= 1, "exact-context execution order is imbalanced")
        # Resample complete prompt pairs. Ratios of marginal percentiles can
        # hide regressions by comparing different prompts in the two tails.
        ratio = lambda sample: sorted(x["packed_exact"]["elapsed_ns"] / x["automatic"]["elapsed_ns"]
                                      for x in sample)[math.ceil(.95 * len(sample)) - 1]
        bound = bootstrap_bound(complete, ratio, True, identity["artifact"] + str(length))
        require(bound <= 1.05, "packed-exact context performance regression")
        decode_ratio = lambda sample: sorted(x["automatic"]["phases_ms"]["decode"] /
                                             x["packed_exact"]["phases_ms"]["decode"]
                                             for x in sample)[max(0, math.ceil(.05 * len(sample)) - 1)]
        decode_bound = bootstrap_bound(complete, decode_ratio, False, identity["artifact"] + str(length) + "decode")
        require(decode_bound >= 1 / 1.05, "packed-exact decode throughput regression")
        results.append({"context_tokens": length, "pairs": len(complete),
                        "packed_exact_elapsed_ratio_upper_95": bound,
                        "packed_exact_decode_ratio_lower_95": decode_bound})
    require(consumed_measured == len(measured) and consumed_warmups == len(warmups),
            "exact-context raw/sample count mismatch")
    return {"schema_version": 1, "stage": "exact_context_qualification", "semantic_identity": identity,
            "provenance": origin, "qualified_context_tokens": sorted(qualified_context_tokens),
            "loaded_identity_verified": True, "loaded_semantic_identity": loaded_semantic_identity,
            "evidence_passed": True, "serving_qualified": False, "results": results,
            "completed": True, "file_provenance": dict(file_provenance), "observations": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("plan", type=Path)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--timeout-seconds", type=float, default=900)
    run.add_argument("--max-rss-mib", type=float, default=8192)
    check = commands.add_parser("audit")
    check.add_argument("receipt", type=Path)
    check.add_argument("--identity", type=Path, required=True)
    check.add_argument("--provenance", type=Path, required=True)
    check.add_argument("--context-tokens", type=int, action="append", required=True)
    check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run":
        run_plan(read_json(args.plan), args.output, args.timeout_seconds, args.max_rss_mib)
    else:
        write_json(args.output, audit(read_json(args.receipt), read_json(args.identity),
                                      read_json(args.provenance), args.context_tokens))


if __name__ == "__main__":
    main()
