#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Qualify the production Metal HTTP worker against the pinned specialty oracle.

Uses the existing process/resource supervisor. This is a local acceptance
gate, not a deployment or a throughput comparison. Source snapshots are
hard-linked into an owned managed-model directory and never rewritten.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

from physical_memory import ProcessGroupPhysicalMemorySampler

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "zig/pkg/inference/scripts"))
from benchmark_resources import (  # noqa: E402
    group_rss_mib,
    memory_free_percent,
    swapout_bytes,
    terminate_group,
    write_json_atomic,
)

MODELS = {
    "pii_multi": "gliner2-privacy-filter-PII-multi",
    "gliguard": "gliguard-LLMGuardrails-300M",
}
SOURCE_FILES = (
    "config.json", "encoder_config/config.json", "tokenizer.json",
    "tokenizer_config.json", "model.safetensors",
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pii_schema(case):
    thresholds = case.get("entity_thresholds", {})
    schema = {"entities": list(case["labels"])}
    if thresholds:
        schema["entity_definitions"] = {label: {"threshold": value} for label, value in thresholds.items()}
    return schema


def guard_schema(case):
    return {"classifications": [
        {"name": head["task"], "labels": head["labels"],
         "mode": "multi" if head["multi_label"] else "single",
         "threshold": head["threshold"]}
        for head in case["heads"]
    ]}


def request_for(profile, case, *, byte_offsets=False):
    options = {"include_confidence": True, "include_spans": True,
               "offset_unit": "unicode_codepoints"}
    options.update(case.get("request_options", {}))
    if byte_offsets:
        options.pop("offset_unit", None)
    return {"schema_version": 2, "model": MODELS[profile],
            "schema": pii_schema(case) if profile == "pii_multi" else guard_schema(case),
            "inputs": [{"id": case["id"], "content": case["text"]}], "options": options}


def finite(value):
    if isinstance(value, float):
        require(math.isfinite(value), "non-finite JSON output")
    elif isinstance(value, dict):
        for child in value.values():
            finite(child)
    elif isinstance(value, list):
        for child in value:
            finite(child)


def compare_row(profile, case, row, *, byte_offsets=False, tolerance=5e-3):
    """Check the captured selections plus Antfly's schema and candidate order."""
    finite(row)
    maximum = 0.0
    if profile == "pii_multi":
        expected = []
        for label in case["labels"]:
            values = list(case["presented_output"]["entities"].get(label, []))
            # Both the legacy greedy and explicit allow decoders return
            # confidence-first order. Capture object keys are sorted for
            # serialization; the ordered label list defines schema order.
            for value in values:
                start, end = value["start"], value["end"]
                if byte_offsets:
                    start = len(case["text"][:start].encode("utf-8"))
                    end = len(case["text"][:end].encode("utf-8"))
                expected.append((label, value["text"], start, end, value["confidence"]))
        actual = row.get("entities", [])
        require(len(actual) == len(expected), f"{case['id']}: entity cardinality changed")
        for want, got in zip(expected, actual, strict=True):
            identity = (got["label"], got["text"], got["start"], got["end"])
            require(identity == want[:4], f"{case['id']}: entity identity/order {identity} != {want[:4]}")
            error = abs(got["score"] - want[4])
            require(error <= tolerance, f"{case['id']}: entity score error {error}")
            maximum = max(maximum, error)
    else:
        expected = []
        presented = case["presented_output"]
        for head in case["heads"]:
            labels = presented[head["task"]]
            if isinstance(labels, str):
                labels = [labels]
            require(isinstance(labels, list), "unknown captured classification presentation")
            require(len(set(labels)) == len(labels) and set(labels) <= set(head["labels"]),
                    "invalid captured classification selections")
            probabilities = dict(zip(head["labels"], head["probabilities"]["values"], strict=True))
            for label in head["labels"]:
                if label in labels:
                    expected.append((head["task"], label, probabilities[label]))
        actual = row.get("classifications", [])
        require(len(actual) == len(expected), f"{case['id']}: classification cardinality changed")
        for want, got in zip(expected, actual, strict=True):
            require((got["name"], got["label"]) == want[:2], f"{case['id']}: classification selection/order changed")
            error = abs(got["score"] - want[2])
            require(error <= tolerance, f"{case['id']}: classifier score error {error}")
            maximum = max(maximum, error)
    return maximum


def http_with_headers(url, path, body=None, timeout=120):
    request = urllib.request.Request(url + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    started = time.monotonic()
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return (response.status, json.load(response), time.monotonic() - started,
                {key.lower(): value for key, value in response.headers.items()})


def http(url, path, body=None, timeout=120):
    status, result, seconds, _ = http_with_headers(url, path, body, timeout)
    return status, result, seconds


def checked_rows(url, body):
    status, result, seconds = http(url, "/ai/v1/extract", body)
    require(status == 200, f"HTTP {status} model={body.get('model')} input={body['inputs'][0].get('id')}: {result}")
    require(len(result["data"]) == len(body["inputs"]), "response row count changed")
    finite(result)
    return result["data"], seconds


def rejection(url, body, status, code):
    actual_status, result, _ = http(url, "/ai/v1/extract", body)
    require(actual_status == status and result.get("error") == code,
            f"expected {status}/{code}, got {actual_status}/{result}")


def prepare_models(source_root, destination, manifest):
    identities = {}
    for profile, name in MODELS.items():
        target = destination / name
        (target / "encoder_config").mkdir(parents=True)
        pin = manifest["models"][profile]
        receipts = {"model.safetensors": {"size_bytes": pin["model_safetensors_size"],
                                         "sha256": pin["model_safetensors_sha256"]}}
        receipts.update(pin["metadata_fixtures"])
        receipts["tokenizer.json"] = {"size_bytes": pin["tokenizer_json_size"],
                                      "sha256": pin["tokenizer_json_sha256"]}
        for relative in SOURCE_FILES:
            source = source_root / name / relative
            expected = receipts[relative]
            require(source.stat().st_size == expected["size_bytes"] and sha256(source) == expected["sha256"],
                    f"source artifact identity mismatch: {source}")
            os.link(source, target / relative)
        public = {"type": "extractor", "tasks": ["extract"], "inputs": ["text"],
                  "capabilities": ["extraction" if profile == "pii_multi" else "classification"]}
        if profile == "gliguard":
            public["gliner_classification_head"] = "label_marker_mlp"
        (target / "model_manifest.json").write_text(json.dumps(public) + "\n")
        identities[profile] = {"repo": pin["repo"], "revision": pin["revision"],
                               "tensor_inventory": pin["tensor_inventory"], "files": receipts}
    return identities


def verify_catalog(catalog):
    extractors = catalog["extractors"]
    for profile, name in MODELS.items():
        info = extractors[name]
        expected = {"extraction"} if profile == "pii_multi" else {"classification", "zero_shot", "multi_label"}
        require(set(info["capabilities"]) == expected, f"unexpected public capabilities for {name}: {info}")
        require(info["inputs"] == ["text"], f"unexpected public inputs for {name}: {info}")
        contract = info["inference_capabilities"]
        require(contract["task"] == "extract", "wrong published executor task")
        require(contract["input_modalities"] == ["text"], "wrong published executor modalities")
        require(contract["batch"]["max_items"] == 128, "wrong published batch ceiling")
        require(contract["result_cardinality"] == "one_per_item", "wrong published result cardinality")
        require(name not in catalog.get("deciders", {}), f"specialty granted Decide: {name}")


def ragged_guard_body(cases, count=9):
    body = request_for("gliguard", cases[0])
    # The wire contract requires a shared schema even with per-input overrides.
    body["inputs"] = [{"id": f"batch-{i}", "content": cases[i % len(cases)]["text"],
                       "schema": guard_schema(cases[i % len(cases)])} for i in range(count)]
    return body


def prepared_boundary(url, profile, case):
    # GLiNER prepares each source word independently. Measure each candidate's
    # piece count and fixed schema cost through the actual served usage, then
    # compose exact boundaries even when no candidate word is a single piece.
    costs = {}
    schema_tokens = None
    for word in ("a", "token", "hello"):
        lengths = []
        for count in (1, 2):
            body = request_for(profile, case)
            body["inputs"][0]["content"] = " ".join([word] * count)
            status, result, _ = http(url, "/ai/v1/extract", body)
            require(status == 200, f"boundary cost probe failed: {status}/{result}")
            finite(result)
            lengths.append(result["usage"]["prompt_tokens"])
        cost = lengths[1] - lengths[0]
        fixed = lengths[0] - cost
        require(type(cost) is int and cost > 0 and 0 < fixed < 511,
                "invalid independently prepared word cost")
        if schema_tokens is None:
            schema_tokens = fixed
        require(schema_tokens == fixed, "schema cost changed across independent word probes")
        costs[word] = cost
    words_for_cost = {0: []}
    for target in range(1, 514 - schema_tokens):
        for word, cost in costs.items():
            if target - cost in words_for_cost:
                words_for_cost[target] = words_for_cost[target - cost] + [word]
                break
    boundary_inputs = {}
    for tokens in (511, 512, 513):
        require(tokens - schema_tokens in words_for_cost,
                f"exact prepared-token boundary unavailable: {profile}/{tokens}/{costs}")
        boundary_inputs[str(tokens)] = " ".join(words_for_cost[tokens - schema_tokens])
        body = request_for(profile, case)
        body["inputs"][0]["content"] = boundary_inputs[str(tokens)]
        if tokens == 513:
            rejection(url, body, 413, "EXTRACTION_LIMIT_EXCEEDED")
        else:
            status, result, _ = http(url, "/ai/v1/extract", body)
            require(status == 200 and result["usage"]["prompt_tokens"] == tokens,
                    f"boundary acceptance changed: expected={tokens} actual={status}/{result}")
            finite(result)
    return {"accepted_prepared_tokens": 512, "previous_prepared_tokens": 511,
            "rejected_prepared_tokens": 513,
            "rejected_count_evidence": "inferred from measured independent word costs and fixed schema; 511 and 512 verified by served usage",
            "schema_prepared_tokens": schema_tokens, "measured_word_piece_costs": costs,
            "boundary_inputs": boundary_inputs}


def concurrent_requests(url, profile, case):
    body = request_for(profile, case)
    ready = threading.Barrier(2)

    def call():
        ready.wait(timeout=10)
        return http_with_headers(url, "/ai/v1/extract", body)

    with ThreadPoolExecutor(max_workers=2) as clients:
        calls = [clients.submit(call) for _ in range(2)]
        # Leaving this context drains both calls, including on any exception.
        results = [pending.result() for pending in calls]

    expected_busy = {"error": "MODEL_RESOURCE_BUSY",
                     "message": "insufficient inference capacity is currently available",
                     "reason": "inference_capacity", "retryable": True, "retry_after_ms": 1000}
    report = {"attempted": 2, "initial_successes": 0, "initial_busy": 0,
              "retry_attempts": 0, "retry_successes": 0, "responses": [], "max_score_error": 0.0}

    def compare(result):
        finite(result)
        require(len(result["data"]) == 1 and result["data"][0].get("id") == case["id"],
                "concurrent response identity changed")
        error = compare_row(profile, case, result["data"][0])
        report["max_score_error"] = max(report["max_score_error"], error)

    for status, result, seconds, headers in results:
        row = {"status": status, "observed_http_seconds": seconds}
        if status == 200:
            compare(result)
            report["initial_successes"] += 1
        else:
            require(status == 503 and result == expected_busy and result.get("retryable") is True and
                    type(result.get("retry_after_ms")) is int and headers.get("retry-after") == "1",
                    f"unexpected concurrent rejection: {status}/{result}/{headers}")
            row.update(error=result, retry_after=headers["retry-after"])
            report["initial_busy"] += 1
        report["responses"].append(row)
    require(report["initial_successes"] >= 1, "both concurrent requests were denied; no progress")
    # The count ceiling does not promise two independent scratch reservations.
    # Retry a valid busy peer exactly once, after both initial futures drained.
    for _ in range(report["initial_busy"]):
        report["retry_attempts"] += 1
        status, result, seconds, _ = http_with_headers(url, "/ai/v1/extract", body)
        require(status == 200, f"post-drain retry failed: {status}/{result}")
        compare(result)
        report["retry_successes"] += 1
        report["retry_observed_http_seconds"] = seconds
    return report


def exercise(url, cases, cycles):
    reports = []
    boundaries = {}
    concurrency = {}
    for profile, capture in cases.items():
        for case in capture["cases"]:
            rows, seconds = checked_rows(url, request_for(profile, case))
            error = compare_row(profile, case, rows[0])
            reports.append({"profile": profile, "case": case["id"], "score_error": error,
                            "observed_http_seconds": seconds})
        # Two simultaneous client connections reach the actual supervisor and
        # worker. Use one warm model so a one-model residency cap is respected.
        concurrency[profile] = concurrent_requests(url, profile, capture["cases"][0])
        if profile == "gliguard":
            maximum = ragged_guard_body(capture["cases"], 128)
        else:
            maximum = request_for(profile, capture["cases"][0])
            maximum["inputs"] = [{"id": f"max-{i}", "content": capture["cases"][0]["text"]}
                                  for i in range(128)]
        rows, _ = checked_rows(url, maximum)
        for i, row in enumerate(rows):
            require(row.get("id") == (f"batch-{i}" if profile == "gliguard" else f"max-{i}"),
                    "maximum batch input order changed")
            compare_row(profile, capture["cases"][i % len(capture["cases"])] if profile == "gliguard"
                        else capture["cases"][0], row)
        boundaries[profile] = prepared_boundary(url, profile, capture["cases"][-1] if profile == "gliguard"
                                               else capture["cases"][0])
        body = request_for(profile, capture["cases"][0])
        body["options"]["long_document"] = {"mode": "window"}
        rejection(url, body, 400, "UNSUPPORTED_EXTRACTION_FEATURE")
        body = request_for(profile, capture["cases"][0])
        body["inputs"] *= 129
        rejection(url, body, 413, "EXTRACTION_LIMIT_EXCEEDED")
    unicode = next(case for case in cases["pii_multi"]["cases"] if case["id"] == "unicode_codepoint_offsets")
    rows, _ = checked_rows(url, request_for("pii_multi", unicode, byte_offsets=True))
    compare_row("pii_multi", unicode, rows[0], byte_offsets=True)
    guard = cases["gliguard"]["cases"]
    # Per-input schemas exercise ragged task and label geometry in one request.
    batch = ragged_guard_body(guard)
    rows, _ = checked_rows(url, batch)
    for i, row in enumerate(rows):
        require(row.get("id") == f"batch-{i}", "ragged input order changed")
        compare_row("gliguard", guard[i % len(guard)], row)
    for cycle in range(cycles):
        for profile, case in (("pii_multi", cases["pii_multi"]["cases"][0]), ("gliguard", guard[-1])):
            rows, _ = checked_rows(url, request_for(profile, case))
            compare_row(profile, case, rows[0])
        print(f"production_http cycle={cycle + 1}/{cycles} parity=true", flush=True)
    verify_catalog(http(url, "/ai/v1/models")[1])
    return {"cases": reports, "prepared_boundaries": boundaries, "concurrency": concurrency,
            "gates": {"captured_oracle_selections_and_api_order": True,
            "finite_scores_within_5e_minus_3": True, "public_catalog_contract": True,
            "window_rejection_both_profiles": True, "input_129_rejection_both_profiles": True,
            "input_128_parity_and_order_both_profiles": True, "prepared_512_accept_513_reject_both_profiles": True,
            "two_concurrent_connections_with_bounded_backpressure_and_post_drain_retry": True,
            "default_utf8_byte_offsets": True, "guard_ragged_nine_input_order": True,
            "alternating_parity_cycles": cycles}}


def run_lane(server, model_root, output, cases, lane, cycles):
    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        port = port_socket.getsockname()[1]
    command = [str(server), "run", "--host", "127.0.0.1", "--port", str(port),
               "--models-dir", str(model_root), "--ml-dir", str(model_root / "ml")]
    if lane == "bounded":
        command += ["--process-memory-budget-mb", "8192", "--host-budget-mb", "2048",
                    "--combined-budget-mb", "4096", "--max-loaded-models", "1",
                    "--max-concurrent-requests", "2"]
    environment = os.environ.copy()
    sanitized = []
    for key in list(environment):
        if (key.startswith("TERMITE_METAL_") or key.startswith("ANTFLY_TEST_") or
                key in ("TERMITE_DISABLE_OUTPUT_HOST_MIRROR_RESYNC", "ANTFLY_INFERENCE_KERNEL_JIT_MODE",
                        "ANTFLY_INFERENCE_SUPERVISED_WORKER", "ANTFLY_INFERENCE_SUPERVISOR_LIFELINE",
                        "ANTFLY_PROCESS_MEMORY_BUDGET_MB", "ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB")):
            sanitized.append(key)
            environment.pop(key)
    environment["ANTFLY_INFERENCE_REQUIRED_BACKEND"] = "metal"
    environment["ANTFLY_INFERENCE_PREFERRED_BACKEND"] = "metal"
    log = output / f"{lane}-server.log"
    report = {"lane": lane, "command": command, "passed": False, "sampled_peak_server_group_rss_mib": 0,
              "samples": 0, "min_free_percent": 100, "swapout_growth_bytes": 0,
              "sanitized_environment_keys": sorted(sanitized),
              "required_backend": "metal", "preferred_backend": "metal"}
    stop = threading.Event()
    violations = []
    physical = ProcessGroupPhysicalMemorySampler()
    baseline_swap = swapout_bytes()
    started = time.monotonic()
    with log.open("wb") as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   env=environment, start_new_session=True)

        def monitor():
            try:
                while not stop.wait(0.25):
                    rss = group_rss_mib(process.pid)
                    free = memory_free_percent()
                    swap = max(0, swapout_bytes() - baseline_swap)
                    memory = physical.snapshot(process.pid)
                    require(memory["live_processes"] > 0 or process.poll() is not None,
                            "owned server physical-memory sample is empty")
                    footprint = memory["conservative_summed_identity_peak_bound_bytes"]
                    report["samples"] += 1
                    report["sampled_peak_server_group_rss_mib"] = max(report["sampled_peak_server_group_rss_mib"], rss)
                    report["min_free_percent"] = min(report["min_free_percent"], free)
                    report["swapout_growth_bytes"] = max(report["swapout_growth_bytes"], swap)
                    report["physical_memory"] = memory
                    with (output / f"{lane}-physical-memory.jsonl").open("a") as memory_log:
                        memory_log.write(json.dumps(memory) + "\n")
                    require(rss <= 8192 and footprint <= 8192 * 1024**2 and free >= 15 and swap == 0,
                            f"resource envelope exceeded: rss={rss}, footprint={footprint}, free={free}, swapout_growth={swap}")
                    require(time.monotonic() - started < 900, "production lane exceeded 900s")
            except BaseException as error:
                violations.append(str(error))
                terminate_group(process)

        watcher = threading.Thread(target=monitor, daemon=True)
        watcher.start()
        try:
            url = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 120
            while True:
                require(process.poll() is None and not violations, f"server exited during startup: {violations}")
                try:
                    status, _, _ = http(url, "/readyz", timeout=2)
                    if status == 200:
                        break
                except (OSError, urllib.error.URLError):
                    pass
                require(time.monotonic() < deadline, "server readiness timed out")
                time.sleep(0.1)
            verify_catalog(http(url, "/ai/v1/models")[1])
            report.update(exercise(url, cases, cycles))
            require(not violations and process.poll() is None, f"server/resource failure: {violations}")
            require(report["samples"] > 0 and report["physical_memory"]["observed_process_identities"] == 2,
                    "expected exactly one stable supervisor and worker identity")
            text = log.read_text(errors="replace")
            for name in MODELS.values():
                require(re.search(r"selected backend metal for [^\n]*" + re.escape(name), text),
                        f"missing selected-Metal proof for {name}")
            require(not re.search(r"selected backend (native|cuda)|panic:|segmentation fault|Metal command buffer.*error|inference worker stopped unexpectedly", text, re.I),
                    "forbidden backend/failure in production log")
            report["gates"]["no_backend_fallback_or_worker_restart"] = True
            report["passed"] = True
            report["cycles"] = cycles
        except BaseException as error:
            report["failure"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            stop.set()
            watcher.join(timeout=15)
            cleanup_errors = []
            if watcher.is_alive():
                cleanup_errors.append("resource monitor failed to stop")
            cleanup_errors.extend(violations)
            if process.poll() is not None:
                cleanup_errors.append("server exited before harness shutdown")
            try:
                terminate_group(process)
                report["returncode_after_shutdown"] = process.returncode
                report["cleanup_group_rss_mib"] = group_rss_mib(process.pid)
                if report["cleanup_group_rss_mib"] != 0:
                    cleanup_errors.append("owned server/worker survived cleanup")
            except BaseException as error:
                cleanup_errors.append(f"{type(error).__name__}: {error}")
            report["elapsed_seconds"] = time.monotonic() - started
            if cleanup_errors:
                report["passed"] = False
                report["cleanup_errors"] = cleanup_errors
            write_json_atomic(output / f"{lane}-receipt.json", report)
            require(not cleanup_errors, f"production cleanup failed: {cleanup_errors}")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, default=ROOT / "zig/pkg/inference/testdata/gliner_specialty")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=25)
    args = parser.parse_args()
    require(args.cycles >= 25, "production gate requires at least 25 cycles")
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((args.fixtures / "manifest.json").read_text())
    cases = {profile: json.loads((args.fixtures / value["path"]).read_text())
             for profile, value in manifest["captures"].items()}
    for value in manifest["captures"].values():
        require(sha256(args.fixtures / value["path"]) == value["sha256"], "oracle capture digest mismatch")
    require(memory_free_percent() >= 15, "insufficient memory headroom before model load")
    report = {"schema": "antfly.gliner_specialty.production_metal_http.v1", "passed": False,
              "scope": "pinned original artifacts, manually staged managed manifests, production supervised HTTP runtime",
              "registry_pull_scope": "registry defaults and synthesis covered by Zig tests; network pull not executed",
              "harness_sha256": sha256(Path(__file__)),
              "server_sha256": sha256(args.server), "fixture_manifest_sha256": sha256(args.fixtures / "manifest.json"),
              "lanes": []}
    try:
        with tempfile.TemporaryDirectory(prefix="managed-", dir=args.output) as directory:
            root = Path(directory)
            report["models"] = prepare_models(args.model_root, root, manifest)
            for lane in ("default", "bounded"):
                report["lanes"].append(run_lane(args.server.resolve(), root, args.output, cases, lane, args.cycles))
        report["passed"] = True
    finally:
        write_json_atomic(args.output / "receipt.json", report)
    print(json.dumps({"passed": True, "receipt": str(args.output / "receipt.json")}))


if __name__ == "__main__":
    main()
