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

"""Paired warm HTTP comparisons for pinned GLiNER extraction/decision cases.

The case file contains {"cases": [{"id", "path", "request", "expected",
"confidence_tolerance"}], "artifacts": [local file paths]}.
Default report-only mode retains the Linux CPU campaign contract. Opt-in gated
mode binds actual native/Metal/CUDA loaded inventory and immutable launch
receipts; the filename is not backend evidence.
The baseline, candidate, and optional same-precision reference receive identical
payloads. All public output fields are checked, except confidence/logit rounding.
This produces HTTP evidence, not automatic hardware/release qualification.
"""

import argparse
import base64
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import sys
import time
import threading
from pathlib import Path
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from linux_cpu import host_provenance, sha256
import paired_benchmark
import qualification_identity


def strict_json(raw):
    def invalid(value):
        raise ValueError(f"non-finite JSON: {value}")

    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError(f"duplicate key: {key}")
            out[key] = value
        return out

    return json.loads(raw, parse_constant=invalid, object_pairs_hook=unique)


def require_equal(expected, actual, tolerance, path="output"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or expected.keys() != actual.keys():
            raise ValueError(f"{path}: keys differ")
        for key, value in expected.items():
            require_equal(value, actual[key], tolerance, f"{path}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise ValueError(f"{path}: lengths differ")
        for i, (left, right) in enumerate(zip(expected, actual)):
            require_equal(left, right, tolerance, f"{path}[{i}]")
    elif path.endswith((".confidence", ".logit", ".score", ".probability")):
        if (
            type(expected) not in (int, float)
            or type(actual) not in (int, float)
            or not math.isfinite(expected)
            or not math.isfinite(actual)
            or abs(expected - actual) > tolerance
        ):
            raise ValueError(f"{path}: numerical mismatch")
    elif type(expected) is not type(actual) or expected != actual:
        raise ValueError(f"{path}: value differs")


def receive(base, case, timeout, trace=None):
    """Capture transport bytes/timestamps only; never parse or validate here."""
    path = case["path"]
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError("case path must be an absolute API path")
    payload = json.dumps(case["request"], allow_nan=False).encode()
    req = Request(
        base.rstrip("/") + path,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.perf_counter_ns()
    try:
        with urlopen(req, timeout=timeout) as response:
            raw = response.read(16 * 1024 * 1024 + 1)
            if response.status != 200 or len(raw) > 16 * 1024 * 1024:
                raise ValueError("unsuccessful or oversized response")
    except HTTPError as error:
        if trace is not None:
            trace["http_status"] = error.code
        try:
            with error:
                body = error.read(16 * 1024 * 1024 + 1 if trace is not None else 4096)
        except BaseException as body_error:
            if trace is not None:
                completed = time.perf_counter_ns()
                trace.update(error={"type": type(body_error).__name__, "message": str(body_error)},
                             duration_ns=completed - start, response_completed_ns=completed)
                partial = getattr(body_error, "partial", None)
                if isinstance(partial, bytes):
                    trace.update(raw=partial[:16 * 1024 * 1024 + 1], response_incomplete=True,
                                 response_truncated=len(partial) > 16 * 1024 * 1024)
            raise
        if trace is not None:
            completed = time.perf_counter_ns()
            trace.update(http_status=error.code, raw=body, response_truncated=len(body) > 16 * 1024 * 1024,
                         duration_ns=completed - start, response_completed_ns=completed)
        detail = body[:4096].decode("utf-8", errors="replace")
        raise ValueError(f"{req.full_url}: HTTP {error.code}: {detail}") from error
    except BaseException as error:
        if trace is not None:
            completed = time.perf_counter_ns()
            trace.update(error={"type": type(error).__name__, "message": str(error)},
                         duration_ns=completed - start, response_completed_ns=completed)
            status = getattr(locals().get("response"), "status", None)
            if type(status) is int:
                trace["http_status"] = status
            partial = getattr(error, "partial", None)
            if isinstance(partial, bytes):
                trace.update(raw=partial[:16 * 1024 * 1024 + 1], response_incomplete=True,
                             response_truncated=len(partial) > 16 * 1024 * 1024)
            elif "raw" in locals():
                trace.update(raw=raw, response_truncated=len(raw) > 16 * 1024 * 1024)
        raise
    completed = time.perf_counter_ns()
    duration = completed - start
    if trace is not None:
        trace.update(duration_ns=duration, response_completed_ns=completed,
                     http_status=response.status, raw=raw, response_truncated=False)
    return {
        "duration_ns": duration,
        "response_completed_ns": completed,
        "raw": raw,
    }


def validate(sample, case):
    output = strict_json(sample["raw"])
    require_equal(case["expected"], output, case["confidence_tolerance"])
    return {
        "duration_ns": sample["duration_ns"],
        "response_completed_ns": sample["response_completed_ns"],
        "output": output,
    }


def request(base, case, timeout):
    return validate(receive(base, case, timeout), case)


def request_round(base, case, timeout, concurrency, traces=None):
    """Capture the entire burst before parsing or validating any response."""
    if concurrency == 1:
        if traces is None:
            return request(base, case, timeout)
        return validate(receive(base, case, timeout, traces[0]), case)
    start = 0

    def release():
        nonlocal start
        start = time.perf_counter_ns()

    ready = threading.Barrier(concurrency + 1, action=release, timeout=timeout)

    def client(index):
        ready.wait()
        if traces is None:
            return receive(base, case, timeout)
        return receive(base, case, timeout, traces[index])

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = [pool.submit(client, index) for index in range(concurrency)]
        ready.wait()
        samples = [future.result() for future in pending]
        duration = max(sample["response_completed_ns"] for sample in samples) - start
    # No worker can compete with a pending response's timestamp for Python
    # execution time. Still finish every oracle check before starting a new arm.
    return {
        "duration_ns": duration,
        "requests": [validate(sample, case) for sample in samples],
    }


def pinned_file(item):
    path = Path(item["path"]).resolve(strict=True)
    expected = item["sha256"]
    if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise ValueError("invalid pinned artifact hash")
    if qualification_identity.sha256_file(path)["sha256"] != expected:
        raise ValueError(f"arm artifact changed: {path}")
    return path


def bind_arms(path, arms, observe=True):
    """Pin launch receipts and observe actual loaded model roots/backends.

    Schema1: arms[name] contains endpoint, source_commit, binary/build_receipt/
    configuration {path,sha256}, working_directory, precision, thread_budget,
    resource_limits, and model_artifacts from qualification_identity. Build
    receipt must contain passed, binary_sha256, source.head and an empty diff.
    This is a controlled-launch binding, not independent socket-to-PID proof.
    """
    raw = path.read_bytes()
    manifest = strict_json(raw)
    if manifest.get("schema") != 1 or set(manifest.get("arms", {})) != set(arms):
        raise ValueError("exact immutable arm receipt inventory required")
    bound = {}
    for name, endpoint in arms.items():
        arm = manifest["arms"][name]
        if arm["endpoint"].rstrip("/") != endpoint.rstrip("/"):
            raise ValueError("arm endpoint differs from launch receipt")
        if re.fullmatch(r"[0-9a-f]{40}", arm["source_commit"]) is None:
            raise ValueError("immutable arm commit required")
        binary = pinned_file(arm["binary"])
        build = strict_json(pinned_file(arm["build_receipt"]).read_bytes())
        contract = arm["build_receipt"].get("format", "bounded_decide")
        if contract == "bounded_decide":
            if (build.get("passed") is not True or build.get("binary_sha256") != arm["binary"]["sha256"]
                    or build.get("source", {}).get("head") != arm["source_commit"]
                    or build.get("source", {}).get("tracked_diff_sha256") != hashlib.sha256(b"").hexdigest()):
                raise ValueError("arm binary/source differs from clean successful build")
        elif contract == "guarded_source_map":
            map_path = pinned_file(arm["source_map"])
            if map_path.stat().st_size > 32 * 1024 * 1024:
                raise ValueError("source map exceeds 32MiB metadata bound")
            source_map = strict_json(map_path.read_bytes())
            if (not isinstance(source_map, dict) or set(source_map) != {"revision", "files"}
                    or not isinstance(source_map["files"], dict) or not source_map["files"]
                    or any(not isinstance(name, str) or not isinstance(digest, str)
                           or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                           for name, digest in source_map["files"].items())):
                raise ValueError("unsupported guarded source map schema")
            if (not isinstance(build.get("image"), str) or not build["image"]
                    or build.get("image_sha256") != arm["binary"]["sha256"]
                    or type(build.get("image_bytes")) is not int or build["image_bytes"] != binary.stat().st_size):
                raise ValueError("guarded build image differs from supplied binary")
            if (type(build.get("returncode")) is not int or build["returncode"] != 0 or build.get("source_unchanged") is not True
                    or build.get("revision") != arm["source_commit"]
                    or build.get("source_map_sha256") != arm["source_map"]["sha256"]
                    or source_map.get("revision") != arm["source_commit"]):
                raise ValueError("guarded build/source map differs from compiled revision")
            source_root = Path(arm["source_root"]).resolve(strict=True)
            noncompiled_tools = {
                "zig/pkg/inference/scripts/gliner25/benchmark_linux_cpu_http.py",
                "zig/pkg/inference/scripts/gliner25/test_benchmark_linux_cpu_http.py",
            }
            if not any(name.endswith(".zig") for name in source_map["files"]):
                raise ValueError("source map lacks compiled production sources")
            for source_name, digest in source_map["files"].items():
                if not isinstance(source_name, str) or Path(source_name).is_absolute() or ".." in Path(source_name).parts:
                    raise ValueError("invalid source map path")
                if source_name in noncompiled_tools or Path(source_name).suffix in (".md", ".rst"):
                    continue
                # Check all other tracked inputs, including embedded cubin,
                # fatbin/PTX/MSL, JSON/config, build scripts and dependency files.
                path = (source_root / source_name).resolve(strict=True)
                if not path.is_relative_to(source_root):
                    raise ValueError("compiled source escapes declared root")
                pinned_file({"path": str(path), "sha256": digest})
        else:
            raise ValueError("unknown build receipt format")
        configuration = strict_json(pinned_file(arm["configuration"]).read_bytes())
        if (not isinstance(arm["precision"], str) or not arm["precision"]
                or not isinstance(arm["thread_budget"], dict) or not arm["thread_budget"]
                or not isinstance(arm["resource_limits"], dict) or not arm["resource_limits"]):
            raise ValueError("explicit equal precision/thread/resource envelope required")
        # This existing owner resolves actual /models/instances model paths
        # below the launch configuration root, hashes every consumed file, and
        # rechecks loaded instance IDs after hashing. No backend inferred by name.
        observed = (qualification_identity.model_artifacts(configuration, endpoint, Path(arm["working_directory"]), model_roots=arm.get("model_roots"))
                    if observe else arm["model_artifacts"])
        qualification_identity.require_matching_artifacts(
            {"model_artifacts": arm["model_artifacts"]}, {"model_artifacts": observed})
        if any(model["backend"] not in ("native", "metal", "cuda") for model in observed["models"]):
            raise ValueError("unqualified backend in actual loaded model inventory")
        instances = None
        if observe:
            status, inventory, _ = qualification_identity.q.http(endpoint, "/models/instances")
            if status != 200 or not inventory.get("data"):
                raise ValueError("loaded instance inventory unavailable")
            instances = sorted(
                [{key: item[key] for key in ("id", "model", "backend", "state")} for item in inventory["data"]],
                key=lambda item: item["id"],
            )
            if (any(not item["id"] or item["state"] != "loaded" for item in instances)
                    or sorted((item["model"], item["backend"]) for item in instances)
                    != sorted((item["model"], item["backend"]) for item in observed["models"])):
                raise ValueError("loaded instance changed after artifact binding")
        bound[name] = dict(arm, model_artifacts_checked=observed, loaded_instances=instances,
                           live_inventory_observed=observe, binary_path=str(binary))
    baseline = bound["baseline"]
    for arm in bound.values():
        for key in ("precision", "thread_budget", "resource_limits"):
            if arm[key] != baseline[key]:
                raise ValueError(f"arm {key} differs")
        qualification_identity.require_matching_artifacts(
            {"model_artifacts": baseline["model_artifacts_checked"]},
            {"model_artifacts": arm["model_artifacts_checked"]})
    return {"receipt_sha256": hashlib.sha256(raw).hexdigest(), "arms": bound,
            "scope": "pinned controlled launch receipts plus live loaded model inventory; socket/PID, precision execution and device isolation require outer campaign proof"}


def acceptance(summary, pairs, require_faster=False):
    median = summary["candidate_ns"]["median"] / summary["baseline_ns"]["median"]
    p95 = summary["candidate_ns"]["p95"] / summary["baseline_ns"]["p95"]
    speedup = paired_benchmark.paired_log_ratio_ci([(b, c) for c, b in pairs])
    no_regression = median <= 1.05 and p95 <= 1.10
    clients = summary.get("client_latency_ns")
    if clients:
        no_regression = no_regression and clients["candidate"]["median"] <= 1.05 * clients["baseline"]["median"] and clients["candidate"]["p95"] <= 1.10 * clients["baseline"]["p95"]
    faster = speedup["lower_95"] > 1.0
    return {"median_ratio": median, "p95_ratio": p95, "limits": {"median": 1.05, "p95": 1.10},
            "baseline_over_candidate_speedup_ci": speedup, "no_regression_pass": no_regression,
            "faster_pass": faster, "require_faster": require_faster,
            "passed": no_regression and (faster or not require_faster)}


def audited_round(base, case, timeout, concurrency, event, metadata):
    traces = [{} for _ in range(concurrency)]
    payload = json.dumps(case["request"], allow_nan=False).encode()
    event(dict(metadata, status="pending", clients=concurrency,
               request_body_base64=base64.b64encode(payload).decode()))
    result = None
    failure = None
    try:
        result = request_round(base, case, timeout, concurrency, traces)
        return result
    except BaseException as error:
        failure = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        # ThreadPool exits only after every client has settled. Persisting or
        # parsing before then could compete with another response's timestamp.
        captured = []
        for trace in traces:
            raw = trace.pop("raw", None)
            captured.append(dict(trace, raw_body_base64=base64.b64encode(raw).decode() if raw is not None else None))
        event(dict(metadata, status="failed" if failure else "complete", clients=captured,
                   result=result, error=failure))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cases", type=Path, required=True)
    p.add_argument("--baseline", required=True)
    p.add_argument("--candidate", required=True)
    p.add_argument("--reference")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--pairs", type=int, default=20)
    p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--arm-receipts", type=Path, help="Schema1 controlled-launch arm receipts, checked before/after")
    p.add_argument("--enforce-no-regression", action="store_true")
    p.add_argument("--load-preflight", action="store_true", help="Explicit untimed, output-validated load before live inventory binding")
    p.add_argument("--require-faster", action="store_true", help="Also require baseline/candidate speedup CI lower95 > 1")
    args = p.parse_args()
    gated = args.enforce_no_regression or args.require_faster
    if args.load_preflight and not args.arm_receipts:
        p.error("--load-preflight requires --arm-receipts")
    if gated and not args.arm_receipts:
        p.error("gated comparison requires --arm-receipts")
    if gated and args.pairs < 10:
        p.error("gated comparison requires at least 10 measured pairs")
    if (
        not 0 <= args.warmup <= 10
        or not 2 <= args.pairs <= 100
        or not 0 < args.timeout <= 300
        or not 1 <= args.concurrency <= 32
    ):
        p.error("invalid sampling or timeout limits")
    arms = {"baseline": args.baseline, "candidate": args.candidate}
    if args.reference:
        arms["reference"] = args.reference
    for url in arms.values():
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname not in (
            "localhost",
            "127.0.0.1",
            "::1",
        ):
            p.error("use loopback HTTP servers on the same qualification host")
    manifest = strict_json(args.cases.read_bytes())
    cases = manifest["cases"]
    if not cases or len(cases) > 100 or len({c["id"] for c in cases}) != len(cases):
        p.error("need 1..100 cases with unique IDs")
    for case in cases:
        tolerance = case["confidence_tolerance"]
        if (
            type(tolerance) not in (int, float)
            or not math.isfinite(tolerance)
            or not 0 <= tolerance <= 0.002
        ):
            p.error(
                "confidence_tolerance must preserve a qualified tolerance in 0..0.002"
            )
    artifacts = {str(Path(f).resolve()): sha256(f) for f in manifest["artifacts"]}
    if not artifacts:
        p.error("pin server binaries and model files in artifacts")
    args.output.mkdir(parents=True, exist_ok=False)
    report = {
        "format_version": 1,
        "status": "running",
        "scope": "warm_http",
        "host": host_provenance(),
        "cases_sha256": sha256(args.cases),
        "artifacts": artifacts,
        "arms": arms,
        "warmup": args.warmup,
        "pairs": args.pairs,
        "concurrency": args.concurrency,
        "round_scope": "single_request"
        if args.concurrency == 1
        else "synchronized_burst",
        "timing": {
            "client_latency": "HTTP send through response-body read; excludes JSON parsing and oracle comparison",
            "burst": "barrier release through last response-body read; includes client dispatch and payload encoding",
            "validation": "all response bodies and timestamps captured before any JSON parsing or oracle comparison",
        },
        "performance_release_qualified": False,
        "samples": [],
        "comparisons": {},
    }
    def event(value):
        with (args.output / "attempts.jsonl").open("a") as stream:
            stream.write(json.dumps(value, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def run_round(arm, case, sample_type, index):
        if not args.arm_receipts:
            return request_round(arms[arm], case, args.timeout, args.concurrency)
        return audited_round(arms[arm], case, args.timeout, args.concurrency, event,
                             {"arm": arm, "case_id": case["id"], "sample_type": sample_type, "index": index})

    report["acceptance_mode"] = "gated" if gated else "report_only"
    report["require_faster"] = args.require_faster
    report["load_preflight"] = args.load_preflight
    tool_paths = [Path(__file__), Path(paired_benchmark.__file__), Path(qualification_identity.__file__), Path(qualification_identity.q.__file__)]
    report["tool_sources_sha256"] = {str(path.resolve()): sha256(path) for path in tool_paths}
    report["source_scope"] = "benchmark tool hashes/host commit are separate from each arm's immutable compiled-image source commit"
    report["arm_binding_scope"] = "unavailable" if not args.arm_receipts else "controlled launch receipts plus live inventory"
    try:
        if args.arm_receipts:
            report["arm_launch_binding"] = bind_arms(args.arm_receipts, arms, observe=False)
            if args.load_preflight:
                for index, case in enumerate(cases):
                    for arm in arms:
                        run_round(arm, case, "load_preflight", index)
            report["arm_binding_before"] = bind_arms(args.arm_receipts, arms)
        for case in cases:
            for index in range(args.warmup):
                order = list(arms)
                if args.arm_receipts:
                    order = list(paired_benchmark.balanced_pair_order(index + 1, "baseline", "candidate"))
                    if args.reference:
                        order += ["reference"]
                for arm in order:
                    run_round(arm, case, "warmup", index)
            pairs = []
            reference_pairs = []
            for pair in range(args.pairs):
                order = list(arms) if pair % 2 == 0 else list(reversed(arms))
                if args.arm_receipts:
                    order = list(paired_benchmark.balanced_pair_order(pair + 1, "baseline", "candidate"))
                    if args.reference:
                        order = order + ["reference"] if pair % 2 == 0 else ["reference"] + order
                row = {
                    "case_id": case["id"],
                    "pair": pair,
                    "order": order,
                    "host_load_average": list(os.getloadavg()),
                }
                if args.arm_receipts:
                    report["samples"].append(row)
                for arm in order:
                    row[arm] = run_round(arm, case, "measured", pair)
                if not args.arm_receipts:
                    report["samples"].append(row)
                pairs.append(
                    (row["candidate"]["duration_ns"], row["baseline"]["duration_ns"])
                )
                if args.reference:
                    reference_pairs.append(
                        (
                            row["candidate"]["duration_ns"],
                            row["reference"]["duration_ns"],
                        )
                    )
            summary = {
                "candidate_ns": paired_benchmark.distribution(c for c, _ in pairs),
                "baseline_ns": paired_benchmark.distribution(b for _, b in pairs),
                "candidate_over_baseline": paired_benchmark.paired_log_ratio_ci(
                    pairs, samples=2000
                ),
            }
            if reference_pairs:
                summary["candidate_over_reference"] = (
                    paired_benchmark.paired_log_ratio_ci(reference_pairs, samples=2000)
                )
            if args.concurrency > 1:
                summary["client_latency_ns"] = {
                    arm: paired_benchmark.distribution(
                        sample["duration_ns"]
                        for row in report["samples"]
                        if row["case_id"] == case["id"]
                        for sample in row[arm]["requests"]
                    )
                    for arm in arms
                }
                summary["requests_per_second"] = {
                    arm: args.pairs
                    * args.concurrency
                    * 1e9
                    / sum(
                        row[arm]["duration_ns"]
                        for row in report["samples"]
                        if row["case_id"] == case["id"]
                    )
                    for arm in arms
                }
            if gated:
                summary["acceptance"] = acceptance(summary, pairs, args.require_faster)
            report["comparisons"][case["id"]] = summary
        if (
            artifacts != {path: sha256(path) for path in artifacts}
            or sha256(args.cases) != report["cases_sha256"]
        ):
            raise ValueError("artifacts changed during measurement")
        if args.arm_receipts:
            report["arm_binding_after"] = bind_arms(args.arm_receipts, arms)
            if report["arm_binding_after"] != report["arm_binding_before"]:
                raise ValueError("immutable arm binding changed during comparison")
        if gated:
            report["acceptance_passed"] = all(value["acceptance"]["passed"] for value in report["comparisons"].values())
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        if gated:
            report["acceptance_passed"] = False
        raise
    finally:
        if args.arm_receipts and "arm_binding_before" in report and "arm_binding_after" not in report:
            try:
                report["arm_binding_after"] = bind_arms(args.arm_receipts, arms)
                if report["arm_binding_after"] != report.get("arm_binding_before"):
                    raise ValueError("immutable arm binding changed")
            except BaseException as error:
                report["binding_error"] = f"{type(error).__name__}: {error}"
                report["status"] = "failed"
                if gated:
                    report["acceptance_passed"] = False
        try:
            report["tool_sources_after"] = {path: sha256(path) == digest for path, digest in report["tool_sources_sha256"].items()}
            if not all(report["tool_sources_after"].values()):
                raise ValueError("benchmark tool or helper source changed")
        except BaseException as error:
            report["status"] = "failed"
            report["tool_source_error"] = f"{type(error).__name__}: {error}"
            if gated:
                report["acceptance_passed"] = False
        (args.output / "report.json").write_text(
            json.dumps(report, indent=2, allow_nan=False) + "\n"
        )
        paired_benchmark.write_evidence_manifest(args.output)
    print(args.output / "report.json")
    return 0 if report["status"] == "complete" and (not gated or report.get("acceptance_passed")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
