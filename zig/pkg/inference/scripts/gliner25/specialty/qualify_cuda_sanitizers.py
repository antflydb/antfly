#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run fail-closed GB10 CUDA sanitizer lanes against an existing test executable.

Default coverage is both GRU residual modes and every frozen PII/GLiGuard head
case under all four tools. --include-http adds the full public loopback HTTP
service tests, including maximum request geometry. --filter can select one or
more exact suite names for staged runs. No compilation is performed.

The optional production-server candidate is fingerprinted for association only;
this runner executes the supplied test binary. Use qualify_http.py separately to
qualify that production executable itself. Sanitizer runs are correctness runs,
not performance measurements.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import traceback

from qualify_http import MANIFEST, PACKAGE, REPO, digest_file, hardware, terminate_server, verify_inputs

TOOLS = ("memcheck", "initcheck", "racecheck", "synccheck")
SUITES = {
    "gru": {"filter": "cuda GLiNER GRU residual modes preserve cancellation-sensitive state on device"},
    "pii-head": {"profile": "pii", "filter": "gliner specialty PII pinned CUDA head oracle"},
    "gliguard-head": {"profile": "gliguard", "filter": "gliner specialty GLiGuard pinned CUDA classifier oracle"},
    "pii-http": {"profile": "pii", "http": True, "filter": "GLiNER specialty PII public HTTP CUDA"},
    "gliguard-http": {"profile": "gliguard", "http": True, "filter": "GLiNER specialty GLiGuard public HTTP CUDA"},
}
BASE_SUITES = ("gru", "pii-head", "gliguard-head")
ARTIFACT_NAMES = ("inference_cuda_kernels.cu", "inference_cuda_kernels.ptx",
                  "inference_cuda_kernels.fatbin", "inference_cuda_kernels_sm89.cubin")
SUMMARY = re.compile(r"(?m)^(\d+) selected; (\d+) passed; (\d+) skipped(?:; (\d+) failed; (\d+) leaked)?\.\s*$")
ERROR_SUMMARY = re.compile(r"(?m)^=+\s*ERROR SUMMARY:\s*(\d+) errors?\s*$")
LEAK_SUMMARY = re.compile(r"(?m)^=+\s*LEAK SUMMARY:\s*(\d+) bytes? leaked in (\d+) allocations?\s*$")
RACE_SUMMARY = re.compile(r"(?m)^=+\s*RACECHECK SUMMARY:\s*(\d+) hazards? displayed \((\d+) errors?, (\d+) warnings?\)\s*$")


# These named entry points occur on the frozen CUDA paths. Cover both FP32
# attention alternatives so a diagnostic override cannot turn a filter into
# a no-op by changing the selected implementation.
ENCODER_KERNELS = ("termite_deberta_attention_f32", "termite_deberta_attention_fused_f32", "termite_take_rows_f32")
PII_KERNELS = (*ENCODER_KERNELS, "termite_gliner_gru_combine_f32", "termite_concat_lastdim_f32",
               "termite_gliner_gather_concat_relu_f32")
REQUIRED_KERNELS = {"gru": ("termite_gliner_gru_combine_f32",),
                    "pii-head": PII_KERNELS, "gliguard-head": ENCODER_KERNELS,
                    "pii-http": PII_KERNELS, "gliguard-http": ENCODER_KERNELS}


def compile_kernel_regex(pattern):
    if not pattern or any(ord(char) < 32 or ord(char) > 126 for char in pattern):
        raise ValueError("kernel regex must be nonempty printable ASCII")
    # Restrict Python-only constructs: validation must agree with the installed
    # sanitizer's standard regex syntax, not merely compile in Python.
    if "(?" in pattern or re.search(r"\\(?:A|Z|N|U)", pattern) or re.search(r"(?:[*+?]|\})\+", pattern):
        raise ValueError("kernel regex uses unsupported nonportable syntax")
    try:
        return re.compile(pattern)
    except re.error as error:
        raise ValueError(f"invalid kernel regex: {error}") from error


def ptx_kernel_symbols(text):
    symbols = set(re.findall(r"\.entry\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(", text))
    if not symbols:
        raise ValueError("CUDA artifact has no PTX kernel entry inventory")
    return symbols


def require_embedded_symbols(binary, symbols):
    missing = {name.encode() for name in symbols}
    overlap = max(map(len, missing), default=1) - 1
    tail = b""
    with binary.open("rb") as source:
        while missing:
            block = source.read(8 << 20)
            if not block:
                break
            data = tail + block
            missing = {name for name in missing if name not in data}
            tail = data[-overlap:] if overlap else b""
    if missing:
        raise ValueError(f"required CUDA symbols absent from test binary: {sorted(name.decode() for name in missing)}")


def kernel_scope(pattern, suites, artifact_symbols, binary):
    compiled = compile_kernel_regex(pattern) if pattern is not None else None
    by_suite = {name: list(REQUIRED_KERNELS[name]) for name in suites}
    required = sorted({symbol for names in by_suite.values() for symbol in names})
    absent = set(required) - artifact_symbols
    if absent:
        raise ValueError(f"required CUDA symbols absent from PTX artifact: {sorted(absent)}")
    if compiled:
        unmatched = [name for name in required if compiled.search(name) is None]
        if unmatched:
            raise ValueError(f"kernel regex excludes required project kernels for selected suites: {unmatched}")
    require_embedded_symbols(binary, required)
    return {"mode": "explicit_kernel_regex" if compiled else "unfiltered_all_kernels", "regex": pattern,
        "sanitizer_argument": "regex=" + pattern if compiled else None,
        "required_symbols_by_suite": by_suite,
        "matched_artifact_symbols": sorted(name for name in artifact_symbols if not compiled or compiled.search(name)),
        "symbol_evidence": "Required kernel entry names exist in the pinned PTX inventory and test executable bytes; runtime CUDA/head receipts are checked separately",
        "vendor_library_scope": "Unmatched cuBLAS/vendor kernels are excluded; vendor-library coverage is not claimed" if compiled else "No kernel-name filtering; project and vendor-library launches remain eligible",
        "all_kernel_coverage_claimed": not bool(compiled),
        "runtime_checked_kernel_list_available": False,
        "runtime_checked_kernel_list_note": "Installed tool has no generic checked-launch listing option; explicit checked/instrumented-kernel log entries are retained if emitted"}


def checked_kernel_log_receipt(text, pattern):
    names = re.findall(r"(?mi)^=+\s*(?:Checking|Instrumenting|Instrumented) kernel\s+[\"']?([A-Za-z_$][A-Za-z0-9_$]*)", text)
    if names and pattern is not None:
        compiled = compile_kernel_regex(pattern)
        if any(compiled.search(name) is None for name in names):
            raise ValueError("sanitizer reported instrumenting kernels outside the requested regex")
    return {"available": bool(names), "kernel_names": sorted(set(names)) if names else None,
            "scope": "Explicit tool kernel-check/instrumentation messages only; absence is unavailable, not zero checked launches"}


def sanitizer_command(sanitizer, tool, test_binary, test_filter, kernel_regex=None):
    command = [str(sanitizer), "--tool", tool, "--target-processes", "all", "--require-cuda-init", "yes",
               "--error-exitcode", "99", "--check-exit-code", "yes", "--print-limit", "0"]
    if kernel_regex is not None:
        compile_kernel_regex(kernel_regex)
        command += ["--kernel-name", "regex=" + kernel_regex, "--print-session-details"]
    if tool == "memcheck":
        command += ["--leak-check", "full"]
    elif tool == "racecheck":
        command += ["--racecheck-report", "all"]
    return command + [str(test_binary), "--test-filter", test_filter]


def assess_log(text, tool, test_filter, returncode, timed_out=False):
    """Require executed tests and explicit clean sanitizer summaries, not exit0 alone."""
    failures = []
    if timed_out:
        failures.append("sanitizer invocation timed out")
    if returncode != 0:
        failures.append(f"sanitizer/application exit code {returncode}")
    summaries = list(SUMMARY.finditer(text))
    if len(summaries) != 1:
        failures.append("missing or ambiguous runtime test summary")
        test_counts = None
    else:
        selected, passed, skipped, failed, leaked = [int(value or 0) for value in summaries[0].groups()]
        test_counts = dict(selected=selected, passed=passed, skipped=skipped, failed=failed, leaked=leaked)
        if selected != 1 or passed != 1 or skipped or failed or leaked:
            failures.append(f"expected exactly one passed test with zero skips/failures/leaks: {test_counts}")
    test_names = re.findall(r"(?m)^\d+/\d+ ([^\n]+?)\.\.\.", text)
    if len(test_names) != 1 or test_filter not in test_names[0]:
        failures.append("runtime did not execute the exact requested test filter")
    if "COMPUTE-SANITIZER" not in text:
        failures.append("missing compute-sanitizer banner")
    error_counts = [int(value) for value in ERROR_SUMMARY.findall(text)]
    race_counts = [list(map(int, values)) for values in RACE_SUMMARY.findall(text)]
    leak_counts = [list(map(int, values)) for values in LEAK_SUMMARY.findall(text)]
    if tool == "memcheck" and (not leak_counts or any(any(values) for values in leak_counts)):
        failures.append(f"missing or nonzero full leak-check summary: {leak_counts}")
    if tool == "racecheck":
        if not race_counts:
            failures.append("missing explicit racecheck summary")
        if any(any(values) for values in race_counts):
            failures.append(f"racecheck reported hazards/errors/warnings: {race_counts}")
    elif not error_counts:
        failures.append("missing explicit sanitizer error summary")
    if any(error_counts):
        failures.append(f"sanitizer reported errors: {error_counts}")
    diagnostics = re.findall(r"(?mi)^=+\s*(?:Error|Warning|Fatal):[^\n]*", text)
    if diagnostics:
        failures.append("sanitizer diagnostic: " + " | ".join(diagnostics))
    if re.search(r"(?mi)^=+.*(?:no attachable process|no CUDA context|no CUDA application|not supported|unsupported GPU|failed to initialize|target application terminated before|no kernels (?:were )?(?:checked|instrumented|matched|launched)|0 kernels (?:checked|instrumented|matched))", text):
        failures.append("sanitizer could not instrument the CUDA workload")
    if re.search(r"(?mi)^=+.*(?:(?:invalid|unsupported|ignored).*(?:filter|regex)|(?:filter|regex).*(?:invalid|unsupported|ignored))", text):
        failures.append("sanitizer rejected or ignored the requested kernel filter")
    return {"pass": not failures, "failures": failures, "test_counts": test_counts,
            "executed_tests": test_names, "sanitizer_error_counts": error_counts,
            "racecheck_counts": race_counts, "sanitizer_leak_counts": leak_counts}


def json_receipts(text, prefix):
    return [json.loads(value) for value in re.findall(re.escape(prefix) + r":\s*(\{[^\n]*\})", text)]


def validate_http_receipt(receipt, profile, expected_cases):
    if receipt.get("profile") != profile or receipt.get("backend") != "cuda" or receipt.get("cases") != expected_cases:
        raise ValueError("HTTP backend/profile/case-count receipt mismatch")
    if receipt.get("kernel_launches", 0) <= 0:
        raise ValueError("HTTP receipt has no CUDA kernel launches")
    gru = receipt.get("gru_calls", -1)
    no_residual = receipt.get("gru_no_residual_calls", -1)
    if (profile == "pii" and not (gru == no_residual and no_residual > 0)) or (profile == "gliguard" and (gru != 0 or no_residual != 0)):
        raise ValueError("HTTP receipt violates the PII/GLiGuard GRU dispatch contract")
    initial = receipt["initial_device_memory"]
    final = receipt["final_device_memory"]
    reserved = receipt["reserved_physical_peak_bytes"]
    if not (0 < initial["live"] <= initial["peak"] <= reserved and 0 < final["live"] <= final["peak"] <= reserved):
        raise ValueError("tracked owned CUDA buffers exceed the reservation receipt")
    if reserved != max(initial["peak"], initial["live"] + receipt["max_reserved_scratch_bytes"]):
        raise ValueError("physical peak reservation does not match initial memory plus scratch")
    idle = receipt["idle_admission"]
    if idle.get("host_scratch_bytes") != 0 or idle.get("backend_scratch_bytes") != 0:
        raise ValueError("HTTP admission receipt retains scratch leases")


def source_receipt():
    # Commit identity binds unchanged shared dependencies. Fingerprint package
    # sources and dirty shared Zig code explicitly; include untracked new sources.
    paths = {path for path in (PACKAGE / "src").rglob("*") if path.is_file()
             and path.suffix in (".zig", ".c", ".h", ".cu", ".cuh", ".m")}
    paths.update(path for path in [PACKAGE / "build.zig", PACKAGE / "build.zig.zon", Path(__file__).resolve(),
                                   Path(__file__).resolve().with_name("qualify_http.py")] if path.exists())
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    dirty = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=REPO, text=True).splitlines()
    for name in dirty:
        path = REPO / name
        if name.startswith("zig/") and path.is_file() and path.suffix in (".zig", ".c", ".h", ".cu", ".cuh", ".m"):
            paths.add(path)
    files = {str(path.relative_to(REPO)): digest_file(path) for path in sorted(paths)}
    fingerprint = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"git_commit": commit, "source_sha256": fingerprint, "files": files,
            "binding_scope": "Observed source files and commit; binary SHA identifies the executable. This does not attest reproducible compilation."}


def run_logged(command, cwd, environment, path, timeout):
    started = time.monotonic()
    timed_out = False
    with path.open("wb") as log:
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
        finally:
            terminate_server(process)
    return {"command": command, "returncode": process.returncode, "timed_out": timed_out,
            "elapsed_seconds": time.monotonic() - started, "log": str(path), "log_sha256": digest_file(path)}


def write_report(path, report):
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


def qualify(args, report):
    manifest, references = verify_inputs(args.models_root)
    selected_cases = {}
    for profile, reference in references.items():
        known = [case["id"] for case in reference["cases"]]
        selected_cases[profile] = [name for name in known if name in (args.case or [])]
    unknown = set(args.case or []) - {case["id"] for ref in references.values() for case in ref["cases"]}
    if unknown:
        raise ValueError(f"unknown requested oracle cases: {sorted(unknown)}")
    version = subprocess.check_output([str(args.sanitizer), "--version"], text=True, stderr=subprocess.STDOUT, timeout=20)
    help_text = subprocess.check_output([str(args.sanitizer), "--help"], text=True, stderr=subprocess.STDOUT, timeout=20)
    for option in ("--target-processes", "--require-cuda-init", "--error-exitcode", "--check-exit-code"):
        if option not in help_text:
            raise ValueError(f"compute-sanitizer lacks required option {option}")
    if args.kernel_regex is not None and ("--kernel-name" not in help_text or "regex" not in help_text):
        raise ValueError("installed sanitizer does not advertise kernel regex filtering")
    requested_suites = args.filter or (*BASE_SUITES, *( ("pii-http", "gliguard-http") if args.include_http else () ))
    artifact_symbols = ptx_kernel_symbols((PACKAGE / "src/ops/cuda/artifacts/inference_cuda_kernels.ptx").read_text())
    report["sanitizer_kernel_scope"] = kernel_scope(args.kernel_regex, requested_suites, artifact_symbols, args.test_binary)
    report.update({"artifact_pins": manifest, "manifest_sha256": digest_file(MANIFEST), "hardware": hardware(), "source": source_receipt(),
        "test_binary": {"path": str(args.test_binary), "sha256": digest_file(args.test_binary)},
        "sanitizer": {"path": str(args.sanitizer), "sha256": digest_file(args.sanitizer), "version": version.strip()},
        "cuda_artifact_files": {name: digest_file(PACKAGE / "src/ops/cuda/artifacts" / name) for name in ARTIFACT_NAMES}})
    if args.candidate_server:
        report["candidate_server"] = {"path": str(args.candidate_server), "sha256": digest_file(args.candidate_server),
            "executed_by_this_runner": False, "qualification": "Use qualify_http.py for the production executable"}
    tool_root = args.sanitizer.resolve().parent
    report["sanitizer"]["injection_libraries"] = {str(path): digest_file(path) for path in sorted(tool_root.glob("*.so*")) if path.is_file()}
    environment = {key: value for key, value in os.environ.items() if not key.startswith("ANTFLY_INFERENCE_TEST_")}
    environment.pop("ANTFLY_GLINER_SPECIALTY_SANITIZER_CASES", None)
    environment.update({"TERMITE_REQUIRE_CUDA_TESTS": "1", "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda",
                        "ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE": "f32", "ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO": "0"})
    for profile, prefix in [("pii", "ANTFLY_GLINER_PII"), ("gliguard", "ANTFLY_GLIGUARD")]:
        environment[prefix + "_MODEL_DIR"] = str(args.models_root / profile)
        environment[prefix + "_CAPTURE"] = str(args.models_root / manifest["models"][profile]["oracle"]["path"])
    report["backend_environment"] = {key: value for key, value in environment.items() if key.startswith(("ANTFLY_GLINER_", "ANTFLY_GLIGUARD")) or key in (
        "TERMITE_REQUIRE_CUDA_TESTS", "ANTFLY_INFERENCE_REQUIRED_BACKEND", "ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE", "ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO")}
    report["lanes"] = []
    for tool in args.tool or TOOLS:
        for name in args.filter or (*BASE_SUITES, *( ("pii-http", "gliguard-http") if args.include_http else () )):
            spec = SUITES[name]
            lane_env = dict(environment)
            profile = spec.get("profile")
            cases = [case["id"] for case in references[profile]["cases"]] if profile else []
            if profile and not spec.get("http") and selected_cases[profile]:
                cases = selected_cases[profile]
                lane_env["ANTFLY_GLINER_SPECIALTY_SANITIZER_CASES"] = ",".join(cases)
            command = sanitizer_command(args.sanitizer, tool, args.test_binary, spec["filter"], args.kernel_regex)
            log_path = args.output.parent / (args.output.stem + "." + tool + "." + name + ".log")
            lane = {"tool": tool, "suite": name, "filter": spec["filter"], "oracle_case_ids": cases,
                    "kernel_regex": args.kernel_regex, "required_project_symbols": list(REQUIRED_KERNELS[name])}
            lane.update(run_logged(command, PACKAGE, lane_env, log_path, args.timeout))
            text = log_path.read_text(errors="replace")
            lane.update(assess_log(text, tool, spec["filter"], lane["returncode"], lane["timed_out"]))
            try:
                lane["checked_kernel_log"] = checked_kernel_log_receipt(text, args.kernel_regex)
                if lane["checked_kernel_log"]["available"]:
                    scope = report["sanitizer_kernel_scope"]
                    scope["runtime_checked_kernel_list_available"] = True
                    scope["runtime_checked_kernel_names_observed"] = sorted(set(scope.get("runtime_checked_kernel_names_observed", [])) | set(lane["checked_kernel_log"]["kernel_names"]))
                lane["head_receipts"] = json_receipts(text, "specialty_head_receipt")
                lane["http_receipts"] = json_receipts(text, "specialty_http_receipt")
                if spec.get("http"):
                    if len(lane["http_receipts"]) != 1:
                        raise ValueError("missing or ambiguous HTTP CUDA execution/memory receipt")
                    validate_http_receipt(lane["http_receipts"][0], profile, len(cases))
                elif profile:
                    if len(lane["head_receipts"]) != 1:
                        raise ValueError("missing head execution/case-selection receipt; test binary may predate sanitizer selector")
                    receipt = lane["head_receipts"][0]
                    if receipt.get("profile") != profile or receipt.get("backend") != "cuda" or receipt.get("case_ids") != cases or receipt.get("cases") != len(cases) or receipt.get("fixture_cases") != len(references[profile]["cases"]):
                        raise ValueError("head case-selection receipt differs from requested oracle cases")
            except (ValueError, KeyError, TypeError) as error:
                lane["failures"].append(str(error))
                lane["pass"] = False
            report["lanes"].append(lane)
            write_report(args.output, report)
            print(f"{tool}/{name}: {'PASS' if lane['pass'] else 'FAIL'} ({lane['elapsed_seconds']:.2f}s)", flush=True)
            if not lane["pass"] and not args.keep_going:
                raise ValueError(f"sanitizer lane failed: {tool}/{name}: {lane['failures']}")
    if digest_file(args.test_binary) != report["test_binary"]["sha256"] or source_receipt()["source_sha256"] != report["source"]["source_sha256"]:
        raise ValueError("test binary or source changed during sanitizer execution")
    if {name: digest_file(PACKAGE / "src/ops/cuda/artifacts" / name) for name in ARTIFACT_NAMES} != report["cuda_artifact_files"]:
        raise ValueError("CUDA artifacts changed during sanitizer execution")
    if args.candidate_server and digest_file(args.candidate_server) != report["candidate_server"]["sha256"]:
        raise ValueError("associated production candidate changed during sanitizer execution")
    if any(not lane["pass"] for lane in report["lanes"]):
        raise ValueError("one or more sanitizer lanes failed")


def coverage_flags(passed, tools, suites, case_selection, kernel_regex):
    full_tools = passed and set(tools) == set(TOOLS)
    full_heads = passed and not case_selection and {"pii-head", "gliguard-head"}.issubset(suites)
    http_included = {"pii-http", "gliguard-http"}.issubset(suites)
    all_kernels = passed and kernel_regex is None
    return {"full_tool_coverage": full_tools, "full_head_case_coverage": full_heads,
        "public_http_included": http_included, "public_http_passed": passed and http_included,
        "all_kernel_coverage": all_kernels, "vendor_library_coverage_claimed": all_kernels,
        "complete_sanitizer_campaign": full_tools and full_heads and http_included and all_kernels}


def main():
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", required=True, type=Path, help="Existing CUDA runtime-filtering Zig test executable")
    parser.add_argument("--candidate-server", type=Path, help="Optional production binary to fingerprint; not executed here")
    parser.add_argument("--sanitizer", type=Path, default=Path("/usr/local/cuda/bin/compute-sanitizer"))
    parser.add_argument("--models-root", type=Path, default=REPO / ".specialty-artifacts")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tool", action="append", choices=TOOLS, help="Repeat to select tools; default all four")
    parser.add_argument("--filter", action="append", choices=(*SUITES, *(spec["filter"] for spec in SUITES.values())), help="Suite alias or exact supported test name; repeat. Default GRU + both complete head fixtures")
    parser.add_argument("--include-http", action="store_true", help="Add full loopback public HTTP tests, including maximum geometry")
    parser.add_argument("--case", action="append", help="Frozen case ID for head lanes only; repeat. Unspecified profile runs all its cases")
    parser.add_argument("--kernel-regex", help="Explicit kernel-name scope, e.g. ^termite_.*; default instruments all kernels including vendor libraries")
    parser.add_argument("--timeout", type=float, default=1800, help="Seconds per sanitizer lane")
    parser.add_argument("--keep-going", action="store_true", help="Record all requested failing lanes instead of stopping on first failure")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.kernel_regex is not None:
        try:
            compile_kernel_regex(args.kernel_regex)
        except ValueError as error:
            parser.error(str(error))
    if args.filter:
        aliases = {spec["filter"]: name for name, spec in SUITES.items()}
        args.filter = list(dict.fromkeys(aliases.get(value, value) for value in args.filter))
    if args.tool:
        args.tool = list(dict.fromkeys(args.tool))
    for name in ("test_binary", "candidate_server", "sanitizer", "models_root", "output"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"format_version": 1, "status": "failed", "campaign": "GB10 CUDA specialty sanitizer correctness",
        "started_unix_seconds": time.time(), "tools": list(args.tool or TOOLS),
        "requested_suites": list(args.filter or (*BASE_SUITES, *( ("pii-http", "gliguard-http") if args.include_http else () ))),
        "case_selection": args.case, "sanitizer_kernel_filter": args.kernel_regex,
        "memory_counter_scope": "Owned CUDA DeviceBuffers; driver/library-private allocations excluded",
        "timing_scope": "Sanitizer wall time only; not a latency/throughput benchmark"}
    try:
        qualify(args, report)
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = str(error)
        report["traceback"] = traceback.format_exc()
        print(report["traceback"], file=sys.stderr)
    finally:
        report["finished_unix_seconds"] = time.time()
        report.update(coverage_flags(report["status"] == "passed", report["tools"],
            report["requested_suites"], args.case, args.kernel_regex))
        write_report(args.output, report)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
