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

"""Fail-closed release evidence audit and serial driver soak for attention matching.

A driver is an explicit local argv array; it must exercise real serving and emit
one bounded JSON report per next command on stdin, and stop on a stop command. This tool cannot make an unqualified runtime capable.
Smoke-duration runs produce diagnostic receipts, never four-hour qualification.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import shutil
import time
import random
import itertools
import struct
import stat as stat_mod
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_resources import DiskSpaceGuard, group_rss_mib, memory_free_percent, swapout_bytes

BUCKETS = {"document_qa", "exact_retrieval", "code", "structured_tool", "multiturn"}
ROUTES = {"native", "cli", "http", "stream", "serial_mixed_batch"}
CONTEXT_ALLOCATION_SCOPE = "request_exclusive_context"
CONTEXT_ALLOCATION_SCOPE_VERSION = 1
CONTEXT_EXECUTION_POOL_ADMISSION_VERSION = 2
CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES = 80 * 1024 * 1024
FAILURE_ORDER = ("cancel_prefill", "cancel_replay", "cancel_fit", "cancel_publish",
                 "oom", "nonfinite", "gpu_failure", "reload")
FAILURES = set(FAILURE_ORDER)
FAILURE_STAGES = {"cancel_prefill": "prefill", "cancel_replay": "replay", "cancel_fit": "fit",
                  "cancel_publish": "publish", "oom": "admission", "nonfinite": "fit",
                  "gpu_failure": "gpu", "reload": "reload"}
FAILURE_MECHANISMS = {
    "cancel_prefill": "cooperative_checkpoint_after_partial_work",
    "cancel_replay": "cooperative_checkpoint_after_partial_work",
    "cancel_fit": "cooperative_checkpoint_after_partial_work",
    "cancel_publish": "transaction_boundary_cancellation",
    "oom": "request_local_allocator_failure",
    "nonfinite": "device_fitter_nonfinite_status",
    "gpu_failure": "simulated_gpu_completion_error",
    "reload": "model_manager_generation_transition",
}
BINDINGS = {"artifact", "tokenizer", "template", "profile", "source_build", "binary", "device", "os"}
SEMANTIC_BINDINGS = {"artifact", "tokenizer", "template", "layout", "profile", "implementation"}
PROVENANCE_BINDINGS = {"binary_sha256", "source_build_sha256", "device_sha256", "os_sha256"}
PROVENANCE_FILES = {"artifact", "profile", "source_build", "binary"}
PERFORMANCE_VARIANTS = ("exact", "packed_exact", "attention_matching")
MIN_PERFORMANCE_PAIRS = 30
MIN_PERFORMANCE_WARMUPS = 5
MIN_QUALITY_CASES = 100
MIN_COMMITTED_SAVINGS = .25
MIN_PACKED_EXACT_DECODE_RATIO = 1 / 1.05
MAX_REPORT = 16 * 1024 * 1024
MAX_RAW_NUMERICAL_CAPTURES = 512
MAX_RAW_NUMERICAL_FITS = 512
MAX_RAW_NUMERICAL_METADATA_BYTES = 64 * 1024 ** 2
MAX_RAW_NUMERICAL_INPUT_BYTES = 8 * 1024 ** 3
MAX_RAW_NUMERICAL_FIT_WORK_BYTES = 64 * 1024 ** 3
MAX_JOURNAL_BYTES = 64 * 1024 ** 2
MAX_JOURNAL_RECORDS = 262144
MAX_DRIVER_LINE_BYTES = 65536
CHILD_FILE_LIMIT_POLICY = {
    "bytes": MAX_JOURNAL_BYTES,
    "sigxfsz_disposition": "ignored",
    "oversize_result": "EFBIG",
}


def _limit_qualification_child_files():
    """Keep the byte ceiling while making cache overflow recoverable by the child."""
    import resource
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_JOURNAL_BYTES, MAX_JOURNAL_BYTES))
    # Popen restores Python's inherited SIG_IGN to SIG_DFL before preexec_fn.
    # Set only SIGXFSZ back to ignored: POSIX then requires writes/truncates
    # beyond RLIMIT_FSIZE to fail with EFBIG. SIGPIPE and other defaults remain.
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def read_json(path, expected_sha256=None):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_REPORT + 1)
    require(len(raw) <= MAX_REPORT, "evidence exceeds byte limit")
    if expected_sha256 is not None:
        require(hashlib.sha256(raw).hexdigest() == expected_sha256, "evidence digest mismatch")
    return decode_json(raw)


def decode_json(raw):
    def bad(value):
        raise ValueError("nonfinite JSON number: " + value)
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key: " + key)
            result[key] = value
        return result
    return json.loads(raw, parse_constant=bad, object_pairs_hook=pairs)


def journal_records(path):
    """Stream bounded JSONL without accepting a truncated final event."""
    require(Path(path).stat().st_size <= MAX_JOURNAL_BYTES, "journal exceeds byte limit")
    with Path(path).open("rb") as stream:
        count = 0
        while line := stream.readline(MAX_DRIVER_LINE_BYTES + 1):
            count += 1
            require(count <= MAX_JOURNAL_RECORDS, "journal exceeds record limit")
            require(len(line) <= MAX_DRIVER_LINE_BYTES and line.endswith(b"\n"),
                    "oversized or truncated journal record")
            yield decode_json(line)


def write_json(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, allow_nan=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _write_driver_command(process, value, deadline, poll_guard):
    """Write one bounded JSON command without escaping the request deadline."""
    require(process.stdin is not None and not process.stdin.closed,
            "driver command pipe is closed")
    raw = (json.dumps(value, allow_nan=False, sort_keys=True) + "\n").encode()
    require(len(raw) <= MAX_DRIVER_LINE_BYTES, "driver command exceeds byte limit")
    descriptor = process.stdin.fileno()
    os.set_blocking(descriptor, False)
    offset = 0
    while offset < len(raw):
        require(process.poll() is None, "driver exited while accepting command")
        require(time.monotonic() <= deadline, "driver command write timed out")
        poll_guard()
        remaining = max(0.0, deadline - time.monotonic())
        try:
            _, writable, _ = select.select([], [descriptor], [], min(.25, remaining))
        except InterruptedError:
            continue
        if not writable:
            continue
        try:
            written = os.write(descriptor, raw[offset:])
        except (BlockingIOError, InterruptedError):
            continue
        except BrokenPipeError as error:
            raise ValueError("driver closed command pipe") from error
        require(written > 0, "driver command pipe stopped accepting bytes")
        offset += written


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def campaign_file(root, entry, label, max_bytes=None):
    """Resolve and hash one immutable campaign input without trusting its label."""
    require(type(entry) is dict and set(entry) == {"path", "sha256"}, "missing " + label + " file provenance")
    require(isinstance(entry["path"], str) and entry["path"], "invalid " + label + " file path")
    require(isinstance(entry["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]),
            "invalid " + label + " file digest")
    relative = Path(entry["path"])
    require(not relative.is_absolute() and ".." not in relative.parts, label + " path escapes campaign")
    unresolved = root / relative
    require(not unresolved.is_symlink(), label + " file must not be a symlink")
    path = unresolved.resolve()
    require(path.is_relative_to(root) and path.is_file(), "unverifiable " + label + " file")
    if max_bytes is not None:
        require(path.stat().st_size <= max_bytes, label + " exceeds byte limit")
    require(sha(path) == entry["sha256"], label + " file digest mismatch")
    return path


CLI_BUILD_RECEIPT_FIELDS = {
    "schema_version", "evidence_scope", "created_utc", "sealed_utc", "head", "source", "build",
    "zig_version", "tools", "diagnostic_only", "serving_qualified", "build_completed", "sealed",
    "binary", "binary_sha256", "binary_bytes", "build_resource_receipt",
    "build_resource_receipt_sha256", "build_stdout", "build_stdout_sha256", "build_stderr",
    "build_stderr_sha256", "source_verified_after_build", "source_archive",
    "source_archive_sha256", "source_archive_note",
}
CLI_BUILD_MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
CLI_BUILD_MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
CLI_BUILD_MAX_SOURCE_FILES = 4096
CLI_BUILD_MAX_LOG_BYTES = 256 * 1024 * 1024
CLI_BUILD_LIMITS = {"timeout_seconds": 1800, "max_rss_mib": 8192,
                    "min_free_percent": 15, "max_swap_growth_mib": 512,
                    "min_disk_free_mib": 4096}


def _cli_build_file(path_text, label, maximum, allow_empty=False):
    require(isinstance(path_text, str) and Path(path_text).is_absolute(),
            "invalid CLI build " + label + " path")
    path = Path(path_text)
    require(not path.is_symlink() and path.is_file(), "missing CLI build " + label)
    size = path.stat().st_size
    require((allow_empty or size > 0) and size <= maximum, "invalid CLI build " + label + " size")
    return path


def _cli_build_resource_contract(receipt, expected_command):
    require(type(receipt) is dict and receipt.get("schema") == "antfly.benchmark_resources.v1" and
            receipt.get("command") == expected_command and receipt.get("pass") is True and
            receipt.get("violation") is None and "cleanup_error" not in receipt,
            "CLI build resource guard failed")
    require(type(receipt.get("returncode")) is int and not isinstance(receipt["returncode"], bool) and
            receipt["returncode"] == 0 and type(receipt.get("samples")) is int and
            not isinstance(receipt["samples"], bool) and receipt["samples"] > 0 and
            type(receipt.get("pid")) is int and not isinstance(receipt["pid"], bool) and
            receipt["pid"] > 0, "invalid CLI build resource completion")
    require(receipt.get("limits") == CLI_BUILD_LIMITS, "unexpected CLI build resource limits")
    peak = number(receipt.get("peak_group_rss_mib"), "CLI build peak RSS")
    minimum_free = number(receipt.get("min_free_percent"), "CLI build minimum free memory")
    initial_free = number(receipt.get("initial_free_percent"), "CLI build initial free memory")
    swap = number(receipt.get("swapout_growth_mib"), "CLI build swap growth")
    disk = number(receipt.get("min_disk_free_mib"), "CLI build minimum free disk")
    elapsed = number(receipt.get("elapsed_seconds"), "CLI build elapsed time")
    require(0 <= peak <= CLI_BUILD_LIMITS["max_rss_mib"] and
            CLI_BUILD_LIMITS["min_free_percent"] <= minimum_free <= initial_free <= 100 and
            0 <= swap <= CLI_BUILD_LIMITS["max_swap_growth_mib"] and
            disk >= CLI_BUILD_LIMITS["min_disk_free_mib"] and
            0 < elapsed <= CLI_BUILD_LIMITS["timeout_seconds"],
            "CLI build resource measurements violate limits")


def cli_build_evidence_contract(receipt_path, expected_receipt_sha256, expected_binary_sha256,
                                expected_binary_path=None):
    """Audit standalone CLI build evidence without promoting serving qualification."""
    require(re.fullmatch(r"[0-9a-f]{64}", expected_receipt_sha256 or "") and
            re.fullmatch(r"[0-9a-f]{64}", expected_binary_sha256 or ""),
            "invalid CLI build evidence identity")
    receipt_path = Path(receipt_path)
    require(not receipt_path.is_symlink() and receipt_path.is_file() and
            receipt_path.stat().st_size <= MAX_REPORT and sha(receipt_path) == expected_receipt_sha256,
            "CLI source build receipt mismatch")
    receipt = read_json(receipt_path, expected_receipt_sha256)
    require(type(receipt) is dict and set(receipt) == CLI_BUILD_RECEIPT_FIELDS and
            receipt["schema_version"] == 1 and receipt["evidence_scope"] == "build_only" and
            receipt["build_completed"] is True and receipt["sealed"] is True and
            receipt["source_verified_after_build"] is True and receipt["diagnostic_only"] is True and
            receipt["serving_qualified"] is False,
            "invalid CLI build evidence scope")
    require(isinstance(receipt["created_utc"], str) and receipt["created_utc"] and
            isinstance(receipt["sealed_utc"], str) and receipt["sealed_utc"] and
            re.fullmatch(r"[0-9a-f]{40}", receipt["head"] or "") and
            isinstance(receipt["zig_version"], str) and receipt["zig_version"],
            "invalid CLI build identity")

    binary = _cli_build_file(receipt["binary"], "binary", 1024 * 1024 * 1024)
    require(receipt["binary_sha256"] == expected_binary_sha256 == sha(binary) and
            type(receipt["binary_bytes"]) is int and not isinstance(receipt["binary_bytes"], bool) and
            receipt["binary_bytes"] == binary.stat().st_size and os.access(binary, os.X_OK),
            "CLI build receipt does not bind executable")
    if expected_binary_path is not None:
        require(binary.resolve() == Path(expected_binary_path).resolve(), "CLI build executable path mismatch")

    build = receipt["build"]
    require(type(build) is dict and set(build) == {"cwd", "command", "output", "target", "optimize",
                                                   "metal", "cuda", "onnx"} and
            build["cwd"] == "zig" and build["output"] == receipt["binary"] and
            build["target"] == "antfly" and build["optimize"] == "ReleaseFast" and
            build["metal"] is True and build["cuda"] is False and build["onnx"] is False,
            "invalid CLI build configuration")
    prefix = str(binary.parent.parent)
    command = build["command"]
    fixed = ["/usr/bin/env", "ANTFLY_ZIG_MAX_RSS=8589934592", "python3",
             "tools/run_bounded_zig_build.py", "build", "antfly", "-j1", "-Dmetal=true",
             "-Dcuda=false", "-Donnx=false", "-Doptimize=ReleaseFast"]
    require(type(command) is list and len(command) == len(fixed) + 3 and
            command[:len(fixed)] == fixed and
            isinstance(command[len(fixed)], str) and command[len(fixed)].startswith("-Dantfly-version=context-") and
            command[len(fixed) + 1:] == ["--prefix", prefix],
            "unexpected CLI build command")

    resource_path = _cli_build_file(receipt["build_resource_receipt"], "resource receipt", MAX_REPORT)
    stdout_path = _cli_build_file(receipt["build_stdout"], "stdout", CLI_BUILD_MAX_LOG_BYTES, True)
    stderr_path = _cli_build_file(receipt["build_stderr"], "stderr", CLI_BUILD_MAX_LOG_BYTES, True)
    require(sha(resource_path) == receipt["build_resource_receipt_sha256"] and
            sha(stdout_path) == receipt["build_stdout_sha256"] and
            sha(stderr_path) == receipt["build_stderr_sha256"],
            "CLI build artifact digest mismatch")
    _cli_build_resource_contract(read_json(resource_path, receipt["build_resource_receipt_sha256"]), command)
    require(not any(line.startswith("error:") for line in
                    stderr_path.read_text(encoding="utf-8", errors="replace").splitlines()),
            "CLI build compiler log contains an error")

    source = receipt["source"]
    require(type(source) is dict and set(source) == {"contract", "files", "deleted"} and
            source["contract"] == "Git HEAD binds unchanged tracked inputs; files/deleted bind the dirty and untracked Zig build-input superset." and
            type(source["files"]) is dict and len(source["files"]) <= CLI_BUILD_MAX_SOURCE_FILES and
            type(source["deleted"]) is list and len(source["deleted"]) <= CLI_BUILD_MAX_SOURCE_FILES,
            "invalid CLI build source manifest")
    def source_name(value):
        if not (isinstance(value, str) and value.startswith("zig/") and not Path(value).is_absolute() and
                all(part not in ("", ".", "..") for part in Path(value).parts)):
            return False
        path = Path(value)
        return path.name in {"build.zig", "build.zig.zon"} or path.suffix in {
            ".zig", ".zon", ".metal", ".m", ".mm", ".c", ".h", ".inc", ".s", ".S"}
    require(all(source_name(name) and re.fullmatch(r"[0-9a-f]{64}", digest or "")
                for name, digest in source["files"].items()) and
            all(source_name(name) for name in source["deleted"]) and
            not (set(source["files"]) & set(source["deleted"])),
            "invalid CLI build source entry")
    tools = receipt["tools"]
    require(type(tools) is dict and set(tools) == {"orchestrator_sha256", "bounded_build_sha256",
                                                   "resource_guard_sha256"} and
            all(re.fullmatch(r"[0-9a-f]{64}", value or "") for value in tools.values()),
            "invalid CLI build tool provenance")

    archive = _cli_build_file(receipt["source_archive"], "source archive", CLI_BUILD_MAX_ARCHIVE_BYTES)
    require(sha(archive) == receipt["source_archive_sha256"] and
            receipt["source_archive_note"] == "Archive contains the source snapshot and every dirty/untracked build input; Git HEAD supplies unchanged tracked inputs.",
            "CLI source archive mismatch")
    snapshot_fields = {"schema_version", "evidence_scope", "created_utc", "head", "source", "build",
                       "zig_version", "tools", "diagnostic_only", "serving_qualified"}
    snapshot = {key: receipt[key] for key in snapshot_fields}
    expected_snapshot = (json.dumps(snapshot, indent=2, sort_keys=True) + "\n").encode()
    expected_members = {"SOURCE-SNAPSHOT.json", *source["files"]}
    with zipfile.ZipFile(archive) as bundle:
        infos = bundle.infolist()
        names = [info.filename for info in infos]
        require(len(names) == len(set(names)) and set(names) == expected_members and
                sum(info.file_size for info in infos) <= CLI_BUILD_MAX_UNCOMPRESSED_BYTES,
                "CLI source archive membership mismatch")
        for info in infos:
            require(not info.is_dir() and
                    (info.filename == "SOURCE-SNAPSHOT.json" or source_name(info.filename)),
                    "unsafe CLI source archive member")
            mode = info.external_attr >> 16
            require(not stat_mod.S_ISLNK(mode), "CLI source archive contains symlink")
        require(bundle.read("SOURCE-SNAPSHOT.json") == expected_snapshot,
                "CLI source archive snapshot mismatch")
        for name, digest in source["files"].items():
            require(hashlib.sha256(bundle.read(name)).hexdigest() == digest,
                    "CLI source archive file digest mismatch")
    return {"build_evidence_only": True, "binary_sha256": expected_binary_sha256,
            "source_archive_sha256": receipt["source_archive_sha256"], "head": receipt["head"]}


FULL_RELEASE_BUILD_FIELDS_V1 = {
    "schema", "evidence_scope", "release_eligible", "serving_qualified", "created_utc", "sealed_utc",
    "head", "zig_version", "tools", "binary", "build", "resource", "stdout", "stderr", "tests", "source",
    "source_archive", "source_verified_after_build",
}
FULL_RELEASE_BUILD_FIELDS_V2 = FULL_RELEASE_BUILD_FIELDS_V1 | {"compiler_profile"}
FULL_RELEASE_BUILD_FIELDS = FULL_RELEASE_BUILD_FIELDS_V1
FULL_RELEASE_SCHEMA_V1 = "antfly.context-release-build.v1"
FULL_RELEASE_SCHEMA_V2 = "antfly.context-release-build.v2"
FULL_RELEASE_SCHEMA_V3 = "antfly.context-release-build.v3"
FULL_RELEASE_SCHEMA_V4 = "antfly.context-release-build.v4"
FULL_RELEASE_EXPECTED_STRIPPED_PROFILE = {"strip_debug_info": True, "have_error_return_tracing": False}
FULL_RELEASE_COMPILER_PROFILE_PREFIX = "context_compiler_profile: "
FULL_RELEASE_SOURCE_NOTE = (
    "Complete Git-visible Zig tree inputs are archived; deleted binds tracked removals; Git-ignored caches and outputs are excluded; gitlinks bind clean or uninitialized submodules."
)
FULL_RELEASE_ARCHIVE_MANIFEST = "FULL-RELEASE-SOURCE.json"
FULL_RELEASE_MAX_SOURCE_FILES = 8192
FULL_RELEASE_MAX_ARCHIVE_BYTES = 512 * 1024 * 1024
FULL_RELEASE_MAX_UNCOMPRESSED_BYTES = 768 * 1024 * 1024
FULL_RELEASE_COMMAND = [
    "/usr/bin/env", "ANTFLY_ZIG_MAX_RSS=8589934592", "python3", "tools/run_bounded_zig_build.py",
    "build", "inference-test-context", "--verbose", "-j1", "-Dmetal=true", "-Dcuda=false", "-Donnx=false",
    "-Doptimize=ReleaseFast",
]
FULL_RELEASE_STRIPPED_COMMAND = [*FULL_RELEASE_COMMAND, "-Dstrip=true"]
FULL_RELEASE_CURRENT_STRIPPED_COMMAND = [
    argument for argument in FULL_RELEASE_COMMAND if argument != "-Donnx=false"
] + ["-Dstrip=true"]
FULL_RELEASE_16G_STRIPPED_COMMAND = [
    "ANTFLY_ZIG_MAX_RSS=17179869184" if argument == "ANTFLY_ZIG_MAX_RSS=8589934592" else argument
    for argument in FULL_RELEASE_CURRENT_STRIPPED_COMMAND
] + ["-Dcontext-test-compile-memory-mb=16384"]
FULL_RELEASE_CLEARED_ENV_PREFIXES = ["ANTFLY_", "TERMITE_", "MTL_", "METAL_", "CUDA_", "GGML_", "ZIG_"]
FULL_RELEASE_ENVIRONMENT = {
    "inherit_non_runtime_environment": True,
    "cleared_prefixes": FULL_RELEASE_CLEARED_ENV_PREFIXES,
    "set_by_command": {"ANTFLY_ZIG_MAX_RSS": "8589934592"},
}
FULL_RELEASE_16G_ENVIRONMENT = {
    **FULL_RELEASE_ENVIRONMENT,
    "set_by_command": {"ANTFLY_ZIG_MAX_RSS": "17179869184"},
}


def _full_release_resource_contract(receipt, expected_command):
    require(type(receipt) is dict and receipt.get("schema") == "antfly.benchmark_resources.v1" and
            receipt.get("command") == expected_command and receipt.get("pass") is True and
            receipt.get("violation") is None and "cleanup_error" not in receipt,
            "full release build resource guard failed")
    require(type(receipt.get("returncode")) is int and not isinstance(receipt["returncode"], bool) and
            receipt["returncode"] == 0 and type(receipt.get("samples")) is int and
            not isinstance(receipt["samples"], bool) and receipt["samples"] > 0 and
            type(receipt.get("pid")) is int and not isinstance(receipt["pid"], bool) and
            receipt["pid"] > 0, "invalid full release build completion")
    limits = receipt.get("limits")
    require(type(limits) is dict and set(limits) == {"timeout_seconds", "max_rss_mib", "min_free_percent",
                                                     "max_swap_growth_mib", "min_disk_free_mib"},
            "invalid full release build limits")
    timeout = number(limits["timeout_seconds"], "full release build timeout", 1)
    rss = number(limits["max_rss_mib"], "full release build RSS cap", 1)
    free = number(limits["min_free_percent"], "full release build memory floor")
    swap = number(limits["max_swap_growth_mib"], "full release build swap cap")
    disk = number(limits["min_disk_free_mib"], "full release build disk floor")
    require(timeout <= 3600 and rss <= 16384 and free >= 15 and 0 <= swap <= 512 and disk >= 4096,
            "insufficient full release build limits")
    peak = number(receipt.get("peak_group_rss_mib"), "full release measured RSS")
    minimum_free = number(receipt.get("min_free_percent"), "full release measured free memory")
    initial_free = number(receipt.get("initial_free_percent"), "full release initial free memory")
    swap_growth = number(receipt.get("swapout_growth_mib"), "full release measured swap growth")
    minimum_disk = number(receipt.get("min_disk_free_mib"), "full release measured disk")
    elapsed = number(receipt.get("elapsed_seconds"), "full release build elapsed")
    require(0 <= peak <= rss and free <= minimum_free <= initial_free <= 100 and
            0 <= swap_growth <= swap and minimum_disk >= disk and 0 < elapsed <= timeout,
            "full release build exceeded resource limits")


def full_release_build_contract(root, receipt_path, expected_binary):
    """Audit source/build evidence only; serving, numerical, quality, and soak remain separate gates."""
    receipt = read_json(receipt_path)
    require(type(receipt) is dict, "invalid full release build receipt")
    require(receipt.get("release_eligible") is not False and
            receipt.get("evidence_scope") == "full_context_release_build",
            "release-ineligible or scoped build receipt cannot support full release")
    schema = receipt.get("schema")
    expected_fields = (FULL_RELEASE_BUILD_FIELDS_V1 if schema == FULL_RELEASE_SCHEMA_V1 else
                       FULL_RELEASE_BUILD_FIELDS_V2 if schema in {FULL_RELEASE_SCHEMA_V2,
                                                                  FULL_RELEASE_SCHEMA_V3,
                                                                  FULL_RELEASE_SCHEMA_V4} else None)
    require(expected_fields is not None and set(receipt) == expected_fields and
            receipt["release_eligible"] is True and receipt["serving_qualified"] is False and
            receipt["source_verified_after_build"] is True,
            "invalid full release build evidence")
    require(isinstance(receipt["created_utc"], str) and receipt["created_utc"] and
            isinstance(receipt["sealed_utc"], str) and receipt["sealed_utc"] and
            re.fullmatch(r"[0-9a-f]{40}", receipt["head"] or "") and
            isinstance(receipt["zig_version"], str) and receipt["zig_version"],
            "invalid full release build identity")
    tools = receipt["tools"]
    require(type(tools) is dict and set(tools) == {"python3", "zig"},
            "invalid full release toolchain")
    for name, entry in tools.items():
        require(type(entry) is dict and set(entry) == {"path", "sha256", "bytes", "version"} and
                isinstance(entry["path"], str) and Path(entry["path"]).is_absolute() and
                re.fullmatch(r"[0-9a-f]{64}", entry["sha256"] or "") and
                type(entry["bytes"]) is int and not isinstance(entry["bytes"], bool) and entry["bytes"] > 0 and
                isinstance(entry["version"], str) and entry["version"],
                "invalid full release tool identity")
        tool_path = Path(entry["path"])
        require(not tool_path.is_symlink() and tool_path.is_file() and os.access(tool_path, os.X_OK) and
                tool_path.stat().st_size == entry["bytes"] and sha(tool_path) == entry["sha256"],
                "full release build tool changed")
    require(receipt["zig_version"] == tools["zig"]["version"], "full release Zig version mismatch")

    binary = receipt["binary"]
    require(type(binary) is dict and set(binary) == {"path", "sha256", "bytes"} and
            binary["path"] == expected_binary["path"] and binary["sha256"] == expected_binary["sha256"] and
            type(binary["bytes"]) is int and not isinstance(binary["bytes"], bool) and binary["bytes"] > 0,
            "full release build receipt does not bind executable")
    binary_path = campaign_file(root, {"path": binary["path"], "sha256": binary["sha256"]},
                                "full release binary", 1024 * 1024 * 1024)
    require(binary_path.stat().st_size == binary["bytes"] and os.access(binary_path, os.X_OK),
            "invalid full release executable")

    build = receipt["build"]
    expected_build_fields = ({"cwd", "command", "target", "optimize", "metal", "cuda", "environment"}
                             if schema in {FULL_RELEASE_SCHEMA_V3, FULL_RELEASE_SCHEMA_V4} else
                             {"cwd", "command", "target", "optimize", "metal", "cuda", "onnx", "environment"})
    require(type(build) is dict and set(build) == expected_build_fields and
            build["cwd"] == "zig" and build["target"] == "inference-test-context" and
            build["optimize"] == "ReleaseFast" and build["metal"] is True and
            build["cuda"] is False and
            (schema in {FULL_RELEASE_SCHEMA_V3, FULL_RELEASE_SCHEMA_V4} or build["onnx"] is False) and
            build["environment"] == (FULL_RELEASE_16G_ENVIRONMENT if schema == FULL_RELEASE_SCHEMA_V4 else FULL_RELEASE_ENVIRONMENT),
            "invalid full release build configuration")
    command = build["command"]
    expected_command = (FULL_RELEASE_COMMAND if schema == FULL_RELEASE_SCHEMA_V1 else
                        FULL_RELEASE_STRIPPED_COMMAND if schema == FULL_RELEASE_SCHEMA_V2 else
                        FULL_RELEASE_CURRENT_STRIPPED_COMMAND if schema == FULL_RELEASE_SCHEMA_V3 else
                        FULL_RELEASE_16G_STRIPPED_COMMAND)
    require(command == expected_command, "invalid full release guarded command")
    if schema in {FULL_RELEASE_SCHEMA_V2, FULL_RELEASE_SCHEMA_V3, FULL_RELEASE_SCHEMA_V4}:
        compiler_profile = receipt["compiler_profile"]
        require(type(compiler_profile) is dict and
                set(compiler_profile) == {"strip_debug_info", "have_error_return_tracing"} and
                type(compiler_profile.get("strip_debug_info")) is bool and
                type(compiler_profile.get("have_error_return_tracing")) is bool and
                compiler_profile == FULL_RELEASE_EXPECTED_STRIPPED_PROFILE,
                "invalid stripped compiler profile")

    refs = {}
    for field, maximum, allow_empty in (("resource", MAX_REPORT, False),
                                         ("stdout", CLI_BUILD_MAX_LOG_BYTES, True),
                                         ("stderr", CLI_BUILD_MAX_LOG_BYTES, True)):
        ref = receipt[field]
        refs[field] = campaign_file(root, ref, "full release build " + field, maximum)
        require(allow_empty or refs[field].stat().st_size > 0, "empty full release build " + field)
    resource = read_json(refs["resource"], receipt["resource"]["sha256"])
    _full_release_resource_contract(resource, command)
    if schema == FULL_RELEASE_SCHEMA_V4:
        require(resource["limits"]["max_rss_mib"] == 16384, "V4 compiler watchdog differs from declared budget")
    stderr = refs["stderr"].read_text(encoding="utf-8", errors="replace")
    require(not any(line.startswith("error:") for line in stderr.splitlines()),
            "full release compiler log contains an error")
    profile_rows = [line[len(FULL_RELEASE_COMPILER_PROFILE_PREFIX):] for line in stderr.splitlines()
                    if line.startswith(FULL_RELEASE_COMPILER_PROFILE_PREFIX)]
    if schema in {FULL_RELEASE_SCHEMA_V2, FULL_RELEASE_SCHEMA_V3, FULL_RELEASE_SCHEMA_V4}:
        require(len(profile_rows) == 1, "stripped full release requires exactly one compiler profile observation")
        try:
            observed_profile = json.loads(profile_rows[0])
        except json.JSONDecodeError as error:
            raise ValueError("invalid compiler profile observation") from error
        require(type(observed_profile) is dict and
                set(observed_profile) == {"schema_version", "strip_debug_info", "have_error_return_tracing"} and
                type(observed_profile.get("schema_version")) is int and
                not isinstance(observed_profile["schema_version"], bool) and observed_profile["schema_version"] == 1 and
                type(observed_profile.get("strip_debug_info")) is bool and
                type(observed_profile.get("have_error_return_tracing")) is bool and
                {key: observed_profile[key] for key in FULL_RELEASE_EXPECTED_STRIPPED_PROFILE} ==
                receipt["compiler_profile"], "compiler profile receipt/log mismatch")

    tests = receipt["tests"]
    require(type(tests) is dict and set(tests) == {"selected", "passed", "skipped", "failed", "leaked"} and
            all(type(tests[field]) is int and not isinstance(tests[field], bool) and tests[field] >= 0
                for field in tests) and tests["selected"] > 0 and tests["passed"] > 0 and
            tests["passed"] + tests["skipped"] == tests["selected"] and
            tests["failed"] == 0 and tests["leaked"] == 0,
            "full release compiled tests did not pass")
    summaries = re.findall(r"(?m)^(\d+) selected; (\d+) passed; (\d+) skipped(?:; (\d+) failed; (\d+) leaked)?\.$", stderr)
    require(summaries, "full release build log lacks test summary")
    selected, passed, skipped, failed, leaked = summaries[-1]
    require((int(selected), int(passed), int(skipped), int(failed or 0), int(leaked or 0)) ==
            tuple(tests[field] for field in ("selected", "passed", "skipped", "failed", "leaked")),
            "full release test summary mismatch")

    source = receipt["source"]
    require(type(source) is dict and set(source) == {"contract", "files", "deleted", "gitlinks"} and
            source["contract"] == FULL_RELEASE_SOURCE_NOTE and type(source["files"]) is dict and
            len(source["files"]) <= FULL_RELEASE_MAX_SOURCE_FILES and type(source["deleted"]) is list and
            len(source["deleted"]) <= FULL_RELEASE_MAX_SOURCE_FILES and type(source["gitlinks"]) is dict and
            len(source["gitlinks"]) <= FULL_RELEASE_MAX_SOURCE_FILES,
            "invalid full release source manifest")
    def source_name(name):
        if not (isinstance(name, str) and name.startswith("zig/") and not Path(name).is_absolute() and
                all(part not in ("", ".", "..") for part in Path(name).parts)):
            return False
        return True
    require(all(source_name(name) and re.fullmatch(r"[0-9a-f]{64}", digest or "")
                for name, digest in source["files"].items()) and
            all(source_name(name) for name in source["deleted"]) and
            all(source_name(name) and re.fullmatch(r"[0-9a-f]{40}", object_id or "")
                for name, object_id in source["gitlinks"].items()) and
            not set(source["files"]).intersection(source["deleted"]) and
            not set(source["gitlinks"]).intersection(set(source["files"]) | set(source["deleted"])),
            "invalid full release source entry")

    archive = receipt["source_archive"]
    require(type(archive) is dict and set(archive) == {"path", "sha256", "bytes", "files"} and
            type(archive["bytes"]) is int and not isinstance(archive["bytes"], bool) and
            0 < archive["bytes"] <= FULL_RELEASE_MAX_ARCHIVE_BYTES and
            archive["files"] == len(source["files"]), "invalid full release source archive")
    archive_path = campaign_file(root, {"path": archive["path"], "sha256": archive["sha256"]},
                                 "full release source archive", FULL_RELEASE_MAX_ARCHIVE_BYTES)
    require(archive_path.stat().st_size == archive["bytes"], "full release source archive size mismatch")
    # The archive is sealed before the compiler starts. Build/test/binary evidence
    # is bound by the outer receipt after the same source snapshot is rehashed.
    manifest = {"head": receipt["head"], "source": source, "build": build, "tools": tools}
    expected_manifest = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    expected_members = {FULL_RELEASE_ARCHIVE_MANIFEST, *source["files"]}
    with zipfile.ZipFile(archive_path) as bundle:
        infos = bundle.infolist()
        names = [info.filename for info in infos]
        require(len(names) == len(set(names)) and set(names) == expected_members and
                sum(info.file_size for info in infos) <= FULL_RELEASE_MAX_UNCOMPRESSED_BYTES,
                "full release source archive membership mismatch")
        for info in infos:
            require(not info.is_dir() and
                    (info.filename == FULL_RELEASE_ARCHIVE_MANIFEST or source_name(info.filename)) and
                    not stat_mod.S_ISLNK(info.external_attr >> 16),
                    "unsafe full release source archive member")
        require(bundle.read(FULL_RELEASE_ARCHIVE_MANIFEST) == expected_manifest,
                "full release source archive manifest mismatch")
        for name, digest in source["files"].items():
            require(hashlib.sha256(bundle.read(name)).hexdigest() == digest,
                    "full release archived source digest mismatch")
    return {"release_build_evidence": True, "head": receipt["head"],
            "binary_sha256": binary["sha256"], "source_archive_sha256": archive["sha256"]}


def verify_provenance_files(root, files, identity, origin):
    require(type(files) is dict and set(files) == PROVENANCE_FILES, "incomplete provenance files")
    paths = {label: campaign_file(root, files[label], label) for label in PROVENANCE_FILES}
    require(files["artifact"]["sha256"] == identity["artifact"], "artifact provenance mismatch")
    require(files["profile"]["sha256"] == identity["profile"], "profile provenance mismatch")
    require(files["source_build"]["sha256"] == origin["source_build_sha256"], "source build provenance mismatch")
    require(files["binary"]["sha256"] == origin["binary_sha256"], "binary provenance mismatch")
    full_release_build_contract(root, paths["source_build"], files["binary"])
    return {label: {"path": files[label]["path"], "sha256": files[label]["sha256"]}
            for label in sorted(PROVENANCE_FILES)}


def _identity_digest(domain, fields):
    digest = hashlib.sha256()
    digest.update(domain)
    for field in fields:
        digest.update(struct.pack("<Q", len(field)))
        digest.update(field)
    return digest.hexdigest()


def loaded_provenance_contract(entry, root, expected_identity, expected_provenance, profile_file):
    """Reconstruct the exact tokenizer/template/profile identities selected by a loaded model.

    The child exports hash preimages read from the retained loaded generation.
    This auditor opens and hashes those immutable campaign copies itself and
    recomputes every domain-separated runtime identity. Missing evidence keeps
    the terminal gate incomplete; malformed evidence is rejected.
    """
    path = campaign_file(root, entry, "loaded compaction provenance", MAX_RAW_NUMERICAL_METADATA_BYTES)
    receipt = read_json(path, entry["sha256"])
    require(type(receipt) is dict and set(receipt) == {
        "schema_version", "stage", "completed", "request", "semantic_identity", "provenance",
        "loaded_identity", "tokenizer", "template", "profile"},
        "invalid loaded compaction provenance receipt")
    require(receipt["schema_version"] == 1 and receipt["stage"] == "loaded_compaction_provenance" and
            receipt["completed"] is True and receipt["semantic_identity"] == expected_identity and
            receipt["provenance"] == expected_provenance,
            "loaded compaction provenance authority mismatch")
    request = receipt["request"]
    require(type(request) is dict and set(request) == {"session_id", "request_index"} and
            isinstance(request["session_id"], str) and re.fullmatch(r"[0-9a-f]{64}", request["session_id"]) and
            type(request["request_index"]) is int and 0 <= request["request_index"] <= 2**32 - 1,
            "invalid loaded provenance request binding")
    loaded = receipt["loaded_identity"]
    loaded_serving_identity({"loaded_identity": loaded}, expected_identity)

    tokenizer = receipt["tokenizer"]
    require(type(tokenizer) is dict and set(tokenizer) == {
        "kind", "source", "parsed_source_sha256", "gguf_metadata", "policy", "loaded_sha256"},
        "invalid loaded tokenizer provenance")
    source = campaign_file(root, tokenizer["source"], "loaded tokenizer source", 64 * 1024 * 1024)
    source_bytes = source.read_bytes()
    require(source_bytes and tokenizer["source"]["sha256"] == hashlib.sha256(source_bytes).hexdigest(),
            "loaded tokenizer source is empty or stale")
    if tokenizer["kind"] == "hf_json":
        require(tokenizer["gguf_metadata"] is None, "HF tokenizer carries GGUF metadata")
        parsed = hashlib.sha256(source_bytes).hexdigest()
    elif tokenizer["kind"] == "gguf_derived":
        metadata = tokenizer["gguf_metadata"]
        keys = ("bos_token_id", "eos_token_id", "padding_token_id", "unknown_token_id")
        require(type(metadata) is dict and set(metadata) == set(keys) | {"add_bos_token", "add_eos_token"},
                "invalid GGUF tokenizer metadata preimage")
        digest = hashlib.sha256()
        digest.update(b"antfly.hf-tokenizer.gguf.v1\0")
        digest.update(struct.pack("<Q", len(source_bytes)))
        digest.update(source_bytes)
        for key in keys:
            value = metadata[key]
            require(value is None or (type(value) is int and -(2**31) <= value < 2**31),
                    "invalid GGUF tokenizer token id")
            digest.update(b"\0" if value is None else b"\1" + struct.pack("<i", value))
        for key in ("add_bos_token", "add_eos_token"):
            value = metadata[key]
            require(value is None or type(value) is bool, "invalid GGUF tokenizer wrap metadata")
            digest.update(b"\0" if value is None else b"\2" if value else b"\1")
        parsed = digest.hexdigest()
    else:
        raise ValueError("unsupported loaded tokenizer provenance kind")
    require(parsed == tokenizer["parsed_source_sha256"], "loaded tokenizer parsed identity mismatch")
    policy = tokenizer["policy"]
    require(type(policy) is dict and set(policy) == {"add_bos_token", "bos_token"} and
            type(policy["add_bos_token"]) is bool and isinstance(policy["bos_token"], str),
            "invalid loaded tokenizer policy preimage")
    digest = hashlib.sha256()
    digest.update(b"antfly.loaded-tokenizer-policy.v1\0")
    digest.update(bytes.fromhex(parsed))
    digest.update(bytes((int(policy["add_bos_token"]),)))
    bos = policy["bos_token"].encode("utf-8")
    digest.update(struct.pack("<Q", len(bos)))
    digest.update(bos)
    require(digest.hexdigest() == tokenizer["loaded_sha256"] == expected_identity["tokenizer"],
            "loaded tokenizer policy identity mismatch")

    template = receipt["template"]
    require(type(template) is dict and set(template) == {
        "source", "bos_token", "eos_token", "unk_token", "pad_token", "loaded_sha256"},
        "invalid loaded template provenance")
    template_path = campaign_file(root, template["source"], "loaded template source", 16 * 1024 * 1024)
    fields = [template_path.read_bytes()]
    for name in ("bos_token", "eos_token", "unk_token", "pad_token"):
        require(isinstance(template[name], str), "invalid loaded template special token")
        fields.append(template[name].encode("utf-8"))
    template_digest = _identity_digest(b"antfly.chat-template.v1\0", fields)
    require(template_digest == template["loaded_sha256"] == expected_identity["template"],
            "loaded template identity mismatch")

    profile = receipt["profile"]
    require(type(profile) is dict and set(profile) == {"artifact_sha256", "semantic_sha256"} and
            profile["artifact_sha256"] == expected_identity["profile"] == profile_file["sha256"],
            "loaded profile artifact identity mismatch")
    frozen = read_json(campaign_file(root, profile_file, "frozen numerical profile"), profile_file["sha256"])
    if frozen.get("schema") == "antfly.context.native.qualification-profile.v1":
        import native_repeat_profile
        candidate = native_repeat_profile.qualification_candidate_view(frozen)
    else:
        candidate = frozen.get("runtime_candidate")
    require(type(candidate) is dict and isinstance(profile["semantic_sha256"], str) and
            re.fullmatch(r"[0-9a-f]{64}", profile["semantic_sha256"]) and
            profile["semantic_sha256"] == candidate.get("profile_semantic_sha256"),
            "loaded profile semantic identity mismatch")
    return {"load_generation": loaded["load_generation"],
            "session_id": request["session_id"], "request_index": request["request_index"],
            "tokenizer_kind": tokenizer["kind"], "profile_semantic_sha256": profile["semantic_sha256"]}


def loaded_provenance_report_contract(report, root, expected_identity, expected_provenance,
                                      profile_file, session_id, request_index):
    require(type(report) is dict and set(report) == {
        "schema_version", "completed", "operation", "session_id", "request_index",
        "loaded_identity", "loaded_provenance"}, "invalid loaded provenance driver reply")
    require(report["schema_version"] == 2 and report["completed"] is True and
            report["operation"] == "describe_provenance" and report["session_id"] == session_id and
            report["request_index"] == request_index, "loaded provenance driver reply mismatch")
    loaded_serving_identity(report, expected_identity)
    audited = loaded_provenance_contract(report["loaded_provenance"], root, expected_identity,
                                         expected_provenance, profile_file)
    require(audited["session_id"] == session_id and audited["request_index"] == request_index and
            audited["load_generation"] == report["loaded_identity"]["load_generation"],
            "loaded provenance receipt/reply mismatch")
    return {"loaded_provenance": report["loaded_provenance"], "audited": audited}


def resource_guard_contract(receipt):
    """Derive resource success from the guard's measurements and limits."""
    require(type(receipt) is dict and receipt.get("schema") == "antfly.benchmark_resources.v1",
            "unsupported Metal numerical resource receipt")
    require(receipt.get("pass") is True and receipt.get("violation") is None and
            "cleanup_error" not in receipt, "Metal numerical resource guard failed")
    require(type(receipt.get("returncode")) is int and not isinstance(receipt["returncode"], bool) and
            receipt["returncode"] == 0, "Metal numerical command did not exit successfully")
    require(type(receipt.get("samples")) is int and not isinstance(receipt["samples"], bool) and
            receipt["samples"] > 0, "Metal numerical resource guard has no valid samples")
    require(type(receipt.get("pid")) is int and not isinstance(receipt["pid"], bool) and
            receipt["pid"] > 0, "invalid Metal numerical resource guard pid")
    command = receipt.get("command")
    require(type(command) is list and command and all(isinstance(value, str) and value for value in command),
            "invalid Metal numerical guarded command")
    limits = receipt.get("limits")
    require(type(limits) is dict and set(limits) == {"timeout_seconds", "max_rss_mib", "min_free_percent",
                                                     "max_swap_growth_mib", "min_disk_free_mib"},
            "invalid Metal numerical resource limits")
    timeout = number(limits["timeout_seconds"], "resource timeout")
    rss_cap = number(limits["max_rss_mib"], "resource RSS cap")
    free_floor = number(limits["min_free_percent"], "resource free-memory floor")
    swap_cap = number(limits["max_swap_growth_mib"], "resource swap cap")
    disk_floor = number(limits["min_disk_free_mib"], "resource disk floor")
    require(timeout > 0 and rss_cap > 0 and 15 <= free_floor <= 100 and swap_cap == 0 and disk_floor >= 4096,
            "insufficient Metal numerical resource limits")
    peak_rss = number(receipt.get("peak_group_rss_mib"), "measured process RSS")
    min_free = number(receipt.get("min_free_percent"), "measured free memory")
    swap_growth = number(receipt.get("swapout_growth_mib"), "measured swap growth")
    min_disk = number(receipt.get("min_disk_free_mib"), "measured free disk")
    initial_free = number(receipt.get("initial_free_percent"), "initial free memory")
    elapsed = number(receipt.get("elapsed_seconds"), "resource elapsed time")
    require(0 <= peak_rss <= rss_cap and min_free >= free_floor and min_free <= initial_free <= 100 and
            swap_growth == 0 and min_disk >= disk_floor and elapsed >= 0,
            "Metal numerical resource measurements exceed limits")
    return {"samples": receipt["samples"], "peak_group_rss_mib": peak_rss,
            "min_free_percent": min_free, "min_disk_free_mib": min_disk}


def number(value, label, minimum=0):
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum, "invalid " + label)
    return value


def binding(value):
    require(type(value) is dict and set(value) == BINDINGS | {"dtype", "kernel_route"}, "incomplete capability identity")
    for key in BINDINGS:
        require(isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{64}", value[key]), "invalid binding: " + key)
    require(value["dtype"] == "f16_kv_f32_bias" and value["kernel_route"] == "metal_context_optimized", "unqualified dtype/kernel route")


def semantic_identity(value):
    require(type(value) is dict and set(value) == SEMANTIC_BINDINGS | {"dtype", "kernel_route"}, "incomplete semantic identity")
    for key in SEMANTIC_BINDINGS:
        require(isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{64}", value[key]), "invalid semantic binding: " + key)
    require(value["dtype"] == "f16_kv_f32_bias" and value["kernel_route"] == "metal_context_optimized", "unqualified dtype/kernel route")


def provenance(value):
    require(type(value) is dict and set(value) == PROVENANCE_BINDINGS, "incomplete evidence provenance")
    for key in PROVENANCE_BINDINGS:
        require(isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{64}", value[key]), "invalid provenance binding: " + key)


def percentile(values, fraction):
    require(values, "empty sample")
    ordered = sorted(values)
    return ordered[min(math.ceil(fraction * len(ordered)) - 1, len(ordered) - 1)]


def bootstrap_bound(rows, statistic, upper, seed_material, rounds=5000):
    """Deterministic percentile bootstrap over paired rows."""
    require(len(rows) >= MIN_PERFORMANCE_PAIRS, "too few paired samples for uncertainty bound")
    seed = int(hashlib.sha256(seed_material.encode()).hexdigest()[:16], 16)
    generator = random.Random(seed)
    estimates = []
    for _ in range(rounds):
        sample = [rows[generator.randrange(len(rows))] for _ in rows]
        estimates.append(statistic(sample))
    return percentile(estimates, .95 if upper else .05)


def quality_upper_bound(cases, seed_material):
    """One-sided paired bootstrap bound for exact minus candidate accuracy."""
    require(len(cases) >= MIN_QUALITY_CASES, "too few quality cases")
    seed = int(hashlib.sha256(seed_material.encode()).hexdigest()[:16], 16)
    generator = random.Random(seed)
    values = [int(row["exact_correct"]) - int(row["candidate_correct"]) for row in cases]
    estimates = []
    for _ in range(5000):
        estimates.append(sum(values[generator.randrange(len(values))] for _ in values) / len(values))
    return percentile(estimates, .95)


def quality_evaluation_source_identities(case):
    """Return raw selected-part identities, never the whole request digest."""
    legacy = case.get("document_sha256")
    ordered = case.get("messages_sha256")
    require((legacy is None) != (ordered is None), "quality case mixes or omits input identity")
    if ordered is None:
        parts = [legacy]
    else:
        require(re.fullmatch(r"[0-9a-f]{64}", case.get("source_sha256", "")),
                "invalid selected-source binding")
        parts = case.get("selected_part_sha256s")
        require(type(parts) is list and parts and len(parts) == len(set(parts)),
                "invalid evaluation source parts")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in parts),
            "invalid evaluation source parts")
    return tuple(parts)


def require_evaluation_calibration_disjoint(evaluation, calibration):
    require(not set(evaluation) & set(calibration), "evaluation corpus overlaps calibration")


def report_contract(report, expected):
    require(report.get("schema_version") == 1 and report.get("identity") == expected, "driver identity mismatch")
    require(report.get("published") is True and report.get("exact_fallback") is False, "driver did not publish actual compaction")
    before = number(report.get("committed_before_bytes"), "old committed bytes", 1)
    after = number(report.get("committed_after_bytes"), "new committed bytes", 1)
    require(after < before, "committed allocation did not decrease")
    require(report.get("backend_kv_readback_bytes") == 0, "backend KV readback detected")
    require(report.get("swapout_growth_bytes") == 0 and report.get("memory_guard_passed") is True, "unclean memory run")
    require(report.get("isolation_passed") is True and report.get("rollback_passed") is True, "isolation/rollback failed")
    require(report.get("live_bytes_after_release") == report.get("live_bytes_before") and
            type(report.get("live_bytes_before")) is int, "request allocation leak")
    require(report.get("route") in ROUTES, "unknown serving route")
    number(report.get("prompt_tokens"), "prompt positions", 1)
    return report


def expected_strata(model):
    owners = {4: [4], 9: [9], 14: [14, 19, 24, 29, 34]} if model == "gemma4-e2b" else {5: [5], 11: [11], 17: [17], 23: [23, 29, 35, 41]}
    group = 8 if model == "gemma4-e2b" else 4
    return {(owner, head // group, consumer, head) for owner, consumers in owners.items() for consumer in consumers for head in range(8)}


def report_contract_v2(report, expected_identity, expected_provenance):
    require(report.get("schema_version") == 2, "unsupported driver report")
    require(report.get("semantic_identity") == expected_identity and report.get("provenance") == expected_provenance,
            "driver binding mismatch")
    require(report.get("published") is True and report.get("exact_fallback") is False, "driver did not publish actual compaction")
    before = number(report.get("committed_before_bytes"), "old committed bytes", 1)
    after = number(report.get("committed_after_bytes"), "new committed bytes", 1)
    for field in ("committed_before_bytes", "committed_after_bytes", "live_bytes_before",
                  "live_bytes_after_release", "backend_kv_readback_bytes", "swapout_growth_bytes", "prompt_tokens"):
        require(type(report.get(field)) is int and report[field] >= 0, "invalid serving counter: " + field)
    require((before - after) / before >= MIN_COMMITTED_SAVINGS, "committed allocation savings below 25 percent")
    require(report.get("backend_kv_readback_bytes") == 0, "backend KV readback detected")
    require(report.get("swapout_growth_bytes") == 0, "swapout growth detected")
    require(report.get("live_bytes_after_release") == report.get("live_bytes_before") and
            type(report.get("live_bytes_before")) is int, "request allocation leak")
    require(report.get("route") in ROUTES, "unknown serving route")
    number(report.get("prompt_tokens"), "prompt positions", 1)
    request_context_tokens(report)
    context_allocation_scope_contract(report)
    return report


def context_allocation_scope_contract(report, label="serving"):
    """Validate request-exclusive context bytes and separate shared residency.

    This is deliberately narrower than RSS, model weights, or total GPU
    residency. The model execution pool is disclosed once as shared residency
    and must not be added to each request's committed/peak context bytes.
    """
    require(type(report) is dict and
            report.get("context_allocation_scope") == CONTEXT_ALLOCATION_SCOPE and
            report.get("context_allocation_scope_version") == CONTEXT_ALLOCATION_SCOPE_VERSION and
            report.get("context_allocation_scope_complete") is True and
            report.get("full_owner_physical_available") is True,
            label + " lacks complete versioned context allocation scope")
    require(report.get("model_execution_pool_admission_version") ==
            CONTEXT_EXECUTION_POOL_ADMISSION_VERSION and
            report.get("model_execution_pool_admitted_backend_bytes") ==
            CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES and
            report.get("model_execution_pool_shared_residency") is True,
            label + " has invalid model execution pool disclosure")
    committed = number(report.get("committed_context_bytes"), label + " committed context bytes", 1)
    peak = number(report.get("context_peak_bytes"), label + " context peak bytes", committed)
    require(report.get("owner_released") is True and
            report.get("owner_post_release_live_bytes") == 0 and
            report.get("live_bytes_after_release") == report.get("live_bytes_before") and
            type(report.get("live_bytes_before")) is int and report["live_bytes_before"] >= 0,
            label + " context owner did not return to its baseline")
    return {"committed_context_bytes": committed, "context_peak_bytes": peak,
            "model_execution_pool_admitted_backend_bytes":
                CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES}


def _mixed_batch_allocations_contract(report, sibling_variants):
    require(type(sibling_variants) is list and len(sibling_variants) >= 2 and
            sibling_variants.count("attention_matching") == 1 and
            all(value in {"exact", "attention_matching"} for value in sibling_variants),
            "invalid mixed batch sibling contract")
    allocations = report.get("allocations")
    require(type(allocations) is list and len(allocations) == 2,
            "mixed batch requires one exact group and one attention matching singleton")
    exact_members = [index for index, value in enumerate(sibling_variants) if value == "exact"]
    seen, pool_disclosures = set(), set()
    for allocation in allocations:
        require(type(allocation) is dict and set(allocation) == {"member_request_indices", "receipt"},
                "invalid mixed batch allocation record")
        members, receipt = allocation["member_request_indices"], allocation["receipt"]
        require(type(members) is list and members and
                all(type(index) is int and 0 <= index < len(sibling_variants) for index in members) and
                len(members) == len(set(members)), "invalid mixed batch members")
        require(type(receipt) is dict and receipt.get("released") is True and
                receipt.get("context_allocation_scope_version") == CONTEXT_ALLOCATION_SCOPE_VERSION and
                receipt.get("context_batch_scope_version") == 1 and
                receipt.get("context_allocation_scope_complete") is True and
                receipt.get("full_owner_physical_available") is True and
                receipt.get("context_batch_expected_items") == len(members) and
                receipt.get("context_batch_completed_items") == len(members),
                "incomplete mixed batch allocation receipt")
        pool = (receipt.get("model_execution_pool_admission_version"),
                receipt.get("model_execution_pool_admitted_backend_bytes"))
        require(pool == (CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                         CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES),
                "invalid mixed batch execution pool disclosure")
        pool_disclosures.add(pool)
        kind = receipt.get("context_batch_kind")
        if kind == "exact_siblings":
            require(members == exact_members and kind not in seen, "mixed exact group membership mismatch")
        elif kind == "attention_matching_singleton":
            require(len(members) == 1 and sibling_variants[members[0]] == "attention_matching" and
                    kind not in seen, "mixed attention matching membership mismatch")
        else:
            raise ValueError("unknown mixed batch allocation kind")
        seen.add(kind)
    require(seen == {"exact_siblings", "attention_matching_singleton"} and len(pool_disclosures) == 1 and
            report.get("model_execution_pool") == {
                "admission_version": CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                "admitted_backend_bytes": CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                "shared_once": True},
            "mixed batch duplicates or omits shared execution pool residency")
    return allocations


def mixed_batch_scope_contract(report, sibling_variants):
    """Audit scope-only mixed execution without granting serving qualification."""
    require(type(report) is dict and report.get("schema_version") == 2 and
            report.get("completed") is True and report.get("operation") == "mixed_batch_scope" and
            report.get("scope_only") is True and report.get("serving_qualified") is False,
            "invalid mixed batch scope report")
    return _mixed_batch_allocations_contract(report, sibling_variants)


def mixed_batch_serving_contract(report, siblings):
    """Bind every successful mixed item to one genuine group allocation.

    Shared exact-group bytes remain one group receipt. They are never copied
    into per-item context claims, while the AM singleton is joined to its own
    receipt by the observed request index.
    """
    exact_top = {"schema_version", "completed", "operation", "scope_only", "serving_qualified",
                 "session_id", "request_index", "items", "model_execution_pool", "allocations"}
    require(type(report) is dict and set(report) == exact_top and report["schema_version"] == 2 and
            report["completed"] is True and report["operation"] == "mixed_batch" and
            report["scope_only"] is False and report["serving_qualified"] is True and
            isinstance(report["session_id"], str) and re.fullmatch(r"[0-9a-f]{64}", report["session_id"]) and
            report["session_id"] != "0" * 64 and type(report["request_index"]) is int and
            report["request_index"] >= 0 and type(siblings) is list,
            "invalid mixed batch serving report")
    variants = [row.get("variant") if type(row) is dict else None for row in siblings]
    allocations = _mixed_batch_allocations_contract(report, variants)
    for allocation in allocations:
        receipt = allocation["receipt"]
        measurement = receipt.get("measurement")
        require(type(measurement) is dict and measurement.get("available") is True and
                type(measurement.get("committed_live_bytes")) is int and
                measurement["committed_live_bytes"] > 0 and
                type(measurement.get("request_peak_bytes")) is int and
                measurement["request_peak_bytes"] >= measurement["committed_live_bytes"] and
                measurement.get("released_baseline_bytes") == 0 and
                receipt.get("post_release_live_bytes") == 0,
                "mixed batch group lacks complete allocation measurement")
    items = report["items"]
    exact_item = {"request_index", "variant", "document_file", "document_sha256", "question_file",
                  "question_sha256", "request_content_sha256", "prompt_sha256", "prompt_tokens",
                  "reserved_output_tokens", "settings_sha256", "variant_settings_sha256",
                  "loaded_identity", "elapsed_ns", "ttft_ns", "decode_ns", "backend_kv_readback_bytes",
                  "published", "exact_fallback", "output_token_ids", "output_sha256",
                  "allocation_group_index"}
    require(type(items) is list and len(items) == len(siblings), "incomplete mixed batch item observations")
    loaded_identity = None
    for index, (item, expected) in enumerate(zip(items, siblings)):
        require(type(item) is dict and set(item) == exact_item and item["request_index"] == index and
                item["variant"] == expected.get("variant") and
                all(item.get(field) == expected.get(field) for field in
                    ("document_file", "document_sha256", "question_file", "question_sha256",
                     "request_content_sha256", "prompt_sha256", "prompt_tokens",
                     "reserved_output_tokens", "settings_sha256", "variant_settings_sha256")),
                "mixed batch item request binding mismatch")
        loaded = item["loaded_identity"]
        loaded_fields = {key + "_sha256" for key in SEMANTIC_BINDINGS} | {"load_generation"}
        require(type(loaded) is dict and set(loaded) == loaded_fields and
                type(loaded["load_generation"]) is int and 0 < loaded["load_generation"] <= 2**64 - 1 and
                all(isinstance(loaded[key + "_sha256"], str) and
                    re.fullmatch(r"[0-9a-f]{64}", loaded[key + "_sha256"])
                    for key in SEMANTIC_BINDINGS),
                "invalid mixed batch loaded identity")
        if loaded_identity is None:
            loaded_identity = item["loaded_identity"]
        require(item["loaded_identity"] == loaded_identity, "mixed batch loaded identity changed")
        number(item["elapsed_ns"], "mixed item elapsed ns", 1)
        number(item["ttft_ns"], "mixed item TTFT ns", 1)
        number(item["decode_ns"], "mixed item decode ns", 1)
        require(type(item["backend_kv_readback_bytes"]) is int and item["backend_kv_readback_bytes"] == 0 and
                item["ttft_ns"] <= item["elapsed_ns"] and item["decode_ns"] <= item["elapsed_ns"] and
                item["exact_fallback"] is False and
                item["published"] is (item["variant"] == "attention_matching") and
                type(item["output_token_ids"]) is list and
                len(item["output_token_ids"]) == item["reserved_output_tokens"] and
                all(type(token) is int and 0 <= token <= 2**32 - 1 for token in item["output_token_ids"]) and
                hashlib.sha256(b"".join(struct.pack("<I", token) for token in
                                        item["output_token_ids"])).hexdigest() == item["output_sha256"],
                "invalid mixed batch item completion")
        group_index = item["allocation_group_index"]
        require(type(group_index) is int and 0 <= group_index < len(allocations) and
                index in allocations[group_index]["member_request_indices"] and
                allocations[group_index]["receipt"]["context_batch_kind"] ==
                    ("attention_matching_singleton" if item["variant"] == "attention_matching" else "exact_siblings"),
                "mixed batch item allocation membership mismatch")
    return {"items": items, "allocations": allocations, "loaded_identity": loaded_identity}


def request_context_tokens(report):
    """A context claim includes the entire reserved continuation, not just its prompt."""
    prompt, reserved = report.get("prompt_tokens"), report.get("reserved_output_tokens")
    require(type(prompt) is int and prompt > 0 and type(reserved) is int and reserved > 0 and
            prompt + reserved <= 2**32 - 1, "invalid prompt plus reserved output positions")
    return prompt + reserved


def loaded_serving_identity(report, identity):
    """Bind a request to bytes retained by its loaded model, including its profile."""
    loaded = report.get("loaded_identity")
    fields = {key + "_sha256": key for key in SEMANTIC_BINDINGS}
    require(type(loaded) is dict and set(loaded) == set(fields) | {"load_generation"},
            "incomplete loaded serving identity")
    require(type(loaded["load_generation"]) is int and 0 < loaded["load_generation"] <= 2**64 - 1,
            "invalid loaded serving generation")
    require(all(loaded[field] == identity[key] for field, key in fields.items()),
            "loaded serving identity mismatch")
    return loaded["load_generation"]


def failure_report_contract(report, identity, origin, expected_request=None):
    """Validate rollback against raw token IDs and original context fingerprints.

    These observations must come from the bound test driver at the actual fault
    site. A claimed `rollback_passed` or a precomputed output digest is not proof.
    """
    require(type(report) is dict and report.get("schema_version") == 2 and
            report.get("semantic_identity") == identity and report.get("provenance") == origin,
            "failure driver binding mismatch")
    fault = report.get("fault")
    require(fault in FAILURES and report.get("route") == "native", "invalid failure driver route or fault")
    if expected_request is not None:
        bindings = ("route", "variant", "fault", "document_file", "document_sha256",
                    "question_file", "question_sha256", "request_content_sha256", "prompt_sha256",
                    "prompt_tokens", "reserved_output_tokens", "settings_sha256", "variant_settings_sha256")
        require(type(expected_request) is dict and
                all(report.get(field) == expected_request.get(field) for field in bindings),
                "failure report/request binding mismatch")
    injection = report.get("injection")
    require(type(injection) is dict and
            set(injection) == {"kind", "stage", "mechanism", "completed_units", "total_units"} and
            injection["kind"] == fault and injection["stage"] == FAILURE_STAGES[fault] and
            injection["mechanism"] == FAILURE_MECHANISMS[fault] and
            type(injection["completed_units"]) is int and
            type(injection["total_units"]) is int and
            0 < injection["completed_units"] <= injection["total_units"] <= 2**32 - 1 and
            ((fault == "cancel_publish" and injection["completed_units"] == injection["total_units"]) or
             (fault in {"cancel_prefill", "cancel_replay", "cancel_fit"} and
              injection["completed_units"] < injection["total_units"]) or
             fault in {"oom", "nonfinite", "gpu_failure", "reload"}),
            "failure report lacks a genuine injection boundary")
    request_context_tokens(report)
    generation = loaded_serving_identity(report, identity)
    before, after = report.get("loaded_generation_before"), report.get("loaded_generation_after")
    request_generation = after if fault == "reload" else before
    require(type(before) is int and type(after) is int and 0 < before <= 2**64 - 1 and
            0 < after <= 2**64 - 1 and generation == request_generation and
            type(report.get("request_generation")) is int and report["request_generation"] == request_generation,
            "failure request lost loaded generation")
    require((after != before) if fault == "reload" else (after == before),
            "failure reload generation transition mismatch")
    live = report.get("live_bytes_before")
    require(type(live) is int and live >= 0 and type(report.get("live_bytes_after_release")) is int and
            report["live_bytes_after_release"] == live, "failure leaked request allocations")
    def valid_snapshot(snapshot):
        return (type(snapshot) is dict and
            set(snapshot) == {"epoch", "logical_tokens", "content_sha256", "descriptors_sha256",
                              "kv_sha256", "diagnostic_kv_readback_bytes"} and
            type(snapshot["epoch"]) is int and 0 < snapshot["epoch"] <= 2**64 - 1 and
            type(snapshot["logical_tokens"]) is int and 0 < snapshot["logical_tokens"] <= 2**32 - 1 and
            type(snapshot["diagnostic_kv_readback_bytes"]) is int and
            snapshot["diagnostic_kv_readback_bytes"] > 0 and
            all(isinstance(snapshot[key], str) and re.fullmatch(r"[0-9a-f]{64}", snapshot[key]) and
                snapshot[key] != "0" * 64
                for key in ("content_sha256", "descriptors_sha256", "kv_sha256")))
    snapshots, partial = report.get("original_context"), report.get("partial_context")
    if fault == "cancel_prefill":
        require(report.get("context_disposition") == "partial_prefill_destroyed" and snapshots is None and
                valid_snapshot(partial) and partial["logical_tokens"] == injection["completed_units"],
                "failure partial prefill was not destroyed")
    elif fault == "oom":
        require(report.get("context_disposition") == "preparation_allocation_rolled_back" and
                snapshots is None and partial is None,
                "failure preparation allocation did not roll back")
    elif fault == "reload":
        require(report.get("context_disposition") == "model_generation_reloaded" and
                snapshots is None and partial is None and report.get("stale_selection_rejected") is True,
                "failure model generation did not reload cleanly")
    else:
        require(report.get("context_disposition") == "original_context_unchanged" and partial is None and
                type(snapshots) is dict and set(snapshots) == {"before", "after"} and
                valid_snapshot(snapshots["before"]) and snapshots["before"] == snapshots["after"],
                "failure original context changed")
    require(report.get("maintenance_kv_readback_bytes") == 0,
            "failure recovery performed backend KV maintenance readback")
    outputs = []
    for name in ("exact", "observer"):
        tokens = report.get(name + "_output_tokens")
        require(type(tokens) is list and 0 < len(tokens) <= 4096 and
                all(type(token) is int and 0 <= token <= 2**31 - 1 for token in tokens),
                "invalid failure raw output tokens")
        digest = hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()
        require(report.get(name + "_output_sha256") == digest, "failure output digest does not match raw tokens")
        outputs.append(tokens)
    require(outputs[0] == outputs[1], "failure isolation output mismatch")
    if fault == "reload":
        tokens = report.get("pre_reload_exact_output_tokens")
        require(type(tokens) is list and tokens == outputs[0] and
                all(type(token) is int and 0 <= token <= 2**31 - 1 for token in tokens),
                "reload pre-transition exact output mismatch")
        digest = hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()
        require(report.get("pre_reload_exact_output_sha256") == digest,
                "reload pre-transition exact digest mismatch")
        require(report.get("pre_reload_exact_generation") == before and
                report.get("attention_matching_generation") == after and
                report.get("exact_after_generation") == after and
                report.get("attention_matching_output_tokens") == outputs[1] and
                report.get("attention_matching_output_sha256") == report.get("observer_output_sha256") and
                report.get("exact_after_output_tokens") == outputs[0] and
                report.get("exact_after_output_sha256") == report.get("exact_output_sha256"),
                "reload route observations are not separately bound")
    events = report.get("events")
    require(type(events) is list and 5 <= len(events) <= 256, "invalid raw failure events")
    previous, names = -1, []
    for event in events:
        require(type(event) is dict and type(event.get("monotonic_ns")) is int and
                0 <= event["monotonic_ns"] and previous <= event["monotonic_ns"] and
                isinstance(event.get("name"), str) and
                type(event.get("live_bytes")) is int and event["live_bytes"] >= 0,
                "invalid raw failure event")
        previous = event["monotonic_ns"]
        names.append(event["name"])
    verified_event = ("partial_context_destroyed" if fault == "cancel_prefill" else
                      "allocation_rollback_verified" if fault == "oom" else
                      "model_generation_reloaded" if fault == "reload" else
                      "original_context_verified")
    required = (("request_started", "fault_injected", verified_event, "stale_selection_rejected",
                 "fresh_attention_matching_published", "observer_verified", "request_released")
                if fault == "reload" else
                ("request_started", "fault_injected", verified_event, "observer_verified", "request_released"))
    require(names[0] == required[0] and names[-1] == required[-1] and
            all(names.count(name) == 1 for name in required) and
            [names.index(name) for name in required] == sorted(names.index(name) for name in required),
            "incomplete raw failure event sequence")
    injected = names.index("fault_injected")
    require(events[injected].get("fault") == fault, "fault request did not reach matching injection site")
    if fault == "reload":
        require("publication_commit" not in names and names.count("fresh_attention_matching_published") == 1,
                "reload did not publish exactly one fresh-generation attention-matching context")
    else:
        require("publication_commit" not in names[injected + 1:], "failure published after injection")
    require(events[0]["live_bytes"] == live and events[-1]["live_bytes"] == live,
            "failure event allocation baseline changed")
    return report


def raw_soak_contract(raw, root, expected_identity, expected_provenance, lengths, minimum_duration=14400):
    """Rebuild trials from parent-clock events, raw replies and resource samples.

    A synthetic or short collector run exercises this contract but never grants
    serving authority. The release audit always uses the four-hour default.
    """
    root = Path(root).resolve()
    require(type(raw) is dict and set(raw) == {"session"}, "invalid raw soak evidence")
    session_path = campaign_file(root, raw["session"], "soak session", MAX_REPORT)
    session = read_json(session_path)
    routes = session.get("route_contract_sha256") is not None
    return _raw_driver_contract(raw, root, expected_identity, expected_provenance, lengths,
                                minimum_duration, False, routes=routes)


def raw_route_soak_contract(raw, root, expected_identity, expected_provenance, lengths,
                            minimum_duration=14400):
    """Audit a sealed five-route campaign, including genuine CLI and mixed batch."""
    return _raw_driver_contract(raw, root, expected_identity, expected_provenance, lengths,
                                minimum_duration, False, routes=True)


def raw_failure_contract(raw, root, identity, origin, lengths):
    return _raw_driver_contract(raw, root, identity, origin, lengths, 0, True)


def raw_performance_contract(raw, root, identity, origin, lengths):
    import paired_context_qualify as paired
    evidence = _raw_driver_contract(raw, root, identity, origin, lengths, 0, False, True)
    return paired.reconstruct(evidence["request_plan"], [trial["report"] for trial in evidence["trials"]],
                              identity, origin, evidence["plan_root"])


def raw_quality_contract(raw, root, identity, origin, lengths):
    import quality_context_qualify as quality
    evidence = _raw_driver_contract(raw, root, identity, origin, lengths, 0, False, quality=True)
    result = quality.reconstruct(evidence["request_plan"], [trial["report"] for trial in evidence["trials"]],
                                 identity, origin, evidence["plan_root"])
    for case in result["cases"]:
        # Derived only after every parent resource sample was verified above.
        case["swapout_growth_bytes"] = 0
    return result


def _raw_driver_contract(raw, root, expected_identity, expected_provenance, lengths, minimum_duration,
                         failures, measurements=False, quality=False, routes=False):
    root = Path(root).resolve()
    require(type(raw) is dict and set(raw) == {"session"}, "invalid raw soak evidence")
    session_path = campaign_file(root, raw["session"], "soak session", MAX_REPORT)
    session = read_json(session_path)
    directory = session_path.parent
    planned = measurements or quality or failures or routes
    stage = "raw_paired_quality" if quality else "raw_paired_performance" if measurements else "raw_failure_injection" if failures else "raw_serving_soak"
    require(session.get("schema_version") == 2 and session.get("stage") == stage and
            session.get("semantic_identity") == expected_identity and session.get("provenance") == expected_provenance,
            "raw soak binding mismatch")
    require(session.get("completed") is True and type(session.get("returncode")) is int and
            session["returncode"] == 0, "raw soak did not terminate successfully")
    require(session.get("collector_sha256") == sha(__file__), "raw soak collector source mismatch")
    require(session.get("driver_binary_sha256") == expected_provenance["binary_sha256"],
            "raw soak did not execute the bound binary")
    command = session.get("driver_command")
    require(type(command) is list and command and all(isinstance(arg, str) and arg for arg in command),
            "invalid raw soak command")
    require(type(session.get("driver_pid")) is int and session["driver_pid"] > 0,
            "invalid raw soak process")
    nonce = session.get("session_id")
    require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{64}", nonce), "invalid raw soak session")
    duration_ns = session.get("elapsed_ns")
    require(type(duration_ns) is int and duration_ns >= minimum_duration * 1e9,
            "raw soak duration below qualification minimum")
    limits = session.get("limits")
    limit_fields = {"max_rss_mib", "min_free_percent", "min_disk_free_mib",
                    "max_swap_growth_bytes", "trial_timeout_seconds"}
    require(type(limits) is dict and
            set(limits) == limit_fields | {"child_file_limit_policy"},
            "invalid raw soak resource limits")
    require(limits["child_file_limit_policy"] == CHILD_FILE_LIMIT_POLICY,
            "invalid child file limit policy")
    rss_cap = number(limits["max_rss_mib"], "soak RSS limit", 1)
    require(number(limits["min_free_percent"], "soak memory floor") >= 15 and
            number(limits["min_disk_free_mib"], "soak disk floor") >= 4096 and
            limits["max_swap_growth_bytes"] == 0 and
            number(limits["trial_timeout_seconds"], "soak trial timeout", .001) > 0,
            "insufficient raw soak resource limits")
    files = session.get("files")
    expected_files = ({"journal", "stdout", "stderr", "plan", "cli_source_build_receipt"}
                      if routes else {"journal", "stdout", "stderr", "plan"}
                      if planned else {"journal", "stdout", "stderr"})
    require(type(files) is dict and set(files) == expected_files, "incomplete raw soak files")
    paths = {key: campaign_file(directory, entry, "soak " + key, MAX_JOURNAL_BYTES) for key, entry in files.items()}
    plan, requests = None, None
    if planned:
        if routes:
            import context_serving_routes_qualify as contract
            plan = contract.plan_contract(read_json(paths["plan"]), directory)
            require(plan["runtime"]["implementation_sha256"] == expected_identity["implementation"] and
                    plan["runtime"]["profile_artifact_sha256"] == expected_identity["profile"],
                    "raw serving route runtime identity mismatch")
            requests = contract.requests(plan)
            require(session.get("route_contract_sha256") == sha(contract.__file__) and
                    session.get("cli_binary_sha256") == plan["cli_binary_sha256"],
                    "raw serving route plan contract mismatch")
            require(plan["cli_source_build_sha256"] == files["cli_source_build_receipt"]["sha256"],
                    "raw CLI source receipt reference mismatch")
            cli_build_evidence_contract(paths["cli_source_build_receipt"],
                                        plan["cli_source_build_sha256"], plan["cli_binary_sha256"])
        elif failures:
            plan = read_json(paths["plan"])
            requests = failure_plan_contract(plan, directory, expected_identity)
            for field in ("document", "question"):
                campaign_file(directory, {"path": plan["request"][field + "_file"],
                    "sha256": plan["request"][field + "_sha256"]}, "failure " + field, MAX_REPORT)
            require(session.get("measurement_contract_sha256") == sha(__file__),
                    "raw failure plan contract source mismatch")
        elif quality:
            import quality_context_qualify as contract
            plan = contract.plan_contract(read_json(paths["plan"]), directory)
            requests = contract.requests(plan)
        else:
            import paired_context_qualify as contract
            plan = contract.plan_contract(read_json(paths["plan"]), directory, expected_identity)
            for request in plan["requests"]:
                for field in ("document", "question", "prompt"):
                    campaign_file(directory, {"path": request[field + "_file"],
                        "sha256": request[field + "_sha256"]}, "performance " + field, MAX_REPORT)
            requests = contract.requests(plan)
        if not failures:
            require(session.get("measurement_contract_sha256") == sha(contract.__file__),
                    "raw planned contract source mismatch")
    replies = iter(journal_records(paths["stdout"]))
    trials, pending, resources = [], None, None
    previous_ns = 0
    previous_sample_ns = None
    baseline_swap, baseline_live, generation = None, None, None
    started, stopped, shutdown = False, False, False
    for event in journal_records(paths["journal"]):
        require(type(event) is dict and type(event.get("elapsed_ns")) is int and
                previous_ns <= event["elapsed_ns"] <= duration_ns, "invalid raw soak clock sequence")
        now = event["elapsed_ns"]
        previous_ns = now
        kind = event.get("event")
        require(not shutdown, "raw soak event after shutdown")
        if kind == "start":
            require(not started and now == 0 and event.get("session_id") == nonce,
                    "invalid raw soak start")
            started = True
        else:
            require(started, "raw soak event before start")
            if kind == "resource":
                require(not stopped, "raw soak resource after stop")
                free = number(event.get("free_percent"), "raw free memory")
                disk = number(event.get("disk_free_mib"), "raw free disk")
                rss = number(event.get("group_rss_mib"), "raw process RSS")
                swap = event.get("swapout_bytes")
                require(type(swap) is int and swap >= 0, "invalid raw swap counter")
                if baseline_swap is None:
                    baseline_swap = swap
                require(swap == baseline_swap and limits["min_free_percent"] <= free <= 100 and
                        disk >= limits["min_disk_free_mib"] and rss <= rss_cap, "raw soak resource guard failed")
                require(now - (previous_sample_ns or 0) <= 2_000_000_000, "raw soak resource sampling gap")
                previous_sample_ns = now
                if pending is not None:
                    resources["samples"] += 1
                    resources["min_free_percent"] = min(resources["min_free_percent"], free)
                    resources["min_disk_free_mib"] = min(resources["min_disk_free_mib"], disk)
                    resources["peak_group_rss_mib"] = max(resources["peak_group_rss_mib"], rss)
            elif kind == "command":
                # Reject surplus planned commands before indexing the sealed
                # request list; malformed journals must fail deliberately.
                if planned and not routes:
                    require(len(trials) < len(requests), "raw planned campaign has an extra command")
                require(not stopped and pending is None and len(trials) < 4096 and
                        type(event.get("request_index")) is int and event["request_index"] == len(trials) and event.get("session_id") == nonce and
                        event.get("route") == (requests[len(trials) % len(requests)]["route"] if routes else
                                               requests[len(trials)]["route"] if planned and len(trials) < len(requests)
                                               else "native" if failures else sorted(ROUTES)[len(trials) % len(ROUTES)]),
                        "invalid raw soak command sequence")
                if planned:
                    expected_request = requests[len(trials) % len(requests)] if routes else requests[len(trials)]
                    require((routes or len(trials) < len(requests)) and
                            all(event.get(key) == value for key, value in expected_request.items()),
                            "invalid raw failure command sequence" if failures else
                            "raw performance command/plan mismatch")
                if failures:
                    require(len(trials) < len(FAILURE_ORDER) and event.get("fault") == FAILURE_ORDER[len(trials)],
                            "invalid raw failure command sequence")
                pending = event
                resources = {"samples": 0, "min_free_percent": 100, "min_disk_free_mib": math.inf,
                             "peak_group_rss_mib": 0, "swapout_growth_bytes": 0}
            elif kind == "report":
                require(pending is not None and type(event.get("request_index")) is int and event["request_index"] == len(trials),
                        "unsolicited raw soak report")
                path = campaign_file(directory, event.get("file"), "raw soak report", MAX_DRIVER_LINE_BYTES)
                report = read_json(path)
                if routes:
                    audited = contract.report_contract(
                        report, requests[len(trials) % len(requests)], expected_identity, expected_provenance,
                        plan["runtime"], plan["cli_binary_sha256"], directory,
                        sys.modules[__name__])
                elif quality:
                    contract.driver_report_contract(report, expected_identity, expected_provenance, requests[len(trials)], directory)
                elif measurements:
                    contract.report_contract(report, expected_identity, expected_provenance, requests[len(trials)])
                elif failures:
                    failure_report_contract(report, expected_identity, expected_provenance, pending)
                else:
                    report_contract_v2(report, expected_identity, expected_provenance)
                if measurements or quality or routes:
                    child_elapsed = audited["elapsed_ns"] if routes else report["elapsed_ns"]
                    require(child_elapsed <= now - pending["elapsed_ns"],
                            "raw child timing exceeds parent duration")
                require(next(replies, None) == report, "raw soak stdout/report mismatch")
                require(report.get("session_id") == nonce and type(report.get("request_index")) is int and report["request_index"] == len(trials) and
                        report.get("route") == pending["route"], "raw soak reply/command mismatch")
                if failures:
                    require(report["fault"] == pending["fault"], "raw failure reply/command mismatch")
                current_generation = (audited["load_generation"] if routes else
                                      loaded_serving_identity(report, expected_identity))
                if generation is None:
                    generation = current_generation
                    baseline_live = (audited["baseline_live_bytes"] if routes else
                                     report["live_bytes_before"])
                if failures:
                    if report["fault"] == "reload":
                        require(report["loaded_generation_before"] == generation and
                                current_generation == report["loaded_generation_after"],
                                "raw reload did not transition the persistent loaded generation")
                    else:
                        require(current_generation == generation,
                                "raw fault request changed the persistent loaded generation")
                    require(report["live_bytes_before"] == baseline_live,
                            "raw fault request changed the allocation baseline")
                    generation = report["loaded_generation_after"]
                else:
                    require(current_generation == generation, "raw soak model generation changed")
                    if routes:
                        route_baseline = audited["baseline_live_bytes"]
                        if route_baseline is not None:
                            if baseline_live is None:
                                baseline_live = route_baseline
                            require(route_baseline == baseline_live,
                                    "raw soak allocation baseline changed")
                    else:
                        require(report["live_bytes_before"] == baseline_live,
                                "raw soak allocation baseline changed")
                if routes:
                    require(all(value in lengths for value in audited["context_tokens"]),
                            "raw serving route request outside qualified context")
                else:
                    require(request_context_tokens(report) in lengths,
                            "raw soak request outside qualified context")
                require(resources["samples"] > 0 and previous_sample_ns is not None and
                        now - previous_sample_ns <= 2_000_000_000 and
                        now - pending["elapsed_ns"] <= limits["trial_timeout_seconds"] * 1e9,
                        "raw soak request lacks timely resource samples")
                if failures:
                    events = report["events"]
                    require(events[-1]["monotonic_ns"] - events[0]["monotonic_ns"] <= now - pending["elapsed_ns"],
                            "failure event duration exceeds parent-clock request")
                trials.append({"start_seconds": pending["elapsed_ns"] / 1e9, "end_seconds": now / 1e9,
                               "report": report, "resources": resources})
                pending = None
            elif kind == "stop":
                require(pending is None and not stopped and trials, "invalid raw soak stop")
                stopped = True
            elif kind == "shutdown":
                require(stopped and type(event.get("returncode")) is int and event["returncode"] == 0 and
                        type(event.get("remaining_group_rss_mib")) in (int, float) and
                        event["remaining_group_rss_mib"] == 0,
                        "raw soak shutdown failed or left live descendants")
                shutdown = True
            else:
                raise ValueError("unknown raw soak event")
    require(started and stopped and shutdown and pending is None and next(replies, None) is None,
            "incomplete raw soak journal")
    duration = duration_ns / 1e9
    require(trials[0]["start_seconds"] <= 60 and duration - trials[-1]["end_seconds"] <= 60 and
            sum(trial["end_seconds"] - trial["start_seconds"] for trial in trials) >= .9 * duration,
            "raw soak is mostly idle time")
    if routes:
        require(len(trials) >= len(requests) and
                {trial["report"]["route"] for trial in trials} == ROUTES,
                "incomplete raw serving route campaign")
    elif planned:
        require(len(trials) == len(requests), "incomplete raw planned campaign")
    elif failures:
        require(len(trials) == len(FAILURES) and {trial["report"]["fault"] for trial in trials} == FAILURES,
                "raw failure campaign misses injection sites")
    else:
        require({trial["report"]["route"] for trial in trials} == ROUTES, "raw soak misses serving routes")
    require(type(session.get("trial_count")) is int and session["trial_count"] == len(trials), "raw soak trial count mismatch")
    result = {"duration_seconds": duration, "trials": trials, "load_generation": generation, "max_rss_mib": rss_cap,
              "resource_samples_verified": True, "loaded_identity_verified": True}
    if routes:
        result["route_contract_verified"] = True
    if planned:
        result["request_plan"] = plan
        result["plan_root"] = directory
    return result


def performance_contract(perf, expected_identity, expected_provenance, qualified_context_tokens):
    require(perf.get("schema_version") == 2 and perf.get("semantic_identity") == expected_identity and
            perf.get("provenance") == expected_provenance, "stale or mismatched performance")
    cells = perf.get("cells")
    require(type(cells) is list and cells, "missing performance cells")
    cell_contracts = {}
    for cell in cells:
        require(type(cell) is dict and set(cell) == {"context_tokens", "reserved_output_tokens",
                                                     "generation_settings_sha256", "variant_settings_sha256"},
                "invalid performance cell")
        context = cell.get("context_tokens")
        reserved = cell.get("reserved_output_tokens")
        require(type(context) is int and context in qualified_context_tokens and
                type(reserved) is int and 0 < reserved < context, "performance cell outside release claim")
        settings = cell.get("variant_settings_sha256")
        require(type(settings) is dict and set(settings) == set(PERFORMANCE_VARIANTS) and
                all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in settings.values()),
                "invalid canonical variant settings")
        require(isinstance(cell["generation_settings_sha256"], str) and
                re.fullmatch(r"[0-9a-f]{64}", cell["generation_settings_sha256"]),
                "invalid canonical generation settings")
        key = (context, reserved)
        require(key not in cell_contracts, "duplicate performance cell")
        cell_contracts[key] = {"generation": cell["generation_settings_sha256"], "variants": settings}
    require({key[0] for key in cell_contracts} == set(qualified_context_tokens),
            "performance cells do not cover qualified context lengths")
    warmups = perf.get("warmups", [])
    rows = perf.get("samples", [])
    require(type(warmups) is list and type(rows) is list, "invalid performance observations")
    all_rows = warmups + rows
    require(all(type(row) is dict for row in all_rows), "invalid performance observation")
    require(len({row.get("pair_id") for row in all_rows}) == len(all_rows) and
            all(isinstance(row.get("pair_id"), str) and row["pair_id"] for row in all_rows),
            "duplicate or invalid performance pair")
    require(len({row.get("prompt_sha256") for row in rows}) == len(rows), "measured performance prompts are not independent")
    require(not ({row.get("prompt_sha256") for row in rows} & {row.get("prompt_sha256") for row in warmups}),
            "performance warmups overlap measured prompts")
    rows_by_cell = {key: [] for key in cell_contracts}
    warmups_by_cell = {key: [] for key in cell_contracts}
    for row in all_rows:
        require(isinstance(row.get("prompt_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", row["prompt_sha256"]), "invalid performance prompt identity")
        require(type(row.get("prompt_tokens")) is int and type(row.get("output_tokens")) is int,
                "performance token counts must be integers")
        prompt_tokens = number(row.get("prompt_tokens"), "performance prompt tokens", 1)
        output_tokens = number(row.get("output_tokens"), "performance output tokens", 1)
        context = row.get("context_tokens")
        reserved = row.get("reserved_output_tokens")
        require(type(context) is int and type(reserved) is int and (context, reserved) in cell_contracts and
                prompt_tokens + reserved == context and output_tokens == reserved,
                "performance observation outside declared context cell")
        require(row.get("settings_sha256") == cell_contracts[(context, reserved)]["generation"],
                "performance generation settings mismatch")
        order = row.get("order")
        require(type(order) is list and len(order) == len(PERFORMANCE_VARIANTS) and
                set(order) == set(PERFORMANCE_VARIANTS), "invalid performance execution order")
        exact, packed, compact = row.get("exact"), row.get("packed_exact"), row.get("attention_matching")
        require(all(type(value) is dict for value in (exact, packed, compact)), "incomplete paired performance sample")
        for label, value in (("exact", exact), ("packed exact", packed), ("attention matching", compact)):
            variant = label.replace(" ", "_")
            require(value.get("completed") is True and isinstance(value.get("attempt_id"), str) and value["attempt_id"],
                    "incomplete performance attempt")
            require(value.get("settings_sha256") == cell_contracts[(context, reserved)]["variants"][variant],
                    "performance variant settings mismatch")
            number(value.get("ttft_ms"), label + " TTFT", .000001)
            number(value.get("decode_tokens_per_second"), label + " decode throughput", .000001)
            number(value.get("committed_context_bytes"), label + " committed context bytes", 1)
            require(value.get("swapout_growth_bytes") == 0, label + " run paged")
        require(exact.get("output_sha256") == packed.get("output_sha256") and
                isinstance(exact.get("output_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", exact["output_sha256"]),
                "exact and packed-exact outputs differ")
        require(compact.get("published") is True and compact.get("exact_fallback") is False,
                "performance sample did not execute compaction")
        baseline_bytes = min(exact["committed_context_bytes"], packed["committed_context_bytes"])
        require((baseline_bytes - compact["committed_context_bytes"]) / baseline_bytes >= MIN_COMMITTED_SAVINGS,
                "performance sample committed savings below 25 percent")
        (warmups_by_cell if row in warmups else rows_by_cell)[(context, reserved)].append(row)

    attempts = [row[variant]["attempt_id"] for row in all_rows for variant in PERFORMANCE_VARIANTS]
    require(len(attempts) == len(set(attempts)), "duplicate performance attempt")

    exact_ratio = lambda sample: percentile([x["packed_exact"]["ttft_ms"] / x["exact"]["ttft_ms"]
                                             for x in sample], .95)
    exact_decode_ratio = lambda sample: percentile([x["packed_exact"]["decode_tokens_per_second"] /
                                                     x["exact"]["decode_tokens_per_second"]
                                                     for x in sample], .50)
    ttft_ratio = lambda sample: percentile([x["attention_matching"]["ttft_ms"] / x["exact"]["ttft_ms"]
                                            for x in sample], .95)
    decode_ratio = lambda sample: percentile([x["attention_matching"]["decode_tokens_per_second"] /
                                              x["exact"]["decode_tokens_per_second"]
                                              for x in sample], .50)
    savings = lambda sample: percentile([1 - x["attention_matching"]["committed_context_bytes"] /
                                         min(x["exact"]["committed_context_bytes"], x["packed_exact"]["committed_context_bytes"])
                                         for x in sample], .50)
    seed = expected_identity["artifact"] + expected_provenance["binary_sha256"]
    bounds = []
    for (context, reserved), measured in sorted(rows_by_cell.items()):
        require(len(warmups_by_cell[(context, reserved)]) >= MIN_PERFORMANCE_WARMUPS,
                "too few performance warmups for context cell")
        require(len(measured) >= MIN_PERFORMANCE_PAIRS, "too few paired performance samples for context cell")
        orders = {order: 0 for order in itertools.permutations(PERFORMANCE_VARIANTS)}
        for row in measured:
            orders[tuple(row["order"])] += 1
        require(max(orders.values()) - min(orders.values()) <= 1, "performance execution order is imbalanced")
        cell_seed = seed + str(context) + ":" + str(reserved)
        cell_bounds = {
            "context_tokens": context,
            "reserved_output_tokens": reserved,
            "pairs": len(measured),
            "packed_exact_ttft_ratio_upper_95": bootstrap_bound(measured, exact_ratio, True, cell_seed + "exact"),
            "packed_exact_decode_ratio_lower_95": bootstrap_bound(measured, exact_decode_ratio, False,
                                                                    cell_seed + "exact-decode"),
            "attention_matching_ttft_ratio_upper_95": bootstrap_bound(measured, ttft_ratio, True, cell_seed + "ttft"),
            "attention_matching_decode_ratio_lower_95": bootstrap_bound(measured, decode_ratio, False, cell_seed + "decode"),
            "committed_context_savings_lower_95": bootstrap_bound(measured, savings, False, cell_seed + "savings"),
        }
        require(cell_bounds["packed_exact_ttft_ratio_upper_95"] <= 1.05, "packed-exact TTFT regression")
        require(cell_bounds["packed_exact_decode_ratio_lower_95"] >= MIN_PACKED_EXACT_DECODE_RATIO,
                "packed-exact decode regression")
        require(cell_bounds["attention_matching_ttft_ratio_upper_95"] <= 1.25, "attention-matching TTFT regression")
        require(cell_bounds["attention_matching_decode_ratio_lower_95"] >= .90, "attention-matching decode regression")
        require(cell_bounds["committed_context_savings_lower_95"] >= MIN_COMMITTED_SAVINGS,
                "committed context savings below 25 percent")
        bounds.append(cell_bounds)
    return bounds


def raw_block_coverage_contract(entries, root, captures, expected_identity, expected_provenance, model_name,
                                qualified_lengths=None):
    """Bind every evaluated block to runtime token eligibility and prompt bytes."""
    import capture_attention_matching as capture
    root = Path(root).resolve()
    require(type(entries) is list and entries and len(entries) <= len(captures),
            "invalid raw numerical block coverage set")
    by_digest, by_document = {}, {}
    origin = {field: expected_provenance[field] for field in ("binary_sha256", "source_build_sha256")}
    for entry in entries:
        require(type(entry) is dict and set(entry) == {"receipt", "prompt_tokens", "prompt_token_offsets", "question_tokens",
                                                       "rendered_prompt", "capture_inputs"},
                "invalid raw block coverage files")
        path = campaign_file(root, entry["receipt"], "runtime block coverage", MAX_RAW_NUMERICAL_METADATA_BYTES)
        prompt = campaign_file(root, entry["prompt_tokens"], "coverage prompt tokens", 131072 * 8)
        offsets = campaign_file(root, entry["prompt_token_offsets"], "coverage prompt offsets", 131072 * 8)
        question_tokens = campaign_file(root, entry["question_tokens"], "coverage question tokens", 131072 * 8)
        rendered_prompt = campaign_file(root, entry["rendered_prompt"], "coverage rendered prompt", MAX_REPORT)
        receipt = read_json(path)
        derived = capture.validate_block_coverage(receipt, entry["prompt_tokens"]["sha256"], provenance=origin)
        prompt_bytes, question_bytes = prompt.read_bytes(), question_tokens.read_bytes()
        question = derived["question"]
        require(len(prompt_bytes) == receipt["prompt_tokens"] * 8 and
                offsets.stat().st_size == len(prompt_bytes) and
                entry["prompt_token_offsets"]["sha256"] == receipt["prompt_token_offsets_sha256"] and
                len(question_bytes) == question["token_count"] * 8 and
                question_bytes == prompt_bytes[question["source_start"] * 8:question["source_end"] * 8] and
                entry["question_tokens"]["sha256"] == question["tokens_sha256"],
                "runtime coverage prompt/question token mismatch")
        require(rendered_prompt.stat().st_size > 0 and
                entry["rendered_prompt"]["sha256"] == receipt["rendered_prompt_sha256"] and
                receipt["admitted_prefill_chunk_rows"] == 64,
                "runtime coverage rendered prompt or prefill chunk mismatch")
        capture_inputs_path = campaign_file(root, entry["capture_inputs"], "numerical capture inputs",
                                            MAX_RAW_NUMERICAL_METADATA_BYTES)
        capture_inputs = read_json(capture_inputs_path, entry["capture_inputs"]["sha256"])
        capture.validate_capture_inputs(capture_inputs, root, entry["receipt"], entry["prompt_tokens"],
                                        entry["prompt_token_offsets"], entry["question_tokens"], derived["loaded_identity"], origin,
                                        receipt["request"], False, entry["rendered_prompt"])
        require(derived["block_starts"], "runtime numerical coverage has no eligible blocks")
        loaded = derived["loaded_identity"]
        require(all(loaded[field + "_sha256"] == expected_identity[field]
                    for field in ("artifact", "tokenizer", "template", "layout", "implementation")),
                "runtime block coverage loaded identity mismatch")
        digest, document = entry["receipt"]["sha256"], derived["document_sha256"]
        require(digest not in by_digest and document not in by_document, "duplicate runtime block coverage")
        by_digest[digest] = receipt
        by_document[document] = derived
    observed = set()
    for meta in captures.values():
        c = meta["capture"]
        binding = c.get("runtime_block_coverage")
        require(meta.get("schema_version") == 2 and c.get("evaluation_rows", 0) > 0 and
                type(binding) is dict and set(binding) ==
                {"receipt_sha256", "prompt_tokens_sha256", "prompt_token_offsets_sha256", "rendered_prompt_sha256",
                 "admitted_prefill_chunk_rows", "qualified_context_tokens", "prompt_tokens",
                 "reserved_output_tokens"},
                "numerical capture lacks runtime block and teacher coverage")
        receipt = by_digest.get(binding["receipt_sha256"])
        require(receipt is not None and meta["document"] == receipt["document"] and
                binding["prompt_tokens_sha256"] == receipt["prompt_tokens_sha256"] and
                binding["prompt_token_offsets_sha256"] == receipt["prompt_token_offsets_sha256"] and
                binding["rendered_prompt_sha256"] == receipt["rendered_prompt_sha256"] and
                binding["admitted_prefill_chunk_rows"] == receipt["admitted_prefill_chunk_rows"] == 64 and
                binding["qualified_context_tokens"] == receipt["qualified_context_tokens"] and
                binding["prompt_tokens"] == receipt["prompt_tokens"] and
                binding["reserved_output_tokens"] == receipt["reserved_output_tokens"] and
                c["rows"] == 512 and all(source["loaded_identity"] == receipt["loaded_identity"]
                                        for source in c["query_sources"]),
                "numerical capture runtime coverage binding mismatch")
        key = (meta["document"]["sha256"], c["owner_layer"], c["block_start"])
        require(key not in observed, "duplicate numerical runtime block")
        observed.add(key)
    owners = {row[0] for row in expected_strata(model_name)}
    expected = {(document, owner, start) for document, row in by_document.items()
                for owner in owners for start in row["block_starts"]}
    require(observed == expected, "incomplete document/owner/runtime-block numerical coverage")
    lengths = sorted({row["qualified_context_tokens"] for row in by_document.values()})
    if qualified_lengths is not None:
        require(lengths == sorted(qualified_lengths), "numerical block coverage misses qualified context length")
    return {"receipts": len(entries), "blocks": len(observed), "qualified_context_tokens": lengths}


def runtime_candidate_contract(profile, captures, expected_identity):
    """Check that serving consumes the same frozen choices evaluated offline.

    Evaluation requests may be shorter than the calibration envelope. Their
    observed lengths establish lower bounds, never permission to rewrite the
    frozen maximums. Calibration provenance is checked separately.
    """
    import attention_matching_calibrate as am

    candidate = profile.get("runtime_candidate")
    if candidate is None:
        return None
    require(type(candidate) is dict and type(candidate.get("implementation")) is dict,
            "invalid frozen runtime candidate")
    implementation = candidate["implementation"]
    projected = am.derive_runtime_candidate(
        captures, profile["profiles"], candidate.get("allocation_page_bytes"),
        candidate.get("minimum_saved_bytes"), implementation.get("split_decode"),
        implementation.get("flash_prefill"))
    require(projected["identity"] == {
        name + "_sha256": expected_identity[name]
        for name in ("artifact", "tokenizer", "template", "layout", "implementation")
    }, "frozen runtime candidate identity mismatch")
    for name in ("max_prompt_tokens", "max_output_tokens"):
        value = candidate.get(name)
        require(type(value) is int and projected[name] <= value <= 131072,
                "evaluation exceeds frozen runtime envelope")
        projected[name] = value
    require(projected["max_prompt_tokens"] + projected["max_output_tokens"] <= 131072,
            "frozen runtime envelope exceeds model capacity")
    projected["profile_semantic_sha256"] = am.profile_semantic_identity(
        projected["owners"], projected["query_protocols"], projected["max_prompt_tokens"], projected["max_output_tokens"],
        projected["minimum_saved_bytes"])
    require(candidate == projected, "runtime candidate differs from frozen numerical profile")
    return {"profile_semantic_sha256": projected["profile_semantic_sha256"],
            "owners": len(projected["owners"]),
            "query_protocols": projected["query_protocols"],
            "max_prompt_tokens": projected["max_prompt_tokens"],
            "max_output_tokens": projected["max_output_tokens"]}


NATIVE_REPEAT_CALIBRATION_SCHEMA = "antfly.context.native.repeat-calibration-evidence.v1"


def raw_native_repeat_calibration_contract(raw, root, expected_identity, expected_provenance,
                                         model_name, calibration_documents, qualified_lengths):
    """Audit native recipe calibration without granting unseen numerical authority.

    A history-repeat partition can freeze retention choices for the real
    five-source serving campaign. It cannot satisfy the independent numerical
    release gate, even when all rounded whole-head diagnostics pass.
    """
    import native_repeat_profile as native_profile

    sources = ("profile", "campaign_spec", "results", "target_bootstrap",
               "target_bootstrap_resource")
    require(type(raw) is dict and set(raw) == {"schema", "evidence_scope", *sources} and
            raw["schema"] == NATIVE_REPEAT_CALIBRATION_SCHEMA and
            raw["evidence_scope"] == "native_repeat_same_replay_calibration",
            "invalid native repeat calibration bundle")
    root = Path(root).resolve(strict=True)
    paths = {name: campaign_file(root, raw[name], "native " + name, 16 * 1024 * 1024)
             for name in sources}
    require(model_name == "gemma4-e4b" and
            raw["profile"]["sha256"] == expected_identity["profile"],
            "native calibration artifact mismatch")
    audited = native_profile.validate_freeze(
        paths["profile"], raw["profile"]["sha256"],
        **{key: value for name in sources[1:]
           for key, value in ((name, paths[name]), (name + "_sha256", raw[name]["sha256"]))})
    require(audited.get("evidence_scope") == "native_repeat_same_replay_calibration" and
            audited.get("calibration_only") is True and
            audited.get("fresh_unseen_numerical_qualified") is False and
            audited.get("serving_qualified") is False and
            audited.get("release_authority") is False,
            "native calibration cannot grant release authority")
    require(audited.get("semantic_identity") == {
        name + "_sha256": expected_identity[name]
        for name in ("artifact", "tokenizer", "template", "layout", "implementation")},
        "native calibration semantic identity mismatch")
    require(audited.get("provenance") == {name: expected_provenance[name]
        for name in ("source_build_sha256", "binary_sha256")},
        "native calibration build provenance mismatch")
    documents = audited.get("calibration_documents")
    require(type(documents) is list and len(documents) == len(set(documents)) and
            set(documents) == calibration_documents,
            "native calibration corpus mismatch")
    heads = audited.get("owner_heads")
    expected_heads = [(owner, head) for owner in (5, 11, 17, 23) for head in (0, 1)]
    require(type(heads) is list and
            [(row.get("owner_layer"), row.get("kv_head")) for row in heads] == expected_heads and
            audited.get("selected_head_count") == len(expected_heads),
            "native calibration lacks complete E4B heads")
    head_numerics = audited.get("per_head_numerics")
    require(type(head_numerics) is list and
            [(row.get("owner_layer"), row.get("kv_head")) for row in head_numerics] == expected_heads,
            "native calibration lacks per-head numerical evidence")
    strata = audited.get("consumer_strata")
    require(type(strata) is list, "native calibration lacks consumer strata")
    actual_strata = [(row.get("owner_layer"), row.get("kv_head"),
                      row.get("consumer_layer"), row.get("query_head")) for row in strata]
    require(set(actual_strata) == expected_strata(model_name) and
            len(actual_strata) == len(set(actual_strata)),
            "native calibration consumer coverage mismatch")
    lengths = audited.get("calibration_context_tokens")
    require(type(lengths) is list and lengths and
            all(type(length) is int and length >= 8192 for length in lengths) and
            (qualified_lengths is None or sorted(lengths) == sorted(qualified_lengths)),
            "native calibration context envelope mismatch")
    return {"evidence_scope": "native_repeat_same_replay_calibration",
            "calibration_only": True, "fresh_unseen_numerical_qualified": False,
            "serving_qualified": False, "release_authority": False,
            "evaluation_documents": [], "calibration_documents": sorted(documents),
            "runtime_block_coverage": None, "calibration_context_tokens": sorted(lengths),
            "owner_heads": heads, "consumer_strata": strata,
            "per_head_numerics": head_numerics,
            "profile_semantic_sha256": audited["profile"]["profile_semantic_sha256"],
            "sources": {name: raw[name] for name in sources}}


def numerical_incomplete_gates(raw_numerical):
    if raw_numerical is None:
        return ["raw_numerical_observations"]
    gates = []
    if raw_numerical.get("runtime_block_coverage") is None:
        gates.append("runtime_numerical_block_coverage")
    if raw_numerical.get("calibration_only") is True:
        gates.append("fresh_unseen_native_numerical_observations")
    return gates


def raw_numerical_contract(raw, root, expected_identity, expected_provenance, model_name, calibration_documents,
                           qualified_lengths=None):
    """Recompute numerical gates from sealed captures and Metal output bytes.

    The existing collectors emit absolute working paths. Release campaigns
    supply campaign-relative copies keyed by their original SHA-256 instead of
    asking this audit to trust or reopen those absolute paths.
    """
    if type(raw) is dict and raw.get("schema") == NATIVE_REPEAT_CALIBRATION_SCHEMA:
        return raw_native_repeat_calibration_contract(
            raw, root, expected_identity, expected_provenance, model_name,
            calibration_documents, qualified_lengths)

    import attention_matching_calibrate as am
    import metal_attention_matching_qualify as metal

    root = Path(root).resolve()
    required = {"offline_numerical", "metal_qualification", "captures", "profile"}
    require(type(raw) is dict and required <= set(raw) <= required | {"coverage_receipts"},
            "invalid raw numerical evidence bundle")
    profile_path = campaign_file(root, raw["profile"], "frozen numerical profile")
    require(raw["profile"]["sha256"] == expected_identity["profile"], "frozen profile artifact mismatch")
    profile = read_json(profile_path)
    require(profile.get("schema_version") == am.SCHEMA and profile.get("stage") == "offline_numerical" and
            profile.get("serving_qualified") is False and profile.get("script_sha256") == am.script_identity(),
            "unsupported frozen numerical profile")
    require(profile.get("retention_policy") == "fixed", "serving requires a fixed retention profile")
    require(type(profile.get("profiles")) is dict and profile["profiles"], "empty frozen numerical profile")
    require(type(profile.get("calibration_document_hashes")) is list and
            set(profile["calibration_document_hashes"]) == calibration_documents,
            "frozen profile calibration corpus mismatch")
    offline_path = campaign_file(root, raw["offline_numerical"], "offline numerical receipt")
    metal_path = campaign_file(root, raw["metal_qualification"], "Metal numerical receipt")
    capture_entries = raw["captures"]
    require(type(capture_entries) is list and capture_entries, "missing sealed numerical captures")
    require(len(capture_entries) <= MAX_RAW_NUMERICAL_CAPTURES,
            "raw numerical capture count exceeds audit limit")
    capture_paths = {}
    for index, entry in enumerate(capture_entries):
        path = campaign_file(root, entry, f"sealed numerical capture {index}")
        require(entry["sha256"] not in capture_paths, "duplicate sealed numerical capture")
        capture_paths[entry["sha256"]] = path

    offline = read_json(offline_path)
    require(offline.get("schema_version") == am.SCHEMA and offline.get("stage") == "offline_numerical" and
            offline.get("serving_qualified") is False and offline.get("script_sha256") == am.script_identity(),
            "unsupported raw offline numerical receipt")
    require(offline.get("profile_sha256") == expected_identity["profile"],
            "raw numerical evaluation does not bind the frozen profile")
    receipt_captures = offline.get("captures", [])
    require(type(receipt_captures) is list and
            {entry.get("sha256") for entry in receipt_captures} == set(capture_paths) and
            len(receipt_captures) == len(capture_paths), "offline numerical capture set mismatch")

    capture_meta_by_id = {}
    aggregate_metadata_bytes = 0
    aggregate_capture_bytes = 0
    evaluation_documents = set()
    observed_strata = set()
    for entry in receipt_captures:
        capture_id = entry["sha256"]
        aggregate_metadata_bytes += capture_paths[capture_id].stat().st_size
        require(aggregate_metadata_bytes <= MAX_RAW_NUMERICAL_METADATA_BYTES,
                "raw numerical metadata bytes exceed audit limit")
        capture_meta = read_json(capture_paths[capture_id])
        shapes = am.shape_contract(capture_meta)
        c = capture_meta["capture"]
        shapes.update(exact_outputs=("<f8", (c["heads"], c["training_rows"] + c["validation_rows"] + c.get("evaluation_rows", 0), c["dim"])),
                      exact_log_partition=("<f8", (c["heads"], c["training_rows"] + c["validation_rows"] + c.get("evaluation_rows", 0))))
        tensors = capture_meta.get("tensors")
        require(type(tensors) is dict and set(tensors) == set(shapes), "incomplete sealed numerical capture")
        for name, tensor in tensors.items():
            require(type(tensor) is dict and set(tensor) == {"path", "sha256", "bytes"} and
                    isinstance(tensor.get("path"), str) and Path(tensor["path"]).name == tensor["path"] and
                    isinstance(tensor.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", tensor["sha256"]) and
                    type(tensor.get("bytes")) is int and not isinstance(tensor["bytes"], bool) and tensor["bytes"] >= 0,
                    "invalid sealed numerical tensor provenance")
            dtype, shape = shapes[name]
            expected_bytes = math.prod(shape) * am.np.dtype(dtype).itemsize
            require(tensor["bytes"] == expected_bytes, "sealed numerical tensor size declaration mismatch")
            tensor_path = capture_paths[capture_id].parent / tensor["path"]
            require(not tensor_path.is_symlink() and tensor_path.resolve().is_relative_to(root) and
                    tensor_path.is_file() and tensor_path.stat().st_size == expected_bytes and
                    sha(tensor_path) == tensor["sha256"],
                    "sealed numerical tensor escapes campaign")
        require(am.digest(capture_paths[capture_id]) == capture_id, "sealed numerical capture digest mismatch")
        require(entry.get("query_sha256") == capture_meta["tensors"]["queries"]["sha256"] and
                entry.get("metadata") == {key: capture_meta[key] for key in ("schema_version", "purpose", "model", "document", "source", "capture")},
                "offline numerical capture metadata mismatch")
        require(capture_meta["purpose"] == "evaluation", "raw release numerics require evaluation captures")
        require(capture_meta["model"]["sha256"] == expected_identity["artifact"] and
                capture_meta["model"]["tokenizer_sha256"] == expected_identity["tokenizer"] and
                capture_meta["model"]["template_sha256"] == expected_identity["template"] and
                capture_meta["model"].get("layout_sha256") == expected_identity["layout"] and
                capture_meta["model"].get("implementation_sha256") == expected_identity["implementation"],
                "raw numerical model identity mismatch")
        document = capture_meta["document"]["sha256"]
        require(document not in calibration_documents, "numerical evaluation overlaps calibration")
        require(capture_meta["document"]["id"] not in profile.get("calibration_document_ids", []) and
                capture_meta["tensors"]["queries"]["sha256"] not in profile.get("calibration_query_hashes", []),
                "numerical evaluation reuses calibration document or queries")
        evaluation_documents.add(document)
        owner = capture_meta["capture"]["owner_layer"]
        observed_strata.update((owner, row["kv_head"], row["consumer_layer"], row["query_head"])
                               for row in capture_meta["capture"]["strata"])
        aggregate_capture_bytes += sum(tensor["bytes"] for tensor in tensors.values())
        require(aggregate_capture_bytes <= MAX_RAW_NUMERICAL_INPUT_BYTES,
                "raw numerical capture bytes exceed audit limit")
        capture_meta_by_id[capture_id] = capture_meta
    am.validate_coverage(receipt_captures)
    require(observed_strata == expected_strata(model_name), "raw numerical consumer/head coverage mismatch")
    block_coverage = None
    if "coverage_receipts" in raw:
        block_coverage = raw_block_coverage_contract(raw["coverage_receipts"], root, capture_meta_by_id,
                                                     expected_identity, expected_provenance, model_name,
                                                     qualified_lengths)

    results = offline.get("results")
    require(type(results) is list and len(results) <= MAX_RAW_NUMERICAL_FITS and
            all(type(row) is dict and type(row.get("selected_attempt")) is bool for row in results),
            "raw numerical fit count exceeds audit limit")
    selected = [row for row in results if row["selected_attempt"]]
    require(selected and all(row.get("accepted") is True and row.get("exact") is False and
                             row.get("finite_storage") is True for row in selected),
            "raw numerical campaign selected a rejected or exact candidate")
    expected_heads = {(capture_id, meta["capture"]["owner_layer"], head)
                      for capture_id, meta in capture_meta_by_id.items()
                      for head in range(meta["capture"]["heads"])}
    require(len(results) == len(selected) == len(expected_heads) and
            {(row.get("capture_sha256"), row.get("owner_layer"), row.get("head")) for row in selected} == expected_heads,
            "fixed numerical profile requires exactly one candidate per capture/head")
    selected_by_key = {}
    observed_profile_keys = set()
    aggregate_fit_bytes = 0
    for row in selected:
        require(row.get("recipe") == "relative_cd_ridge_rms" and
                row.get("solver_identity") == am.RECIPE_IDENTITIES["relative_cd_ridge_rms"],
                "raw numerical recipe is not implemented by serving maintenance")
        key = (row.get("capture_sha256"), row.get("owner_layer"), row.get("head"), row.get("retained_rows"))
        require(isinstance(key[0], str) and all(type(value) is int and not isinstance(value, bool) for value in key[1:]) and
                key[0] in capture_meta_by_id and key not in selected_by_key and
                key[1] == capture_meta_by_id[key[0]]["capture"]["owner_layer"] and
                0 <= key[2] < capture_meta_by_id[key[0]]["capture"]["heads"] and
                0 < key[3] < capture_meta_by_id[key[0]]["capture"]["rows"],
                "duplicate, unknown, or invalid selected numerical candidate")
        candidate_meta = capture_meta_by_id[key[0]]
        profile_key = f"{am.model_key(candidate_meta)}:{key[1]}:{key[2]}"
        chosen = profile["profiles"].get(profile_key)
        require(type(chosen) is dict and chosen.get("retained_rows") == key[3] and
                chosen.get("recipe") == row["recipe"] and "retention_schedule" not in chosen and
                all(chosen.get(field) == value for field, value in am.capture_contract(candidate_meta).items()),
                "selected candidate differs from frozen profile")
        observed_profile_keys.add(profile_key)
        estimate = am.estimated_fit_bytes(capture_meta_by_id[key[0]], key[3])
        require(estimate <= am.DEFAULT_MAX_WORKING_SET_BYTES,
                "raw numerical fit exceeds per-fit audit memory limit")
        aggregate_fit_bytes += estimate
        require(aggregate_fit_bytes <= MAX_RAW_NUMERICAL_FIT_WORK_BYTES,
                "raw numerical aggregate fit work exceeds audit limit")
        selected_by_key[key] = row
    require(observed_profile_keys == set(profile["profiles"]), "frozen numerical profile owner/head set mismatch")
    runtime_candidate = runtime_candidate_contract(profile, capture_meta_by_id, expected_identity)

    terminal = read_json(metal_path)
    require(terminal.get("schema_version") == 1 and terminal.get("stage") == "metal_fitter_parity" and
            terminal.get("serving_qualified") is False, "unsupported Metal numerical receipt")
    require(terminal.get("binary_sha256") == expected_provenance["binary_sha256"] and
            terminal.get("build_receipt_sha256") == expected_provenance["source_build_sha256"] and
            terminal.get("numerical_receipt_sha256") == raw["offline_numerical"]["sha256"] and
            terminal.get("driver_sha256") == sha(metal.__file__), "Metal numerical provenance mismatch")
    require(number(terminal.get("metric_parity_tolerance"), "Metal metric tolerance") == metal.METRIC_PARITY_TOLERANCE and
            number(terminal.get("report_parity_tolerance"), "Metal report tolerance") == metal.REPORT_PARITY_TOLERANCE,
            "Metal numerical tolerance mismatch")
    cases = terminal.get("candidates", [])
    require(type(cases) is list and len(cases) == len(selected_by_key) and
            all(type(case) is dict for case in cases), "incomplete Metal numerical candidate set")
    aggregate_input_bytes = aggregate_capture_bytes
    for index, case in enumerate(cases):
        case_dir = metal_path.parent / f"case-{index:03d}"
        input_path = case_dir / "input.bin"
        require(case_dir.resolve().is_relative_to(root) and not case_dir.is_symlink() and
                not input_path.is_symlink() and input_path.resolve().is_relative_to(root) and input_path.is_file(),
                "Metal numerical input escapes campaign")
        aggregate_input_bytes += input_path.stat().st_size
        require(aggregate_input_bytes <= MAX_RAW_NUMERICAL_INPUT_BYTES,
                "raw numerical input bytes exceed audit limit")

    derived = []
    active_capture_id, meta, arrays = None, None, None
    for index, case in enumerate(cases):
        key = (case.get("capture_sha256"), case.get("owner_layer"), case.get("head"), case.get("retained_rows"))
        require(key in selected_by_key, "Metal candidate does not match selected FP64 candidate")
        result = selected_by_key.pop(key)
        if active_capture_id != key[0]:
            meta, arrays = am.load_capture(capture_paths[key[0]])
            active_capture_id = key[0]
        recomputed = am.fit_head(meta, arrays, result["head"], result["retained_rows"], result["recipe"])
        require(recomputed.get("accepted") is True and
                all(recomputed.get(field) == result.get(field) for field in
                    ("selected_source_rows", "training", "validation", "consumers", "solver_identity", "fit_diagnostics")),
                "offline numerical candidate differs from sealed capture")
        case_dir = metal_path.parent / f"case-{index:03d}"
        require(case_dir.resolve().is_relative_to(root) and not case_dir.is_symlink(),
                "Metal numerical case escapes campaign")
        input_path, resources_path = case_dir / "input.bin", case_dir / "resources.json"
        require(not input_path.is_symlink() and input_path.resolve().is_relative_to(root) and
                input_path.is_file() and sha(input_path) == case.get("input_sha256"),
                "Metal numerical input digest mismatch")
        require(case["input_sha256"] == metal.input_digest(meta, arrays, result),
                "Metal fitter input differs from sealed fit/validation queries")
        require(not resources_path.is_symlink() and resources_path.resolve().is_relative_to(root) and
                resources_path.is_file() and sha(resources_path) == case.get("resource_receipt_sha256"),
                "Metal numerical resource receipt mismatch")
        resources = read_json(resources_path)
        resource_guard_contract(resources)
        prefix = case_dir / "candidate"
        outputs = case.get("outputs")
        require(type(outputs) is dict and set(outputs) == {"keys.f16", "values.f16", "bias.f32", "report.bin"},
                "incomplete Metal numerical outputs")
        for suffix, digest in outputs.items():
            path = Path(str(prefix) + "-" + suffix)
            require(not path.is_symlink() and path.resolve().is_relative_to(root) and
                    path.is_file() and sha(path) == digest, "Metal numerical output digest mismatch")
        retained, dim = result["retained_rows"], meta["capture"]["dim"]
        keys = am.raw_array(str(prefix) + "-keys.f16", "<f2", (retained, dim))
        values = am.raw_array(str(prefix) + "-values.f16", "<f2", (retained, dim))
        bias = am.raw_array(str(prefix) + "-bias.f32", "<f4", (retained,))
        report_bytes = Path(str(prefix) + "-report.bin").read_bytes()
        require(len(report_bytes) == metal.REPORT.size, "invalid raw Metal numerical report")
        report_fields = ("failure_flags", "iterations", "mass_relative_max_error", "output_relative_rmse",
                         "training_mass_relative_max_error", "evaluated", "max_consumer_output_relative_rmse",
                         "validation_groups")
        report = dict(zip(report_fields, metal.REPORT.unpack(report_bytes)))
        require(report == case.get("gpu_report"), "Metal numerical report summary differs from raw bytes")
        oracle = metal.evaluate_candidate(meta, arrays, result, keys, values, bias, report)
        require(oracle == case.get("oracle"), "Metal numerical oracle summary differs from raw outputs")
        require(case.get("passed") is True, "Metal numerical case is not terminal")
        derived.extend({"owner_layer": key[1], "kv_head": row["query_head"] //
                        (next(item["query_heads"] for item in meta["capture"]["consumers"]
                              if item["layer"] == row["consumer_layer"]) // meta["capture"]["heads"]),
                        "consumer_layer": row["consumer_layer"], "query_head": row["query_head"],
                        "mass_max": row["mass_max"], "output_relative_rmse": row["output_relative_rmse"]}
                       for row in oracle["consumers"])
    require(not selected_by_key, "selected FP64 candidate lacks Metal evidence")
    require({(row["owner_layer"], row["kv_head"], row["consumer_layer"], row["query_head"])
             for row in derived} == expected_strata(model_name), "derived Metal numerical coverage mismatch")
    return {"captures": len(capture_meta_by_id), "evaluation_documents": sorted(evaluation_documents),
            "runtime_block_coverage": block_coverage,
            "runtime_candidate": runtime_candidate,
            "metal_candidates": len(cases), "consumer_observations": len(derived)}


def _finish_v2_audit(models):
    incomplete = sorted({gate for model in models for gate in model["incomplete_gates"]})
    passed = not incomplete
    return {"schema_version": 2, "stage": "release_evidence_audit", "evidence_passed": passed,
            "diagnostic_only": not passed, "serving_qualified": passed,
            "reason": None if passed else "One or more independently audited production gates remain incomplete.",
            "incomplete_gates": incomplete, "models": models}


def audit_v2(manifest, directory):
    models = manifest.get("models", [])
    require(1 <= len(models) <= 2, "release manifest must contain one or two artifact candidates")
    require(len({x.get("model") for x in models}) == len(models) and
            all(x.get("model") in {"gemma4-e2b", "gemma4-e4b"} for x in models), "invalid or duplicate artifact candidate")
    result, artifact_ids, all_eval, all_calibration = [], set(), set(), set()
    root = Path(directory).resolve()
    for model in models:
        identity, origin = model.get("semantic_identity"), model.get("provenance")
        semantic_identity(identity)
        provenance(origin)
        require(identity["artifact"] not in artifact_ids, "model artifact reused")
        artifact_ids.add(identity["artifact"])
        lengths = model.get("qualified_context_tokens")
        require(type(lengths) is list and lengths and len(lengths) == len(set(lengths)) and
                all(type(x) is int and x >= 8192 for x in lengths), "invalid qualified context lengths")
        calibration = set(model.get("calibration_documents", []))
        require(calibration and all(re.fullmatch(r"[0-9a-f]{64}", x) for x in calibration), "missing calibration corpus identities")
        all_calibration |= calibration
        verified_files = verify_provenance_files(root, model.get("provenance_files"), identity, origin)
        loaded_provenance = None
        if model.get("loaded_provenance") is not None:
            loaded_provenance = loaded_provenance_contract(
                model["loaded_provenance"], root, identity, origin, model["provenance_files"]["profile"])
        evidence = {}
        for kind in ("numerical", "quality", "soak", "failures", "performance", "exact_context"):
            entry = model.get(kind)
            require(type(entry) is dict and set(entry) == {"path", "sha256"}, "missing " + kind + " evidence")
            relative = Path(entry["path"])
            require(not relative.is_absolute() and ".." not in relative.parts, "evidence path escapes campaign")
            path = (root / relative).resolve()
            require(path.is_relative_to(root), "evidence symlink escapes campaign")
            evidence[kind] = read_json(path, entry["sha256"])
            require(evidence[kind].get("semantic_identity") == identity and evidence[kind].get("provenance") == origin,
                    "stale or mismatched " + kind)

        # Use the same auditor as the standalone exact-context command. Keeping
        # a second, weaker parser here once silently discarded incomplete pairs
        # and accepted campaigns without warmups or full decode reservations.
        import exact_context_qualify as exact_context
        exact = evidence["exact_context"]
        raw_exact = None
        if exact.get("raw_evidence") is not None:
            raw_exact = exact_context.raw_contract(exact["raw_evidence"], root, identity, origin)
        exact_recomputed = exact_context.audit(raw_exact if raw_exact is not None else exact, identity, origin, lengths)
        require(exact.get("qualified_context_tokens") == sorted(lengths) and
                exact.get("results") == exact_recomputed["results"],
                "exact-context summary does not match raw observations")
        numerical = evidence["numerical"]
        raw_numerical = None
        if numerical.get("raw_evidence") is not None:
            raw_numerical = raw_numerical_contract(
                numerical["raw_evidence"], root, identity, origin, model["model"], calibration, lengths)
            all_eval.update(raw_numerical["evaluation_documents"])
        else:
            # Schema-2 summary fields remain readable for old diagnostic
            # campaigns, but they never satisfy the raw numerical gate.
            require(numerical.get("frozen_before_evaluation") is True and
                    numerical.get("all_owner_heads_covered") is True and
                    numerical.get("metal_fp64_parity_passed") is True, "incomplete numerical campaign")
            rows = numerical.get("consumers", [])
            actual_strata = [(row["owner_layer"], row["kv_head"], row["consumer_layer"], row["query_head"])
                             for row in rows]
            require(set(actual_strata) == expected_strata(model["model"]) and
                    len(actual_strata) == len(set(actual_strata)), "missing or duplicate numerical consumer/head")
            for row in rows:
                require(number(row["mass_max"], "mass error") <= .05 and
                        number(row["output_relative_rmse"], "output error") <= .10 and
                        row.get("after_f16") is True, "numerical gate failed")

        raw_quality = None
        if evidence["quality"].get("raw_evidence") is not None:
            raw_quality = raw_quality_contract(evidence["quality"]["raw_evidence"], root, identity, origin, lengths)
            require(evidence["quality"].get("cases") == raw_quality["cases"],
                    "quality summary does not match raw reports")
        cases = (raw_quality if raw_quality is not None else evidence["quality"]).get("cases", [])
        require(cases and len({x.get("id") for x in cases}) == len(cases), "missing or duplicate quality case")
        evaluation_inputs = []
        for case in cases:
            report_contract_v2(case, identity, origin)
            require(case.get("bucket") in BUCKETS and request_context_tokens(case) in lengths, "quality case outside release claim")
            parts = quality_evaluation_source_identities(case)
            evaluation_inputs.extend(parts)
            require(type(case.get("exact_correct")) is bool and type(case.get("candidate_correct")) is bool, "invalid paired accuracy")
            require(type(case.get("scored_tokens")) is int and case["scored_tokens"] > 0, "empty teacher-forced continuation")
            all_eval.update(parts)
        require(len(set(evaluation_inputs)) == len(evaluation_inputs),
                "quality evaluation inputs are not independent")
        for length in lengths:
            for bucket in BUCKETS:
                group = [x for x in cases if request_context_tokens(x) == length and x["bucket"] == bucket]
                require(len(group) >= MIN_QUALITY_CASES, f"too few quality cases: {length}/{bucket}")
                require(quality_upper_bound(group, identity["artifact"] + str(length) + bucket) <= .05,
                        f"bucket quality uncertainty gate failed: {length}/{bucket}")
        quality_counts = [sum(1 for case in cases if request_context_tokens(case) == length and case["bucket"] == bucket)
                          for length in lengths for bucket in sorted(BUCKETS)]
        require(len(set(quality_counts)) == 1, "quality cells are imbalanced")
        require(quality_upper_bound(cases, identity["artifact"] + "aggregate") <= .02, "aggregate quality uncertainty gate failed")
        tokens = sum(x["scored_tokens"] for x in cases)
        require((sum(number(x["candidate_nll_sum"], "candidate NLL") for x in cases) -
                 sum(number(x["exact_nll_sum"], "exact NLL") for x in cases)) / tokens <= math.log(1.05), "perplexity regression")

        soak = evidence["soak"]
        raw_soak = None
        if soak.get("raw_evidence") is not None:
            raw_soak = raw_soak_contract(soak["raw_evidence"], root, identity, origin, lengths)
            # Every timing, report and resource value is reconstructed from the
            # bound collector journal; legacy summary fields have no authority.
            soak = {**raw_soak, "completed": True, "persistent_driver": True}
        trials = soak.get("trials", [])
        require(soak.get("completed") is True and soak.get("persistent_driver") is True and
                number(soak.get("duration_seconds"), "soak duration") >= 14400 and trials and len(trials) <= 4096,
                "incomplete four-hour soak")
        require({x["report"]["route"] for x in trials} == ROUTES, "missing serving route in soak")
        route_soak = raw_soak is not None and raw_soak.get("route_contract_verified") is True
        previous_end = 0
        live_baseline = next((trial["report"].get("live_bytes_before") for trial in trials
                              if trial["report"].get("live_bytes_before") is not None), None)
        for index, trial in enumerate(trials):
            start, end = number(trial.get("start_seconds"), "trial start"), number(trial.get("end_seconds"), "trial end")
            require(previous_end <= start <= end <= soak["duration_seconds"], "nonserial soak timeline")
            require(trial["report"].get("request_index") == index, "soak report sequence mismatch")
            if route_soak:
                # The raw route contract already audited genuine mixed-group
                # allocation receipts and the separate CLI child receipt.
                if trial["report"]["route"] != "serial_mixed_batch":
                    require(trial["report"].get("live_bytes_before") == live_baseline,
                            "soak live allocation baseline grew")
            else:
                report_contract_v2(trial["report"], identity, origin)
                require(trial["report"].get("live_bytes_before") == live_baseline,
                        "soak live allocation baseline grew")
            resources = trial.get("resources", {})
            require(resources.get("swapout_growth_bytes") == 0 and number(resources.get("min_free_percent"), "free memory") >= 15 and
                    number(resources.get("min_disk_free_mib"), "free disk") >= 4096 and
                    number(resources.get("peak_group_rss_mib"), "process RSS") <= soak["max_rss_mib"], "soak resource guard failed")
            previous_end = end
        require(trials[0]["start_seconds"] <= 60 and soak["duration_seconds"] - previous_end <= 60 and
                sum(x["end_seconds"] - x["start_seconds"] for x in trials) >= .9 * soak["duration_seconds"], "soak is mostly idle time")

        raw_failures = None
        if evidence["failures"].get("raw_evidence") is not None:
            raw_failures = raw_failure_contract(evidence["failures"]["raw_evidence"], root, identity, origin, lengths)
        failures = ([trial["report"] for trial in raw_failures["trials"]] if raw_failures is not None
                    else evidence["failures"].get("cases", []))
        require({x.get("fault") for x in failures} == FAILURES and len(failures) == len(FAILURES), "incomplete failure/reload campaign")
        for case in failures:
            events = case.get("events", [])
            require(events and all(type(event) is dict and type(event.get("monotonic_ns")) is int and
                                   event["monotonic_ns"] >= 0 and isinstance(event.get("name"), str) and
                                   type(event.get("live_bytes")) is int and event["live_bytes"] >= 0 for event in events),
                    "invalid failure event journal")
            require([event["monotonic_ns"] for event in events] == sorted(event["monotonic_ns"] for event in events),
                    "failure event journal is not ordered")
            names = [event["name"] for event in events]
            verified_event = ("partial_context_destroyed" if case.get("fault") == "cancel_prefill" else
                              "allocation_rollback_verified" if case.get("fault") == "oom" else
                              "model_generation_reloaded" if case.get("fault") == "reload" else
                              "original_context_verified")
            require({"fault_injected", verified_event, "request_released"} <= set(names) and
                    names[-1] == "request_released", "incomplete failure event journal")
            injected = names.index("fault_injected")
            if case["fault"] == "reload":
                require(case.get("stale_selection_rejected") is True and
                        names.count("stale_selection_rejected") == 1 and
                        names.count("fresh_attention_matching_published") == 1 and
                        names.index("stale_selection_rejected") < names.index("fresh_attention_matching_published") and
                        "publication_commit" not in names,
                        "reload did not reject stale state before fresh publication")
            else:
                require("publication_commit" not in names[injected + 1:], "failure published after injection")
            require(events[-1]["live_bytes"] == case.get("live_bytes_before"), "failure leaked request allocations")
            require(case.get("observer_output_sha256") == case.get("exact_output_sha256") and
                    isinstance(case.get("exact_output_sha256"), str) and
                    re.fullmatch(r"[0-9a-f]{64}", case["exact_output_sha256"]), "failure isolation output mismatch")
            if case["fault"] == "reload":
                require(case.get("request_generation") == case.get("loaded_generation_after") and
                        case.get("loaded_generation_after") != case.get("loaded_generation_before"),
                        "reload generation ownership failed")
        performance = evidence["performance"]
        raw_performance = None
        if performance.get("raw_evidence") is not None:
            raw_performance = raw_performance_contract(performance["raw_evidence"], root, identity, origin, lengths)
            require(all(performance.get(key) == raw_performance[key] for key in ("cells", "warmups", "samples")),
                    "performance summary does not match raw reports")
        bounds = performance_contract(raw_performance if raw_performance is not None else performance, identity, origin, lengths)
        incomplete = [] if loaded_provenance is not None else ["loaded_tokenizer_template_profile_provenance"]
        if raw_quality is None:
            incomplete.append("raw_quality_observations")
        if raw_performance is None:
            incomplete.append("raw_performance_observations")
        if raw_exact is None:
            incomplete.append("raw_exact_context_observations")
        if raw_failures is None:
            incomplete.append("raw_failure_injection_observations")
        if raw_soak is None:
            incomplete.append("v2_raw_soak_collector")
        incomplete.extend(numerical_incomplete_gates(raw_numerical))
        result.append({"model": model["model"], "semantic_identity": identity, "provenance": origin,
                       "qualified_context_tokens": sorted(lengths), "quality_cases": len(cases),
                       "performance_bounds": bounds, "provenance_files": verified_files,
                       "diagnostic_checks": ["campaign_file_digest_consistency", "exact_context_observation_recalculation",
                                             "paired_performance_statistics", "paired_quality_statistics"],
                       "raw_numerical": raw_numerical,
                       "loaded_provenance": loaded_provenance,
                       "raw_quality": None if raw_quality is None else {
                           "cases": len(raw_quality["cases"]), "resource_samples_verified": True,
                           "loaded_identity_verified": True, "raw_reference_and_token_scoring": True},
                       "raw_exact_context": None if raw_exact is None else {
                           "observations": len(raw_exact["observations"]), "resource_samples_verified": True,
                           "loaded_identity_verified": True},
                       "raw_performance": None if raw_performance is None else {
                           "pairs": len(raw_performance["samples"]), "warmups": len(raw_performance["warmups"]),
                           "resource_samples_verified": True, "loaded_identity_verified": True},
                       "raw_soak": None if raw_soak is None else {
                           key: value for key, value in raw_soak.items() if key != "trials"},
                       "raw_failures": None if raw_failures is None else {
                           key: value for key, value in raw_failures.items() if key != "trials"},
                       "incomplete_gates": incomplete, "evidence_passed": not incomplete})
    require_evaluation_calibration_disjoint(all_eval, all_calibration)
    return _finish_v2_audit(result)


def audit_v1(manifest, directory):
    """Compute gates from paired measurements. Never trust a top-level pass bit."""
    require(manifest.get("schema_version") == 1, "unsupported release manifest")
    models = manifest.get("models", [])
    require({x["model"] for x in models} == {"gemma4-e2b", "gemma4-e4b"} and len(models) == 2, "E2B and E4B require separate evidence")
    result = []
    artifact_ids = set()
    all_eval = set()
    all_calibration = set()
    for model in models:
        expected = model["identity"]
        binding(expected)
        require(expected["artifact"] not in artifact_ids, "model artifact reused")
        artifact_ids.add(expected["artifact"])
        calibration = set(model["calibration_documents"])
        require(calibration and all(re.fullmatch(r"[0-9a-f]{64}", x) for x in calibration), "missing calibration corpus identities")
        all_calibration |= calibration
        evidence = {}
        for kind in ("numerical", "quality", "soak", "failures", "performance"):
            entry = model[kind]
            relative = Path(entry["path"])
            require(not relative.is_absolute() and ".." not in relative.parts, "evidence path escapes campaign")
            path = (Path(directory) / relative).resolve()
            require(path.is_relative_to(Path(directory).resolve()), "evidence symlink escapes campaign")
            evidence[kind] = read_json(path, entry["sha256"])
            require(evidence[kind].get("identity") == expected, "stale or mismatched " + kind)
        numerical = evidence["numerical"]
        require(numerical.get("frozen_before_evaluation") is True and numerical.get("all_owner_heads_covered") is True and
                numerical.get("metal_fp64_parity_passed") is True, "incomplete numerical campaign")
        rows = numerical.get("consumers", [])
        actual_strata = [(row["owner_layer"], row["kv_head"], row["consumer_layer"], row["query_head"]) for row in rows]
        require(set(actual_strata) == expected_strata(model["model"]) and len(actual_strata) == len(set(actual_strata)), "missing or duplicate numerical consumer/head")
        for row in rows:
            require(number(row["mass_max"], "mass error") <= .05 and
                    number(row["output_relative_rmse"], "output error") <= .10 and
                    row.get("after_f16") is True, "numerical gate failed")
        cases = evidence["quality"].get("cases", [])
        require(cases and {x["bucket"] for x in cases} == BUCKETS, "missing quality bucket")
        require(len({x["id"] for x in cases}) == len(cases), "duplicate quality case")
        require({(x["prompt_tokens"], x["bucket"]) for x in cases} >= {(length, bucket) for length in (4096, 8192) for bucket in BUCKETS}, "missing 4K/8K quality bucket")
        nll_before = nll_after = tokens = 0
        for case in cases:
            report_contract(case, expected)
            require(re.fullmatch(r"[0-9a-f]{64}", case["document_sha256"]), "invalid evaluation document")
            all_eval.add(case["document_sha256"])
            require(type(case["exact_correct"]) is bool and type(case["candidate_correct"]) is bool, "invalid paired accuracy")
            count = case["scored_tokens"]
            require(type(count) is int and count > 0, "empty teacher-forced continuation")
            tokens += count
            nll_before += number(case["exact_nll_sum"], "exact NLL")
            nll_after += number(case["candidate_nll_sum"], "candidate NLL")
        accuracy = lambda rows: sum(int(x["exact_correct"]) - int(x["candidate_correct"]) for x in rows) / len(rows)
        require(accuracy(cases) <= .02 + 1e-12, "aggregate quality regression")
        for bucket in BUCKETS:
            require(accuracy([x for x in cases if x["bucket"] == bucket]) <= .05 + 1e-12, "bucket quality regression: " + bucket)
        require((nll_after - nll_before) / tokens <= math.log(1.05), "perplexity regression")
        soak = evidence["soak"]
        trials = soak.get("trials", [])
        require(trials and len(trials) <= 4096 and soak.get("persistent_driver") is True and soak.get("completed") is True and number(soak["duration_seconds"], "soak duration") >= 14400, "incomplete four-hour soak")
        require({x["report"]["route"] for x in trials} == ROUTES, "missing serving route in soak")
        previous_end = 0
        live_baseline = trials[0]["report"]["live_bytes_before"]
        for trial_index, trial in enumerate(trials):
            require(trial["report"]["request_index"] == trial_index, "soak report sequence mismatch")
            start = number(trial["start_seconds"], "trial start")
            end = number(trial["end_seconds"], "trial end")
            require(previous_end <= start <= end <= soak["duration_seconds"], "nonserial soak timeline")
            report_contract(trial["report"], expected)
            require(trial["report"]["live_bytes_before"] == live_baseline, "soak live allocation baseline grew")
            resources = trial["resources"]
            require(resources.get("pass") is True and resources["swapout_growth_bytes"] == 0 and
                    number(resources["min_free_percent"], "free memory") >= 15 and
                    number(resources["min_disk_free_mib"], "free disk") >= 4096 and
                    number(resources["peak_group_rss_mib"], "process RSS") <= soak["max_rss_mib"], "soak resource guard failed")
            previous_end = end
        require(trials[0]["start_seconds"] <= 60 and soak["duration_seconds"] - previous_end <= 60 and
                sum(x["end_seconds"] - x["start_seconds"] for x in trials) >= 0.9 * soak["duration_seconds"], "soak is mostly idle time")
        failures = evidence["failures"].get("cases", [])
        require({x["fault"] for x in failures} == FAILURES and all(x.get("rollback_passed") is True and
                x.get("isolation_passed") is True and x.get("released_all_allocations") is True for x in failures), "incomplete failure/reload campaign")
        perf = evidence["performance"]
        require(perf.get("clean_memory") is True and perf.get("matched_output_identity") is True and
                number(perf["exact_path_latency_ratio"], "latency ratio", .000001) <= 1.05, "exact-path performance regression")
        result.append({"model": model["model"], "identity": expected, "cases": len(cases), "soak_trials": len(trials),
                       "qualified_context_tokens": sorted({x["prompt_tokens"] for x in cases}), "evidence_passed": True})
    require(not all_eval & all_calibration, "evaluation corpus overlaps calibration")
    return {"schema_version": 1, "stage": "release_evidence_audit", "evidence_passed": True,
            "diagnostic_only": True, "serving_qualified": False,
            "reason": "Schema 1 evidence is retained for diagnostics and cannot qualify a release.", "models": result}


def audit(manifest, directory):
    if manifest.get("schema_version") == 2:
        return audit_v2(manifest, directory)
    if manifest.get("schema_version") == 1:
        return audit_v1(manifest, directory)
    raise ValueError("unsupported release manifest")


def collect_loaded_provenance_v2(command, identity, origin, profile_file, directory, timeout=900):
    """Export loaded tokenizer/template preimages from one genuine pinned model.

    The child selects the frozen profile against the actual loaded generation,
    writes its exact preimages beneath the parent-owned campaign directory, and
    returns only compact immutable references. No request supplies identities.
    """
    semantic_identity(identity); provenance(origin)
    require(type(command) is list and command and all(isinstance(arg, str) and arg for arg in command),
            "provenance driver must be an argv array")
    binary = Path(command[0]).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK) and sha(binary) == origin["binary_sha256"],
            "provenance driver binary mismatch")
    source_profile = Path(profile_file).resolve()
    require(source_profile.is_file() and not source_profile.is_symlink() and
            source_profile.stat().st_size <= MAX_REPORT and sha(source_profile) == identity["profile"],
            "provenance frozen profile mismatch")
    number(timeout, "provenance timeout", .001)
    directory = Path(directory).resolve(); directory.mkdir(exist_ok=False)
    profile_path = directory / "frozen-profile.json"
    with profile_path.open("xb") as stream: stream.write(source_profile.read_bytes())
    require(sha(profile_path) == identity["profile"], "provenance profile changed while copying")
    nonce = os.urandom(32).hex()
    request = {"command": "describe_provenance", "session_id": nonce, "request_index": 0}
    stop = {"command": "stop", "session_id": nonce}
    driver_command = [str(binary), *command[1:]]
    env = dict(os.environ)
    for name in ("ANTFLY_CONTEXT_COVERAGE_MANIFEST", "ANTFLY_CONTEXT_COVERAGE_MANIFEST_SHA256",
                 "ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT"):
        env.pop(name, None)
    env.update(ANTFLY_CONTEXT_SESSION_ID=nonce, ANTFLY_CONTEXT_CAMPAIGN_DIR=str(directory),
               ANTFLY_CONTEXT_PROFILE=str(profile_path), ANTFLY_CONTEXT_PROFILE_SHA256=identity["profile"],
               ANTFLY_CONTEXT_SOURCE_BUILD_SHA256=origin["source_build_sha256"],
               ANTFLY_CONTEXT_DEVICE_SHA256=origin["device_sha256"],
               ANTFLY_CONTEXT_OS_SHA256=origin["os_sha256"])
    started = time.monotonic_ns(); process = None; failure = None
    receipt = {"schema_version": 1, "stage": "raw_loaded_compaction_provenance",
               "semantic_identity": identity, "provenance": origin, "session_id": nonce,
               "driver_command": driver_command, "driver_binary_sha256": origin["binary_sha256"],
               "completed": False, "returncode": None,
               "child_file_limit_policy": dict(CHILD_FILE_LIMIT_POLICY)}

    stdout_path, stderr_path = directory / "driver.jsonl", directory / "driver.stderr"
    try:
        with stdout_path.open("xb") as stdout_stream, stderr_path.open("xb") as stderr_stream:
            process = subprocess.Popen(driver_command, stdin=subprocess.PIPE, stdout=stdout_stream,
                                       stderr=stderr_stream, start_new_session=True,
                                       preexec_fn=_limit_qualification_child_files, env=env)
            payload = ((json.dumps(request, allow_nan=False, sort_keys=True) + "\n") +
                       (json.dumps(stop, allow_nan=False, sort_keys=True) + "\n")).encode()
            process.communicate(payload, timeout=timeout)
        receipt["returncode"] = process.returncode
        stdout, stderr = stdout_path.read_bytes(), stderr_path.read_bytes()
        require(process.returncode == 0 and len(stdout) <= MAX_DRIVER_LINE_BYTES and
                len(stderr) <= MAX_JOURNAL_BYTES, "provenance driver failed or exceeded output bounds")
        lines = stdout.splitlines()
        require(len(lines) == 1, "provenance driver emitted missing or unsolicited reports")
        report = decode_json(lines[0])
        profile_ref = {"path": profile_path.name, "sha256": identity["profile"]}
        audited = loaded_provenance_report_contract(report, directory, identity, origin,
                                                     profile_ref, nonce, 0)
        receipt["loaded_provenance"] = audited["loaded_provenance"]
        receipt["loaded"] = audited["audited"]
        receipt["completed"] = True
    except BaseException as error:
        failure = error; receipt["failure"] = str(error)
    finally:
        if process is not None and process.poll() is None:
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try: os.killpg(process.pid, sig)
                except ProcessLookupError: break
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired: continue
                break
        if process is not None and receipt["returncode"] is None: receipt["returncode"] = process.returncode
        receipt["elapsed_ns"] = time.monotonic_ns() - started
        write_json(directory / "session.json", receipt)
    if failure is not None: raise failure
    return receipt


def collect_soak_v2(command, identity, origin, directory, duration=14400, timeout=300, max_rss_mib=8192):
    """Collect raw parent-clock evidence from a persistent native serving driver.

    The executable itself must match the campaign binary, so an unbound shell or
    Python wrapper cannot supply release observations. Short runs remain useful
    protocol diagnostics. Every exit leaves a terminal receipt and raw journal.
    """
    return _collect_driver_v2(command, identity, origin, directory, duration, timeout, max_rss_mib, False)


def collect_route_soak_v2(command, identity, origin, plan, plan_root, cli_binary,
                          cli_source_build_receipt, directory,
                          duration=14400, timeout=300, max_rss_mib=8192):
    """Run a sealed five-route schedule through one persistent loaded owner.

    CLI is a separate child process attached to the same warm loopback server;
    the server observation remains authoritative for model facts while the
    child executable and argv receive an independent receipt.
    """
    return _collect_driver_v2(
        command, identity, origin, directory, duration, timeout, max_rss_mib, False,
        route_plan=plan, route_root=plan_root, cli_binary=cli_binary,
        cli_source_build_receipt=cli_source_build_receipt)


def collect_failures_v2(command, identity, origin, plan, plan_root, directory, timeout=300, max_rss_mib=8192):
    """Run every required fault through one bounded persistent native driver."""
    return _collect_driver_v2(command, identity, origin, directory, 0, timeout, max_rss_mib, True,
                              failure_plan=plan, failure_root=plan_root)


def collect_performance_v2(command, identity, origin, plan, directory, timeout=900, max_rss_mib=8192):
    """Execute a predeclared matched campaign on one pinned loaded generation."""
    return _collect_driver_v2(command, identity, origin, directory, 0, timeout, max_rss_mib, False, plan)


def collect_quality_v2(command, identity, origin, plan, plan_root, directory, timeout=900, max_rss_mib=8192):
    return _collect_driver_v2(command, identity, origin, directory, 0, timeout, max_rss_mib, False,
                              quality_plan=plan, quality_root=plan_root)


def _coverage_generation_settings(reserved_output_tokens):
    require(type(reserved_output_tokens) is int and 0 < reserved_output_tokens < 2**32,
            "invalid coverage output reservation")
    return hashlib.sha256(b"antfly.context-serving.generation.v1\0" +
                          struct.pack("<I", reserved_output_tokens) + bytes((1, 0, 0, 0, 0, 0))).hexdigest()


def _variant_settings_v2(variant, policy, implementation_sha256, profile_artifact_sha256,
                         profile_semantic_sha256, split_decode, flash_prefill):
    require(isinstance(variant, str) and variant.isascii() and variant and len(variant) < 2**32,
            "invalid variant settings role")
    require(type(policy) is dict and set(policy) == {"max_context_tokens", "max_kv_tokens",
            "keep_prefix_tokens", "keep_recent_tokens"}, "invalid variant settings policy")
    for field in ("max_context_tokens", "max_kv_tokens", "keep_prefix_tokens", "keep_recent_tokens"):
        require(type(policy[field]) is int and 0 <= policy[field] < 2**32,
                "invalid variant settings " + field)
    require(type(split_decode) is bool and type(flash_prefill) is bool,
            "invalid variant settings kernel flags")
    digests = (implementation_sha256, profile_artifact_sha256, profile_semantic_sha256)
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in digests),
            "invalid variant settings identity")
    encoded = variant.encode("ascii")
    raw = bytearray(b"antfly.context-serving.variant.v2\0")
    raw.extend(struct.pack("<I", len(encoded))); raw.extend(encoded)
    raw.extend(struct.pack("<IIII", *(policy[field] for field in
                                      ("max_context_tokens", "max_kv_tokens",
                                       "keep_prefix_tokens", "keep_recent_tokens"))))
    raw.extend(bytes((int(split_decode), int(flash_prefill))))
    for value in digests:
        raw.extend(bytes.fromhex(value))
    return hashlib.sha256(raw).hexdigest()


def _coverage_variant_settings(policy, identity):
    # Coverage runs the compiled candidate kernel route without a retention
    # profile. Both profile bindings are deliberately zero and the loaded
    # implementation digest remains mandatory.
    return _variant_settings_v2("packed_exact", policy, identity["implementation"], "0" * 64,
                                "0" * 64, True, True)


def _coverage_structured_request_identity(document, question):
    require(isinstance(document, bytes) and document and isinstance(question, bytes) and question,
            "empty structured coverage input")
    hashed = hashlib.sha256()
    hashed.update(b"antfly.context-serving.structured-request.v1\0")
    hashed.update(b"user\0")
    hashed.update(bytes((2,)))
    for selected, value in ((1, document), (0, question)):
        hashed.update(bytes((1, selected)))
        hashed.update(struct.pack("<Q", len(value)))
        hashed.update(value)
    hashed.update(struct.pack("<II", 0, 0))
    return hashed.hexdigest()


def failure_plan_contract(plan, root, identity):
    """Validate and expand one sealed structured request across every fault site."""
    root = Path(root).resolve()
    semantic_identity(identity)
    require(type(plan) is dict and set(plan) == {"schema_version", "policy", "runtime", "request", "faults"} and
            plan["schema_version"] == 1, "invalid failure collection plan")
    policy = plan["policy"]
    require(type(policy) is dict and set(policy) == {"max_context_tokens", "max_kv_tokens",
            "keep_prefix_tokens", "keep_recent_tokens"}, "invalid failure policy")
    require(all(type(policy[key]) is int and 0 <= policy[key] <= 2**32 - 1 for key in policy) and
            policy["max_context_tokens"] > 0 and policy["max_kv_tokens"] > 0 and
            policy["max_kv_tokens"] <= policy["max_context_tokens"], "invalid failure policy bounds")
    runtime = plan["runtime"]
    require(type(runtime) is dict and set(runtime) == {"optimize_decode", "optimize_prefill",
            "admitted_prefill_chunk_rows", "profile_semantic_sha256"} and
            type(runtime["optimize_decode"]) is bool and type(runtime["optimize_prefill"]) is bool and
            runtime["admitted_prefill_chunk_rows"] == 64 and
            isinstance(runtime["profile_semantic_sha256"], str) and
            re.fullmatch(r"[0-9a-f]{64}", runtime["profile_semantic_sha256"]),
            "invalid failure runtime authority")
    request = plan["request"]
    required = {"document_file", "document_sha256", "question_file", "question_sha256",
                "request_content_sha256", "prompt_sha256", "prompt_tokens", "reserved_output_tokens"}
    require(type(request) is dict and set(request) == required, "invalid failure request")
    files = {}
    for name in ("document", "question"):
        relative = Path(request[name + "_file"])
        require(not relative.is_absolute() and ".." not in relative.parts and relative.parts and
                isinstance(request[name + "_sha256"], str) and
                re.fullmatch(r"[0-9a-f]{64}", request[name + "_sha256"]),
                "invalid failure input path or digest")
        unresolved = root / relative
        path = unresolved.resolve()
        require(path.is_relative_to(root) and path.is_file() and not unresolved.is_symlink() and
                0 < path.stat().st_size <= MAX_REPORT and sha(path) == request[name + "_sha256"],
                "failure input file identity mismatch")
        files[name] = path.read_bytes()
    require(_coverage_structured_request_identity(files["document"], files["question"]) ==
            request["request_content_sha256"], "failure structured request identity mismatch")
    require(isinstance(request["prompt_sha256"], str) and
            re.fullmatch(r"[0-9a-f]{64}", request["prompt_sha256"]) and
            type(request["prompt_tokens"]) is int and 0 < request["prompt_tokens"] <= 131072 and
            type(request["reserved_output_tokens"]) is int and
            0 < request["reserved_output_tokens"] <= 4096 and
            request["prompt_tokens"] + request["reserved_output_tokens"] <= policy["max_context_tokens"],
            "invalid failure rendered prompt binding")
    require(type(plan["faults"]) is list and tuple(plan["faults"]) == FAILURE_ORDER,
            "failure plan must use canonical complete fault order")
    settings = _coverage_generation_settings(request["reserved_output_tokens"])
    variant = _variant_settings_v2("attention_matching", policy, identity["implementation"],
                                   identity["profile"], runtime["profile_semantic_sha256"],
                                   runtime["optimize_decode"], runtime["optimize_prefill"])
    common = {**request, "route": "native", "variant": "attention_matching",
              "settings_sha256": settings, "variant_settings_sha256": variant}
    return [{**common, "fault": fault} for fault in FAILURE_ORDER]


def _coverage_plan_contract(plan, root):
    root = Path(root).resolve()
    require(type(plan) is dict and set(plan) == {"schema_version", "policy", "cases"} and
            plan["schema_version"] == 2, "invalid coverage collection plan")
    policy = plan["policy"]
    require(type(policy) is dict and set(policy) == {"max_context_tokens", "max_kv_tokens",
            "keep_prefix_tokens", "keep_recent_tokens"}, "invalid coverage policy")
    for field in ("max_context_tokens", "max_kv_tokens", "keep_recent_tokens"):
        require(type(policy[field]) is int and 0 < policy[field] < 2**32,
                "invalid coverage policy " + field)
    require(type(policy["keep_prefix_tokens"]) is int and 0 <= policy["keep_prefix_tokens"] < 2**32 and
            policy["max_context_tokens"] <= 131072 and
            policy["max_kv_tokens"] < policy["max_context_tokens"] and
            policy["keep_prefix_tokens"] + policy["keep_recent_tokens"] < policy["max_kv_tokens"],
            "unsupported coverage policy")
    cases = plan["cases"]
    require(type(cases) is list and 0 < len(cases) <= 4096, "empty or oversized coverage plan")
    ids, prompts, documents = set(), set(), set()
    exact = {"case_id", "document_file", "document_sha256", "bucket", "question_file",
             "question_sha256", "request_content_sha256", "prompt_tokens",
             "reserved_output_tokens", "route"}
    for case in cases:
        require(type(case) is dict and set(case) == exact and
                isinstance(case["case_id"], str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", case["case_id"]) and
                isinstance(case["bucket"], str) and case["bucket"] in BUCKETS and
                isinstance(case["route"], str) and case["route"] in {"native", "http", "stream"},
                "invalid coverage case")
        for field in ("document", "question"):
            require(isinstance(case[field + "_file"], str) and case[field + "_file"] and
                    isinstance(case[field + "_sha256"], str) and
                    re.fullmatch(r"[0-9a-f]{64}", case[field + "_sha256"]),
                    "invalid coverage " + field + " identity")
        require(isinstance(case["request_content_sha256"], str) and
                re.fullmatch(r"[0-9a-f]{64}", case["request_content_sha256"]),
                "invalid structured coverage request identity")
        require(case["case_id"] not in ids and case["request_content_sha256"] not in prompts and
                case["document_sha256"] not in documents, "duplicate coverage case identity")
        ids.add(case["case_id"]); prompts.add(case["request_content_sha256"]); documents.add(case["document_sha256"])
        require((case["prompt_tokens"] is None or
                 type(case["prompt_tokens"]) is int and 0 < case["prompt_tokens"] <= 131072) and
                type(case["reserved_output_tokens"]) is int and case["reserved_output_tokens"] > 0 and
                (case["prompt_tokens"] is None or
                 case["prompt_tokens"] + case["reserved_output_tokens"] <= policy["max_context_tokens"]),
                "coverage case exceeds context ceiling")
        files = {}
        for field in ("document", "question"):
            relative = Path(case[field + "_file"])
            require(not relative.is_absolute() and ".." not in relative.parts,
                    "coverage input path escapes plan root")
            path = (root / relative).resolve()
            require(path.is_relative_to(root) and path.is_file() and not (root / relative).is_symlink() and
                    path.stat().st_size <= MAX_REPORT and sha(path) == case[field + "_sha256"],
                    "coverage input identity mismatch")
            files[field] = path.read_bytes()
        require(_coverage_structured_request_identity(files["document"], files["question"]) ==
                case["request_content_sha256"], "structured coverage request identity mismatch")
    return plan


def _coverage_report_contract(report, root, request, identity, origin, nonce, index):
    import capture_attention_matching as capture
    require(type(report) is dict and set(report) == {"schema_version", "completed", "operation",
            "session_id", "request_index", "diagnostic_only", "observed_prompt_tokens",
            "admitted_prefill_chunk_rows", "receipt", "prompt_tokens", "question_tokens",
            "prompt_token_offsets", "rendered_prompt", "capture_inputs"} and
            report["schema_version"] == 2 and report["completed"] is True and
            report["operation"] == "coverage" and report["session_id"] == nonce and
            type(report["request_index"]) is int and report["request_index"] == index and
            type(report["diagnostic_only"]) is bool and
            report["diagnostic_only"] == (request["prompt_tokens"] is None) and
            type(report["observed_prompt_tokens"]) is int and
            0 < report["observed_prompt_tokens"] <= 131072 and
            report["admitted_prefill_chunk_rows"] == 64,
            "invalid coverage driver report")
    receipt_path = campaign_file(root, report["receipt"], "coverage receipt", MAX_RAW_NUMERICAL_METADATA_BYTES)
    token_path = campaign_file(root, report["prompt_tokens"], "coverage prompt tokens", 131072 * 8)
    offsets_path = campaign_file(root, report["prompt_token_offsets"],
                                 "coverage prompt token offsets", 131072 * 8)
    question_path = campaign_file(root, report["question_tokens"], "coverage question tokens", 131072 * 8)
    rendered_path = campaign_file(root, report["rendered_prompt"], "coverage rendered prompt", MAX_REPORT)
    receipt = read_json(receipt_path, report["receipt"]["sha256"])
    derived = capture.validate_block_coverage(
        receipt, report["prompt_tokens"]["sha256"],
        provenance={field: origin[field] for field in ("binary_sha256", "source_build_sha256")},
        request_binding={"session_id": nonce, "request_index": index,
                         "question_sha256": request["question_sha256"],
                         "request_content_sha256": request["request_content_sha256"]})
    require(all(derived["loaded_identity"][field + "_sha256"] == identity[field] for field in
                ("artifact", "tokenizer", "template", "layout", "implementation")),
            "coverage loaded identity mismatch")
    prompt_bytes, question_bytes = token_path.read_bytes(), question_path.read_bytes()
    question = derived["question"]
    require(len(prompt_bytes) == receipt["prompt_tokens"] * 8 and
            offsets_path.stat().st_size == receipt["prompt_tokens"] * 8 and
            report["prompt_token_offsets"]["sha256"] == receipt["prompt_token_offsets_sha256"] and
            len(question_bytes) == question["token_count"] * 8 and
            question_bytes == prompt_bytes[question["source_start"] * 8:question["source_end"] * 8] and
            report["question_tokens"]["sha256"] == question["tokens_sha256"] and
            rendered_path.stat().st_size > 0 and
            report["rendered_prompt"]["sha256"] == receipt["rendered_prompt_sha256"] and
            report["admitted_prefill_chunk_rows"] == receipt["admitted_prefill_chunk_rows"] == 64 and
            report["observed_prompt_tokens"] == receipt["prompt_tokens"] and
            receipt["document"] == {"id": request["case_id"],
                                    "sha256": request["document_sha256"], "bucket": request["bucket"]} and
            (request["prompt_tokens"] is None or receipt["prompt_tokens"] == request["prompt_tokens"]) and
            receipt["reserved_output_tokens"] == request["reserved_output_tokens"] and
            (request["prompt_tokens"] is None or
             receipt["qualified_context_tokens"] == request["prompt_tokens"] + request["reserved_output_tokens"]),
            "coverage receipt/request mismatch")
    capture_inputs_path = campaign_file(root, report["capture_inputs"], "numerical capture inputs",
                                        MAX_RAW_NUMERICAL_METADATA_BYTES)
    capture_inputs = read_json(capture_inputs_path, report["capture_inputs"]["sha256"])
    capture.validate_capture_inputs(capture_inputs, root, report["receipt"], report["prompt_tokens"],
                                    report["prompt_token_offsets"], report["question_tokens"],
                                    derived["loaded_identity"],
                                    {field: origin[field] for field in ("binary_sha256", "source_build_sha256")},
                                    receipt["request"], report["diagnostic_only"], report["rendered_prompt"])
    return {"receipt": report["receipt"], "prompt_tokens": report["prompt_tokens"],
            "prompt_token_offsets": report["prompt_token_offsets"],
            "question_tokens": report["question_tokens"],
            "rendered_prompt": report["rendered_prompt"],
            "capture_inputs": report["capture_inputs"],
            "derived": derived}


def collect_coverage_v2(command, identity, origin, plan, plan_root, source_build_receipt,
                        directory, timeout=900, max_rss_mib=8192):
    """Collect genuine prepared-payload coverage through one profile-free driver.

    The parent seals every document, question, build receipt, nonce and request
    index before launch. The child returns only immutable file references; this
    collector reopens, hashes and reconstructs every receipt before accepting it.
    """
    semantic_identity(identity); provenance(origin)
    require(type(command) is list and command and all(isinstance(arg, str) and arg for arg in command),
            "coverage driver must be an argv array")
    binary = Path(command[0]).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK) and sha(binary) == origin["binary_sha256"],
            "coverage driver binary provenance mismatch")
    plan_root = Path(plan_root).resolve()
    plan = json.loads(json.dumps(_coverage_plan_contract(plan, plan_root)))
    source_build_receipt = Path(source_build_receipt).resolve()
    require(source_build_receipt.is_file() and not source_build_receipt.is_symlink() and
            source_build_receipt.stat().st_size <= MAX_REPORT and
            sha(source_build_receipt) == origin["source_build_sha256"],
            "coverage source build receipt mismatch")
    import context_release_build_adapter as release_build
    build = release_build.validate(source_build_receipt, binary)
    require(build["receipt_sha256"] == origin["source_build_sha256"] and
            build["binary_sha256"] == origin["binary_sha256"],
            "coverage full release receipt does not bind driver binary")
    number(timeout, "coverage timeout", .001); number(max_rss_mib, "coverage RSS budget", 1)
    directory = Path(directory).resolve(); directory.mkdir(exist_ok=False)
    nonce = os.urandom(32).hex()
    receipt = {"schema_version": 2, "stage": "raw_runtime_block_coverage", "semantic_identity": identity,
               "provenance": origin, "collector_sha256": sha(__file__), "session_id": nonce,
               "driver_command": [str(binary), *command[1:]], "driver_binary_sha256": origin["binary_sha256"],
               "completed": False, "returncode": None, "serving_qualified": False,
               "diagnostic_only": any(case["prompt_tokens"] is None for case in plan["cases"]),
               "missing_gates": (["sealed_expected_prompt_tokens"] if any(case["prompt_tokens"] is None
                                                                            for case in plan["cases"]) else []),
               "limits": {"max_rss_mib": max_rss_mib, "min_free_percent": 15,
                          "min_disk_free_mib": 4096, "max_swap_growth_bytes": 0,
                          "trial_timeout_seconds": timeout,
                          "child_file_limit_policy": dict(CHILD_FILE_LIMIT_POLICY)}, "coverage_receipts": [],
               "diagnostic_coverage_receipts": []}
    paths = {name: directory / filename for name, filename in
             (("journal", "events.jsonl"), ("stdout", "driver.jsonl"), ("stderr", "driver.stderr"),
              ("plan", "coverage-plan.json"), ("manifest", "coverage-manifest.json"),
              ("source_build_receipt", "source-build-receipt.json"))}
    copied_build = release_build.materialize_bundle(source_build_receipt, directory,
                                                     paths["source_build_receipt"].name)
    require(copied_build["receipt_sha256"] == origin["source_build_sha256"] and
            copied_build["binary_sha256"] == origin["binary_sha256"],
            "copied coverage full release evidence changed")
    copied_cases, manifest_requests = [], []
    for index, case in enumerate(plan["cases"]):
        copied = dict(case)
        for field in ("document", "question"):
            source = (plan_root / case[field + "_file"]).resolve()
            suffix = ".txt"
            destination = directory / f"{field}-{index:06d}-{case[field + '_sha256']}{suffix}"
            with destination.open("xb") as stream: stream.write(source.read_bytes())
            require(sha(destination) == case[field + "_sha256"], "coverage input changed while copying")
            copied[field + "_file"] = destination.name
        manifest_requests.append({"id": case["case_id"],
            "document": {"path": copied["document_file"], "sha256": case["document_sha256"],
                         "bucket": case["bucket"]},
            "question": {"path": copied["question_file"], "sha256": case["question_sha256"]}})
        copied_cases.append(copied)
    plan["cases"] = copied_cases
    write_json(paths["plan"], plan)
    write_json(paths["manifest"], {"schema_version": 2, "session_id": nonce,
                                   "requests": manifest_requests})
    manifest_sha = sha(paths["manifest"])
    started = time.monotonic_ns(); process = None; failure = None; baseline_swap = None; last_sample_ns = None
    disk_guard = DiskSpaceGuard(directory)
    count = 0; journal_count = 0; loaded_generation = None

    def emit(journal, event, **values):
        nonlocal journal_count
        row = {"event": event, "elapsed_ns": time.monotonic_ns() - started, **values}
        raw = (json.dumps(row, allow_nan=False, sort_keys=True) + "\n").encode(); journal_count += 1
        require(len(raw) <= MAX_DRIVER_LINE_BYTES and journal_count <= MAX_JOURNAL_RECORDS and
                journal.tell() + len(raw) <= MAX_JOURNAL_BYTES, "coverage journal exceeds resource bound")
        journal.write(raw); journal.flush(); return row

    def sample(journal):
        nonlocal baseline_swap, last_sample_ns
        free, swap = memory_free_percent(), swapout_bytes(); disk = disk_guard.sample_mib()
        rss = group_rss_mib(process.pid) if process is not None else 0
        row = emit(journal, "resource", free_percent=free, swapout_bytes=swap,
                   disk_free_mib=disk, group_rss_mib=rss)
        if baseline_swap is None: baseline_swap = swap
        require(row["elapsed_ns"] - (last_sample_ns or 0) <= 2_000_000_000,
                "coverage resource sampling gap")
        last_sample_ns = row["elapsed_ns"]
        require(15 <= free <= 100 and disk >= 4096 and rss <= max_rss_mib and swap == baseline_swap,
                "coverage resource guard failed")


    try:
        with paths["journal"].open("xb") as journal, paths["stdout"].open("xb") as stdout, paths["stderr"].open("xb") as stderr:
            emit(journal, "start", elapsed_ns=0, session_id=nonce, manifest_sha256=manifest_sha,
                 source_build_receipt_sha256=origin["source_build_sha256"]); sample(journal)
            driver_env = dict(os.environ)
            # This campaign proves payload preparation before any retention
            # profile exists. A stale caller environment must not inject one.
            driver_env.pop("ANTFLY_CONTEXT_PROFILE", None)
            driver_env.pop("ANTFLY_CONTEXT_PROFILE_SHA256", None)
            policy = plan["policy"]
            driver_env.update(ANTFLY_CONTEXT_SESSION_ID=nonce, ANTFLY_CONTEXT_CAMPAIGN_DIR=str(directory),
                              ANTFLY_CONTEXT_COVERAGE_MANIFEST=paths["manifest"].name,
                              ANTFLY_CONTEXT_COVERAGE_MANIFEST_SHA256=manifest_sha,
                              ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT=paths["source_build_receipt"].name,
                              ANTFLY_CONTEXT_MAX_TOKENS=str(policy["max_context_tokens"]),
                              ANTFLY_CONTEXT_MAX_KV_TOKENS=str(policy["max_kv_tokens"]),
                              ANTFLY_CONTEXT_KEEP_PREFIX_TOKENS=str(policy["keep_prefix_tokens"]),
                              ANTFLY_CONTEXT_KEEP_RECENT_TOKENS=str(policy["keep_recent_tokens"]))
            process = subprocess.Popen(receipt["driver_command"], stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
                                       start_new_session=True, preexec_fn=_limit_qualification_child_files, env=driver_env)
            receipt["driver_pid"] = process.pid
            with paths["stdout"].open("rb") as incoming:
                for index, case in enumerate(copied_cases):
                    require(process.poll() is None, "coverage driver exited between requests")
                    request = {"command": "coverage", "session_id": nonce, "request_index": index,
                               "route": case["route"], "variant": "packed_exact",
                               "prompt_tokens": case["prompt_tokens"],
                               "reserved_output_tokens": case["reserved_output_tokens"],
                               "settings_sha256": _coverage_generation_settings(case["reserved_output_tokens"]),
                               "variant_settings_sha256": _coverage_variant_settings(policy, identity),
                               "case_id": case["case_id"], "document_sha256": case["document_sha256"],
                               "question_sha256": case["question_sha256"],
                               "request_content_sha256": case["request_content_sha256"],
                               "bucket": case["bucket"], "binary_receipt_id": origin["binary_sha256"],
                               "source_receipt_id": origin["source_build_sha256"]}
                    deadline = time.monotonic() + timeout
                    emit(journal, "command", **request)
                    _write_driver_command(process, request, deadline, lambda: sample(journal))
                    raw = bytearray()
                    while True:
                        sample(journal); raw.extend(incoming.readline(MAX_DRIVER_LINE_BYTES + 1 - len(raw)))
                        require(len(raw) <= MAX_DRIVER_LINE_BYTES, "coverage driver report exceeds byte limit")
                        require(time.monotonic() <= deadline, "coverage request timed out")
                        if raw.endswith(b"\n"): break
                        require(process.poll() is None, "coverage driver failed before reporting")
                        time.sleep(.25)
                    report_path = directory / f"trial-{index:06d}.json"
                    with report_path.open("xb") as stream:
                        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
                    report = decode_json(raw)
                    audited = _coverage_report_contract(report, directory, request, identity, origin, nonce, index)
                    current_generation = audited["derived"]["loaded_identity"]["load_generation"]
                    if loaded_generation is None:
                        loaded_generation = current_generation
                    require(current_generation == loaded_generation,
                            "coverage loaded generation changed within campaign")
                    evidence_key = ("diagnostic_coverage_receipts" if case["prompt_tokens"] is None
                                    else "coverage_receipts")
                    receipt[evidence_key].append({key: audited[key] for key in
                                                  ("receipt", "prompt_tokens", "question_tokens",
                                                   "rendered_prompt", "capture_inputs")})
                    receipt.setdefault("observed_prompt_tokens", []).append({"case_id": case["case_id"],
                        "prompt_tokens": audited["derived"]["prompt_tokens"],
                        "qualified_context_tokens": audited["derived"]["qualified_context_tokens"]})
                    emit(journal, "report", request_index=index,
                         file={"path": report_path.name, "sha256": sha(report_path)},
                         receipt=audited["receipt"], prompt_tokens=audited["prompt_tokens"],
                         question_tokens=audited["question_tokens"],
                         rendered_prompt=audited["rendered_prompt"], capture_inputs=audited["capture_inputs"])
                    os.fsync(journal.fileno()); count += 1
                sample(journal)
                _write_driver_command(process, {"command": "stop", "session_id": nonce},
                                      time.monotonic() + timeout, lambda: sample(journal))
                emit(journal, "stop")
                process.stdin.close(); receipt["returncode"] = process.wait(timeout=5)
                remaining_rss = group_rss_mib(process.pid)
                emit(journal, "shutdown", returncode=receipt["returncode"], remaining_group_rss_mib=remaining_rss)
                os.fsync(journal.fileno())
                require(receipt["returncode"] == 0 and remaining_rss == 0 and incoming.read(1) == b"",
                        "coverage driver failed shutdown or emitted unsolicited reports")
                require(sha(binary) == origin["binary_sha256"], "coverage driver binary changed")
                for entry in receipt["coverage_receipts"] + receipt["diagnostic_coverage_receipts"]:
                    campaign_file(directory, entry["receipt"], "coverage receipt",
                                  MAX_RAW_NUMERICAL_METADATA_BYTES)
                    campaign_file(directory, entry["prompt_tokens"], "coverage prompt tokens", 131072 * 8)
                    campaign_file(directory, entry["question_tokens"], "coverage question tokens", 131072 * 8)
                    campaign_file(directory, entry["rendered_prompt"], "coverage rendered prompt", MAX_REPORT)
                    campaign_file(directory, entry["capture_inputs"], "numerical capture inputs",
                                  MAX_RAW_NUMERICAL_METADATA_BYTES)
                receipt["completed"] = True
    except BaseException as error:
        failure = error; receipt["failure"] = str(error)
    finally:
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                try: process.stdin.close()
                except (BrokenPipeError, OSError): pass
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try: os.killpg(process.pid, sig)
                except ProcessLookupError: break
                if sig == signal.SIGTERM:
                    try: process.wait(timeout=2)
                    except subprocess.TimeoutExpired: pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired as error:
                receipt["termination_failure"] = "coverage driver leader survived SIGKILL deadline"
                if failure is None: failure = error
            if receipt["returncode"] is None: receipt["returncode"] = process.returncode
        receipt["elapsed_ns"] = time.monotonic_ns() - started; receipt["trial_count"] = count
        receipt["files"] = {key: {"path": path.name, "sha256": sha(path)}
                            for key, path in paths.items() if path.is_file()}
        receipt["partial_coverage_files"] = [
            {"path": path.name, "sha256": sha(path), "size": path.stat().st_size}
            for pattern in ("coverage-receipt-*.json", "coverage-prompt-*.i64", "coverage-rendered-prompt-*.txt", "coverage-question-*.i64",
                            "coverage-capture-inputs-*.json", "coverage-fit-*.i64", "coverage-gate-*.i64")
            for path in sorted(directory.glob(pattern)) if path.is_file() and not path.is_symlink()
        ]
        write_json(directory / "session.json", receipt)
    if failure is not None: raise failure
    return receipt


def _collect_driver_v2(command, identity, origin, directory, duration, timeout, max_rss_mib, failures,
                       measurement_plan=None, quality_plan=None, quality_root=None,
                       failure_plan=None, failure_root=None,
                       route_plan=None, route_root=None, cli_binary=None,
                       cli_source_build_receipt=None):
    semantic_identity(identity)
    provenance(origin)
    require(type(command) is list and command and all(isinstance(arg, str) and arg for arg in command),
            "driver must be an argv array")
    binary = Path(command[0]).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK) and sha(binary) == origin["binary_sha256"],
            "soak driver binary provenance mismatch")
    measurements, quality = measurement_plan is not None, quality_plan is not None
    failure_planned, routes = failure_plan is not None, route_plan is not None
    planned = measurements or quality or failure_planned or routes
    require(failures == failure_planned and sum((measurements, quality, failures, routes)) <= 1,
            "conflicting or unplanned raw driver mode")
    number(duration, "duration", 0 if failures or planned else .001)
    number(timeout, "trial timeout", .001)
    number(max_rss_mib, "RSS budget", 1)
    directory = Path(directory).resolve()
    directory.mkdir(exist_ok=False)
    command = [str(binary), *command[1:]]
    nonce = os.urandom(32).hex()
    receipt = {"schema_version": 2, "stage": "raw_paired_quality" if quality else "raw_paired_performance" if measurements else "raw_failure_injection" if failures else "raw_serving_soak", "semantic_identity": identity,
               "provenance": origin, "collector_sha256": sha(__file__), "session_id": nonce,
               "driver_command": command, "driver_binary_sha256": origin["binary_sha256"],
               "completed": False, "returncode": None, "serving_qualified": False,
               "limits": {"max_rss_mib": max_rss_mib, "min_free_percent": 15,
                          "min_disk_free_mib": 4096, "max_swap_growth_bytes": 0,
                          "trial_timeout_seconds": timeout,
                          "child_file_limit_policy": dict(CHILD_FILE_LIMIT_POLICY)}}
    paths = {key: directory / name for key, name in
             (("journal", "events.jsonl"), ("stdout", "driver.jsonl"), ("stderr", "driver.stderr"))}
    requests = None
    if routes:
        import context_serving_routes_qualify as contract
        plan = json.loads(json.dumps(contract.plan_contract(route_plan, route_root)))
        require(plan["runtime"]["implementation_sha256"] == identity["implementation"] and
                plan["runtime"]["profile_artifact_sha256"] == identity["profile"],
                "serving route runtime identity mismatch")
        executable = Path(cli_binary).resolve() if cli_binary is not None else None
        require(executable is not None and executable.is_file() and os.access(executable, os.X_OK) and
                sha(executable) == plan["cli_binary_sha256"],
                "CLI binary provenance mismatch")
        cli_source_build_receipt = Path(cli_source_build_receipt).resolve() if cli_source_build_receipt is not None else None
        require(cli_source_build_receipt is not None and cli_source_build_receipt.is_file() and
                not cli_source_build_receipt.is_symlink() and cli_source_build_receipt.stat().st_size <= MAX_REPORT and
                sha(cli_source_build_receipt) == plan["cli_source_build_sha256"],
                "CLI source build receipt mismatch")
        cli_build_evidence_contract(cli_source_build_receipt, plan["cli_source_build_sha256"],
                                    plan["cli_binary_sha256"], executable)
        for request in plan["requests"]:
            entries = request["siblings"] if request["route"] == "serial_mixed_batch" else (request,)
            for entry in entries:
                for field in ("document", "question"):
                    source = Path(route_root) / entry[field + "_file"]
                    expected = entry[field + "_sha256"]
                    require(source.is_file() and not source.is_symlink() and
                            source.stat().st_size <= MAX_REPORT and sha(source) == expected,
                            "serving route input identity mismatch")
                    destination = directory / (field + "-" + expected + ".txt")
                    if not destination.exists():
                        with destination.open("xb") as stream:
                            stream.write(source.read_bytes())
                    require(sha(destination) == expected, "serving route input changed while copying")
                    entry[field + "_file"] = destination.name
        plan = contract.plan_contract(plan, directory)
        requests = contract.requests(plan)
        paths["plan"] = directory / "request-plan.json"
        write_json(paths["plan"], plan)
        paths["cli_source_build_receipt"] = directory / "cli-source-build-receipt.json"
        with paths["cli_source_build_receipt"].open("xb") as stream:
            stream.write(cli_source_build_receipt.read_bytes())
        require(sha(paths["cli_source_build_receipt"]) == plan["cli_source_build_sha256"],
                "CLI source build receipt changed while copying")
        receipt["route_contract_sha256"] = sha(contract.__file__)
        receipt["cli_binary_sha256"] = plan["cli_binary_sha256"]
        cli_binary = executable
    elif failure_planned:
        plan = json.loads(json.dumps(failure_plan))
        requests = failure_plan_contract(plan, failure_root, identity)
        # Preserve the exact sealed inputs before starting the persistent owner.
        for field in ("document", "question"):
            source = Path(failure_root) / plan["request"][field + "_file"]
            expected = plan["request"][field + "_sha256"]
            destination = directory / (field + "-" + expected + ".txt")
            with destination.open("xb") as stream:
                stream.write(source.read_bytes())
            require(sha(destination) == expected, "failure input changed while copying")
            plan["request"][field + "_file"] = destination.name
        requests = failure_plan_contract(plan, directory, identity)
        paths["plan"] = directory / "request-plan.json"
        write_json(paths["plan"], plan)
        receipt["measurement_contract_sha256"] = sha(__file__)
    elif planned:
        if quality:
            import quality_context_qualify as contract
            plan = json.loads(json.dumps(contract.plan_contract(quality_plan, quality_root)))
            entries = plan["cases"]
            fields = None
        else:
            import paired_context_qualify as contract
            plan = json.loads(json.dumps(contract.plan_contract(measurement_plan)))
            entries, fields = plan["requests"], ("document", "question", "prompt")
        # Preserve every predeclared input before starting the persistent owner.
        for request in entries:
            request_fields = (("messages", "prompt", "reference", "reference_tokens")
                              if quality and "messages_file" in request else
                              ("document", "question", "prompt", "reference", "reference_tokens")
                              if quality else fields)
            for field in request_fields:
                source = Path(quality_root) / request[field + "_file"] if quality else Path(request[field + "_file"])
                expected = request[field + "_sha256"]
                require(source.is_file() and source.stat().st_size <= MAX_REPORT and sha(source) == expected,
                        "planned input file identity mismatch")
                destination = directory / (field + "-" + expected +
                    (".json" if field in ("reference_tokens", "messages") else ".txt"))
                if not destination.exists():
                    with destination.open("xb") as stream:
                        stream.write(source.read_bytes())
                require(sha(destination) == expected, "planned input changed while copying")
                request[field + "_file"] = destination.name
        if quality:
            contract.plan_contract(plan, directory)
        else:
            runtime = plan["runtime"]
            source = Path(runtime["profile_file"])
            require(source.is_file() and not source.is_symlink() and source.stat().st_size <= MAX_REPORT and
                    sha(source) == runtime["profile_sha256"], "paired frozen profile identity mismatch")
            destination = directory / ("profile-" + runtime["profile_sha256"] + ".json")
            with destination.open("xb") as stream:
                stream.write(source.read_bytes())
            runtime["profile_file"] = destination.name
            contract.plan_contract(plan, directory, identity)
        paths["plan"] = directory / "request-plan.json"
        write_json(paths["plan"], plan)
        receipt["measurement_contract_sha256"] = sha(contract.__file__)
        requests = contract.requests(plan)
    started = time.monotonic_ns()
    process, failure = None, None
    baseline_swap, baseline_live, generation = None, None, None
    disk_guard = DiskSpaceGuard(directory)
    count, journal_count = 0, 0
    last_sample_ns = None

    def emit(journal, event, **values):
        nonlocal journal_count
        record = {"event": event, "elapsed_ns": time.monotonic_ns() - started, **values}
        raw = (json.dumps(record, allow_nan=False, sort_keys=True) + "\n").encode()
        journal_count += 1
        require(len(raw) <= MAX_DRIVER_LINE_BYTES and journal_count <= MAX_JOURNAL_RECORDS and
                journal.tell() + len(raw) <= MAX_JOURNAL_BYTES, "soak journal exceeds resource bound")
        journal.write(raw)
        journal.flush()
        return record

    def sample(journal):
        nonlocal baseline_swap, last_sample_ns
        free, swap = memory_free_percent(), swapout_bytes()
        disk = disk_guard.sample_mib()
        rss = group_rss_mib(process.pid) if process is not None else 0
        event = emit(journal, "resource", free_percent=free, swapout_bytes=swap,
                     disk_free_mib=disk, group_rss_mib=rss)
        if baseline_swap is None:
            baseline_swap = swap
        require(event["elapsed_ns"] - (last_sample_ns or 0) <= 2_000_000_000,
                "soak resource sampling gap")
        last_sample_ns = event["elapsed_ns"]
        require(free >= 15 and free <= 100 and disk >= 4096 and rss <= max_rss_mib and
                swap == baseline_swap, "soak resource guard failed")


    try:
        with paths["journal"].open("xb") as journal, paths["stdout"].open("xb") as stdout, paths["stderr"].open("xb") as stderr:
            emit(journal, "start", elapsed_ns=0, session_id=nonce)
            sample(journal)
            # The persistent owner must validate the parent's nonce and open
            # sealed inputs relative to this exact campaign directory. Inherit
            # model/build configuration, but never a stale session binding.
            driver_env = dict(os.environ)
            driver_env.update(ANTFLY_CONTEXT_SESSION_ID=nonce,
                              ANTFLY_CONTEXT_CAMPAIGN_DIR=str(directory),
                              ANTFLY_CONTEXT_SOURCE_BUILD_SHA256=origin["source_build_sha256"],
                              ANTFLY_CONTEXT_DEVICE_SHA256=origin["device_sha256"],
                              ANTFLY_CONTEXT_OS_SHA256=origin["os_sha256"])
            if routes:
                driver_env.update(ANTFLY_CONTEXT_CLI_BINARY=str(cli_binary),
                                  ANTFLY_CONTEXT_CLI_BINARY_SHA256=plan["cli_binary_sha256"])
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
                                       start_new_session=True, preexec_fn=_limit_qualification_child_files, env=driver_env)
            receipt["driver_pid"] = process.pid
            with paths["stdout"].open("rb") as incoming:
                while ((count < len(requests) or time.monotonic_ns() - started < duration * 1e9) if routes else
                       (count < len(requests)) if planned else
                       (count < len(FAILURES)) if failures else
                       (count < len(ROUTES) or time.monotonic_ns() - started < duration * 1e9)):
                    require(count < 4096, "soak trial count exceeds bound; use longer serving requests")
                    require(process.poll() is None, "soak driver exited between requests")
                    route = (requests[count % len(requests)]["route"] if routes else
                             requests[count]["route"] if planned else sorted(ROUTES)[count % len(ROUTES)])
                    fault_fields = {"fault": FAILURE_ORDER[count]} if failures else {}
                    request_fields = (dict(requests[count % len(requests)]) if routes else
                                      dict(requests[count]) if planned else {"route": route, **fault_fields})
                    deadline = time.monotonic() + timeout
                    command_event = emit(journal, "command", session_id=nonce, request_index=count,
                                         **request_fields)
                    driver_fields = dict(request_fields)
                    # Preserve relative sealed names on the wire. The native
                    # driver rejects absolute paths and traverses no-follow
                    # beneath its already-open campaign directory.
                    _write_driver_command(process,
                        {"command": "quality" if quality else "measure" if measurements else "fault" if failures else "next",
                         "session_id": nonce, "request_index": count, **driver_fields},
                        deadline, lambda: sample(journal))
                    raw = bytearray()
                    while True:
                        sample(journal)
                        raw.extend(incoming.readline(MAX_DRIVER_LINE_BYTES + 1 - len(raw)))
                        require(len(raw) <= MAX_DRIVER_LINE_BYTES, "driver report exceeds byte limit")
                        require(time.monotonic() <= deadline, "soak trial timed out")
                        if raw.endswith(b"\n"):
                            break
                        require(process.poll() is None, "soak driver failed before reporting")
                        time.sleep(.25)
                    report_path = directory / f"trial-{count:06d}.json"
                    with report_path.open("xb") as stream:
                        stream.write(raw)
                        stream.flush()
                        os.fsync(stream.fileno())
                    report = decode_json(raw)
                    if routes:
                        audited = contract.report_contract(
                            report, request_fields, identity, origin, plan["runtime"],
                            plan["cli_binary_sha256"], directory, sys.modules[__name__])
                    elif quality:
                        contract.driver_report_contract(report, identity, origin, request_fields, directory)
                    elif measurements:
                        contract.report_contract(report, identity, origin, request_fields)
                    elif failures:
                        failure_report_contract(report, identity, origin, request_fields)
                    else:
                        report_contract_v2(report, identity, origin)
                    if routes:
                        require(audited["elapsed_ns"] <= time.monotonic_ns() - started - command_event["elapsed_ns"],
                                "serving route child timing exceeds parent duration")
                    require(report.get("session_id") == nonce and type(report.get("request_index")) is int and
                            report["request_index"] == count and report.get("route") == route,
                            "soak driver reply/command mismatch")
                    if failures:
                        require(report["fault"] == fault_fields["fault"], "failure driver reply/command mismatch")
                    current_generation = (audited["load_generation"] if routes else
                                          loaded_serving_identity(report, identity))
                    if generation is None:
                        generation = current_generation
                        baseline_live = (audited["baseline_live_bytes"] if routes else
                                         report["live_bytes_before"])
                    if failures:
                        if report["fault"] == "reload":
                            require(report["loaded_generation_before"] == generation and
                                    current_generation == report["loaded_generation_after"],
                                    "reload did not transition the persistent loaded generation")
                        else:
                            require(current_generation == generation,
                                    "fault request changed the persistent loaded generation")
                        require(report["live_bytes_before"] == baseline_live,
                                "fault request changed the live allocation baseline")
                        generation = report["loaded_generation_after"]
                    else:
                        require(current_generation == generation, "soak generation changed")
                        if routes:
                            route_baseline = audited["baseline_live_bytes"]
                            if route_baseline is not None:
                                if baseline_live is None:
                                    baseline_live = route_baseline
                                require(route_baseline == baseline_live,
                                        "soak live allocation baseline changed")
                        else:
                            require(report["live_bytes_before"] == baseline_live,
                                    "soak live allocation baseline changed")
                    emit(journal, "report", request_index=count,
                         file={"path": report_path.name, "sha256": sha(report_path)})
                    os.fsync(journal.fileno())
                    count += 1
                sample(journal)
                _write_driver_command(process, {"command": "stop"},
                                      time.monotonic() + timeout, lambda: sample(journal))
                emit(journal, "stop")
                process.stdin.close()
                receipt["returncode"] = process.wait(timeout=5)
                remaining_rss = group_rss_mib(process.pid)
                emit(journal, "shutdown", returncode=receipt["returncode"], remaining_group_rss_mib=remaining_rss)
                os.fsync(journal.fileno())
                require(receipt["returncode"] == 0 and remaining_rss == 0,
                        "soak driver failed during shutdown or left live descendants")
                require(incoming.read(1) == b"", "driver emitted unsolicited reports")
                require(sha(binary) == origin["binary_sha256"], "soak driver binary changed during collection")
                receipt["completed"] = True
    except BaseException as error:
        failure = error
        receipt["failure"] = str(error)
    finally:
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except (BrokenPipeError, OSError):
                    pass
            # A completed leader may still have descendants owning the model.
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, sig)
                except ProcessLookupError:
                    break
                if sig == signal.SIGTERM:
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired as error:
                receipt["termination_failure"] = "driver leader survived SIGKILL deadline"
                if failure is None:
                    failure = error
            if receipt["returncode"] is None:
                receipt["returncode"] = process.returncode
        receipt["elapsed_ns"] = time.monotonic_ns() - started
        receipt["trial_count"] = count
        receipt["files"] = {key: {"path": path.name, "sha256": sha(path)}
                            for key, path in paths.items() if path.is_file()}
        write_json(directory / "session.json", receipt)
    if failure is not None:
        raise failure
    return receipt


def soak(command, expected, directory, duration=14400, timeout=300, max_rss_mib=8192):
    """One persistent driver owns the server and handles next/stop JSON commands.

    A next command produces exactly one JSON report line after request release.
    The driver must keep its server/model alive between commands. stderr is for
    logs. No shell, no restart between trials, and no cumulative checkpoint copies.
    """
    binding(expected)
    require(isinstance(command, list) and command and all(isinstance(x, str) and x for x in command), "driver must be an argv array")
    number(duration, "duration", .001)
    number(timeout, "trial timeout", .001)
    number(max_rss_mib, "RSS budget", 1)
    directory = Path(directory)
    directory.mkdir(exist_ok=False)
    started = time.monotonic()
    receipt = {"schema_version": 1, "identity": expected, "completed": False, "duration_seconds": 0,
               "trials": [], "serving_qualified": False, "max_rss_mib": max_rss_mib, "persistent_driver": True,
               "child_file_limit_policy": dict(CHILD_FILE_LIMIT_POLICY)}
    failure = None
    process = None
    baseline_swap = None
    baseline_live = None
    disk_guard = DiskSpaceGuard(directory)
    output_path = directory / "driver.jsonl"
    stderr_path = directory / "driver.stderr"
    def sample(resources):
        nonlocal baseline_swap
        free = memory_free_percent()
        swap = swapout_bytes()
        disk = disk_guard.sample_mib()
        rss = group_rss_mib(process.pid if process is not None else os.getpgrp())
        if baseline_swap is None:
            baseline_swap = swap
        resources["min_free_percent"] = min(resources.get("min_free_percent", 100), free)
        resources["min_disk_free_mib"] = min(resources.get("min_disk_free_mib", disk), disk)
        resources["swapout_growth_bytes"] = max(swap - baseline_swap, 0)
        resources["peak_group_rss_mib"] = max(resources.get("peak_group_rss_mib", 0), rss)
        require(free >= 15 and disk >= 4096 and rss <= max_rss_mib and swap == baseline_swap, "soak resource guard failed")
    try:
        sample({})
        with output_path.open("xb") as stdout, stderr_path.open("xb") as stderr, (directory / "trials.jsonl").open("x") as journal:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
                                       start_new_session=True, preexec_fn=_limit_qualification_child_files)
            receipt["driver_pid"] = process.pid
            with output_path.open("rb") as incoming:
                while time.monotonic() - started < duration:
                    require(len(receipt["trials"]) < 4096, "soak trial count exceeds bounded receipt; use longer serving trials")
                    start = time.monotonic() - started
                    require(process.poll() is None, "soak driver exited between requests")
                    process.stdin.write((json.dumps({"command": "next", "request_index": len(receipt["trials"])}) + "\n").encode())
                    process.stdin.flush()
                    deadline = time.monotonic() + timeout
                    raw = bytearray()
                    resources = {"pass": False}
                    while True:
                        part = incoming.readline(65537 - len(raw))
                        raw.extend(part)
                        require(len(raw) <= 65536, "driver report exceeds byte limit")
                        if raw.endswith(b"\n"):
                            break
                        require(process.poll() is None, "soak driver failed before reporting")
                        require(time.monotonic() < deadline, "soak trial timed out")
                        sample(resources)
                        time.sleep(.05)
                    sample(resources)
                    resources["pass"] = True
                    report_path = directory / f"trial-{len(receipt['trials']):06d}.json"
                    with report_path.open("xb") as stream:
                        stream.write(raw)
                    report = report_contract(read_json(report_path), expected)
                    require(type(report.get("request_index")) is int and report["request_index"] == len(receipt["trials"]), "driver report sequence mismatch")
                    if baseline_live is None:
                        baseline_live = report["live_bytes_before"]
                    require(report["live_bytes_before"] == baseline_live, "live allocation baseline grew across requests")
                    # Keep a bounded protocol summary, not arbitrary output text.
                    fields = {"schema_version", "identity", "published", "exact_fallback", "committed_before_bytes",
                              "committed_after_bytes", "backend_kv_readback_bytes", "swapout_growth_bytes", "memory_guard_passed",
                              "isolation_passed", "rollback_passed", "live_bytes_before", "live_bytes_after_release", "route", "prompt_tokens", "request_index"}
                    summary = {key: report[key] for key in fields}
                    trial = {"start_seconds": start, "end_seconds": time.monotonic() - started,
                             "report_sha256": sha(report_path), "report": summary, "resources": resources}
                    receipt["trials"].append(trial)
                    journal.write(json.dumps(trial, allow_nan=False, sort_keys=True) + "\n")
                    journal.flush()
                    os.fsync(journal.fileno())
                process.stdin.write(b'{"command":"stop"}\n')
                process.stdin.flush()
                process.stdin.close()
                require(process.wait(timeout=5) == 0, "soak driver failed during shutdown")
                require(incoming.read(1) == b"", "driver emitted unsolicited reports")
                receipt["completed"] = True
    except BaseException as error:
        failure = error
        receipt["failure"] = str(error)
    finally:
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except (BrokenPipeError, OSError):
                    pass
            # Include descendants even when the driver leader has exited.
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, sig)
                except ProcessLookupError:
                    break
                if sig == signal.SIGTERM:
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        pass
            process.wait()
        receipt["duration_seconds"] = time.monotonic() - started
        write_json(directory / "soak.json", receipt)
    if failure is not None:
        raise failure
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("audit")
    check.add_argument("manifest", type=Path)
    check.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("soak")
    run.add_argument("--driver", type=Path, required=True, help="JSON argv array, no shell")
    run.add_argument("--identity", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    run.add_argument("--duration-seconds", type=float, default=14400)
    run.add_argument("--trial-timeout-seconds", type=float, default=300)
    run.add_argument("--max-rss-mib", type=float, default=8192)
    raw = commands.add_parser("soak-v2")
    raw.add_argument("--driver", type=Path, required=True, help="JSON argv array; first entry must be bound native binary")
    raw.add_argument("--identity", type=Path, required=True, help="Semantic identity JSON")
    raw.add_argument("--provenance", type=Path, required=True)
    raw.add_argument("--output-dir", type=Path, required=True)
    raw.add_argument("--duration-seconds", type=float, default=14400)
    raw.add_argument("--trial-timeout-seconds", type=float, default=300)
    raw.add_argument("--max-rss-mib", type=float, default=8192)
    routes = commands.add_parser("routes-v2")
    routes.add_argument("--driver", type=Path, required=True,
                        help="JSON argv array; first entry must be bound native binary")
    routes.add_argument("--identity", type=Path, required=True)
    routes.add_argument("--provenance", type=Path, required=True)
    routes.add_argument("--plan", type=Path, required=True,
                        help="Sealed five-route schedule with structured requests")
    routes.add_argument("--cli-binary", type=Path, required=True,
                        help="Exact production CLI executable named by the sealed plan")
    routes.add_argument("--cli-source-build-receipt", type=Path, required=True,
                        help="Build receipt whose exact bytes bind the CLI executable")
    routes.add_argument("--output-dir", type=Path, required=True)
    routes.add_argument("--duration-seconds", type=float, default=14400)
    routes.add_argument("--trial-timeout-seconds", type=float, default=300)
    routes.add_argument("--max-rss-mib", type=float, default=8192)
    loaded = commands.add_parser("provenance-v2")
    loaded.add_argument("--driver", type=Path, required=True)
    loaded.add_argument("--identity", type=Path, required=True)
    loaded.add_argument("--provenance", type=Path, required=True)
    loaded.add_argument("--profile", type=Path, required=True)
    loaded.add_argument("--output-dir", type=Path, required=True)
    loaded.add_argument("--timeout-seconds", type=float, default=900)
    faults = commands.add_parser("failures-v2")
    faults.add_argument("--driver", type=Path, required=True, help="JSON argv array; first entry must be bound native binary")
    faults.add_argument("--identity", type=Path, required=True)
    faults.add_argument("--provenance", type=Path, required=True)
    faults.add_argument("--plan", type=Path, required=True,
                        help="Sealed structured request and canonical fault order")
    faults.add_argument("--output-dir", type=Path, required=True)
    faults.add_argument("--trial-timeout-seconds", type=float, default=300)
    faults.add_argument("--max-rss-mib", type=float, default=8192)
    performance = commands.add_parser("performance-v2")
    performance.add_argument("--driver", type=Path, required=True)
    performance.add_argument("--identity", type=Path, required=True)
    performance.add_argument("--provenance", type=Path, required=True)
    performance.add_argument("--plan", type=Path, required=True)
    performance.add_argument("--output-dir", type=Path, required=True)
    performance.add_argument("--trial-timeout-seconds", type=float, default=900)
    performance.add_argument("--max-rss-mib", type=float, default=8192)
    quality = commands.add_parser("quality-v2")
    quality.add_argument("--driver", type=Path, required=True)
    quality.add_argument("--identity", type=Path, required=True)
    quality.add_argument("--provenance", type=Path, required=True)
    quality.add_argument("--plan", type=Path, required=True)
    quality.add_argument("--output-dir", type=Path, required=True)
    quality.add_argument("--trial-timeout-seconds", type=float, default=900)
    quality.add_argument("--max-rss-mib", type=float, default=8192)
    coverage = commands.add_parser("coverage-v2")
    coverage.add_argument("--driver", type=Path, required=True)
    coverage.add_argument("--identity", type=Path, required=True)
    coverage.add_argument("--provenance", type=Path, required=True)
    coverage.add_argument("--plan", type=Path, required=True)
    coverage.add_argument("--source-build-receipt", type=Path, required=True)
    coverage.add_argument("--output-dir", type=Path, required=True)
    coverage.add_argument("--trial-timeout-seconds", type=float, default=900)
    coverage.add_argument("--max-rss-mib", type=float, default=8192)
    args = parser.parse_args()
    if args.command == "audit":
        result = audit(read_json(args.manifest), args.manifest.parent)
        result["manifest_sha256"] = sha(args.manifest)
        result["auditor_sha256"] = sha(__file__)
        write_json(args.output, result)
    elif args.command == "soak-v2":
        collect_soak_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance),
                        args.output_dir, args.duration_seconds, args.trial_timeout_seconds, args.max_rss_mib)
    elif args.command == "routes-v2":
        collect_route_soak_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance),
                              read_json(args.plan), args.plan.parent, args.cli_binary,
                              args.cli_source_build_receipt, args.output_dir,
                              args.duration_seconds, args.trial_timeout_seconds, args.max_rss_mib)
    elif args.command == "provenance-v2":
        collect_loaded_provenance_v2(read_json(args.driver), read_json(args.identity),
                                     read_json(args.provenance), args.profile,
                                     args.output_dir, args.timeout_seconds)
    elif args.command == "failures-v2":
        collect_failures_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance),
                           read_json(args.plan), args.plan.parent, args.output_dir,
                           args.trial_timeout_seconds, args.max_rss_mib)
    elif args.command == "quality-v2":
        collect_quality_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance),
                          read_json(args.plan), args.plan.parent, args.output_dir,
                          args.trial_timeout_seconds, args.max_rss_mib)
    elif args.command == "coverage-v2":
        collect_coverage_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance),
                            read_json(args.plan), args.plan.parent, args.source_build_receipt,
                            args.output_dir, args.trial_timeout_seconds, args.max_rss_mib)
    elif args.command == "performance-v2":
        plan = read_json(args.plan)
        for request in plan.get("requests", []):
            request["prompt_file"] = str((args.plan.parent / request["prompt_file"]).resolve())
        collect_performance_v2(read_json(args.driver), read_json(args.identity), read_json(args.provenance), plan,
                              args.output_dir, args.trial_timeout_seconds, args.max_rss_mib)
    else:
        soak(read_json(args.driver), read_json(args.identity), args.output_dir, args.duration_seconds, args.trial_timeout_seconds, args.max_rss_mib)


if __name__ == "__main__":
    main()
