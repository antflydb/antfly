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

"""Seal and run the unfiltered, serial Gemma context release build.

The source archive is created before the compiler starts.  A successful run is
sealed only after the complete source snapshot is unchanged and the executable,
resource receipt, logs, and exact test summary have been bound.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import zipfile

SCHEMA_V1 = "antfly.context-release-build.v1"
SCHEMA_V2 = "antfly.context-release-build.v2"
SCHEMA_V3 = "antfly.context-release-build.v3"
SCHEMA = SCHEMA_V1
SCOPE = "full_context_release_build"
MANIFEST = "FULL-RELEASE-SOURCE.json"
SOURCE_NOTE = "Complete Git-visible Zig tree inputs are archived; deleted binds tracked removals; Git-ignored caches and outputs are excluded; gitlinks bind clean or uninitialized submodules."
BASE_COMMAND = [
    "/usr/bin/env", "ANTFLY_ZIG_MAX_RSS=8589934592", "python3", "tools/run_bounded_zig_build.py",
    "build", "inference-test-context", "--verbose", "-j1", "-Dmetal=true", "-Dcuda=false",
    "-Donnx=false", "-Doptimize=ReleaseFast",
]
CURRENT_MAIN_BASE_COMMAND = [argument for argument in BASE_COMMAND if argument != "-Donnx=false"]
CLEARED_ENV_PREFIXES = ["ANTFLY_", "TERMITE_", "MTL_", "METAL_", "CUDA_", "GGML_", "ZIG_"]
ENVIRONMENT = {"inherit_non_runtime_environment": True, "cleared_prefixes": CLEARED_ENV_PREFIXES,
               "set_by_command": {"ANTFLY_ZIG_MAX_RSS": "8589934592"}}
COMMAND = list(BASE_COMMAND)
BUILD = {"cwd": "zig", "command": COMMAND, "target": "inference-test-context", "environment": ENVIRONMENT,
         "optimize": "ReleaseFast", "metal": True, "cuda": False, "onnx": False}
LIMITS = {"timeout": 3600, "max_rss_mib": 8192, "min_free_percent": 15,
          "max_swap_growth_mib": 512, "min_disk_free_mib": 4096}
MAX_FILES = 8192
MAX_UNCOMPRESSED = 768 * 1024 * 1024
EXPECTED_STRIPPED_COMPILER_PROFILE = {"strip_debug_info": True, "have_error_return_tracing": False}
COMPILER_PROFILE_PREFIX = "context_compiler_profile: "

def configure_profile(stripped: bool) -> None:
    global SCHEMA, COMMAND, BUILD
    # V1/V2 remain the exact historical contracts. Current main no longer has
    # an `onnx` build option, so new stripped evidence uses V3 rather than
    # silently changing the command accepted by historical V2 receipts.
    SCHEMA = SCHEMA_V3 if stripped else SCHEMA_V1
    COMMAND = [*CURRENT_MAIN_BASE_COMMAND, "-Dstrip=true"] if stripped else list(BASE_COMMAND)
    BUILD = {"cwd": "zig", "command": COMMAND, "target": "inference-test-context",
             "environment": ENVIRONMENT, "optimize": "ReleaseFast", "metal": True,
             "cuda": False}
    if not stripped:
        BUILD["onnx"] = False

def parse_compiler_profile(stderr: str) -> dict:
    rows = [line[len(COMPILER_PROFILE_PREFIX):] for line in stderr.splitlines()
            if line.startswith(COMPILER_PROFILE_PREFIX)]
    if len(rows) != 1:
        fail("stripped full release requires exactly one compiler profile observation")
    try:
        value = json.loads(rows[0])
    except json.JSONDecodeError as error:
        raise RuntimeError("invalid compiler profile observation") from error
    if (type(value) is not dict or set(value) != {"schema_version", "strip_debug_info",
                                                  "have_error_return_tracing"} or
            type(value.get("schema_version")) is not int or
            isinstance(value["schema_version"], bool) or value["schema_version"] != 1 or
            type(value.get("strip_debug_info")) is not bool or
            type(value.get("have_error_return_tracing")) is not bool or
            {key: value[key] for key in EXPECTED_STRIPPED_COMPILER_PROFILE} !=
            EXPECTED_STRIPPED_COMPILER_PROFILE):
        fail("stripped compiler profile does not match the supported release profile")
    return {key: value[key] for key in EXPECTED_STRIPPED_COMPILER_PROFILE}


def fail(message: str) -> None:
    raise RuntimeError(message)


def digest_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def tool_identity() -> dict:
    result = {}
    for name in ("python3", "zig"):
        found = shutil.which(name)
        if not found:
            fail("missing build tool: " + name)
        path = Path(found).resolve(strict=True)
        if not path.is_file() or not os.access(path, os.X_OK):
            fail("build tool is not executable: " + name)
        version_command = [str(path), "--version"] if name == "python3" else [str(path), "version"]
        version = subprocess.check_output(version_command, text=True, stderr=subprocess.STDOUT).strip()
        result[name] = {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size,
                        "version": version}
    return result


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=root)


@contextlib.contextmanager
def sanitized_environment():
    removed = {name: value for name, value in os.environ.items()
               if any(name.startswith(prefix) for prefix in CLEARED_ENV_PREFIXES)}
    try:
        for name in removed:
            os.environ.pop(name, None)
        yield
    finally:
        os.environ.update(removed)


def source_name(name: str) -> bool:
    path = Path(name)
    return (name.startswith("zig/") and not path.is_absolute() and
            all(part not in ("", ".", "..") for part in path.parts))


def validate_gitlinks(root: Path, gitlinks: dict[str, str]) -> None:
    for name, expected in gitlinks.items():
        path = root / name
        if not path.exists():
            continue
        if not path.is_dir() or path.is_symlink():
            fail("gitlink worktree is not a directory: " + name)
        if not (path / ".git").exists():
            if any(path.iterdir()):
                fail("uninitialized gitlink worktree is not empty: " + name)
            continue
        try:
            actual = subprocess.check_output(
                ["git", "-C", str(path), "rev-parse", "HEAD"], stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        except subprocess.CalledProcessError:
            fail("initialized gitlink worktree has no readable HEAD: " + name)
        if actual != expected:
            fail("gitlink worktree commit does not match the index: " + name)
        status = subprocess.check_output(
            ["git", "-C", str(path), "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"],
            text=True,
        )
        if status:
            fail("gitlink worktree contains tracked, untracked, or ignored changes: " + name)


def source_snapshot(root: Path) -> tuple[str, dict]:
    head = git(root, "rev-parse", "HEAD").decode().strip()
    if not re.fullmatch(r"[0-9a-f]{40}", head):
        fail("invalid Git HEAD")
    tracked = {}
    for entry in git(root, "ls-files", "-s", "-z", "--", "zig").split(b"\0"):
        if not entry:
            continue
        metadata, raw_name = entry.split(b"\t", 1)
        mode, object_id, stage = metadata.decode().split()
        if stage != "0":
            fail("source closure contains an unmerged index entry")
        tracked[raw_name.decode()] = (mode, object_id)
    untracked = {entry.decode() for entry in git(root, "ls-files", "--others", "--exclude-standard", "-z", "--", "zig").split(b"\0") if entry}
    names = sorted(name for name in set(tracked) | untracked if source_name(name))
    if len(names) > MAX_FILES:
        fail("source closure exceeds file limit")
    files, deleted, gitlinks, total = {}, [], {}, 0
    for name in names:
        if name in tracked and tracked[name][0] == "160000":
            gitlinks[name] = tracked[name][1]
            continue
        path = root / name
        if not path.exists():
            if name not in tracked:
                fail("untracked source disappeared")
            deleted.append(name)
            continue
        if path.is_symlink() or not path.is_file():
            fail("source closure contains a symlink or non-file: " + name)
        total += path.stat().st_size
        if total > MAX_UNCOMPRESSED:
            fail("source closure exceeds byte limit")
        files[name] = digest(path)
    validate_gitlinks(root, gitlinks)
    return head, {"contract": SOURCE_NOTE, "files": files, "deleted": deleted, "gitlinks": gitlinks}


def canonical(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def freeze_archive(root: Path, campaign: Path, head: str, source: dict, tools: dict) -> Path:
    archive = campaign / "full-release-source.zip"
    if archive.exists():
        fail("source archive already exists")
    manifest = {"head": head, "source": source, "build": BUILD, "tools": tools}
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as bundle:
        bundle.writestr(MANIFEST, canonical(manifest))
        for name, expected in source["files"].items():
            raw = (root / name).read_bytes()
            if digest_bytes(raw) != expected:
                fail("source changed while archive was created: " + name)
            bundle.writestr(name, raw)
    return archive


def parse_tests(stderr: str) -> dict:
    matches = re.findall(r"(?m)^(\d+) selected; (\d+) passed; (\d+) skipped(?:; (\d+) failed; (\d+) leaked)?\.$", stderr)
    if not matches:
        fail("full release build log lacks exact test summary")
    selected, passed, skipped, failed, leaked = (int(value or 0) for value in matches[-1])
    result = {"selected": selected, "passed": passed, "skipped": skipped,
              "failed": failed, "leaked": leaked}
    if selected <= 0 or passed <= 0 or passed + skipped != selected or failed or leaked:
        fail("full release compiled tests did not pass")
    return result


def executed_binary(stderr: str, zig_root: Path) -> Path:
    candidates = []
    # --verbose emits the executed context-tests command. Accept only a cache
    # artifact beneath this exact Zig checkout; copy it before sealing.
    for token in re.findall(r"(?:^|\s)([^\s'\"]*\.zig-cache/o/[0-9a-f]+/context-tests)(?=\s|$)", stderr):
        path = Path(token)
        if not path.is_absolute():
            # The inference build runner executes tests after `cd pkg/inference`.
            # Its verbose command therefore commonly uses ../../.zig-cache.
            base = zig_root / "pkg/inference" if token.startswith("./../../.zig-cache/") else zig_root
            path = base / path
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError:
            continue
        if resolved.is_relative_to((zig_root / ".zig-cache").resolve()) and resolved.is_file() and os.access(resolved, os.X_OK):
            candidates.append(resolved)
    if not candidates:
        fail("verbose build log does not bind the executed context-tests binary")
    return candidates[-1]


def ref(campaign: Path, path: Path) -> dict:
    return {"path": str(path.relative_to(campaign)), "sha256": digest(path)}


def seal(root: Path, campaign: Path, created: str, head: str, source: dict, archive: Path,
         resource_path: Path, tools: dict) -> Path:
    current_head, current_source = source_snapshot(root)
    if current_head != head or current_source != source:
        fail("source closure changed during build")
    if tool_identity() != tools:
        fail("build tool identity changed during build")
    stdout_path, stderr_path = resource_path.with_suffix(".stdout"), resource_path.with_suffix(".stderr")
    report = json.loads(resource_path.read_text())
    if (report.get("schema") != "antfly.benchmark_resources.v1" or report.get("command") != COMMAND or
            report.get("pass") is not True or report.get("returncode") != 0 or report.get("violation") is not None or
            "cleanup_error" in report or not isinstance(report.get("samples"), int) or report["samples"] <= 0):
        fail("full release resource guard did not pass")
    if report.get("limits") != {"timeout_seconds": LIMITS["timeout"], "max_rss_mib": LIMITS["max_rss_mib"],
                                "min_free_percent": LIMITS["min_free_percent"],
                                "max_swap_growth_mib": LIMITS["max_swap_growth_mib"],
                                "min_disk_free_mib": LIMITS["min_disk_free_mib"]}:
        fail("full release resource limits drifted")
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")
    if any(line.startswith("error:") for line in stderr.splitlines()):
        fail("compiler log contains an error")
    tests = parse_tests(stderr)
    source_binary = executed_binary(stderr, root / "zig")
    binary = campaign / "context-tests"
    with source_binary.open("rb") as incoming, binary.open("xb") as outgoing:
        shutil.copyfileobj(incoming, outgoing)
    binary.chmod(source_binary.stat().st_mode | stat.S_IXUSR)
    compiler_profile = parse_compiler_profile(stderr) if SCHEMA in {SCHEMA_V2, SCHEMA_V3} else None
    receipt = {
        "schema": SCHEMA, "evidence_scope": SCOPE, "release_eligible": True,
        "serving_qualified": False, "created_utc": created,
        "sealed_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "head": head,
        "zig_version": tools["zig"]["version"], "tools": tools,
        "binary": {**ref(campaign, binary), "bytes": binary.stat().st_size}, "build": BUILD,
        "resource": ref(campaign, resource_path), "stdout": ref(campaign, stdout_path),
        "stderr": ref(campaign, stderr_path), "tests": tests, "source": source,
        "source_archive": {**ref(campaign, archive), "bytes": archive.stat().st_size,
                           "files": len(source["files"])},
        "source_verified_after_build": True,
    }
    if compiler_profile is not None:
        receipt["compiler_profile"] = compiler_profile
    receipt_path = campaign / "full-release-build.json"
    with receipt_path.open("x") as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return receipt_path


def run(root: Path, campaign: Path) -> Path:
    root, campaign = root.resolve(), campaign.resolve()
    if campaign.exists():
        fail("campaign directory already exists")
    campaign.mkdir(parents=True)
    created = dt.datetime.now(dt.timezone.utc).isoformat()
    head, source = source_snapshot(root)
    tools = tool_identity()
    archive = freeze_archive(root, campaign, head, source, tools)
    archive_sha = digest(archive)
    sys.path.insert(0, str(root / "zig/pkg/inference/scripts"))
    from benchmark_resources import run_guarded
    resource = campaign / "build-resource.json"
    previous = Path.cwd()
    try:
        os.chdir(root / "zig")
        with sanitized_environment():
            run_guarded(COMMAND, resource, **LIMITS)
    finally:
        os.chdir(previous)
    if digest(archive) != archive_sha:
        fail("pre-build source archive changed")
    return seal(root, campaign, created, head, source, archive, resource, tools)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--stripped-profile", action="store_true")
    args = parser.parse_args()
    configure_profile(args.stripped_profile)
    receipt = run(args.root, args.campaign)
    print(json.dumps({"receipt": str(receipt), "sha256": digest(receipt)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
