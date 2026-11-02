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

"""Qualify production API/serverless lake ownership in a frozen source overlay.

This proves Debug archive cache and symbol ownership, not release size or cold
build performance. Every authored Zig source and construction file is copied;
package downloads and immutable assets stay shared. Caches remain in-worktree.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from check_architecture_probe_cache import compile_state, link_children, own
from check_image_owner_cache import construction_sources, run_phase

ZIG = Path(__file__).resolve().parents[1]
SOURCES = {
    "physical": "pkg/antfly/src/api/lake_execution_build_impl.zig",
    "client": "pkg/antfly/src/api/lake_execution_native_client.zig",
    "contract": "pkg/antfly/src/api/lake_execution_native_contract.zig",
}
OWNERS = {"api": "antfly-runtime-api_kernel", "serverless": "antfly-runtime-serverless"}
MIGRATED = re.compile(
    r"lake_execution_build_impl\.|lake_index_publication_builders\."
    r"|lake_index_native_(?:text|sparse|dense|rows)\.build(?:Incremental)?\b"
    r"|lake_index_native_aggregates\.build(?:Indexed|Incremental|WithReuse)?\b"
)


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def archive_receipt(root, name):
    candidates = list((root / ".zig-cache/o").glob("*/lib" + name + ".a"))
    if not candidates:
        raise RuntimeError("missing archive " + name)
    path = max(candidates, key=lambda value: value.stat().st_mtime_ns)
    symbols = subprocess.run(
        ["nm", "-a", str(path)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout
    migrated = [line.strip() for line in symbols.splitlines() if MIGRATED.search(line)]
    return {
        "archive": str(path.relative_to(root)),
        "bytes": path.stat().st_size,
        "sha256": file_hash(path),
        "migrated_symbol_count": len(migrated),
        "migrated_symbol_sample": migrated[:30],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = ZIG / ".zig-cache/lake-execution-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "--",
        "build",
        "runtime-unit-api_kernel",
        "runtime-unit-serverless",
        "-Dshared-lake-execution=true",
        "-Dshared-image-operations=true",
        "-Doptimize=debug",
        "-Dstrip=false",
        "-Dcpu=baseline",
        "-Dmetal=false",
        "-Dcuda=false",
        "-Dsystem-blas=false",
        "--summary",
        "all",
        "-j1",
    ]
    report = {
        "format_version": 1,
        "base_git_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ZIG.parent, text=True
        ).strip(),
        "script_sha256": file_hash(Path(__file__)),
        "passed": False,
        "scope": "production API/serverless Debug codegen, archive bytes and migrated symbol ownership; no release size/speed claim",
        "command": command,
        "phases": [],
    }
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        # Build constructors resolve schema/manifest inputs relative to ../.
        # Preserve that repository context without copying immutable assets.
        for child in ZIG.parent.iterdir():
            if child.name not in {"zig", ".git", ".worktrees"}:
                (Path(directory) / child.name).symlink_to(
                    child, target_is_directory=child.is_dir()
                )
        root = Path(directory) / "zig"
        link_children(ZIG, root)
        frozen = set(construction_sources())
        for prefix in (
            "lib",
            "pkg/antfly/src",
            "pkg/antfly-embedded/src",
            "pkg/inference/src",
        ):
            frozen.update(
                str(path.relative_to(ZIG))
                for path in (ZIG / prefix).rglob("*.zig")
                if not any(
                    part in {".zig-cache", "zig-out", "zig-pkg"} for part in path.parts
                )
            )
        for relative in sorted(frozen):
            own(root, relative)
        report["frozen_source_count"] = len(frozen)
        report["frozen_sources_sha256"] = {
            relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
            for relative in sorted(frozen)
        }
        changed = {phase: root / relative for phase, relative in SOURCES.items()}
        for phase in ("initial", "warm", "physical", "client", "contract"):
            print("lake execution:", phase, flush=True)
            row = {"phase": phase, "passed": False, "errors": []}
            report["phases"].append(row)
            if phase == "physical":
                path = changed[phase]
                source = path.read_text()
                marker = "memo, policy.allow_reuse, policy.identity_spill_bytes) catch"
                if source.count(marker) != 1:
                    raise RuntimeError("physical probe marker changed")
                path.write_text(
                    source.replace(
                        marker,
                        "memo, lakeCacheProbeReuse(policy.allow_reuse), policy.identity_spill_bytes) catch",
                    )
                    + "\nfn lakeCacheProbeReuse(value: bool) callconv(.c) bool { return value; }\n"
                )
            elif phase == "client":
                path = changed[phase]
                source = path.read_text()
                marker = "    const response = try c.execute(request);"
                if source.count(marker) != 1:
                    raise RuntimeError("client probe marker changed")
                path.write_text(
                    source.replace(
                        marker,
                        "    request.policy.allow_reuse = lakeCacheProbeReuse(request.policy.allow_reuse);\n"
                        + marker,
                    )
                    + "\nfn lakeCacheProbeReuse(value: bool) callconv(.c) bool { return value; }\n"
                )
            elif phase == "contract":
                path = changed[phase]
                source = path.read_text()
                marker = (
                    'return .of("antfly_lake_execute_v2", Function, Request, Response);'
                )
                if source.count(marker) != 1:
                    raise RuntimeError("contract probe marker changed")
                path.write_text(
                    source.replace(
                        marker,
                        marker.replace(
                            "antfly_lake_execute_v2",
                            "antfly_lake_execute_cache_probe_v1",
                        ),
                    )
                )
            log = report_path.with_name(report_path.stem + "-" + phase + ".log")
            row["log"] = str(log)
            row.update(run_phase(root, command, log, args.timeout))
            output = log.read_text(errors="replace")
            if row["returncode"] != 0 or row["interruption"]:
                row["errors"].append("build failed; inspect retained log")
            else:
                states = {
                    key: compile_state(output, "lib", name)
                    for key, name in OWNERS.items()
                }
                expected = {
                    "api": "cached" if phase in ("warm", "physical") else "success",
                    "serverless": "cached"
                    if phase in ("warm", "client")
                    else "success",
                }
                row.update(compilers=states, expected=expected)
                if states != expected:
                    row["errors"].append("cache states differ: " + repr(states))
                row["archives"] = {
                    key: archive_receipt(root, name) for key, name in OWNERS.items()
                }
                if row["archives"]["api"]["migrated_symbol_count"]:
                    row["errors"].append("API still emits migrated physical builders")
                if not row["archives"]["serverless"]["migrated_symbol_count"]:
                    row["errors"].append("serverless does not emit migrated builders")
                if phase in ("physical", "client"):
                    owner = "serverless" if phase == "physical" else "api"
                    other = "api" if phase == "physical" else "serverless"
                    previous = report["phases"][-2]["archives"]
                    if previous[owner]["sha256"] == row["archives"][owner]["sha256"]:
                        row["errors"].append(
                            "reachable edit did not change owning archive bytes"
                        )
                    if previous[other]["sha256"] != row["archives"][other]["sha256"]:
                        row["errors"].append("unrelated archive bytes changed")
            row["passed"] = not row["errors"]
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            if not row["passed"]:
                print(json.dumps({"phase": phase, "errors": row["errors"]}), flush=True)
                raise SystemExit(1)
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"passed": True, "phases": len(report["phases"])}), flush=True)


if __name__ == "__main__":
    main()
