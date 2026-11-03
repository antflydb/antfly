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

"""Qualify the public mutation decoder's source closure and cache boundary.

Runs Debug tests in a private worktree cache overlay. This measures the small
public decoder compilation, not whole-product release performance or runtime
throughput. Private command parsing/storage is deliberately outside this root.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

from check_architecture_probe_cache import link_children, own

ZIG = Path(__file__).resolve().parents[1]
REPO = ZIG.parent
MODULES = {
    "antfly-json": "lib/json/src/mod.zig",
    "antfly_public_limits": "pkg/antfly-embedded/src/api/public_limits.zig",
}
ROOT = "pkg/antfly-embedded/src/public_batch_test_root.zig"
EDITS = {
    "private": "pkg/antfly-embedded/src/api/batch_internal.zig",
    "storage": "pkg/antfly-embedded/src/storage/db/transform.zig",
    "public": "pkg/antfly-embedded/src/api/batch.zig",
    "values": "pkg/antfly-embedded/src/storage/db/mutation_values.zig",
    "validation": "pkg/antfly-embedded/src/storage/db/mutation_validation.zig",
}
BUILD = """const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const root = b.createModule(.{ .root_source_file = b.path("ROOT"), .target = target, .optimize = .Debug });
    root.addImport("antfly-json", b.createModule(.{ .root_source_file = b.path("lib/json/src/mod.zig"), .target = target, .optimize = .Debug }));
    root.addImport("antfly_public_limits", b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/api/public_limits.zig"), .target = target, .optimize = .Debug }));
    const unit = b.addTest(.{ .name = "public-mutation-tests", .root_module = root });
    b.step("test", "Qualify public mutation values").dependOn(&b.addRunArtifact(unit).step);
}
""".replace("ROOT", ROOT)


def source_closure(root):
    """Resolve literal imports; reject unrecognized named inputs instead of guessing."""
    pending = [ROOT]
    paths = {}
    while pending:
        relative = pending.pop()
        if relative in paths:
            continue
        path = root / relative
        source = path.read_text()
        paths[relative] = hashlib.sha256(source.encode()).hexdigest()
        for imported in re.findall(r'@import\("([^"\n]+)"\)', source):
            if imported in {"std", "builtin"}:
                continue
            if imported.endswith(".zig"):
                child = (path.parent / imported).resolve()
                # Symlinked files are read from the checkout; keep logical paths.
                logical = os.path.normpath(str(Path(relative).parent / imported))
                if logical.startswith("../"):
                    raise ValueError(
                        f"Import escapes source root: {relative}: {imported}"
                    )
                if not child.exists():
                    raise ValueError(f"Missing import {logical}")
                pending.append(logical)
            elif imported in MODULES:
                pending.append(MODULES[imported])
            else:
                raise ValueError(f"Unexpected named import: {imported}")
    forbidden = {
        "selected_root.zig",
        "types.zig",
        "batch_internal.zig",
        "document_mapper.zig",
        "transform.zig",
        "engine.zig",
        "engine_exports.zig",
    }
    bad = [name for name in paths if Path(name).name in forbidden]
    if bad:
        raise ValueError(f"Private implementation in public closure: {bad}")
    return dict(sorted(paths.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--zig", default="zig")
    args = parser.parse_args()
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = ZIG / ".zig-cache/public-mutation-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        scope=__doc__,
        passed=False,
        phases=[],
        source_closure_sha256=source_closure(ZIG),
        mutation_inputs_sha256={
            phase: hashlib.sha256((ZIG / relative).read_bytes()).hexdigest()
            for phase, relative in EDITS.items()
        },
        zig_version=subprocess.check_output([args.zig, "version"], text=True).strip(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        root = Path(directory) / "zig"
        link_children(ZIG, root)
        own(root, "build.zig").write_text(BUILD)
        for relative in report["source_closure_sha256"]:
            own(root, relative)
        changed = {phase: own(root, relative) for phase, relative in EDITS.items()}
        for phase in ("initial", "warm", *EDITS):
            if phase in changed:
                path = changed[phase]
                path.write_text(
                    path.read_text()
                    + "\n// Public mutation cache qualification edit.\n"
                )
            log = report_path.with_name(report_path.stem + "-" + phase + ".log")
            started = time.monotonic()
            env = dict(
                os.environ,
                ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                ZIG_GLOBAL_CACHE_DIR=str(ZIG / ".zig-cache/global"),
            )
            with log.open("w") as stream:
                result = subprocess.run(
                    [args.zig, "build", "test", "--summary", "all"],
                    cwd=root,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    timeout=300,
                )
            output = log.read_text()
            matches = re.findall(
                r"compile test public-mutation-tests debug native(?:-native)? (cached|success)\b",
                output,
            )
            state = matches[0] if len(matches) == 1 else None
            expected = (
                "cached" if phase in {"warm", "private", "storage"} else "success"
            )
            row = dict(
                phase=phase,
                elapsed_s=time.monotonic() - started,
                returncode=result.returncode,
                compiler=state,
                expected=expected,
                log=str(log),
                log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
            )
            if phase in changed:
                row["edited_source_sha256"] = hashlib.sha256(
                    changed[phase].read_bytes()
                ).hexdigest()
            row["passed"] = (
                result.returncode == 0
                and state == expected
                and bool(re.search(r"Build Summary: (\d+)/\1 steps succeeded", output))
            )
            report["phases"].append(row)
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(row), flush=True)
            if not row["passed"]:
                raise SystemExit(1)
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
