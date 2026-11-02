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

"""Check compiled image owner/client isolation with real source and code edits.

Debug is the routine default. Private source overlays and build output remain
inside this worktree's cache and are removed afterward. The source decoder
oracle is expected to rebuild for codec edits; it is not the native client.
This proves compiler-cache reuse, not cold-build speed or product size.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

from check_architecture_probe_cache import link_children, own
from check_image_owner_transport import analyze

ZIG = Path(__file__).resolve().parents[1]
REPO = ZIG.parent
SOURCES = {
    "provider_code": "pkg/antfly-embedded/src/storage/image_native_provider.zig",
    "jpeg": "lib/image/src/jpeg.zig",
    "webp": "lib/image/src/webp.zig",
    "facade": "pkg/antfly-embedded/src/storage/image_facade.zig",
    "values": "lib/image/src/values.zig",
    "contract": "pkg/antfly-embedded/src/storage/image_native_contract.zig",
    "protocol": "pkg/antfly-embedded/src/storage/media_native_abi.zig",
}
ARTIFACTS = {
    "provider": ("lib", "antfly-runtime-enrichment_compute"),
    "native_client": ("test_obj", "image-native-owner-tests"),
    "source_oracle": ("test_obj", "image-source-oracle-tests"),
}
FORBIDDEN = (
    "antfly-storage-engine",
    "antfly-storage-kernel",
    "antfly-inference-core",
    "antfly-runtime-api_kernel",
    "antfly-runtime-local_query",
    "antfly-runtime-distributed",
    "antfly-runtime-cli",
    "antfly-runtime-inference",
    "antfly-runtime-serverless",
)


def construction_sources():
    paths = {ZIG / "build.zig", ZIG / "build.zig.zon"}
    for relative in ("build_support", "pkg/antfly/build", "pkg/inference/build"):
        paths.update((ZIG / relative).rglob("*.zig"))
    for library in (ZIG / "lib").iterdir():
        if library.is_dir():
            paths.update(library.glob("build*.zig"))
            paths.update(library.glob("build*.zig.zon"))
            if (library / "build").is_dir():
                paths.update((library / "build").rglob("*.zig"))
    for package in (ZIG / "pkg").iterdir():
        if package.is_dir():
            paths.update(package.glob("build*.zig"))
            paths.update(package.glob("build*.zig.zon"))
    return sorted(str(path.relative_to(ZIG)) for path in paths if path.is_file())


def construction_hashes(root, paths):
    return {
        relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
        for relative in paths
    }


PROVIDER_ORIGINAL = """    pub fn inspect(bytes: []const u8) !c.values.EncodedImageInfo {
        return image.inspectEncoded(bytes);
    }
"""
PROVIDER_CHANGED = """    pub fn inspect(bytes: []const u8) !c.values.EncodedImageInfo {
        const facts = try image.inspectEncoded(bytes);
        return .{ .width = cacheProbeDimension(facts.width), .height = cacheProbeDimension(facts.height) };
    }
    noinline fn cacheProbeDimension(value: u32) u32 {
        const guard: *volatile const u32 = &value;
        return guard.*;
    }
"""


def compiler_states(output, optimize):
    states = {}
    errors = []
    for role, (kind, name) in ARTIFACTS.items():
        found = re.findall(
            rf"\bcompile {kind} {re.escape(name)} {re.escape(optimize)} \S+ (cached|success)\b",
            output,
        )
        if len(found) != 1:
            errors.append(
                f"Expected one compiler attribution for {name}, found {found}"
            )
        else:
            states[role] = found[0]
    for name in FORBIDDEN:
        if re.search(rf"\bcompile lib {re.escape(name)}\s", output):
            errors.append(f"Image-only test pulled unrelated owner: {name}")
    return states, errors


def expected_states(phase):
    return {
        "provider": "cached" if phase in ("warm", "facade") else "success",
        "native_client": "success"
        if phase in ("initial", "facade", "values", "contract", "protocol")
        else "cached",
        "source_oracle": "cached"
        if phase in ("warm", "provider_code", "facade")
        else "success",
    }


def provider_archive(root):
    candidates = list(
        (root / ".zig-cache/o").glob("*/libantfly-runtime-enrichment_compute.a")
    )
    if not candidates:
        return {}
    artifact = max(candidates, key=lambda path: path.stat().st_mtime_ns)
    return dict(
        provider_archive=str(artifact.relative_to(root)),
        provider_bytes=artifact.stat().st_size,
        provider_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
    )


def run_phase(root, command, log_path, timeout):
    started = time.monotonic()
    failure = None
    process = None
    with log_path.open("wb") as log:
        try:
            process = subprocess.Popen(
                command,
                cwd=root,
                env=dict(
                    os.environ,
                    ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                    ZIG_GLOBAL_CACHE_DIR=str(ZIG / ".zig-cache/global"),
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
            failure = type(exc).__name__
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
        except OSError as exc:
            failure = f"{type(exc).__name__}: {exc}"
    return dict(
        elapsed_s=time.monotonic() - started,
        returncode=process.returncode if process is not None else None,
        interruption=failure,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--optimize", choices=("debug", "fast", "safe"), default="debug"
    )
    parser.add_argument(
        "--timeout", type=int, default=900, help="Per-phase build timeout"
    )
    parser.add_argument(
        "--phase",
        choices=tuple(SOURCES),
        help="Reproduce one edit after fresh initial/warm builds; omitted runs the complete matrix",
    )
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = ZIG / ".zig-cache/image-owner-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "--",
        "build",
        "compiled-image-native-test",
        "-Dshared-image-operations=true",
        f"-Doptimize={args.optimize}",
        f"-Dstrip={str(args.optimize != 'debug').lower()}",
        "-Dcpu=baseline",
        "-Dmetal=false",
        "-Dcuda=false",
        "-Dsystem-blas=false",
        "--summary",
        "all",
    ]
    report = dict(
        format_version=1,
        scope="Compiled image owner/native client isolation, with a separate source oracle; this is not cold-build or product-size qualification",
        base_git_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        initial_sources_sha256={
            relative: hashlib.sha256((ZIG / relative).read_bytes()).hexdigest()
            for relative in SOURCES.values()
        },
        construction_snapshot="Private copies of build scripts, image source and oracle; package downloads/assets stay shared",
        optimize=args.optimize,
        matrix_scope="single_edit" if args.phase else "full",
        selected_phase=args.phase,
        command=command,
        passed=False,
        phases=[],
    )
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        root = Path(directory) / "zig"
        link_children(ZIG, root)
        construction = construction_sources()
        for relative in construction:
            own(root, relative)
        for path in (ZIG / "lib/image").rglob("*.zig"):
            own(root, str(path.relative_to(ZIG)))
        own(root, "pkg/antfly/src/image_native_owner_test_root.zig")
        report["construction_sources_sha256"] = construction_hashes(root, construction)
        changed = {phase: own(root, relative) for phase, relative in SOURCES.items()}
        for phase in ("initial", "warm", *((args.phase,) if args.phase else SOURCES)):
            print("image owner:", phase, flush=True)
            row = dict(phase=phase, passed=False, errors=[])
            report["phases"].append(row)
            if (
                construction_hashes(root, construction)
                != report["construction_sources_sha256"]
            ):
                row["errors"].append(
                    "Build construction sources changed during cache matrix; rerun with a stable graph"
                )
            if phase in changed:
                path = changed[phase]
                source = path.read_text()
                if phase == "provider_code":
                    if source.count(PROVIDER_ORIGINAL) != 1:
                        row["errors"].append(
                            "Image inspect operation changed; update code probe"
                        )
                    else:
                        path.write_text(
                            source.replace(PROVIDER_ORIGINAL, PROVIDER_CHANGED)
                        )
                else:
                    path.write_text(
                        source + "\n// Image owner cache isolation experiment.\n"
                    )
                row["edited_source_sha256"] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
            log = report_path.with_name(report_path.stem + "-" + phase + ".log")
            row["log"] = str(log)
            if not row["errors"]:
                row.update(run_phase(root, command, log, args.timeout))
                if (
                    construction_hashes(root, construction)
                    != report["construction_sources_sha256"]
                ):
                    row["errors"].append(
                        "Build construction sources changed during this phase; cache causality is not qualified"
                    )
                output = log.read_text(errors="replace")
                row["log_sha256"] = hashlib.sha256(log.read_bytes()).hexdigest()
                states, failures = compiler_states(output, args.optimize)
                row.update(compilers=states, expected=expected_states(phase))
                row["errors"].extend(failures)
                reference = analyze(output)
                row["fixture_counts"] = reference["fixture_counts"]
                row["vp8_counts"] = reference["vp8_counts"]
                row["errors"].extend(reference["errors"])
                row.update(provider_archive(root))
                if row["returncode"] != 0 or row.get("interruption"):
                    row["errors"].append(
                        f"Build failed: exit={row['returncode']} interruption={row.get('interruption')}"
                    )
                if states != row["expected"]:
                    row["errors"].append(
                        f"Expected {row['expected']}, observed {states}"
                    )
                if phase == "provider_code":
                    previous = report["phases"][-2].get("provider_sha256")
                    if not previous or previous == row.get("provider_sha256"):
                        row["errors"].append(
                            "Reachable provider-code edit did not change archive bytes"
                        )
            row["passed"] = not row["errors"]
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            if not row["passed"]:
                print(json.dumps({"phase": phase, "errors": row["errors"]}))
                raise SystemExit(1)
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"passed": True, "phases": len(report["phases"])}))


if __name__ == "__main__":
    main()
