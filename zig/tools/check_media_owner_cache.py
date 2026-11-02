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

"""Check real media owner/client invalidation without editing the working source.

Overlay sources and private build output stay inside this worktree and are
removed afterward. This measures cache reuse, not cold-build speed or product
size. Implementation edits must rebuild the provider while caching the caller.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

from check_architecture_probe_cache import link_children, own

ZIG_ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--retained",
        action="store_true",
        help="Check native retained readers, rendering implementation and pure value contracts",
    )
    parser.add_argument(
        "--optimize",
        choices=("debug", "fast", "safe"),
        default="debug",
        help="Use Debug for routine correctness/cache checks; Fast for final LLVM qualification",
    )
    args = parser.parse_args()
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_parent = ZIG_ROOT / ".zig-cache/media-owner-overlays"
    overlay_parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        scope="compiled retained PDF owner and reader-free client"
        if args.retained
        else "compiled media owner and source-free PDF caller",
        base_git_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ZIG_ROOT, text=True
        ).strip(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        optimize=args.optimize,
        passed=False,
        phases=[],
    )
    with tempfile.TemporaryDirectory(dir=overlay_parent) as directory:
        root = Path(directory) / "zig"
        link_children(ZIG_ROOT, root)
        paths = {
            "provider": "pkg/antfly-embedded/src/storage/enrichment_compute_provider.zig",
            "client": "pkg/antfly-embedded/src/storage/pdf_session_client.zig",
            "contract": "pkg/antfly-embedded/src/storage/enrichment_compute_abi.zig",
        }
        if args.retained:
            paths = {
                "provider": "pkg/antfly-embedded/src/storage/pdf_native_provider.zig",
                "reader": "lib/pdf/src/reader.zig",
                "renderer": "lib/pdf/src/render.zig",
                "provider_code": "pkg/antfly-embedded/src/storage/pdf_native_provider.zig",
                "decoder": "lib/image/src/jpeg.zig",
                "control": "lib/image/src/work_control.zig",
                "client": "pkg/antfly-embedded/src/storage/pdf_facade.zig",
                "contract": "lib/pdf/src/values.zig",
                "protocol": "pkg/antfly-embedded/src/storage/pdf_native_contract.zig",
            }
        report["initial_sources_sha256"] = {
            relative: hashlib.sha256((ZIG_ROOT / relative).read_bytes()).hexdigest()
            for relative in paths.values()
        }
        changed_sources = {phase: own(root, path) for phase, path in paths.items()}
        for phase in ("initial", "warm", *paths):
            if phase in changed_sources:
                path = changed_sources[phase]
                source = path.read_text()
                if phase == "provider_code":
                    original = "        return reader(doc).value.pageCount();"
                    replacement = "        return cacheProbePageCount(try reader(doc).value.pageCount());\n    }\n    noinline fn cacheProbePageCount(page_total: usize) usize {\n        const guard: *volatile const usize = &page_total;\n        return guard.*;"
                    if source.count(original) != 1:
                        raise RuntimeError(
                            "retained count operation changed; update reachable-code probe"
                        )
                    path.write_text(source.replace(original, replacement))
                else:
                    path.write_text(source + "\n// Media cache isolation experiment.\n")
            print("media owner:", phase, flush=True)
            started = time.monotonic()
            process = subprocess.Popen(
                [
                    "python3",
                    "tools/run_bounded_zig_build.py",
                    "--",
                    "build",
                    "compiled-pdf-native-test"
                    if args.retained
                    else "opaque-pdf-session-test",
                    *(["-Dshared-pdf-sessions=true"] if args.retained else []),
                    f"-Doptimize={args.optimize}",
                    f"-Dstrip={str(args.optimize != 'debug').lower()}",
                    "-Dcpu=baseline",
                    "-Dmetal=false",
                    "-Dcuda=false",
                    "-Dsystem-blas=false",
                    "--summary",
                    "all",
                ],
                cwd=root,
                env=dict(
                    os.environ,
                    ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                    ZIG_GLOBAL_CACHE_DIR=str(ZIG_ROOT / ".zig-cache/global"),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                output, _ = process.communicate(timeout=600)
            except BaseException:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate()
                raise
            log = report_path.with_name(report_path.stem + "-" + phase + ".log")
            log.write_text(output)
            row = dict(
                phase=phase,
                elapsed_s=time.monotonic() - started,
                returncode=process.returncode,
                log=str(log),
            )
            candidates = list(
                (root / ".zig-cache/o").glob("*/libantfly-runtime-enrichment_compute.a")
            )
            if candidates:
                archive = max(candidates, key=lambda path: path.stat().st_mtime_ns)
                row["provider_bytes"] = archive.stat().st_size
                row["provider_sha256"] = hashlib.sha256(
                    archive.read_bytes()
                ).hexdigest()
            report["phases"].append(row)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if process.returncode:
                raise RuntimeError("media owner test failed: " + str(log))
            if phase == "provider_code":
                previous = report["phases"][-2].get("provider_sha256")
                if not previous or row.get("provider_sha256") == previous:
                    raise RuntimeError(
                        "reachable provider edit did not change its archive bytes"
                    )
            for forbidden in (
                "antfly-storage-engine",
                "antfly-storage-kernel",
                "antfly-inference-core",
            ):
                if re.search(rf"compile lib {re.escape(forbidden)} ", output):
                    raise RuntimeError(
                        "media-only test pulled unrelated owner: " + forbidden
                    )
            actual = {}
            for name, kind, artifact in (
                ("provider", "lib", "antfly-runtime-enrichment_compute"),
                (
                    "client",
                    "test_obj",
                    "pdf-native-owner-tests"
                    if args.retained
                    else "opaque-pdf-session-tests",
                ),
            ):
                match = re.search(
                    rf"compile {kind} {artifact} {re.escape(args.optimize)} \S+ (cached|success)\b",
                    output,
                )
                if match is None:
                    raise RuntimeError("missing compiler attribution: " + artifact)
                actual[name] = match[1]
            expected = dict(
                provider="success"
                if phase
                in (
                    "initial",
                    "provider",
                    "reader",
                    "renderer",
                    "provider_code",
                    "decoder",
                    "control",
                    "contract",
                    "protocol",
                )
                else "cached",
                client="success"
                if phase in ("initial", "client", "control", "contract", "protocol")
                else "cached",
            )
            row.update(compilers=actual, expected=expected)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if actual != expected:
                raise RuntimeError(
                    f"{phase}: expected {expected}, observed {actual}; see {log}"
                )
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
