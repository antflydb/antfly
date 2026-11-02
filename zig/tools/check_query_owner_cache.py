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

"""Check production query/storage archive isolation in a private source overlay.

The temporary overlay and compiler caches stay inside the task worktree. This
checks real owner code generation, not cold-build performance or final linking.
Private DB edits must preserve the independent dispatcher and opaque consumer.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from check_architecture_probe_cache import link_children, own, compile_state

ZIG_ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    cache = ZIG_ROOT / ".zig-cache" / "query-owner-overlays"
    cache.mkdir(parents=True, exist_ok=True)
    report = {
        "scope": "production query and storage owner codegen",
        "passed": False,
        "phases": [],
    }
    with tempfile.TemporaryDirectory(dir=cache) as directory:
        root = Path(directory) / "zig"
        link_children(ZIG_ROOT, root)
        # Freeze authored code and construction inputs for the complete matrix.
        # Implementation work can continue in the source tree without changing
        # the compiler's inputs halfway through an initial or mutation phase.
        # Assets remain shared; every mutated source is a private overlay copy.
        sources = [ZIG_ROOT / "build.zig"]
        for family in ("lib", "pkg", "build_support"):
            sources.extend((ZIG_ROOT / family).rglob("*.zig"))
        for source in sources:
            own(root, source.relative_to(ZIG_ROOT))
        report["source_snapshot"] = {
            "files": len(sources),
            "bytes": sum(source.stat().st_size for source in sources),
        }
        provider = own(root, "pkg/antfly/src/storage/local_query_provider.zig")
        dispatcher = own(root, "pkg/antfly/src/storage/local_query_dispatch.zig")
        layout = own(root, "pkg/antfly-embedded/src/storage/db/db.zig")
        storage = own(root, "pkg/antfly/src/capi/server_owner.zig")
        contract = own(root, "pkg/antfly-embedded/src/storage/local_query_abi.zig")
        for phase in (
            "initial",
            "warm",
            "dispatch",
            "execution",
            "storage",
            "private_layout",
            "contract",
        ):
            changed = {
                "dispatch": dispatcher,
                "execution": provider,
                "storage": storage,
                "private_layout": layout,
                "contract": contract,
            }.get(phase)
            if changed:
                source = changed.read_text()
                if phase == "private_layout":
                    marker = (
                        "    query_schema_identity: std.atomic.Value(u64) = .init(0),"
                    )
                    if source.count(marker) != 1:
                        raise RuntimeError(
                            "DB declaration changed; update private-layout probe"
                        )
                    changed.write_text(
                        source.replace(
                            marker, "    query_layout_cache_probe: u64 = 0,\n" + marker
                        )
                    )
                else:
                    changed.write_text(
                        source + "\n// Isolated production owner cache mutation.\n"
                    )
            print("query owner:", phase, flush=True)
            started = time.monotonic()
            process = subprocess.Popen(
                [
                    "python3",
                    "tools/run_bounded_zig_build.py",
                    "--",
                    "build",
                    "runtime-unit-local_query",
                    "runtime-unit-storage-adapter",
                    "opaque-query-consumer-compile",
                    "-Doptimize=debug",
                    "-Dmetal=false",
                    "-Dcuda=false",
                    "--summary",
                    "all",
                ],
                cwd=root,
                env=dict(
                    os.environ,
                    ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                    ZIG_GLOBAL_CACHE_DIR=str(ZIG_ROOT / ".zig-cache" / "global"),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                output, _ = process.communicate(timeout=1800)
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
            row = {
                "phase": phase,
                "elapsed_s": time.monotonic() - started,
                "returncode": process.returncode,
                "log": str(log),
            }
            report["phases"].append(row)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if process.returncode:
                raise RuntimeError("owner compilation failed: " + str(log))
            actual = {
                "query": compile_state(output, "lib", "antfly-runtime-local_query"),
                "storage": compile_state(output, "lib", "antfly-storage-kernel"),
                "consumer": compile_state(
                    output, "test_obj", "opaque-query-owner-tests"
                ),
            }
            expected = {
                "initial": {
                    "query": "success",
                    "storage": "success",
                    "consumer": "success",
                },
                "warm": {"query": "cached", "storage": "cached", "consumer": "cached"},
                "dispatch": {
                    "query": "success",
                    "storage": "cached",
                    "consumer": "cached",
                },
                "execution": {
                    "query": "cached",
                    "storage": "success",
                    "consumer": "cached",
                },
                "storage": {
                    "query": "cached",
                    "storage": "success",
                    "consumer": "cached",
                },
                "private_layout": {
                    "query": "cached",
                    "storage": "success",
                    "consumer": "cached",
                },
                "contract": {
                    "query": "success",
                    "storage": "success",
                    "consumer": "success",
                },
            }[phase]
            row["compilers"] = actual
            row["expected"] = expected
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if actual != expected:
                raise RuntimeError(
                    f"{phase}: expected {expected}, observed {actual}; see {log}"
                )
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
