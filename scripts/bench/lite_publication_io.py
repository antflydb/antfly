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

"""Count durable publication I/O for the matched synthetic workload on macOS.

Compile lite_io_counter.c once into a task-owned dylib, then pass --shim.
This measures operations, not throughput. Default engine is Antfly so recorded
SQLite references are not rerun. One closed fixture is reused and deleted after
full verification. Calling-thread counters describe execution, not request
ownership when group commit hands work to another thread.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from lite_contention import sdk
from lite_sqlite_comparison import (
    bootstrap,
    cleanup,
    encoded,
    key,
    payload,
    sqlite_open,
    verify,
)


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def snapshot(trace):
    values = (ctypes.c_uint64 * 36)()
    trace.af_trace_read(values)
    fields = [
        "fsync_calls",
        "write_calls",
        "write_bytes",
        "read_calls",
        "read_bytes",
        "fsync_ns",
    ]
    return {
        lane: {
            kind: dict(
                zip(
                    fields,
                    list(values)[(i * 3 + j) * 6 : (i * 3 + j + 1) * 6],
                    strict=True,
                )
            )
            for j, kind in enumerate(["main", "wal", "auxiliary"])
        }
        for i, lane in enumerate(["reset_thread", "other_threads"])
    }


def worker(args):
    library = str(args.library.resolve())
    indexed = args.worker == "indexed"
    path = args.directory / (
        "antfly.aflite" if args.engine == "antfly" else "sqlite.db"
    )
    if path.exists():
        raise RuntimeError("The fixed fixture must be absent before starting a case")
    trace = ctypes.CDLL(str(args.shim.resolve()))
    trace.af_trace_reset.argtypes = []
    trace.af_trace_read.argtypes = [ctypes.POINTER(ctypes.c_uint64)]
    bootstrap(args.engine, path, indexed, library, 1)
    db = (
        sqlite_open(path)
        if args.engine == "sqlite"
        else sdk(library).open(path, no_sync=False, busy_timeout=5)
    )
    try:
        time.sleep(0.25)
        trace.af_trace_reset()
        for sequence in range(128):
            document = payload(0, sequence)
            document_key = key(0, sequence)
            if args.engine == "sqlite":
                db.execute("BEGIN IMMEDIATE")
                try:
                    db.execute(
                        "INSERT INTO documents(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        (document_key, encoded(document)),
                    )
                    db.execute("COMMIT")
                except BaseException:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise
            else:
                db.batch_json(
                    {
                        "inserts": {document_key: document},
                        "sync_level": "full_index" if indexed else "write",
                    }
                )
        after = snapshot(trace)
    finally:
        db.close()
    if not sum(row["fsync_calls"] for lane in after.values() for row in lane.values()):
        raise RuntimeError("Interposer did not observe durable syncs")
    checked = verify(
        args.engine, path, indexed, library, [{"worker": 0, "successful_calls": 128}]
    )
    return {
        "engine": args.engine,
        "indexed": indexed,
        "writes": 128,
        "local_diagnostic_not_performance_qualification": True,
        "after_mutations": after,
        "verification": checked,
        "cleanup": cleanup(path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--shim", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--engine", choices=["antfly", "sqlite"], default="antfly")
    parser.add_argument(
        "--mode", choices=["primary", "indexed", "both"], default="both"
    )
    parser.add_argument("--source-commit", default="unspecified")
    parser.add_argument(
        "--worker", choices=["primary", "indexed"], help=argparse.SUPPRESS
    )
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("The supplied syscall interposer requires macOS")
    args.directory = args.directory.resolve()
    args.directory.mkdir(parents=True, exist_ok=True)
    if args.worker:
        print(json.dumps(worker(args)))
        return
    if args.output.exists():
        parser.error("Output already exists; preserve earlier receipts")
    report = {
        "source_commit": args.source_commit,
        "library_sha256": sha256(args.library),
        "shim_sha256": sha256(args.shim),
        "script_sha256": sha256(__file__),
        "counter_source_sha256": sha256(Path(__file__).with_name("lite_io_counter.c")),
        "scope": "Synthetic operation counts, not elapsed performance qualification; reset_thread identifies execution thread, not semantic request ownership.",
        "status": "running",
        "cases": [],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    modes = ["primary", "indexed"] if args.mode == "both" else [args.mode]
    try:
        for mode in modes:
            env = dict(
                os.environ,
                DYLD_INSERT_LIBRARIES=str(args.shim.resolve()),
                ANTFLY_IO_PROFILE_DIR=str(args.directory),
            )
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--library",
                str(args.library.resolve()),
                "--shim",
                str(args.shim.resolve()),
                "--directory",
                str(args.directory),
                "--output",
                str(args.output.resolve()),
                "--engine",
                args.engine,
                "--worker",
                mode,
            ]
            result = subprocess.run(
                command, env=env, capture_output=True, text=True, timeout=300
            )
            if result.returncode:
                raise RuntimeError(f"{mode}: {result.stderr[-4000:]}")
            report["cases"].append(json.loads(result.stdout))
            if (
                sha256(args.library) != report["library_sha256"]
                or sha256(args.shim) != report["shim_sha256"]
            ):
                raise RuntimeError("A measured binary changed during the diagnostic")
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=repr(error))
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "cases": len(report["cases"])}))


if __name__ == "__main__":
    main()
