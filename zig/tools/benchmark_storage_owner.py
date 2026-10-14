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

"""Compare model-free C API storage workloads in alternating isolated processes.

Run with stripped release libraries and no concurrent compiler workload. JSON
results belong outside the repository. Each process loads one library to avoid
symbol interposition between baseline and candidate.
"""

import argparse
import ctypes
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time


class Slice(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class Buffer(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]


def one_library(path, batches, batch_size, warmup):
    lib = ctypes.CDLL(str(Path(path).resolve()))
    lib.antfly_db_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.antfly_db_open.restype = ctypes.c_int
    lib.antfly_db_close.argtypes = [ctypes.c_void_p]
    lib.antfly_db_close.restype = None
    lib.antfly_buffer_free.argtypes = [ctypes.POINTER(Buffer)]
    lib.antfly_buffer_free.restype = None
    lib.antfly_last_error_detail.restype = ctypes.c_char_p
    for name in ["antfly_db_batch_json", "antfly_db_sql_json", "antfly_db_lookup_json"]:
        fn = getattr(lib, name)
        fn.argtypes = [ctypes.c_void_p, Slice, ctypes.POINTER(Buffer)]
        fn.restype = ctypes.c_int

    def checked(status):
        if status:
            raise RuntimeError(
                f"C API status {status}: {lib.antfly_last_error_detail()!r}"
            )

    def request(value):
        return ctypes.create_string_buffer(
            json.dumps(value, separators=(",", ":")).encode()
        )

    def call(name, handle, payload):
        out = Buffer()
        checked(
            getattr(lib, name)(
                handle,
                Slice(ctypes.cast(payload, ctypes.c_void_p), len(payload) - 1),
                ctypes.byref(out),
            )
        )
        try:
            return json.loads(ctypes.string_at(out.ptr, out.len)) if out.len else None
        finally:
            lib.antfly_buffer_free(ctypes.byref(out))

    result = {}
    for mode in ["document", "relational"]:
        with tempfile.TemporaryDirectory(prefix="antfly-storage-owner-") as directory:
            handle = ctypes.c_void_p()
            checked(lib.antfly_db_open(directory.encode(), ctypes.byref(handle)))
            try:
                if mode == "relational":
                    call(
                        "antfly_db_sql_json",
                        handle,
                        request(
                            {
                                "statement": "CREATE TABLE rows_bench(id TEXT PRIMARY KEY,n BIGINT,body TEXT)"
                            }
                        ),
                    )
                payloads = []
                for batch in range(warmup + batches):
                    rows = {
                        f"doc-{batch * batch_size + i:08d}": {
                            "id": f"doc-{batch * batch_size + i:08d}",
                            "n": i,
                            "body": "storage owner benchmark",
                        }
                        for i in range(batch_size)
                    }
                    if mode == "relational":
                        values = ",".join(
                            "('%s',%d,'%s')" % (key, row["n"], row["body"])
                            for key, row in rows.items()
                        )
                        payloads.append(
                            request(
                                {
                                    "statement": "INSERT INTO rows_bench(id,n,body) VALUES "
                                    + values
                                }
                            )
                        )
                    else:
                        payloads.append(
                            request({"inserts": rows, "sync_level": "write"})
                        )
                fn = (
                    "antfly_db_sql_json"
                    if mode == "relational"
                    else "antfly_db_batch_json"
                )
                count_key = "rows_affected" if mode == "relational" else "inserted"

                def execute(payload):
                    response = call(fn, handle, payload)
                    if response.get(count_key) != batch_size:
                        raise RuntimeError(f"insert-count mismatch: {response!r}")

                for payload in payloads[:warmup]:
                    execute(payload)
                start = time.perf_counter_ns()
                for payload in payloads[warmup:]:
                    execute(payload)
                elapsed = time.perf_counter_ns() - start
                last_key = f"doc-{(warmup + batches) * batch_size - 1:08d}"
                if mode == "relational":
                    response = call(
                        "antfly_db_sql_json",
                        handle,
                        request(
                            {
                                "statement": "SELECT COUNT(*) AS row_count FROM rows_bench"
                            }
                        ),
                    )
                    if int(response["rows"][0][0]) != (warmup + batches) * batch_size:
                        raise RuntimeError(f"SQL count mismatch: {response!r}")
                    response = call(
                        "antfly_db_sql_json",
                        handle,
                        request(
                            {
                                "statement": f"SELECT id FROM rows_bench WHERE id='{last_key}'"
                            }
                        ),
                    )
                else:
                    response = call(
                        "antfly_db_lookup_json",
                        handle,
                        ctypes.create_string_buffer(last_key.encode()),
                    )
                if last_key not in json.dumps(response):
                    raise RuntimeError(f"read-back mismatch: {response!r}")
                result[mode] = {
                    "elapsed_ns": elapsed,
                    "rows": batches * batch_size,
                    "rows_per_second": batches * batch_size * 1e9 / elapsed,
                }
            finally:
                lib.antfly_db_close(handle)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--library", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--rounds", type=int, default=4, help="runs per variant")
    parser.add_argument("--batches", type=int, default=1200)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--warmup-batches", type=int, default=64)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.rounds, args.batches, args.batch_size) < 1 or args.warmup_batches < 0:
        parser.error("counts must be positive and warmup must be nonnegative")
    if args.library:
        print(
            json.dumps(
                one_library(
                    args.library, args.batches, args.batch_size, args.warmup_batches
                )
            )
        )
        return
    if not args.baseline or not args.candidate:
        parser.error("--baseline and --candidate are required")
    samples = {"baseline": [], "candidate": []}
    for index in range(args.rounds):
        order = (
            ["baseline", "candidate"] if index % 2 == 0 else ["candidate", "baseline"]
        )
        for kind in order:
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--library",
                str(getattr(args, kind).resolve()),
                "--batches",
                str(args.batches),
                "--batch-size",
                str(args.batch_size),
                "--warmup-batches",
                str(args.warmup_batches),
            ]
            sample = json.loads(subprocess.check_output(command, text=True))
            samples[kind].append(sample)
            print(f"{kind} round {index + 1}: {sample}", file=sys.stderr, flush=True)
    summary = {}
    for mode in ["document", "relational"]:
        old = statistics.median(x[mode]["elapsed_ns"] for x in samples["baseline"])
        new = statistics.median(x[mode]["elapsed_ns"] for x in samples["candidate"])
        # Nearby pairs control host drift better than a ratio of independent
        # medians; retain both summaries so variability stays visible.
        paired = [
            100 * (new_sample[mode]["elapsed_ns"] / old_sample[mode]["elapsed_ns"] - 1)
            for old_sample, new_sample in zip(samples["baseline"], samples["candidate"])
        ]
        summary[mode] = {
            "baseline_median_ns": old,
            "candidate_median_ns": new,
            "time_change_percent": 100 * (new / old - 1),
            "paired_time_change_percent": paired,
            "paired_median_time_change_percent": statistics.median(paired),
        }
    result = {
        "host": platform.platform(),
        "libraries": {
            kind: {
                "path": str(getattr(args, kind).resolve()),
                "bytes": getattr(args, kind).stat().st_size,
            }
            for kind in samples
        },
        "workload": {
            "rounds": args.rounds,
            "batches": args.batches,
            "batch_size": args.batch_size,
            "warmup_batches": args.warmup_batches,
        },
        "samples": samples,
        "summary": summary,
    }
    encoded = json.dumps(result, indent=2)
    if args.output:
        args.output.write_text(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    main()
