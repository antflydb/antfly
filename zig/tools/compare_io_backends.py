#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
"""Run equal-work I/O comparisons and retain every measured sample."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import platform
import statistics
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(args):
    result = subprocess.run(
        args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180
    )
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result.stdout + result.stderr


def records(output, allow_header=False):
    result = []
    for line in output.splitlines():
        if allow_header and line.startswith("LMDB benchmark "):
            continue
        result.append(json.loads(line))
    if not result:
        raise ValueError("Benchmark produced no samples")
    return result


def summaries(rows, backend_key, time_key, keys):
    groups = {}
    for row in rows:
        identity = tuple(row.get(key) for key in keys)
        groups.setdefault(identity, {}).setdefault(row[backend_key], []).append(
            row[time_key]
        )
    result = []
    for identity, backends in groups.items():
        if set(backends) != {"threaded", "evented"}:
            raise ValueError(f"Missing comparison for {identity}")
        medians = {name: statistics.median(values) for name, values in backends.items()}
        item = dict(zip(keys, identity))
        item.update(
            median_ns=medians,
            evented_over_threaded=medians["evented"] / medians["threaded"],
        )
        result.append(item)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", required=True, type=Path, help="io-backend-bench executable"
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument(
        "--docker-image", help="Run Linux executable on an anonymous disk-backed volume"
    )
    parser.add_argument("--lmdb-threaded", type=Path)
    parser.add_argument("--lmdb-evented", type=Path)
    args = parser.parse_args()
    if args.samples < 3 or bool(args.lmdb_threaded) != bool(args.lmdb_evented):
        parser.error("Use at least 3 samples and supply both LMDB executables together")
    if args.docker_image and args.lmdb_threaded:
        parser.error("LMDB comparison currently runs locally")

    host = {"platform": platform.platform(), "machine": platform.machine()}
    if platform.system() == "Darwin":
        host["cpu"] = command(["sysctl", "-n", "machdep.cpu.brand_string"]).strip()
    environment = {"host": host, "filesystem": "host temporary directory"}
    if args.docker_image:
        prefix = [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "seccomp=unconfined",
            "--memory",
            "512m",
            "--pids-limit",
            "128",
        ]
        environment.update(
            container_image=args.docker_image,
            kernel=command(prefix + [args.docker_image, "uname", "-a"]).strip(),
            filesystem="anonymous Docker volume on VM disk; host cache/storage shared",
            container_memory_bytes=512 * 1024 * 1024,
        )
        rows = records(
            command(
                prefix
                + [
                    "--mount",
                    "type=volume,target=/work",
                    "--mount",
                    f"type=bind,source={args.binary.resolve()},target=/bench,readonly",
                    args.docker_image,
                    "/bench",
                    "/work",
                    str(args.samples),
                ]
            )
        )
    else:
        with tempfile.TemporaryDirectory(prefix="antfly-io-compare-") as directory:
            rows = records(
                command([str(args.binary.resolve()), directory, str(args.samples)])
            )
    expected = {
        (backend, workload, concurrency, sample)
        for backend in ("threaded", "evented")
        for workload in ("cached_read", "durable_write")
        for concurrency in (1, 8, 32)
        for sample in range(args.samples)
    }
    actual = {
        (row["backend"], row["workload"], row["concurrency"], row["sample"])
        for row in rows
    }
    if actual != expected or len(rows) != len(expected):
        raise ValueError("Incomplete or duplicate positional benchmark matrix")
    for row in rows:
        if row["elapsed_ns"] <= 0 or row["operations"] != (
            16384 if row["workload"] == "cached_read" else 128
        ):
            raise ValueError("Unexpected operation count or elapsed time")

    lmdb_rows = []
    lmdb_flags = [
        "--samples",
        "1",
        "--cycles",
        "8",
        "--keys",
        "512",
        "--dups",
        "32",
        "--named-keys",
        "128",
        "--async-io",
        "--kv-only",
    ]
    if args.lmdb_threaded:
        binaries = {"threaded": args.lmdb_threaded, "evented": args.lmdb_evented}
        # Warm both binaries, then alternate independent process runs.
        for binary in binaries.values():
            command([str(binary.resolve())] + lmdb_flags)
        for sample in range(args.samples):
            order = (
                ("threaded", "evented") if sample % 2 == 0 else ("evented", "threaded")
            )
            for backend in order:
                current = records(
                    command([str(binaries[backend].resolve())] + lmdb_flags),
                    allow_header=True,
                )
                for row in current:
                    if (
                        row["backend"] != "zig"
                        or row["async_runtime"] != backend
                        or row["ns"] <= 0
                    ):
                        raise ValueError(
                            "LMDB executable did not exercise the requested runtime"
                        )
                    row["sample"] = sample
                lmdb_rows.extend(current)

    output = {
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip(),
        "environment": environment,
        "samples_per_case": args.samples,
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "fixture": {
            "block_size": 4096,
            "read_file_bytes": 64 * 1024 * 1024,
            "read_operations": 16384,
            "write_operations": 128,
            "fsync_after_each_write": True,
            "concurrency": [1, 8, 32],
            "order": "alternating backend order; one warmup per case",
        },
        "limitations": [
            "Reads use a warmed page cache, not controlled cold storage.",
            "fsync tests filesystem synchronization, not power-loss recovery or macOS F_FULLFSYNC.",
            "These I/O measurements do not establish Lite, LSM, full-text, or vector application performance.",
            "Linux Docker results reflect a VM filesystem, not bare-metal Linux storage.",
        ],
        "positional_summary": summaries(
            rows, "backend", "elapsed_ns", ("workload", "concurrency")
        ),
        "positional_samples": rows,
    }
    if lmdb_rows:
        output.update(
            lmdb_command_flags=lmdb_flags,
            lmdb_summary=summaries(
                lmdb_rows, "async_runtime", "ns", ("workload", "phase")
            ),
            lmdb_samples=lmdb_rows,
            lmdb_binary_sha256={
                name: hashlib.sha256(binary.read_bytes()).hexdigest()
                for name, binary in binaries.items()
            },
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    for row in output["positional_summary"]:
        median = row["median_ns"]
        print(
            f"{row['workload']:14} concurrency={row['concurrency']:2}: "
            f"Threaded={median['threaded'] / 1e6:.3f}ms Evented={median['evented'] / 1e6:.3f}ms "
            f"ratio={row['evented_over_threaded']:.2f}"
        )
    print(f"Saved raw samples and metadata to {args.output}")


if __name__ == "__main__":
    main()
