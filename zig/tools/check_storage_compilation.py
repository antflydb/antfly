#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify source ownership with the real compiler, archives, and owner tests.

Unlike the inexpensive build-configuration fixtures, these checks compile the
production implementations. Run with zig-full or locally after boundary edits.
Every mutation lives in an isolated source overlay; the checkout is read only.
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import re
import signal
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

ZIG_ROOT = Path(__file__).resolve().parents[1]
ARCHIVES = {
    "antfly-storage-kernel",
    *(
        f"antfly-runtime-{unit}"
        for unit in (
            "cli",
            "distributed",
            "enrichment_compute",
            "serverless",
            "inference",
            "api_kernel",
        )
    ),
}
CONSUMERS = {
    "api-table-read-tests",
    "api-table-write-tests",
    "api-table-write-lifecycle-tests",
    "data-runtime-tests",
}
COMPILES = re.compile(r"compile (lib|test_obj|exe) (\S+) Debug \S+ (cached|success)\b")


def tree_rss(snapshot: str, root_pid: int) -> tuple[int, int]:
    """Return summed and largest RSS for this build, excluding other builds."""
    processes = {}
    for line in snapshot.splitlines():
        fields = line.split()
        if len(fields) == 3:
            pid, parent, rss_kib = map(int, fields)
            processes[pid] = (parent, rss_kib * 1024)
    descendants = {root_pid}
    while True:
        added = {pid for pid, (parent, _) in processes.items() if parent in descendants}
        if added <= descendants:
            break
        descendants |= added
    sizes = [rss for pid, (_, rss) in processes.items() if pid in descendants]
    return sum(sizes), max(sizes, default=0)


def measured_build(command: list[str], cwd: Path) -> tuple[int, str, dict]:
    """Sample concurrent RSS; wait4 accounts CPU for the entire waited tree."""
    started = time.monotonic()
    peak_tree = peak_process = samples = 0
    with tempfile.TemporaryFile(mode="w+") as output:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            while True:
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.monotonic() - started > 1800:
                    raise TimeoutError("production compilation exceeded 30 minutes")
                snapshot = subprocess.check_output(
                    ["ps", "-axo", "pid=,ppid=,rss="],
                    text=True,
                )
                tree, largest = tree_rss(snapshot, process.pid)
                peak_tree = max(peak_tree, tree)
                peak_process = max(peak_process, largest)
                samples += 1
                time.sleep(0.25)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
        output.seek(0)
        return (
            process.returncode,
            output.read(),
            {
                "wall_seconds": round(time.monotonic() - started, 3),
                "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 3),
                "peak_build_tree_rss_bytes": peak_tree,
                "peak_process_rss_bytes": peak_process,
                "rss_samples": samples,
                "rss_sample_interval_seconds": 0.25,
            },
        )


def link_children(source: Path, destination: Path) -> None:
    destination.mkdir()
    for child in source.iterdir():
        if child.name in {".git", ".worktrees", ".zig-cache", "zig-out"}:
            continue
        (destination / child.name).symlink_to(child, target_is_directory=child.is_dir())


def own(root: Path, relative: str) -> Path:
    path = root
    for component in Path(relative).parts[:-1]:
        path /= component
        if path.is_symlink():
            source = path.resolve()
            path.unlink()
            link_children(source, path)
    path = root / relative
    if path.is_symlink():
        source = path.resolve()
        path.unlink()
        shutil.copyfile(source, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--global-cache-dir")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    records = []
    if args.report:
        atexit.register(
            lambda: args.report.write_text(json.dumps(records, indent=2) + "\n")
        )
    with tempfile.TemporaryDirectory(prefix="antfly-storage-compilation-") as temporary:
        work = Path(temporary)
        root = work / "repo"
        link_children(ZIG_ROOT.parent, root)
        build_file = own(root, "zig/build.zig")
        shutil.copyfile(build_file, build_file.with_name("project_build.zig"))
        shutil.copyfile(ZIG_ROOT / "tools/fixtures/storage_compilation.zig", build_file)

        cases = (
            (
                "read coordination",
                "api/table_reads.zig",
                {"antfly-runtime-distributed"},
                (ARCHIVES - {"antfly-runtime-distributed"})
                | {"storage-owner-tests", "storage-owner-enrichment-tests"},
            ),
            (
                "write coordination",
                "api/table_writes.zig",
                {"antfly-runtime-distributed"},
                (ARCHIVES - {"antfly-runtime-distributed"})
                | {"storage-owner-tests", "storage-owner-enrichment-tests"},
            ),
            (
                "physical DB",
                "storage/db/db.zig",
                {"antfly-storage-kernel"},
                (ARCHIVES - {"antfly-storage-kernel"})
                | {
                    "storage-owner-tests",
                    "storage-owner-source-tests",
                    "storage-owner-enrichment-tests",
                }
                | CONSUMERS,
            ),
            (
                "physical local query",
                "storage/local_query.zig",
                {"antfly-storage-kernel"},
                (ARCHIVES - {"antfly-storage-kernel"})
                | {
                    "storage-owner-tests",
                    "storage-owner-source-tests",
                    "storage-owner-enrichment-tests",
                }
                | CONSUMERS,
            ),
            (
                "owner integration test",
                "storage/kernel_owner_test.zig",
                {"storage-owner-tests"},
                ARCHIVES,
            ),
            (
                "consumer test root",
                "api_table_reads_test_root.zig",
                {"api-table-read-tests"},
                ARCHIVES | (CONSUMERS - {"api-table-read-tests"}),
            ),
            (
                "storage contract",
                "storage/kernel_owner_abi.zig",
                {
                    "antfly-storage-kernel",
                    "antfly-runtime-distributed",
                    "storage-owner-tests",
                },
                {
                    "antfly-runtime-cli",
                    "antfly-runtime-enrichment_compute",
                    "antfly-runtime-inference",
                    "storage-owner-enrichment-tests",
                },
            ),
        )
        # Establish the overlay layout before the baseline. A mutation changes
        # file contents only, not symlink resolution or compiler source paths.
        for _, relative, _, _ in cases:
            own(root, f"zig/pkg/antfly/src/{relative}")

        def build(label: str) -> dict[str, str]:
            print(f"{label}: compiling production artifacts", flush=True)
            command = [
                args.zig,
                "build",
                "check-storage-compilation",
                "-Doptimize=Debug",
                "-Dmetal=false",
                "-Dsystem-blas=false",
                "-Donnx=false",
                "-Dantfly-version=compilation-check",
                f"-j{args.jobs}",
                "--summary",
                "all",
                "--color",
                "off",
                "--cache-dir",
                str(work / "cache"),
            ]
            if args.global_cache_dir:
                command += ["--global-cache-dir", args.global_cache_dir]
            returncode, output, measurements = measured_build(command, root / "zig")
            states = {
                ("link:" + name if kind == "exe" else name): state
                for kind, name, state in COMPILES.findall(output)
            }
            elapsed = measurements["wall_seconds"]
            records.append(
                {
                    "case": label,
                    **measurements,
                    "returncode": returncode,
                    "artifacts": states,
                }
            )
            if returncode:
                # Zig's failed compiler command can span hundreds of KB.
                print(
                    "\n".join(line for line in output.splitlines() if len(line) < 1000)
                )
                raise RuntimeError(f"{label}: build failed ({returncode})")
            missing = (ARCHIVES | CONSUMERS) - states.keys()
            if missing:
                raise AssertionError(
                    f"{label}: missing compiler results: {sorted(missing)}"
                )
            print(
                f"{label}: {elapsed}s; rebuilt: "
                + ", ".join(
                    name
                    for name, status in sorted(states.items())
                    if status == "success"
                ),
                flush=True,
            )
            return states

        build("cold")
        warm = build("warm")
        assert all(warm[name] == "cached" for name in ARCHIVES | CONSUMERS), warm
        for label, relative, rebuilt, cached in cases:
            path = own(root, f"zig/pkg/antfly/src/{relative}")
            # Keep earlier edits in this private overlay. Restoring one would
            # itself invalidate Zig's most recent manifest and confound the
            # next case, even if it restores bytes from the cold build.
            path.write_bytes(
                path.read_bytes() + b"\n// storage compilation ownership regression\n"
            )
            states = build(label)
            for name in rebuilt:
                assert states.get(name) == "success", (label, name, states)
            for name in cached:
                assert states.get(name) == "cached", (label, name, states)
            if label in {"physical DB", "physical local query"}:
                for name in CONSUMERS:
                    assert states.get("link:" + name) == "success", (
                        label,
                        name,
                        states,
                    )


if __name__ == "__main__":
    main()
