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

"""Verify source ownership with the real compiler, archives, and owner tests.

Unlike the inexpensive build-configuration fixtures, these checks compile the
production implementations. Run with zig-full or locally after boundary edits.
Every mutation lives in an isolated source overlay; the checkout is read only.
"""

from __future__ import annotations

import argparse
from collections import deque
import json
import os
import platform
import re
import signal
import shlex
import shutil
import subprocess
import tempfile
import time
import sys
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
COMPILES = re.compile(r"compile (lib|test_obj|exe) (\S+) debug \S+ (cached|success)\b")


# Cache manifests track literal imports even in unselected test bodies. Skip
# comments and strings, but deliberately retain imports inside tests/branches.
IMPORT_TOKENS = re.compile(
    r"//[^\n]*|(?m:^[ \t]*\\\\[^\n]*)|'(?:\\.|[^'\\])*'|"
    r'"(?:\\.|[^"\\])*"|'
    r'@import\s*\(\s*"(?P<path>[^"\\\n]+)"\s*\)'
)


def literal_import_path(root: Path, target: Path) -> list[Path] | None:
    """Find an authored relative-import path that invalidates a source owner."""
    root, target = root.resolve(), target.resolve()
    pending = deque([(root, [root])])
    seen = set()
    while pending:
        source, path = pending.popleft()
        if source == target:
            return path
        if source in seen or not source.is_file():
            continue
        seen.add(source)
        for token in IMPORT_TOKENS.finditer(source.read_text()):
            relative = token.group("path")
            if relative is not None and relative.endswith(".zig"):
                imported = (source.parent / relative).resolve()
                pending.append((imported, [*path, imported]))
    return None


def control_catalog_contents() -> str:
    """Exclude literal physical imports, including unused declarations.

    Named source capabilities are rebound by the consumer profile. Whether a
    helper's tests require a physical owner is a separate partitioning decision.
    """
    local = ZIG_ROOT / "pkg/antfly-embedded/src"
    physical = {
        local / path
        for path in ("storage/db/db.zig", "storage/query.zig", "storage/write.zig")
    }
    parsed = {}
    classified = {}

    def requires_physical(root):
        pending = [root]
        seen = set()
        while pending:
            path = pending.pop()
            if path in physical:
                classified[root] = True
                return True
            if path in seen or path == local / "storage/db/selected_root.zig":
                continue
            seen.add(path)
            if path in classified:
                if classified[path]:
                    classified[root] = True
                    return True
                continue
            if path not in parsed:
                text = path.read_text()
                parsed[path] = [
                    (path.parent / relative).resolve()
                    for token in IMPORT_TOKENS.finditer(text)
                    if (relative := token.group("path")) is not None
                    and relative.endswith(".zig")
                ]
            pending.extend(path for path in parsed[path] if path.is_relative_to(local))
        for path in seen:
            classified[path] = False
        return False

    output = []
    names = set()
    for line in (local / "source_catalog.zig").read_text().splitlines(keepends=True):
        declaration = re.fullmatch(
            r'pub const (?P<name>\w+) = @import\("(?P<path>[^"\n]+\.zig)"\);\n?', line
        )
        if declaration:
            name = declaration["name"]
            if name in names:
                raise ValueError(f"duplicate source catalog entry: {name}")
            names.add(name)
        if declaration and requires_physical((local / declaration["path"]).resolve()):
            continue
        output.append(line)
    output.insert(
        16,
        "//! Generated by tools/check_storage_compilation.py --write-control-catalog.\n",
    )
    # Removing imports must not leave blank lines that zig fmt rewrites.
    return re.sub(r"\n{3,}", "\n\n", "".join(output))


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


def process_diagnostics(
    snapshot: str, root_pid: int, cwd: Path | None = None
) -> list[dict]:
    """Attribute stalled work without retaining enormous compiler command lines."""
    processes = {}
    for line in snapshot.splitlines():
        fields = line.split(None, 6)
        if len(fields) != 7:
            continue
        pid, parent, rss, elapsed, cpu, state, command = fields
        try:
            processes[int(pid)] = (
                int(parent),
                int(rss) * 1024,
                elapsed,
                cpu,
                state,
                command,
            )
        except ValueError:
            continue
    descendants = {root_pid}
    while True:
        added = {pid for pid, row in processes.items() if row[0] in descendants}
        if added <= descendants:
            break
        descendants |= added
    result = []
    for pid in sorted(descendants & processes.keys()):
        parent, rss, elapsed, cpu, state, command = processes[pid]
        try:
            arguments = shlex.split(command)
        except ValueError:
            arguments = command.split()
        expanded = []
        for argument in arguments:
            if (
                cwd is not None
                and argument.startswith("@")
                and Path(arguments[0]).name == "zig"
            ):
                response = cwd / argument[1:]
                try:
                    if response.stat().st_size <= 4 * 1024 * 1024:
                        expanded.extend(shlex.split(response.read_text()))
                        continue
                except (OSError, UnicodeError, ValueError):
                    pass
            expanded.append(argument)
        artifact = None
        if "--name" in expanded:
            index = expanded.index("--name") + 1
            if index < len(expanded):
                artifact = expanded[index]
        source = next(
            (arg[len("-Mroot=") :] for arg in expanded if arg.startswith("-Mroot=")),
            None,
        )
        result.append(
            {
                "pid": pid,
                "parent_pid": parent,
                "rss_bytes": rss,
                "elapsed": elapsed,
                "cpu_time": cpu,
                "state": state,
                "executable": Path(command.split()[0]).name,
                "artifact": artifact,
                "source_root": source,
            }
        )
    return result


def active_build_processes(root_pid: int, cwd: Path) -> list[dict]:
    try:
        snapshot = subprocess.check_output(
            ["ps", "-axo", "pid=,ppid=,rss=,etime=,time=,state=,args="],
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return []
    return process_diagnostics(snapshot, root_pid, cwd)


# Immutable Nix compiler from native run 40380791678 (glibc-linked Zig 0.17.0).
# Its complete stripped Debug audit peaked at 7,656,402,944 bytes for the whole
# process tree. The audit-only 9 GiB cap retains >25% above that sampled peak
# and leaves 2 GiB for auxiliary claims inside the existing 20 GiB budget.
# A different derivation must be measured before opting into this profile.
QUALIFIED_AUDIT_COMPILER = Path(
    "/nix/store/3swwykg6la02jblm03z3s4fxhiqdmpsm-zig-0.17.0/bin/zig"
)


def qualified_audit_memory(zig: str, jobs: int = 2) -> bool:
    if jobs != 2 or sys.platform != "linux" or platform.machine() != "x86_64":
        return False
    executable = shutil.which(zig)
    return (
        executable is not None
        and Path(executable).resolve() == QUALIFIED_AUDIT_COMPILER
    )


def cgroup_memory_stat() -> dict[str, int]:
    """Separate anonymous compiler memory from reclaimable cached file pages."""
    if sys.platform != "linux":
        return {}
    try:
        fields = dict(
            line.split()
            for line in Path("/sys/fs/cgroup/memory.stat").read_text().splitlines()
        )
        return {
            key: int(fields[key])
            for key in (
                "anon",
                "file",
                "file_mapped",
                "file_dirty",
                "inactive_file",
                "kernel",
            )
            if key in fields
        }
    except (OSError, ValueError):
        return {}


def bounded_build_command(zig: str, arguments: list[str]) -> list[str]:
    return [
        sys.executable,
        str(ZIG_ROOT / "tools/run_bounded_zig_build.py"),
        "--zig",
        zig,
        "--",
        *arguments,
    ]


def write_report(path: Path, records: list[dict]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(records, indent=2) + "\n")
    temporary.replace(path)


class CompilerWatchdog:
    """Bound each compiler and idle orchestration, independent of graph size."""

    def __init__(self, seconds: float, started: float):
        self.seconds = seconds
        self.last_work = started
        self.first_seen: dict[int, float] = {}

    def expired(self, now: float, processes: list[dict]) -> bool:
        compilers = [p for p in processes if p.get("executable") == "zig"]
        live = {p["pid"] for p in compilers}
        self.first_seen = {
            pid: start for pid, start in self.first_seen.items() if pid in live
        }
        if compilers:
            self.last_work = now
        for process in compilers:
            started = self.first_seen.setdefault(process["pid"], now)
            elapsed = process.get("elapsed", "")
            try:
                days, _, clock = elapsed.rpartition("-")
                seconds = 0
                for component in clock.split(":"):
                    seconds = seconds * 60 + int(component)
                seconds += int(days or 0) * 86400
            except ValueError:
                seconds = now - started
            if seconds >= self.seconds:
                return True
        return not compilers and now - self.last_work >= self.seconds


def measured_build(
    command: list[str],
    cwd: Path,
    *,
    timeout_seconds: float = 1800,
    compiler_timeout_seconds: float | None = None,
    progress=None,
    env=None,
) -> tuple[int, str, dict]:
    """Sample concurrent RSS; wait4 accounts CPU for the entire waited tree."""
    started = time.monotonic()
    peak_tree = peak_process = samples = 0
    min_disk_free = shutil.disk_usage(cwd).free
    timed_out = False
    active_processes = []
    cgroup = {}
    peak_anon = peak_file = None
    last_report = started
    watchdog = (
        CompilerWatchdog(compiler_timeout_seconds, started)
        if compiler_timeout_seconds is not None
        else None
    )
    with tempfile.TemporaryFile(mode="w+") as output:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
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
                now = time.monotonic()
                if not timed_out and now - started > timeout_seconds:
                    timed_out = True
                    # Capture before killing: wait4 cannot recover compiler CPU
                    # clocks or the active artifact after its parent is killed.
                    active_processes = active_build_processes(process.pid, cwd)
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                snapshot = subprocess.check_output(
                    ["ps", "-axo", "pid=,ppid=,rss="],
                    text=True,
                )
                tree, largest = tree_rss(snapshot, process.pid)
                peak_tree = max(peak_tree, tree)
                peak_process = max(peak_process, largest)
                min_disk_free = min(min_disk_free, shutil.disk_usage(cwd).free)
                samples += 1
                if now - last_report >= 30:
                    cgroup = cgroup_memory_stat()
                    if "anon" in cgroup:
                        peak_anon = max(peak_anon or 0, cgroup["anon"])
                    if "file" in cgroup:
                        peak_file = max(peak_file or 0, cgroup["file"])
                    if not timed_out:
                        active_processes = active_build_processes(process.pid, cwd)
                        if watchdog is not None and watchdog.expired(
                            now, active_processes
                        ):
                            timed_out = True
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                    current = {
                        "active_processes": active_processes,
                        "cgroup_memory_stat": cgroup,
                        "peak_sampled_cgroup_anon_bytes": peak_anon,
                        "peak_sampled_cgroup_file_bytes": peak_file,
                        "wall_seconds": round(now - started, 3),
                        "peak_build_tree_rss_bytes": peak_tree,
                        "peak_process_rss_bytes": peak_process,
                        "min_disk_free_bytes": min_disk_free,
                        "rss_samples": samples,
                    }
                    print(f"compilation progress: {current}", flush=True)
                    if progress is not None:
                        progress(current)
                    last_report = now
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
                "cpu_accounting_complete": not timed_out,
                "active_processes": active_processes,
                "cgroup_memory_stat": cgroup,
                "peak_sampled_cgroup_anon_bytes": peak_anon,
                "peak_sampled_cgroup_file_bytes": peak_file,
                "cgroup_sample_interval_seconds": 30,
                "peak_build_tree_rss_bytes": peak_tree,
                "peak_process_rss_bytes": peak_process,
                "rss_samples": samples,
                "rss_sample_interval_seconds": 0.25,
                "min_disk_free_bytes": min(min_disk_free, shutil.disk_usage(cwd).free),
                "timed_out": timed_out,
                "build_timeout_seconds": timeout_seconds,
                "compiler_timeout_seconds": compiler_timeout_seconds,
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


def source_owner_path(relative: str) -> str:
    embedded = {
        "storage/db/db.zig",
        "storage/query.zig",
        "storage/kernel_owner_abi.zig",
    }
    owner = "antfly-embedded/src" if relative in embedded else "antfly/src"
    return f"zig/pkg/{owner}/{relative}"


def seed_global_cache(destination: Path) -> None:
    """Reuse pinned source inputs, never compiled objects, in a cold audit cache."""
    destination.mkdir()
    source = os.environ.get("ZIG_GLOBAL_CACHE_DIR")
    if not source:
        return
    packages = Path(source) / "p"
    if not packages.is_dir():
        return
    inventory = json.loads(
        (ZIG_ROOT.parent / "scripts/ci/zig-dependencies.json").read_text()
    )
    for package in inventory["packages"]:
        name = package["hash"] + ".tar.gz"
        archive = packages / name
        if archive.is_file():
            (destination / "p").mkdir(exist_ok=True)
            shutil.copyfile(archive, destination / "p" / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--report", type=Path)
    catalog_mode = parser.add_mutually_exclusive_group()
    catalog_mode.add_argument("--write-control-catalog", action="store_true")
    catalog_mode.add_argument("--check-control-catalog", action="store_true")
    args = parser.parse_args()
    if args.write_control_catalog:
        (ZIG_ROOT / "pkg/antfly-embedded/src/source_catalog_control.zig").write_text(
            control_catalog_contents()
        )
        return
    if args.check_control_catalog:
        path = ZIG_ROOT / "pkg/antfly-embedded/src/source_catalog_control.zig"
        if path.read_text() != control_catalog_contents():
            parser.exit(
                1,
                "Stale control-source catalog; run make -C zig source-catalog-generate\n",
            )
        return
    records = []
    measured_memory = qualified_audit_memory(args.zig, args.jobs)
    if args.jobs <= 0:
        parser.error("--jobs must be positive")

    def publish():
        if args.report:
            write_report(args.report, records)

    publish()
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
                # A retained multi-range cut leases its DB generations inside
                # the physical owner and reuses the canonical read merger.
                # Both owners must therefore invalidate this shared source.
                {"antfly-runtime-distributed", "antfly-storage-kernel"},
                (ARCHIVES - {"antfly-runtime-distributed", "antfly-storage-kernel"})
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
                "storage/query.zig",
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
            own(root, source_owner_path(relative))
        local_cache = work / "cache"
        global_cache = work / "global-cache"
        seed_global_cache(global_cache)

        def build(label: str) -> dict[str, str]:
            print(f"{label}: compiling production artifacts", flush=True)
            arguments = [
                "build",
                "check-storage-compilation",
                "-Doptimize=debug",
                # Ownership and cache invalidation depend on Debug code and
                # imports, not debug symbols. Retaining symbols across source
                # mutations fills the PVC and forces repeated cold rebuilds.
                "-Dstrip=true",
                "-Dmetal=false",
                "-Dsystem-blas=false",
                "-Dantfly-version=compilation-check",
                f"-j{args.jobs}",
                "--summary",
                "all",
                "--color",
                "off",
                "--cache-dir",
                str(local_cache),
            ]
            if measured_memory:
                # Keep every qualified axis explicit; other compiler/profile
                # combinations retain the production reservations unchanged.
                arguments.extend(
                    [
                        "-Dnative-linux-ownership-memory=true",
                        "-Dcpu=baseline",
                        "-Dcuda=false",
                        "-Dinference-optimize=debug",
                    ]
                )
            command = bounded_build_command(args.zig, arguments)
            record = {
                "case": label,
                "memory_profile": "nix-glibc-0.17-stripped-debug"
                if measured_memory
                else "production",
                "status": "running",
                "command": command,
                "disk_free_before_bytes": shutil.disk_usage(work).free,
            }
            records.append(record)
            publish()

            def progress(current):
                record.update(current)
                publish()

            try:
                returncode, output, measurements = measured_build(
                    command,
                    root / "zig",
                    progress=progress,
                    env={**os.environ, "ZIG_GLOBAL_CACHE_DIR": str(global_cache)},
                    # The cold graph has several independent archives and
                    # owner/consumer executables. A newly started compiler is
                    # not stalled because earlier units consumed 30 minutes.
                    # Keep the original bound per compiler/idle period, plus
                    # an absolute one-hour bound for the complete cold graph.
                    timeout_seconds=3600 if label.endswith("cold") else 1800,
                    compiler_timeout_seconds=1800,
                )
            except BaseException as err:
                record.update(status="interrupted", error=type(err).__name__)
                publish()
                raise
            states = {
                ("link:" + name if kind == "exe" else name): state
                for kind, name, state in COMPILES.findall(output)
            }
            elapsed = measurements["wall_seconds"]
            record.update(
                {
                    **measurements,
                    "disk_free_after_bytes": shutil.disk_usage(work).free,
                    "status": "failed" if returncode else "completed",
                    "returncode": returncode,
                    "artifacts": states,
                }
            )
            publish()
            if returncode:
                if measurements["timed_out"]:
                    print(
                        f"{label}: production compilation exceeded its build or compiler deadline",
                        flush=True,
                    )
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

        def restart_cache(reason: str) -> None:
            # Remove Zig's objects and manifests together. A manifest retained
            # without its output can produce a false cache hit. The source
            # overlay stays at the same path with earlier edits still applied.
            for directory in (local_cache, global_cache):
                if directory.exists():
                    shutil.rmtree(directory)
            seed_global_cache(global_cache)
            build(f"{reason} cold")
            warmed = build(f"{reason} warm")
            assert all(warmed[name] == "cached" for name in ARCHIVES | CONSUMERS), (
                warmed
            )

        for label, relative, rebuilt, cached in cases:
            # Keep the measured warm cache across mutations. A second cold
            # build adds a full production compile without testing another
            # ownership property. Roll over only under real disk pressure.
            if shutil.disk_usage(work).free < 12 * (1 << 30):
                restart_cache(label)
            path = own(root, source_owner_path(relative))
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
