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

"""Measure alternating frozen product revisions in one disposable worktree.

Dependencies are primed equivalently; compiler and generator caches are cold
before each revision, then one warm build follows. Compiler RSS/CPU and same-poll
aggregate samples come from the existing diagnostic collector. Diagnostic time
reports must run separately. A failed build never contributes a size/time win.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def order(pairs):
    return [
        side
        for i in range(pairs)
        for side in (
            ("baseline", "candidate") if i % 2 == 0 else ("candidate", "baseline")
        )
    ]


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def definitions(values):
    if any(
        not re.fullmatch(r"-D[a-z][a-z0-9-]*=[^\n\x00]+", value) for value in values
    ):
        raise ValueError("extra arguments must be explicit -Dname=value options")
    reserved = {
        "target",
        "cpu",
        "optimize",
        "strip",
        "antfly-version",
        "metal",
        "cuda",
        "system-blas",
    }
    if any(value[2:].split("=", 1)[0] in reserved for value in values):
        raise ValueError("extra arguments cannot override the fixed comparison profile")
    return values


def clear_outputs(tree):
    # Only called for the worktree this invocation created. Preserve immutable
    # downloaded packages, never compiler objects or previous installed products.
    for path in (
        tree / "zig/.zig-cache/local",
        tree / "zig/zig-out",
        tree / "rs/target",
    ):
        if path.exists():
            shutil.rmtree(path)
    global_cache = tree / "zig/.zig-cache/global"
    for name in ("o", "h", "z", "tmp"):
        path = global_cache / name
        if path.exists():
            shutil.rmtree(path)


class CacheLease:
    """Keep long-running compilers visible to age-based cache cleaners."""

    def __init__(self, tree):
        self.markers = []
        self.last_touch = 0.0
        for suffix in ("", "local", "global"):
            directory = tree / "zig/.zig-cache" / suffix
            directory.mkdir(parents=True, exist_ok=True)
            marker = directory / ".antfly-active-product-measurement"
            marker.write_text(str(os.getpid()) + "\n")
            self.markers.append(marker)

    def pulse(self):
        if any(not marker.is_file() for marker in self.markers):
            raise RuntimeError("active compiler cache was removed; comparison invalid")
        now = time.monotonic()
        if now - self.last_touch >= 5:
            for marker in self.markers:
                marker.touch(exist_ok=True)
            self.last_touch = now

    def close(self):
        for marker in self.markers:
            marker.unlink(missing_ok=True)


def run_group(command, *, cwd, env, output, timeout, cache_tree=None):
    lease = CacheLease(cache_tree) if cache_tree is not None else None
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except BaseException:
        if lease is not None:
            lease.close()
        raise
    deadline = time.monotonic() + timeout
    try:
        while True:
            if lease is not None:
                lease.pulse()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout)
            try:
                result = process.wait(timeout=min(1, remaining))
                if lease is not None:
                    lease.pulse()
                return result
            except subprocess.TimeoutExpired:
                continue
    except BaseException:
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
        raise
    finally:
        if lease is not None:
            lease.close()


def toolchain_profile(zig):
    text = subprocess.check_output([zig, "env"], text=True)
    match = re.search(
        r'^\s*\.lib_dir\s*=\s*("(?:[^"\\]|\\.)*"),\s*$', text, re.MULTILINE
    )
    if not match:
        raise ValueError("cannot identify the pinned Zig library directory")
    lib = Path(ast.literal_eval(match[1]))
    digest = hashlib.sha256()
    for path in sorted(p for p in lib.rglob("*") if p.is_file()):
        digest.update(
            str(path.relative_to(lib)).encode() + b"\0" + sha(path).encode() + b"\0"
        )
    sdk = {}
    if sys.platform == "darwin":
        directory = Path(
            subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        )
        sdk = {
            "path": str(directory),
            "settings_sha256": sha(directory / "SDKSettings.json"),
        }
    return {
        "zig_env": text,
        "zig_lib_sha256": digest.hexdigest(),
        "sdk": sdk,
        "cpu_count": os.cpu_count(),
        "cpu_affinity": sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None,
        "resource_policy": {
            key: os.environ.get(key)
            for key in ("ANTFLY_ZIG_MAX_RSS", "SOURCE_DATE_EPOCH")
        },
    }


def product_paths(tree):
    lib = tree / "zig/zig-out/lib"
    libraries = [
        p
        for p in (lib / "libantfly.dylib", lib / "libantfly.so", lib / "antfly.dll")
        if p.is_file()
    ]
    binary = tree / "zig/zig-out/bin/antfly"
    if not binary.exists():
        binary = binary.with_suffix(".exe")
    if not binary.is_file() or len(libraries) != 1:
        raise ValueError(
            "successful build must install exactly one server and one shared library"
        )
    return [binary, libraries[0]]


def valid_memory_capture(memory, phase):
    # A truly compiler-cold product build must sample its dominant compiler.
    # A short linker/helper sample alone cannot qualify compiler peak memory.
    return bool(memory.get("aggregate_is_same_poll")) and (
        phase == "warm"
        or memory.get("units", {}).get("storage_engine", {}).get("peak_rss_bytes", 0)
        > 0
    )


def metrics(runs):
    result = []
    for index in sorted({r["pair"] for r in runs}):
        cold = {
            r["side"]: r for r in runs if r["pair"] == index and r["phase"] == "cold"
        }
        if set(cold) != {"baseline", "candidate"} or any(
            r["exit_code"] != 0 or not r.get("capture_valid", False)
            for r in cold.values()
        ):
            result.append({"pair": index, "comparable": False})
            continue
        a, b = cold["baseline"], cold["candidate"]
        am, bm = a.get("compiler_memory", {}), b.get("compiler_memory", {})
        ap = am.get("sampled_aggregate_peak_bytes", 0)
        bp = bm.get("sampled_aggregate_peak_bytes", 0)
        au, bu = am.get("units", {}), bm.get("units", {})
        result.append(
            {
                "pair": index,
                "comparable": True,
                "elapsed_ratio": b["elapsed_s"] / a["elapsed_s"],
                "sampled_aggregate_peak_ratio": bp / ap
                if ap
                and bp
                and am.get("aggregate_is_same_poll")
                and bm.get("aggregate_is_same_poll")
                else None,
                "compiler_peak_byte_deltas": {
                    name: bu[name]["sampled_peak_rss_bytes"]
                    - au[name]["sampled_peak_rss_bytes"]
                    for name in sorted(au.keys() & bu.keys())
                },
                "baseline_only_compilers": sorted(au.keys() - bu.keys()),
                "candidate_only_compilers": sorted(bu.keys() - au.keys()),
                "product_byte_deltas": {
                    key: b["products"][key]["bytes"] - a["products"][key]["bytes"]
                    for key in a["products"]
                },
            }
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument(
        "--timeout",
        type=int,
        default=7200,
        help="seconds per build, including its descendants",
    )
    parser.add_argument("--target", default="native")
    parser.add_argument("--optimize", choices=("debug", "fast", "safe"), default="fast")
    parser.add_argument(
        "--jobs",
        type=int,
        help="omit for normal build scheduler; local screens may serialize",
    )
    parser.add_argument("--baseline-arg", action="append", default=[])
    parser.add_argument("--candidate-arg", action="append", default=[])
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--zig", default=shutil.which("zig"))
    parser.add_argument("--worktree-name", default="build-cost-paired-screen")
    parser.add_argument(
        "--scope", choices=("local-screen", "normal-runner"), default="local-screen"
    )
    parser.add_argument(
        "--dependency-cache",
        type=Path,
        help="copy immutable package downloads into the owned worktree, never share writable cache paths",
    )
    args = parser.parse_args()
    if not args.zig or args.pairs < 1 or args.jobs is not None and args.jobs < 1:
        parser.error("provide Zig, positive pairs and a positive optional jobs count")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.worktree_name):
        parser.error("worktree name must be one directory component")
    for values in (args.baseline_arg, args.candidate_arg):
        definitions(values)
    common = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=ROOT,
            text=True,
        ).strip()
    ).parent
    tree = common / ".worktrees" / args.worktree_name
    if tree.exists():
        parser.error(f"refusing to adopt or delete an existing worktree: {tree}")
    refs = {
        side: subprocess.check_output(
            ["git", "rev-parse", "--verify", ref + "^{commit}"], cwd=ROOT, text=True
        ).strip()
        for side, ref in (("baseline", args.baseline), ("candidate", args.candidate))
    }
    report_path = args.report.resolve()
    if report_path.is_relative_to(tree):
        parser.error("report must live outside the disposable worktree")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    logs = report_path.parent / (report_path.stem + "-logs")
    if logs.exists():
        parser.error(f"use a fresh report name: {logs}")
    logs.mkdir()
    shutil.copy2(__file__, logs / "measurement-tool.py")
    # Use identical, retained sampling and attribution code for both revisions,
    # including baselines predating the current collector protocol.
    collector_root = logs / "collector"
    collector_scripts = collector_root / "scripts"
    collector_scripts.mkdir(parents=True)
    collector_files = {}
    for name in ("diagnose-zig-build-memory.sh", "summarize_zig_compile_memory.py"):
        destination = collector_scripts / name
        shutil.copy2(ROOT / "scripts" / name, destination)
        collector_files[name] = sha(destination)
    collector = collector_scripts / "diagnose-zig-build-memory.sh"
    report = {
        "tool_sha256": sha(Path(__file__)),
        "collector_files_sha256": collector_files,
        "format_version": 1,
        "scope": args.scope,
        "qualification": False,
        "passed": False,
        "refs": refs,
        "target": args.target,
        "cpu": "baseline",
        "optimize": args.optimize,
        "strip": True,
        "jobs": args.jobs,
        "pairs": args.pairs,
        "host": platform.platform(),
        "zig": str(Path(args.zig).resolve()),
        "zig_sha256": sha(Path(args.zig)),
        "zig_version": subprocess.check_output(
            [args.zig, "version"], text=True
        ).strip(),
        "cache_profile": "immutable packages primed; worktree compiler/generator outputs reset, followed by warm build; OS page cache is not reset",
        "memory_scope": "scoped process samples; Linux cgroup before/after peaks retain collector scope labels",
        "runs": [],
    }
    report["toolchain"] = toolchain_profile(args.zig)

    def write():
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    write()
    created = False
    try:
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(tree), refs["baseline"]],
            cwd=ROOT,
            check=True,
        )
        created = True
        env = dict(
            os.environ,
            ZIG_LOCAL_CACHE_DIR=str(tree / "zig/.zig-cache/local"),
            ZIG_GLOBAL_CACHE_DIR=str(tree / "zig/.zig-cache/global"),
            CARGO_TARGET_DIR=str(tree / "rs/target"),
            PATH=str(Path(args.zig).resolve().parent) + os.pathsep + os.environ["PATH"],
        )
        if args.dependency_cache and args.dependency_cache.is_dir():
            shutil.copytree(
                args.dependency_cache.resolve(),
                tree / "zig/.zig-cache/global/p",
                dirs_exist_ok=True,
            )
            report["dependency_seed"] = "copied immutable downloads into owned cache"
        else:
            report["dependency_seed"] = (
                "no available seed; resolve pinned downloads during unmeasured priming"
            )
        common_args = [
            f"-Dtarget={args.target}",
            "-Dcpu=baseline",
            f"-Doptimize={args.optimize}",
            "-Dstrip=true",
            "-Dantfly-version=cost-profile",
            "-Dmetal=false",
            "-Dcuda=false",
            "-Dsystem-blas=false",
        ]
        for position, side in enumerate(order(args.pairs)):
            subprocess.run(
                ["git", "checkout", "--detach", refs[side]], cwd=tree, check=True
            )
            extra = args.baseline_arg if side == "baseline" else args.candidate_arg
            profile = common_args + extra
            with (logs / f"{position}-{side}-prime.log").open("w") as output:
                prime = run_group(
                    [args.zig, "build", "--help", *profile],
                    cwd=tree / "zig",
                    env=env,
                    output=output,
                    timeout=args.timeout,
                    cache_tree=tree,
                )
            if prime:
                raise RuntimeError(
                    f"dependency priming failed: {side}; inspect retained log"
                )
            subprocess.run(
                ["git", "diff", "--quiet", "HEAD", "--"], cwd=tree, check=True
            )
            clear_outputs(tree)
            for phase in ("cold", "warm"):
                label = f"pair{position // 2}-{side}-{phase}"
                build = [
                    sys.executable,
                    "tools/run_bounded_zig_build.py",
                    "--",
                    "build",
                    "antfly",
                    "capi",
                    "runtime-archives",
                    *profile,
                    "--summary",
                    "all",
                ]
                if args.jobs:
                    build.append(f"-j{args.jobs}")
                command = [
                    str(collector),
                    "--zig-dir",
                    str(tree / "zig"),
                    "--out-dir",
                    str(logs),
                    "--target",
                    args.target,
                    "--optimize",
                    args.optimize,
                    "--strip",
                    "true",
                    "--install-step",
                    "products",
                    "--no-stack-sample",
                    "--label",
                    label,
                    "--command",
                    "--",
                    *build,
                ]
                start = time.monotonic()
                with (logs / f"{label}-collector.log").open("w") as output:
                    code = run_group(
                        command,
                        cwd=tree,
                        env=env,
                        output=output,
                        timeout=args.timeout,
                        cache_tree=tree,
                    )
                row = {
                    "pair": position // 2,
                    "side": side,
                    "ref": refs[side],
                    "phase": phase,
                    "exit_code": code,
                    "elapsed_s": time.monotonic() - start,
                    "command": command,
                    "profile": profile,
                    "products": {},
                    "logs": str(logs),
                    "label": label,
                }
                if code == 0:
                    for product in product_paths(tree):
                        digest = sha(product)
                        artifact = logs / "products" / digest / product.name
                        artifact.parent.mkdir(parents=True, exist_ok=True)
                        if not artifact.exists():
                            shutil.copy2(product, artifact)
                        row["products"][
                            "antfly" if product.parent.name == "bin" else "libantfly"
                        ] = {
                            "bytes": product.stat().st_size,
                            "sha256": digest,
                            "artifact": str(artifact),
                        }
                    maps = sorted((tree / "zig/.zig-cache").rglob("libantfly.map"))
                    if maps:
                        shutil.copy2(
                            max(maps, key=lambda p: p.stat().st_mtime_ns),
                            logs / f"{label}-libantfly.map",
                        )
                row["tracked_source_clean"] = (
                    subprocess.run(
                        ["git", "diff", "--quiet", "HEAD", "--"], cwd=tree
                    ).returncode
                    == 0
                )
                memory_path = logs / (label + ".memory.json")
                row["memory_report"] = str(memory_path)
                row["capture_valid"] = code == 0 and memory_path.is_file()
                if memory_path.is_file():
                    memory = json.loads(memory_path.read_text())
                    row["compiler_memory"] = {
                        "units": {
                            name: {"sampled_peak_rss_bytes": value["peak_rss_bytes"]}
                            for name, value in memory["units"].items()
                        },
                        "sampled_aggregate_peak_bytes": memory[
                            "sampled_compiler_aggregate_peak_bytes"
                        ],
                        "aggregate_is_same_poll": memory["aggregate_is_same_poll"],
                    }
                    row["capture_valid"] = code == 0 and valid_memory_capture(
                        memory, phase
                    )
                row["collector_sha256"] = collector_files[
                    "diagnose-zig-build-memory.sh"
                ]
                report["runs"].append(row)
                if not row["tracked_source_clean"]:
                    raise RuntimeError(
                        "build changed tracked source; frozen comparison is invalid"
                    )
                report["comparisons"] = metrics(report["runs"])
                write()
                print(
                    f"{label}: exit={code}, elapsed={row['elapsed_s']:.2f}s", flush=True
                )
                if code:
                    break  # A failed cold build cannot produce a valid warm comparison.
        report["passed"] = len(report["runs"]) == 4 * args.pairs and all(
            r["exit_code"] == 0 and r["capture_valid"] for r in report["runs"]
        )
        report["repeated_normal_runner_capture_complete"] = (
            report["passed"]
            and args.scope == "normal-runner"
            and args.pairs >= 3
            and args.jobs is None
        )
        report["qualification_note"] = (
            "Capture completion is not acceptance: assess both product sizes, variance, per-compiler/aggregate memory, critical path and runtime regressions before enabling defaults."
        )
    except BaseException as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if created:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(tree)],
                cwd=ROOT,
                check=True,
            )
            subprocess.run(["git", "worktree", "prune"], cwd=ROOT, check=True)
        write()
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
