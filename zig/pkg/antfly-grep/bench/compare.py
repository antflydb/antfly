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

"""Reproducible throughput/RSS comparison; rg is an optional development oracle.

Run the native ReleaseFast binary. Each measurement runs in a fresh child and
collects that child's own resource use. Results include compiler/host metadata.
"""

import argparse
import hashlib
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


def measure(argv, cwd, env):
    load = list(os.getloadavg()) if hasattr(os, "getloadavg") else None
    start = time.perf_counter()
    proc = subprocess.Popen(
        argv, cwd=cwd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    if hasattr(os, "wait4"):
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        rss = usage.ru_maxrss / (1024 if platform.system() == "Darwin" else 1)
        counters = {
            "user_seconds": usage.ru_utime,
            "system_seconds": usage.ru_stime,
            "minor_faults": usage.ru_minflt,
            "major_faults": usage.ru_majflt,
        }
    else:
        proc.wait()
        rss = None
        counters = {}
    elapsed = time.perf_counter() - start
    error = proc.stderr.read().decode(errors="replace")
    proc.stderr.close()
    if proc.returncode not in (0, 1):
        raise RuntimeError(f"{argv}: {error}")
    return {
        "seconds": elapsed,
        "peak_rss_kib": rss,
        "exit": proc.returncode,
        "load_average": load,
        **counters,
    }


def first_result(argv, cwd, env):
    # Separate from throughput/RSS timing: observe one byte, then drain output.
    # The corpus and output validation are identical to the throughput run.
    start = time.perf_counter()
    with tempfile.TemporaryFile() as errors:
        proc = subprocess.Popen(
            argv, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=errors
        )
        first = proc.stdout.read(1)
        elapsed = time.perf_counter() - start if first else None
        while proc.stdout.read(256 * 1024):
            pass
        proc.stdout.close()
        proc.wait()
        if proc.returncode not in (0, 1):
            errors.seek(0)
            raise RuntimeError(errors.read().decode(errors="replace"))
    return elapsed


def output_digest(argv, cwd, env, unordered=False):
    # Correctness is checked outside timed/RSS measurements.
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(
            argv, cwd=cwd, env=env, stdout=output, stderr=subprocess.PIPE, check=False
        )
        if result.returncode not in (0, 1):
            raise RuntimeError(result.stderr.decode(errors="replace"))
        output.seek(0)
        digest = hashlib.sha256()
        size = 0
        if unordered:
            for line in sorted(output.readlines()):
                digest.update(line)
                size += len(line)
        else:
            while chunk := output.read(256 * 1024):
                digest.update(chunk)
                size += len(chunk)
        return {"sha256": digest.hexdigest(), "bytes": size, "exit": result.returncode}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("--mib", type=int, default=32)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--repository", type=Path, help="Also benchmark an existing source repository"
    )
    parser.add_argument("--repository-pattern", default="TODO|FIXME|retry")
    parser.add_argument(
        "--baseline", type=Path, help="also compare an earlier antfly-grep binary"
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        help="include dense counts, PCRE kernels and incompressible bzip2 data",
    )
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--case", action="append", help="measure only this named case (repeatable)"
    )
    parser.add_argument(
        "--codecs",
        action="store_true",
        help="also measure streamed gzip/bzip2/XZ input",
    )
    args = parser.parse_args()
    if args.mib < 1 or args.rounds < 1:
        parser.error("--mib and --rounds must be positive")
    binaries = {"antfly-grep": str(args.binary.resolve())}
    if shutil.which("rg"):
        binaries["ripgrep"] = shutil.which("rg")
    if args.baseline:
        binaries["baseline"] = str(args.baseline.resolve())
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("RIPGREP_", "ANTFLY_GREP_"))
    }
    result = {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
        "measurement": "elapsed and first-result latency use separate paired rounds with alternating binary order; correctness outside timing",
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
        "mib": args.mib,
        "rounds": args.rounds,
        "binaries": {},
        "cases": {},
    }
    for name, binary in binaries.items():
        result["binaries"][name] = {
            "path": binary,
            "size_bytes": Path(binary).stat().st_size,
            "sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
            "version": subprocess.check_output(
                [binary, "--version"], env=env, text=True
            ).strip(),
        }
    with tempfile.TemporaryDirectory(prefix="antfly-grep-bench-") as directory:
        root = Path(directory)
        env.update(
            HOME=str(root / "home"),
            XDG_CONFIG_HOME=str(root / "home" / "config"),
            NO_COLOR="1",
            TERM="dumb",
        )
        (root / "home").mkdir()
        row = b"2026-10-15 info worker processed request id=123456 status=success\n"
        block = row * max(1, (args.mib * 1024 * 1024) // len(row))
        (root / "large.log").write_bytes(block + b"ERRORfinal42 hit\n")
        (root / "unicode.log").write_bytes(
            ("Été Σίσυφος request42\n" * max(1, len(block) // 30)).encode()
        )
        (root / "small").mkdir()
        for i in range(1000):
            (root / "small" / f"{i:04}.txt").write_bytes(
                row * 4 + (b"ERROR42\n" if i % 100 == 0 else b"")
            )
        (root / "empty.log").write_bytes(b"")
        (root / "first.log").write_bytes(b"hit first\n" + block)
        cases = {
            "startup": ["-q", "-F", "absent", "empty.log"],
            "first-result": ["--line-buffered", "-F", "hit", "first.log"],
            "literal-miss": ["-F", "unfindable-literal", "large.log"],
            "regex-prefix-miss": ["absent[A-Z]+", "large.log"],
            "regex-prefix-hit": ["ERROR[a-zA-Z0-9]+", "large.log"],
            "casefold-literal": ["-i", "REQUEST", "large.log"],
            "unicode-word": ["-o", r"\p{Greek}+", "unicode.log"],
            "small-files": ["--sort", "path", "ERROR[0-9]+", "small"],
            "small-files-unsorted": ["-l", "ERROR[0-9]+", "small"],
            "buffered-literal": ["--no-mmap", "-F", "unfindable-literal", "large.log"],
            "buffered-regex": ["--no-mmap", "ERROR[a-zA-Z0-9]+", "large.log"],
            "boolean-dfa": ["(?:worker|request)[a-z =]+[0-9]+", "large.log"],
            "mmap-literal": ["--mmap", "-F", "unfindable-literal", "large.log"],
        }
        repository_bytes = 0
        if args.repository:
            repository = args.repository.resolve()
            listing = subprocess.run(
                [binaries["antfly-grep"], "--files", "--sort", "path", str(repository)],
                cwd=root,
                env=env,
                capture_output=True,
                check=False,
            )
            if listing.returncode not in (0, 1):
                raise RuntimeError(listing.stderr.decode(errors="replace"))
            corpus_hash = hashlib.sha256()
            file_count = 0
            for path in listing.stdout.splitlines():
                source_path = Path(os.fsdecode(path))
                data = source_path.read_bytes()
                repository_bytes += len(data)
                corpus_hash.update(
                    os.fsencode(str(source_path.relative_to(repository)))
                )
                corpus_hash.update(b"\0")
                corpus_hash.update(hashlib.sha256(data).digest())
                file_count += 1
            result["repository"] = {
                "name": repository.name,
                "files": file_count,
                "bytes": repository_bytes,
                "tree_sha256": corpus_hash.hexdigest(),
                "pattern": args.repository_pattern,
            }
            cases["repository-files"] = ["--files", str(repository)]
            cases["repository-search"] = [
                "-l",
                args.repository_pattern,
                str(repository),
            ]
        expanded_sizes = {}
        if args.codecs:
            import bz2
            import gzip
            import lzma

            source = (root / "large.log").read_bytes()
            for suffix, encode, command in [
                ("gz", gzip.compress, "gzip"),
                ("bz2", bz2.compress, "bzip2"),
                ("xz", lzma.compress, "xz"),
            ]:
                if "ripgrep" in binaries and shutil.which(command) is None:
                    continue
                path = "large." + suffix
                (root / path).write_bytes(encode(source))
                cases["stream-" + suffix] = ["-z", "ERROR[a-zA-Z0-9]+", path]
                expanded_sizes[path] = len(source)
        if args.extended:
            for name, row in [
                ("repeat.log", b"a" * 256 + b"z\n"),
                ("pcre-unicode.log", "Éabcdefgh123\n".encode()),
                ("subroutine.log", b"a" * 17 + b"\n"),
            ]:
                (root / name).write_bytes(row * max(1, len(block) // len(row)))
            cases.update(
                {
                    "pcre-repeat": ["-Pc", "a+z", "repeat.log"],
                    "pcre-captures": [
                        "-Pc",
                        r"(2026)-(10)-(15) (info) (worker) (processed) (request) id=(123456) status=(success)",
                        "large.log",
                    ],
                    "pcre-reset": [
                        "-Pc",
                        r"(id)=(123)(456)\K status=(success)",
                        "large.log",
                    ],
                    "pcre-unicode-chain": ["-Pic", "é[a-z]{8}\\d+", "pcre-unicode.log"],
                    "pcre-subroutine": ["-Pc", "(?<n>a)(?&n){16}", "subroutine.log"],
                }
            )
            (root / "dense.log").write_bytes(b"k " * (args.mib * 1024 * 512) + b"\n")
            cases["dense-fold-count"] = ["-i", "--count-matches", "k", "dense.log"]
            if args.codecs and ("ripgrep" not in binaries or shutil.which("bzip2")):
                source = random.Random(58472).randbytes(args.mib * 1024 * 1024) + b"\n"
                path = "entropy.bz2"
                (root / path).write_bytes(bz2.compress(source))
                cases["stream-bz2-entropy"] = [
                    "-az",
                    "-F",
                    "absent-literal-8c07f35d839d",
                    path,
                ]
                expanded_sizes[path] = len(source)
        if args.case:
            missing = set(args.case) - cases.keys()
            if missing:
                parser.error("unknown or disabled cases: " + ", ".join(sorted(missing)))
            cases = {name: cases[name] for name in args.case}
        for case, flags in cases.items():
            result["cases"][case] = {}
            source_bytes = (
                repository_bytes
                if case.startswith("repository-")
                else (
                    sum(p.stat().st_size for p in (root / "small").iterdir())
                    if flags[-1] == "small"
                    else (root / flags[-1]).stat().st_size
                )
            )
            source_bytes = expanded_sizes.get(flags[-1], source_bytes)
            expected = None
            outputs = {}
            for name, binary in binaries.items():
                output = output_digest(
                    [binary, "--color=never", *flags],
                    root,
                    env,
                    unordered=case == "small-files-unsorted"
                    or case.startswith("repository-"),
                )
                if expected is None:
                    expected = output
                elif output != expected:
                    raise RuntimeError(
                        f"output mismatch for {case}: {name}: {output} != {expected}"
                    )
                outputs[name] = output
                measure([binary, "--color=never", *flags], root, env)
            runs_by_binary = {name: [] for name in binaries}
            order = list(binaries)
            random.Random(case).shuffle(order)
            for iteration in range(args.rounds):
                for name in order if iteration % 2 == 0 else reversed(order):
                    runs_by_binary[name].append(
                        measure([binaries[name], "--color=never", *flags], root, env)
                    )
            first_by_binary = {name: [] for name in binaries}
            for iteration in range(args.rounds):
                for name in order if iteration % 2 == 0 else reversed(order):
                    first_by_binary[name].append(
                        first_result(
                            [binaries[name], "--color=never", *flags], root, env
                        )
                    )
            for name, runs in runs_by_binary.items():
                seconds = statistics.median(r["seconds"] for r in runs)
                rss = [r["peak_rss_kib"] for r in runs if r["peak_rss_kib"] is not None]
                cpu = [
                    r["user_seconds"] + r["system_seconds"]
                    for r in runs
                    if "user_seconds" in r
                ]
                first_runs = first_by_binary[name]
                observed = [value for value in first_runs if value is not None]
                result["cases"][case][name] = {
                    "first_result_seconds": (
                        statistics.median(observed) if observed else None
                    ),
                    "first_result_runs": first_runs,
                    "seconds": seconds,
                    "cpu_seconds": statistics.median(cpu) if cpu else None,
                    "output": outputs[name],
                    "mib_per_second": source_bytes / (1024 * 1024) / seconds,
                    "peak_rss_kib": max(rss) if rss else None,
                    "runs": runs,
                }
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    if not args.quiet:
        print(rendered, end="")


if __name__ == "__main__":
    main()
