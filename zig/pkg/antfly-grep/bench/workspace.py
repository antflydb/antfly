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

"""Qualify pinned workspace queries against ordinary warm invocations.

Uses the explicitly supplied native inference fixture, never a live provider.
Capturing and refreshing are measured separately from pinned query latency.
"""

import argparse
import hashlib
import json
import os
import platform
import select
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


def signature(rows):
    return sorted(
        json.dumps(
            {
                key: row.get(key)
                for key in ("data", "probability", "scope", "line_number")
            },
            sort_keys=True,
        )
        for row in rows
        if row["type"] == "match"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--files", type=int, default=1024)
    parser.add_argument("--rounds", type=int, default=9)
    parser.add_argument(
        "--profile",
        type=Path,
        help="macOS sample output, sampled from this exact binary",
    )
    parser.add_argument("--profile-queries", type=int, default=100)
    parser.add_argument(
        "--trim-profile",
        type=Path,
        help="sample idle footprint after releasing optional workspace resources",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.files < 1 or args.rounds < 3 or args.profile_queries < 1:
        parser.error("positive file/query counts and at least three rounds required")
    binary, library = args.binary.resolve(), args.library.resolve()
    cache_root = Path(__file__).resolve().parents[3] / ".zig-cache"
    cache_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="workspace-bench-", dir=cache_root
    ) as temporary:
        root = Path(temporary)
        repository = args.repository.resolve() if args.repository else root / "src"
        if not args.repository:
            repository.mkdir()
            for i in range(args.files):
                (repository / f"f{i:05}.py").write_text(
                    f"def retry_{i}():\n" + "    STOP()\n" * 32
                )
        cwd = repository.parent if not args.repository else repository
        source_path = repository.name if not args.repository else "."
        env = dict(os.environ)
        for key in list(env):
            if key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG")):
                del env[key]
        env.update(
            HOME=str(root), XDG_CONFIG_HOME=str(root / "config"), TMPDIR=str(root)
        )
        query = [
            "--ask=retry",
            "--model=fixture",
            "--backend=local",
            f"--library={library}",
            f"--cache-dir={root / 'scores'}",
            "--json",
            "--no-refine",
            "--threads=1",
            "--max-result-files=15",
            "--max-requests=unlimited",
        ]
        ordinary = [str(binary), *query, "--max-files=0", source_path]
        cold_start = time.monotonic()
        qualified = subprocess.run(
            ordinary, cwd=cwd, env=env, capture_output=True, check=False, timeout=120
        )
        cold_seconds = time.monotonic() - cold_start
        if qualified.returncode not in (0, 1):
            raise RuntimeError(qualified.stderr.decode(errors="replace"))
        expected_exit = qualified.returncode
        qualified_rows = [json.loads(line) for line in qualified.stdout.splitlines()]
        expected = signature(qualified_rows)
        stat_keys = (
            "files",
            "bytes",
            "matches",
            "reading_leads",
            "occurrences",
            "files_with_matches",
            "regions_considered",
            "regions_evaluated",
            "errors",
            "invalid_utf8_files",
            "complete",
        )
        expected_stats = {key: qualified_rows[-1]["stats"][key] for key in stat_keys}
        start = time.monotonic()
        process = subprocess.Popen(
            [str(binary), "workspace", "serve", source_path],
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0,
        )
        profiler = None
        profiled_queries = 0

        pending = bytearray()
        cursor = 0

        def read():
            nonlocal cursor
            while True:
                end = pending.find(b"\n", cursor)
                if end >= 0:
                    line = bytes(pending[cursor:end])
                    cursor = end + 1
                    return json.loads(line)
                if cursor:
                    del pending[:cursor]
                    cursor = 0
                if not select.select([process.stdout], [], [], 120)[0]:
                    raise RuntimeError("workspace response deadline")
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError(f"workspace exited: {process.poll()}")
                pending.extend(chunk)

        try:
            ready = read()
            if ready["type"] != "workspace_ready":
                raise RuntimeError(ready)
            capture_seconds = time.monotonic() - start
            runs = {"ordinary": [], "workspace": []}
            request_id = 0

            def workspace_query():
                nonlocal request_id
                request_id += 1
                started = time.monotonic()
                process.stdin.write(
                    json.dumps(
                        {
                            "version": 1,
                            "id": request_id,
                            "op": "search",
                            "revision": ready["revision"],
                            "args": query,
                        }
                    ).encode()
                    + b"\n"
                )
                rows = []
                first = None
                while True:
                    row = read()
                    if row["type"] == "workspace_error":
                        raise RuntimeError(row)
                    if row["type"] in ("match", "file") and first is None:
                        first = time.monotonic() - started
                    if row["type"] == "workspace_end":
                        if row["exit_code"] != expected_exit:
                            raise RuntimeError(row)
                        break
                    rows.append(row)
                elapsed = time.monotonic() - started
                if signature(rows) != expected:
                    raise RuntimeError(
                        "workspace evidence differs from ordinary invocation"
                    )
                summary = next(row for row in rows if row["type"] == "summary")
                if {key: summary["stats"][key] for key in stat_keys} != expected_stats:
                    raise RuntimeError(
                        f"workspace scan statistics differ: expected={expected_stats!r} actual={summary['stats']!r}"
                    )
                if summary["requests"] or summary["stats"]["source_parses"]:
                    raise RuntimeError(
                        f"warm query evaluated or reparsed sources: requests={summary['requests']} parses={summary['stats']['source_parses']} files={summary['stats']['files']}"
                    )
                return {
                    "seconds": elapsed,
                    "first_result_seconds": first,
                    "summary": summary,
                }

            workspace_query()
            for trial in range(args.rounds):
                for kind in (
                    ("ordinary", "workspace")
                    if trial % 2 == 0
                    else ("workspace", "ordinary")
                ):
                    if kind == "workspace":
                        runs[kind].append(workspace_query())
                    else:
                        started = time.monotonic()
                        result = subprocess.run(
                            ordinary,
                            cwd=cwd,
                            env=env,
                            capture_output=True,
                            check=False,
                            timeout=120,
                        )
                        rows = [json.loads(line) for line in result.stdout.splitlines()]
                        elapsed = time.monotonic() - started
                        if (
                            result.returncode != expected_exit
                            or signature(rows) != expected
                        ):
                            raise RuntimeError("ordinary evidence changed")
                        runs[kind].append({"seconds": elapsed, "summary": rows[-1]})
            if args.profile:
                args.profile.parent.mkdir(parents=True, exist_ok=True)
                profiler = subprocess.Popen(
                    [
                        "sample",
                        str(process.pid),
                        "1",
                        "1",
                        "-file",
                        str(args.profile.resolve()),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                for _ in range(args.profile_queries):
                    workspace_query()
                    profiled_queries += 1
                    if profiler.poll() is not None:
                        break
                profiler.wait(timeout=10)
                if profiler.returncode:
                    raise RuntimeError("sample failed")
            trimmed = None
            if args.trim_profile:
                process.stdin.write(b'{"version":1,"id":0,"op":"trim"}\n')
                trimmed = read()
                if (
                    trimmed["type"] != "workspace_trimmed"
                    or trimmed["revision"] != ready["revision"]
                    or trimmed["retained_cache_bytes"]
                ):
                    raise RuntimeError(trimmed)
                args.trim_profile.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(
                    [
                        "sample",
                        str(process.pid),
                        "1",
                        "1",
                        "-file",
                        str(args.trim_profile.resolve()),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True,
                    timeout=10,
                )
            refresh_start = time.monotonic()
            process.stdin.write(b'{"version":1,"id":0,"op":"refresh"}\n')
            refreshed = read()
            refresh_seconds = time.monotonic() - refresh_start
            if (
                refreshed["type"] != "workspace_ready"
                or refreshed["revision"] != ready["revision"]
                or refreshed["preparations_built"]
            ):
                raise RuntimeError(
                    "unchanged refresh lost identity or reparsed sources"
                )
            report = {
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "fixture_library_sha256": hashlib.sha256(
                    library.read_bytes()
                ).hexdigest(),
                "version": subprocess.check_output(
                    [str(binary), "--version"], env=env, text=True
                ).strip(),
                "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
                "repository": str(args.repository)
                if args.repository
                else "synthetic Python corpus",
                "fixture_only": True,
                "rounds": args.rounds,
                "capture": ready,
                "ordinary_cold_seconds": cold_seconds,
                "capture_seconds": capture_seconds,
                "refresh_seconds": refresh_seconds,
                "refresh": refreshed,
                "runs": runs,
                "median_seconds": {
                    kind: statistics.median(run["seconds"] for run in samples)
                    for kind, samples in runs.items()
                },
                "median_engine_seconds": {
                    kind: statistics.median(
                        run["summary"]["data"]["elapsed_total"]["secs"]
                        + run["summary"]["data"]["elapsed_total"]["nanos"] / 1e9
                        for run in samples
                    )
                    for kind, samples in runs.items()
                },
                "profile_queries": profiled_queries,
                "trimmed": trimmed,
                "limitations": "Warm filesystem on this host, explicit immutable snapshot, native constant-score fixture. Pinned query latency excludes capture/refresh; ordinary latency includes process startup. Client elapsed includes consumer JSON decoding; engine elapsed includes output backpressure but excludes client decoding work and final summary serialization. Both paths read and return the same selected evidence. Retained memory excludes per-query scratch and the old generation during transactional refresh. No live-model quality, billing or independent Linux hardware claims.",
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdout.close()
            if profiler and profiler.poll() is None:
                profiler.kill()
                profiler.wait()


if __name__ == "__main__":
    main()
