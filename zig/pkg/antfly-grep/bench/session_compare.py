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

"""Alternate equivalent warm queries between two revision-pinned services.

Native fixture only. Both services stay resident; reports retain every paired
trial and flag host contention rather than presenting sequential runs as wins.
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

from workspace import signature


class Service:
    def __init__(self, binary, cwd, source, query, env, start_args=()):
        self.query = query
        self.pending = bytearray()
        self.cursor = 0
        self.wire_bytes = 0
        self.process = subprocess.Popen(
            [str(binary), "workspace", "serve", *start_args, source],
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0,
        )
        started = time.monotonic()
        self.ready = self.read()
        self.capture_seconds = time.monotonic() - started
        if self.ready["type"] != "workspace_ready":
            raise RuntimeError(self.ready)

    def _response_too_large(self):
        # Stop the owned producer rather than blocking on an unconsumed pipe.
        self.process.kill()
        self.process.wait(timeout=5)
        raise RuntimeError("workspace response budget exceeded")

    def read(self):
        response_limit = getattr(self, "max_response_bytes", None)
        response_start = getattr(self, "response_start", 0)

        deadline = time.monotonic() + 120
        while True:
            end = self.pending.find(b"\n", self.cursor)
            if end >= 0:
                if (
                    response_limit is not None
                    and self.wire_bytes - response_start + end + 1 - self.cursor
                    > response_limit
                ):
                    self._response_too_large()
                line = bytes(self.pending[self.cursor : end])
                self.wire_bytes += end + 1 - self.cursor
                self.cursor = end + 1
                self.last_record = line
                return json.loads(line)
            if self.cursor:
                del self.pending[: self.cursor]
                self.cursor = 0
            remaining = deadline - time.monotonic()
            if (
                remaining <= 0
                or not select.select([self.process.stdout], [], [], remaining)[0]
            ):
                raise RuntimeError("workspace response timeout")
            data = os.read(self.process.stdout.fileno(), 65536)
            if not data:
                raise RuntimeError("workspace closed unexpectedly")
            if (
                response_limit is not None
                and self.wire_bytes - response_start + len(self.pending) + len(data)
                > response_limit
            ):
                self._response_too_large()
            self.pending.extend(data)

    def search(self, token=None, query=None, require_warm=True):
        request = {
            "version": 1,
            "id": 1,
            "op": "search",
            "revision": self.ready["revision"],
            "args": self.query if query is None else query,
        }
        if token is not None:
            request["if_result_token"] = token
        started = time.monotonic()
        before_bytes = self.wire_bytes
        self.response_start = before_bytes
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()
        rows = []
        while True:
            row = self.read()
            if row["type"] == "workspace_error":
                raise RuntimeError(row)
            if row["type"] == "workspace_end":
                if row["exit_code"] not in (0, 1):
                    raise RuntimeError(row)
                break
            rows.append(row)
        elapsed = time.monotonic() - started
        summary = next(row for row in rows if row["type"] == "summary")
        if require_warm and (
            summary["requests"]
            or summary["stats"]["source_parses"]
            or not summary["stats"]["complete"]
        ):
            raise RuntimeError(summary)
        self.rows = rows
        native = summary["data"]["elapsed_total"]
        return {
            "seconds": elapsed,
            "producer_seconds": native["secs"] + native["nanos"] / 1e9,
            "response_bytes": self.wire_bytes - before_bytes,
            "summary": summary,
        }, signature(rows)

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.process.stdout.close()


def cpu_seconds(pid):
    # Linux reports process user/system ticks, including its worker threads.
    # Darwin ps has hundredth-second precision; qualify repeated request batches
    # rather than interpreting one sub-millisecond call as a CPU measurement.
    if platform.system() == "Linux":
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    if platform.system() != "Darwin":
        return None
    value = subprocess.check_output(
        ["/bin/ps", "-p", str(pid), "-o", "time="], text=True
    ).strip()
    parts = [float(part) for part in value.split(":")]
    result = 0.0
    for part in parts:
        result = result * 60 + part
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--files", type=int, default=1024)
    parser.add_argument("--rounds", type=int, default=51)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 9 or args.files < 1:
        parser.error("at least nine rounds and one file required")
    binaries = {
        "baseline": args.baseline.resolve(),
        "candidate": args.candidate.resolve(),
    }
    cache_root = Path(__file__).resolve().parents[3] / ".zig-cache"
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_"))
    }
    with tempfile.TemporaryDirectory(
        prefix="paired-session-", dir=cache_root
    ) as directory:
        root = Path(directory)
        source = args.repository.resolve() if args.repository else root / "src"
        if not args.repository:
            source.mkdir()
            for i in range(args.files):
                (source / f"f{i:05}.py").write_text(
                    f"def retry_{i}():\n" + "    STOP()\n" * 32
                )
        cwd, source_path = source.parent, source.name
        env["HOME"] = str(root)
        query = [
            "--ask=retry",
            "--model=fixture",
            "--backend=local",
            f"--library={args.library.resolve()}",
            f"--cache-dir={root / 'scores'}",
            "--json",
            "--no-refine",
            "--threads=1",
            "--max-result-files=15",
            "--max-requests=unlimited",
        ]
        qualified = subprocess.run(
            [str(binaries["baseline"]), *query, "--max-files=0", source_path],
            cwd=cwd,
            env=env,
            capture_output=True,
            timeout=120,
        )
        if qualified.returncode not in (0, 1):
            raise RuntimeError(qualified.stderr.decode(errors="replace"))
        expected = signature([json.loads(row) for row in qualified.stdout.splitlines()])
        services = {}
        try:
            for name, binary in binaries.items():
                services[name] = Service(binary, cwd, source_path, query, env)
                _, evidence = services[name].search()
                if evidence != expected:
                    raise RuntimeError("warmup evidence differs")
            before = {
                name: cpu_seconds(service.process.pid)
                for name, service in services.items()
            }
            runs = []
            for trial in range(args.rounds):
                pair = {"load_average": os.getloadavg()}
                for name in (
                    ("baseline", "candidate")
                    if trial % 2 == 0
                    else ("candidate", "baseline")
                ):
                    result, evidence = services[name].search()
                    if evidence != expected:
                        raise RuntimeError("paired evidence differs")
                    pair[name] = result

                def logical_stats(summary):
                    return {
                        key: value
                        for key, value in summary["stats"].items()
                        if key
                        not in (
                            "query_cache_hits",
                            "file_evaluation_cache_hits",
                            "incremental_cache_hits",
                            "incremental_files_evaluated",
                            "source_parses",
                            "source_cache_hits",
                        )
                    }

                if logical_stats(pair["baseline"]["summary"]) != logical_stats(
                    pair["candidate"]["summary"]
                ):
                    raise RuntimeError("paired scan statistics differ")
                runs.append(pair)
            cpu = {
                name: (
                    (cpu_seconds(service.process.pid) - before[name]) / args.rounds
                    if before[name] is not None
                    else None
                )
                for name, service in services.items()
            }
            medians = {
                name: statistics.median(pair[name]["seconds"] for pair in runs)
                for name in binaries
            }
            mad = {
                name: statistics.median(
                    abs(pair[name]["seconds"] - medians[name]) for pair in runs
                )
                / medians[name]
                for name in binaries
            }
            report = {
                "fixture_only": True,
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "rounds": args.rounds,
                "binaries": {
                    name: {
                        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                        "version": subprocess.check_output(
                            [str(binary), "--version"], text=True
                        ).strip(),
                    }
                    for name, binary in binaries.items()
                },
                "library_sha256": hashlib.sha256(args.library.read_bytes()).hexdigest(),
                "repository": str(args.repository)
                if args.repository
                else "synthetic-1024"
                if args.files == 1024
                else f"synthetic-{args.files}",
                "captures": {
                    name: {"seconds": service.capture_seconds, "ready": service.ready}
                    for name, service in services.items()
                },
                "median_seconds": medians,
                "cpu_seconds_per_query": cpu,
                "relative_mad": mad,
                "high_variance": any(value > 0.2 for value in mad.values()),
                "median_paired_speedup": statistics.median(
                    pair["baseline"]["seconds"] / pair["candidate"]["seconds"]
                    for pair in runs
                ),
                "verified_scan_stats": runs[0]["baseline"]["summary"]["stats"],
                "runs": [
                    {
                        "load_average": pair["load_average"],
                        **{
                            name: {
                                "seconds": pair[name]["seconds"],
                                "producer_seconds": pair[name]["producer_seconds"],
                                "response_bytes": pair[name]["response_bytes"],
                                "requests": pair[name]["summary"]["requests"],
                                "source_parses": pair[name]["summary"]["stats"][
                                    "source_parses"
                                ],
                                "cache_hits": pair[name]["summary"]["cache_hits"],
                                "query_cache_hits": pair[name]["summary"]["stats"].get(
                                    "query_cache_hits", 0
                                ),
                                "file_evaluation_cache_hits": pair[name]["summary"][
                                    "stats"
                                ].get("file_evaluation_cache_hits", 0),
                            }
                            for name in binaries
                        },
                    }
                    for pair in runs
                ],
                "limitations": "Native macOS shared host; warm immutable snapshots; native constant-score fixture; capture excluded. Both services resident and query order alternates. Every timed query has identical matched evidence and scan statistics, zero provider calls and zero source parses. CPU is cumulative process CPU sampled with Darwin ps (0.01s resolution) across the campaign, divided by queries. Contended/high-variance elapsed measurements are not performance guarantees. No live-model quality or independent Linux hardware claims.",
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                json.dumps(
                    {
                        key: report[key]
                        for key in (
                            "median_seconds",
                            "cpu_seconds_per_query",
                            "relative_mad",
                            "high_variance",
                            "median_paired_speedup",
                        )
                    }
                )
            )
        finally:
            for service in services.values():
                service.close()


if __name__ == "__main__":
    main()
