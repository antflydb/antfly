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

"""Qualify registered query handles, exact deltas and discovery journals.

Both paths use this exact binary, compact output and a native fixture. Client
elapsed includes framing/decoding; producer elapsed includes output backpressure.
Conservative forced capture and opt-in journal capture are labeled separately.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from session_compare import Service


class Client(Service):
    def exchange(self, op, *, consume=None, **fields):
        started = time.monotonic()
        before = self.wire_bytes
        self.response_start = before
        self.process.stdin.write(
            json.dumps({"version": 1, "id": 1, "op": op, **fields}).encode() + b"\n"
        )
        self.process.stdin.flush()
        rows = []
        first_event = None
        begun = False
        while True:
            row = self.read()
            if consume is None or not consume(row, self.last_record):
                rows.append(row)
            if first_event is None and row["type"] in (
                "match",
                "batch",
                "workspace_unchanged",
                "workspace_lexical_delta",
                "workspace_not_modified",
            ):
                first_event = time.monotonic() - started
            begun = begun or row["type"] == "workspace_begin"
            if row["type"] == "workspace_error" and not begun:
                raise RuntimeError(rows)
            if row["type"] in (
                "query_registered",
                "workspace_end",
                "workspace_ready",
                "workspace_trimmed",
                "workspace_not_modified",
                "workspace_invalidated",
                "query_forgotten",
                "workspace_closed",
            ):
                break
        result = {
            "seconds": time.monotonic() - started,
            "response_bytes": self.wire_bytes - before,
            "first_event_seconds": first_event,
        }
        if op == "refresh":
            self.ready = rows[-1]
            result["ready"] = rows[-1]
        if op == "search":
            if rows[-1]["exit_code"] not in (0, 1):
                raise RuntimeError(rows)
            if rows[-1]["type"] == "workspace_not_modified":
                if not rows[-1]["complete"]:
                    raise RuntimeError(rows)
                result.update(producer_seconds=None, summary=None)
                return result, rows
            summary = next(r for r in rows if r["type"] == "summary")
            if not summary["stats"]["complete"]:
                raise RuntimeError(rows)
            elapsed = summary["data"]["elapsed_total"]
            result.update(
                producer_seconds=elapsed["secs"] + elapsed["nanos"] / 1e9,
                summary=summary,
            )
        return result, rows


def rss_kib(service):
    if platform.system() != "Darwin":
        return None
    return int(
        subprocess.check_output(
            ["/bin/ps", "-p", str(service.process.pid), "-o", "rss="], text=True
        ).strip()
    )


def update_view(view, rows):
    # Reconstruct result data transactionally, as a consuming editor would.
    result = dict(view)
    files = {}
    for row in rows:
        if row["type"] == "workspace_delta":
            for path in row["removed"]:
                result.pop(path, None)
            result = {path: result[path] for path in row["order"] if path in result}
        elif row["type"] == "begin":
            path = row["data"]["path"]["text"]
            files[row["file_id"]] = (path, row["source"])
            result[path] = []
        elif row["type"] == "match":
            path, source = files[row["file_id"]]
            result[path].append(
                {
                    "data": row["data"],
                    "probability": row["probability"],
                    "scope": row["scope"],
                    "uri": source["uri"],
                    "revision": source["revision"],
                }
            )
    if not any(r["type"] in ("workspace_delta", "workspace_unchanged") for r in rows):
        result = {path: result[path] for path, _ in files.values()}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=51)
    parser.add_argument("--result-files", type=int, default=15)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 9:
        parser.error("at least nine rounds required")
    binary, library = args.binary.resolve(), args.library.resolve()
    cache = Path(__file__).resolve().parents[3] / ".zig-cache"
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    with tempfile.TemporaryDirectory(prefix="registered-", dir=cache) as directory:
        root = Path(directory)
        (root / "src").mkdir()
        for i in range(128):
            (root / "src" / f"f{i:05}.py").write_text(
                f"def retry_{i}():\n" + "    STOP()\n" * 32
            )
        env.update(HOME=str(root), XDG_CONFIG_HOME=str(root / "config"))
        services = {}
        queries = {}
        for name in ("ordinary", "registered"):
            (root / ("scores-" + name)).mkdir()
        for name in ("ordinary", "registered"):
            queries[name] = [
                "--ask=retry",
                "--model=fixture",
                "--backend=local",
                f"--library={library}",
                f"--cache-dir={root / ('scores-' + name)}",
                "--json-compact",
                "--no-refine",
                "--threads=2",
                f"--max-result-files={args.result_files}",
                "--max-requests=unlimited",
            ]
            services[name] = Client(
                binary, root, "src", queries[name], env, ("--workspace-watch",)
            )
        runs = {mode: [] for mode in ("full", "conditional", "edit")}
        refreshes = []
        try:
            _, registration = services["registered"].exchange(
                "register", args=queries["registered"]
            )
            handle = registration[0]["query_handle"]
            tokens, views = {}, {}
            for name, service in services.items():
                fields = (
                    {"query_handle": handle}
                    if name == "registered"
                    else {"args": queries[name]}
                )
                _, rows = service.exchange(
                    "search", revision=service.ready["revision"], **fields
                )
                tokens[name] = next(
                    r["result_token"] for r in rows if r["type"] == "result_token"
                )
                views[name] = update_view({}, rows)
            if views["ordinary"] != views["registered"]:
                raise RuntimeError("initial selected evidence differs")
            resident = {
                "after_warm": {
                    name: rss_kib(service) for name, service in services.items()
                }
            }
            for mode in ("full", "conditional"):
                for trial in range(args.rounds):
                    pair = {"load_average": os.getloadavg()}
                    for name in (
                        ("ordinary", "registered")
                        if trial % 2 == 0
                        else ("registered", "ordinary")
                    ):
                        service = services[name]
                        fields = (
                            {"query_handle": handle}
                            if name == "registered"
                            else {"args": queries[name]}
                        )
                        if mode == "conditional":
                            fields["if_result_token"] = tokens[name]
                        result, rows = service.exchange(
                            "search", revision=service.ready["revision"], **fields
                        )
                        if (
                            result["summary"]["requests"]
                            or result["summary"]["stats"]["query_cache_hits"] != 1
                        ):
                            raise RuntimeError(result)
                        views[name] = update_view(views[name], rows)
                        if mode == "conditional" and not any(
                            r["type"] == "workspace_unchanged" for r in rows
                        ):
                            raise RuntimeError(rows)
                        pair[name] = result
                    if views["ordinary"] != views["registered"]:
                        raise RuntimeError("warm selected evidence differs")
                    runs[mode].append(pair)
            for trial in range(19):
                (root / "src/f00127.py").write_text(
                    f"def retry_127():\n    pass # revision {trial}\n"
                )
                pair, capture = {"load_average": os.getloadavg()}, {}
                for name in (
                    ("ordinary", "registered")
                    if trial % 2 == 0
                    else ("registered", "ordinary")
                ):
                    service = services[name]
                    capture[name], _ = service.exchange(
                        "refresh", force_rescan=name == "ordinary"
                    )
                    fields = (
                        {
                            "query_handle": handle,
                            "delta": True,
                            "if_result_token": tokens[name],
                        }
                        if name == "registered"
                        else {"args": queries[name]}
                    )
                    result, rows = service.exchange(
                        "search", revision=service.ready["revision"], **fields
                    )
                    if name == "registered" and (
                        result["summary"]["stats"]["incremental_files_evaluated"] != 1
                        or not any(r["type"] == "workspace_delta" for r in rows)
                    ):
                        raise RuntimeError(rows)
                    views[name] = update_view(views[name], rows)
                    tokens[name] = next(
                        r["result_token"] for r in rows if r["type"] == "result_token"
                    )
                    pair[name] = result
                if views["ordinary"] != views["registered"]:
                    raise RuntimeError(
                        "delta reconstruction differs from full selected evidence"
                    )
                runs["edit"].append(pair)
                refreshes.append(capture)
            resident["after_edits"] = {
                name: rss_kib(service) for name, service in services.items()
            }
            trims = {
                name: service.exchange("trim")[1][-1]
                for name, service in services.items()
            }
            resident["after_trim"] = {
                name: rss_kib(service) for name, service in services.items()
            }
        finally:
            for service in services.values():
                service.close()
        report = {
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "fixture_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
            "host": platform.platform(),
            "cpu_count": os.cpu_count(),
            "fixture_only": True,
            "max_result_files": args.result_files,
            "native_rss_kib": resident,
            "trim": trims,
            "median": {
                mode: {
                    name: {
                        key: statistics.median(r[name][key] for r in trials)
                        for key in ("seconds", "producer_seconds", "response_bytes")
                    }
                    for name in services
                }
                for mode, trials in runs.items()
            },
            "refresh_median_seconds": {
                name: statistics.median(r[name]["seconds"] for r in refreshes)
                for name in services
            },
            "normalized_evidence_and_delta_reconstruction_equal": True,
            "runs": runs,
            "refreshes": refreshes,
            "limits": "Same production binary; ordinary request options versus registered immutable handles, both compact. Native fixture; warm caches; shared macOS host. Producer elapsed includes backpressure and excludes setup/summary serialization; client elapsed includes JSON/framing. Refresh compares forced conservative rescans with opt-in kernel journals, requiring event-visible writers. No native Linux hardware, cold-cache, live-model quality, literal zero-cost, or new lexical/JIT throughput claim.",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: report[k] for k in ("median", "refresh_median_seconds")}))


if __name__ == "__main__":
    main()
