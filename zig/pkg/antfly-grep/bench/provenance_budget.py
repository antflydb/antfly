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

"""Bounded indexed JSON encoding with heavily escaped provenance (fixture only)."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 1 or not hasattr(os, "wait4"):
        parser.error("positive rounds and POSIX wait4 required")
    binaries = {
        "antfly-grep": str(args.binary.resolve()),
        "baseline": str(args.baseline.resolve()),
    }
    document = {
        "version": 1,
        "path": "large.py",
        "content": "STOP\n",
        "uri": "s3://fixture/large.py",
        "revision": "immutable",
        "indexed_at": 1700000000,
        "permissions": {"public": True, "readers": []},
        "metadata": {"escaped": "\x01" * (2 * 1024 * 1024)},
    }
    response = json.dumps(
        {
            "responses": [
                {
                    "status": 200,
                    "took": 1,
                    "remote_snapshot": "fixture",
                    "hits": {
                        "hits": [
                            {"_id": "0", "_sort": ["0"], "_source": {"grep": document}}
                        ]
                    },
                }
            ]
        }
    ).encode()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    report = {
        "host": platform.platform(),
        "fixture_only": True,
        "metadata_bytes": 2 * 1024 * 1024,
        "output_budget": 1024,
        "limitations": "Shared macOS host, synthetic escaped metadata, no model inference; Linux RSS is omitted because wait4 may inherit the parent high-water mark",
        "binaries": {
            name: {"sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest()}
            for name, binary in binaries.items()
        },
        "cases": {},
    }
    cache_root = Path(__file__).resolve().parents[3] / ".zig-cache"
    try:
        with tempfile.TemporaryDirectory(
            prefix="grep-provenance-", dir=cache_root
        ) as directory:
            config = Path(directory) / "tables.json"
            config.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "endpoint": f"http://127.0.0.1:{server.server_port}/api/v1",
                        "page_size": 2,
                        "max_response_bytes": 16 * 1024 * 1024,
                        "tables": [{"name": "fixture"}],
                    }
                )
            )
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
            }
            env["HOME"] = directory
            cases = [
                (name, binary, flag)
                for name, binary in binaries.items()
                for flag in ("--json", "--json-compact")
            ]
            for trial in range(args.rounds):
                for name, binary, flag in cases if trial % 2 == 0 else reversed(cases):
                    started = time.perf_counter()
                    process = subprocess.Popen(
                        [
                            binary,
                            flag,
                            "--max-output-bytes=1024",
                            "--table-config",
                            str(config),
                            "STOP",
                        ],
                        cwd=directory,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    _, status, usage = os.wait4(process.pid, 0)
                    process.returncode = os.waitstatus_to_exitcode(status)
                    out, err = process.communicate()
                    if process.returncode != 2 or len(out) > 1024:
                        raise RuntimeError(
                            (process.returncode, len(out), err.decode(errors="replace"))
                        )
                    for line in out.splitlines():
                        json.loads(line)
                    report["cases"].setdefault(name + "/" + flag, []).append(
                        {
                            "seconds": time.perf_counter() - started,
                            "peak_rss_kib": usage.ru_maxrss / 1024
                            if platform.system() == "Darwin"
                            else None,
                            "output_bytes": len(out),
                            "exit": process.returncode,
                        }
                    )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    report["median_rss_kib"] = {
        name: statistics.median(run["peak_rss_kib"] for run in runs)
        if platform.system() == "Darwin"
        else None
        for name, runs in report["cases"].items()
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
