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

"""Measure bounded indexed page overlap with synthetic network/provider latency."""

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
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--delay-ms", type=int, default=40)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 1 or args.delay_ms < 0:
        parser.error("rounds must be positive and delay nonnegative")
    binary = args.binary.resolve()
    documents = [
        {
            "_id": str(i),
            "_sort": [str(i)],
            "_source": {
                "grep": {
                    "version": 1,
                    "path": f"source{i}.txt",
                    "uri": f"s3://fixture/{i}",
                    "revision": "fixture-v1",
                    "indexed_at": 1700000000,
                    "permissions": {"public": True, "readers": []},
                    "content": "STOP retry\n",
                }
            },
        }
        for i in range(8)
    ]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            time.sleep(args.delay_ms / 1000)
            if "/tables/" in self.path:
                after = body.get("search_after", [""])[0]
                assert body["order_by"] == [{"field": "_id", "desc": False}]
                assert not after or body["remote_snapshot"] == "fixture-snapshot"
                hits = [d for d in documents if d["_id"] > after][: body["limit"]]
                response = {
                    "responses": [
                        {
                            "status": 200,
                            "remote_snapshot": "fixture-snapshot",
                            "hits": {"hits": hits},
                        }
                    ]
                }
            else:
                response = {
                    "data": [
                        {
                            "input_index": i,
                            "id": item["id"],
                            "answers": [
                                {
                                    "name": q["name"],
                                    "type": "predicate",
                                    "decision_method": "typed",
                                    "probability": 0.9,
                                }
                                for q in body["questions"]
                            ],
                        }
                        for i, item in enumerate(body["inputs"])
                    ]
                }
            with self.server.lock:
                self.server.requests += 1
            raw = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.lock = threading.Lock()
    server.requests = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    report = {
        "platform": platform.platform(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "fixture_sha256": hashlib.sha256(
            json.dumps(documents, sort_keys=True).encode()
        ).hexdigest(),
        "delay_ms_per_request": args.delay_ms,
        "limitations": "Synthetic equal HTTP/provider latency; no live provider or storage service.",
        "runs": {"sequential": [], "prefetch": []},
    }
    expected = None
    try:
        with tempfile.TemporaryDirectory(prefix="grep-indexed-overlap-") as directory:
            root = Path(directory)
            env = {"PATH": os.environ.get("PATH", ""), "HOME": directory}
            for n in range(-1, args.rounds):
                for overlap in [False, True] if n % 2 else [True, False]:
                    config = {
                        "version": 1,
                        "endpoint": f"http://127.0.0.1:{server.server_port}/api/v1",
                        "page_size": 1,
                        "prefetch": overlap,
                        "tables": [{"name": "source"}],
                    }
                    path = root / "tables.json"
                    path.write_text(json.dumps(config))
                    with server.lock:
                        server.requests = 0
                    start = time.perf_counter()
                    result = subprocess.run(
                        [
                            str(binary),
                            "--table-config",
                            str(path),
                            "--ask",
                            "find retry logic",
                            "--model",
                            "fixture",
                            "--endpoint",
                            f"http://127.0.0.1:{server.server_port}/decisions",
                            "--no-cache",
                            "--max-requests=unlimited",
                            "--no-config",
                            "-j1",
                        ],
                        cwd=root,
                        env=env,
                        capture_output=True,
                        timeout=30,
                    )
                    elapsed = time.perf_counter() - start
                    if result.returncode != 0:
                        raise RuntimeError(result.stderr.decode())
                    digest = hashlib.sha256(result.stdout).hexdigest()
                    if expected is None:
                        expected = digest
                    if expected != digest:
                        raise RuntimeError("prefetch changed exact output")
                    if n >= 0:
                        report["runs"]["prefetch" if overlap else "sequential"].append(
                            {
                                "seconds": elapsed,
                                "requests": server.requests,
                                "output_sha256": digest,
                            }
                        )
        report["median_seconds"] = {
            k: statistics.median(r["seconds"] for r in runs)
            for k, runs in report["runs"].items()
        }
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
