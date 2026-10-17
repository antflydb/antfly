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

"""Measure shared-context transport with synthetic source and a loopback fixture.

Constant fixture scores measure serialization and orchestration, not relevance,
provider token usage, or actual provider cost. No live provider is contacted.
"""

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
    parser.add_argument(
        "--backend", choices=("typesafe", "decision"), default="typesafe"
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Warm and measure disk-cache hits instead of provider requests",
    )
    parser.add_argument("--lines", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (
        not 2 <= args.lines <= 4096
        or not 1 <= args.batch_size <= 128
        or args.rounds < 1
    ):
        parser.error("lines must be 2..4096, batch-size 1..128, and rounds positive")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            body = json.loads(raw)
            with self.server.lock:
                self.server.requests.append(
                    {
                        "bytes": len(raw),
                        "state_bytes": len(body.get("state", "").encode())
                        + sum(len(x.encode()) for x in body.get("contexts", [])),
                        "questions": len(body["questions"])
                        * len(body.get("inputs", [None])),
                    }
                )
            if args.backend == "typesafe":
                response = {
                    "answers": {
                        name: {"type": "noul", "noul": 0.9}
                        for name in body["questions"]
                    }
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
            encoded = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.lock = threading.Lock()
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    binaries = {
        "antfly-grep": args.binary.resolve(),
        "baseline": args.baseline.resolve(),
    }
    source = "".join(
        f"let value_{i:04} = process_request({i});\n" for i in range(args.lines)
    )
    report = {
        "evaluation": f"synthetic_{args.backend}_transport_fixture",
        "limitations": "Constant scores; no model quality, provider tokens, or actual cost measured.",
        "platform": platform.platform(),
        "cache": args.cache,
        "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "source_bytes": len(source.encode()),
        "lines": args.lines,
        "batch_size": args.batch_size,
        "binaries": {},
    }
    try:
        with tempfile.TemporaryDirectory(
            prefix="grep-decision-transport-"
        ) as directory:
            root = Path(directory)
            (root / "fixture.txt").write_text(source)
            env = {"PATH": os.environ.get("PATH", ""), "HOME": directory}
            expected = None
            for name, binary in binaries.items():
                report["binaries"][name] = {
                    "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    "runs": [],
                }
            for round_index in range(-1 if args.cache else 0, args.rounds):
                order = list(binaries) if round_index % 2 == 0 else reversed(binaries)
                for name in order:
                    with server.lock:
                        server.requests = []
                    start = time.perf_counter()
                    result = subprocess.run(
                        [
                            str(binaries[name]),
                            "--ask",
                            "find request processing",
                            "--backend",
                            "typesafe" if args.backend == "typesafe" else "http",
                            *(
                                ["--decision-context=shared-v1"]
                                if args.backend == "decision" and name == "antfly-grep"
                                else []
                            ),
                            "--model",
                            "fixture",
                            "--endpoint",
                            f"http://127.0.0.1:{server.server_port}/decisions",
                            "--broad",
                            *(
                                ["--cache-dir", str(root / (name + "-cache"))]
                                if args.cache
                                else ["--no-cache"]
                            ),
                            "--no-config",
                            "--refine",
                            "--chunk-lines",
                            str(args.lines),
                            "--batch-size",
                            str(args.batch_size),
                            "-j1",
                            "fixture.txt",
                        ],
                        cwd=root,
                        env=env,
                        stdin=subprocess.DEVNULL,
                        capture_output=True,
                        timeout=120,
                    )
                    elapsed = time.perf_counter() - start
                    if result.returncode != 0:
                        raise RuntimeError(result.stderr.decode(errors="replace"))
                    digest = hashlib.sha256(result.stdout).hexdigest()
                    if expected is None:
                        expected = digest
                    if digest != expected:
                        raise RuntimeError(
                            "fixture output differs between binaries/rounds"
                        )
                    with server.lock:
                        requests = server.requests.copy()
                    if round_index < 0:
                        continue
                    report["binaries"][name]["runs"].append(
                        {
                            "seconds": elapsed,
                            "output_sha256": digest,
                            "requests": len(requests),
                            "request_bytes": sum(r["bytes"] for r in requests),
                            "state_bytes": sum(r["state_bytes"] for r in requests),
                            "questions": sum(r["questions"] for r in requests),
                        }
                    )
            for binary in report["binaries"].values():
                binary["median_seconds"] = statistics.median(
                    r["seconds"] for r in binary["runs"]
                )
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
