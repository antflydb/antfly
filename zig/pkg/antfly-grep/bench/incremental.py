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

"""Paired warm preparation, incremental decisions and indexed projection fixtures.

No model quality, real-provider tokens, storage-service latency or billing is
measured. Request counts, projected wire bytes and executable hashes are exact.
"""

import argparse
import copy
import hashlib
import json
import os
import platform
import socket
import statistics
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from compare import measure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=9)
    parser.add_argument(
        "--source",
        type=Path,
        help="Real Python source to measure (default: repository auth tests)",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    report = {
        "platform": platform.platform(),
        "rounds": args.rounds,
        "limitations": __doc__,
        "binaries": {},
        "cases": {},
    }
    state = {"decisions": 0, "questions": 0, "index_bytes": 0}
    state_lock = threading.Lock()
    document = {
        "version": 1,
        "path": "large.py",
        "uri": "s3://fixture/large.py",
        "revision": "r1",
        "indexed_at": 1,
        "permissions": {"public": True, "readers": []},
        "content": "padding\n" * 524288,
        "metadata": {"unused": "x" * 262144},
    }

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if "/tables/" in self.path:
                selected = {}
                for field in body["fields"]:
                    value = {"grep": document}
                    parts = field.split(".")
                    for part in parts:
                        if not isinstance(value, dict) or part not in value:
                            break
                        value = value[part]
                    else:
                        parent = selected
                        for part in parts[:-1]:
                            parent = parent.setdefault(part, {})
                        parent[parts[-1]] = copy.deepcopy(value)
                hits = (
                    []
                    if body.get("search_after")
                    else [{"_id": "a", "_sort": ["a"], "_source": selected}]
                )
                response = {
                    "responses": [
                        {
                            "status": 200,
                            "remote_snapshot": "snapshot-v1",
                            "hits": {"hits": hits},
                        }
                    ]
                }
            else:
                with state_lock:
                    state["decisions"] += 1
                    state["questions"] += len(body["inputs"]) * len(body["questions"])
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
            encoded = json.dumps(response, separators=(",", ":")).encode()
            if "/tables/" in self.path:
                with state_lock:
                    state["index_bytes"] += len(encoded)
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="grep-incremental-") as directory:
            root = Path(directory)
            source_path = (
                args.source
                or Path(__file__).resolve().parents[4] / "zig/e2e/antfly/test_auth.py"
            )
            source = source_path.read_text()
            report["source_path"] = str(source_path)
            (root / "source.py").write_text(source)
            (root / "lines.txt").write_text(
                "".join(f"STOP unique_{i}\n" for i in range(32))
            )
            report["source_sha256"] = hashlib.sha256(source.encode()).hexdigest()
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
            }
            env.update(HOME=str(root), XDG_CONFIG_HOME=str(root))
            endpoint = f"http://127.0.0.1:{server.server_port}"
            config = root / "tables.json"
            config.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "endpoint": endpoint,
                        "tables": [{"name": "sources"}],
                    }
                )
            )
            warm_commands = {}
            for name, binary in (
                ("antfly-grep", args.binary.resolve()),
                ("baseline", args.baseline.resolve()),
            ):
                report["binaries"][name] = {
                    "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    "bytes": binary.stat().st_size,
                }
                base = [
                    str(binary),
                    "--no-config",
                    "--ask=retry",
                    "--model=fixture",
                    "--endpoint",
                    endpoint + "/decisions",
                    "--cache-dir",
                    str(root / (name + "-cache")),
                    "--threads=4",
                    "--no-refine",
                ]
                warm_argv = [*base, "--json", "source.py"]
                subprocess.run(
                    warm_argv,
                    cwd=root,
                    env=env,
                    capture_output=True,
                    check=True,
                    timeout=60,
                )
                warm_commands[name] = warm_argv
                line_base = [*base, "--context-lines=0", "--unit=line", "lines.txt"]
                subprocess.run(
                    [*line_base, "--batch-size=8"],
                    cwd=root,
                    env=env,
                    check=True,
                    capture_output=True,
                    timeout=60,
                )
                before = state.copy()
                changed_batch = subprocess.run(
                    [*line_base, "--batch-size=3"],
                    cwd=root,
                    env=env,
                    check=True,
                    capture_output=True,
                    timeout=60,
                )
                report["cases"].setdefault("rebatching", {})[name] = {
                    "requests": state["decisions"] - before["decisions"],
                    "questions": state["questions"] - before["questions"],
                    "output_sha256": hashlib.sha256(changed_batch.stdout).hexdigest(),
                }
                (root / "lines.txt").write_text(
                    "STOP changed_0\n"
                    + "".join(f"STOP unique_{i}\n" for i in range(1, 32))
                )
                before = state.copy()
                changed_line = subprocess.run(
                    [*line_base, "--batch-size=8"],
                    cwd=root,
                    env=env,
                    check=True,
                    capture_output=True,
                    timeout=60,
                )
                report["cases"].setdefault("one-changed-line", {})[name] = {
                    "requests": state["decisions"] - before["decisions"],
                    "questions": state["questions"] - before["questions"],
                    "output_sha256": hashlib.sha256(changed_line.stdout).hexdigest(),
                }
                (root / "lines.txt").write_text(
                    "".join(f"STOP unique_{i}\n" for i in range(32))
                )
                before = state["index_bytes"]
                listing = subprocess.run(
                    [
                        str(binary),
                        "--no-config",
                        "--table-config",
                        str(config),
                        "--files",
                    ],
                    cwd=root,
                    env=env,
                    check=True,
                    capture_output=True,
                    timeout=60,
                )
                report["cases"].setdefault("indexed-listing", {})[name] = {
                    "response_bytes": state["index_bytes"] - before,
                    "output_sha256": hashlib.sha256(listing.stdout).hexdigest(),
                }
            # Alternate binaries each round so host-load drift cannot favor
            # all trials of one binary. Cache warming is excluded from timing.
            warm_runs = {name: [] for name in warm_commands}
            for round_index in range(args.rounds):
                order = list(warm_commands.items())
                if round_index % 2:
                    order.reverse()
                for name, command in order:
                    before = state["decisions"]
                    trial = measure(command, root, env=env)
                    trial["provider_requests"] = state["decisions"] - before
                    assert trial["exit"] == 0 and trial["provider_requests"] == 0
                    warm_runs[name].append(trial)
            for name, command in warm_commands.items():
                result = subprocess.run(
                    command,
                    cwd=root,
                    env=env,
                    capture_output=True,
                    check=True,
                    timeout=60,
                )
                events = [json.loads(row) for row in result.stdout.splitlines()]
                summary = events[-1]
                matches = [row["data"] for row in events if row["type"] == "match"]
                report["cases"].setdefault("warm-source-preparation", {})[name] = {
                    "runs": warm_runs[name],
                    "median_seconds": statistics.median(
                        r["seconds"] for r in warm_runs[name]
                    ),
                    "source_cache_hits": summary["stats"].get("source_cache_hits", 0),
                    "requests": summary["requests"],
                    "output_sha256": hashlib.sha256(
                        json.dumps(matches, sort_keys=True).encode()
                    ).hexdigest(),
                }
            for case in (
                "rebatching",
                "one-changed-line",
                "indexed-listing",
                "warm-source-preparation",
            ):
                pair = report["cases"][case]
                assert (
                    pair["antfly-grep"]["output_sha256"]
                    == pair["baseline"]["output_sha256"]
                ), case
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
