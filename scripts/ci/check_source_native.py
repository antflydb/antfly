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

"""Qualify the shared source contract against a disposable real native owner."""

import argparse
import json
import os
from pathlib import Path
import socket
import shlex
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if not args.no_build:
        build_flags = shlex.split(os.environ.get("ZIG_BUILD_FLAGS", ""))
        subprocess.run(
            [
                "python3",
                "tools/run_bounded_zig_build.py",
                "--zig",
                os.environ.get("ZIG", "zig"),
                "--",
                "build",
                "antfly-inference-managed-embedder-test",
                "-Dtest-filter=managed embedder preserves coverage policy in storage config",
                *build_flags,
            ],
            cwd=root / "zig",
            check=True,
        )
        subprocess.run(
            [
                "python3",
                "tools/run_bounded_zig_build.py",
                "--zig",
                os.environ.get("ZIG", "zig"),
                "--",
                "build",
                "source-vector-contract-test",
                "query-contract-test",
                *build_flags,
            ],
            cwd=root / "zig",
            check=True,
        )
        subprocess.run(["make", "-C", "zig", "build"], cwd=root, check=True)
    with tempfile.TemporaryDirectory(prefix=".source-native-ci-", dir=root) as name:
        state = Path(name)

        def port():
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                return sock.getsockname()[1]

        api_port, health_port = port(), port()
        while health_port == api_port:
            health_port = port()
        config = state / "config.json"
        config.write_text(
            json.dumps(
                {
                    # This disposable owner only fetches the local fixture.
                    # Production content security defaults remain unchanged.
                    "remote_content": {
                        "security": {
                            "allowed_hosts": ["127.0.0.1"],
                            "block_private_ips": False,
                            "max_download_size_bytes": 2 << 20,
                        }
                    },
                    "inference": {
                        "models_dir": str(state / "models"),
                        "ml_dir": str(state / "ml"),
                    },
                }
            )
        )
        with (state / "server.log").open("w+") as log:
            server = subprocess.Popen(
                [
                    str(root / "zig/zig-out/bin/antfly"),
                    "standalone",
                    "--config",
                    str(config),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(api_port),
                    "--health-port",
                    str(health_port),
                    "--data-dir",
                    str(state / "data"),
                    "--replica-root-dir",
                    str(state / "replicas"),
                    "--replica-catalog-path",
                    str(state / "catalog"),
                    "--snapshot-root-dir",
                    str(state / "snapshots"),
                    "--control-tick-ms",
                    "5",
                ],
                cwd=root,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 45
                while True:
                    if server.poll() is not None:
                        raise RuntimeError("native owner exited during startup")
                    try:
                        with socket.create_connection(
                            ("127.0.0.1", api_port), timeout=0.2
                        ):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("native owner did not listen")
                        time.sleep(0.1)
                env = {
                    **os.environ,
                    "GOWORK": "off",
                    "ANTFLY_SOURCES_E2E_URL": f"http://127.0.0.1:{api_port}/db/v1",
                    "ANTFLY_SOURCES_KNOWLEDGE_RESTART_STATE": str(
                        state / "knowledge-restart.json"
                    ),
                }
                subprocess.run(
                    [
                        "go",
                        "-C",
                        "go/pkg/sources",
                        "test",
                        "-count=1",
                        "-v",
                        "-run",
                        "^TestNative(SourceSearchStreams|TranscriptSearchEvidence|PDFInputAdmission|SourceKnowledge|SourceKnowledgeRecipeMigration)$",
                        "-timeout",
                        "4m",
                    ],
                    cwd=root,
                    env=env,
                    check=True,
                    timeout=270,
                )
                subprocess.run(
                    [
                        "go",
                        "-C",
                        "go/pkg/sources",
                        "run",
                        "./examples/source-knowledge",
                        "--endpoint",
                        env["ANTFLY_SOURCES_E2E_URL"],
                        "--catalog",
                        str(state / "knowledge-catalog.json"),
                    ],
                    cwd=root,
                    env=env,
                    check=True,
                    timeout=150,
                )
                # Reopen the same durable owner and qualify migrated recipes,
                # shared curation and owner retraction without rebuilding inputs.
                command = server.args
                server.terminate()
                server.wait(timeout=15)
                server = subprocess.Popen(
                    command, cwd=root, stdout=log, stderr=subprocess.STDOUT
                )
                deadline = time.monotonic() + 45
                while True:
                    if server.poll() is not None:
                        raise RuntimeError("native owner exited during restart")
                    try:
                        with socket.create_connection(
                            ("127.0.0.1", api_port), timeout=0.2
                        ):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("restarted native owner did not listen")
                        time.sleep(0.1)
                subprocess.run(
                    [
                        "go",
                        "-C",
                        "go/pkg/sources",
                        "test",
                        "-count=1",
                        "-v",
                        "-run",
                        "^TestNativeSourceKnowledgeAfterRestart$",
                        "-timeout",
                        "3m",
                    ],
                    cwd=root,
                    env=env,
                    check=True,
                    timeout=210,
                )
            except BaseException:
                log.flush()
                log.seek(max(0, log.tell() - 8192))
                print(log.read(), flush=True)
                raise
            finally:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == "__main__":
    main()
