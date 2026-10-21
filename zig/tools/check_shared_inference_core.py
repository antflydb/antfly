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

"""Exercise the real compiled engine boundary without downloading models."""

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--server-binary", action="store_true")
    args = parser.parse_args()
    prefix = [str(args.binary.resolve())]
    if args.server_binary:
        prefix.append("inference")
    for command in (
        "embed",
        "classify",
        "generate",
        "chat",
        "compile-artifact",
        "export",
        "quantize",
        "run-artifact",
        "transcribe",
        "read",
        "extract",
        "finetune",
    ):
        result = subprocess.run(
            [*prefix, command, "--help"], capture_output=True, timeout=30
        )
        output = (result.stdout + result.stderr).decode()
        # Some authored offline commands print usage and return InvalidArguments
        # when --help occupies their mandatory model argument. Preserve that.
        if (
            result.returncode not in (0, 1)
            or not output.strip()
            or "(error.RuntimeBoundaryFailure)" in output
            or "untransportable error" in output
        ):
            raise RuntimeError(f"{command} help failed: {output}")
    with tempfile.TemporaryDirectory(prefix="antfly-engine-check-") as temporary:
        root = Path(temporary)
        models, ml = root / "models", root / "ml"
        models.mkdir()
        ml.mkdir()
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        log_path = root / "server.log"
        with log_path.open("w") as log:
            environment = os.environ.copy()
            environment["ANTFLY_INFERENCE_SUPERVISED_WORKER"] = "1"
            environment["ANTFLY_INFERENCE_SUPERVISOR_LIFELINE"] = "stdin-v1"
            process = subprocess.Popen(
                [
                    *prefix,
                    "run",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--models-dir",
                    str(models),
                    "--ml-dir",
                    str(ml),
                ],
                stdout=log,
                stderr=log,
                stdin=subprocess.PIPE,
                env=environment,
            )
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(
                            f"server exited early: {log_path.read_text()}"
                        )
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/healthz", timeout=1
                        ) as response:
                            assert response.status == 200
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise RuntimeError(
                                f"server did not become healthy: {log_path.read_text()}"
                            )
                        time.sleep(0.1)
                for path in ("/readyz", "/ml/v1/models"):
                    while True:
                        try:
                            with urllib.request.urlopen(
                                f"http://127.0.0.1:{port}{path}", timeout=5
                            ) as response:
                                assert response.status == 200
                                json.loads(response.read())
                            break
                        except urllib.error.HTTPError as error:
                            body = error.read().decode()
                            if path == "/readyz" and error.code == 503:
                                inventory = json.loads(body)
                                assert inventory["status"] == "not_ready"
                                assert all(
                                    count == 0 for count in inventory["models"].values()
                                )
                                break
                            if error.code != 503 or time.monotonic() >= deadline:
                                raise RuntimeError(
                                    f"{path}: HTTP {error.code}: {body}\n{log_path.read_text()}"
                                ) from error
                            time.sleep(0.1)
                process.send_signal(signal.SIGTERM)
                if (status := process.wait(timeout=10)) != 0:
                    raise RuntimeError(
                        f"graceful shutdown failed ({status}): {log_path.read_text()}"
                    )
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    print(
        "12 CLI help paths, HTTP health/readiness/model inventory, and graceful shutdown passed"
    )


if __name__ == "__main__":
    main()
